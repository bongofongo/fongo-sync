//! An in-memory sync server that follows the protocol closely enough to drive the client, and can
//! be told to misbehave; replicas over in-memory SQLite to sync through it; and a writer of
//! envelopes in formats this build does not read.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use fongo_sync_client::protocol::api::{
    AddDevice, Appended, BatchPage, Capabilities, CreateLibrary, DeviceRecord, Devices, ErrorBody,
    ErrorCode, ReadBatches, Route, SignedWrapKey, StoredBatch,
};
use fongo_sync_client::protocol::{
    BATCH_FORMAT, DeviceId, Envelope, HEADER_DEVICE, HEADER_NONCE, HEADER_SIGNATURE,
    HEADER_TIMESTAMP, LibraryId, LibraryKeys, ListedMemberProof, PROTOCOL_VERSION, RequestAuth,
    SigningKey, VerifyingKey, encode_batch, open, verify_envelope, verify_request,
};
use fongo_sync_client::sqlite::core::Change;
use fongo_sync_client::sqlite::{Collection, Registry, Replica};
use fongo_sync_client::{
    Credentials, Error, Method, Request, Response, SyncReport, Transport, TransportError, sync,
};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};

pub const NOW: u64 = 1_700_000_000;

pub fn now() -> u64 {
    NOW
}

type Tamper = Box<dyn FnMut(BatchPage) -> Vec<u8> + Send>;
type Respond = Box<dyn FnMut(&Request) -> Option<Response> + Send>;
type Lose = Box<dyn FnMut(&Request) -> bool + Send>;

pub struct Library {
    pub devices: Vec<DeviceRecord>,
    pub log: Vec<Vec<u8>>,
}

pub struct State {
    pub caps: Capabilities,
    pub now: u64,
    pub libraries: HashMap<LibraryId, Library>,
    seen: HashSet<(DeviceId, [u8; 16])>,
    /// Every request that reached the server, as method and target.
    pub requests: Vec<(Method, String)>,
    /// The length of the body answering each request in `requests`, whether or not it arrived.
    pub answered: Vec<usize>,
    /// An append of bytes the log already holds stores them again, as the protocol allows.
    pub store_again: bool,
    /// Loses the response to each request it returns true for, once handled.
    lose_if: Option<Lose>,
    /// Fails each request it returns true for before it reaches the server.
    fail_if: Option<Lose>,
    counted: usize,
    /// The n-th request from when it was set never reaches the server.
    fail_at: Option<usize>,
    /// The n-th request from when it was set is handled, but its response is lost.
    lose_at: Option<usize>,
    /// The n-th request from when it was set, handled or not, never gets an answer.
    hang_at: Option<(usize, bool)>,
    /// A request has hung.
    hung: bool,
    /// Woken when a request hangs.
    hung_waker: Option<std::task::Waker>,
    /// Rewrites every batch page before it is sent.
    tamper: Option<Tamper>,
    /// Answers a request in the server's place when it returns a response.
    respond: Option<Respond>,
    /// The next device list leaves out the device added last.
    pub stale_devices: bool,
    /// Keys every device list leaves out.
    pub hidden: Vec<VerifyingKey>,
    /// Stores this as every registered device's membership proof in place of the one sent, as a
    /// server that drops or mangles the field would.
    pub stored_proof: Option<Option<ListedMemberProof>>,
    /// Stores this as every registered device's wrap key in place of the one sent.
    pub stored_wrap: Option<Option<SignedWrapKey>>,
    /// Holds the first request it returns true for, before the server handles it or, when set,
    /// after, until released.
    hold: Option<(Lose, bool, Arc<Hold>)>,
}

/// A request held in the transport: [`Hold::reached`] returns once it is, and [`Hold::release`]
/// lets it go on.
#[derive(Default)]
pub struct Hold {
    reached: tokio::sync::Notify,
    released: tokio::sync::Notify,
}

impl Hold {
    pub async fn reached(&self) {
        self.reached.notified().await;
    }

    /// Lets whoever waits in [`Hold::reached`] go on, as though the request had been held.
    pub fn reach(&self) {
        self.reached.notify_one();
    }

    pub fn release(&self) {
        self.released.notify_one();
    }
}

pub struct Server {
    state: Mutex<State>,
}

fn error(code: ErrorCode) -> Response {
    Response {
        status: code.status(),
        headers: vec![("content-type".into(), "application/json".into())],
        body: serde_json::to_vec(&ErrorBody::new(code)).unwrap(),
    }
}

fn json<T: serde::Serialize>(status: u16, value: &T) -> Response {
    Response {
        status,
        headers: vec![("content-type".into(), "application/json".into())],
        body: serde_json::to_vec(value).unwrap(),
    }
}

fn header<'a>(request: &'a Request, name: &str) -> Result<&'a str, ErrorCode> {
    request
        .headers
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, v)| v.as_str())
        .ok_or(ErrorCode::Unauthenticated)
}

impl Server {
    pub fn new() -> Server {
        Server {
            state: Mutex::new(State {
                caps: Capabilities {
                    protocol_version: PROTOCOL_VERSION,
                    blobs: false,
                    notifications: true,
                    skew_secs: 300,
                    max_batch_bytes: Some(1 << 20),
                    max_library_bytes: None,
                    max_devices: Some(8),
                    max_page_batches: Some(4),
                    max_page_bytes: None,
                    max_blob_bytes: None,
                    max_query_ids: None,
                    blob_wait_days: None,
                    blob_incomplete_days: None,
                    device_removal: false,
                },
                now: NOW,
                libraries: HashMap::new(),
                seen: HashSet::new(),
                requests: Vec::new(),
                answered: Vec::new(),
                store_again: false,
                lose_if: None,
                fail_if: None,
                counted: 0,
                fail_at: None,
                lose_at: None,
                hang_at: None,
                hung: false,
                hung_waker: None,
                tamper: None,
                respond: None,
                stale_devices: false,
                hidden: Vec::new(),
                stored_proof: None,
                stored_wrap: None,
                hold: None,
            }),
        }
    }

    pub fn with<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        f(&mut self.state.lock().unwrap())
    }

    pub fn fail_at(&self, n: usize) {
        self.with(|s| {
            s.counted = 0;
            s.fail_at = Some(n);
        });
    }

    pub fn lose_at(&self, n: usize) {
        self.with(|s| {
            s.counted = 0;
            s.lose_at = Some(n);
        });
    }

    /// The n-th request, handled by the server when `handled`, never gets an answer.
    pub fn hang_at(&self, n: usize, handled: bool) {
        self.with(|s| {
            s.counted = 0;
            s.hang_at = Some((n, handled));
            s.hung = false;
        });
    }

    /// Returns once a request has hung.
    pub async fn hung(&self) {
        std::future::poll_fn(|cx| {
            self.with(|s| {
                if s.hung {
                    std::task::Poll::Ready(())
                } else {
                    s.hung_waker = Some(cx.waker().clone());
                    std::task::Poll::Pending
                }
            })
        })
        .await
    }

    pub fn tamper(&self, f: impl FnMut(BatchPage) -> Vec<u8> + Send + 'static) {
        self.with(|s| s.tamper = Some(Box::new(f)));
    }

    pub fn lose_if(&self, f: impl FnMut(&Request) -> bool + Send + 'static) {
        self.with(|s| s.lose_if = Some(Box::new(f)));
    }

    pub fn fail_if(&self, f: impl FnMut(&Request) -> bool + Send + 'static) {
        self.with(|s| s.fail_if = Some(Box::new(f)));
    }

    /// Bytes of the bodies answering requests to targets starting with `path`, from request
    /// number `from` on.
    pub fn bytes_since(&self, from: usize, path: &str) -> usize {
        self.with(|s| {
            s.requests[from..]
                .iter()
                .zip(&s.answered[from..])
                .filter(|((_, t), _)| t.starts_with(path))
                .map(|(_, n)| n)
                .sum()
        })
    }

    /// Holds the first request from now that `f` returns true for: before the server handles it,
    /// or with `handled` after, so that it is stored and only its answer waits.
    pub fn hold_if(
        &self,
        f: impl FnMut(&Request) -> bool + Send + 'static,
        handled: bool,
    ) -> Arc<Hold> {
        let hold = Arc::new(Hold::default());
        self.with(|s| s.hold = Some((Box::new(f), handled, hold.clone())));
        hold
    }

    pub fn respond(&self, f: impl FnMut(&Request) -> Option<Response> + Send + 'static) {
        self.with(|s| s.respond = Some(Box::new(f)));
    }

    pub fn behave(&self) {
        self.with(|s| {
            s.fail_at = None;
            s.lose_at = None;
            s.hang_at = None;
            s.tamper = None;
            s.respond = None;
            s.lose_if = None;
            s.fail_if = None;
            s.store_again = false;
            s.stale_devices = false;
            s.hidden.clear();
            s.hold = None;
        });
    }

    pub fn log(&self, library: LibraryId) -> Vec<Vec<u8>> {
        self.with(|s| s.libraries[&library].log.clone())
    }

    /// Appends bytes to a library's log without any of the checks an append gets.
    pub fn inject(&self, library: LibraryId, envelope: Vec<u8>) {
        self.with(|s| s.libraries.get_mut(&library).unwrap().log.push(envelope));
    }

    /// Puts other bytes at `seq` in a library's log, as a server read by a build that understands
    /// what was stored there would serve it.
    pub fn replace(&self, library: LibraryId, seq: u64, envelope: Vec<u8>) {
        self.with(|s| s.libraries.get_mut(&library).unwrap().log[seq as usize - 1] = envelope);
    }

    /// Keeps the first `len` envelopes of a library's log and drops the rest, as a server restored
    /// from a backup would.
    pub fn roll_back(&self, library: LibraryId, len: usize) {
        self.with(|s| {
            let log = &mut s.libraries.get_mut(&library).unwrap().log;
            assert!(len <= log.len(), "the log is shorter than {len}");
            log.truncate(len);
        });
    }

    /// Lists a key as a device without any request.
    pub fn register(&self, library: LibraryId, key: VerifyingKey) {
        self.with(|s| {
            s.libraries
                .get_mut(&library)
                .unwrap()
                .devices
                .push(DeviceRecord {
                    public_key: key,
                    recovery: false,
                    wrap: None,
                    member_proof: None,
                    removed: false,
                    last_seen: None,
                })
        });
    }

    /// Marks a listed key removed, as a removal would, without any request.
    pub fn mark_removed(&self, library: LibraryId, key: VerifyingKey) {
        self.with(|s| {
            let devices = &mut s.libraries.get_mut(&library).unwrap().devices;
            let record = devices
                .iter_mut()
                .find(|d| d.public_key == key)
                .expect("the key is listed");
            record.removed = true;
        });
    }

    /// Every change in the log, opened with the library's keys.
    pub fn changes(&self, keys: &LibraryKeys) -> Vec<Change> {
        self.with(|s| {
            let library = &s.libraries[&keys.id];
            let mut changes = Vec::new();
            for bytes in &library.log {
                let opened = open(bytes, keys, |id| {
                    library
                        .devices
                        .iter()
                        .find(|d| !d.recovery && DeviceId::of(&d.public_key) == id)
                        .map(|d| d.public_key)
                });
                if let Ok(opened) = opened {
                    changes.extend(opened.changes);
                }
            }
            changes
        })
    }

    /// Every change in the log, opened with the library's keys, taking copies of one envelope once.
    pub fn changes_once(&self, keys: &LibraryKeys) -> Vec<Change> {
        let log = self.log(keys.id);
        let mut seen = HashSet::new();
        let mut kept = Vec::new();
        for (i, bytes) in log.iter().enumerate() {
            if seen.insert(bytes.clone()) {
                kept.push(i);
            }
        }
        self.with(|s| {
            let library = &s.libraries[&keys.id];
            let mut changes = Vec::new();
            for i in kept {
                let opened = open(&library.log[i], keys, |id| {
                    library
                        .devices
                        .iter()
                        .find(|d| !d.recovery && DeviceId::of(&d.public_key) == id)
                        .map(|d| d.public_key)
                });
                if let Ok(opened) = opened {
                    changes.extend(opened.changes);
                }
            }
            changes
        })
    }

    pub fn count(&self, method: Method, path: &str) -> usize {
        self.with(|s| {
            s.requests
                .iter()
                .filter(|(m, t)| *m == method && t.starts_with(path))
                .count()
        })
    }
}

impl Transport for Server {
    async fn send(&self, request: Request) -> Result<Response, TransportError> {
        let held = self.with(|s| match s.hold.take() {
            Some((mut f, handled, hold)) => {
                if f(&request) {
                    Some((handled, hold))
                } else {
                    s.hold = Some((f, handled, hold));
                    None
                }
            }
            None => None,
        });
        let answer = match held {
            Some((false, hold)) => {
                hold.reach();
                hold.released.notified().await;
                self.exchange(request)
            }
            Some((true, hold)) => {
                let answer = self.exchange(request);
                hold.reach();
                hold.released.notified().await;
                answer
            }
            None => self.exchange(request),
        };
        match answer {
            Some(answer) => answer,
            None => std::future::pending().await,
        }
    }
}

impl Server {
    /// The answer to one request, or none when it hangs.
    fn exchange(&self, request: Request) -> Option<Result<Response, TransportError>> {
        let mut state = self.state.lock().unwrap();
        state.counted += 1;
        let n = state.counted;
        let fail = state.fail_if.as_mut().is_some_and(|f| f(&request));
        if state.fail_at == Some(n) || fail {
            return Some(Err(TransportError::new("connection refused")));
        }
        if state.hang_at == Some((n, false)) {
            state.hang();
            return None;
        }
        state
            .requests
            .push((request.method, request.target.clone()));
        let response = match state.respond.as_mut().and_then(|f| f(&request)) {
            Some(response) => response,
            None => state.handle(&request).unwrap_or_else(error),
        };
        state.answered.push(response.body.len());
        if state.hang_at == Some((n, true)) {
            state.hang();
            return None;
        }
        let lose = state.lose_if.as_mut().is_some_and(|f| f(&request));
        if state.lose_at == Some(n) || lose {
            return Some(Err(TransportError::new("connection reset")));
        }
        Some(Ok(response))
    }
}

impl State {
    fn hang(&mut self) {
        self.hung = true;
        if let Some(waker) = self.hung_waker.take() {
            waker.wake();
        }
    }

    fn authenticate(&mut self, request: &Request, key: &VerifyingKey) -> Result<(), ErrorCode> {
        let auth = RequestAuth::from_headers(
            header(request, HEADER_DEVICE)?,
            header(request, HEADER_TIMESTAMP)?,
            header(request, HEADER_NONCE)?,
            header(request, HEADER_SIGNATURE)?,
        )?;
        verify_request(
            &auth,
            key,
            request.method.as_str(),
            &request.target,
            &request.body,
            self.now,
            self.caps.skew_secs,
        )?;
        if !self.seen.insert((auth.device, auth.nonce)) {
            return Err(ErrorCode::ReplayedRequest);
        }
        Ok(())
    }

    /// The record of the library's key that signed the request.
    fn member(&mut self, request: &Request, id: LibraryId) -> Result<DeviceRecord, ErrorCode> {
        let device = DeviceId::from_hex(header(request, HEADER_DEVICE)?)
            .ok_or(ErrorCode::Unauthenticated)?;
        let library = self.libraries.get(&id).ok_or(ErrorCode::LibraryNotFound)?;
        let record = library
            .devices
            .iter()
            .find(|d| DeviceId::of(&d.public_key) == device)
            .cloned()
            .ok_or(ErrorCode::UnknownDevice)?;
        self.authenticate(request, &record.public_key)?;
        Ok(record)
    }

    fn handle(&mut self, request: &Request) -> Result<Response, ErrorCode> {
        let (route, query) = Route::parse(&request.target).ok_or(ErrorCode::NotFound)?;
        match (route, request.method) {
            (Route::Capabilities, Method::Get) => Ok(json(200, &self.caps)),
            (Route::Libraries, Method::Post) => {
                let create: CreateLibrary = serde_json::from_slice(&request.body)
                    .map_err(|_| ErrorCode::MalformedRequest)?;
                self.authenticate(request, &create.device_key)?;
                if !create.verify() || (create.wrap.is_some() && !create.verify_wrap()) {
                    return Err(ErrorCode::InvalidProof);
                }
                if self.libraries.contains_key(&create.library) {
                    return Err(ErrorCode::LibraryExists);
                }
                let devices = vec![
                    DeviceRecord {
                        public_key: create.device_key,
                        recovery: false,
                        wrap: self.stored_wrap.unwrap_or(create.wrap),
                        member_proof: self
                            .stored_proof
                            .unwrap_or(create.member_proof.map(Into::into)),
                        removed: false,
                        last_seen: None,
                    },
                    DeviceRecord {
                        public_key: create.recovery_key,
                        recovery: true,
                        wrap: None,
                        member_proof: None,
                        removed: false,
                        last_seen: None,
                    },
                ];
                self.libraries.insert(
                    create.library,
                    Library {
                        devices: devices.clone(),
                        log: Vec::new(),
                    },
                );
                Ok(json(201, &Devices { devices }))
            }
            (Route::Devices(id), Method::Get) => {
                self.member(request, id)?;
                let mut devices = self.libraries[&id].devices.clone();
                if std::mem::take(&mut self.stale_devices) {
                    devices.pop();
                }
                devices.retain(|d| !self.hidden.contains(&d.public_key));
                Ok(json(200, &Devices { devices }))
            }
            (Route::Devices(id), Method::Post) => {
                self.member(request, id)?;
                let add: AddDevice = serde_json::from_slice(&request.body)
                    .map_err(|_| ErrorCode::MalformedRequest)?;
                if add.wrap.is_some() && !add.verify_wrap(id) {
                    return Err(ErrorCode::InvalidProof);
                }
                let max = self.caps.max_devices;
                let library = self.libraries.get_mut(&id).unwrap();
                let record = DeviceRecord {
                    public_key: add.device_key,
                    recovery: false,
                    wrap: self.stored_wrap.unwrap_or(add.wrap),
                    member_proof: self
                        .stored_proof
                        .unwrap_or(add.member_proof.map(Into::into)),
                    removed: false,
                    last_seen: None,
                };
                let live = library
                    .devices
                    .iter()
                    .filter(|d| !d.recovery && !d.removed)
                    .count();
                let full = max.is_some_and(|max| live >= max as usize);
                let status = match library
                    .devices
                    .iter_mut()
                    .find(|d| d.public_key == record.public_key)
                {
                    Some(listed) if !listed.removed => 200,
                    Some(_) if full => return Err(ErrorCode::DeviceLimit),
                    Some(listed) => {
                        *listed = record;
                        201
                    }
                    None if full => return Err(ErrorCode::DeviceLimit),
                    None => {
                        library.devices.push(record);
                        200
                    }
                };
                Ok(json(
                    status,
                    &Devices {
                        devices: library.devices.clone(),
                    },
                ))
            }
            (Route::Batches(id), Method::Post) => {
                let record = self.member(request, id)?;
                if record.recovery {
                    return Err(ErrorCode::Forbidden);
                }
                if self
                    .caps
                    .max_batch_bytes
                    .is_some_and(|max| request.body.len() as u64 > max)
                {
                    return Err(ErrorCode::TooLarge);
                }
                let envelope = verify_envelope(&request.body, &record.public_key)?;
                if envelope.library != id {
                    return Err(ErrorCode::EnvelopeWrongLibrary);
                }
                let log = &mut self.libraries.get_mut(&id).unwrap().log;
                let stored = log.iter().position(|stored| *stored == request.body);
                let seq = match stored.filter(|_| !self.store_again) {
                    Some(at) => at + 1,
                    None => {
                        log.push(request.body.clone());
                        log.len()
                    }
                };
                Ok(json(200, &Appended { seq: seq as u64 }))
            }
            (Route::Batches(id), Method::Get) => {
                if self.member(request, id)?.recovery {
                    return Err(ErrorCode::Forbidden);
                }
                let read = ReadBatches::parse_query(query.unwrap_or(""))
                    .map_err(|_| ErrorCode::MalformedRequest)?;
                let limit = read
                    .limit
                    .unwrap_or(u32::MAX)
                    .min(self.caps.max_page_batches.unwrap_or(u32::MAX))
                    as usize;
                let log = &self.libraries[&id].log;
                let after = (read.after as usize).min(log.len());
                let batches: Vec<StoredBatch> = log[after..]
                    .iter()
                    .take(limit)
                    .enumerate()
                    .map(|(i, envelope)| StoredBatch {
                        seq: (after + i + 1) as u64,
                        envelope: envelope.clone(),
                    })
                    .collect();
                let page = BatchPage {
                    more: after + batches.len() < log.len(),
                    batches,
                };
                let body = match self.tamper.as_mut() {
                    Some(tamper) => tamper(page),
                    None => page.encode(),
                };
                Ok(Response {
                    status: 200,
                    headers: vec![("content-type".into(), "application/octet-stream".into())],
                    body,
                })
            }
            _ => Err(ErrorCode::MethodNotAllowed),
        }
    }
}

const SCHEMA: &[&str] = &[
    "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT NOT NULL, year INTEGER)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
        body TEXT)",
    "CREATE TABLE settings (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)",
];

pub fn registry() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("year"),
        Collection::minted("notes", "notes", "id")
            .owner("book_id", "books")
            .field_reporting_conflicts("body"),
        Collection::natural("settings", "settings", &["key"])
            .field("key")
            .field("value"),
    ])
    .unwrap()
}

pub struct Peer {
    pub conn: SqliteConnection,
    pub replica: Replica,
    pub credentials: Credentials,
}

impl Peer {
    pub async fn new(credentials: Credentials) -> Peer {
        let options = SqliteConnectOptions::from_str("sqlite::memory:").unwrap();
        Peer::create(options, credentials).await
    }

    /// A new replica over a database file.
    pub async fn new_at(path: &std::path::Path, credentials: Credentials) -> Peer {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true);
        Peer::create(options, credentials).await
    }

    async fn create(options: SqliteConnectOptions, credentials: Credentials) -> Peer {
        let mut conn = options.foreign_keys(true).connect().await.unwrap();
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }
        let replica = Replica::new(registry());
        replica.install(&mut conn).await.unwrap();
        Peer {
            conn,
            replica,
            credentials,
        }
    }

    /// A replica over a database file that already holds the schema, such as a backup.
    pub async fn open(path: &std::path::Path, credentials: Credentials) -> Peer {
        let mut conn = SqliteConnectOptions::new()
            .filename(path)
            .foreign_keys(true)
            .connect()
            .await
            .unwrap();
        let replica = Replica::new(registry());
        replica.install(&mut conn).await.unwrap();
        Peer {
            conn,
            replica,
            credentials,
        }
    }

    /// Writes a copy of the database to `path`.
    pub async fn back_up(&mut self, path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let path = path.to_str().unwrap();
        assert!(!path.contains('\''));
        sqlx::query(&format!("VACUUM INTO '{path}'"))
            .execute(&mut self.conn)
            .await
            .unwrap();
    }

    pub async fn run(&mut self, sql: &str) {
        sqlx::query(sql).execute(&mut self.conn).await.unwrap();
    }

    pub async fn sync(&mut self, server: &Server) -> Result<SyncReport, Error> {
        sync(
            &mut self.conn,
            &self.replica,
            server,
            &self.credentials,
            now,
        )
        .await
    }

    pub async fn cursor(&mut self) -> u64 {
        fongo_sync_client::cursor(&mut self.conn, self.credentials.library_id())
            .await
            .unwrap()
    }

    pub async fn lines(&mut self, sql: &str) -> Vec<String> {
        sqlx::query_scalar(sql)
            .fetch_all(&mut self.conn)
            .await
            .unwrap()
    }

    /// Every row, each with the global id of its entity and of the entity it names.
    pub async fn dump(&mut self) -> Vec<String> {
        let books: Vec<(i64, String, Option<i64>)> =
            sqlx::query_as("SELECT id, title, year FROM books")
                .fetch_all(&mut self.conn)
                .await
                .unwrap();
        let notes: Vec<(i64, Option<String>, Option<i64>)> =
            sqlx::query_as("SELECT id, body, book_id FROM notes")
                .fetch_all(&mut self.conn)
                .await
                .unwrap();
        let mut lines = Vec::new();
        for (id, title, year) in books {
            let gid = self.gid("books", Some(id)).await;
            let year = year.map_or("-".to_string(), |y| y.to_string());
            lines.push(format!(
                "book {} {title} y={year}",
                gid.as_deref().unwrap_or("UNMAPPED")
            ));
        }
        for (id, body, book) in notes {
            let gid = self.gid("notes", Some(id)).await;
            let book = self.gid("books", book).await;
            lines.push(format!(
                "note {} {} b={}",
                gid.as_deref().unwrap_or("UNMAPPED"),
                body.as_deref().unwrap_or("-"),
                book.as_deref().unwrap_or("-"),
            ));
        }
        lines.extend(
            self.settings()
                .await
                .into_iter()
                .map(|s| format!("setting {s}")),
        );
        lines.sort();
        lines
    }

    async fn gid(&mut self, collection: &str, id: Option<i64>) -> Option<String> {
        let id = id?;
        let gid = self
            .replica
            .gid_of(&mut self.conn, collection, id)
            .await
            .unwrap();
        gid.map(|gid| gid.to_hex())
    }

    /// The merged state: the winning entry of every field, and which entities are one.
    pub async fn merged(&mut self) -> String {
        let inspection = self.replica.inspect(&mut self.conn).await.unwrap();
        format!("{:#?}\n{:#?}", inspection.registers, inspection.aliases)
    }

    pub async fn settings(&mut self) -> Vec<String> {
        self.lines("SELECT key || '=' || value FROM settings ORDER BY key")
            .await
    }
}

/// Whether `request` appends an envelope.
pub fn is_append(request: &Request) -> bool {
    request.method == Method::Post && request.target.ends_with("/batches")
}

/// Whether `request` reads the log.
pub fn is_read(request: &Request) -> bool {
    request.method == Method::Get && request.target.contains("/batches")
}

/// A directory of its own for one test's files.
pub fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("client-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Syncs every peer in turn until a whole round moves nothing.
pub async fn settle(peers: &mut [Peer], server: &Server) {
    for _ in 0..10 {
        let mut moved = false;
        for peer in peers.iter_mut() {
            let report = peer.sync(server).await.unwrap();
            moved |= report.sent > 0 || report.received > 0;
        }
        if !moved {
            return;
        }
    }
    panic!("the peers never stopped sending");
}

/// Every peer holds the same rows and the same merged state, and every row has a global id.
pub async fn assert_converged(peers: &mut [Peer]) -> Vec<String> {
    let first = peers[0].dump().await;
    let merged = peers[0].merged().await;
    assert!(
        !first.iter().any(|line| line.contains("UNMAPPED")),
        "a row has no global id: {first:#?}"
    );
    for peer in peers.iter_mut().skip(1) {
        assert_eq!(peer.dump().await, first);
        assert_eq!(peer.merged().await, merged);
    }
    first
}

/// No change sits in the log twice but in copies of one envelope: nothing was sealed into two
/// different envelopes.
pub fn assert_no_change_sent_twice(server: &Server, keys: &LibraryKeys) {
    let changes = server.changes_once(keys);
    let mut seen = HashSet::new();
    for change in &changes {
        let key = (
            change.collection.clone(),
            change.gid,
            change.field.clone(),
            change.clock,
        );
        assert!(seen.insert(key), "sent twice: {change:?}");
    }
}

/// Seals `plaintext` as the protocol seals a batch, writing `format` as the envelope's format in
/// its bytes, its signature and its associated data: what a version that kept the layout but
/// wrote that format would send. With `ENVELOPE_FORMAT` and `encode_batch`'s output it is
/// exactly what `seal` writes.
pub fn seal_as(
    format: u8,
    plaintext: &[u8],
    keys: &LibraryKeys,
    epoch: u32,
    device: &SigningKey,
    nonce: [u8; 24],
) -> Vec<u8> {
    use chacha20poly1305::aead::{Aead, KeyInit, Payload};

    let mut info = b"fongo-sync/v1/batch-key".to_vec();
    info.extend_from_slice(&epoch.to_be_bytes());
    let mut key = [0u8; 32];
    hkdf::Hkdf::<sha2::Sha256>::new(Some(&keys.id.0), keys.secret())
        .expand(&info, &mut key)
        .unwrap();
    let device_id = DeviceId::of(&device.verifying_key());
    let mut aad = b"fongo-sync/v1/batch\0".to_vec();
    aad.push(format);
    aad.extend_from_slice(&keys.id.0);
    aad.extend_from_slice(&device_id.0);
    aad.extend_from_slice(&epoch.to_be_bytes());
    let ciphertext = chacha20poly1305::XChaCha20Poly1305::new(&key.into())
        .encrypt(
            chacha20poly1305::XNonce::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .unwrap();
    let mut out = vec![format];
    out.extend_from_slice(&keys.id.0);
    out.extend_from_slice(&device_id.0);
    out.extend_from_slice(&epoch.to_be_bytes());
    out.extend_from_slice(&nonce);
    put_varint(&mut out, ciphertext.len() as u64);
    out.extend_from_slice(&ciphertext);
    sign_unsigned(out, device)
}

/// Appends to an envelope's unsigned bytes `device`'s signature over them.
fn sign_unsigned(mut unsigned: Vec<u8>, device: &SigningKey) -> Vec<u8> {
    use ed25519_dalek::Signer;

    let mut message = b"fongo-sync/v1/envelope\0".to_vec();
    message.extend_from_slice(&unsigned);
    unsigned.extend_from_slice(&device.sign(&message).to_bytes());
    unsigned
}

/// `envelope` signed again by `device`, so that what was changed in it is all that is wrong.
pub fn re_signed(envelope: &Envelope, device: &SigningKey) -> Vec<u8> {
    let bytes = envelope.encode();
    sign_unsigned(bytes[..bytes.len() - 64].to_vec(), device)
}

/// `encode_batch`'s bytes with another format and flags.
pub fn batch_as(format: u8, flags: u8, changes: &[Change]) -> Vec<u8> {
    let mut bytes = encode_batch(changes);
    bytes[0] = format;
    bytes[1] = flags;
    bytes
}

/// A batch of `changes` in the current format, but with the first change's value written as
/// `tag` followed by `payload`, as a version with a type of value this build lacks would.
pub fn batch_with_value(changes: &[Change], tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![BATCH_FORMAT, 0];
    put_varint(&mut out, changes.len() as u64);
    let first = &changes[0];
    put_varint(&mut out, first.collection.len() as u64);
    out.extend_from_slice(first.collection.as_bytes());
    out.extend_from_slice(&first.gid.0);
    put_varint(&mut out, first.field.len() as u64);
    out.extend_from_slice(first.field.as_bytes());
    out.push(tag);
    out.extend_from_slice(payload);
    out.extend_from_slice(&first.clock.wall_ms.to_be_bytes());
    out.extend_from_slice(&first.clock.counter.to_be_bytes());
    out.extend_from_slice(&first.clock.node.0);
    for change in &changes[1..] {
        // A batch of one change: format, flags and a count of 1, then the change.
        out.extend_from_slice(&encode_batch(std::slice::from_ref(change))[3..]);
    }
    out
}

fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

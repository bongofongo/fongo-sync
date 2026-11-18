//! An in-memory sync server that follows the protocol closely enough to drive the client, and can
//! be told to misbehave; and replicas over in-memory SQLite to sync through it.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Mutex;

use fongo_sync_client::protocol::api::{
    AddDevice, Appended, BatchPage, Capabilities, CreateLibrary, DeviceRecord, Devices, ErrorBody,
    ErrorCode, ReadBatches, Route, StoredBatch,
};
use fongo_sync_client::protocol::{
    DeviceId, HEADER_DEVICE, HEADER_NONCE, HEADER_SIGNATURE, HEADER_TIMESTAMP, LibraryId,
    LibraryKeys, PROTOCOL_VERSION, RequestAuth, VerifyingKey, open, verify_envelope,
    verify_request,
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
    counted: usize,
    /// The n-th request from when it was set never reaches the server.
    fail_at: Option<usize>,
    /// The n-th request from when it was set is handled, but its response is lost.
    lose_at: Option<usize>,
    /// Rewrites every batch page before it is sent.
    tamper: Option<Tamper>,
    /// Answers a request in the server's place when it returns a response.
    respond: Option<Respond>,
    /// The next device list leaves out the device added last.
    pub stale_devices: bool,
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
                },
                now: NOW,
                libraries: HashMap::new(),
                seen: HashSet::new(),
                requests: Vec::new(),
                counted: 0,
                fail_at: None,
                lose_at: None,
                tamper: None,
                respond: None,
                stale_devices: false,
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

    pub fn tamper(&self, f: impl FnMut(BatchPage) -> Vec<u8> + Send + 'static) {
        self.with(|s| s.tamper = Some(Box::new(f)));
    }

    pub fn respond(&self, f: impl FnMut(&Request) -> Option<Response> + Send + 'static) {
        self.with(|s| s.respond = Some(Box::new(f)));
    }

    pub fn behave(&self) {
        self.with(|s| {
            s.fail_at = None;
            s.lose_at = None;
            s.tamper = None;
            s.respond = None;
            s.stale_devices = false;
        });
    }

    pub fn log(&self, library: LibraryId) -> Vec<Vec<u8>> {
        self.with(|s| s.libraries[&library].log.clone())
    }

    /// Appends bytes to a library's log without any of the checks an append gets.
    pub fn inject(&self, library: LibraryId, envelope: Vec<u8>) {
        self.with(|s| s.libraries.get_mut(&library).unwrap().log.push(envelope));
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
                })
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
        let mut state = self.state.lock().unwrap();
        state.counted += 1;
        let n = state.counted;
        if state.fail_at == Some(n) {
            return Err(TransportError::new("connection refused"));
        }
        state
            .requests
            .push((request.method, request.target.clone()));
        let response = match state.respond.as_mut().and_then(|f| f(&request)) {
            Some(response) => response,
            None => state.handle(&request).unwrap_or_else(error),
        };
        if state.lose_at == Some(n) {
            return Err(TransportError::new("connection reset"));
        }
        Ok(response)
    }
}

impl State {
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
                if !create.verify() {
                    return Err(ErrorCode::InvalidProof);
                }
                if self.libraries.contains_key(&create.library) {
                    return Err(ErrorCode::LibraryExists);
                }
                let devices = vec![
                    DeviceRecord {
                        public_key: create.device_key,
                        recovery: false,
                    },
                    DeviceRecord {
                        public_key: create.recovery_key,
                        recovery: true,
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
                Ok(json(200, &Devices { devices }))
            }
            (Route::Devices(id), Method::Post) => {
                self.member(request, id)?;
                let add: AddDevice = serde_json::from_slice(&request.body)
                    .map_err(|_| ErrorCode::MalformedRequest)?;
                let max = self.caps.max_devices;
                let library = self.libraries.get_mut(&id).unwrap();
                let record = DeviceRecord {
                    public_key: add.device_key,
                    recovery: false,
                };
                if !library.devices.contains(&record) {
                    let count = library.devices.iter().filter(|d| !d.recovery).count();
                    if max.is_some_and(|max| count >= max as usize) {
                        return Err(ErrorCode::DeviceLimit);
                    }
                    library.devices.push(record);
                }
                Ok(json(
                    200,
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
                let seq = match log.iter().position(|stored| *stored == request.body) {
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
    "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
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

const DUMP: &str = "
    SELECT 'book ' || COALESCE(hex(i.gid), 'UNMAPPED') || ' ' || b.title || ' y='
           || COALESCE(b.year, '-')
      FROM books b LEFT JOIN fongo_ids i ON i.collection = 'books' AND i.key = json_array(b.id)
    UNION ALL
    SELECT 'note ' || COALESCE(hex(i.gid), 'UNMAPPED') || ' ' || COALESCE(n.body, '-') || ' b='
           || COALESCE((SELECT hex(gid) FROM fongo_ids
                        WHERE collection = 'books' AND key = json_array(n.book_id)), '-')
      FROM notes n LEFT JOIN fongo_ids i ON i.collection = 'notes' AND i.key = json_array(n.id)
    UNION ALL
    SELECT 'setting ' || key || '=' || value FROM settings
    ORDER BY 1";

const SHADOW: &str = "
    SELECT hex(gid) || ' ' || field || ' ' || quote(value) || ' ' || is_ref || ' ' || clock
      FROM fongo_shadow ORDER BY 1";

pub struct Peer {
    pub conn: SqliteConnection,
    pub replica: Replica,
    pub credentials: Credentials,
}

impl Peer {
    pub async fn new(credentials: Credentials) -> Peer {
        let mut conn = SqliteConnectOptions::from_str("sqlite::memory:")
            .unwrap()
            .foreign_keys(true)
            .connect()
            .await
            .unwrap();
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

    pub async fn dump(&mut self) -> Vec<String> {
        self.lines(DUMP).await
    }

    pub async fn settings(&mut self) -> Vec<String> {
        self.lines("SELECT key || '=' || value FROM settings ORDER BY key")
            .await
    }
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
    let shadow = peers[0].lines(SHADOW).await;
    assert!(
        !first.iter().any(|line| line.contains("UNMAPPED")),
        "a row has no global id: {first:#?}"
    );
    for peer in peers.iter_mut().skip(1) {
        assert_eq!(peer.dump().await, first);
        assert_eq!(peer.lines(SHADOW).await, shadow);
    }
    first
}

/// No change sits in the log twice: nothing was sealed into two different envelopes.
pub fn assert_no_change_sent_twice(server: &Server, keys: &LibraryKeys) {
    let changes = server.changes(keys);
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

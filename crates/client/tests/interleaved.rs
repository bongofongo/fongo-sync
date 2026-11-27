//! Two or three syncs at once on one database, interleaved at random at every request they make
//! and every gap between their transactions, while the application writes, one or two other
//! devices write and sync, requests fail before the server sees them or lose their answers after,
//! and syncs crash. The server stores an identical envelope again or not, and pages hold one to
//! five envelopes, as each seed draws. The server is honest throughout, so no sync may find a fault in its log, and
//! every device ends with the same fingerprint of it. A run is a function of its seed: one sync runs at a time, each until it reaches a point
//! where it waits, and the databases' clocks and random bytes come from the seed. The clock stands
//! still for many writes and sometimes steps back.

#[allow(dead_code)]
mod support;

use std::ffi::{CString, c_int, c_void};
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;

use fongo_sync_client::{
    Apply, Error, Pace, Request, Response, SyncReport, Transport, TransportError, create_library,
    fingerprint, join_library, sync_with,
};
use libsqlite3_sys as ffi;
use sqlx::SqliteConnection;
use support::{Peer, Server, assert_converged, assert_no_change_sent_twice, now, scratch, settle};

const STEPS: usize = 90;

/// The seeds run are a fixed list, `FONGO_INTERLEAVED_SEEDS` of them from
/// `FONGO_INTERLEAVED_FROM`; 48 from 0 by default.
fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        splitmix(&mut self.0) % n
    }

    fn one_in(&mut self, n: u64) -> bool {
        self.below(n) == 0
    }
}

/// A wall clock shared by every database of a run: it moves one reading in eight, and one reading
/// in thirty-two steps back up to three milliseconds.
struct Wall {
    ms: u64,
    rng: u64,
}

struct Source {
    wall: Arc<Mutex<Wall>>,
    rng: Mutex<u64>,
}

/// Replaces `unixepoch` and `randomblob` on `conn` with the run's clock and a stream of its own.
async fn control(conn: &mut SqliteConnection, wall: &Arc<Mutex<Wall>>, stream: u64) {
    let mut handle = conn.lock_handle().await.unwrap();
    let db = handle.as_raw_handle().as_ptr();
    type Call =
        unsafe extern "C" fn(*mut ffi::sqlite3_context, c_int, *mut *mut ffi::sqlite3_value);
    for (name, call) in [
        ("unixepoch", unixepoch as Call),
        ("randomblob", randomblob as Call),
    ] {
        let source = Box::new(Source {
            wall: wall.clone(),
            rng: Mutex::new(stream ^ name.len() as u64),
        });
        let name = CString::new(name).unwrap();
        // SAFETY: `db` is the open connection's handle, locked for this call; SQLite owns the
        // source from here and frees it with `drop_source`.
        let status = unsafe {
            ffi::sqlite3_create_function_v2(
                db,
                name.as_ptr(),
                1,
                ffi::SQLITE_UTF8 | ffi::SQLITE_INNOCUOUS,
                Box::into_raw(source).cast::<c_void>(),
                Some(call),
                None,
                None,
                Some(drop_source),
            )
        };
        assert_eq!(status, ffi::SQLITE_OK);
    }
}

unsafe extern "C" fn drop_source(source: *mut c_void) {
    // SAFETY: SQLite passes back the pointer `control` gave it, once.
    drop(unsafe { Box::from_raw(source.cast::<Source>()) });
}

/// # Safety
/// Called by SQLite with a context whose user data is a `Source`.
unsafe fn source<'a>(ctx: *mut ffi::sqlite3_context) -> &'a Source {
    unsafe { &*ffi::sqlite3_user_data(ctx).cast::<Source>() }
}

unsafe extern "C" fn unixepoch(
    ctx: *mut ffi::sqlite3_context,
    _: c_int,
    _: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: registered with a `Source`.
    let source = unsafe { source(ctx) };
    let mut wall = source.wall.lock().unwrap();
    let roll = splitmix(&mut wall.rng);
    if roll.is_multiple_of(8) {
        wall.ms += 1;
    }
    if roll % 32 == 1 {
        wall.ms -= 1 + (roll >> 8) % 3;
    }
    // Half a millisecond on, so `CAST(x * 1000 AS INTEGER)` reads back the same millisecond.
    let seconds = (wall.ms as f64 + 0.5) / 1000.0;
    // SAFETY: `ctx` is the context SQLite called this with.
    unsafe { ffi::sqlite3_result_double(ctx, seconds) };
}

unsafe extern "C" fn randomblob(
    ctx: *mut ffi::sqlite3_context,
    _: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls this with one argument, as registered, and a `Source`.
    let n = unsafe { ffi::sqlite3_value_int64(*argv) }.clamp(1, 1 << 20) as usize;
    let source = unsafe { source(ctx) };
    let mut rng = source.rng.lock().unwrap();
    let bytes: Vec<u8> = (0..n.div_ceil(8))
        .flat_map(|_| splitmix(&mut rng).to_le_bytes())
        .take(n)
        .collect();
    // SAFETY: SQLite copies the bytes before this returns.
    unsafe {
        ffi::sqlite3_result_blob(
            ctx,
            bytes.as_ptr().cast::<c_void>(),
            n as c_int,
            ffi::SQLITE_TRANSIENT(),
        );
    }
}

/// Where a sync waits for the scheduler, and what the scheduler tells it on letting it go.
#[derive(Default)]
struct Slot {
    parked: bool,
    go: bool,
    /// The request fails before the server sees it, or its answer is lost.
    fail: bool,
}

type Shared = Arc<Mutex<Slot>>;

/// Waits until the scheduler lets this sync go on; whether it said to fail.
async fn park(slot: &Shared) -> bool {
    slot.lock().unwrap().parked = true;
    std::future::poll_fn(|_| {
        let mut slot = slot.lock().unwrap();
        if slot.go {
            slot.go = false;
            slot.parked = false;
            Poll::Ready(std::mem::take(&mut slot.fail))
        } else {
            Poll::Pending
        }
    })
    .await
}

/// The server, reached through a sync's slot: each request waits before the server handles it
/// and again before its answer comes back.
struct Gated {
    server: Arc<Server>,
    slot: Shared,
}

impl Transport for Gated {
    async fn send(&self, request: Request) -> Result<Response, TransportError> {
        if park(&self.slot).await {
            return Err(TransportError::new("connection refused"));
        }
        let answer = self.server.send(request).await;
        if park(&self.slot).await {
            return Err(TransportError::new("connection reset"));
        }
        answer
    }
}

/// Waits in every gap between the sync's transactions, and seals and applies in small pieces.
struct Paced {
    slot: Shared,
    seal: u32,
}

impl Apply for Paced {
    async fn between(&mut self, _: &mut SqliteConnection) -> Result<(), Error> {
        park(&self.slot).await;
        Ok(())
    }

    fn pace(&self) -> Pace {
        Pace {
            apply: 3,
            collect: 2,
            seal: self.seal,
            pause: std::time::Duration::ZERO,
            ..Pace::default()
        }
    }
}

type Running = Pin<Box<dyn Future<Output = (Peer, Result<SyncReport, Error>)> + Send>>;

struct Task {
    slot: Shared,
    future: Running,
}

/// Runs a sync until it waits for the scheduler, or ends and gives back its connection.
async fn advance(task: &mut Task) -> Option<(Peer, Result<SyncReport, Error>)> {
    std::future::poll_fn(|cx| match task.future.as_mut().poll(cx) {
        Poll::Ready(done) => Poll::Ready(Some(done)),
        Poll::Pending if task.slot.lock().unwrap().parked => Poll::Ready(None),
        Poll::Pending => Poll::Pending,
    })
    .await
}

fn start(mut peer: Peer, server: &Arc<Server>, seal: u32) -> Task {
    let slot = Shared::default();
    let transport = Gated {
        server: server.clone(),
        slot: slot.clone(),
    };
    let mut paced = Paced {
        slot: slot.clone(),
        seal,
    };
    let future = Box::pin(async move {
        let result = sync_with(
            &mut peer.conn,
            &peer.replica,
            &transport,
            &peer.credentials,
            now,
            &mut paced,
        )
        .await;
        (peer, result)
    });
    Task { slot, future }
}

/// A write of the application's, chosen by `rng`.
fn a_write(rng: &mut Rng, by: &str) -> String {
    let book = 1 + rng.below(6);
    match rng.below(7) {
        0 | 1 => format!(
            "INSERT INTO books (title, year) VALUES ('{by}', {})",
            rng.below(100)
        ),
        2 => format!(
            "UPDATE books SET year = {} WHERE id = {book}",
            rng.below(100)
        ),
        3 => format!(
            "INSERT INTO notes (book_id, body) SELECT id, '{by}' FROM books WHERE id = {book}"
        ),
        4 => format!(
            "UPDATE notes SET body = '{by} {}' WHERE book_id = {book}",
            rng.below(100)
        ),
        5 => format!(
            "INSERT INTO settings VALUES ('k{}', '{by}') \
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            rng.below(4)
        ),
        _ => format!("DELETE FROM books WHERE id = {book}"),
    }
}

async fn open(path: &Path, peer: &Peer, wall: &Arc<Mutex<Wall>>, stream: u64) -> Peer {
    let mut opened = Peer::open(path, peer.credentials.clone()).await;
    control(&mut opened.conn, wall, stream).await;
    opened
}

async fn in_flight(peer: &mut Peer) -> Option<Vec<u8>> {
    sqlx::query_scalar("SELECT sending_envelope FROM fongo_client_state")
        .fetch_one(&mut peer.conn)
        .await
        .unwrap()
}

async fn run(seed: u64) {
    let mut rng = Rng(seed.wrapping_mul(0xA076_1D64_78BD_642F));
    let wall = Arc::new(Mutex::new(Wall {
        ms: 1_900_000_000_000,
        rng: splitmix(&mut rng.0),
    }));
    let dir = scratch(&format!("interleaved-{seed}"));
    let path = dir.join("x.db");
    let server = Arc::new(Server::new());
    // A server may store an identical envelope again, and serves pages of its own size.
    let again = rng.one_in(2);
    let page = 1 + rng.below(5) as u32;
    server.with(|s| {
        s.store_again = again;
        s.caps.max_page_batches = Some(page);
    });
    let made = create_library(&*server, now).await.unwrap();
    let mut others = Vec::new();
    for _ in 0..1 + rng.below(2) {
        let joined = join_library(&*server, &made.recovery_code(), now)
            .await
            .unwrap();
        let mut other = Peer::new(joined).await;
        control(&mut other.conn, &wall, splitmix(&mut rng.0)).await;
        others.push(other);
    }
    let mut writer = Peer::new_at(&path, made).await;
    control(&mut writer.conn, &wall, splitmix(&mut rng.0)).await;

    let connections = 2 + rng.below(2) as usize;
    let mut idle: Vec<Peer> = Vec::new();
    for _ in 0..connections {
        let stream = splitmix(&mut rng.0);
        idle.push(open(&path, &writer, &wall, stream).await);
    }
    let mut running: Vec<Task> = Vec::new();
    let mut outcomes: Vec<Result<SyncReport, Error>> = Vec::new();

    let mut step = 0;
    while step < STEPS || !running.is_empty() {
        step += 1;
        let choice = if step >= STEPS { 9 } else { rng.below(10) };
        match choice {
            0 => {
                for _ in 0..1 + rng.below(4) {
                    let sql = a_write(&mut rng, "here");
                    writer.run(&sql).await;
                }
            }
            1 => {
                let at = rng.below(others.len() as u64) as usize;
                for _ in 0..rng.below(3) {
                    let sql = a_write(&mut rng, &format!("there{at}"));
                    others[at].run(&sql).await;
                }
                others[at].sync(&server).await.unwrap();
            }
            2 | 3 if !idle.is_empty() => {
                let peer = idle.remove(rng.below(idle.len() as u64) as usize);
                let mut task = start(peer, &server, 1 + rng.below(4) as u32);
                match advance(&mut task).await {
                    None => running.push(task),
                    Some((peer, result)) => {
                        outcomes.push(result);
                        idle.push(peer);
                    }
                }
            }
            _ if !running.is_empty() => {
                let at = rng.below(running.len() as u64) as usize;
                if step < STEPS && rng.one_in(30) {
                    drop(running.remove(at));
                    let stream = splitmix(&mut rng.0);
                    idle.push(open(&path, &writer, &wall, stream).await);
                    continue;
                }
                {
                    let mut slot = running[at].slot.lock().unwrap();
                    slot.fail = step < STEPS && rng.one_in(12);
                    slot.go = true;
                }
                match advance(&mut running[at]).await {
                    None => {}
                    Some((peer, result)) => {
                        running.remove(at);
                        outcomes.push(result);
                        idle.push(peer);
                    }
                }
            }
            _ => {}
        }
    }

    let keys = writer.credentials.library().clone();
    for outcome in &outcomes {
        match outcome {
            Ok(report) => assert!(
                report.head <= server.log(keys.id).len() as u64,
                "seed {seed}"
            ),
            Err(Error::Transport(_)) => {}
            Err(error) => panic!("seed {seed}: {error:?}"),
        }
    }
    assert_no_change_sent_twice(&server, &keys);

    let mut peers = vec![idle.pop().unwrap()];
    peers.extend(others);
    settle(&mut peers, &server).await;
    assert_no_change_sent_twice(&server, &keys);
    let log = server.log(keys.id).len() as u64;
    let first = fingerprint(&mut peers[0].conn, keys.id).await.unwrap();
    assert_eq!(first.seq, log, "seed {seed}");
    for peer in peers.iter_mut() {
        assert_eq!(peer.cursor().await, log, "seed {seed}");
        let print = fingerprint(&mut peer.conn, keys.id).await.unwrap();
        assert_eq!(print, first, "seed {seed}");
        assert_eq!(in_flight(peer).await, None, "seed {seed}");
        let unsent = peer.replica.outbox(&mut peer.conn, 1).await.unwrap();
        assert!(unsent.changes.is_empty(), "seed {seed}");
    }
    assert_converged(&mut peers).await;
}

#[tokio::test]
async fn interleaved_syncs_on_one_database_send_each_change_once_and_converge() {
    let from = env("FONGO_INTERLEAVED_FROM", 0);
    for seed in from..from + env("FONGO_INTERLEAVED_SEEDS", 48) {
        eprintln!("seed {seed}");
        run(seed).await;
    }
}

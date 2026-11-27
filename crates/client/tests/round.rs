//! The cost of one sync round that sends a few hundred changes from a library of tens of thousands
//! of rows: statements run on the sync's connection and wall time, printed. While each append is
//! in flight, another connection must be able to take the write lock at once.
//!
//! `cargo test --release -p fongo-sync-client --test round -- --ignored --nocapture`

#[allow(dead_code)]
mod support;

use std::ffi::{c_int, c_uint, c_void};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use fongo_sync_client::{Request, Response, Transport, TransportError, create_library, sync};
use libsqlite3_sys as ffi;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};
use support::{Peer, Server, is_append, now, scratch};

static STATEMENTS: AtomicUsize = AtomicUsize::new(0);

unsafe extern "C" fn count(_: c_uint, _: *mut c_void, _: *mut c_void, _: *mut c_void) -> c_int {
    STATEMENTS.fetch_add(1, Ordering::Relaxed);
    0
}

async fn trace(conn: &mut SqliteConnection) {
    let mut handle = conn.lock_handle().await.unwrap();
    // SAFETY: the handle is open and locked for this call; the callback reads nothing it is given.
    let status = unsafe {
        ffi::sqlite3_trace_v2(
            handle.as_raw_handle().as_ptr(),
            ffi::SQLITE_TRACE_STMT as c_uint,
            Some(count),
            std::ptr::null_mut(),
        )
    };
    assert_eq!(status, ffi::SQLITE_OK);
}

/// The server, with a check at each append that another connection takes the write lock at once.
struct Probing<'a> {
    server: &'a Server,
    other: tokio::sync::Mutex<SqliteConnection>,
    appends: AtomicUsize,
}

impl Transport for Probing<'_> {
    async fn send(&self, request: Request) -> Result<Response, TransportError> {
        if is_append(&request) {
            let mut other = self.other.lock().await;
            sqlx::query("BEGIN IMMEDIATE")
                .execute(&mut *other)
                .await
                .expect("the write lock is held while an append is in flight");
            sqlx::query("ROLLBACK").execute(&mut *other).await.unwrap();
            self.appends.fetch_add(1, Ordering::Relaxed);
        }
        self.server.send(request).await
    }
}

#[tokio::test]
#[ignore]
async fn measure() {
    const BOOKS: u64 = 20_000;
    const PENDING: u64 = 300;
    const ROUNDS: u64 = 7;
    let dir = scratch("round");
    let path = dir.join("x.db");
    let server = Server::new();
    server.with(|s| {
        s.caps.max_page_batches = None;
        s.caps.max_batch_bytes = Some(1 << 22);
    });
    let made = create_library(&server, now).await.unwrap();
    let mut x = Peer::new_at(&path, made).await;
    x.run(&format!(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {BOOKS})
         INSERT INTO books (id, title, year) SELECT i, 'Middlemarch ' || i, 1800 + i % 200 FROM n"
    ))
    .await;
    x.run(&format!(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {BOOKS})
         INSERT INTO notes (id, book_id, body) SELECT i, i, 'a note on book ' || i FROM n"
    ))
    .await;
    x.sync(&server).await.unwrap();
    let other = SqliteConnectOptions::new()
        .filename(&path)
        .busy_timeout(Duration::ZERO)
        .connect()
        .await
        .unwrap();
    let probing = Probing {
        server: &server,
        other: tokio::sync::Mutex::new(other),
        appends: AtomicUsize::new(0),
    };
    trace(&mut x.conn).await;
    let mut rounds = Vec::new();
    for round in 0..ROUNDS {
        x.run(&format!(
            "UPDATE books SET year = year + 1 WHERE id > {} AND id <= {}",
            round * PENDING,
            (round + 1) * PENDING
        ))
        .await;
        STATEMENTS.store(0, Ordering::Relaxed);
        let began = Instant::now();
        let report = sync(&mut x.conn, &x.replica, &probing, &x.credentials, now)
            .await
            .unwrap();
        let took = began.elapsed();
        assert_eq!(report.sent as u64, PENDING);
        rounds.push((took, STATEMENTS.load(Ordering::Relaxed)));
    }
    assert_eq!(probing.appends.load(Ordering::Relaxed) as u64, ROUNDS);
    rounds.sort();
    let (median, statements) = rounds[rounds.len() / 2];
    println!(
        "{BOOKS} books and notes, {PENDING} pending changes: median round {median:?}, \
         {statements} statements; every round {rounds:?}"
    );
}

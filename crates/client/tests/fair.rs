//! A large sync shares the database with other writers: no transaction of its own holds the write
//! lock for long, and between them it leaves room for a writer gated by the application and for
//! one that only SQLite's busy handler coordinates.

#[allow(dead_code)]
mod support;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use fongo_sync_client::sqlite::core::Change;
use fongo_sync_client::sqlite::{Applied, Replica};
use fongo_sync_client::{Apply, Credentials, Error, Pace, create_library, join_library, sync_with};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};
use support::{Peer, Server, now, scratch};
use tokio::sync::{Mutex, OwnedMutexGuard};

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Seals envelopes of at most `seal` changes.
struct Sealing(u32);

impl Apply for Sealing {
    fn pace(&self) -> Pace {
        Pace {
            seal: self.0,
            ..Pace::default()
        }
    }
}

/// A library of `books` books with a note each, six changes a book, made on one device and
/// appended in envelopes of at most `seal` changes; and the credentials of a second device.
async fn library(dir: &Path, books: u64, seal: u32) -> (Server, Credentials) {
    let server = Server::new();
    server.with(|s| {
        s.caps.max_page_batches = None;
        s.caps.max_batch_bytes = Some(1 << 22);
    });
    let made = create_library(&server, now).await.unwrap();
    let joined = join_library(&server, &made.recovery_code(), now)
        .await
        .unwrap();
    let mut a = Peer::new_at(&dir.join("a.db"), made).await;
    a.run(&format!(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {books})
         INSERT INTO books (id, title, year) SELECT i, 'Middlemarch ' || i, 1800 + i % 200 FROM n"
    ))
    .await;
    a.run(&format!(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {books})
         INSERT INTO notes (id, book_id, body) SELECT i, i, 'a note on book ' || i FROM n"
    ))
    .await;
    let mut sealing = Sealing(seal);
    let report = sync_with(
        &mut a.conn,
        &a.replica,
        &server,
        &a.credentials,
        now,
        &mut sealing,
    )
    .await
    .unwrap();
    assert_eq!(report.sent as u64, 6 * books);
    (server, joined)
}

/// The second device's replica over a file in WAL mode, with a table the writers write to.
async fn joining(dir: &Path, credentials: Credentials) -> (Peer, PathBuf) {
    let path = dir.join("b.db");
    let mut b = Peer::new_at(&path, credentials).await;
    b.run("PRAGMA journal_mode = WAL").await;
    b.run("CREATE TABLE w (id INTEGER PRIMARY KEY, at INTEGER)")
        .await;
    (b, path)
}

#[derive(Debug, Default)]
struct Waits {
    writes: usize,
    failed: usize,
    worst: Duration,
}

impl Waits {
    fn add(&mut self, waited: Duration, ok: bool) {
        self.writes += 1;
        self.failed += usize::from(!ok);
        self.worst = self.worst.max(waited);
    }
}

/// Writes a row every `every` on a connection of its own with a busy timeout of `busy`, until
/// `stop`, timing each write from when it was due. With a gate, each write holds it.
fn writer(
    path: PathBuf,
    busy: Duration,
    every: Duration,
    stop: Arc<AtomicBool>,
    gate: Option<Arc<Mutex<()>>>,
) -> std::thread::JoinHandle<Waits> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let mut conn = runtime.block_on(async {
            SqliteConnectOptions::new()
                .filename(&path)
                .busy_timeout(busy)
                .connect()
                .await
                .unwrap()
        });
        let mut waits = Waits::default();
        let mut due = Instant::now();
        while !stop.load(Ordering::Relaxed) {
            let gated = gate.as_ref().map(|gate| gate.blocking_lock());
            let wrote =
                runtime.block_on(sqlx::query("INSERT INTO w (at) VALUES (1)").execute(&mut conn));
            drop(gated);
            waits.add(due.elapsed(), wrote.is_ok());
            due += every;
            let now = Instant::now();
            if due > now {
                std::thread::sleep(due - now);
            } else {
                due = now;
            }
        }
        waits
    })
}

/// Applies as an application might: some work of its own per change, inside the transaction,
/// and the application's write gate held from one gap to the next, so its own writers go first
/// in each gap.
struct Gated {
    pace: Pace,
    work: Duration,
    gate: Option<Arc<Mutex<()>>>,
    held: Option<OwnedMutexGuard<()>>,
    applies: Vec<(usize, Duration)>,
    betweens: usize,
}

impl Gated {
    fn new(pace: Pace, work: Duration, gate: Option<Arc<Mutex<()>>>) -> Gated {
        Gated {
            pace,
            work,
            gate,
            held: None,
            applies: Vec::new(),
            betweens: 0,
        }
    }

    /// Takes the gate for the sync about to begin, which holds it but in the gaps.
    async fn hold(&mut self) {
        if let Some(gate) = &self.gate {
            self.held = Some(gate.clone().lock_owned().await);
        }
    }
}

impl Apply for Gated {
    async fn apply(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        changes: &[Change],
    ) -> Result<Applied, Error> {
        let began = Instant::now();
        let applied = replica.apply(conn, changes).await?;
        std::thread::sleep(self.work * changes.len() as u32);
        self.applies.push((changes.len(), began.elapsed()));
        Ok(applied)
    }

    async fn between(&mut self, _: &mut SqliteConnection) -> Result<(), Error> {
        self.betweens += 1;
        if let Some(gate) = &self.gate {
            self.held = None;
            self.held = Some(gate.clone().lock_owned().await);
        }
        Ok(())
    }

    fn pace(&self) -> Pace {
        self.pace
    }
}

/// A device joins a library of 6,000 changes, sent in envelopes of 100, and its application does
/// 100 µs of work per change inside each apply. Applied in one transaction, as when nothing bounds
/// it, that holds the write lock for about 0.85 s, and both writers below fail. Paced, no apply
/// is given more than 300 changes (about 45 ms), and the sync leaves the lock free for 100 ms
/// once it has held it for 100 ms.
///
/// Meanwhile two writers each write a row every 20 ms with a busy timeout of 500 ms: one through
/// the application's gate, which the hook opens between transactions, and one on a connection of
/// its own that only SQLite's busy handler coordinates. No write fails, and each waits at most
/// about the held time, one transaction and one pause: the gated writer waits out the pause too,
/// since the application keeps its gate until the hook.
#[tokio::test]
async fn a_large_sync_leaves_room_for_a_gated_writer_and_for_one_on_its_own() {
    let dir = scratch("fair");
    let (server, joined) = library(&dir, 1_000, 100).await;
    let (mut b, path) = joining(&dir, joined).await;
    let busy = Duration::from_millis(500);
    let every = Duration::from_millis(20);
    let stop = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(Mutex::new(()));
    let pace = Pace {
        apply: 300,
        hold: Duration::from_millis(100),
        pause: Duration::from_millis(100),
        ..Pace::default()
    };
    let mut gated = Gated::new(pace, Duration::from_micros(100), Some(gate.clone()));
    let writers = [
        writer(path.clone(), busy, every, stop.clone(), Some(gate)),
        writer(path.clone(), busy, every, stop.clone(), None),
    ];
    std::thread::sleep(Duration::from_millis(100));
    gated.hold().await;
    let report = sync_with(
        &mut b.conn,
        &b.replica,
        &server,
        &b.credentials,
        now,
        &mut gated,
    )
    .await;
    gated.held = None;
    std::thread::sleep(Duration::from_millis(100));
    stop.store(true, Ordering::Relaxed);
    let [inside, outside] = writers.map(|w| w.join().unwrap());
    assert_eq!(report.unwrap().received, 6_000);
    assert!(
        gated.applies.iter().all(|&(n, _)| n <= 300),
        "{:?}",
        gated.applies
    );
    assert_eq!(gated.betweens, gated.applies.len());
    let at = format!(
        "applies {:?}; gated {inside:?}; on its own {outside:?}",
        gated.applies
    );
    assert!(inside.writes > 20 && outside.writes > 20, "{at}");
    assert_eq!((inside.failed, outside.failed), (0, 0), "{at}");
    assert!(inside.worst < Duration::from_millis(300), "{at}");
    assert!(outside.worst < Duration::from_millis(400), "{at}");
    let _ = std::fs::remove_dir_all(dir);
}

/// A writer in a process of its own, for [`measure`]: writes every 20 ms to the database at
/// `FAIR_WRITER_DB` with a busy timeout of `FAIR_BUSY_MS` until the file `FAIR_STOP` exists, then
/// prints what it waited.
#[test]
#[ignore]
fn writer_process() {
    let Ok(db) = std::env::var("FAIR_WRITER_DB") else {
        return;
    };
    let stop_file = PathBuf::from(std::env::var("FAIR_STOP").unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let busy = Duration::from_millis(env("FAIR_BUSY_MS", 1000));
    let w = writer(
        db.into(),
        busy,
        Duration::from_millis(20),
        stop.clone(),
        None,
    );
    while !stop_file.exists() {
        std::thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    let waits = w.join().unwrap();
    println!(
        "WAITS writes {} failed {} worst {:?}",
        waits.writes, waits.failed, waits.worst
    );
}

/// The full-size measurement. `FAIR_BOOKS` books with a note each (six changes a book; 8,334 for
/// 50,000 changes and 33,334 for 200,000) reach a second device, in envelopes of 1,000, while a
/// writer writes every 20 ms with a busy timeout of `FAIR_BUSY_MS`. `FAIR_WRITER` picks it: `gated`
/// goes through the application's gate, `thread` has a connection of its own, `process` runs in
/// a process of its own, `none` does not run. `FAIR_APPLY`, `FAIR_HOLD_MS` and `FAIR_PAUSE_MS`
/// set the pace. Run it optimised:
///
/// ```text
/// FAIR_BOOKS=33334 FAIR_WRITER=process cargo test --release -p fongo-sync-client --test fair \
///     -- --ignored --nocapture measure
/// ```
///
/// On an 8 GB laptop, WAL mode, worst wait and failed writes with a 1 s busy timeout, and the
/// sync's time. Unbounded is `FAIR_APPLY=100000000 FAIR_PAUSE_MS=0`, one apply as before the
/// bound; the pace's defaults are 4,000 changes, 250 ms and 100 ms.
///
/// | changes | writer | unbounded | default pace, no pause | default pace |
/// |---|---|---|---|---|
/// | 50,000 | process | 0.94 s, 0 failed; 0.99 s | 0.95 s, 0; 1.16 s | 0.42 s, 0; 1.54 s |
/// | 50,000 | gated | | 0.13 s, 0; 1.15 s | 0.23 s, 0; 1.50 s |
/// | 200,000 | process | 1.11 s, 3 failed; 4.55 s | 1.13 s, 0; 6.91 s | 0.42 s, 0; 9.32 s |
/// | 200,000 | gated | | 0.21 s, 0; 6.98 s | 0.34 s, 0; 9.31 s |
#[tokio::test]
#[ignore]
async fn measure() {
    let books = env("FAIR_BOOKS", 8_334);
    let busy = Duration::from_millis(env("FAIR_BUSY_MS", 1000));
    let mode = std::env::var("FAIR_WRITER").unwrap_or_else(|_| "thread".into());
    let dir = scratch("fair-measure");
    let (server, joined) = library(&dir, books, 1_000).await;
    let (mut b, path) = joining(&dir, joined).await;
    let base = Pace::default();
    let pace = Pace {
        apply: env("FAIR_APPLY", base.apply as u64) as usize,
        hold: Duration::from_millis(env("FAIR_HOLD_MS", base.hold.as_millis() as u64)),
        pause: Duration::from_millis(env("FAIR_PAUSE_MS", base.pause.as_millis() as u64)),
        ..base
    };
    let gate = (mode == "gated").then(|| Arc::new(Mutex::new(())));
    let mut gated = Gated::new(pace, Duration::ZERO, gate.clone());
    let stop = Arc::new(AtomicBool::new(false));
    let stop_file = dir.join("stop");
    let mut child = None;
    let mut thread = None;
    match mode.as_str() {
        "process" => {
            child = Some(
                std::process::Command::new(std::env::current_exe().unwrap())
                    .args(["--ignored", "--exact", "writer_process", "--nocapture"])
                    .env("FAIR_WRITER_DB", &path)
                    .env("FAIR_STOP", &stop_file)
                    .env("FAIR_BUSY_MS", busy.as_millis().to_string())
                    .stdout(std::process::Stdio::piped())
                    .spawn()
                    .unwrap(),
            )
        }
        "none" => {}
        _ => {
            thread = Some(writer(
                path.clone(),
                busy,
                Duration::from_millis(20),
                stop.clone(),
                gate,
            ))
        }
    }
    std::thread::sleep(Duration::from_millis(300));
    gated.hold().await;
    let began = Instant::now();
    let report = sync_with(
        &mut b.conn,
        &b.replica,
        &server,
        &b.credentials,
        now,
        &mut gated,
    )
    .await;
    let took = began.elapsed();
    gated.held = None;
    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    std::fs::write(&stop_file, b"").unwrap();
    let waits = match (thread, child) {
        (Some(thread), _) => format!("{:?}", thread.join().unwrap()),
        (_, Some(child)) => {
            let out = child.wait_with_output().unwrap();
            String::from_utf8_lossy(&out.stdout)
                .lines()
                .find(|l| l.starts_with("WAITS"))
                .unwrap_or("no output")
                .to_string()
        }
        _ => "no writer".into(),
    };
    let report = report.unwrap();
    let longest = gated.applies.iter().map(|a| a.1).max().unwrap_or_default();
    let total: Duration = gated.applies.iter().map(|a| a.1).sum();
    eprintln!(
        "received {} in {took:?}: {} applies, longest {longest:?}, all {total:?}; {} gaps; \
         writer ({mode}, busy {busy:?}): {waits}",
        report.received,
        gated.applies.len(),
        gated.betweens,
    );
    let _ = std::fs::remove_dir_all(dir);
}

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
    /// When each statement that succeeded returned.
    #[allow(dead_code)]
    done: Vec<Instant>,
}

impl Waits {
    fn add(&mut self, waited: Duration, ok: bool) {
        self.writes += 1;
        self.failed += usize::from(!ok);
        self.worst = self.worst.max(waited);
        if ok {
            self.done.push(Instant::now());
        }
    }

    /// Statements that succeeded between `from` and `to`.
    fn within(&self, from: Instant, to: Instant) -> usize {
        self.done.iter().filter(|&&t| from < t && t < to).count()
    }
}

/// Writes a row, or with `read` reads the table, every `every` on a connection of its own with a
/// busy timeout of `busy`, until `stop`, timing each statement from when it was due. With a gate,
/// each statement holds it.
fn writer(
    path: PathBuf,
    busy: Duration,
    every: Duration,
    stop: Arc<AtomicBool>,
    gate: Option<Arc<Mutex<()>>>,
    read: bool,
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
        let sql = if read {
            "SELECT count(*) FROM w"
        } else {
            "INSERT INTO w (at) VALUES (1)"
        };
        let mut waits = Waits::default();
        let mut due = Instant::now();
        while !stop.load(Ordering::Relaxed) {
            let gated = gate.as_ref().map(|gate| gate.blocking_lock());
            let done = runtime.block_on(sqlx::query(sql).execute(&mut conn));
            drop(gated);
            waits.add(due.elapsed(), done.is_ok());
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
    /// When the first apply began and the last one ended.
    span: Option<(Instant, Instant)>,
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
            span: None,
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
        let first = self.span.map_or(began, |(first, _)| first);
        self.span = Some((first, Instant::now()));
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

/// A device joins a library of 6,000 changes, in envelopes of 1,000, and its application does
/// 100 µs of work per change inside each apply. Applied in one transaction, as when nothing bounds
/// it, that holds the write lock for about 0.85 s with only a gap of well under a millisecond
/// before `caught_up`. Paced at one envelope per apply and a pause of 200 ms before every
/// transaction after the first, the sync leaves six gaps.
///
/// Meanwhile two writers each write a row every 20 ms: one through the application's gate, which
/// the hook opens in each gap, and one on a connection of its own that only SQLite's busy handler
/// coordinates, which sleeps at most 100 ms between tries. What is checked does not depend on how
/// fast the machine is: each apply stays within the bound, the hook runs in every gap, no write
/// fails, and each writer finishes writes between the first apply and the last, at least one in
/// every other gap between them.
/// The only times checked are the writers' busy timeouts, 2 s, several times what they wait.
#[tokio::test]
async fn a_large_sync_leaves_room_for_a_gated_writer_and_for_one_on_its_own() {
    let dir = scratch("fair");
    let (server, joined) = library(&dir, 1_000, 1_000).await;
    let (mut b, path) = joining(&dir, joined).await;
    let busy = Duration::from_secs(2);
    let every = Duration::from_millis(20);
    let stop = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(Mutex::new(()));
    let pace = Pace {
        apply: 1_000,
        hold: Duration::ZERO,
        pause: Duration::from_millis(200),
        ..Pace::default()
    };
    let mut gated = Gated::new(pace, Duration::from_micros(100), Some(gate.clone()));
    let writers = [
        writer(path.clone(), busy, every, stop.clone(), Some(gate), false),
        writer(path.clone(), busy, every, stop.clone(), None, false),
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
    let sizes: Vec<usize> = gated.applies.iter().map(|&(n, _)| n).collect();
    assert_eq!(sizes, [1_000; 6]);
    assert_eq!(gated.betweens, 6);
    let at = format!(
        "applies {:?}; gated {inside:?}; on its own {outside:?}",
        gated.applies
    );
    assert_eq!((inside.failed, outside.failed), (0, 0), "{at}");
    let (first, last) = gated.span.unwrap();
    for waits in [&inside, &outside] {
        assert!(waits.within(first, last) >= 3, "{at}");
    }
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
        false,
    );
    while !stop_file.exists() {
        std::thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    let waits = w.join().unwrap();
    println!(
        "WAITS {} {} {}",
        waits.writes,
        waits.failed,
        waits.worst.as_micros()
    );
}

/// The full-size measurement. `FAIR_BOOKS` books with a note each (six changes a book; 8,334 for
/// 50,000 changes and 33,334 for 200,000) reach a second device, in envelopes of 1,000, with the
/// database in `FAIR_JOURNAL` mode (`wal` or `delete`). Meanwhile, every 20 ms with a busy timeout
/// of `FAIR_BUSY_MS`, a process of its own writes a row, a connection in this process writes one
/// and another reads; or, with `FAIR_GATED=1`, a writer in this process writes through the
/// application's gate alone. `FAIR_APPLY`, `FAIR_HOLD_MS` and `FAIR_PAUSE_MS` set the pace.
/// Run it optimised:
///
/// ```text
/// FAIR_BOOKS=33334 FAIR_JOURNAL=delete cargo test --release -p fongo-sync-client --test fair \
///     -- --ignored --nocapture measure
/// ```
///
/// On an 8 GB laptop, with a 1 s busy timeout unless noted: the sync's time, then the worst wait
/// of each of the others, and how many of their statements failed. Unbounded is
/// `FAIR_APPLY=100000000 FAIR_PAUSE_MS=0`, one apply as before the bound; the defaults are 4,000
/// changes, 250 ms and 150 ms. Ranges are over the runs.
///
/// | journal | changes | pace | sync | writer here | writer, other process | reader |
/// |---|---|---|---|---|---|---|
/// | WAL | 50,000 | unbounded | 0.95 s | 0.95 s | 0.97–1.05 s | 0.01 s |
/// | WAL | 50,000 | default | 1.65–1.88 s | 0.40 s | 0.40–0.41 s | 0.01 s |
/// | WAL | 200,000 | unbounded | 4.42 s | 1.13–1.15 s, 3 of 40 failed | 1.13 s, 3 of 40 | 0.02 s |
/// | WAL | 200,000 | default | 10.4 s | 0.49–0.51 s | 0.80–0.82 s | 0.01 s |
/// | DELETE | 50,000 | unbounded | 0.90 s | 0.93–0.94 s | 0.85–0.94 s | 0.61–0.63 s |
/// | DELETE | 50,000 | default | 1.62–1.63 s | 0.39–0.40 s | 0.39–0.40 s | 0.09 s |
/// | DELETE | 200,000 | unbounded | 4.24–4.27 s | 1.13–1.14 s, 3 of 40 | 1.12–1.14 s, 3 of 40 | 1.13–1.14 s, 2 of 95 |
/// | DELETE | 200,000 | default, 6 runs | 9.6–10.6 s | 0.40–0.82 s | 0.41–1.14 s, 1 of 184 in one run | 0.17–0.28 s |
/// | DELETE | 200,000 | default, 5 s timeout | 9.6 s | 0.40–0.82 s | 0.93–0.95 s | 0.17–0.40 s |
///
/// Over ten runs each with a pause of 150, 200 and 250 ms (rollback-journal mode, 200,000
/// changes, 1 s timeout), no write failed at any of them and each had a run whose worst wait was
/// just over 1 s; each 50 ms more pause cost about a second of sync time. With a 1 s busy timeout
/// the wait is marginal at all of them, and the remedy is a longer timeout, not a longer pause.
///
/// A writer through the application's gate alone (`FAIR_GATED=1`), with the pause at zero, waited
/// at most 0.14 s at 50,000 changes and 0.23 s at 200,000 in WAL mode (sync 1.16 s and 6.97 s), and
/// 0.12 s and 0.19 s in rollback-journal mode.
#[tokio::test]
#[ignore]
async fn measure() {
    let books = env("FAIR_BOOKS", 8_334);
    let busy = Duration::from_millis(env("FAIR_BUSY_MS", 1000));
    let journal = std::env::var("FAIR_JOURNAL").unwrap_or_else(|_| "wal".into());
    let dir = scratch("fair-measure");
    let (server, joined) = library(&dir, books, 1_000).await;
    let (mut b, path) = joining(&dir, joined).await;
    b.run(&format!("PRAGMA journal_mode = {journal}")).await;
    let base = Pace::default();
    let pace = Pace {
        apply: env("FAIR_APPLY", base.apply as u64) as usize,
        hold: Duration::from_millis(env("FAIR_HOLD_MS", base.hold.as_millis() as u64)),
        pause: Duration::from_millis(env("FAIR_PAUSE_MS", base.pause.as_millis() as u64)),
        ..base
    };
    let every = Duration::from_millis(20);
    let stop = Arc::new(AtomicBool::new(false));
    let stop_file = dir.join("stop");
    let gated_only = env("FAIR_GATED", 0) == 1;
    let gate = gated_only.then(|| Arc::new(Mutex::new(())));
    let mut gated = Gated::new(pace, Duration::ZERO, gate.clone());
    let mut threads = Vec::new();
    let mut child = None;
    if gated_only {
        threads.push((
            "gated writer",
            writer(path.clone(), busy, every, stop.clone(), gate, false),
        ));
    } else {
        child = Some(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--ignored", "--exact", "writer_process", "--nocapture"])
                .env("FAIR_WRITER_DB", &path)
                .env("FAIR_STOP", &stop_file)
                .env("FAIR_BUSY_MS", busy.as_millis().to_string())
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap(),
        );
        threads.push((
            "writer",
            writer(path.clone(), busy, every, stop.clone(), None, false),
        ));
        threads.push((
            "reader",
            writer(path.clone(), busy, every, stop.clone(), None, true),
        ));
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
    let mut waits = Vec::new();
    for (name, thread) in threads {
        let w = thread.join().unwrap();
        waits.push(format!(
            "{name} {:.2} s, {}/{} failed",
            w.worst.as_secs_f64(),
            w.failed,
            w.writes
        ));
    }
    if let Some(child) = child {
        let out = child.wait_with_output().unwrap();
        let line = String::from_utf8_lossy(&out.stdout)
            .lines()
            .find_map(|l| l.strip_prefix("WAITS ").map(str::to_string))
            .unwrap_or_default();
        let n: Vec<u128> = line.split(' ').filter_map(|v| v.parse().ok()).collect();
        if let [writes, failed, worst] = n[..] {
            waits.push(format!(
                "process {:.2} s, {failed}/{writes} failed",
                worst as f64 / 1e6
            ));
        }
    }
    let report = report.unwrap();
    let longest = gated.applies.iter().map(|a| a.1).max().unwrap_or_default();
    eprintln!(
        "RESULT {journal} received {} in {:.2} s, {} applies, longest {:.0} ms; {}",
        report.received,
        took.as_secs_f64(),
        gated.applies.len(),
        longest.as_secs_f64() * 1e3,
        waits.join("; "),
    );
    let _ = std::fs::remove_dir_all(dir);
}

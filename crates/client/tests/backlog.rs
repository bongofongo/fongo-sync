//! A device that wrote much since it last synced collects it in paced steps: no transaction of
//! the sync collects more than about [`Pace::collect`] rows, other writers get their turns
//! between them, and what the server receives is what one collect of everything would send.

#[allow(dead_code)]
mod support;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fongo_sync_client::protocol::LibraryKeys;
use fongo_sync_client::sqlite::core::{Change, Gid, Hlc, Value};
use fongo_sync_client::sqlite::{
    Applied, COLLECT_ROWS, Collected, Collection, Registry, Replica, Resume,
};
use fongo_sync_client::{Apply, Error, Pace, check_connection, create_library, sync_with};
use sqlx::sqlite::{SqliteConnectOptions, SqliteOperation};
use sqlx::{ConnectOptions, Connection, SqliteConnection};
use support::{Peer, Server, now, scratch};

/// What each committed transaction on a connection did to the kernel's tables: rows it took off
/// the list of rows to collect, and changes it put in the outbox.
#[derive(Clone, Default)]
struct Transactions(Arc<Mutex<Counted>>);

/// Rows and changes of each committed transaction, then of the one open.
type Counted = (Vec<(u64, u64)>, (u64, u64));

impl Transactions {
    async fn watch(conn: &mut SqliteConnection) -> Transactions {
        let seen = Transactions::default();
        conn.ping().await.unwrap();
        let mut handle = conn.lock_handle().await.unwrap();
        let open = seen.0.clone();
        handle.set_update_hook(move |change| {
            let mut state = open.lock().unwrap();
            match (change.operation, change.table) {
                (SqliteOperation::Delete, "fongo_dirty") => state.1.0 += 1,
                (SqliteOperation::Insert, "fongo_outbox") => state.1.1 += 1,
                _ => {}
            }
        });
        let committed = seen.0.clone();
        handle.set_commit_hook(move || {
            let mut state = committed.lock().unwrap();
            let done = std::mem::take(&mut state.1);
            state.0.push(done);
            true
        });
        let rolled = seen.0.clone();
        handle.set_rollback_hook(move || {
            rolled.lock().unwrap().1 = (0, 0);
        });
        seen
    }

    /// Rows collected and changes made by each transaction that collected anything.
    fn collecting(&self) -> Vec<(u64, u64)> {
        let state = self.0.lock().unwrap();
        state
            .0
            .iter()
            .copied()
            .filter(|&(rows, _)| rows > 0)
            .collect()
    }
}

/// Paces a sync and records what it saw between transactions and before each apply.
struct Watched {
    pace: Pace,
    replica: Replica,
    /// Rows still to collect at each gap.
    gaps: Vec<usize>,
    /// Rows still to collect when each apply began.
    applies: Vec<usize>,
}

impl Apply for Watched {
    async fn apply(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        changes: &[Change],
    ) -> Result<Applied, Error> {
        self.applies
            .push(self.replica.uncollected_rows(conn, usize::MAX).await?);
        Ok(replica.apply(conn, changes).await?)
    }

    async fn between(&mut self, conn: &mut SqliteConnection) -> Result<(), Error> {
        self.gaps
            .push(self.replica.uncollected_rows(conn, usize::MAX).await?);
        Ok(())
    }

    fn pace(&self) -> Pace {
        self.pace
    }
}

/// A device of a new library over a file, with `books` books and a note on each written since
/// it was installed, none of it sent.
async fn backlog(dir: &Path, server: &Server, books: u64) -> (Peer, PathBuf) {
    let made = create_library(server, now).await.unwrap();
    let path = dir.join("a.db");
    let mut a = Peer::new_at(&path, made).await;
    write_books(&mut a.conn, books).await;
    (a, path)
}

/// `books` books and a note on each, fifty rows to a statement, each statement in a millisecond
/// of its own: a step never ends inside a millisecond, so rows written in one statement are
/// collected together.
async fn write_books(conn: &mut SqliteConnection, books: u64) {
    for table in ["books", "notes"] {
        for from in (1..=books).step_by(50) {
            let to = (from + 49).min(books);
            let sql = if table == "books" {
                format!(
                    "WITH RECURSIVE n(i) AS (SELECT {from} UNION ALL SELECT i + 1 FROM n WHERE i < {to})
                     INSERT INTO books (id, title, year) SELECT i, 'Middlemarch ' || i, 1800 + i % 200 FROM n"
                )
            } else {
                format!(
                    "WITH RECURSIVE n(i) AS (SELECT {from} UNION ALL SELECT i + 1 FROM n WHERE i < {to})
                     INSERT INTO notes (id, book_id, body) SELECT i, i, 'a note on book ' || i FROM n"
                )
            };
            sqlx::query(&sql).execute(&mut *conn).await.unwrap();
            std::thread::sleep(Duration::from_millis(2));
        }
    }
}

/// Writes a row every 20 ms on a connection of its own with a busy timeout of `busy`, until
/// `stop`. Returns how many writes failed and the longest any waited.
fn writer(
    path: PathBuf,
    busy: Duration,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<(usize, usize, Duration)> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut conn = SqliteConnectOptions::new()
                .filename(&path)
                .busy_timeout(busy)
                .connect()
                .await
                .unwrap();
            let (mut writes, mut failed, mut worst) = (0, 0, Duration::ZERO);
            while !stop.load(Ordering::Relaxed) {
                let began = Instant::now();
                let done = sqlx::query("INSERT INTO w (at) VALUES (1)")
                    .execute(&mut conn)
                    .await;
                writes += 1;
                failed += usize::from(done.is_err());
                worst = worst.max(began.elapsed());
                std::thread::sleep(Duration::from_millis(20));
            }
            (writes, failed, worst)
        })
    })
}

/// A device with 3,000 rows to collect syncs with steps of 200 rows. No transaction of the sync
/// collects more than a step, each apply finds fewer rows left than a step, `between` runs in
/// the gaps between steps, and a writer on another connection with a busy timeout of 2 s never
/// fails. Everything written is sent once.
#[tokio::test]
async fn a_backlog_is_collected_in_paced_steps() {
    let dir = scratch("backlog");
    let server = Server::new();
    let (mut a, path) = backlog(&dir, &server, 1_500).await;
    a.run("CREATE TABLE w (id INTEGER PRIMARY KEY, at INTEGER)")
        .await;
    let transactions = Transactions::watch(&mut a.conn).await;
    let stop = Arc::new(AtomicBool::new(false));
    let other = writer(path, Duration::from_secs(2), stop.clone());
    let mut watched = Watched {
        pace: Pace {
            collect: 200,
            hold: Duration::ZERO,
            pause: Duration::from_millis(30),
            ..Pace::default()
        },
        replica: a.replica.clone(),
        gaps: Vec::new(),
        applies: Vec::new(),
    };
    let report = sync_with(
        &mut a.conn,
        &a.replica,
        &server,
        &a.credentials,
        now,
        &mut watched,
    )
    .await
    .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    stop.store(true, Ordering::Relaxed);
    let (writes, failed, worst) = other.join().unwrap();
    assert_eq!(report.sent, 6 * 1_500);
    let collecting = transactions.collecting();
    let at = format!("{collecting:?}; gaps {:?}", watched.gaps);
    eprintln!("{at}; applies {:?}", watched.applies);
    assert!(collecting.len() >= 15, "{at}");
    assert!(collecting.iter().all(|&(rows, _)| rows <= 200), "{at}");
    assert_eq!(
        collecting.iter().map(|&(rows, _)| rows).sum::<u64>(),
        3_000,
        "{at}"
    );
    assert!(watched.applies.iter().all(|&left| left < 200), "{at}");
    assert!(watched.gaps.len() >= collecting.len() - 1, "{at}");
    assert!(watched.gaps.windows(2).all(|w| w[0] >= w[1]), "{at}");
    assert_eq!(failed, 0, "{writes} writes, worst {worst:?}");
    support::assert_no_change_sent_twice(&server, a.credentials.library());
    let _ = std::fs::remove_dir_all(dir);
}

/// Changes as the server's log holds them, with what differs between two runs of one script
/// renamed in order of first appearance: gids, nodes and clocks, which are ranked.
fn canonical(changes: &[Change]) -> Vec<String> {
    let mut gids: BTreeMap<Gid, usize> = BTreeMap::new();
    let mut nodes: BTreeMap<Gid, usize> = BTreeMap::new();
    let mut clocks: Vec<Hlc> = changes.iter().map(|c| c.clock).collect();
    clocks.sort();
    clocks.dedup();
    let name = |gid: Gid, gids: &mut BTreeMap<Gid, usize>| {
        let next = gids.len();
        *gids.entry(gid).or_insert(next)
    };
    changes
        .iter()
        .map(|c| {
            let gid = name(c.gid, &mut gids);
            let value = match &c.value {
                Value::Ref(g) => format!("ref {}", name(*g, &mut gids)),
                other => format!("{other:?}"),
            };
            let next = nodes.len();
            let node = *nodes.entry(c.clock.node).or_insert(next);
            let rank = clocks.binary_search(&c.clock).unwrap();
            format!(
                "{} {gid} {} {value} at {rank} by {node}",
                c.collection, c.field
            )
        })
        .collect()
}

/// A script of writes, each statement a millisecond or more after the last, so that runs order
/// their clocks alike.
async fn script(conn: &mut SqliteConnection, books: u64) {
    write_books(conn, books).await;
    for sql in [
        "UPDATE books SET title = title || '!' WHERE id % 3 = 0".to_string(),
        "DELETE FROM books WHERE id % 7 = 0".to_string(),
        "INSERT INTO settings SELECT 'key ' || id, 'value' FROM books WHERE id % 5 = 0".to_string(),
        "UPDATE notes SET body = 'again' WHERE id % 4 = 0".to_string(),
        format!(
            "INSERT INTO notes (book_id, body) SELECT id, 'second' FROM books WHERE id > {}",
            books / 2
        ),
        "UPDATE books SET year = 1871 WHERE id % 11 = 0".to_string(),
    ] {
        std::thread::sleep(Duration::from_millis(3));
        sqlx::query(&sql).execute(&mut *conn).await.unwrap();
    }
}

/// What a server receives from a device that ran [`script`] and synced with `pace`.
async fn received(books: u64, pace: Pace) -> Vec<String> {
    let dir = scratch(&format!("received-{}", pace.collect));
    let server = Server::new();
    let made = create_library(&server, now).await.unwrap();
    let keys: LibraryKeys = made.library().clone();
    let mut a = Peer::new_at(&dir.join("a.db"), made).await;
    script(&mut a.conn, books).await;
    let mut watched = Watched {
        pace,
        replica: a.replica.clone(),
        gaps: Vec::new(),
        applies: Vec::new(),
    };
    sync_with(
        &mut a.conn,
        &a.replica,
        &server,
        &a.credentials,
        now,
        &mut watched,
    )
    .await
    .unwrap();
    let _ = std::fs::remove_dir_all(dir);
    canonical(&server.changes(&keys))
}

/// The same script sent by a sync that collects in steps of 40 rows and by one that collects
/// everything in its apply, as before steps: the server receives the same changes, in the same
/// order, with clocks in the same order.
#[tokio::test]
async fn a_backlog_collected_in_steps_sends_what_one_collect_sends() {
    let pace = |collect| Pace {
        collect,
        hold: Duration::ZERO,
        pause: Duration::ZERO,
        ..Pace::default()
    };
    let whole = received(400, pace(usize::MAX)).await;
    let stepped = received(400, pace(40)).await;
    assert!(whole.len() > 2_000);
    assert_eq!(whole, stepped);
}

/// For comparing a build of this tree with one of another: writes what the server receives
/// from [`script`] over 16,667 books (100,000 rows and more) synced with the defaults, renamed
/// as [`canonical`] does, to the file `BACKLOG_DUMP` names.
///
/// ```text
/// BACKLOG_DUMP=/tmp/tip.txt cargo test --release -p fongo-sync-client --test backlog \
///     -- --ignored --nocapture dump_what_a_backlog_sends
/// ```
#[tokio::test]
#[ignore]
async fn dump_what_a_backlog_sends() {
    let Ok(out) = std::env::var("BACKLOG_DUMP") else {
        return;
    };
    let dir = scratch("dump");
    let server = Server::new();
    let made = create_library(&server, now).await.unwrap();
    let keys: LibraryKeys = made.library().clone();
    let mut a = Peer::new_at(&dir.join("a.db"), made).await;
    script(&mut a.conn, 16_667).await;
    let began = Instant::now();
    let report = a.sync(&server).await.unwrap();
    eprintln!(
        "sent {} in {:.2} s",
        report.sent,
        began.elapsed().as_secs_f64()
    );
    std::fs::write(out, canonical(&server.changes(&keys)).join("\n")).unwrap();
    let _ = std::fs::remove_dir_all(dir);
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

const MIXED: &[&str] = &[
    "CREATE TABLE books (
        id INTEGER PRIMARY KEY, title TEXT, isbn TEXT UNIQUE, year INTEGER, pages INTEGER)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        body TEXT)",
    "CREATE INDEX notes_book ON notes (book_id)",
    "CREATE TABLE tags (
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        tag TEXT NOT NULL,
        note TEXT,
        PRIMARY KEY (book_id, tag))",
    "CREATE TABLE wide (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d TEXT, e TEXT, f TEXT,
        g INTEGER, h INTEGER, i INTEGER, j INTEGER, k REAL, l REAL)",
    "CREATE TABLE w (id INTEGER PRIMARY KEY, at INTEGER)",
    "CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT,
        favourite INTEGER REFERENCES works(id) ON DELETE SET NULL)",
    "CREATE TABLE works (id INTEGER PRIMARY KEY, title TEXT,
        author_id INTEGER REFERENCES authors(id) ON DELETE SET NULL)",
    "CREATE TABLE highlights (id INTEGER PRIMARY KEY, body TEXT,
        next INTEGER REFERENCES highlights(id) ON DELETE SET NULL)",
];

/// Books with an identity, notes they own, tags with natural keys, and wide rows; and for
/// [`chained_writes`], works and authors that name each other, and highlights that name the next.
fn mixed() -> Registry {
    let mut wide = Collection::minted("wide", "wide", "id");
    for f in ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"] {
        wide = wide.field(f);
    }
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("isbn")
            .field("year")
            .field("pages")
            .identity(&["isbn"]),
        Collection::minted("notes", "notes", "id")
            .owner("book_id", "books")
            .field("body"),
        Collection::natural("tags", "tags", &["book_id", "tag"])
            .owner("book_id", "books")
            .field("tag")
            .field("note"),
        wide,
        Collection::minted("authors", "authors", "id")
            .field("name")
            .reference("favourite", "works"),
        Collection::minted("works", "works", "id")
            .field("title")
            .reference("author_id", "authors"),
        Collection::minted("highlights", "highlights", "id")
            .field("body")
            .reference("next", "highlights"),
    ])
    .unwrap()
}

/// `rows` rows that each name one written after them, so that no step can take fewer than all
/// of them: works and authors in turn, each work naming its author and each author the next
/// work as a favourite (`chain`), or highlights, each naming the next (`list`).
async fn chained_writes(conn: &mut SqliteConnection, rows: u64, shape: &str) {
    let seq = |n: u64| {
        format!("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {n})")
    };
    let mut writes = Vec::new();
    if shape == "chain" {
        let n = rows / 2;
        writes.push(format!(
            "{} INSERT INTO authors (id, name) SELECT i, 'author ' || i FROM n",
            seq(n)
        ));
        writes.push(format!(
            "{} INSERT INTO works (id, title) SELECT i, 'Middlemarch ' || i FROM n",
            seq(n)
        ));
        for i in 1..=n {
            writes.push(format!("UPDATE works SET author_id = {i} WHERE id = {i}"));
            if i < n {
                writes.push(format!(
                    "UPDATE authors SET favourite = {} WHERE id = {i}",
                    i + 1
                ));
            }
        }
    } else {
        writes.push(format!(
            "{} INSERT INTO highlights (id, body) SELECT i, 'passage ' || i FROM n",
            seq(rows)
        ));
        for i in 1..rows {
            writes.push(format!(
                "UPDATE highlights SET next = {} WHERE id = {i}",
                i + 1
            ));
        }
    }
    sqlx::query("BEGIN").execute(&mut *conn).await.unwrap();
    for sql in writes {
        sqlx::query(&sql).execute(&mut *conn).await.unwrap();
    }
    commit(conn).await;
}

/// About `rows` rows of every shape, a quarter each: books, a note on each, a tag on each, and
/// wide rows, the `round`th time new ones. A hundred of each shape to a statement, and each
/// statement in a millisecond of its own, since a step takes every row written in one.
async fn mixed_writes(conn: &mut SqliteConnection, rows: u64, round: u64) {
    let n = rows / 4;
    for lo in (round * n + 1..=round * n + n).step_by(100) {
        let hi = (lo + 99).min(round * n + n);
        let seq = format!(
            "WITH RECURSIVE n(i) AS (SELECT {lo} UNION ALL SELECT i + 1 FROM n WHERE i < {hi})"
        );
        for sql in [
            format!(
                "{seq} INSERT INTO books (id, title, isbn, year, pages)
                 SELECT i, 'Middlemarch ' || i, 'isbn ' || i, 1800 + i % 200, i FROM n"
            ),
            format!(
                "{seq} INSERT INTO notes (id, book_id, body) SELECT i, i, 'a note on book ' || i FROM n"
            ),
            format!("{seq} INSERT INTO tags (book_id, tag) SELECT i, 'read' FROM n"),
            format!(
                "{seq} INSERT INTO wide (id, a, b, c, d, e, f, g, h, i, j, k, l)
                 SELECT i, 'a' || i, 'b' || i, 'c' || i, 'd' || i, 'e' || i, 'f' || i,
                        i, i, i, i, i * 0.5, i * 0.25 FROM n"
            ),
        ] {
            sqlx::query(&sql).execute(&mut *conn).await.unwrap();
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// How long each write transaction on a connection held the write lock, from its first write
/// to its commit, and for the longest, how many rows of each table it wrote.
#[derive(Clone, Default)]
struct Holds(Arc<Mutex<Held>>);

#[derive(Default)]
struct Held {
    began: Option<Instant>,
    tables: BTreeMap<String, u64>,
    holds: Vec<Duration>,
    longest: (Duration, String),
}

impl Holds {
    async fn watch(conn: &mut SqliteConnection) -> Holds {
        let holds = Holds::default();
        conn.ping().await.unwrap();
        let mut handle = conn.lock_handle().await.unwrap();
        let first = holds.0.clone();
        handle.set_update_hook(move |change| {
            let mut state = first.lock().unwrap();
            state.began.get_or_insert_with(Instant::now);
            *state.tables.entry(change.table.to_string()).or_default() += 1;
        });
        let done = holds.0.clone();
        handle.set_commit_hook(move || {
            let mut state = done.lock().unwrap();
            let tables = std::mem::take(&mut state.tables);
            if let Some(began) = state.began.take() {
                let held = began.elapsed();
                state.holds.push(held);
                if held > state.longest.0 {
                    state.longest = (held, format!("{tables:?}"));
                }
            }
            true
        });
        let rolled = holds.0.clone();
        handle.set_rollback_hook(move || {
            let mut state = rolled.lock().unwrap();
            state.began = None;
            state.tables.clear();
        });
        holds
    }

    fn take(&self) -> Vec<Duration> {
        std::mem::take(&mut self.0.lock().unwrap().holds)
    }

    /// The longest transaction since the last call, and the rows it wrote by table.
    fn longest(&self) -> (Duration, String) {
        std::mem::take(&mut self.0.lock().unwrap().longest)
    }
}

/// Writes, or with `read` reads, every 20 ms on a connection of its own with a busy timeout of
/// `busy`, until `stop`; returns its statements, how many failed, and the longest wait.
fn every_20_ms(
    path: PathBuf,
    busy: Duration,
    stop: Arc<AtomicBool>,
    read: bool,
) -> std::thread::JoinHandle<(usize, usize, Duration)> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        runtime.block_on(async {
            let mut conn = SqliteConnectOptions::new()
                .filename(&path)
                .busy_timeout(busy)
                .connect()
                .await
                .unwrap();
            let sql = if read {
                "SELECT count(*) FROM w"
            } else {
                "INSERT INTO w (at) VALUES (1)"
            };
            let (mut n, mut failed, mut worst) = (0, 0, Duration::ZERO);
            let mut due = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                let done = sqlx::query(sql).execute(&mut conn).await;
                n += 1;
                failed += usize::from(done.is_err());
                worst = worst.max(due.elapsed());
                due += Duration::from_millis(20);
                let now = Instant::now();
                if due > now {
                    std::thread::sleep(due - now);
                } else {
                    due = now;
                }
            }
            (n, failed, worst)
        })
    })
}

/// The writer in a process of its own, for [`measure_backlog`]: writes every 20 ms to the
/// database at `BACKLOG_WRITER_DB` with a busy timeout of `BACKLOG_BUSY_MS` until the file
/// `BACKLOG_STOP` exists, then prints what it waited.
#[test]
#[ignore]
fn backlog_writer_process() {
    let Ok(db) = std::env::var("BACKLOG_WRITER_DB") else {
        return;
    };
    let stop_file = PathBuf::from(std::env::var("BACKLOG_STOP").unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let busy = Duration::from_millis(env("BACKLOG_BUSY_MS", 1_000));
    let w = every_20_ms(db.into(), busy, stop.clone(), false);
    while !stop_file.exists() {
        std::thread::sleep(Duration::from_millis(5));
    }
    stop.store(true, Ordering::Relaxed);
    let (n, failed, worst) = w.join().unwrap();
    println!("WAITS {n} {failed} {}", worst.as_micros());
}

/// Others using the database while a phase runs: a writer and a reader on connections here and
/// a writer in another process. Returns what each waited, as text.
async fn alongside<F: std::future::Future<Output = ()>>(
    path: &Path,
    busy: Duration,
    phase: F,
) -> (Duration, String) {
    let stop = Arc::new(AtomicBool::new(false));
    let stop_file = path.with_extension("stop");
    let _ = std::fs::remove_file(&stop_file);
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "backlog_writer_process",
            "--nocapture",
        ])
        .env("BACKLOG_WRITER_DB", path)
        .env("BACKLOG_STOP", &stop_file)
        .env("BACKLOG_BUSY_MS", busy.as_millis().to_string())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let writer = every_20_ms(path.into(), busy, stop.clone(), false);
    let reader = every_20_ms(path.into(), busy, stop.clone(), true);
    std::thread::sleep(Duration::from_millis(300));
    let began = Instant::now();
    phase.await;
    let took = began.elapsed();
    std::thread::sleep(Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    std::fs::write(&stop_file, b"").unwrap();
    let mut out = Vec::new();
    for (name, thread) in [("writer", writer), ("reader", reader)] {
        let (n, failed, worst) = thread.join().unwrap();
        out.push(format!("{name} {:.2} s {failed}/{n}", worst.as_secs_f64()));
    }
    let printed = child.wait_with_output().unwrap();
    let line = String::from_utf8_lossy(&printed.stdout)
        .lines()
        .find_map(|l| l.strip_prefix("WAITS ").map(str::to_string))
        .unwrap_or_default();
    let n: Vec<u128> = line.split(' ').filter_map(|v| v.parse().ok()).collect();
    if let [n, failed, worst] = n[..] {
        out.push(format!("process {:.2} s {failed}/{n}", worst as f64 / 1e6));
    }
    (took, out.join(", "))
}

/// Begins a transaction that takes the write lock at once, waiting for it in the busy handler:
/// the kernel's own transaction would read first, and a write after its read can fail at once.
async fn immediate(conn: &mut SqliteConnection) {
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .unwrap();
}

async fn commit(conn: &mut SqliteConnection) {
    sqlx::query("COMMIT").execute(&mut *conn).await.unwrap();
}

/// Collects in steps as an application would between syncs: steps of [`COLLECT_ROWS`] rows, and
/// once they have held the write lock 250 ms, a pause of 150 ms, as [`Pace`] does by default.
async fn in_steps(replica: &Replica, conn: &mut SqliteConnection, first: Collected) {
    let mut held = Duration::ZERO;
    let mut more = first.more;
    while more {
        if held >= Duration::from_millis(250) {
            tokio::time::sleep(Duration::from_millis(150)).await;
            held = Duration::ZERO;
        }
        let began = Instant::now();
        more = replica.collect_step(conn, COLLECT_ROWS).await.unwrap().more;
        held += began.elapsed();
    }
}

/// The measurement. A device writes `BACKLOG_ROWS` rows of mixed shapes (a quarter each of books
/// with an identity, notes they own, tags with natural keys, and rows of twelve fields) and then:
/// installs again, as an application does at each sync; writes as many more while its triggers
/// are suspended and resumes; writes as many more and syncs; then, twice, writes as many rows
/// that each name one written after them ([`chained_writes`]) and collects them. Meanwhile, every
/// 20 ms with a busy timeout of `BACKLOG_BUSY_MS`, a connection here writes a row and another
/// reads, and a process of its own writes one. With `BACKLOG_BOUNDED=0` each is done as before steps, in one
/// transaction; otherwise in steps. `BACKLOG_JOURNAL` is `wal` or `delete`. Run it optimised:
///
/// ```text
/// BACKLOG_ROWS=100000 BACKLOG_JOURNAL=delete cargo test --release -p fongo-sync-client \
///     --test backlog -- --ignored --nocapture measure_backlog
/// ```
///
/// On an 8 GB laptop, with a busy timeout of 1 s and another test running: how long each phase
/// took, its longest transaction, and the worst wait of the writer here, the reader and the
/// writer in another process, with how many of their statements failed where any did. "Whole" is
/// as before steps; resume collects whole either way. In steps the writers' worst waits are the
/// hold, one step and the pause of 150 ms, more when they compete for one gap, so a 1 s busy
/// timeout stays marginal for a writer in another process.
///
/// | journal | rows | phase | whole: took, longest, waits | steps: took, longest, waits |
/// |---|---|---|---|---|
/// | DELETE | 10,000 | install | 0.89 s, 0.88 s; 0.94, 0.81, 0.93 s | 1.39 s, 0.11 s; 0.42, 0.09, 0.90 s |
/// | DELETE | 10,000 | sync | 1.89 s, 1.11 s; 1.11, 1.09, 1.12 s | 2.90 s, 0.16 s; 0.82, 0.14, 0.82 s |
/// | WAL | 10,000 | install | 0.94 s, 0.89 s; 0.92, 0.01, 0.93 s | 1.61 s, 0.11 s; 0.50, 0.01, 0.40 s |
/// | WAL | 10,000 | sync | 1.73 s, 1.11 s; 1.14, 0.01, 1.11 s | 2.85 s, 0.15 s; 0.38, 0.01, 0.23 s |
/// | DELETE | 100,000 | install | 12.0 s, 12.0 s; 10 of 37, 10 of 70, 10 of 40 failed | 24.5 s, 0.20 s; 1.03, 0.52, 1.04 s |
/// | DELETE | 100,000 | sync | 31.6 s, 22.5 s; about 20 of 480 failed each | 62.8 s, 0.54 s; 1.11, 0.60, 1.11 s, 1 failed each |
/// | WAL | 100,000 | install | 11.6 s, 11.2 s; 10 of 49 failed, reader 0.01 s | 28.0 s, 0.24 s; 0.83, 0.01, 0.51 s |
/// | WAL | 100,000 | sync | 24.2 s, 17.7 s; 16 of 340 failed, reader 0.02 s | 65.0 s, 0.81 s; 0.83, 0.01, 0.92 s |
///
/// Rows that each name one written after them, a chain of works and authors or a list of
/// highlights, cannot be collected in part: one step takes them all, holding the lock as long as
/// one collect or longer. At 10,000 rows that is 0.75 to 0.77 s against 0.71 to 0.76 s; at
/// 100,000, 10.6 to 19.5 s against 10.3 to 11.8 s, and writers fail as they do for one collect.
///
/// At 10,000 rows no statement failed but one of a whole sync's. Resume, whole in both, held the
/// lock 1.0 s at 10,000 rows and 15 s at 100,000, where writers failed as above. A sync in steps
/// takes longer mostly for its pauses. Its longest transaction at 100,000 rows, 0.5 to 0.8 s, is
/// a step of 1,000 rows of mixed shapes in a library of about half a million changes.
#[tokio::test]
#[ignore]
async fn measure_backlog() {
    let rows = env("BACKLOG_ROWS", 10_000);
    let busy = Duration::from_millis(env("BACKLOG_BUSY_MS", 1_000));
    let bounded = env("BACKLOG_BOUNDED", 1) == 1;
    let journal = std::env::var("BACKLOG_JOURNAL").unwrap_or_else(|_| "wal".into());
    let dir = scratch("backlog-measure");
    let path = dir.join("a.db");
    let server = Server::new();
    server.with(|s| {
        s.caps.max_page_batches = None;
        s.caps.max_batch_bytes = Some(1 << 22);
    });
    let made = create_library(&server, now).await.unwrap();
    let mut conn = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .foreign_keys(true)
        .connect()
        .await
        .unwrap();
    sqlx::query(&format!("PRAGMA journal_mode = {journal}"))
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query(&format!(
        "PRAGMA busy_timeout = {}",
        busy.as_millis().max(5_000)
    ))
    .execute(&mut conn)
    .await
    .unwrap();
    for sql in MIXED {
        sqlx::query(sql).execute(&mut conn).await.unwrap();
    }
    let replica = Replica::new(mixed());
    replica.install(&mut conn).await.unwrap();
    let holds = Holds::watch(&mut conn).await;
    let mut lines = Vec::new();
    let mut report = |what: &str, took: Duration, waits: String, holds: Vec<Duration>| {
        let longest = holds.iter().max().copied().unwrap_or_default();
        lines.push(format!(
            "{journal} {rows} {} {what}: {:.2} s, {} transactions, longest {:.0} ms; {waits}",
            if bounded { "steps" } else { "whole" },
            took.as_secs_f64(),
            holds.len(),
            longest.as_secs_f64() * 1e3
        ));
    };

    mixed_writes(&mut conn, rows, 0).await;
    holds.take();
    let (took, waits) = alongside(&path, busy, async {
        if bounded {
            let first = replica.install_step(&mut conn, COLLECT_ROWS).await.unwrap();
            in_steps(&replica, &mut conn, first).await;
        } else {
            immediate(&mut conn).await;
            replica.install(&mut conn).await.unwrap();
            commit(&mut conn).await;
        }
    })
    .await;
    report("install", took, waits, holds.take());

    replica.suspend(&mut conn).await.unwrap();
    mixed_writes(&mut conn, rows, 1).await;
    holds.take();
    let (took, waits) = alongside(&path, busy, async {
        immediate(&mut conn).await;
        replica.resume(&mut conn, Resume::Send).await.unwrap();
        commit(&mut conn).await;
    })
    .await;
    report("resume", took, waits, holds.take());

    mixed_writes(&mut conn, rows, 2).await;
    holds.take();
    holds.longest();
    let credentials = made;
    let (took, waits) = alongside(&path, busy, async {
        let mut watched = Watched {
            pace: Pace {
                collect: if bounded { COLLECT_ROWS } else { usize::MAX },
                ..Pace::default()
            },
            replica: replica.clone(),
            gaps: Vec::new(),
            applies: Vec::new(),
        };
        sync_with(
            &mut conn,
            &replica,
            &server,
            &credentials,
            now,
            &mut watched,
        )
        .await
        .unwrap();
    })
    .await;
    report("sync", took, waits, holds.take());
    let (held, tables) = holds.longest();
    let sync = format!(
        "{journal} {rows} sync's longest transaction {:.0} ms wrote {tables}",
        held.as_secs_f64() * 1e3
    );

    for shape in ["chain", "list"] {
        chained_writes(&mut conn, rows, shape).await;
        holds.take();
        let (took, waits) = alongside(&path, busy, async {
            if bounded {
                let first = replica.collect_step(&mut conn, COLLECT_ROWS).await.unwrap();
                in_steps(&replica, &mut conn, first).await;
            } else {
                immediate(&mut conn).await;
                replica.collect(&mut conn).await.unwrap();
                commit(&mut conn).await;
            }
        })
        .await;
        report(shape, took, waits, holds.take());
    }
    lines.push(sync);
    for line in lines {
        eprintln!("RESULT {line}");
    }
    let _ = std::fs::remove_dir_all(dir);
}

/// Records what each apply and each step of collecting reported touching.
#[derive(Default)]
struct Touching {
    pace: Pace,
    touched: Vec<(String, String, bool)>,
}

impl Apply for Touching {
    async fn apply(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        changes: &[Change],
    ) -> Result<Applied, Error> {
        let applied = replica.apply(conn, changes).await?;
        for t in &applied.touched {
            self.touched
                .push((t.collection.clone(), t.key.clone(), t.deleted));
        }
        Ok(applied)
    }

    async fn collect(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        rows: usize,
    ) -> Result<Collected, Error> {
        let collected = replica.collect_step(conn, rows).await?;
        for t in &collected.touched {
            self.touched
                .push((t.collection.clone(), t.key.clone(), t.deleted));
        }
        Ok(collected)
    }

    fn pace(&self) -> Pace {
        self.pace
    }
}

/// A row held back by a unique index here is written once this device frees the title, by the
/// first step of collecting that follows: the step reports it to the application, in its own
/// transaction, as one apply of everything would have, and the sync's report lists it.
#[tokio::test]
async fn a_row_a_step_of_collecting_writes_is_reported_as_an_apply_would() {
    async fn freed(collect: usize) -> (Vec<(String, String, bool)>, Vec<String>, Vec<String>) {
        let server = Server::new();
        let made = create_library(&server, now).await.unwrap();
        let joined = fongo_sync_client::join_library(&server, &made.recovery_code(), now)
            .await
            .unwrap();
        let mut a = Peer::new(made).await;
        let mut b = Peer::new(joined).await;
        a.run("CREATE UNIQUE INDEX books_title ON books (title)")
            .await;
        a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
            .await;
        b.run("INSERT INTO books (title) VALUES ('Middlemarch')")
            .await;
        b.sync(&server).await.unwrap();
        a.sync(&server).await.unwrap();
        a.run("UPDATE books SET title = 'Middlemarch (mine)' WHERE title = 'Middlemarch'")
            .await;
        a.run("INSERT INTO settings VALUES ('k1', 'v'), ('k2', 'v'), ('k3', 'v')")
            .await;
        let mut touching = Touching {
            pace: Pace {
                collect,
                ..Pace::default()
            },
            ..Touching::default()
        };
        let report = sync_with(
            &mut a.conn,
            &a.replica,
            &server,
            &a.credentials,
            now,
            &mut touching,
        )
        .await
        .unwrap();
        let reported = report
            .applied
            .touched
            .iter()
            .map(|t| format!("{} {} {}", t.collection, t.key, t.deleted))
            .collect();
        let rows = a
            .lines("SELECT id || ' ' || title FROM books ORDER BY id")
            .await;
        (touching.touched, reported, rows)
    }
    let whole = freed(usize::MAX).await;
    let stepped = freed(2).await;
    assert!(!whole.0.is_empty());
    assert_eq!(stepped, whole);
}

const LIBRARY: &[&str] = &[
    "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, isbn TEXT, year INTEGER)",
    "CREATE TABLE highlights (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        text TEXT, pos INTEGER)",
    "CREATE INDEX highlights_book ON highlights (book_id)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        body TEXT)",
    "CREATE INDEX notes_book ON notes (book_id)",
    "CREATE TABLE w (id INTEGER PRIMARY KEY, at INTEGER)",
];

fn library_registry() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("isbn")
            .field("year")
            .identity(&["isbn"]),
        Collection::minted("highlights", "highlights", "id")
            .owner("book_id", "books")
            .field("text")
            .field("pos"),
        Collection::minted("notes", "notes", "id")
            .owner("book_id", "books")
            .field("body"),
    ])
    .unwrap()
}

/// The first install over an existing library: `FIRST_BOOKS` books, each with a highlight and a
/// note referring to it, written before the kernel was installed, in `FIRST_JOURNAL` mode, while
/// a connection here writes and another reads every 20 ms, and a process of its own writes, with
/// a busy timeout of `FIRST_BUSY_MS`. It reports how long the install held the write lock, what
/// the others waited, and, for comparison, how long a copy of every row the install adopts
/// takes, done in SQL in one statement per table: the least an install could hold the lock for
/// that made its own record of what it found.
///
/// ```text
/// FIRST_BOOKS=50000 FIRST_JOURNAL=delete cargo test --release -p fongo-sync-client \\
///     --test backlog -- --ignored --nocapture measure_first_install
/// ```
///
/// On an 8 GB laptop, with a 1 s busy timeout: 10,000 books (30,000 rows) held the lock 2.0 s
/// in either journal mode, and one statement of each other connection failed; 50,000 books held
/// it 13.3 to 14.1 s, and about 12 of 40 statements of each writer failed (in WAL mode the
/// reader never waited). A copy of every row took 0.01 s and 0.03 s.
#[tokio::test]
#[ignore]
async fn measure_first_install() {
    let books = env("FIRST_BOOKS", 10_000);
    let busy = Duration::from_millis(env("FIRST_BUSY_MS", 1_000));
    let journal = std::env::var("FIRST_JOURNAL").unwrap_or_else(|_| "wal".into());
    let dir = scratch("first-install");
    let path = dir.join("a.db");
    let mut conn = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .foreign_keys(true)
        .busy_timeout(Duration::from_secs(5))
        .connect()
        .await
        .unwrap();
    sqlx::query(&format!("PRAGMA journal_mode = {journal}"))
        .execute(&mut conn)
        .await
        .unwrap();
    for sql in LIBRARY {
        sqlx::query(sql).execute(&mut conn).await.unwrap();
    }
    let seq = format!(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {books})"
    );
    for sql in [
        format!(
            "{seq} INSERT INTO books SELECT i, 'Middlemarch ' || i, 'isbn ' || i, 1800 + i % 200 FROM n"
        ),
        format!("{seq} INSERT INTO highlights SELECT i, i, 'a passage of book ' || i, i FROM n"),
        format!("{seq} INSERT INTO notes SELECT i, i, 'a note on book ' || i FROM n"),
    ] {
        sqlx::query(&sql).execute(&mut conn).await.unwrap();
    }
    let began = Instant::now();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut conn)
        .await
        .unwrap();
    for (table, columns) in [
        ("books", "title, isbn, year"),
        ("highlights", "book_id, text, pos"),
        ("notes", "book_id, body"),
    ] {
        sqlx::query(&format!(
            "CREATE TEMP TABLE copy_{table} AS
             SELECT json_array(id) AS key, json_array({columns}) AS image FROM {table}"
        ))
        .execute(&mut conn)
        .await
        .unwrap();
    }
    sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
    let copy = began.elapsed();
    let replica = Replica::new(library_registry());
    let holds = Holds::watch(&mut conn).await;
    let (took, waits) = alongside(&path, busy, async {
        immediate(&mut conn).await;
        replica.install(&mut conn).await.unwrap();
        commit(&mut conn).await;
    })
    .await;
    let longest = holds.take().into_iter().max().unwrap_or_default();
    eprintln!(
        "RESULT {journal} {books} books, {} rows: first install {:.2} s, held {:.2} s; {waits}; a copy of every row {:.2} s",
        books * 3,
        took.as_secs_f64(),
        longest.as_secs_f64(),
        copy.as_secs_f64()
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// Writes rows between the sync's transactions, as fast as it collects them.
struct Busy {
    pace: Pace,
    next: u64,
    steps: usize,
    /// Rows the steps took, and rows waiting when each began.
    taken: usize,
    waiting: Vec<usize>,
}

impl Apply for Busy {
    async fn collect(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        rows: usize,
    ) -> Result<Collected, Error> {
        self.steps += 1;
        self.waiting
            .push(replica.uncollected_rows(conn, usize::MAX).await?);
        let collected = replica.collect_step(conn, rows).await?;
        self.taken += collected.rows;
        Ok(collected)
    }

    async fn between(&mut self, conn: &mut SqliteConnection) -> Result<(), Error> {
        let from = self.next;
        self.next += 150;
        sqlx::query(&format!(
            "WITH RECURSIVE n(i) AS (SELECT {from} UNION ALL SELECT i + 1 FROM n WHERE i < {})
             INSERT INTO settings SELECT 'key ' || i, 'v' FROM n",
            from + 149
        ))
        .execute(conn)
        .await?;
        Ok(())
    }

    fn pace(&self) -> Pace {
        self.pace
    }
}

/// An application that writes more rows in each gap than a step collects does not keep the sync
/// from its apply: the steps take no more rows than waited when the first began, and the apply
/// collects the rest.
#[tokio::test]
async fn writes_between_steps_do_not_keep_a_sync_collecting() {
    let server = Server::new();
    let made = create_library(&server, now).await.unwrap();
    let mut a = Peer::new(made).await;
    write_books(&mut a.conn, 200).await;
    let mut busy = Busy {
        pace: Pace {
            collect: 100,
            hold: Duration::ZERO,
            pause: Duration::ZERO,
            ..Pace::default()
        },
        next: 1,
        steps: 0,
        taken: 0,
        waiting: Vec::new(),
    };
    let report = sync_with(
        &mut a.conn,
        &a.replica,
        &server,
        &a.credentials,
        now,
        &mut busy,
    )
    .await
    .unwrap();
    // 400 rows waited; the steps stop once they have taken that many, though 150 more are
    // written in every gap.
    let at = format!(
        "{} steps, {} rows, waiting {:?}",
        busy.steps, busy.taken, busy.waiting
    );
    assert_eq!(busy.waiting[0], 400, "{at}");
    assert!(busy.taken >= 400 && busy.taken < 500, "{at}");
    assert!(busy.steps <= 4, "{at}");
    assert!(report.sent > 6 * 200, "{}", report.sent);
}

/// What a collect hook does wrong after its step.
#[derive(Clone, Copy, Debug)]
enum Wrong {
    Fails,
    Commits,
    RollsBack,
    Hangs,
}

struct Wayward(Wrong);

impl Apply for Wayward {
    async fn collect(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        rows: usize,
    ) -> Result<Collected, Error> {
        let collected = replica.collect_step(conn, rows).await?;
        match self.0 {
            Wrong::Fails => return Err(Error::TransactionEnded),
            Wrong::Commits => {
                sqlx::query("COMMIT").execute(&mut *conn).await?;
            }
            Wrong::RollsBack => {
                sqlx::query("ROLLBACK").execute(&mut *conn).await?;
            }
            Wrong::Hangs => std::future::pending::<()>().await,
        }
        Ok(collected)
    }

    fn pace(&self) -> Pace {
        Pace {
            collect: 50,
            ..Pace::default()
        }
    }
}

/// A collect hook that fails, ends the sync's transaction or never returns, and is dropped,
/// leaves the connection sound and loses nothing: what it collected and did not commit is still
/// to collect, and the next sync sends every row.
#[tokio::test]
async fn a_collect_hook_that_goes_wrong_loses_nothing() {
    for wrong in [Wrong::Fails, Wrong::Commits, Wrong::RollsBack, Wrong::Hangs] {
        let server = Server::new();
        let made = create_library(&server, now).await.unwrap();
        let mut a = Peer::new(made).await;
        write_books(&mut a.conn, 300).await;
        let marked = a
            .replica
            .uncollected_rows(&mut a.conn, usize::MAX)
            .await
            .unwrap();
        let waiting = a
            .replica
            .outbox(&mut a.conn, u32::MAX)
            .await
            .unwrap()
            .changes
            .len();
        let mut hook = Wayward(wrong);
        let synced = tokio::time::timeout(
            Duration::from_millis(500),
            sync_with(
                &mut a.conn,
                &a.replica,
                &server,
                &a.credentials,
                now,
                &mut hook,
            ),
        )
        .await;
        assert!(!matches!(synced, Ok(Ok(_))), "{wrong:?}: the sync went on");
        check_connection(&mut a.conn).await.unwrap();
        if !matches!(wrong, Wrong::Commits) {
            let left = a
                .replica
                .uncollected_rows(&mut a.conn, usize::MAX)
                .await
                .unwrap();
            let outbox = a
                .replica
                .outbox(&mut a.conn, u32::MAX)
                .await
                .unwrap()
                .changes
                .len();
            assert_eq!((left, outbox), (marked, waiting), "{wrong:?}");
        }
        let report = a.sync(&server).await.unwrap();
        assert!(report.sent >= 300, "{wrong:?}: sent {}", report.sent);
    }
}

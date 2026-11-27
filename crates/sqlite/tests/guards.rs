//! What every entry point does before it works: it reads whether the kernel's tables are there,
//! of which layout, and whether capture is suspended, and refuses what it cannot do soundly in
//! that state, changing nothing. And every call that writes takes SQLite's write lock before it
//! reads, so another connection's write makes it wait rather than fail partway.

mod support;

use std::ffi::{CStr, CString, c_int, c_uint, c_void};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use fongo_sync_sqlite::core::{Change, Gid, Value};
use fongo_sync_sqlite::{Collection, Error, Registry, Replica, Resume};
use libsqlite3_sys as ffi;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};
use support::world::World;

const SCHEMA: &[&str] = &[
    "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT NOT NULL, isbn TEXT UNIQUE)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        body TEXT)",
];

fn registry() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("isbn")
            .identity(&["isbn"]),
        Collection::minted("notes", "notes", "id")
            .owner("book_id", "books")
            .field("body"),
    ])
    .unwrap()
}

async fn run(conn: &mut SqliteConnection, sql: &str) {
    sqlx::query(sql).execute(conn).await.unwrap();
}

async fn database(world: &World, n: u64) -> SqliteConnection {
    let mut conn = world.connect(n).await;
    for sql in SCHEMA {
        run(&mut conn, sql).await;
    }
    conn
}

async fn titles(conn: &mut SqliteConnection) -> Vec<String> {
    sqlx::query_scalar("SELECT title FROM books ORDER BY id")
        .fetch_all(conn)
        .await
        .unwrap()
}

async fn sent(replica: &Replica, conn: &mut SqliteConnection) -> Vec<Change> {
    let outbox = replica.outbox(conn, u32::MAX).await.unwrap();
    replica.acknowledge(conn, outbox.through).await.unwrap();
    outbox.changes
}

fn sends(changes: &[Change], title: &str) -> bool {
    changes
        .iter()
        .any(|c| c.field == "title" && c.value == Value::Text(title.into()))
}

/// Every object in the schema and every row of every table, the kernel's included.
async fn dump(conn: &mut SqliteConnection) -> Vec<String> {
    let mut out: Vec<String> = support::rows(conn, "SELECT * FROM sqlite_master", &[])
        .await
        .into_iter()
        .map(|row| format!("sqlite_master {row:?}"))
        .collect();
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY 1",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    for t in tables {
        for row in support::rows(conn, &format!("SELECT * FROM \"{t}\""), &[]).await {
            out.push(format!("{t} {row:?}"));
        }
    }
    out.sort();
    out
}

/// The application writes while suspended, as a migration would, and the process ends before it
/// resumes. The next open installs: that install is refused, and the resume it asks for sends
/// the write.
#[tokio::test]
async fn writes_made_while_suspended_are_not_lost_to_an_install() {
    let world = World::new(1);
    let mut c = database(&world, 0).await;
    let replica = Replica::new(registry());
    replica.install(&mut c).await.unwrap();
    run(&mut c, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
    replica.collect(&mut c).await.unwrap();
    sent(&replica, &mut c).await;
    replica.suspend(&mut c).await.unwrap();
    run(&mut c, "UPDATE books SET title = 'Middlemarch, a study'").await;
    let before = dump(&mut c).await;
    let installed = replica.install(&mut c).await;
    assert!(matches!(installed, Err(Error::Suspended)), "{installed:?}");
    let stepped = replica.install_step(&mut c, 10).await;
    assert!(matches!(stepped, Err(Error::Suspended)), "{stepped:?}");
    assert_eq!(dump(&mut c).await, before);
    assert!(replica.suspended(&mut c).await.unwrap());
    replica.resume(&mut c, Resume::Send).await.unwrap();
    assert!(!replica.suspended(&mut c).await.unwrap());
    replica.install(&mut c).await.unwrap();
    replica.collect(&mut c).await.unwrap();
    assert!(
        sends(&sent(&replica, &mut c).await, "Middlemarch, a study"),
        "the write made while suspended is sent"
    );
}

/// An apply made while suspended would write a field the application wrote meanwhile without
/// recording that write anywhere; it is refused, and once resumed the write is sent.
#[tokio::test]
async fn an_apply_while_suspended_does_not_lose_a_write_to_the_same_field() {
    let world = World::new(2);
    let replica = Replica::new(registry());
    let mut a = database(&world, 0).await;
    let mut b = database(&world, 1).await;
    replica.install(&mut a).await.unwrap();
    replica.install(&mut b).await.unwrap();
    run(&mut a, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
    replica.collect(&mut a).await.unwrap();
    let first = sent(&replica, &mut a).await;
    replica.apply(&mut b, &first).await.unwrap();
    replica.suspend(&mut b).await.unwrap();
    run(&mut b, "UPDATE books SET title = 'quiet'").await;
    run(&mut a, "UPDATE books SET title = 'loud'").await;
    replica.collect(&mut a).await.unwrap();
    let second = sent(&replica, &mut a).await;
    let before = dump(&mut b).await;
    let applied = replica.apply(&mut b, &second).await;
    assert!(matches!(applied, Err(Error::Suspended)), "{applied:?}");
    assert_eq!(dump(&mut b).await, before);
    replica.resume(&mut b, Resume::Send).await.unwrap();
    replica.apply(&mut b, &second).await.unwrap();
    assert!(
        sends(&sent(&replica, &mut b).await, "quiet"),
        "the write made while suspended is sent: {:?}",
        titles(&mut b).await
    );
}

/// Suspending twice is suspending once; one resume ends it, and what was written meanwhile is
/// sent.
#[tokio::test]
async fn suspending_again_changes_nothing_and_one_resume_ends_it() {
    let world = World::new(3);
    let mut c = database(&world, 0).await;
    let replica = Replica::new(registry());
    replica.install(&mut c).await.unwrap();
    assert!(!replica.suspended(&mut c).await.unwrap());
    run(&mut c, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
    replica.collect(&mut c).await.unwrap();
    replica.suspend(&mut c).await.unwrap();
    run(&mut c, "UPDATE books SET title = 'Romola'").await;
    let before = dump(&mut c).await;
    replica.suspend(&mut c).await.unwrap();
    assert_eq!(dump(&mut c).await, before);
    assert!(replica.suspended(&mut c).await.unwrap());
    replica.resume(&mut c, Resume::Send).await.unwrap();
    assert!(!replica.suspended(&mut c).await.unwrap());
    replica.collect(&mut c).await.unwrap();
    assert!(sends(&sent(&replica, &mut c).await, "Romola"));
    run(&mut c, "UPDATE books SET title = 'Felix Holt'").await;
    replica.collect(&mut c).await.unwrap();
    assert!(sends(&sent(&replica, &mut c).await, "Felix Holt"));
}

/// Uninstalling a suspended replica leaves nothing of the suspension: installing again is a
/// fresh install that captures writes.
#[tokio::test]
async fn uninstalling_ends_a_suspension() {
    let world = World::new(4);
    let mut c = database(&world, 0).await;
    let replica = Replica::new(registry());
    replica.install(&mut c).await.unwrap();
    replica.suspend(&mut c).await.unwrap();
    replica.uninstall(&mut c).await.unwrap();
    assert!(!fongo_sync_sqlite::installed(&mut c).await.unwrap());
    replica.install(&mut c).await.unwrap();
    assert!(!replica.suspended(&mut c).await.unwrap());
    run(&mut c, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
    replica.collect(&mut c).await.unwrap();
    assert!(sends(&sent(&replica, &mut c).await, "Middlemarch"));
}

/// A resume when capture is not suspended is an install: rows written since the last collect
/// stay edits to send, whichever way it is asked to resume.
#[tokio::test]
async fn a_resume_when_not_suspended_keeps_uncollected_edits() {
    for how in [Resume::Send, Resume::Baseline] {
        let world = World::new(5);
        let mut c = database(&world, 0).await;
        let replica = Replica::new(registry());
        replica.install(&mut c).await.unwrap();
        run(&mut c, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
        replica.collect(&mut c).await.unwrap();
        sent(&replica, &mut c).await;
        run(&mut c, "UPDATE books SET title = 'Romola'").await;
        replica.resume(&mut c, how).await.unwrap();
        assert!(
            replica.inspect(&mut c).await.unwrap().pinned.is_empty(),
            "{how:?}"
        );
        replica.collect(&mut c).await.unwrap();
        assert!(
            sends(&sent(&replica, &mut c).await, "Romola"),
            "{how:?}: an edit collected by no one is sent"
        );
    }
}

/// A resume with no kernel tables is an install: the rows found are adopted as found, at the
/// oldest clocks, as an install adopts them.
#[tokio::test]
async fn a_resume_with_nothing_installed_is_an_install() {
    let installed = async |resume: bool| {
        let world = World::new(6);
        let mut c = database(&world, 0).await;
        run(&mut c, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
        let replica = Replica::new(registry());
        if resume {
            replica.suspend(&mut c).await.unwrap();
            replica.resume(&mut c, Resume::Send).await.unwrap();
        } else {
            replica.install(&mut c).await.unwrap();
        }
        replica.collect(&mut c).await.unwrap();
        (dump(&mut c).await, sent(&replica, &mut c).await)
    };
    assert_eq!(installed(true).await, installed(false).await);
}

/// An installed replica with a library collected and a row left to collect.
async fn installed(world: &World) -> (SqliteConnection, Replica) {
    let mut c = database(world, 0).await;
    let replica = Replica::new(registry());
    replica.install(&mut c).await.unwrap();
    run(
        &mut c,
        "INSERT INTO books (title, isbn) VALUES ('Middlemarch', 'x')",
    )
    .await;
    run(
        &mut c,
        "INSERT INTO notes (book_id, body) VALUES (1, 'slow')",
    )
    .await;
    replica.collect(&mut c).await.unwrap();
    run(&mut c, "INSERT INTO books (title) VALUES ('Romola')").await;
    (c, replica)
}

/// Every public entry point that reads or writes the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Call {
    Installed,
    Install,
    InstallStep,
    Suspend,
    Suspended,
    Resume,
    Uninstall,
    Collect,
    CollectStep,
    UncollectedRows,
    Outbox,
    Acknowledge,
    Apply,
    ReleaseBases,
    Merged,
    Made,
    WrittenHere,
    GidForKey,
    KeyForGid,
    GroupOf,
    GidOf,
    Inspect,
}

const CALLS: &[Call] = &[
    Call::Installed,
    Call::Install,
    Call::InstallStep,
    Call::Suspend,
    Call::Suspended,
    Call::Resume,
    Call::Uninstall,
    Call::Collect,
    Call::CollectStep,
    Call::UncollectedRows,
    Call::Outbox,
    Call::Acknowledge,
    Call::Apply,
    Call::ReleaseBases,
    Call::Merged,
    Call::Made,
    Call::WrittenHere,
    Call::GidForKey,
    Call::KeyForGid,
    Call::GroupOf,
    Call::GidOf,
    Call::Inspect,
];

/// What a call returned: `Ok`, with the answer where it is a count or a flag, or the error.
async fn call(call: Call, replica: &Replica, conn: &mut SqliteConnection) -> String {
    let gid = Gid([7; 16]);
    let shown = |r: Result<String, Error>| match r {
        Ok(v) if v.is_empty() => "Ok".to_string(),
        Ok(v) => format!("Ok({v})"),
        Err(e) => format!("Err({e:?})"),
    };
    let none = |_| String::new();
    shown(match call {
        Call::Installed => fongo_sync_sqlite::installed(conn)
            .await
            .map(|v| v.to_string()),
        Call::Install => replica.install(conn).await.map(none),
        Call::InstallStep => replica.install_step(conn, 1).await.map(|_| String::new()),
        Call::Suspend => replica.suspend(conn).await.map(none),
        Call::Suspended => replica.suspended(conn).await.map(|v| v.to_string()),
        Call::Resume => replica.resume(conn, Resume::Send).await.map(none),
        Call::Uninstall => replica.uninstall(conn).await.map(none),
        Call::Collect => replica.collect(conn).await.map(|_| String::new()),
        Call::CollectStep => replica.collect_step(conn, 1).await.map(|_| String::new()),
        Call::UncollectedRows => replica
            .uncollected_rows(conn, 10)
            .await
            .map(|v| v.to_string()),
        Call::Outbox => replica.outbox(conn, 10).await.map(|_| String::new()),
        Call::Acknowledge => replica.acknowledge(conn, 1).await.map(none),
        Call::Apply => replica.apply(conn, &[]).await.map(|_| String::new()),
        Call::ReleaseBases => replica.release_bases(conn, 1).await.map(none),
        Call::Merged => replica
            .merged(conn, "books", gid)
            .await
            .map(|_| String::new()),
        Call::Made => replica
            .made(conn, "books", gid)
            .await
            .map(|_| String::new()),
        Call::WrittenHere => replica
            .written_here(conn, "books", gid, &["title"])
            .await
            .map(|_| String::new()),
        Call::GidForKey => replica
            .gid_for_key(conn, "books", "[1]")
            .await
            .map(|_| String::new()),
        Call::KeyForGid => replica
            .key_for_gid(conn, "books", gid)
            .await
            .map(|_| String::new()),
        Call::GroupOf => replica
            .group_of(conn, "books", gid)
            .await
            .map(|_| String::new()),
        Call::GidOf => replica
            .gid_of(conn, "books", 1)
            .await
            .map(|_| String::new()),
        Call::Inspect => replica.inspect(conn).await.map(|_| String::new()),
    })
}

/// The state of the database a call meets.
#[derive(Debug, Clone, Copy)]
enum Laid {
    Installed,
    Suspended,
    /// Kernel tables of a layout one past this build's.
    Newer,
    /// Kernel tables of layout 3, laid down as the upgrade tests lay them.
    Older,
    /// Kernel tables of layout 2, which this build does not upgrade.
    Unknown,
    /// Kernel tables that record no layout.
    Unrecorded,
    /// No kernel tables.
    Absent,
    /// No kernel tables, but the client's own.
    ClientOnly,
    /// Kernel tables without `fongo_meta`.
    NoMeta,
    /// Kernel tables without `fongo_dirty`.
    NoDirty,
    /// Kernel tables without `fongo_ids`.
    NoIds,
    /// Every capture trigger dropped by hand, and no suspension recorded.
    TriggersLost,
    /// A suspension recorded with a value no build writes.
    OddFlag,
}

/// Drops the kernel's triggers whose names are like `like`.
async fn drop_triggers(conn: &mut SqliteConnection, like: &str) {
    let names: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE ?1",
    )
    .bind(like)
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    assert!(!names.is_empty(), "no trigger like {like}");
    for name in names {
        run(conn, &format!("DROP TRIGGER \"{name}\"")).await;
    }
}

async fn laid(state: Laid) -> (SqliteConnection, Replica) {
    let world = World::new(7);
    if let Laid::Absent | Laid::ClientOnly = state {
        let mut c = database(&world, 0).await;
        if let Laid::ClientOnly = state {
            run(
                &mut c,
                "CREATE TABLE fongo_client_state (library BLOB PRIMARY KEY)",
            )
            .await;
        }
        return (c, Replica::new(registry()));
    }
    let (mut c, replica) = installed(&world).await;
    let sql: &[&str] = match state {
        Laid::Installed | Laid::Absent | Laid::ClientOnly => &[],
        Laid::NoMeta => &["DROP TABLE fongo_meta"],
        Laid::NoDirty => &["DROP TABLE fongo_dirty"],
        Laid::NoIds => &["DROP TABLE fongo_ids"],
        Laid::TriggersLost => {
            drop_triggers(&mut c, "fongo%").await;
            &["UPDATE books SET title = 'Romola!' WHERE id = 2"]
        }
        Laid::OddFlag => &["INSERT INTO fongo_meta (key, value) VALUES ('suspended', 'x')"],
        Laid::Suspended => {
            replica.suspend(&mut c).await.unwrap();
            &["UPDATE books SET title = 'Romola!' WHERE id = 2"]
        }
        Laid::Newer => &["UPDATE fongo_meta SET value = 5 WHERE key = 'layout'"],
        Laid::Older => &[
            "DROP TABLE fongo_found",
            "UPDATE fongo_meta SET value = 3 WHERE key = 'layout'",
        ],
        Laid::Unknown => &["UPDATE fongo_meta SET value = 2 WHERE key = 'layout'"],
        Laid::Unrecorded => &["DELETE FROM fongo_meta WHERE key = 'layout'"],
    };
    for s in sql {
        run(&mut c, s).await;
    }
    (c, replica)
}

/// What `call` returns in `state`, and whether it must leave the database as it was.
fn expected(state: Laid, call: Call) -> (String, bool) {
    use Call::*;
    let refused = |e: &str| (format!("Err({e})"), true);
    let ok = |unchanged: bool| ("Ok".to_string(), unchanged);
    let read = matches!(
        call,
        Outbox | Merged | Made | WrittenHere | GidForKey | KeyForGid | GroupOf | GidOf | Inspect
    );
    match state {
        Laid::Installed => match call {
            Installed | Suspended => (format!("Ok({})", call == Installed), true),
            UncollectedRows => ("Ok(1)".into(), true),
            _ => ok(read),
        },
        Laid::Suspended | Laid::TriggersLost | Laid::OddFlag => match call {
            Install | InstallStep | Collect | CollectStep | Apply | UncollectedRows => {
                refused("Suspended")
            }
            Installed | Suspended => ("Ok(true)".into(), true),
            // Without a record, suspending records one.
            Suspend => ok(!matches!(state, Laid::TriggersLost)),
            _ => ok(read),
        },
        Laid::NoMeta | Laid::NoDirty | Laid::NoIds => match call {
            Uninstall => ok(false),
            _ => ("Err(Corrupt(".into(), true),
        },
        Laid::Newer => refused("NewerLayout { found: 5, known: 4 }"),
        Laid::Older => match call {
            Install | InstallStep | Resume | Uninstall => ok(false),
            Suspended => ("Ok(false)".into(), true),
            _ => refused("OlderLayout { found: 3, known: 4 }"),
        },
        Laid::Unknown => match call {
            Uninstall => ok(false),
            _ => refused("UnknownLayout { found: Some(2), known: 4 }"),
        },
        Laid::Unrecorded => match call {
            Uninstall => ok(false),
            _ => refused("UnknownLayout { found: None, known: 4 }"),
        },
        Laid::Absent | Laid::ClientOnly => match call {
            Install | InstallStep | Resume => ok(false),
            Suspend => ok(true),
            // It removes the client's tables too.
            Uninstall => ok(matches!(state, Laid::Absent)),
            Installed => ("Ok(false)".into(), true),
            UncollectedRows => ("Ok(0)".into(), true),
            _ => refused("NotInstalled"),
        },
    }
}

/// Every entry point in every state: what it returns, and that a refusal changes nothing.
#[tokio::test]
async fn every_call_in_every_state() {
    let mut failures = Vec::new();
    for state in [
        Laid::Installed,
        Laid::Suspended,
        Laid::Newer,
        Laid::Older,
        Laid::Unknown,
        Laid::Unrecorded,
        Laid::Absent,
        Laid::ClientOnly,
        Laid::NoMeta,
        Laid::NoDirty,
        Laid::NoIds,
        Laid::TriggersLost,
        Laid::OddFlag,
    ] {
        for &c in CALLS {
            let (mut conn, replica) = laid(state).await;
            let before = dump(&mut conn).await;
            let got = call(c, &replica, &mut conn).await;
            let (want, unchanged) = expected(state, c);
            let matched = match want.strip_suffix('(') {
                Some(prefix) => got.starts_with(prefix),
                None => got == want,
            };
            if !matched {
                failures.push(format!("{state:?} {c:?}: {got}, not {want}"));
            }
            if unchanged && dump(&mut conn).await != before {
                failures.push(format!("{state:?} {c:?}: changed the database"));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// Layout 3 is upgraded by an install, and only then do the other calls work on it.
#[tokio::test]
async fn an_older_layout_works_once_installed() {
    for &c in CALLS {
        let (mut conn, replica) = laid(Laid::Older).await;
        replica.install(&mut conn).await.unwrap();
        let got = call(c, &replica, &mut conn).await;
        assert!(got.starts_with("Ok"), "{c:?}: {got}");
    }
}

/// A second connection that, when the watched one starts its first write, writes in turn: in WAL
/// mode it commits; in rollback-journal mode, where it cannot commit while the first reads, it
/// begins and holds the write lock, as a writer about to commit would.
struct Intruder {
    other: *mut ffi::sqlite3,
    sql: CString,
    fired: AtomicBool,
    outcome: Mutex<Option<c_int>>,
}

// SAFETY: the other connection is used by one thread at a time: the trace callback, then the
// test once the call it watched has returned.
unsafe impl Send for Intruder {}
unsafe impl Sync for Intruder {}

unsafe extern "C" fn intrude(
    _: c_uint,
    ctx: *mut c_void,
    stmt: *mut c_void,
    _: *mut c_void,
) -> c_int {
    // SAFETY: `ctx` is the intruder leaked for the life of the test, and `stmt` the statement
    // starting, whose text lives as long as it does.
    let state = unsafe { &*ctx.cast::<Intruder>() };
    let text = unsafe { ffi::sqlite3_sql(stmt.cast()) };
    if text.is_null() {
        return 0;
    }
    let text = unsafe { CStr::from_ptr(text) }
        .to_string_lossy()
        .to_ascii_uppercase();
    let writes = ["INSERT", "UPDATE", "DELETE", "DROP"]
        .iter()
        .any(|w| text.trim_start().starts_with(w));
    if writes && !state.fired.swap(true, Ordering::SeqCst) {
        // SAFETY: the other connection is open, and nothing else uses it meanwhile.
        let rc = unsafe {
            ffi::sqlite3_exec(
                state.other,
                state.sql.as_ptr(),
                None,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        *state.outcome.lock().unwrap() = Some(rc);
    }
    0
}

async fn trace(conn: &mut SqliteConnection, intruder: Option<&'static Intruder>) {
    let mut handle = support::lock(conn).await;
    let ctx = intruder.map_or(std::ptr::null_mut(), |i| {
        std::ptr::from_ref::<Intruder>(i)
            .cast_mut()
            .cast::<c_void>()
    });
    // SAFETY: the handle is locked; the intruder is leaked and outlives the connection.
    let status = unsafe {
        ffi::sqlite3_trace_v2(
            handle.as_raw_handle().as_ptr(),
            if intruder.is_some() {
                ffi::SQLITE_TRACE_STMT as c_uint
            } else {
                0
            },
            intruder.map(|_| intrude as _),
            ctx,
        )
    };
    assert_eq!(status, ffi::SQLITE_OK);
}

/// Every call that writes.
#[derive(Debug, Clone, Copy)]
enum Writer {
    Collect,
    CollectStep,
    Apply,
    Install,
    InstallStep,
    Resume,
    Suspend,
    Uninstall,
    Acknowledge,
    ReleaseBases,
}

const WRITERS: &[Writer] = &[
    Writer::Collect,
    Writer::CollectStep,
    Writer::Apply,
    Writer::Install,
    Writer::InstallStep,
    Writer::Resume,
    Writer::Suspend,
    Writer::Uninstall,
    Writer::Acknowledge,
    Writer::ReleaseBases,
];

async fn write(w: Writer, replica: &Replica, conn: &mut SqliteConnection) -> Result<(), Error> {
    match w {
        Writer::Collect => replica.collect(conn).await.map(drop),
        Writer::CollectStep => replica.collect_step(conn, 100).await.map(drop),
        Writer::Apply => replica.apply(conn, &[]).await.map(drop),
        Writer::Install => replica.install(conn).await,
        Writer::InstallStep => replica.install_step(conn, 100).await.map(drop),
        Writer::Resume => replica.resume(conn, Resume::Send).await,
        Writer::Suspend => replica.suspend(conn).await,
        Writer::Uninstall => replica.uninstall(conn).await,
        Writer::Acknowledge => replica.acknowledge(conn, i64::MAX).await,
        Writer::ReleaseBases => replica.release_bases(conn, i64::MAX).await,
    }
}

/// Where a writing call runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Around {
    /// With no transaction open: the call owns its transaction.
    Nothing,
    /// In the caller's transaction, begun with `BEGIN IMMEDIATE`.
    Immediate,
    /// In the caller's transaction, begun deferred and holding a read.
    Deferred,
}

/// Each writing call, while another connection writes between its first read and its first
/// write: owning its transaction, or in a caller's begun immediately, it has the lock already and
/// keeps the other out; in a caller's deferred transaction it cannot, and fails as SQLite fails a
/// read that cannot become a write.
#[tokio::test]
async fn a_writing_call_takes_the_lock_before_it_reads() {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("guards-busy-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut failures = Vec::new();
    for journal in ["delete", "wal"] {
        for around in [Around::Nothing, Around::Immediate, Around::Deferred] {
            for &w in WRITERS {
                let path = dir.join(format!("{journal}-{around:?}-{w:?}.db"));
                let _ = std::fs::remove_file(&path);
                let options = SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true)
                    .foreign_keys(true)
                    .busy_timeout(std::time::Duration::from_secs(5));
                let mut a = options.connect().await.unwrap();
                run(&mut a, &format!("PRAGMA journal_mode = {journal}")).await;
                for sql in SCHEMA {
                    run(&mut a, sql).await;
                }
                run(&mut a, "CREATE TABLE other (x)").await;
                let replica = Replica::new(registry());
                replica.install(&mut a).await.unwrap();
                run(&mut a, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
                replica.collect(&mut a).await.unwrap();
                run(&mut a, "INSERT INTO books (title) VALUES ('Romola')").await;
                if matches!(w, Writer::Resume) {
                    replica.suspend(&mut a).await.unwrap();
                }
                let mut other = std::ptr::null_mut();
                let name = CString::new(path.to_str().unwrap()).unwrap();
                // SAFETY: `name` is a path, and `other` receives the handle.
                let rc = unsafe {
                    ffi::sqlite3_open_v2(
                        name.as_ptr(),
                        &mut other,
                        ffi::SQLITE_OPEN_READWRITE,
                        std::ptr::null(),
                    )
                };
                assert_eq!(rc, ffi::SQLITE_OK);
                let sql = if journal == "wal" {
                    "INSERT INTO other VALUES (1)"
                } else {
                    "BEGIN IMMEDIATE; INSERT INTO other VALUES (1)"
                };
                let intruder: &'static Intruder = Box::leak(Box::new(Intruder {
                    other,
                    sql: CString::new(sql).unwrap(),
                    fired: AtomicBool::new(false),
                    outcome: Mutex::new(None),
                }));
                match around {
                    Around::Nothing => {}
                    Around::Immediate => run(&mut a, "BEGIN IMMEDIATE").await,
                    Around::Deferred => {
                        run(&mut a, "BEGIN").await;
                        run(&mut a, "SELECT count(*) FROM books").await;
                    }
                }
                trace(&mut a, Some(intruder)).await;
                let result = write(w, &replica, &mut a).await;
                trace(&mut a, None).await;
                match around {
                    Around::Nothing => {}
                    Around::Immediate => run(&mut a, "COMMIT").await,
                    Around::Deferred => run(&mut a, "ROLLBACK").await,
                }
                // SAFETY: the call has returned, so nothing else uses the other connection.
                unsafe {
                    if ffi::sqlite3_get_autocommit(other) == 0 {
                        let commit = CString::new("COMMIT").unwrap();
                        let rc = ffi::sqlite3_exec(
                            other,
                            commit.as_ptr(),
                            None,
                            std::ptr::null_mut(),
                            std::ptr::null_mut(),
                        );
                        assert_eq!(rc, ffi::SQLITE_OK);
                    }
                    ffi::sqlite3_close(other);
                }
                let at = format!("{journal} {around:?} {w:?}");
                let kept_out = *intruder.outcome.lock().unwrap();
                if !intruder.fired.load(Ordering::SeqCst) {
                    failures.push(format!("{at}: no write to intrude on"));
                }
                match (around, result) {
                    (Around::Deferred, Err(Error::Sqlite { code, .. }))
                        if code & 0xff == ffi::SQLITE_BUSY => {}
                    (Around::Deferred, other) => {
                        failures.push(format!("{at}: {other:?}, not a refusal of the lock"));
                    }
                    (_, Err(e)) => failures.push(format!("{at}: {e}")),
                    (_, Ok(())) if kept_out != Some(ffi::SQLITE_BUSY) => {
                        failures.push(format!("{at}: the other connection wrote: {kept_out:?}"));
                    }
                    (_, Ok(())) => {}
                }
            }
        }
    }
    let _ = std::fs::remove_dir_all(dir);
    assert!(failures.is_empty(), "{failures:#?}");
}

fn pinned(inspection: &fongo_sync_sqlite::Inspection, field: &str) -> bool {
    inspection.pinned.iter().any(|(_, _, f)| f == field)
}

/// Capture dropped without a suspend being recorded, as a build that recorded none left it, at
/// this layout or at layout 3: an install is refused, and a resume compares the tables as after
/// a suspend, sending or keeping what was written meanwhile as asked.
#[tokio::test]
async fn capture_lost_without_a_record_is_resumed_as_a_suspension() {
    for layout in [4, 3] {
        for how in [Resume::Send, Resume::Baseline] {
            let world = World::new(10);
            let mut c = database(&world, 0).await;
            let replica = Replica::new(registry());
            replica.install(&mut c).await.unwrap();
            run(&mut c, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
            replica.collect(&mut c).await.unwrap();
            sent(&replica, &mut c).await;
            drop_triggers(&mut c, "fongo%").await;
            run(&mut c, "UPDATE books SET title = 'Romola'").await;
            if layout == 3 {
                run(&mut c, "DROP TABLE fongo_found").await;
                run(
                    &mut c,
                    "UPDATE fongo_meta SET value = 3 WHERE key = 'layout'",
                )
                .await;
            }
            let at = format!("layout {layout}, {how:?}");
            let before = dump(&mut c).await;
            let installed = replica.install(&mut c).await;
            assert!(
                matches!(installed, Err(Error::Suspended)),
                "{at}: {installed:?}"
            );
            assert_eq!(dump(&mut c).await, before, "{at}");
            replica.resume(&mut c, how).await.unwrap();
            assert!(!replica.suspended(&mut c).await.unwrap(), "{at}");
            replica.collect(&mut c).await.unwrap();
            let out = sent(&replica, &mut c).await;
            let inspection = replica.inspect(&mut c).await.unwrap();
            match how {
                Resume::Send => assert!(sends(&out, "Romola"), "{at}: {out:?}"),
                Resume::Baseline => {
                    assert!(!sends(&out, "Romola"), "{at}: {out:?}");
                    assert!(pinned(&inspection, "title"), "{at}: {inspection:?}");
                }
            }
            run(&mut c, "UPDATE books SET title = 'Felix Holt'").await;
            replica.collect(&mut c).await.unwrap();
            assert!(sends(&sent(&replica, &mut c).await, "Felix Holt"), "{at}");
        }
    }
}

/// One trigger of one collection missing suspends capture; the resume compares that
/// collection's table alone, so an edit to another collection, captured and not yet collected,
/// stays an edit to send even when the resume keeps what it finds as a baseline.
#[tokio::test]
async fn capture_lost_for_one_collection_is_resumed_for_it_alone() {
    let world = World::new(11);
    let mut c = database(&world, 0).await;
    let replica = Replica::new(registry());
    replica.install(&mut c).await.unwrap();
    run(&mut c, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
    run(
        &mut c,
        "INSERT INTO notes (book_id, body) VALUES (1, 'slow')",
    )
    .await;
    replica.collect(&mut c).await.unwrap();
    sent(&replica, &mut c).await;
    run(&mut c, "UPDATE notes SET body = 'quick'").await;
    drop_triggers(&mut c, "fongo_books_au").await;
    run(&mut c, "UPDATE books SET title = 'Romola'").await;
    assert!(replica.suspended(&mut c).await.unwrap());
    for refused in [
        replica.install(&mut c).await.map(drop),
        replica.collect(&mut c).await.map(drop),
        replica.apply(&mut c, &[]).await.map(drop),
    ] {
        assert!(matches!(refused, Err(Error::Suspended)), "{refused:?}");
    }
    replica.resume(&mut c, Resume::Baseline).await.unwrap();
    replica.collect(&mut c).await.unwrap();
    let out = sent(&replica, &mut c).await;
    let inspection = replica.inspect(&mut c).await.unwrap();
    assert!(!sends(&out, "Romola"), "{out:?}");
    assert!(pinned(&inspection, "title"), "{inspection:?}");
    assert!(!pinned(&inspection, "body"), "{inspection:?}");
    assert!(
        out.iter()
            .any(|ch| ch.field == "body" && ch.value == Value::Text("quick".into())),
        "{out:?}"
    );
}

/// An application that rebuilds a synced table without suspending, as SQLite's way of changing
/// a table does, drops its triggers with it: an install is refused, and a resume sends what the
/// rebuild changed.
#[tokio::test]
async fn a_table_rebuilt_without_a_suspend_is_resumed() {
    let world = World::new(12);
    let mut c = database(&world, 0).await;
    let replica = Replica::new(registry());
    replica.install(&mut c).await.unwrap();
    run(&mut c, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
    replica.collect(&mut c).await.unwrap();
    sent(&replica, &mut c).await;
    for sql in [
        "PRAGMA foreign_keys = OFF",
        "CREATE TABLE books_new (id INTEGER PRIMARY KEY, title TEXT NOT NULL, isbn TEXT UNIQUE, year INTEGER)",
        "INSERT INTO books_new (id, title, isbn) SELECT id, upper(title), isbn FROM books",
        "DROP TABLE books",
        "ALTER TABLE books_new RENAME TO books",
        "PRAGMA foreign_keys = ON",
    ] {
        run(&mut c, sql).await;
    }
    let installed = replica.install(&mut c).await;
    assert!(matches!(installed, Err(Error::Suspended)), "{installed:?}");
    replica.resume(&mut c, Resume::Send).await.unwrap();
    replica.collect(&mut c).await.unwrap();
    assert!(sends(&sent(&replica, &mut c).await, "MIDDLEMARCH"));
}

/// Capture is intact where it was never laid: a first install in steps part of the way through,
/// and a collection the registry gains or loses.
#[tokio::test]
async fn capture_never_laid_is_not_taken_for_lost() {
    let world = World::new(13);
    let mut c = database(&world, 0).await;
    run(
        &mut c,
        "INSERT INTO books (title) VALUES ('Middlemarch'), ('Romola')",
    )
    .await;
    run(
        &mut c,
        "INSERT INTO notes (book_id, body) VALUES (1, 'slow'), (2, 'quick')",
    )
    .await;
    let books = Replica::new(
        Registry::new(vec![
            Collection::minted("books", "books", "id")
                .field("title")
                .field("isbn")
                .identity(&["isbn"]),
        ])
        .unwrap(),
    );
    assert!(books.install_step(&mut c, 1).await.unwrap().more);
    assert!(!books.suspended(&mut c).await.unwrap());
    while books.collect_step(&mut c, 1).await.unwrap().more {}
    books.install(&mut c).await.unwrap();
    let both = Replica::new(registry());
    assert!(!both.suspended(&mut c).await.unwrap());
    both.install(&mut c).await.unwrap();
    both.collect(&mut c).await.unwrap();
    books.install(&mut c).await.unwrap();
    assert!(!books.suspended(&mut c).await.unwrap());
    books.collect(&mut c).await.unwrap();
}

/// How many changes sent set `field` to `value`.
fn sent_times(changes: &[Change], field: &str, value: &str) -> usize {
    changes
        .iter()
        .filter(|c| c.field == field && c.value == Value::Text(value.into()))
        .count()
}

/// A migration that renames a synced table away, keeps it, and fills a new table of the name
/// takes the capture triggers along to the old table: capture of the collection is lost, so an
/// install is refused, and a resume lays the triggers on the new table, sends or keeps what the
/// migration changed as asked, once, and captures nothing written to the old table after.
#[tokio::test]
async fn a_table_renamed_away_and_kept_is_resumed() {
    for how in [Resume::Send, Resume::Baseline] {
        let world = World::new(14);
        let mut c = database(&world, 0).await;
        let replica = Replica::new(registry());
        replica.install(&mut c).await.unwrap();
        run(&mut c, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
        replica.collect(&mut c).await.unwrap();
        sent(&replica, &mut c).await;
        for sql in [
            "PRAGMA foreign_keys = OFF",
            "ALTER TABLE books RENAME TO books_old",
            "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT NOT NULL, isbn TEXT UNIQUE, year INTEGER)",
            "INSERT INTO books (id, title, isbn) SELECT id, upper(title), isbn FROM books_old",
            "PRAGMA foreign_keys = ON",
        ] {
            run(&mut c, sql).await;
        }
        assert!(replica.suspended(&mut c).await.unwrap(), "{how:?}");
        let installed = replica.install(&mut c).await;
        assert!(
            matches!(installed, Err(Error::Suspended)),
            "{how:?}: {installed:?}"
        );
        replica.resume(&mut c, how).await.unwrap();
        replica.collect(&mut c).await.unwrap();
        let out = sent(&replica, &mut c).await;
        let inspection = replica.inspect(&mut c).await.unwrap();
        match how {
            Resume::Send => assert_eq!(sent_times(&out, "title", "MIDDLEMARCH"), 1, "{out:?}"),
            Resume::Baseline => {
                assert_eq!(sent_times(&out, "title", "MIDDLEMARCH"), 0, "{out:?}");
                assert!(pinned(&inspection, "title"), "{inspection:?}");
            }
        }
        run(&mut c, "UPDATE books_old SET title = 'stale'").await;
        replica.collect(&mut c).await.unwrap();
        assert!(sent(&replica, &mut c).await.is_empty(), "{how:?}");
        assert_eq!(replica.uncollected_rows(&mut c, 10).await.unwrap(), 0);
        run(&mut c, "UPDATE books SET title = 'Romola'").await;
        replica.collect(&mut c).await.unwrap();
        assert_eq!(
            sent_times(&sent(&replica, &mut c).await, "title", "Romola"),
            1
        );
    }
}

/// An edit the triggers captured and no collect took, then capture suspended or one trigger
/// lost: a resume that keeps what changed meanwhile as a baseline still sends the captured edit,
/// once, and pins nothing of it; a field changed only while capture was off is pinned.
#[tokio::test]
async fn a_captured_edit_is_sent_on_a_baseline_resume() {
    for suspend in [true, false] {
        let world = World::new(15);
        let mut c = database(&world, 0).await;
        let replica = Replica::new(registry());
        replica.install(&mut c).await.unwrap();
        run(
            &mut c,
            "INSERT INTO books (title) VALUES ('Middlemarch'), ('Felix Holt')",
        )
        .await;
        replica.collect(&mut c).await.unwrap();
        sent(&replica, &mut c).await;
        run(&mut c, "UPDATE books SET title = 'Romola' WHERE id = 1").await;
        if suspend {
            replica.suspend(&mut c).await.unwrap();
        } else {
            drop_triggers(&mut c, "fongo_books_au").await;
        }
        run(&mut c, "UPDATE books SET title = 'quiet' WHERE id = 2").await;
        replica.resume(&mut c, Resume::Baseline).await.unwrap();
        replica.collect(&mut c).await.unwrap();
        let out = sent(&replica, &mut c).await;
        let inspection = replica.inspect(&mut c).await.unwrap();
        let at = if suspend {
            "suspended"
        } else {
            "a trigger lost"
        };
        assert_eq!(sent_times(&out, "title", "Romola"), 1, "{at}: {out:?}");
        assert_eq!(sent_times(&out, "title", "quiet"), 0, "{at}: {out:?}");
        assert_eq!(
            inspection.pinned,
            [("books".to_string(), "[2]".to_string(), "title".to_string())],
            "{at}"
        );
    }
}

/// A captured edit to one field and a change to another made while suspended: a baseline resume
/// sends the row's whole difference, the safe side, and pins nothing of it.
#[tokio::test]
async fn a_captured_row_changed_again_while_suspended_is_sent_whole() {
    let world = World::new(16);
    let mut c = database(&world, 0).await;
    let replica = Replica::new(registry());
    replica.install(&mut c).await.unwrap();
    run(
        &mut c,
        "INSERT INTO books (title, isbn) VALUES ('Middlemarch', 'x')",
    )
    .await;
    replica.collect(&mut c).await.unwrap();
    sent(&replica, &mut c).await;
    run(&mut c, "UPDATE books SET title = 'Romola'").await;
    replica.suspend(&mut c).await.unwrap();
    run(&mut c, "UPDATE books SET isbn = 'y'").await;
    replica.resume(&mut c, Resume::Baseline).await.unwrap();
    replica.collect(&mut c).await.unwrap();
    let out = sent(&replica, &mut c).await;
    assert_eq!(sent_times(&out, "title", "Romola"), 1, "{out:?}");
    assert_eq!(sent_times(&out, "isbn", "y"), 1, "{out:?}");
    assert!(replica.inspect(&mut c).await.unwrap().pinned.is_empty());
}

/// On layout 3, which recorded no suspension, `suspended` answers from the triggers, changing
/// nothing.
#[tokio::test]
async fn suspended_answers_on_an_older_layout() {
    for lose in [false, true] {
        let (mut c, replica) = laid(Laid::Older).await;
        if lose {
            drop_triggers(&mut c, "fongo_notes%").await;
        }
        let before = dump(&mut c).await;
        assert_eq!(replica.suspended(&mut c).await.unwrap(), lose);
        assert_eq!(dump(&mut c).await, before);
    }
}

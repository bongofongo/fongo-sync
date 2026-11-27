#[allow(dead_code)]
mod support;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use fongo_sync_client::protocol::{LibraryKeys, SigningKey};
use fongo_sync_client::protocol::{generate_signing_key, open, random_nonce, seal};
use fongo_sync_client::sqlite::core::Change;
use fongo_sync_client::sqlite::{Applied, Collection, Registry, Replica};
use fongo_sync_client::{
    Apply, Credentials, Error, Method, Pace, Request, Response, SyncReport, Transport,
    TransportError, create_library, join_library, sync, sync_with,
};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{ConnectOptions, Connection, SqliteConnection};
use support::{Library, Peer, Server, assert_converged, assert_no_change_sent_twice, now, scratch};

async fn credentials(server: &Server) -> [Credentials; 2] {
    let made = create_library(server, now).await.unwrap();
    let joined = join_library(server, &made.recovery_code(), now)
        .await
        .unwrap();
    [made, joined]
}

/// A replica over a file of its own, so a second connection can read what reached the file.
async fn file_peer(credentials: Credentials) -> Peer {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = DIR.get_or_init(|| scratch("apply"));
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    Peer::new_at(&dir.join(format!("{n}.db")), credentials).await
}

/// The connection is out of every transaction as sqlx and SQLite both count them, and a second
/// connection to the file reads every table as this one does.
async fn assert_durable(peer: &mut Peer, at: &str) {
    assert_durable_on(&mut peer.conn, at).await;
}

async fn assert_durable_on(conn: &mut SqliteConnection, at: &str) {
    assert_clean_on(conn, at).await;
    let file: String =
        sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name = 'main'")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
    assert!(!file.is_empty(), "{at}: the database is not a file");
    let mut other = SqliteConnectOptions::new()
        .filename(&file)
        .connect()
        .await
        .unwrap();
    assert_eq!(
        everything(&mut other).await,
        everything(&mut *conn).await,
        "{at}"
    );
}

/// sqlx and SQLite agree that the connection is in no transaction.
async fn assert_clean_on(conn: &mut SqliteConnection, at: &str) {
    drop(conn.begin().await.unwrap());
    let begun = sqlx::query("BEGIN").execute(&mut *conn).await;
    assert!(begun.is_ok(), "{at}: still in a transaction: {begun:?}");
    sqlx::query("ROLLBACK").execute(&mut *conn).await.unwrap();
}

/// An envelope from `signer` setting `name`, sealed outside any peer the test syncs.
async fn sealed_setting(keys: &LibraryKeys, signer: &SigningKey, name: &str) -> Vec<u8> {
    let mut sealer = Peer::new(Credentials::new(keys.clone(), signer.clone(), 0)).await;
    sealer
        .run(&format!("INSERT INTO settings VALUES ('{name}', 'x')"))
        .await;
    sealer.replica.collect(&mut sealer.conn).await.unwrap();
    let outbox = sealer.replica.outbox(&mut sealer.conn, 100).await.unwrap();
    seal(&outbox.changes, keys, 0, signer, random_nonce().unwrap())
}

async fn pair(server: &Server) -> [Peer; 2] {
    let [made, joined] = credentials(server).await;
    let mut peers = [file_peer(made).await, file_peer(joined).await];
    for peer in &mut peers {
        peer.run(MARKS).await;
        peer.run(ONCE).await;
    }
    peers
}

const ONCE: &str = "CREATE TABLE once (id INTEGER PRIMARY KEY)";

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

async fn sync_hooked<A: Apply>(
    peer: &mut Peer,
    server: &Server,
    hook: &mut A,
) -> Result<SyncReport, Error> {
    sync_with(
        &mut peer.conn,
        &peer.replica,
        server,
        &peer.credentials,
        now,
        hook,
    )
    .await
}

/// A copy of the server's libraries and limits, as a second server.
fn copy_of(server: &Server) -> Server {
    let copy = Server::new();
    let (libraries, caps) = server.with(|s| {
        let libraries: HashMap<_, _> = s
            .libraries
            .iter()
            .map(|(id, library)| {
                let copied = Library {
                    devices: library.devices.clone(),
                    log: library.log.clone(),
                };
                (*id, copied)
            })
            .collect();
        (libraries, s.caps.clone())
    });
    copy.with(|s| {
        s.libraries = libraries;
        s.caps = caps;
    });
    copy
}

/// Every row of every table, the kernel's and the client's included.
async fn everything(conn: &mut SqliteConnection) -> Vec<String> {
    let tables: Vec<String> =
        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .fetch_all(&mut *conn)
            .await
            .unwrap();
    let mut out = Vec::new();
    for table in tables {
        let columns: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info(?)")
            .bind(&table)
            .fetch_all(&mut *conn)
            .await
            .unwrap();
        let row = columns
            .iter()
            .map(|c| format!("quote(\"{c}\")"))
            .collect::<Vec<_>>()
            .join(" || ',' || ");
        let mut rows: Vec<String> = sqlx::query_scalar(&format!("SELECT {row} FROM \"{table}\""))
            .fetch_all(&mut *conn)
            .await
            .unwrap();
        rows.sort();
        out.push(format!("{table}: {}", rows.join(" | ")));
    }
    out
}

#[derive(Clone, Copy, PartialEq)]
enum Via {
    Sync,
    Default,
}

async fn sync_via(via: Via, peer: &mut Peer, server: &Server) -> String {
    let result = match via {
        Via::Sync => peer.sync(server).await,
        Via::Default => sync_hooked(peer, server, &mut ()).await,
    };
    // How long it held the lock is a matter of timing.
    let result = result.map(|report| SyncReport {
        held: Duration::ZERO,
        ..report
    });
    format!("{result:?}")
}

/// Runs one scripted exchange over copies of two databases and a server, and returns every
/// sync's result and both databases.
async fn scripted(
    via: Via,
    dir: &Path,
    server: &Server,
    [a, b]: &[Credentials; 2],
) -> (Vec<String>, Vec<String>) {
    let server = copy_of(server);
    let mut a = Peer::open(&dir.join("a.db"), a.clone()).await;
    let mut b = Peer::open(&dir.join("b.db"), b.clone()).await;
    let mut results = vec![
        sync_via(via, &mut a, &server).await,
        sync_via(via, &mut b, &server).await,
    ];
    server.lose_at(4);
    results.push(sync_via(via, &mut a, &server).await);
    server.behave();
    results.push(sync_via(via, &mut a, &server).await);
    b.run("DROP TRIGGER no_drafts").await;
    for _ in 0..2 {
        results.push(sync_via(via, &mut b, &server).await);
        results.push(sync_via(via, &mut a, &server).await);
    }
    let mut tables = everything(&mut a.conn).await;
    tables.extend(everything(&mut b.conn).await);
    (results, tables)
}

#[tokio::test]
async fn the_default_apply_does_exactly_what_sync_does() {
    let dir = scratch("default-apply");
    let server = Server::new();
    server.with(|s| {
        s.caps.max_page_batches = Some(1);
        s.caps.max_batch_bytes = Some(400);
    });
    let creds = credentials(&server).await;
    let mut a = Peer::new_at(&dir.join("a.db"), creds[0].clone()).await;
    let mut b = Peer::new_at(&dir.join("b.db"), creds[1].clone()).await;
    b.run(
        "CREATE TRIGGER no_drafts BEFORE INSERT ON books WHEN NEW.title = 'draft'
         BEGIN SELECT RAISE(ABORT, 'no drafts'); END",
    )
    .await;
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
        .await;
    a.sync(&server).await.unwrap();
    b.sync(&server).await.unwrap();
    a.run("UPDATE notes SET body = 'one'").await;
    a.run("INSERT INTO books (title) VALUES ('draft')").await;
    for n in 0..3 {
        a.run(&format!("INSERT INTO settings VALUES ('a{n}', 'x')"))
            .await;
    }
    later();
    b.run("UPDATE notes SET body = 'two'").await;
    b.run("UPDATE books SET year = 1871").await;
    for n in 0..3 {
        b.run(&format!("INSERT INTO settings VALUES ('b{n}', 'x')"))
            .await;
    }
    // Collected now, so the clocks of these edits are the same in every run below.
    for peer in [&mut a, &mut b] {
        peer.replica.collect(&mut peer.conn).await.unwrap();
    }

    let mut runs = Vec::new();
    for (n, via) in [Via::Sync, Via::Sync, Via::Default].into_iter().enumerate() {
        let world = dir.join(format!("world{n}"));
        std::fs::create_dir_all(&world).unwrap();
        a.back_up(&world.join("a.db")).await;
        b.back_up(&world.join("b.db")).await;
        runs.push(scripted(via, &world, &server, &creds).await);
    }
    let (results, tables) = &runs[0];
    assert!(
        results.iter().any(|r| r.contains("conflicts: [Conflict")),
        "{results:#?}"
    );
    assert!(
        results.iter().any(|r| r.starts_with("Err(Transport")),
        "{results:#?}"
    );
    assert!(
        results.iter().any(|r| r.contains("waiting: 1")),
        "{results:#?}"
    );
    // Two runs through `sync` agree, so a difference below is the entry point's.
    assert_eq!(runs[1], runs[0]);
    assert_eq!(&runs[2].0, results);
    assert_eq!(&runs[2].1, tables);
    let _ = std::fs::remove_dir_all(dir);
}

const MARKS: &str = "CREATE TABLE marks (
    collection TEXT NOT NULL, gid TEXT NOT NULL, field TEXT NOT NULL, clock TEXT NOT NULL,
    value TEXT)";

#[derive(Clone, Copy, PartialEq, Debug)]
enum Fail {
    BeforeApplying,
    AfterApplying,
}

/// Writes a row in the application's own table for every change it is given, and counts its
/// calls. It can be told to fail, and to settle by running a statement and applying again.
#[derive(Default)]
struct Marking {
    applies: usize,
    caught_up: usize,
    /// Fail the next this many applies, at this point.
    fail: Option<(Fail, usize)>,
    fail_caught_up: bool,
    settle: Option<&'static str>,
    /// Run after every apply, before the client's own writes in the same transaction.
    after: Option<String>,
    pace: Pace,
    /// Calls of `between`, and how many of them found a transaction open.
    betweens: usize,
    inside: usize,
    /// Fail the `between` call with this number, counting from 1.
    fail_between: Option<usize>,
    /// Never return from the `between` call with this number, raising the signal instead.
    hang_between: Option<(usize, Arc<Signal>)>,
    /// Run in the `between` call with this number.
    in_between: Option<(usize, &'static str)>,
    /// What happened, in order: each apply's changes, each gap.
    events: Vec<String>,
}

/// One envelope per apply, and no pause.
fn one_envelope() -> Pace {
    Pace {
        apply: 0,
        hold: Duration::ZERO,
        pause: Duration::ZERO,
        ..Pace::default()
    }
}

/// The paces the sweeps run under: the default, and the smallest.
fn paces() -> [Pace; 2] {
    [Pace::default(), one_envelope()]
}

fn failed(what: &str) -> Error {
    Error::Application(what.into())
}

impl Apply for Marking {
    async fn apply(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        changes: &[Change],
    ) -> Result<Applied, Error> {
        self.applies += 1;
        self.events.push(format!("apply {}", changes.len()));
        let fail = match &mut self.fail {
            Some((at, left)) if *left > 0 => {
                *left -= 1;
                Some(*at)
            }
            _ => None,
        };
        for change in changes {
            let value = match &change.value {
                fongo_sync_client::sqlite::core::Value::Text(text) => Some(text.clone()),
                _ => None,
            };
            sqlx::query("INSERT INTO marks VALUES (?, ?, ?, ?, ?)")
                .bind(&change.collection)
                .bind(change.gid.to_hex())
                .bind(&change.field)
                .bind(format!("{:?}", change.clock))
                .bind(value)
                .execute(&mut *conn)
                .await?;
        }
        if fail == Some(Fail::BeforeApplying) {
            return Err(failed("before applying"));
        }
        let applied = replica.apply(conn, changes).await?;
        if fail == Some(Fail::AfterApplying) {
            return Err(failed("after applying"));
        }
        if let Some(sql) = &self.after {
            sqlx::query(sql).execute(&mut *conn).await?;
        }
        Ok(applied)
    }

    async fn caught_up(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
    ) -> Result<Option<Applied>, Error> {
        self.caught_up += 1;
        self.events.push("caught up".into());
        sqlx::query("CREATE TABLE IF NOT EXISTS settled (n INTEGER)")
            .execute(&mut *conn)
            .await?;
        sqlx::query("INSERT INTO settled VALUES (?)")
            .bind(self.caught_up as i64)
            .execute(&mut *conn)
            .await?;
        if self.fail_caught_up {
            return Err(failed("settling"));
        }
        let Some(sql) = self.settle else {
            return Ok(None);
        };
        sqlx::query(sql).execute(&mut *conn).await?;
        Ok(Some(replica.apply(conn, &[]).await?))
    }

    async fn between(&mut self, conn: &mut SqliteConnection) -> Result<(), Error> {
        self.betweens += 1;
        self.events.push("between".into());
        if conn.is_in_transaction() || fongo_sync_client::check_connection(conn).await.is_err() {
            self.inside += 1;
        }
        if self.fail_between == Some(self.betweens) {
            return Err(failed("between"));
        }
        if let Some((at, sql)) = self.in_between
            && at == self.betweens
        {
            sqlx::query(sql).execute(&mut *conn).await?;
        }
        if let Some((at, hung)) = &self.hang_between
            && *at == self.betweens
        {
            hung.raise();
            std::future::pending::<()>().await;
        }
        Ok(())
    }

    fn pace(&self) -> Pace {
        self.pace
    }
}

async fn marked(peer: &mut Peer, value: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM marks WHERE value = ?")
        .bind(value)
        .fetch_one(&mut peer.conn)
        .await
        .unwrap()
}

async fn settled(peer: &mut Peer) -> Vec<i64> {
    let exists: Option<i64> =
        sqlx::query_scalar("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'settled'")
            .fetch_optional(&mut peer.conn)
            .await
            .unwrap();
    if exists.is_none() {
        return Vec::new();
    }
    sqlx::query_scalar("SELECT n FROM settled ORDER BY n")
        .fetch_all(&mut peer.conn)
        .await
        .unwrap()
}

/// Every change in the first `envelopes` of the log, which `signer` sealed, was marked exactly
/// once, and no change was marked twice. Marking per change says this only because no change is
/// in the log twice here; an application works from what the apply touched.
async fn marked_once(peer: &mut Peer, server: &Server, signer: &Credentials, envelopes: usize) {
    let keys = peer.credentials.library().clone();
    assert_no_change_sent_twice(server, &keys);
    let key = signer.device().verifying_key();
    let mut theirs = BTreeSet::new();
    for envelope in &server.log(keys.id)[..envelopes] {
        let opened = open(envelope, &keys, |_| Some(key)).unwrap();
        for change in opened.changes {
            theirs.insert((
                change.collection,
                change.gid.to_hex(),
                change.field,
                format!("{:?}", change.clock),
            ));
        }
    }
    let marks: Vec<(String, String, String, String, i64)> = sqlx::query_as(
        "SELECT collection, gid, field, clock, count(*) FROM marks
         GROUP BY collection, gid, field, clock",
    )
    .fetch_all(&mut peer.conn)
    .await
    .unwrap();
    let mut seen = BTreeMap::new();
    for (collection, gid, field, clock, n) in marks {
        assert_eq!(n, 1, "marked {n} times: {collection} {gid} {field} {clock}");
        seen.insert((collection, gid, field, clock), n);
    }
    for change in theirs {
        assert!(seen.contains_key(&change), "never marked: {change:?}");
    }
}

/// The first device has a book and a note to send and the second device's setting to read; its
/// sync makes four requests: capabilities, a page, the device list and one append.
async fn crash_scene(server: &Server) -> [Peer; 2] {
    let [mut a, mut b] = pair(server).await;
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'slow start')")
        .await;
    b.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    b.sync(server).await.unwrap();
    [a, b]
}

#[tokio::test]
async fn what_the_hook_writes_is_kept_with_the_applied_rows_and_the_cursor_or_not_at_all() {
    for (pace, lose) in paces().into_iter().flat_map(|p| [(p, false), (p, true)]) {
        for n in 1..=5 {
            let server = Server::new();
            let [mut a, mut b] = crash_scene(&server).await;
            if lose {
                server.lose_at(n);
            } else {
                server.fail_at(n);
            }
            let mut hook = Marking {
                pace,
                ..Marking::default()
            };
            let result = sync_hooked(&mut a, &server, &mut hook).await;
            assert_eq!(result.is_ok(), n == 5, "lose={lose} n={n}: {result:?}");
            let applied = a.settings().await == ["theme=dark"];
            let marked = marked(&mut a, "dark").await;
            assert_eq!(applied, n >= 4, "lose={lose} n={n}");
            assert_eq!(applied, a.cursor().await >= 1, "lose={lose} n={n}");
            assert_eq!(marked, i64::from(applied), "lose={lose} n={n}");
            assert_durable(&mut a, &format!("lose={lose} n={n}")).await;

            server.behave();
            for _ in 0..3 {
                sync_hooked(&mut a, &server, &mut hook).await.unwrap();
                b.sync(&server).await.unwrap();
            }
            let signer = b.credentials.clone();
            marked_once(&mut a, &server, &signer, 1).await;
            assert_durable(&mut a, &format!("lose={lose} n={n}, at the end")).await;
            assert_converged(&mut [a, b]).await;
        }
    }
}

#[tokio::test]
async fn a_sync_dropped_at_any_request_keeps_the_hooks_rows_with_the_cursor() {
    for (pace, handled) in paces().into_iter().flat_map(|p| [(p, false), (p, true)]) {
        for n in 1.. {
            assert!(n < 100, "the sync never ended");
            let server = Server::new();
            server.with(|s| s.caps.max_page_batches = Some(1));
            let [mut a, mut b] = pair(&server).await;
            for i in 0..3 {
                b.run(&format!("INSERT INTO books (title) VALUES ('theirs {i}')"))
                    .await;
                b.sync(&server).await.unwrap();
            }
            a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
                .await;
            for i in 0..3 {
                a.run(&format!(
                    "INSERT INTO settings VALUES ('key{i}', 'value{i}')"
                ))
                .await;
            }
            server.with(|s| s.caps.max_batch_bytes = Some(400));
            server.hang_at(n, handled);
            let mut hook = Marking {
                pace,
                ..Marking::default()
            };
            let ended = tokio::select! {
                result = sync_hooked(&mut a, &server, &mut hook) => {
                    result.unwrap();
                    true
                }
                () = server.hung() => false,
            };
            let cursor = a.cursor().await;
            let titles = a.lines("SELECT title FROM books").await;
            for i in 0..3u64 {
                let title = format!("theirs {i}");
                let applied = titles.contains(&title);
                let at = format!("{pace:?} handled={handled} n={n} i={i}");
                assert_eq!(applied, cursor > i, "{at}");
                assert_eq!(marked(&mut a, &title).await, i64::from(applied), "{at}");
            }
            assert_durable(&mut a, &format!("handled={handled} n={n}")).await;

            server.behave();
            for _ in 0..3 {
                sync_hooked(&mut a, &server, &mut hook).await.unwrap();
                b.sync(&server).await.unwrap();
            }
            let signer = b.credentials.clone();
            marked_once(&mut a, &server, &signer, 3).await;
            assert_durable(&mut a, &format!("handled={handled} n={n}, at the end")).await;
            assert_converged(&mut [a, b]).await;
            if ended {
                break;
            }
        }
    }
}

/// The client's writes after the hook's apply, in the transaction that applies an envelope from a
/// device listed late together with this device's own envelope, whose append's answer was lost.
const CLIENT_WRITES: [&str; 5] = [
    "UPDATE ON fongo_client_state",
    "DELETE ON fongo_outbox",
    "INSERT ON fongo_client_appended",
    "DELETE ON fongo_client_appended",
    "DELETE ON fongo_bases",
];

#[tokio::test]
async fn a_failure_in_any_of_the_clients_writes_after_the_hook_keeps_none_of_its_rows() {
    for (pace, write) in paces()
        .into_iter()
        .flat_map(|p| CLIENT_WRITES.map(|w| (p, w)))
    {
        let server = Server::new();
        let [mut a, mut b] = pair(&server).await;
        let keys = a.credentials.library().clone();
        let stranger = generate_signing_key().unwrap();
        a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
            .await;
        a.run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
            .await;
        a.sync(&server).await.unwrap();
        let mut c = Peer::new(Credentials::new(keys.clone(), stranger.clone(), 0)).await;
        c.run("INSERT INTO settings VALUES ('stranger', 'x')").await;
        c.replica.collect(&mut c.conn).await.unwrap();
        let outbox = c.replica.outbox(&mut c.conn, 100).await.unwrap();
        server.inject(
            keys.id,
            seal(
                &outbox.changes,
                &keys,
                0,
                &stranger,
                random_nonce().unwrap(),
            ),
        );
        a.run("UPDATE notes SET body = 'slow start'").await;
        // Capabilities, a page, the device list twice, then the append, whose answer is lost.
        server.lose_at(5);
        assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));
        server.behave();
        assert_eq!((server.log(keys.id).len(), a.cursor().await), (3, 1));
        server.register(keys.id, stranger.verifying_key());

        let mut hook = Marking {
            after: Some(format!(
                "CREATE TEMP TRIGGER IF NOT EXISTS crash BEFORE {write}
                 BEGIN SELECT RAISE(ABORT, 'crash'); END"
            )),
            pace,
            ..Marking::default()
        };
        let result = sync_hooked(&mut a, &server, &mut hook).await;
        assert!(
            matches!(result, Err(Error::Database(_))),
            "{write}: {result:?}"
        );
        // One envelope per apply puts the stranger's envelope in a transaction of its own, whose
        // only client write is the cursor's; it commits, with the trigger, unless that write fails.
        let kept = pace == one_envelope() && write != CLIENT_WRITES[0];
        let at = format!("{pace:?} {write}");
        assert_eq!(
            (hook.applies, hook.caught_up),
            (2 + usize::from(kept), 0),
            "{at}"
        );
        assert_eq!(a.cursor().await, 1 + u64::from(kept), "{at}");
        assert_eq!(a.settings().await.len(), usize::from(kept), "{at}");
        assert_eq!(marked(&mut a, "stranger").await, i64::from(kept), "{at}");
        assert_durable(&mut a, &at).await;

        hook.after = None;
        a.run("DROP TRIGGER IF EXISTS temp.crash").await;
        sync_hooked(&mut a, &server, &mut hook).await.unwrap();
        assert_eq!(a.settings().await, ["stranger=x"], "{write}");
        assert_eq!(marked(&mut a, "stranger").await, 1, "{write}");
        assert_eq!(server.log(keys.id).len(), 3, "{write}");
        assert_no_change_sent_twice(&server, &keys);
        b.sync(&server).await.unwrap();
        assert_converged(&mut [a, b]).await;
    }
}

#[tokio::test]
async fn an_apply_that_fails_keeps_nothing_and_the_next_sync_recovers() {
    for (pace, at) in paces()
        .into_iter()
        .flat_map(|p| [(p, Fail::BeforeApplying), (p, Fail::AfterApplying)])
    {
        let server = Server::new();
        let [mut a, mut b] = crash_scene(&server).await;
        let id = a.credentials.library_id();
        let mut hook = Marking {
            fail: Some((at, 1)),
            pace,
            ..Marking::default()
        };
        let result = sync_hooked(&mut a, &server, &mut hook).await;
        assert!(
            matches!(result, Err(Error::Application(_))),
            "{at:?}: {result:?}"
        );
        assert_eq!((hook.applies, hook.caught_up), (1, 0), "{at:?}");
        assert_eq!(a.cursor().await, 0, "{at:?}");
        assert!(a.settings().await.is_empty(), "{at:?}");
        assert_eq!(marked(&mut a, "dark").await, 0, "{at:?}");
        // Nothing was sent: the error came before the outbox.
        assert_eq!(server.log(id).len(), 1, "{at:?}");
        assert_durable(&mut a, &format!("{at:?}")).await;

        let report = sync_hooked(&mut a, &server, &mut hook).await.unwrap();
        assert_eq!(report.received, 3, "{at:?}");
        assert!(report.sent > 0, "{at:?}");
        assert_eq!(a.settings().await, ["theme=dark"], "{at:?}");
        assert_eq!(marked(&mut a, "dark").await, 1, "{at:?}");
        assert_eq!(a.cursor().await, 2, "{at:?}");
        assert_durable(&mut a, &format!("{at:?}, recovered")).await;
        b.sync(&server).await.unwrap();
        assert_converged(&mut [a, b]).await;
    }
}

/// The first device has an append past its cursor that it has not read back, and an envelope
/// from a device it has not listed yet before it, so its next pull applies at its own envelope
/// and again at the end.
async fn behind_its_own_append(server: &Server) -> [Peer; 2] {
    let [mut a, mut b] = pair(server).await;
    let keys = a.credentials.library().clone();
    b.run("INSERT INTO settings VALUES ('first', 'x')").await;
    b.sync(server).await.unwrap();
    let stranger = generate_signing_key().unwrap();
    server.inject(keys.id, sealed_setting(&keys, &stranger, "stranger").await);
    a.run("INSERT INTO settings VALUES ('mine', 'x')").await;
    let blocked = a.sync(server).await.unwrap();
    assert_eq!(blocked.blocked.map(|b| b.seq), Some(2));
    assert_eq!(a.cursor().await, 1);
    b.run("INSERT INTO settings VALUES ('last', 'x')").await;
    b.sync(server).await.unwrap();
    server.register(keys.id, stranger.verifying_key());
    [a, b]
}

#[tokio::test]
async fn an_apply_that_fails_part_way_through_a_pull_is_tried_once_more_before_the_error() {
    for pace in paces() {
        let server = Server::new();
        let [mut a, mut b] = behind_its_own_append(&server).await;
        let mut hook = Marking {
            fail: Some((Fail::AfterApplying, 1)),
            pace,
            ..Marking::default()
        };
        let result = sync_hooked(&mut a, &server, &mut hook).await;
        assert!(matches!(result, Err(Error::Application(_))), "{result:?}");
        // The apply failed and was rolled back; the sync stopped reading, applied what it held
        // once more, and kept that. By default that apply is the one at its own envelope; with
        // one envelope per apply, the stranger's alone, before its own envelope is read.
        assert_eq!((hook.applies, hook.caught_up), (2, 0), "{pace:?}");
        let read = if pace == one_envelope() { 2 } else { 3 };
        assert_eq!(a.cursor().await, read, "{pace:?}");
        assert_eq!(a.settings().await, ["first=x", "mine=x", "stranger=x"]);
        assert_eq!(marked(&mut a, "stranger").await, 1, "{pace:?}");
        assert_durable(&mut a, "after the retry").await;

        sync_hooked(&mut a, &server, &mut hook).await.unwrap();
        assert_eq!(hook.caught_up, 1, "{pace:?}");
        assert_durable(&mut a, "recovered").await;
        assert_eq!(
            a.settings().await,
            ["first=x", "last=x", "mine=x", "stranger=x"]
        );
        b.sync(&server).await.unwrap();
        assert_converged(&mut [a, b]).await;
    }
}

/// The second device sent three envelopes, a book in each, and the first has a setting to send.
async fn three_envelopes(server: &Server) -> [Peer; 2] {
    let [mut a, mut b] = pair(server).await;
    for i in 0..3 {
        b.run(&format!("INSERT INTO books (title) VALUES ('theirs {i}')"))
            .await;
        b.sync(server).await.unwrap();
    }
    a.run("INSERT INTO settings VALUES ('mine', 'x')").await;
    [a, b]
}

/// Each of the second device's books is on the first device and marked once exactly when the
/// cursor is past its envelope.
async fn applied_with_the_cursor(a: &mut Peer, at: &str) -> u64 {
    let cursor = a.cursor().await;
    let titles = a.lines("SELECT title FROM books").await;
    for i in 0..3u64 {
        let title = format!("theirs {i}");
        let applied = titles.contains(&title);
        assert_eq!(applied, cursor > i, "{at}: {title}, cursor {cursor}");
        assert_eq!(marked(a, &title).await, i64::from(applied), "{at}: {title}");
    }
    cursor
}

#[tokio::test]
async fn between_is_called_once_in_each_gap_and_never_inside_a_transaction() {
    for (pace, applies) in [(Pace::default(), 1), (one_envelope(), 3)] {
        let server = Server::new();
        let [mut a, _b] = three_envelopes(&server).await;
        let mut hook = Marking {
            pace,
            ..Marking::default()
        };
        let report = sync_hooked(&mut a, &server, &mut hook).await.unwrap();
        assert_eq!(report.sent, 3);
        let events: Vec<&str> = hook
            .events
            .iter()
            .map(|e| if e.starts_with("apply") { "apply" } else { e })
            .collect();
        // Each apply, `caught_up`, and the record and acknowledgment of the one envelope sent.
        let mut want = vec!["apply"];
        for _ in 1..applies {
            want.extend(["between", "apply"]);
        }
        want.extend(["between", "caught up", "between", "between"]);
        assert_eq!(events, want, "{pace:?}");
        assert_eq!(hook.inside, 0, "{pace:?}");
    }
}

#[tokio::test]
async fn a_failing_between_stops_the_sync_and_keeps_each_apply_with_its_cursor() {
    let server = Server::new();
    let [mut a, mut b] = three_envelopes(&server).await;
    let id = a.credentials.library_id();
    let mut hook = Marking {
        pace: one_envelope(),
        fail_between: Some(1),
        ..Marking::default()
    };
    let result = sync_hooked(&mut a, &server, &mut hook).await;
    assert!(matches!(result, Err(Error::Application(_))), "{result:?}");
    // The first envelope was committed with the cursor past it. The gap after it failed, and the
    // second envelope, already held, was applied once more without another call.
    assert_eq!((hook.applies, hook.betweens, hook.caught_up), (2, 1, 0));
    assert_eq!(applied_with_the_cursor(&mut a, "the gap failed").await, 2);
    assert_eq!(server.log(id).len(), 3);
    assert_durable(&mut a, "the gap failed").await;

    hook.fail_between = None;
    sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    b.sync(&server).await.unwrap();
    let signer = b.credentials.clone();
    marked_once(&mut a, &server, &signer, 3).await;
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn a_sync_dropped_inside_between_keeps_each_apply_with_its_cursor() {
    let server = Server::new();
    let [mut a, mut b] = three_envelopes(&server).await;
    let id = a.credentials.library_id();
    let hung = Arc::new(Signal::default());
    let mut hook = Marking {
        pace: one_envelope(),
        hang_between: Some((1, hung.clone())),
        ..Marking::default()
    };
    tokio::select! {
        result = sync_hooked(&mut a, &server, &mut hook) => panic!("it returned: {result:?}"),
        () = hung.raised() => {}
    }
    assert_eq!(
        applied_with_the_cursor(&mut a, "dropped in the gap").await,
        1
    );
    fongo_sync_client::check_connection(&mut a.conn)
        .await
        .unwrap();
    assert_durable(&mut a, "dropped in the gap").await;
    assert_eq!(server.log(id).len(), 3);

    hook.hang_between = None;
    sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    b.sync(&server).await.unwrap();
    let signer = b.credentials.clone();
    marked_once(&mut a, &server, &signer, 3).await;
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn a_between_that_leaves_a_transaction_open_stops_the_sync_as_unsound() {
    let server = Server::new();
    let [mut a, mut b] = three_envelopes(&server).await;
    let id = a.credentials.library_id();
    let mut hook = Marking {
        pace: one_envelope(),
        in_between: Some((1, "BEGIN")),
        ..Marking::default()
    };
    let result = sync_hooked(&mut a, &server, &mut hook).await;
    assert!(
        matches!(result, Err(Error::ConnectionUnsound { source: None })),
        "{result:?}"
    );
    // Nothing was written after the gap: the second envelope, held, was not applied.
    assert_eq!((hook.applies, hook.betweens), (1, 1));
    a.run("ROLLBACK").await;
    assert_eq!(applied_with_the_cursor(&mut a, "left open").await, 1);
    assert_durable(&mut a, "left open").await;
    assert_eq!(server.log(id).len(), 3);

    hook.in_between = None;
    sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    b.sync(&server).await.unwrap();
    let signer = b.credentials.clone();
    marked_once(&mut a, &server, &signer, 3).await;
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn between_is_not_called_after_a_failure() {
    let server = Server::new();
    let [mut a, _b] = three_envelopes(&server).await;
    server.with(|s| s.caps.max_page_batches = Some(1));
    // Capabilities, the first page, the device list, the second page; the third page fails after
    // both envelopes were committed, and the gap after the second was given.
    server.fail_at(5);
    let mut hook = Marking {
        pace: one_envelope(),
        ..Marking::default()
    };
    let result = sync_hooked(&mut a, &server, &mut hook).await;
    assert!(matches!(result, Err(Error::Transport(_))), "{result:?}");
    let events: Vec<&str> = hook.events.iter().map(|e| &e[..5]).collect();
    assert_eq!(events, ["apply", "betwe", "apply"]);
    assert_eq!(applied_with_the_cursor(&mut a, "a page failed").await, 2);
}

/// A sync whose apply reads before the kernel's first write, while another connection tries to
/// write in between. The sync's transaction holds the write lock from its first statement, so the
/// other connection waits and gives up, and the sync goes on. With a transaction that only takes
/// the lock at its first write, in WAL mode the other connection commits and the sync's write then
/// fails at once, since its snapshot is out of date; in rollback-journal mode the other connection
/// is kept out either way.
#[tokio::test]
async fn an_apply_that_reads_first_keeps_other_writers_out_and_completes() {
    struct ReadsFirst {
        path: PathBuf,
        other_wrote: Vec<bool>,
    }

    impl Apply for ReadsFirst {
        async fn apply(
            &mut self,
            conn: &mut SqliteConnection,
            replica: &Replica,
            changes: &[Change],
        ) -> Result<Applied, Error> {
            let _: i64 = sqlx::query_scalar("SELECT count(*) FROM books")
                .fetch_one(&mut *conn)
                .await?;
            let path = self.path.clone();
            let wrote = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                runtime.block_on(async {
                    let mut other = SqliteConnectOptions::new()
                        .filename(&path)
                        .busy_timeout(Duration::from_millis(50))
                        .connect()
                        .await
                        .unwrap();
                    sqlx::query("INSERT INTO once VALUES (1)")
                        .execute(&mut other)
                        .await
                        .is_ok()
                })
            })
            .join()
            .unwrap();
            self.other_wrote.push(wrote);
            Ok(replica.apply(conn, changes).await?)
        }
    }

    for mode in ["WAL", "DELETE"] {
        let server = Server::new();
        let [mut a, mut b] = pair(&server).await;
        a.run(&format!("PRAGMA journal_mode = {mode}")).await;
        b.run("INSERT INTO books (title) VALUES ('Middlemarch')")
            .await;
        b.sync(&server).await.unwrap();
        let path = PathBuf::from(file_of(&mut a.conn).await);
        let mut hook = ReadsFirst {
            path,
            other_wrote: Vec::new(),
        };
        let result = sync_hooked(&mut a, &server, &mut hook).await;
        assert!(result.is_ok(), "{mode}: {result:?}");
        assert_eq!(hook.other_wrote, [false], "{mode}");
        assert_eq!(a.lines("SELECT title FROM books").await, ["Middlemarch"]);
    }
}

#[tokio::test]
async fn a_local_edit_written_between_two_applies_is_a_conflict_of_its_own() {
    for pace in paces() {
        let server = Server::new();
        let [mut a, mut b] = pair(&server).await;
        a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
            .await;
        a.run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
            .await;
        a.sync(&server).await.unwrap();
        b.sync(&server).await.unwrap();
        b.run("UPDATE notes SET body = 'mine 1'").await;
        b.replica.collect(&mut b.conn).await.unwrap();
        for n in 1..=2 {
            later();
            a.run(&format!("UPDATE notes SET body = 'theirs {n}'"))
                .await;
            a.sync(&server).await.unwrap();
        }
        later();
        let mut hook = Marking {
            pace,
            in_between: Some((1, "UPDATE notes SET body = 'mine 2'")),
            ..Marking::default()
        };
        let report = sync_hooked(&mut b, &server, &mut hook).await.unwrap();
        let mut found: Vec<String> = report
            .applied
            .conflicts
            .iter()
            .map(|c| format!("{:?} against {:?}", c.local, c.remote))
            .collect();
        found.sort();
        // With one apply for both envelopes the second edit is written after the pull, and only
        // the first is a conflict; with one apply each, both are, and neither hides the other.
        let want: &[&str] = if pace == one_envelope() {
            &[
                "Text(\"mine 1\") against Text(\"theirs 1\")",
                "Text(\"mine 2\") against Text(\"theirs 2\")",
            ]
        } else {
            &["Text(\"mine 1\") against Text(\"theirs 2\")"]
        };
        assert_eq!(found, want, "{pace:?}");
    }
}

/// A conflict on a note, then, in later envelopes of the same pull, the note deleted, and a new
/// note that takes the local id the first had. The report's key is the row as the sync leaves it,
/// whatever an earlier apply saw.
#[tokio::test]
async fn a_conflicts_key_is_the_row_its_entity_has_at_the_end_of_the_sync() {
    for (pace, reused) in paces().into_iter().flat_map(|p| [(p, false), (p, true)]) {
        let server = Server::new();
        let [mut a, mut b] = pair(&server).await;
        a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
            .await;
        a.run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
            .await;
        a.sync(&server).await.unwrap();
        b.sync(&server).await.unwrap();
        b.run("UPDATE notes SET body = 'mine'").await;
        b.replica.collect(&mut b.conn).await.unwrap();
        later();
        a.run("UPDATE notes SET body = 'theirs'").await;
        a.sync(&server).await.unwrap();
        let note = a.replica.gid_of(&mut a.conn, "notes", 1).await.unwrap();
        a.run("DELETE FROM notes").await;
        a.sync(&server).await.unwrap();
        if reused {
            a.run("INSERT INTO notes (book_id, body) VALUES (1, 'another')")
                .await;
            a.sync(&server).await.unwrap();
        }
        let mut hook = Marking {
            pace,
            ..Marking::default()
        };
        let report = sync_hooked(&mut b, &server, &mut hook).await.unwrap();
        let at = format!("{pace:?} reused={reused}");
        assert_eq!(report.applied.conflicts.len(), 1, "{at}");
        let conflict = &report.applied.conflicts[0];
        assert_eq!(
            (Some(conflict.gid), conflict.key.as_deref()),
            (note, None),
            "{at}"
        );
        let ids = b.lines("SELECT CAST(id AS TEXT) FROM notes").await;
        assert_eq!(ids.len(), usize::from(reused), "{at}");
        if reused && pace == one_envelope() {
            // Applied apart from the deletion, the new note took the deleted one's local id.
            assert_eq!(ids, ["1"], "{at}");
        }
    }
}

#[tokio::test]
async fn a_caller_that_syncs_again_at_once_carries_the_held_time_into_the_next_sync() {
    let server = Server::new();
    let [mut a, _b] = three_envelopes(&server).await;
    let report = sync_hooked(&mut a, &server, &mut Marking::default())
        .await
        .unwrap();
    assert!(report.held > Duration::ZERO);
    let pause = Duration::from_millis(300);
    let mut hook = Marking {
        pace: Pace {
            hold: Duration::from_millis(100),
            pause,
            held: Duration::from_millis(100),
            ..Pace::default()
        },
        ..Marking::default()
    };
    let began = std::time::Instant::now();
    let report = sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    assert!(began.elapsed() >= pause, "{:?}", began.elapsed());
    assert!(report.held > Duration::ZERO);
}

const TAGS: &str = "CREATE TABLE tags (id INTEGER PRIMARY KEY, isbn TEXT UNIQUE, note TEXT)";

/// A replica of tags, one entity per isbn, over a file of its own.
async fn tag_peer(dir: &Path, name: &str, credentials: Credentials) -> Peer {
    let mut conn = SqliteConnectOptions::new()
        .filename(dir.join(name))
        .create_if_missing(true)
        .connect()
        .await
        .unwrap();
    sqlx::query(TAGS).execute(&mut conn).await.unwrap();
    let registry = Registry::new(vec![
        Collection::minted("tags", "tags", "id")
            .field("isbn")
            .field_reporting_conflicts("note")
            .identity(&["isbn"]),
    ])
    .unwrap();
    let replica = Replica::new(registry);
    replica.install(&mut conn).await.unwrap();
    Peer {
        conn,
        replica,
        credentials,
    }
}

/// Two tags this device edited become one entity part-way through a pull, and a later envelope
/// changes the merged tag's note again. However many envelopes an apply takes, each local edit is
/// reported once, under the group's id and the key of its row.
#[tokio::test]
async fn edits_of_entities_merged_part_way_through_a_pull_are_reported_once_each() {
    let dir = scratch("merged-mid-pull");
    let mut runs = Vec::new();
    for (n, pace) in [
        one_envelope(),
        Pace::default(),
        Pace {
            apply: usize::MAX,
            ..Pace::default()
        },
    ]
    .into_iter()
    .enumerate()
    {
        let server = Server::new();
        server.with(|s| s.caps.max_page_batches = None);
        let [made, joined] = credentials(&server).await;
        let key = generate_signing_key().unwrap();
        fongo_sync_client::add_device(&server, &made, &key.verifying_key(), now)
            .await
            .unwrap();
        let third = Credentials::new(made.library().clone(), key, 0);
        let mut a = tag_peer(&dir, &format!("a{n}.db"), made).await;
        let mut b = tag_peer(&dir, &format!("b{n}.db"), joined).await;
        let mut c = tag_peer(&dir, &format!("c{n}.db"), third).await;
        a.run("INSERT INTO tags (isbn, note) VALUES ('Z', 'z0'), ('Y', 'y0')")
            .await;
        for peer in [&mut a, &mut b, &mut c] {
            peer.sync(&server).await.unwrap();
        }
        b.run("UPDATE tags SET note = 'mine z' WHERE isbn = 'Z'")
            .await;
        b.run("UPDATE tags SET note = 'mine y' WHERE isbn = 'Y'")
            .await;
        b.replica.collect(&mut b.conn).await.unwrap();
        later();
        a.run("UPDATE tags SET isbn = 'X', note = 'theirs 1' WHERE isbn = 'Z'")
            .await;
        a.sync(&server).await.unwrap();
        later();
        c.run("UPDATE tags SET isbn = 'X', note = 'theirs c' WHERE isbn = 'Y'")
            .await;
        c.sync(&server).await.unwrap();
        later();
        a.sync(&server).await.unwrap();
        a.run("UPDATE tags SET note = 'theirs 2' WHERE isbn = 'X'")
            .await;
        a.sync(&server).await.unwrap();

        let mut hook = Marking {
            pace,
            ..Marking::default()
        };
        b.run(MARKS).await;
        let report = sync_hooked(&mut b, &server, &mut hook).await.unwrap();
        let rows = b
            .lines("SELECT '[' || id || '] ' || coalesce(isbn, '-') || ' ' || note FROM tags")
            .await;
        let mut found = Vec::new();
        for conflict in &report.applied.conflicts {
            let group = b
                .replica
                .group_of(&mut b.conn, "tags", conflict.gid)
                .await
                .unwrap();
            assert_eq!((conflict.gid, conflict.key.clone()), group, "{pace:?}");
            found.push(format!(
                "{:?} {:?} against {:?}, key {:?}",
                conflict.base, conflict.local, conflict.remote, conflict.key
            ));
        }
        found.sort();
        eprintln!("{pace:?}: {found:#?} rows {rows:?}");
        runs.push((found, rows));
    }
    assert_eq!(runs[1], runs[0]);
    assert_eq!(runs[2], runs[0]);
    let _ = std::fs::remove_dir_all(dir);
}

/// Ends the sync unable to look a conflict's entity up: `caught_up` stores the keys of the
/// kernel's rows as blobs, which no lookup reads as keys.
struct HidesRows;

impl Apply for HidesRows {
    async fn caught_up(
        &mut self,
        conn: &mut SqliteConnection,
        _: &Replica,
    ) -> Result<Option<Applied>, Error> {
        sqlx::query("UPDATE fongo_ids SET key = CAST(key AS BLOB)")
            .execute(&mut *conn)
            .await?;
        Ok(None)
    }
}

#[tokio::test]
async fn a_conflict_that_cannot_be_looked_up_at_the_end_keeps_what_its_apply_said() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
        .await;
    a.sync(&server).await.unwrap();
    b.sync(&server).await.unwrap();
    b.run("UPDATE notes SET body = 'mine'").await;
    b.replica.collect(&mut b.conn).await.unwrap();
    later();
    a.run("UPDATE notes SET body = 'theirs'").await;
    a.sync(&server).await.unwrap();
    let note = b.replica.gid_of(&mut b.conn, "notes", 1).await.unwrap();
    let report = sync_hooked(&mut b, &server, &mut HidesRows).await.unwrap();
    b.run("UPDATE fongo_ids SET key = CAST(key AS TEXT)").await;
    assert_eq!(report.sent, 1);
    assert_eq!(report.applied.conflicts.len(), 1);
    let conflict = &report.applied.conflicts[0];
    assert_eq!(
        (Some(conflict.gid), conflict.key.as_deref()),
        (note, Some("[1]"))
    );
    a.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
}

/// Notes when the first apply begins.
struct FirstApply {
    pace: Pace,
    at: Option<std::time::Instant>,
}

impl Apply for FirstApply {
    async fn apply(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        changes: &[Change],
    ) -> Result<Applied, Error> {
        self.at.get_or_insert_with(std::time::Instant::now);
        Ok(replica.apply(conn, changes).await?)
    }

    fn pace(&self) -> Pace {
        self.pace
    }
}

#[tokio::test]
async fn a_sync_that_has_not_held_the_lock_does_not_pause_before_its_first_write() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.sync(&server).await.unwrap();
    let mut hook = FirstApply {
        pace: Pace {
            hold: Duration::ZERO,
            pause: Duration::from_secs(1),
            ..Pace::default()
        },
        at: None,
    };
    let began = std::time::Instant::now();
    sync_hooked(&mut b, &server, &mut hook).await.unwrap();
    let first = hook.at.unwrap() - began;
    assert!(first < Duration::from_millis(750), "{first:?}");
}

/// Drops a sync that applies one envelope at a time at each of its suspensions in turn, from a
/// copy of one scene each time; see `a_sync_dropped_anywhere_around_an_ended_transaction_loses_nothing`
/// for what a busy machine does to the suspensions visited. Nothing here ends a transaction, so
/// the connection is always left sound.
#[tokio::test]
async fn a_sync_of_one_envelope_per_apply_dropped_anywhere_keeps_each_apply_with_its_cursor() {
    const LIMIT: usize = 400;
    let server = Server::new();
    let mut scene = three_envelopes(&server).await;
    for k in 1.. {
        assert!(
            k <= LIMIT,
            "the sync suspended {LIMIT} times and had not ended"
        );
        let (server, [mut a, mut b]) = copy_scene(&server, &mut scene).await;
        let mut hook = Marking {
            pace: one_envelope(),
            ..Marking::default()
        };
        let ended = drive_to(sync_hooked(&mut a, &server, &mut hook), k).await;
        let at = format!("dropped at suspension {k}");
        applied_with_the_cursor(&mut a, &at).await;
        fongo_sync_client::check_connection(&mut a.conn)
            .await
            .unwrap_or_else(|e| panic!("{at}: {e:?}"));
        assert_durable(&mut a, &at).await;
        sync_hooked(&mut a, &server, &mut Marking::default())
            .await
            .unwrap();
        b.sync(&server).await.unwrap();
        assert_eq!(applied_with_the_cursor(&mut a, &at).await, 4, "{at}");
        assert_converged(&mut [a, b]).await;
        if ended {
            break;
        }
    }
}

/// The number of changes in each envelope of the log, which `signer` sealed.
fn envelope_sizes(server: &Server, signer: &Credentials) -> Vec<usize> {
    let keys = signer.library().clone();
    let key = signer.device().verifying_key();
    server
        .log(keys.id)
        .iter()
        .map(|envelope| open(envelope, &keys, |_| Some(key)).unwrap().changes.len())
        .collect()
}

fn sealing(seal: u32) -> Marking {
    Marking {
        pace: Pace {
            seal,
            ..Pace::default()
        },
        ..Marking::default()
    }
}

#[tokio::test]
async fn an_apply_takes_whole_envelopes_up_to_the_bound_and_a_larger_one_alone() {
    let server = Server::new();
    server.with(|s| s.caps.max_page_batches = None);
    let [a, mut b] = pair(&server).await;
    // Three changes a setting.
    for n in 0..5 {
        b.run(&format!("INSERT INTO settings VALUES ('k{n}', 'v')"))
            .await;
    }
    sync_hooked(&mut b, &server, &mut sealing(4)).await.unwrap();
    let signer = b.credentials.clone();
    assert_eq!(envelope_sizes(&server, &signer), [4, 4, 4, 3]);
    for (apply, want) in [
        (usize::MAX, &[15][..]),
        (15, &[15]),
        (9, &[8, 7]),
        (8, &[8, 7]),
        (7, &[4, 4, 7]),
        (3, &[4, 4, 4, 3]),
        (0, &[4, 4, 4, 3]),
    ] {
        let mut fresh = file_peer(a.credentials.clone()).await;
        fresh.run(MARKS).await;
        let mut hook = Marking {
            pace: Pace {
                apply,
                ..Pace::default()
            },
            ..Marking::default()
        };
        let report = sync_hooked(&mut fresh, &server, &mut hook).await.unwrap();
        assert_eq!(report.received, 15);
        let sizes: Vec<usize> = hook
            .events
            .iter()
            .filter_map(|e| e.strip_prefix("apply ")?.parse().ok())
            .collect();
        assert_eq!(sizes, want, "apply at most {apply}");
        marked_once(&mut fresh, &server, &signer, 4).await;
        assert_eq!(fresh.settings().await.len(), 5);
    }
}

#[tokio::test]
async fn a_hundred_thousand_pending_changes_are_sealed_in_envelopes_within_the_bound() {
    let server = Server::new();
    server.with(|s| s.caps.max_batch_bytes = Some(1 << 22));
    let [mut a, _b] = pair(&server).await;
    a.run(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 33334)
         INSERT INTO settings SELECT 'k' || i, 'v' FROM n",
    )
    .await;
    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.sent, 100_002);
    let signer = a.credentials.clone();
    let mut want = vec![1_000; 100];
    want.push(2);
    assert_eq!(envelope_sizes(&server, &signer), want);
}

#[tokio::test]
async fn an_append_whose_answer_was_lost_among_several_is_found_and_not_sent_again() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    for n in 0..4 {
        a.run(&format!("INSERT INTO settings VALUES ('k{n}', 'v')"))
            .await;
    }
    // Twelve changes in envelopes of three. Capabilities, a page, then the appends; the third's
    // answer is lost.
    server.lose_at(5);
    let result = sync_hooked(&mut a, &server, &mut sealing(3)).await;
    assert!(matches!(result, Err(Error::Transport(_))), "{result:?}");
    server.behave();
    assert_eq!((server.log(keys.id).len(), a.cursor().await), (3, 2));
    let report = sync_hooked(&mut a, &server, &mut sealing(3)).await.unwrap();
    assert_eq!(report.sent, 6);
    let signer = a.credentials.clone();
    assert_eq!(envelope_sizes(&server, &signer), [3, 3, 3, 3]);
    assert_no_change_sent_twice(&server, &keys);
    b.sync(&server).await.unwrap();
    assert_eq!(b.settings().await.len(), 4);
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn a_server_limit_below_the_bound_still_decides_the_envelope() {
    let server = Server::new();
    server.with(|s| s.caps.max_batch_bytes = Some(400));
    let [mut a, mut b] = pair(&server).await;
    for n in 0..20 {
        a.run(&format!("INSERT INTO settings VALUES ('k{n}', 'v')"))
            .await;
    }
    sync_hooked(&mut a, &server, &mut sealing(50))
        .await
        .unwrap();
    let signer = a.credentials.clone();
    let sizes = envelope_sizes(&server, &signer);
    assert_eq!(sizes.iter().sum::<usize>(), 60);
    assert!(
        sizes.len() > 1 && sizes.iter().all(|&n| n < 50),
        "{sizes:?}"
    );
    let id = signer.library_id();
    assert!(server.log(id).iter().all(|e| e.len() <= 400));
    b.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
}

/// The same exchange, from copies of the same databases and log, under a pace: every sync's
/// conflicts, sorted, both databases whole, and every change in the log, envelope by envelope.
async fn exchanged(
    pace: Pace,
    dir: &Path,
    server: &Server,
    [a, b]: &[Credentials; 2],
) -> (Vec<String>, Vec<String>, Vec<Vec<String>>) {
    let server = copy_of(server);
    let mut a = Peer::open(&dir.join("a.db"), a.clone()).await;
    let mut b = Peer::open(&dir.join("b.db"), b.clone()).await;
    let mut conflicts = Vec::new();
    let mut applies = Vec::new();
    for n in 0..4 {
        let peer = if n % 2 == 0 { &mut a } else { &mut b };
        let mut hook = Marking {
            pace,
            ..Marking::default()
        };
        let report = sync_hooked(peer, &server, &mut hook).await.unwrap();
        let mut found: Vec<String> = report
            .applied
            .conflicts
            .iter()
            .map(|c| format!("{c:?}"))
            .collect();
        found.sort();
        conflicts.push(found.join("; "));
        applies.push(hook.applies);
    }
    if pace == one_envelope() {
        assert_eq!(
            applies[0], 2,
            "the first pull applies one envelope at a time"
        );
    }
    let mut tables = everything(&mut a.conn).await;
    tables.extend(everything(&mut b.conn).await);
    let keys = a.credentials.library().clone();
    let log = server
        .log(keys.id)
        .iter()
        .map(|envelope| {
            let opened = open(envelope, &keys, |device| {
                [&a.credentials, &b.credentials]
                    .into_iter()
                    .find(|c| c.device_id() == device)
                    .map(|c| c.device().verifying_key())
            })
            .unwrap();
            opened.changes.iter().map(|c| format!("{c:?}")).collect()
        })
        .collect();
    (conflicts, tables, log)
}

#[tokio::test]
async fn conflicts_and_bases_do_not_depend_on_how_many_envelopes_an_apply_takes() {
    let dir = scratch("conflicts-by-pace");
    let server = Server::new();
    server.with(|s| s.caps.max_page_batches = None);
    let creds = credentials(&server).await;
    let mut a = Peer::new_at(&dir.join("a.db"), creds[0].clone()).await;
    let mut b = Peer::new_at(&dir.join("b.db"), creds[1].clone()).await;
    for peer in [&mut a, &mut b] {
        peer.run(MARKS).await;
    }
    a.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
        .await;
    a.sync(&server).await.unwrap();
    b.sync(&server).await.unwrap();
    // The second device edits the note twice, in two envelopes, before the first sends its own
    // edit of it, and then edits it again before reading that.
    b.run("UPDATE notes SET body = 'theirs, first'").await;
    b.sync(&server).await.unwrap();
    later();
    b.run("UPDATE notes SET body = 'theirs, second'").await;
    b.run("UPDATE books SET year = 1871").await;
    b.sync(&server).await.unwrap();
    later();
    a.run("UPDATE notes SET body = 'mine'").await;
    a.run("INSERT INTO settings VALUES ('font', 'serif')").await;
    later();
    b.run("UPDATE notes SET body = 'theirs, third'").await;
    // Collected now, so the clocks of these edits are the same in every run below.
    for peer in [&mut a, &mut b] {
        peer.replica.collect(&mut peer.conn).await.unwrap();
    }

    let mut runs = Vec::new();
    for (n, pace) in [
        one_envelope(),
        Pace::default(),
        Pace {
            apply: usize::MAX,
            ..Pace::default()
        },
    ]
    .into_iter()
    .enumerate()
    {
        let world = dir.join(format!("world{n}"));
        std::fs::create_dir_all(&world).unwrap();
        a.back_up(&world.join("a.db")).await;
        b.back_up(&world.join("b.db")).await;
        runs.push(exchanged(pace, &world, &server, &creds).await);
    }
    let (conflicts, _, _) = &runs[0];
    assert!(conflicts[0].contains("theirs, second"), "{conflicts:#?}");
    assert!(!conflicts[0].contains("theirs, first"), "{conflicts:#?}");
    assert!(
        conflicts[1].contains("remote: Text(\"mine\")"),
        "{conflicts:#?}"
    );
    assert_eq!(runs[1], runs[0]);
    assert_eq!(runs[2], runs[0]);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn caught_up_that_fails_keeps_the_pull_and_sends_nothing_until_it_succeeds() {
    let server = Server::new();
    let [mut a, mut b] = crash_scene(&server).await;
    let id = a.credentials.library_id();
    let mut hook = Marking {
        fail_caught_up: true,
        ..Marking::default()
    };
    let result = sync_hooked(&mut a, &server, &mut hook).await;
    assert!(matches!(result, Err(Error::Application(_))), "{result:?}");
    assert_eq!((hook.applies, hook.caught_up), (1, 1));
    assert_eq!(a.settings().await, ["theme=dark"]);
    assert_eq!(marked(&mut a, "dark").await, 1);
    assert_eq!(a.cursor().await, 1);
    assert!(settled(&mut a).await.is_empty());
    assert_eq!(server.log(id).len(), 1);
    assert_durable(&mut a, "caught_up failed").await;

    hook.fail_caught_up = false;
    let report = sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    assert_eq!((hook.applies, hook.caught_up), (2, 2));
    assert_eq!(report.received, 0);
    assert!(report.sent > 0);
    assert_eq!(settled(&mut a).await, [2]);
    assert_eq!(server.log(id).len(), 2);
    assert_durable(&mut a, "recovered").await;
    b.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn caught_up_is_called_once_by_each_sync_that_reads_to_the_head() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let mut hook = Marking::default();
    let report = sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    assert_eq!((report.received, hook.applies, hook.caught_up), (0, 1, 1));

    server.with(|s| s.caps.max_page_batches = Some(1));
    for n in 0..3 {
        b.run(&format!("INSERT INTO settings VALUES ('b{n}', 'x')"))
            .await;
        b.sync(&server).await.unwrap();
    }
    a.run("INSERT INTO settings VALUES ('a', 'x')").await;
    let report = sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    assert_eq!(report.received, 9);
    assert_eq!(hook.caught_up, 2);
    assert_eq!(settled(&mut a).await, [1, 2]);
}

#[tokio::test]
async fn caught_up_waits_while_the_pull_is_blocked_at_an_unlisted_signer() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    let stranger = generate_signing_key().unwrap();
    let mut c = Peer::new(Credentials::new(keys.clone(), stranger.clone(), 0)).await;
    c.run("INSERT INTO settings VALUES ('stranger', 'x')").await;
    c.replica.collect(&mut c.conn).await.unwrap();
    let outbox = c.replica.outbox(&mut c.conn, 100).await.unwrap();
    server.inject(
        keys.id,
        seal(
            &outbox.changes,
            &keys,
            0,
            &stranger,
            random_nonce().unwrap(),
        ),
    );
    let mut hook = Marking::default();
    for _ in 0..2 {
        let report = sync_hooked(&mut a, &server, &mut hook).await.unwrap();
        assert!(report.blocked.is_some());
    }
    assert_eq!((hook.applies, hook.caught_up), (2, 0));

    server.register(keys.id, stranger.verifying_key());
    let report = sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    assert_eq!(report.blocked, None);
    assert_eq!(hook.caught_up, 1);
    assert_eq!(a.settings().await, ["stranger=x"]);
    b.sync(&server).await.unwrap();
}

#[tokio::test]
async fn caught_up_waits_while_a_page_fails() {
    let server = Server::new();
    server.with(|s| s.caps.max_page_batches = Some(1));
    let [mut a, mut b] = pair(&server).await;
    for n in 0..3 {
        b.run(&format!("INSERT INTO settings VALUES ('b{n}', 'x')"))
            .await;
        b.sync(&server).await.unwrap();
    }
    // Capabilities, the first page, the device list, the second page, then the third fails.
    server.fail_at(5);
    let mut hook = Marking::default();
    assert!(matches!(
        sync_hooked(&mut a, &server, &mut hook).await,
        Err(Error::Transport(_))
    ));
    assert_eq!((hook.applies, hook.caught_up), (1, 0));
    assert_eq!(a.settings().await, ["b0=x", "b1=x"]);
    server.behave();
    sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    assert_eq!(hook.caught_up, 1);
    assert_eq!(a.settings().await, ["b0=x", "b1=x", "b2=x"]);
}

#[tokio::test]
async fn caught_up_waits_while_the_page_bound_leaves_more_to_read() {
    let server = Server::new();
    server.with(|s| s.caps.max_page_batches = Some(1));
    let [mut a, _b] = pair(&server).await;
    let id = a.credentials.library_id();
    // One call reads at most a thousand pages.
    for _ in 0..1_001 {
        server.inject(id, b"not an envelope".to_vec());
    }
    let mut hook = Marking::default();
    let report = sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    assert!(report.more);
    assert_eq!(a.cursor().await, 1_000);
    assert_eq!(hook.caught_up, 0);
    let report = sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    assert!(!report.more);
    assert_eq!(a.cursor().await, 1_001);
    assert_eq!(hook.caught_up, 1);
}

#[tokio::test]
async fn what_caught_up_applies_is_merged_into_the_report_and_sent() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    b.run(
        "CREATE TRIGGER no_drafts BEFORE INSERT ON books WHEN NEW.title = 'draft'
         BEGIN SELECT RAISE(ABORT, 'no drafts'); END",
    )
    .await;
    a.run("INSERT INTO books (title) VALUES ('draft')").await;
    a.sync(&server).await.unwrap();
    let book = a.replica.gid_of(&mut a.conn, "books", 1).await.unwrap();

    let mut hook = Marking {
        settle: Some("DROP TRIGGER no_drafts; INSERT INTO settings VALUES ('settled', 'x')"),
        ..Marking::default()
    };
    let report = sync_hooked(&mut b, &server, &mut hook).await.unwrap();
    assert_eq!(hook.caught_up, 1);
    assert_eq!(report.received, 2);
    // The pull's apply held the book back; the apply after settling wrote it, and that is what
    // the report says.
    assert_eq!((report.applied.waiting, report.applied.held.len()), (0, 0));
    let touched: Vec<_> = report
        .applied
        .touched
        .iter()
        .map(|t| {
            (
                t.collection.as_str(),
                t.key.as_str(),
                Some(t.gid),
                t.deleted,
            )
        })
        .collect();
    assert_eq!(touched, [("books", "[1]", book, false)]);
    assert_eq!(report.sent, 3);
    a.sync(&server).await.unwrap();
    assert_eq!(a.settings().await, ["settled=x"]);
    assert_converged(&mut [a, b]).await;
}

const PRESENT: &str = "CREATE TABLE present (
    collection TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY (collection, key))";
const COUNTS: &str = "CREATE TABLE counts (collection TEXT PRIMARY KEY, n INTEGER NOT NULL)";
const CAUGHT_UP: &str = "CREATE TABLE caught_up (n INTEGER NOT NULL)";
const ROWS: &str = "SELECT 'books', json_array(id) FROM books
    UNION ALL SELECT 'notes', json_array(id) FROM notes
    UNION ALL SELECT 'settings', json_array(key) FROM settings";

/// Keeps a count of rows per collection, from what each apply touched, and counts its calls once
/// a pull is complete.
#[derive(Default)]
struct Counting {
    caught_up: usize,
    /// Fail the next apply, at this point.
    fail: Option<Fail>,
    /// End the next apply's transaction once the counts are written, and then give up on it.
    abandon: Option<Abandon>,
    /// Arm the commit hook once the counts are written, so the client's commit fails.
    fail_commit: Option<Arc<AtomicBool>>,
    pace: Pace,
    /// Fail the `between` call with this number, counting from 1.
    fail_between: Option<usize>,
    betweens: usize,
}

#[derive(Clone, Debug)]
enum Abandon {
    /// Panic.
    Panics,
    /// Never return; the flag tells the test to drop the sync.
    Hangs(Arc<Signal>),
}

impl Apply for Counting {
    async fn apply(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        changes: &[Change],
    ) -> Result<Applied, Error> {
        if self.fail.take_if(|f| *f == Fail::BeforeApplying).is_some() {
            return Err(failed("before applying"));
        }
        let applied = replica.apply(conn, changes).await?;
        // Between the rows and the counts kept from them, which only one transaction keeps true.
        if self.fail.take().is_some() {
            return Err(failed("after applying"));
        }
        for touched in &applied.touched {
            let sql = if touched.deleted {
                "DELETE FROM present WHERE collection = ? AND key = ?"
            } else {
                "INSERT OR IGNORE INTO present VALUES (?, ?)"
            };
            let moved = sqlx::query(sql)
                .bind(&touched.collection)
                .bind(&touched.key)
                .execute(&mut *conn)
                .await?
                .rows_affected() as i64;
            let by = if touched.deleted { -moved } else { moved };
            sqlx::query(
                "INSERT INTO counts VALUES (?, ?)
                 ON CONFLICT (collection) DO UPDATE SET n = n + excluded.n",
            )
            .bind(&touched.collection)
            .bind(by)
            .execute(&mut *conn)
            .await?;
        }
        if let Some(armed) = self.fail_commit.take() {
            armed.store(true, Ordering::SeqCst);
        }
        if let Some(abandon) = self.abandon.take() {
            end_the_transaction(conn).await?;
            match abandon {
                Abandon::Panics => panic!("abandoned after ending the transaction"),
                Abandon::Hangs(hung) => {
                    hung.raise();
                    std::future::pending::<()>().await;
                }
            }
        }
        Ok(applied)
    }

    async fn caught_up(
        &mut self,
        conn: &mut SqliteConnection,
        _replica: &Replica,
    ) -> Result<Option<Applied>, Error> {
        self.caught_up += 1;
        sqlx::query("INSERT INTO caught_up VALUES (1)")
            .execute(&mut *conn)
            .await?;
        Ok(None)
    }

    async fn between(&mut self, _: &mut SqliteConnection) -> Result<(), Error> {
        self.betweens += 1;
        if self.fail_between == Some(self.betweens) {
            return Err(failed("between"));
        }
        Ok(())
    }

    fn pace(&self) -> Pace {
        self.pace
    }
}

/// A local write, and the application bringing its counts up to date for it.
async fn write(peer: &mut Peer, sql: &str) {
    peer.run(sql).await;
    peer.run("DELETE FROM present").await;
    peer.run(&format!("INSERT INTO present {ROWS}")).await;
    peer.run("DELETE FROM counts").await;
    peer.run("INSERT INTO counts SELECT collection, count(*) FROM present GROUP BY collection")
        .await;
}

async fn assert_counted(peer: &mut Peer, at: &str) {
    let mut actual: Vec<(String, String)> = sqlx::query_as(ROWS)
        .fetch_all(&mut peer.conn)
        .await
        .unwrap();
    actual.sort();
    let present: Vec<(String, String)> =
        sqlx::query_as("SELECT collection, key FROM present ORDER BY collection, key")
            .fetch_all(&mut peer.conn)
            .await
            .unwrap();
    assert_eq!(present, actual, "{at}");
    let mut want: BTreeMap<String, i64> = BTreeMap::new();
    for (collection, _) in &actual {
        *want.entry(collection.clone()).or_default() += 1;
    }
    let counts: BTreeMap<String, i64> =
        sqlx::query_as::<_, (String, i64)>("SELECT collection, n FROM counts WHERE n != 0")
            .fetch_all(&mut peer.conn)
            .await
            .unwrap()
            .into_iter()
            .collect();
    assert_eq!(counts, want, "{at}");
}

async fn counted_sync(peer: &mut Peer, server: &Server, pace: Pace, at: &str) -> SyncReport {
    let mut hook = Counting {
        pace,
        ..Counting::default()
    };
    let report = sync_hooked(peer, server, &mut hook).await.unwrap();
    assert_counted(peer, at).await;
    report
}

#[tokio::test]
async fn three_devices_keep_counts_from_what_each_apply_touched() {
    let server = Server::new();
    server.with(|s| s.caps.max_page_batches = Some(2));
    let [made, joined] = credentials(&server).await;
    let key = generate_signing_key().unwrap();
    fongo_sync_client::add_device(&server, &made, &key.verifying_key(), now)
        .await
        .unwrap();
    let third = Credentials::new(made.library().clone(), key, 0);
    let mut peers = [
        file_peer(made).await,
        file_peer(joined).await,
        file_peer(third).await,
    ];
    for peer in &mut peers {
        peer.run(PRESENT).await;
        peer.run(COUNTS).await;
        peer.run(CAUGHT_UP).await;
        // A second entity with a title takes the local id of the one it replaced.
        peer.run("CREATE UNIQUE INDEX books_title ON books (title)")
            .await;
    }

    let script: &[(usize, &str)] = &[
        (0, "INSERT INTO books (title) VALUES ('Middlemarch')"),
        (2, "INSERT INTO books (title) VALUES ('Persuasion')"),
        (1, "INSERT INTO settings VALUES ('theme', 'dark')"),
        (2, "INSERT INTO settings VALUES ('theme', 'light')"),
        (0, "UPDATE books SET year = 1871"),
        (
            1,
            "INSERT INTO notes (book_id, body) SELECT id, 'shared' FROM books",
        ),
        (2, "DELETE FROM books WHERE title = 'Persuasion'"),
        (0, "INSERT INTO settings VALUES ('font', 'serif')"),
        (1, "DELETE FROM books WHERE title = 'Middlemarch'"),
        (1, "INSERT INTO books (title) VALUES ('Middlemarch')"),
        (
            2,
            "INSERT INTO notes (book_id, body) SELECT id, 'late' FROM books",
        ),
        (0, "DELETE FROM settings WHERE key = 'theme'"),
    ];
    for (step, &(who, sql)) in script.iter().enumerate() {
        write(&mut peers[who], sql).await;
        for other in [(who + 1) % 3, who, (who + 2) % 3] {
            let at = format!("step {step}, device {other}");
            counted_sync(&mut peers[other], &server, Pace::default(), &at).await;
        }
    }
    for round in 0..10 {
        let mut moved = false;
        for (n, peer) in peers.iter_mut().enumerate() {
            let at = format!("round {round}, device {n}");
            let report = counted_sync(peer, &server, Pace::default(), &at).await;
            moved |= report.sent > 0 || report.received > 0;
        }
        if !moved {
            break;
        }
        assert!(round < 9, "the peers never stopped sending");
    }
    let state = assert_converged(&mut peers).await;
    assert!(
        state.iter().any(|l| l.contains("Middlemarch")),
        "{state:#?}"
    );
    for (n, peer) in peers.iter_mut().enumerate() {
        let at = format!("device {n} at the end");
        assert_counted(peer, &at).await;
        assert_durable(peer, &at).await;
    }
}

/// Ends the transaction it is given, as SQLite does for `INSERT OR ROLLBACK` on a conflict, and
/// swallows the error: in its next `applies` applies, and once the pull is complete when
/// `in_caught_up`.
struct Swallowing {
    applies: usize,
    in_caught_up: bool,
    /// Return an error once it has ended the transaction, instead of carrying on.
    fails: bool,
}

async fn end_the_transaction(conn: &mut SqliteConnection) -> Result<(), Error> {
    sqlx::query("INSERT OR IGNORE INTO once VALUES (1)")
        .execute(&mut *conn)
        .await?;
    let ended = sqlx::query("INSERT OR ROLLBACK INTO once VALUES (1)")
        .execute(&mut *conn)
        .await;
    assert!(ended.is_err());
    Ok(())
}

impl Apply for Swallowing {
    async fn apply(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        changes: &[Change],
    ) -> Result<Applied, Error> {
        let applied = replica.apply(conn, changes).await?;
        if self.applies > 0 {
            self.applies -= 1;
            end_the_transaction(conn).await?;
            if self.fails {
                return Err(failed("ended"));
            }
        }
        Ok(applied)
    }

    async fn caught_up(
        &mut self,
        conn: &mut SqliteConnection,
        _replica: &Replica,
    ) -> Result<Option<Applied>, Error> {
        if self.in_caught_up {
            end_the_transaction(conn).await?;
            if self.fails {
                return Err(failed("ended"));
            }
        }
        Ok(None)
    }
}

/// The error a sync whose step ended its transaction returns: the step's own, or the client's
/// when the step hid what happened.
fn ended(fails: bool, result: &Result<SyncReport, Error>) -> bool {
    if fails {
        matches!(result, Err(Error::Application(_)))
    } else {
        matches!(result, Err(Error::TransactionEnded))
    }
}

#[tokio::test]
async fn an_apply_that_ends_the_transaction_fails_the_sync_and_moves_no_cursor() {
    for fails in [false, true] {
        let server = Server::new();
        let [mut a, mut b] = crash_scene(&server).await;
        let id = a.credentials.library_id();
        let mut hook = Swallowing {
            applies: 1,
            in_caught_up: false,
            fails,
        };
        let result = sync_hooked(&mut a, &server, &mut hook).await;
        assert!(ended(fails, &result), "{fails}: {result:?}");
        assert_eq!(a.cursor().await, 0, "{fails}");
        assert!(a.settings().await.is_empty(), "{fails}");
        assert_eq!(server.log(id).len(), 1, "{fails}");
        assert_durable(&mut a, &format!("{fails}: the transaction ended")).await;

        let report = a.sync(&server).await.unwrap();
        assert_eq!(report.received, 3, "{fails}");
        assert_eq!(a.settings().await, ["theme=dark"], "{fails}");
        assert_eq!(a.cursor().await, 2, "{fails}");
        assert_durable(&mut a, &format!("{fails}: the same connection synced")).await;
        b.sync(&server).await.unwrap();
        assert_converged(&mut [a, b]).await;
    }
}

#[tokio::test]
async fn caught_up_that_ends_the_transaction_fails_the_sync_and_sends_nothing() {
    for fails in [false, true] {
        let server = Server::new();
        let [mut a, mut b] = crash_scene(&server).await;
        let id = a.credentials.library_id();
        let mut hook = Swallowing {
            applies: 0,
            in_caught_up: true,
            fails,
        };
        let result = sync_hooked(&mut a, &server, &mut hook).await;
        assert!(ended(fails, &result), "{fails}: {result:?}");
        assert_eq!(a.cursor().await, 1, "{fails}");
        assert_eq!(a.settings().await, ["theme=dark"], "{fails}");
        assert_eq!(server.log(id).len(), 1, "{fails}");
        assert_durable(&mut a, &format!("{fails}: the transaction ended")).await;

        let report = a.sync(&server).await.unwrap();
        assert!(report.sent > 0, "{fails}");
        assert_eq!(server.log(id).len(), 2, "{fails}");
        assert_durable(&mut a, &format!("{fails}: the same connection synced")).await;
        b.sync(&server).await.unwrap();
        assert_converged(&mut [a, b]).await;
    }
}

#[tokio::test]
async fn an_apply_that_ends_the_transaction_part_way_through_a_pull_is_tried_once_more() {
    for fails in [false, true] {
        for applies in [1, 2] {
            let at = format!("fails={fails} applies={applies}");
            let server = Server::new();
            let [mut a, mut b] = behind_its_own_append(&server).await;
            let mut hook = Swallowing {
                applies,
                in_caught_up: false,
                fails,
            };
            let result = sync_hooked(&mut a, &server, &mut hook).await;
            assert!(ended(fails, &result), "{at}: {result:?}");
            // The apply at its own envelope ended the transaction; the client applied what it
            // held once more, which kept it when that apply left the transaction alone.
            let kept = applies == 1;
            assert_eq!(a.cursor().await, if kept { 3 } else { 1 }, "{at}");
            let mut want = vec!["first=x", "mine=x"];
            if kept {
                want.push("stranger=x");
            }
            assert_eq!(a.settings().await, want, "{at}");
            assert_durable(&mut a, &at).await;

            a.sync(&server).await.unwrap();
            assert_eq!(
                a.settings().await,
                ["first=x", "last=x", "mine=x", "stranger=x"],
                "{at}"
            );
            assert_durable(&mut a, &format!("{at}, the same connection synced")).await;
            b.sync(&server).await.unwrap();
            assert_converged(&mut [a, b]).await;
        }
    }
}

#[tokio::test]
async fn after_a_transaction_ends_inside_a_step_later_syncs_reach_the_file() {
    let server = Server::new();
    let [mut a, mut b] = crash_scene(&server).await;
    let mut swallowing = Swallowing {
        applies: 1,
        in_caught_up: false,
        fails: false,
    };
    let result = sync_hooked(&mut a, &server, &mut swallowing).await;
    assert!(matches!(result, Err(Error::TransactionEnded)), "{result:?}");
    let mut failing = Marking {
        fail: Some((Fail::AfterApplying, 1)),
        ..Marking::default()
    };
    let result = sync_hooked(&mut a, &server, &mut failing).await;
    assert!(matches!(result, Err(Error::Application(_))), "{result:?}");
    assert_durable(&mut a, "after two failed syncs").await;
    a.sync(&server).await.unwrap();
    assert_eq!(a.settings().await, ["theme=dark"]);
    assert_durable(&mut a, "after a plain sync").await;
    b.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn a_row_whose_write_ends_the_transaction_fails_the_sync_and_leaves_the_connection_sound() {
    let server = Server::new();
    let [mut a, mut b] = crash_scene(&server).await;
    a.run(
        "CREATE TRIGGER no_dark BEFORE INSERT ON settings WHEN NEW.value = 'dark'
         BEGIN SELECT RAISE(ROLLBACK, 'no dark'); END",
    )
    .await;
    let result = a.sync(&server).await;
    assert!(result.is_err(), "{result:?}");
    assert_eq!(a.cursor().await, 0);
    assert!(a.settings().await.is_empty());
    assert_durable(&mut a, "the kernel's apply ended the transaction").await;

    a.run("DROP TRIGGER no_dark").await;
    a.sync(&server).await.unwrap();
    assert_eq!(a.settings().await, ["theme=dark"]);
    assert_durable(&mut a, "recovered").await;
    b.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn an_acknowledgment_that_ends_the_transaction_fails_the_sync_and_leaves_the_connection_sound()
 {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    a.sync(&server).await.unwrap();
    a.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    a.run(
        "CREATE TEMP TRIGGER ends BEFORE INSERT ON fongo_client_appended
         BEGIN SELECT RAISE(ROLLBACK, 'ends'); END",
    )
    .await;
    // The server stores the envelope; recording where it landed ends the transaction.
    assert!(a.sync(&server).await.is_err());
    assert_eq!(server.log(id).len(), 1);
    assert_durable(&mut a, "the acknowledgment ended the transaction").await;

    a.run("DROP TRIGGER temp.ends").await;
    let report = a.sync(&server).await.unwrap();
    assert_eq!(server.log(id).len(), 1);
    assert_eq!(report.sent, 3);
    assert_durable(&mut a, "recovered").await;
    b.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
}

/// Rolls back to the client's savepoint, or releases it, after applying.
struct Undoing(&'static str);

impl Apply for Undoing {
    async fn apply(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        changes: &[Change],
    ) -> Result<Applied, Error> {
        let applied = replica.apply(conn, changes).await?;
        sqlx::query(self.0).execute(&mut *conn).await?;
        Ok(applied)
    }
}

#[tokio::test]
async fn a_step_that_rolls_back_or_releases_the_clients_savepoint_fails_the_sync() {
    for sql in [
        "ROLLBACK TO fongo_client_watch",
        "RELEASE fongo_client_watch",
    ] {
        let server = Server::new();
        let [mut a, mut b] = pair(&server).await;
        // A sync before, so what the client keeps for its savepoint outlasts the transaction.
        sync_hooked(&mut a, &server, &mut ()).await.unwrap();
        b.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
        b.sync(&server).await.unwrap();
        let result = sync_hooked(&mut a, &server, &mut Undoing(sql)).await;
        assert!(
            matches!(result, Err(Error::TransactionEnded)),
            "{sql}: {result:?}"
        );
        assert_eq!(a.cursor().await, 0, "{sql}");
        assert!(a.settings().await.is_empty(), "{sql}");
        assert_durable(&mut a, sql).await;
        a.sync(&server).await.unwrap();
        b.sync(&server).await.unwrap();
        assert_converged(&mut [a, b]).await;
    }
}

/// Ends the transaction after applying, then returns, fails, or panics.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Then {
    Returns,
    Fails,
    Panics,
}

impl Apply for Then {
    async fn apply(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        changes: &[Change],
    ) -> Result<Applied, Error> {
        let applied = replica.apply(conn, changes).await?;
        end_the_transaction(conn).await?;
        match self {
            Then::Returns => Ok(applied),
            Then::Fails => Err(failed("ended")),
            Then::Panics => panic!("ended"),
        }
    }
}

/// The path of the database file `conn` has open.
async fn file_of(conn: &mut SqliteConnection) -> String {
    sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name = 'main'")
        .fetch_one(&mut *conn)
        .await
        .unwrap()
}

/// Closes the peer's connection and opens a fresh one to its file, as an application does with a
/// connection a sync reports unsound.
async fn reconnect(peer: &mut Peer) {
    let file = file_of(&mut peer.conn).await;
    let fresh = SqliteConnectOptions::new()
        .filename(&file)
        .foreign_keys(true)
        .connect()
        .await
        .unwrap();
    let old = std::mem::replace(&mut peer.conn, fresh);
    let _ = old.close().await;
    peer.replica.install(&mut peer.conn).await.unwrap();
}

/// After the first device's sync was given up on: syncs on the same connection either succeed
/// with what they wrote on the file or are refused as unsound, and are never anything else. Once
/// refused, a fresh connection to the file syncs, and the two devices converge with nothing lost.
async fn on_or_after_reconnecting(peers: &mut [Peer; 2], server: &Server, at: &str) {
    let mut refused = false;
    for n in 0..3 {
        match peers[0].sync(server).await {
            Ok(_) => {
                let at = format!("{at}, sync {n} on the same connection");
                assert_durable(&mut peers[0], &at).await;
            }
            Err(Error::ConnectionUnsound { .. }) => {
                refused = true;
                break;
            }
            Err(other) => panic!("{at}, sync {n}: {other:?}"),
        }
    }
    if refused {
        reconnect(&mut peers[0]).await;
    }
    for who in [0, 1, 0] {
        peers[who].sync(server).await.unwrap();
    }
    let a = &mut peers[0];
    assert_eq!(a.settings().await, ["theme=dark"], "{at}");
    assert_eq!(
        a.lines("SELECT title FROM books").await,
        ["Middlemarch"],
        "{at}"
    );
    assert_durable(a, &format!("{at}, at the end")).await;
    assert_converged(peers).await;
}

#[tokio::test]
async fn a_step_that_ends_the_transaction_and_panics_leaves_a_connection_the_next_sync_refuses() {
    let server = Server::new();
    let mut peers = crash_scene(&server).await;
    let panicked = CatchUnwind(Box::pin(sync_hooked(
        &mut peers[0],
        &server,
        &mut Then::Panics,
    )))
    .await;
    assert!(panicked.is_err());
    let result = peers[0].sync(&server).await;
    assert!(
        matches!(result, Err(Error::ConnectionUnsound { .. })),
        "{result:?}"
    );
    on_or_after_reconnecting(&mut peers, &server, "panicked").await;

    // The same for a sync dropped right there, as a timeout drops one.
    let server = Server::new();
    let mut peers = crash_scene(&server).await;
    let ended = Arc::new(Signal::default());
    let mut hook = Counting {
        abandon: Some(Abandon::Hangs(ended.clone())),
        ..Counting::default()
    };
    for peer in &mut peers {
        peer.run(PRESENT).await;
        peer.run(COUNTS).await;
        peer.run(CAUGHT_UP).await;
    }
    tokio::select! {
        result = sync_hooked(&mut peers[0], &server, &mut hook) => panic!("{result:?}"),
        () = ended.raised() => {}
    }
    let result = peers[0].sync(&server).await;
    assert!(
        matches!(result, Err(Error::ConnectionUnsound { .. })),
        "{result:?}"
    );
    on_or_after_reconnecting(&mut peers, &server, "dropped").await;

    // The same in memory, where a sync is refused just as well but nothing outlives the
    // connection.
    let server = Server::new();
    let [made, joined] = credentials(&server).await;
    let (mut a, mut b) = (Peer::new(made).await, Peer::new(joined).await);
    for peer in [&mut a, &mut b] {
        peer.run(MARKS).await;
        peer.run(ONCE).await;
    }
    b.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    b.sync(&server).await.unwrap();
    let panicked = CatchUnwind(Box::pin(sync_hooked(&mut a, &server, &mut Then::Panics))).await;
    assert!(panicked.is_err());
    let result = a.sync(&server).await;
    assert!(
        matches!(result, Err(Error::ConnectionUnsound { .. })),
        "{result:?}"
    );
}

/// A copy of the server's log and of both databases, so each drop point starts from the same
/// scene without building it again.
async fn copy_scene(server: &Server, peers: &mut [Peer; 2]) -> (Server, [Peer; 2]) {
    let mut copies = Vec::new();
    for peer in peers.iter_mut() {
        let file = file_of(&mut peer.conn).await;
        let copy = Path::new(&file)
            .with_extension(format!("{}.db", COPIES.fetch_add(1, Ordering::Relaxed)));
        std::fs::copy(&file, &copy).unwrap();
        copies.push(Peer::open(&copy, peer.credentials.clone()).await);
    }
    let b = copies.pop().unwrap();
    let a = copies.pop().unwrap();
    (copy_of(server), [a, b])
}

static COPIES: AtomicUsize = AtomicUsize::new(0);

/// Drops the sync at each of its suspensions in turn, from a copy of one scene each time. An
/// await suspends unless sqlx's worker has already answered when it is first polled, so on a busy
/// machine a run can suspend less often and a given drop point move or go unvisited; on an idle
/// one every suspension is visited. The drop right after a step ends the transaction, which is
/// the one that leaves the connection unsound, is covered on its own by
/// `a_step_that_ends_the_transaction_and_panics_leaves_a_connection_the_next_sync_refuses`.
#[tokio::test]
async fn a_sync_dropped_anywhere_around_an_ended_transaction_loses_nothing() {
    // Far more than the sync's suspensions, which are about one per statement.
    const LIMIT: usize = 200;
    for then in [Then::Fails, Then::Returns] {
        let server = Server::new();
        let mut scene = crash_scene(&server).await;
        for k in 1.. {
            assert!(
                k <= LIMIT,
                "{then:?}: the sync suspended {LIMIT} times and had not ended"
            );
            let (server, mut peers) = copy_scene(&server, &mut scene).await;
            let mut hook = then;
            let ended = drive_to(sync_hooked(&mut peers[0], &server, &mut hook), k).await;
            let at = format!("{then:?} dropped at suspension {k}");
            on_or_after_reconnecting(&mut peers, &server, &at).await;
            if ended {
                break;
            }
        }
    }
}

/// sqlx counts a transaction SQLite does not have, as a sync given up on can leave it.
async fn miscount(peer: &mut Peer) {
    let mut tx = peer.conn.begin().await.unwrap();
    sqlx::query("ROLLBACK").execute(&mut *tx).await.unwrap();
    std::mem::forget(tx);
}

#[tokio::test]
async fn a_miscounted_connection_is_refused_and_a_fresh_one_syncs() {
    let server = Server::new();
    let mut peers = crash_scene(&server).await;
    miscount(&mut peers[0]).await;
    let requests = server.with(|s| s.requests.len());
    let result = peers[0].sync(&server).await;
    assert!(
        matches!(result, Err(Error::ConnectionUnsound { .. })),
        "{result:?}"
    );
    assert_eq!(server.with(|s| s.requests.len()), requests);
    on_or_after_reconnecting(&mut peers, &server, "miscounted").await;
}

#[tokio::test]
async fn a_sync_inside_a_transaction_is_refused_and_leaves_it_whole() {
    for step in [None, Some(Then::Fails)] {
        let server = Server::new();
        let [mut a, mut b] = crash_scene(&server).await;
        let requests = server.with(|s| s.requests.len());
        let mut tx = a.conn.begin().await.unwrap();
        sqlx::query("INSERT INTO marks VALUES ('c', 'g', 'f', 'k', 'callers')")
            .execute(&mut *tx)
            .await
            .unwrap();
        let result = match step {
            None => sync(&mut tx, &a.replica, &server, &a.credentials, now).await,
            Some(mut then) => {
                sync_with(&mut tx, &a.replica, &server, &a.credentials, now, &mut then).await
            }
        };
        assert!(
            matches!(result, Err(Error::ConnectionUnsound { .. })),
            "{result:?}"
        );
        assert_eq!(server.with(|s| s.requests.len()), requests);
        tx.commit().await.unwrap();
        assert_eq!(marked(&mut a, "callers").await, 1);
        assert_eq!(a.cursor().await, 0);
        assert_durable(&mut a, "the caller committed").await;
        a.sync(&server).await.unwrap();
        b.sync(&server).await.unwrap();
        assert_converged(&mut [a, b]).await;
    }

    // Transactions opened with a statement, which sqlx does not count.
    for begin in [
        "BEGIN",
        "BEGIN IMMEDIATE",
        "BEGIN EXCLUSIVE",
        "SAVEPOINT callers",
    ] {
        let server = Server::new();
        let [mut a, mut b] = crash_scene(&server).await;
        let requests = server.with(|s| s.requests.len());
        a.run(begin).await;
        a.run("INSERT INTO marks VALUES ('c', 'g', 'f', 'k', 'callers')")
            .await;
        let result = a.sync(&server).await;
        assert!(
            matches!(result, Err(Error::ConnectionUnsound { .. })),
            "{begin}: {result:?}"
        );
        assert_eq!(server.with(|s| s.requests.len()), requests, "{begin}");
        a.run(if begin.starts_with("SAVEPOINT") {
            "RELEASE callers"
        } else {
            "COMMIT"
        })
        .await;
        assert_eq!(marked(&mut a, "callers").await, 1, "{begin}");
        assert_durable(&mut a, begin).await;
        a.sync(&server).await.unwrap();
        b.sync(&server).await.unwrap();
        assert_converged(&mut [a, b]).await;
    }
}

/// After a panicked sync, the application begins a transaction of its own and drops it: SQLite is
/// left inside a transaction nothing will end, so its later writes, even outside any transaction,
/// never reach the file. The next sync refuses the connection, and a fresh one is the remedy.
#[tokio::test]
async fn using_a_connection_a_panicked_sync_left_loses_writes_until_it_is_replaced() {
    let server = Server::new();
    let mut peers = crash_scene(&server).await;
    let a = &mut peers[0];
    let panicked = CatchUnwind(Box::pin(sync_hooked(a, &server, &mut Then::Panics))).await;
    assert!(panicked.is_err());
    drop(a.conn.begin().await.unwrap());
    a.run("INSERT INTO marks VALUES ('c', 'g', 'f', 'k', 'meanwhile')")
        .await;
    assert_eq!(marked(a, "meanwhile").await, 1);
    let file = file_of(&mut a.conn).await;
    let mut other = SqliteConnectOptions::new()
        .filename(&file)
        .connect()
        .await
        .unwrap();
    let on_file: i64 = sqlx::query_scalar("SELECT count(*) FROM marks WHERE value = 'meanwhile'")
        .fetch_one(&mut other)
        .await
        .unwrap();
    assert_eq!(on_file, 0);
    drop(other);
    let result = a.sync(&server).await;
    assert!(
        matches!(result, Err(Error::ConnectionUnsound { .. })),
        "{result:?}"
    );
    reconnect(a).await;
    assert_eq!(marked(a, "meanwhile").await, 0);
    on_or_after_reconnecting(&mut peers, &server, "used after a panic").await;
}

async fn changes_and_level(conn: &mut SqliteConnection) -> (i64, i64, Vec<String>) {
    let changes: i64 = sqlx::query_scalar("SELECT total_changes()")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    let level: i64 = sqlx::query_scalar("PRAGMA main.synchronous")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    (changes, level, everything(conn).await)
}

#[tokio::test]
async fn checking_a_sound_connection_writes_nothing() {
    let server = Server::new();
    let [mut a, _b] = crash_scene(&server).await;
    a.sync(&server).await.unwrap();
    let before = changes_and_level(&mut a.conn).await;
    fongo_sync_client::check_connection(&mut a.conn)
        .await
        .unwrap();
    assert_eq!(changes_and_level(&mut a.conn).await, before);
}

#[tokio::test]
async fn a_pooled_connection_a_panicked_sync_left_is_refused_and_detached() {
    let server = Server::new();
    let [a, mut b] = crash_scene(&server).await;
    let Peer {
        mut conn,
        replica,
        credentials,
    } = a;
    let file = file_of(&mut conn).await;
    drop(conn);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .filename(&file)
                .foreign_keys(true),
        )
        .await
        .unwrap();
    {
        let mut conn = pool.acquire().await.unwrap();
        let panicked = CatchUnwind(Box::pin(sync_with(
            &mut conn,
            &replica,
            &server,
            &credentials,
            now,
            &mut Then::Panics,
        )))
        .await;
        assert!(panicked.is_err());
    }
    let mut conn = pool.acquire().await.unwrap();
    let result = sync(&mut conn, &replica, &server, &credentials, now).await;
    assert!(
        matches!(result, Err(Error::ConnectionUnsound { .. })),
        "{result:?}"
    );
    let _ = conn.detach().close().await;
    let mut conn = pool.acquire().await.unwrap();
    sync(&mut conn, &replica, &server, &credentials, now)
        .await
        .unwrap();
    let settings: Vec<String> = sqlx::query_scalar("SELECT key FROM settings")
        .fetch_all(&mut *conn)
        .await
        .unwrap();
    assert_eq!(settings, ["theme"]);
    assert_durable_on(&mut conn, "pooled").await;
    b.sync(&server).await.unwrap();
}

/// Fails the next commit on `conn` once `armed` is set, as SQLite does when a commit hook refuses:
/// the commit becomes a rollback.
async fn fail_commits(conn: &mut SqliteConnection, armed: Arc<AtomicBool>) {
    // As the adapter does: a `lock_handle` straight after another can leave sqlx an unlock to
    // answer later, and enough of those deadlock the connection.
    conn.ping().await.unwrap();
    conn.lock_handle()
        .await
        .unwrap()
        .set_commit_hook(move || !armed.swap(false, Ordering::SeqCst));
}

/// Ways a step can leave its sync's connection unsound and still return.
#[derive(Clone, Copy, Debug)]
enum Leaves {
    /// Ends the transaction inside a savepoint of its own, and returns the error its commit gives.
    EndedInsideItsSavepoint,
    /// Leaves a savepoint of its own behind.
    ItsSavepoint,
    /// Arms the commit hook, so the client's commit becomes a rollback.
    AFailingCommit,
}

struct Leaving {
    what: Leaves,
    in_caught_up: bool,
    armed: Arc<AtomicBool>,
}

impl Leaving {
    async fn leave(&self, conn: &mut SqliteConnection) -> Result<(), Error> {
        match self.what {
            Leaves::EndedInsideItsSavepoint => {
                let mut savepoint = conn.begin().await?;
                end_the_transaction(&mut savepoint).await?;
                savepoint.commit().await?;
            }
            Leaves::ItsSavepoint => std::mem::forget(conn.begin().await?),
            Leaves::AFailingCommit => self.armed.store(true, Ordering::SeqCst),
        }
        Ok(())
    }
}

impl Apply for Leaving {
    async fn apply(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        changes: &[Change],
    ) -> Result<Applied, Error> {
        let applied = replica.apply(conn, changes).await?;
        if !self.in_caught_up {
            self.leave(conn).await?;
        }
        Ok(applied)
    }

    async fn caught_up(
        &mut self,
        conn: &mut SqliteConnection,
        _replica: &Replica,
    ) -> Result<Option<Applied>, Error> {
        if self.in_caught_up {
            self.leave(conn).await?;
        }
        Ok(None)
    }
}

/// Carries requests to the server, arming the commit hook as an append goes out, so the commit
/// that acknowledges it fails.
struct ArmsOnAppend<'a> {
    server: &'a Server,
    armed: Arc<AtomicBool>,
}

impl Transport for ArmsOnAppend<'_> {
    async fn send(&self, request: Request) -> Result<Response, TransportError> {
        if request.method == Method::Post {
            self.armed.store(true, Ordering::SeqCst);
        }
        self.server.send(request).await
    }
}

#[tokio::test]
async fn a_sync_whose_failure_left_the_connection_unsound_says_so_and_keeps_the_cause() {
    let mut cases: Vec<(String, Option<Leaving>)> = Vec::new();
    for what in [
        Leaves::EndedInsideItsSavepoint,
        Leaves::ItsSavepoint,
        Leaves::AFailingCommit,
    ] {
        for in_caught_up in [false, true] {
            let leaving = Leaving {
                what,
                in_caught_up,
                armed: Arc::new(AtomicBool::new(false)),
            };
            cases.push((
                format!("{what:?} in_caught_up={in_caught_up}"),
                Some(leaving),
            ));
        }
    }
    cases.push(("a failing acknowledgment".into(), None));
    for (at, step) in cases {
        let server = Server::new();
        let mut peers = crash_scene(&server).await;
        let keys = peers[0].credentials.library().clone();
        let armed = match &step {
            Some(leaving) => leaving.armed.clone(),
            None => Arc::new(AtomicBool::new(false)),
        };
        fail_commits(&mut peers[0].conn, armed.clone()).await;
        let a = &mut peers[0];
        let result = match step {
            Some(mut leaving) => sync_hooked(a, &server, &mut leaving).await,
            None => {
                let arming = ArmsOnAppend {
                    server: &server,
                    armed,
                };
                sync(&mut a.conn, &a.replica, &arming, &a.credentials, now).await
            }
        };
        match &result {
            Err(Error::ConnectionUnsound {
                source: Some(cause),
            }) => {
                assert!(matches!(**cause, Error::Database(_)), "{at}: {cause:?}");
            }
            other => panic!("{at}: {other:?}"),
        }
        let next = a.sync(&server).await;
        assert!(
            matches!(next, Err(Error::ConnectionUnsound { source: None })),
            "{at}: {next:?}"
        );
        on_or_after_reconnecting(&mut peers, &server, &at).await;
        assert_no_change_sent_twice(&server, &keys);
    }
}

#[tokio::test]
async fn a_copy_of_an_envelope_gives_its_changes_again_and_touches_nothing() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    a.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    a.sync(&server).await.unwrap();
    let mut hook = Marking::default();
    let first = sync_hooked(&mut b, &server, &mut hook).await.unwrap();
    assert_eq!(first.applied.touched.len(), 1);
    assert_eq!(marked(&mut b, "dark").await, 1);

    // A server that stores an identical envelope twice, which the client does not rely on.
    let copy = server.log(id)[0].clone();
    server.inject(id, copy);
    let again = sync_hooked(&mut b, &server, &mut hook).await.unwrap();
    assert_eq!(again.received, first.received);
    assert!(again.applied.touched.is_empty(), "{again:#?}");
    assert_eq!(marked(&mut b, "dark").await, 2);
    assert_eq!(b.settings().await, ["theme=dark"]);
    assert_converged(&mut [a, b]).await;
}

/// Statements a random step runs on one device.
const WRITES: &[&str] = &[
    "INSERT OR IGNORE INTO books (title) VALUES ('Middlemarch')",
    "INSERT OR IGNORE INTO books (title) VALUES ('Persuasion')",
    "INSERT OR REPLACE INTO settings VALUES ('theme', 'dark')",
    "INSERT OR REPLACE INTO settings VALUES ('theme', 'light')",
    "INSERT OR REPLACE INTO settings VALUES ('font', 'serif')",
    "DELETE FROM settings WHERE key = 'theme'",
    "UPDATE books SET year = coalesce(year, 1870) + 1",
    "UPDATE books SET year = 1871",
    "INSERT INTO notes (book_id, body) SELECT id, 'n' FROM books LIMIT 1",
    "DELETE FROM books WHERE title = 'Persuasion'",
    "DELETE FROM books WHERE title = 'Middlemarch'",
    "DELETE FROM notes WHERE id = (SELECT min(id) FROM notes)",
];

/// A small xorshift generator, so a case is a function of its seed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Polls a future, turning a panic inside it into `Err`.
struct CatchUnwind<F>(Pin<Box<F>>);

impl<F: Future> Future for CatchUnwind<F> {
    type Output = Result<F::Output, ()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future = self.0.as_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| future.poll(cx))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        }
    }
}

/// Raised once, waking whatever waits for it.
#[derive(Debug, Default)]
struct Signal {
    raised: AtomicBool,
    waiting: Mutex<Option<Waker>>,
}

impl Signal {
    fn raise(&self) {
        self.raised.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waiting.lock().unwrap().take() {
            waker.wake();
        }
    }

    async fn raised(&self) {
        std::future::poll_fn(|cx| {
            *self.waiting.lock().unwrap() = Some(cx.waker().clone());
            if self.raised.load(Ordering::SeqCst) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.raise();
    }
}

/// Polls `future` until it completes or has suspended `stop` times, and then drops it. After each
/// suspension it waits to be woken before polling again, so each suspension counts once however
/// long the wait. Whether an await suspends at all depends on whether sqlx's worker thread has
/// answered by the time it is first polled, so a busy machine can see fewer. Returns whether the
/// future completed.
async fn drive_to<F: Future>(future: F, stop: usize) -> bool {
    let mut future = std::pin::pin!(future);
    let woken = Arc::new(Signal::default());
    let waker = Waker::from(woken.clone());
    for _ in 0..stop {
        woken.raised.store(false, Ordering::SeqCst);
        if future
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
        {
            return true;
        }
        woken.raised().await;
    }
    false
}

/// Carries requests to the server and notes whether any was an append.
struct Noting<'a> {
    server: &'a Server,
    appended: AtomicBool,
}

impl Transport for Noting<'_> {
    async fn send(&self, request: Request) -> Result<Response, TransportError> {
        if request.method == Method::Post {
            self.appended.store(true, Ordering::Relaxed);
        }
        self.server.send(request).await
    }
}

async fn assert_clean(peer: &mut Peer, at: &str) {
    assert_clean_on(&mut peer.conn, at).await;
}

/// Three devices write at random and sync through a server that fails, loses or hangs a random
/// request, stores copies of envelopes already in the log, holds envelopes from signers it lists
/// only later, sometimes holds more than one sync reads, and pages one to three envelopes at a
/// time. Each case picks how many changes an apply takes and an envelope holds, how many rows a
/// step of collecting takes, and whether the sync pauses. Their applies fail at random before or after applying, or end the transaction and
/// then panic or never return, after which the sync is given up on, or make the commit after them
/// fail; a gap between transactions sometimes fails too. After every sync the counts kept
/// from what each apply touched are right, and `caught_up` was called exactly when the pull read
/// to the head, and never by a sync that failed before it appended. A sync that returned leaves the
/// connection sound unless it says otherwise; one given up on may not, and the device then opens a
/// fresh connection, at once or when its next sync refuses the old one. At the end every device agrees, and a second
/// connection reads what each wrote.
///
/// `FONGO_CLIENT_CASES` sets how many seeds run from `FONGO_CLIENT_SEED`. A seed fixes the steps;
/// keys and ids are still drawn afresh, so a failure names its seed but may need a few runs.
#[tokio::test]
async fn devices_with_random_writes_failures_and_copies_keep_counts_and_converge() {
    let first = env("FONGO_CLIENT_SEED", 1);
    for seed in first..first + env("FONGO_CLIENT_CASES", 6) {
        let mut rng = Rng::new(seed);
        let server = Server::new();
        let page = 1 + seed % 3;
        server.with(|s| s.caps.max_page_batches = Some(page as u32));
        let pace = Pace {
            apply: [0, 1, 2, 3, 5, 4_000, usize::MAX][rng.below(7) as usize],
            collect: [0, 1, 2, 3, 1_000, usize::MAX][(seed % 6) as usize],
            seal: [1, 2, 3, 1_000][rng.below(4) as usize],
            hold: Duration::ZERO,
            pause: Duration::from_millis(rng.below(2)),
            held: Duration::ZERO,
        };
        let [made, joined] = credentials(&server).await;
        let key = generate_signing_key().unwrap();
        fongo_sync_client::add_device(&server, &made, &key.verifying_key(), now)
            .await
            .unwrap();
        let third = Credentials::new(made.library().clone(), key, 0);
        let keys = made.library().clone();
        let id = keys.id;
        let mut peers = [
            file_peer(made).await,
            file_peer(joined).await,
            file_peer(third).await,
        ];
        for peer in &mut peers {
            peer.run(PRESENT).await;
            peer.run(COUNTS).await;
            peer.run(CAUGHT_UP).await;
            peer.run(ONCE).await;
        }
        let armed: Vec<Arc<AtomicBool>> =
            (0..3).map(|_| Arc::new(AtomicBool::new(false))).collect();
        for (peer, armed) in peers.iter_mut().zip(&armed) {
            fail_commits(&mut peer.conn, armed.clone()).await;
        }
        let mut unlisted: Vec<SigningKey> = Vec::new();
        let mut strangers = 0;
        let mut long = false;
        for step in 0..40 {
            let who = rng.below(3) as usize;
            let at = format!("seed {seed} step {step} device {who}");
            match rng.below(10) {
                0..=2 => {
                    let sql = WRITES[rng.below(WRITES.len() as u64) as usize];
                    write(&mut peers[who], sql).await;
                }
                3 => {
                    let log = server.log(id);
                    if !log.is_empty() {
                        let copy = log[rng.below(log.len() as u64) as usize].clone();
                        server.inject(id, copy);
                    }
                }
                4 if rng.below(2) == 0 => {
                    let stranger = generate_signing_key().unwrap();
                    let name = format!("stranger{strangers}");
                    strangers += 1;
                    server.inject(id, sealed_setting(&keys, &stranger, &name).await);
                    unlisted.push(stranger);
                }
                4 => {
                    for stranger in unlisted.drain(..) {
                        server.register(id, stranger.verifying_key());
                    }
                }
                // More than one sync reads, once in a case at most: a thousand pages.
                5 if page == 1 && !long && rng.below(3) == 0 => {
                    long = true;
                    for _ in 0..1_001 {
                        server.inject(id, b"not an envelope".to_vec());
                    }
                }
                5 => {}
                _ => {
                    let n = 1 + rng.below(8) as usize;
                    match rng.below(4) {
                        0 => server.fail_at(n),
                        1 => server.lose_at(n),
                        2 => server.hang_at(n, rng.below(2) == 0),
                        _ => {}
                    }
                    let hung = Arc::new(Signal::default());
                    let mut hook = Counting {
                        caught_up: 0,
                        fail: match rng.below(4) {
                            0 => Some(Fail::BeforeApplying),
                            1 => Some(Fail::AfterApplying),
                            _ => None,
                        },
                        abandon: None,
                        fail_commit: None,
                        pace,
                        fail_between: (rng.below(6) == 0).then(|| 1 + rng.below(4) as usize),
                        betweens: 0,
                    };
                    match rng.below(8) {
                        0 => hook.abandon = Some(Abandon::Panics),
                        1 => hook.abandon = Some(Abandon::Hangs(hung.clone())),
                        2 => hook.fail_commit = Some(armed[who].clone()),
                        _ => {}
                    }
                    let noting = Noting {
                        server: &server,
                        appended: AtomicBool::new(false),
                    };
                    let peer = &mut peers[who];
                    let before = peer
                        .lines("SELECT CAST(count(*) AS TEXT) FROM caught_up")
                        .await;
                    let result = tokio::select! {
                        result = CatchUnwind(Box::pin(sync_with(
                            &mut peer.conn, &peer.replica, &noting, &peer.credentials, now,
                            &mut hook,
                        ))) => result.ok(),
                        () = server.hung() => None,
                        () = hung.raised() => None,
                    };
                    let abandoned = result.is_none();
                    let refused = matches!(result, Some(Err(Error::ConnectionUnsound { .. })));
                    let gap_failed = hook.fail_between.is_some_and(|n| hook.betweens >= n);
                    match result {
                        Some(Ok(report)) => {
                            let complete = report.blocked.is_none() && !report.more;
                            assert_eq!(hook.caught_up, usize::from(complete), "{at}");
                        }
                        // Appends come after `caught_up`, so a sync that failed or was dropped
                        // before one never called it, unless the gap after it failed.
                        _ if !noting.appended.load(Ordering::Relaxed) && !gap_failed => {
                            assert_eq!(hook.caught_up, 0, "{at}: {result:?}");
                        }
                        _ => assert!(hook.caught_up <= 1, "{at}"),
                    }
                    server.behave();
                    // A step that failed or was given up on before the commit leaves it unarmed.
                    armed[who].store(false, Ordering::SeqCst);
                    // No request comes between `caught_up` and its commit, so a call is kept.
                    let after = peers[who]
                        .lines("SELECT CAST(count(*) AS TEXT) FROM caught_up")
                        .await;
                    let kept =
                        after[0].parse::<usize>().unwrap() - before[0].parse::<usize>().unwrap();
                    assert_eq!(kept, hook.caught_up, "{at}");
                    // A sync given up on after its transaction ended may leave the connection
                    // unsound. The device opens a fresh one when a sync refuses it, or at once.
                    if refused || (abandoned && rng.below(2) == 0) {
                        if fongo_sync_client::check_connection(&mut peers[who].conn)
                            .await
                            .is_err()
                        {
                            reconnect(&mut peers[who]).await;
                            fail_commits(&mut peers[who].conn, armed[who].clone()).await;
                        }
                    } else if !abandoned {
                        fongo_sync_client::check_connection(&mut peers[who].conn)
                            .await
                            .unwrap_or_else(|e| panic!("{at}: a sync that returned: {e:?}"));
                        assert_clean(&mut peers[who], &at).await;
                    }
                    assert_counted(&mut peers[who], &at).await;
                }
            }
        }
        for stranger in unlisted.drain(..) {
            server.register(id, stranger.verifying_key());
        }
        for (peer, armed) in peers.iter_mut().zip(&armed) {
            if fongo_sync_client::check_connection(&mut peer.conn)
                .await
                .is_err()
            {
                reconnect(peer).await;
                fail_commits(&mut peer.conn, armed.clone()).await;
            }
        }
        for round in 0.. {
            assert!(round < 12, "seed {seed}: the devices never stopped sending");
            let mut moved = false;
            for (n, peer) in peers.iter_mut().enumerate() {
                let at = format!("seed {seed} round {round} device {n}");
                let report = counted_sync(peer, &server, pace, &at).await;
                moved |= report.sent > 0 || report.received > 0 || report.more;
            }
            if !moved {
                break;
            }
        }
        assert_converged(&mut peers).await;
        for (n, peer) in peers.iter_mut().enumerate() {
            assert_durable(peer, &format!("seed {seed} device {n} at the end")).await;
        }
    }
}

fn assert_send<F: Future + Send>(_: F) {}

#[allow(dead_code)]
fn every_future_is_send(
    conn: &mut SqliteConnection,
    replica: &Replica,
    server: &Server,
    credentials: &Credentials,
    hook: &mut Marking,
) {
    assert_send(sync_with(conn, replica, server, credentials, now, hook));
}

#[allow(dead_code)]
fn takes_any_transport<T: Transport>(
    conn: &mut SqliteConnection,
    replica: &Replica,
    transport: &T,
    credentials: &Credentials,
) {
    assert_send(sync_with(
        conn,
        replica,
        transport,
        credentials,
        now,
        &mut (),
    ));
}

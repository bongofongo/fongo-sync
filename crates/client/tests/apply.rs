#[allow(dead_code)]
mod support;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use fongo_sync_client::protocol::{LibraryKeys, SigningKey};
use fongo_sync_client::protocol::{generate_signing_key, open, random_nonce, seal};
use fongo_sync_client::sqlite::core::Change;
use fongo_sync_client::sqlite::{Applied, Replica};
use fongo_sync_client::{
    Apply, Credentials, Error, Method, Request, Response, SyncReport, Transport, TransportError,
    create_library, join_library, sync_with,
};
use sqlx::sqlite::SqliteConnectOptions;
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
    drop(peer.conn.begin().await.unwrap());
    let begun = sqlx::query("BEGIN").execute(&mut peer.conn).await;
    assert!(begun.is_ok(), "{at}: still in a transaction: {begun:?}");
    sqlx::query("ROLLBACK")
        .execute(&mut peer.conn)
        .await
        .unwrap();
    let file: String =
        sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name = 'main'")
            .fetch_one(&mut peer.conn)
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
        everything(&mut peer.conn).await,
        "{at}"
    );
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
    }
    peers
}

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
    for lose in [false, true] {
        for n in 1..=5 {
            let server = Server::new();
            let [mut a, mut b] = crash_scene(&server).await;
            if lose {
                server.lose_at(n);
            } else {
                server.fail_at(n);
            }
            let mut hook = Marking::default();
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
    for handled in [false, true] {
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
            let mut hook = Marking::default();
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
                let at = format!("handled={handled} n={n} i={i}");
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
    for write in CLIENT_WRITES {
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
                "CREATE TEMP TRIGGER crash BEFORE {write} BEGIN SELECT RAISE(ABORT, 'crash'); END"
            )),
            ..Marking::default()
        };
        let result = sync_hooked(&mut a, &server, &mut hook).await;
        assert!(
            matches!(result, Err(Error::Database(_))),
            "{write}: {result:?}"
        );
        assert_eq!((hook.applies, hook.caught_up), (2, 0), "{write}");
        assert_eq!(a.cursor().await, 1, "{write}");
        assert_eq!(a.settings().await, Vec::<String>::new(), "{write}");
        assert_eq!(marked(&mut a, "stranger").await, 0, "{write}");
        assert_durable(&mut a, write).await;

        hook.after = None;
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
    for at in [Fail::BeforeApplying, Fail::AfterApplying] {
        let server = Server::new();
        let [mut a, mut b] = crash_scene(&server).await;
        let id = a.credentials.library_id();
        let mut hook = Marking {
            fail: Some((at, 1)),
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
    let server = Server::new();
    let [mut a, mut b] = behind_its_own_append(&server).await;
    let mut hook = Marking {
        fail: Some((Fail::AfterApplying, 1)),
        ..Marking::default()
    };
    let result = sync_hooked(&mut a, &server, &mut hook).await;
    assert!(matches!(result, Err(Error::Application(_))), "{result:?}");
    // The apply at its own envelope failed and was rolled back; the sync stopped reading, applied
    // what it held once more, and kept that.
    assert_eq!((hook.applies, hook.caught_up), (2, 0));
    assert_eq!(a.cursor().await, 3);
    assert_eq!(a.settings().await, ["first=x", "mine=x", "stranger=x"]);
    assert_eq!(marked(&mut a, "stranger").await, 1);
    assert_durable(&mut a, "after the retry").await;

    sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    assert_eq!(hook.caught_up, 1);
    assert_durable(&mut a, "recovered").await;
    assert_eq!(
        a.settings().await,
        ["first=x", "last=x", "mine=x", "stranger=x"]
    );
    b.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
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

async fn counted_sync(peer: &mut Peer, server: &Server, at: &str) -> SyncReport {
    let report = sync_hooked(peer, server, &mut Counting::default())
        .await
        .unwrap();
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
            counted_sync(&mut peers[other], &server, &at).await;
        }
    }
    for round in 0..10 {
        let mut moved = false;
        for (n, peer) in peers.iter_mut().enumerate() {
            let report = counted_sync(peer, &server, &format!("round {round}, device {n}")).await;
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
        }
        Ok(None)
    }
}

#[tokio::test]
async fn an_apply_that_ends_the_transaction_and_hides_it_fails_the_sync_and_moves_no_cursor() {
    let server = Server::new();
    let [mut a, mut b] = crash_scene(&server).await;
    let id = a.credentials.library_id();
    a.run("CREATE TABLE once (id INTEGER PRIMARY KEY)").await;
    let mut hook = Swallowing {
        applies: 1,
        in_caught_up: false,
    };
    let result = sync_hooked(&mut a, &server, &mut hook).await;
    assert!(matches!(result, Err(Error::TransactionEnded)), "{result:?}");
    assert_eq!(a.cursor().await, 0);
    assert!(a.settings().await.is_empty());
    assert_eq!(server.log(id).len(), 1);
    assert_durable(&mut a, "the transaction ended").await;

    let report = a.sync(&server).await.unwrap();
    assert_eq!(report.received, 3);
    assert_eq!(a.settings().await, ["theme=dark"]);
    assert_eq!(a.cursor().await, 2);
    assert_durable(&mut a, "recovered").await;
    b.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn caught_up_that_ends_the_transaction_and_hides_it_fails_the_sync_and_sends_nothing() {
    let server = Server::new();
    let [mut a, mut b] = crash_scene(&server).await;
    let id = a.credentials.library_id();
    a.run("CREATE TABLE once (id INTEGER PRIMARY KEY)").await;
    let mut hook = Swallowing {
        applies: 0,
        in_caught_up: true,
    };
    let result = sync_hooked(&mut a, &server, &mut hook).await;
    assert!(matches!(result, Err(Error::TransactionEnded)), "{result:?}");
    assert_eq!(a.cursor().await, 1);
    assert_eq!(a.settings().await, ["theme=dark"]);
    assert_eq!(server.log(id).len(), 1);
    assert_durable(&mut a, "the transaction ended").await;

    let report = a.sync(&server).await.unwrap();
    assert!(report.sent > 0);
    assert_eq!(server.log(id).len(), 2);
    assert_durable(&mut a, "recovered").await;
    b.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn an_apply_that_ends_the_transaction_part_way_through_a_pull_is_tried_once_more() {
    for applies in [1, 2] {
        let server = Server::new();
        let [mut a, mut b] = behind_its_own_append(&server).await;
        a.run("CREATE TABLE once (id INTEGER PRIMARY KEY)").await;
        let mut hook = Swallowing {
            applies,
            in_caught_up: false,
        };
        let result = sync_hooked(&mut a, &server, &mut hook).await;
        assert!(
            matches!(result, Err(Error::TransactionEnded)),
            "{applies}: {result:?}"
        );
        // The apply at its own envelope ended the transaction; the client applied what it held
        // once more, which kept it when that apply left the transaction alone.
        let kept = applies == 1;
        assert_eq!(a.cursor().await, if kept { 3 } else { 1 }, "{applies}");
        let mut want = vec!["first=x", "mine=x"];
        if kept {
            want.push("stranger=x");
        }
        assert_eq!(a.settings().await, want, "{applies}");
        assert_durable(&mut a, &format!("{applies}")).await;

        a.sync(&server).await.unwrap();
        assert_eq!(
            a.settings().await,
            ["first=x", "last=x", "mine=x", "stranger=x"],
            "{applies}"
        );
        assert_durable(&mut a, &format!("{applies}, recovered")).await;
        b.sync(&server).await.unwrap();
        assert_converged(&mut [a, b]).await;
    }
}

#[tokio::test]
async fn after_a_transaction_ends_inside_a_step_later_syncs_reach_the_file() {
    let server = Server::new();
    let [mut a, mut b] = crash_scene(&server).await;
    a.run("CREATE TABLE once (id INTEGER PRIMARY KEY)").await;
    let mut swallowing = Swallowing {
        applies: 1,
        in_caught_up: false,
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

/// sqlx and SQLite agree that the connection is in no transaction.
async fn assert_clean(peer: &mut Peer, at: &str) {
    drop(peer.conn.begin().await.unwrap());
    let begun = sqlx::query("BEGIN").execute(&mut peer.conn).await;
    assert!(begun.is_ok(), "{at}: still in a transaction: {begun:?}");
    sqlx::query("ROLLBACK")
        .execute(&mut peer.conn)
        .await
        .unwrap();
}

/// Three devices write at random and sync through a server that fails, loses or hangs a random
/// request, stores copies of envelopes already in the log, holds envelopes from signers it lists
/// only later, sometimes holds more than one sync reads, and pages one to three envelopes at a
/// time; their applies fail at random before or after applying. After every sync the counts kept
/// from what each apply touched are right, the connection is in no transaction, and `caught_up`
/// was called exactly when the pull read to the head, and never by a sync that failed before it
/// appended. At the end every device agrees, and a second connection reads what each wrote.
///
/// `FONGO_CLIENT_CASES` sets how many seeds run from `FONGO_CLIENT_SEED`. A seed fixes the steps;
/// keys and ids are still drawn afresh, so a failure names its seed but may need a few runs.
#[tokio::test]
async fn devices_with_random_writes_failures_and_copies_keep_counts_and_converge() {
    let first = env("FONGO_CLIENT_SEED", 1);
    for seed in first..first + env("FONGO_CLIENT_CASES", 15) {
        let mut rng = Rng::new(seed);
        let server = Server::new();
        let page = 1 + seed % 3;
        server.with(|s| s.caps.max_page_batches = Some(page as u32));
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
                    let mut hook = Counting {
                        caught_up: 0,
                        fail: match rng.below(4) {
                            0 => Some(Fail::BeforeApplying),
                            1 => Some(Fail::AfterApplying),
                            _ => None,
                        },
                    };
                    let noting = Noting {
                        server: &server,
                        appended: AtomicBool::new(false),
                    };
                    let peer = &mut peers[who];
                    let before = peer
                        .lines("SELECT CAST(count(*) AS TEXT) FROM caught_up")
                        .await;
                    let result = tokio::select! {
                        result = sync_with(
                            &mut peer.conn, &peer.replica, &noting, &peer.credentials, now,
                            &mut hook,
                        ) => Some(result),
                        () = server.hung() => None,
                    };
                    match result {
                        Some(Ok(report)) => {
                            let complete = report.blocked.is_none() && !report.more;
                            assert_eq!(hook.caught_up, usize::from(complete), "{at}");
                        }
                        // Appends come after `caught_up`, so a sync that failed or was dropped
                        // before one never called it.
                        _ if !noting.appended.load(Ordering::Relaxed) => {
                            assert_eq!(hook.caught_up, 0, "{at}: {result:?}");
                        }
                        _ => assert!(hook.caught_up <= 1, "{at}"),
                    }
                    server.behave();
                    // No request comes between `caught_up` and its commit, so a call is kept.
                    let after = peers[who]
                        .lines("SELECT CAST(count(*) AS TEXT) FROM caught_up")
                        .await;
                    let kept =
                        after[0].parse::<usize>().unwrap() - before[0].parse::<usize>().unwrap();
                    assert_eq!(kept, hook.caught_up, "{at}");
                    assert_clean(&mut peers[who], &at).await;
                    assert_counted(&mut peers[who], &at).await;
                }
            }
        }
        for stranger in unlisted.drain(..) {
            server.register(id, stranger.verifying_key());
        }
        for round in 0.. {
            assert!(round < 12, "seed {seed}: the devices never stopped sending");
            let mut moved = false;
            for (n, peer) in peers.iter_mut().enumerate() {
                let at = format!("seed {seed} round {round} device {n}");
                let report = counted_sync(peer, &server, &at).await;
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

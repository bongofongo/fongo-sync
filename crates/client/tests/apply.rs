#[allow(dead_code)]
mod support;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::path::Path;

use fongo_sync_client::protocol::{generate_signing_key, open, random_nonce, seal};
use fongo_sync_client::sqlite::core::Change;
use fongo_sync_client::sqlite::{Applied, Replica};
use fongo_sync_client::{
    Apply, Credentials, Error, SyncReport, Transport, create_library, join_library, sync_with,
};
use sqlx::SqliteConnection;
use support::{Library, Peer, Server, assert_converged, assert_no_change_sent_twice, now, scratch};

async fn credentials(server: &Server) -> [Credentials; 2] {
    let made = create_library(server, now).await.unwrap();
    let joined = join_library(server, &made.recovery_code(), now)
        .await
        .unwrap();
    [made, joined]
}

async fn pair(server: &Server) -> [Peer; 2] {
    let [made, joined] = credentials(server).await;
    let mut peers = [Peer::new(made).await, Peer::new(joined).await];
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
/// once, and no change was marked twice.
async fn marked_once(peer: &mut Peer, server: &Server, signer: &Credentials, envelopes: usize) {
    let keys = peer.credentials.library().clone();
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

            server.behave();
            for _ in 0..3 {
                sync_hooked(&mut a, &server, &mut hook).await.unwrap();
                b.sync(&server).await.unwrap();
            }
            let signer = b.credentials.clone();
            marked_once(&mut a, &server, &signer, 1).await;
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

            server.behave();
            for _ in 0..3 {
                sync_hooked(&mut a, &server, &mut hook).await.unwrap();
                b.sync(&server).await.unwrap();
            }
            let signer = b.credentials.clone();
            marked_once(&mut a, &server, &signer, 3).await;
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

        let report = sync_hooked(&mut a, &server, &mut hook).await.unwrap();
        assert_eq!(report.received, 3, "{at:?}");
        assert!(report.sent > 0, "{at:?}");
        assert_eq!(a.settings().await, ["theme=dark"], "{at:?}");
        assert_eq!(marked(&mut a, "dark").await, 1, "{at:?}");
        assert_eq!(a.cursor().await, 2, "{at:?}");
        b.sync(&server).await.unwrap();
        assert_converged(&mut [a, b]).await;
    }
}

/// The first device has an append past its cursor that it has not read back, and an envelope
/// from a device it has not listed yet before it, so its next pull applies at its own envelope
/// and again at the end.
#[tokio::test]
async fn an_apply_that_fails_part_way_through_a_pull_is_tried_once_more_before_the_error() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let keys = a.credentials.library().clone();
    b.run("INSERT INTO settings VALUES ('first', 'x')").await;
    b.sync(&server).await.unwrap();
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
    a.run("INSERT INTO settings VALUES ('mine', 'x')").await;
    let blocked = a.sync(&server).await.unwrap();
    assert_eq!(blocked.blocked.map(|b| b.seq), Some(2));
    assert_eq!(a.cursor().await, 1);
    b.run("INSERT INTO settings VALUES ('last', 'x')").await;
    b.sync(&server).await.unwrap();
    server.register(keys.id, stranger.verifying_key());

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

    sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    assert_eq!(hook.caught_up, 1);
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

    hook.fail_caught_up = false;
    let report = sync_hooked(&mut a, &server, &mut hook).await.unwrap();
    assert_eq!((hook.applies, hook.caught_up), (2, 2));
    assert_eq!(report.received, 0);
    assert!(report.sent > 0);
    assert_eq!(settled(&mut a).await, [2]);
    assert_eq!(server.log(id).len(), 2);
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
const ROWS: &str = "SELECT 'books', json_array(id) FROM books
    UNION ALL SELECT 'notes', json_array(id) FROM notes
    UNION ALL SELECT 'settings', json_array(key) FROM settings";

/// Keeps a count of rows per collection, from what each apply touched.
struct Counting;

impl Apply for Counting {
    async fn apply(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        changes: &[Change],
    ) -> Result<Applied, Error> {
        let applied = replica.apply(conn, changes).await?;
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
    let report = sync_hooked(peer, server, &mut Counting).await.unwrap();
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
        Peer::new(made).await,
        Peer::new(joined).await,
        Peer::new(third).await,
    ];
    for peer in &mut peers {
        peer.run(PRESENT).await;
        peer.run(COUNTS).await;
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
        assert_counted(peer, &format!("device {n} at the end")).await;
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

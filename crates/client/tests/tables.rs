//! The client's own tables: layouts 1, which recorded no layout, and 2 are upgraded in place; a
//! build of layout 1 fails on later tables, and one of layout 2 refuses them by their recorded
//! layout; and this build refuses a later layout.

#[allow(dead_code)]
mod support;

use fongo_sync_client::protocol::api::CAPABILITIES_PATH;
use fongo_sync_client::{
    Apply, Error, LogFault, Method, create_library, fingerprint, join_library, sync_with,
};
use support::{Peer, Server, assert_converged, now};

async fn pair(server: &Server) -> [Peer; 2] {
    let made = create_library(server, now).await.unwrap();
    let joined = join_library(server, &made.recovery_code(), now)
        .await
        .unwrap();
    [Peer::new(made).await, Peer::new(joined).await]
}

/// The tables as a build of layout 1 makes them.
const LAYOUT_ONE: [&str; 2] = [
    "CREATE TABLE IF NOT EXISTS fongo_client_state (
    library BLOB PRIMARY KEY,
    cursor INTEGER NOT NULL DEFAULT 0,
    through INTEGER,
    envelope BLOB)",
    "CREATE TABLE IF NOT EXISTS fongo_client_appended (
    library BLOB NOT NULL,
    seq INTEGER NOT NULL,
    through INTEGER NOT NULL,
    PRIMARY KEY (library, seq))",
];

/// A row of `fongo_client_state` that layout 1 has columns for.
type StateRow = (Vec<u8>, i64, Option<i64>, Option<Vec<u8>>);

/// Puts the client's rows into tables of layout 1, as a build of that layout would have left them.
async fn as_layout_one(peer: &mut Peer) {
    let state: Vec<StateRow> = sqlx::query_as(
        "SELECT library, cursor, sending_through, sending_envelope FROM fongo_client_state",
    )
    .fetch_all(&mut peer.conn)
    .await
    .unwrap();
    let appended: Vec<(Vec<u8>, i64, i64)> =
        sqlx::query_as("SELECT library, seq, through FROM fongo_client_appended")
            .fetch_all(&mut peer.conn)
            .await
            .unwrap();
    for table in ["meta", "state", "appended", "passed"] {
        peer.run(&format!("DROP TABLE fongo_client_{table}")).await;
    }
    for sql in LAYOUT_ONE {
        peer.run(sql).await;
    }
    for (library, cursor, through, envelope) in state {
        sqlx::query("INSERT INTO fongo_client_state VALUES (?, ?, ?, ?)")
            .bind(library)
            .bind(cursor)
            .bind(through)
            .bind(envelope)
            .execute(&mut peer.conn)
            .await
            .unwrap();
    }
    for (library, seq, through) in appended {
        sqlx::query("INSERT INTO fongo_client_appended VALUES (?, ?, ?)")
            .bind(library)
            .bind(seq)
            .bind(through)
            .execute(&mut peer.conn)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn tables_of_layout_one_are_upgraded_in_place_and_a_build_of_it_then_fails() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    b.run("INSERT INTO settings VALUES ('b1', 'x')").await;
    b.sync(&server).await.unwrap();
    a.sync(&server).await.unwrap();
    // An append whose answer is lost leaves its envelope recorded.
    a.run("INSERT INTO settings VALUES ('a1', 'x')").await;
    let mut once = true;
    server.lose_if(move |r| r.method == Method::Post && std::mem::take(&mut once));
    assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));
    b.run("INSERT INTO settings VALUES ('b2', 'x')").await;
    b.sync(&server).await.unwrap();

    as_layout_one(&mut a).await;
    assert_eq!(a.cursor().await, 1);
    a.sync(&server).await.unwrap();
    assert_eq!(
        a.lines("SELECT CAST(layout AS TEXT) FROM fongo_client_meta")
            .await,
        ["3"]
    );
    // The recorded envelope was found in the log, not sent again.
    assert_eq!(server.log(id).len(), 3);
    assert_eq!(a.cursor().await, 3);

    // A layout-1 build's sync asks for capabilities and adds a row for a library that has none,
    // then reads these columns, and would later write them; both now fail, so it stops at the
    // read, before it reads the log or writes anything else.
    for sql in [
        "SELECT cursor, through, envelope FROM fongo_client_state WHERE library = ?",
        "UPDATE fongo_client_state SET through = NULL, envelope = NULL WHERE library = ?",
    ] {
        let failed = sqlx::query(sql).bind(&id.0[..]).execute(&mut a.conn).await;
        assert!(failed.is_err(), "{sql}");
    }
    b.sync(&server).await.unwrap();
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn tables_of_a_later_layout_are_refused_before_reaching_the_server() {
    let server = Server::new();
    let [mut a, _b] = pair(&server).await;
    a.sync(&server).await.unwrap();
    a.run("UPDATE fongo_client_meta SET layout = 4").await;
    let asked = server.count(Method::Get, CAPABILITIES_PATH);
    let refused = a.sync(&server).await;
    assert!(
        matches!(
            refused,
            Err(Error::NewerClientTables { found: 4, known: 3 })
        ),
        "{refused:?}"
    );
    assert_eq!(server.count(Method::Get, CAPABILITIES_PATH), asked);
    let id = a.credentials.library_id();
    assert!(matches!(
        fongo_sync_client::cursor(&mut a.conn, id).await,
        Err(Error::NewerClientTables { .. })
    ));
}

/// The tables as a build of layout 2 makes them, and its record of their layout.
const LAYOUT_TWO: [&str; 4] = [
    "CREATE TABLE IF NOT EXISTS fongo_client_meta (layout INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS fongo_client_state (
    library BLOB PRIMARY KEY,
    cursor INTEGER NOT NULL DEFAULT 0,
    sending_through INTEGER,
    sending_envelope BLOB,
    sending_after INTEGER,
    stopped_seq INTEGER,
    stopped_digest BLOB,
    passing_seq INTEGER,
    passing_digest BLOB)",
    "CREATE TABLE IF NOT EXISTS fongo_client_appended (
    library BLOB NOT NULL,
    seq INTEGER NOT NULL,
    through INTEGER NOT NULL,
    digest BLOB,
    PRIMARY KEY (library, seq))",
    "INSERT INTO fongo_client_meta (layout) VALUES (2)",
];

/// The columns of `fongo_client_state` that layout 2 has.
const STATE_TWO: &str = "library, cursor, sending_through, sending_envelope, sending_after, \
                         stopped_seq, stopped_digest, passing_seq, passing_digest";

/// Puts the client's rows into tables of layout 2, as a build of that layout would have left them.
async fn as_layout_two(peer: &mut Peer) {
    for sql in [
        "CREATE TEMP TABLE old_state AS SELECT * FROM fongo_client_state".to_string(),
        "CREATE TEMP TABLE old_appended AS SELECT * FROM fongo_client_appended".to_string(),
        "DROP TABLE fongo_client_meta".to_string(),
        "DROP TABLE fongo_client_state".to_string(),
        "DROP TABLE fongo_client_appended".to_string(),
        "DROP TABLE fongo_client_passed".to_string(),
    ]
    .into_iter()
    .chain(LAYOUT_TWO.map(String::from))
    .chain([
        format!("INSERT INTO fongo_client_state SELECT {STATE_TWO} FROM temp.old_state"),
        "INSERT INTO fongo_client_appended SELECT library, seq, through, digest
         FROM temp.old_appended"
            .to_string(),
        "DROP TABLE temp.old_state".to_string(),
        "DROP TABLE temp.old_appended".to_string(),
    ]) {
        peer.run(&sql).await;
    }
}

async fn layout_of(peer: &mut Peer) -> Vec<String> {
    peer.lines("SELECT CAST(layout AS TEXT) FROM fongo_client_meta")
        .await
}

#[tokio::test]
async fn tables_of_layout_two_are_upgraded_in_place_and_hash_the_log_read_so_far() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    for key in ["b1", "b2", "b3"] {
        b.run(&format!("INSERT INTO settings VALUES ('{key}', 'x')"))
            .await;
        b.sync(&server).await.unwrap();
    }
    a.sync(&server).await.unwrap();
    // An append whose answer is lost leaves its envelope recorded.
    a.run("INSERT INTO settings VALUES ('a1', 'x')").await;
    let mut once = true;
    server.lose_if(move |r| r.method == Method::Post && std::mem::take(&mut once));
    assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));
    server.behave();
    as_layout_two(&mut a).await;
    assert_eq!(layout_of(&mut a).await, ["2"]);
    assert_eq!(
        fingerprint(&mut a.conn, id).await.unwrap().seq,
        0,
        "layout 2 kept no fingerprint"
    );

    // A failure part-way through the upgrade leaves layout 2 as it was.
    a.run(
        "CREATE TEMP TRIGGER crash BEFORE UPDATE ON fongo_client_state
         BEGIN SELECT RAISE(ABORT, 'crash'); END",
    )
    .await;
    assert!(matches!(a.sync(&server).await, Err(Error::Database(_))));
    a.run("DROP TRIGGER temp.crash").await;
    assert_eq!(layout_of(&mut a).await, ["2"]);
    let columns = a
        .lines("SELECT name FROM pragma_table_info('fongo_client_state')")
        .await;
    assert_eq!(columns.join(", "), STATE_TWO);

    a.sync(&server).await.unwrap();
    assert_eq!(layout_of(&mut a).await, ["3"]);
    // The recorded envelope was found in the log, not sent again.
    assert_eq!(server.log(id).len(), 4);
    assert_eq!(a.cursor().await, 4);
    b.sync(&server).await.unwrap();
    let ours = fingerprint(&mut a.conn, id).await.unwrap();
    assert_eq!(ours.seq, 4);
    assert_eq!(ours, fingerprint(&mut b.conn, id).await.unwrap());
    // A build of layout 2 reads the recorded layout first and refuses any but its own.
    assert_eq!(layout_of(&mut a).await, ["3"]);
    assert_converged(&mut [a, b]).await;
}

#[tokio::test]
async fn a_log_rolled_back_before_an_upgrade_is_found_by_hashing_it() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    for key in ["b1", "b2", "b3"] {
        b.run(&format!("INSERT INTO settings VALUES ('{key}', 'x')"))
            .await;
        b.sync(&server).await.unwrap();
    }
    a.sync(&server).await.unwrap();
    as_layout_two(&mut a).await;
    server.roll_back(id, 2);
    assert!(matches!(
        a.sync(&server).await,
        Err(Error::LogFault(LogFault::Behind { cursor: 3 }))
    ));
}

/// Several connections to one database of layout 2 sync at once, each on a thread of its own: one
/// upgrades the tables and the others wait for it, in either journal mode.
#[test]
fn connections_upgrading_at_once_all_succeed() {
    let runtime = || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    };
    for wal in [false, true] {
        for round in 0..8 {
            let dir = support::scratch(&format!("upgrading-at-once-{wal}-{round}"));
            let path = dir.join("x.db");
            let server = Server::new();
            let made = runtime().block_on(async {
                let made = create_library(&server, now).await.unwrap();
                let mut a = Peer::new_at(&path, made.clone()).await;
                if wal {
                    a.run("PRAGMA journal_mode = WAL").await;
                }
                a.run("INSERT INTO settings VALUES ('a', 'x')").await;
                a.sync(&server).await.unwrap();
                as_layout_two(&mut a).await;
                made
            });
            std::thread::scope(|scope| {
                let syncs: Vec<_> = (0..3)
                    .map(|_| {
                        scope.spawn(|| {
                            runtime().block_on(async {
                                let mut peer = Peer::open(&path, made.clone()).await;
                                peer.sync(&server).await
                            })
                        })
                    })
                    .collect();
                for sync in syncs {
                    let synced = sync.join().unwrap();
                    assert!(synced.is_ok(), "wal {wal}, round {round}: {synced:?}");
                }
            });
            runtime().block_on(async {
                let mut a = Peer::open(&path, made).await;
                assert_eq!(layout_of(&mut a).await, ["3"]);
            });
        }
    }
}

/// The layout of the client's tables as a build of layout 2 reads it before anything else, its
/// statements and rule as that build has them: `Err` with the layout found when it refuses.
async fn as_layout_two_reads_it(conn: &mut sqlx::SqliteConnection) -> Result<(), (i64, i64)> {
    const LAYOUT: i64 = 2;
    let exists = |table: &'static str| {
        sqlx::query("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").bind(table)
    };
    if exists("fongo_client_meta")
        .fetch_optional(&mut *conn)
        .await
        .unwrap()
        .is_none()
    {
        return Ok(());
    }
    let found: Vec<i64> = sqlx::query_scalar("SELECT layout FROM fongo_client_meta")
        .fetch_all(&mut *conn)
        .await
        .unwrap();
    match found[..] {
        [] | [LAYOUT] => Ok(()),
        [later] if later > LAYOUT => Err((later, LAYOUT)),
        _ => panic!("no single known layout"),
    }
}

#[tokio::test]
async fn a_build_of_layout_two_refuses_upgraded_tables() {
    let server = Server::new();
    let [mut a, _b] = pair(&server).await;
    a.sync(&server).await.unwrap();
    as_layout_two(&mut a).await;
    assert_eq!(as_layout_two_reads_it(&mut a.conn).await, Ok(()));
    a.sync(&server).await.unwrap();
    assert_eq!(as_layout_two_reads_it(&mut a.conn).await, Err((3, 2)));
}

fn chain(library: [u8; 16], log: &[Vec<u8>]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let domain = b"fongo-sync/v1/log-chain\0";
    let mut at: [u8; 32] = Sha256::new()
        .chain_update(domain)
        .chain_update([0])
        .chain_update(library)
        .finalize()
        .into();
    for (i, envelope) in log.iter().enumerate() {
        at = Sha256::new()
            .chain_update(domain)
            .chain_update([1])
            .chain_update(at)
            .chain_update((i as u64 + 1).to_be_bytes())
            .chain_update(Sha256::digest(envelope))
            .finalize()
            .into();
    }
    at
}

fn page_reads(server: &Server, from: usize) -> usize {
    server.with(|s| {
        s.requests[from..]
            .iter()
            .filter(|(m, t)| *m == Method::Get && t.contains("/batches"))
            .count()
    })
}

/// The first sync after an upgrade from layout 2 hashes the log read so far page by page. A
/// request failing at any page keeps the pages hashed before it, and the next sync goes on from
/// there, reading each page once more at most.
#[tokio::test]
async fn hashing_the_log_after_an_upgrade_goes_on_from_where_a_failure_stopped_it() {
    const PAGE: u64 = 4;
    const LOG: u64 = 40;
    let server = Server::new();
    server.with(|s| s.caps.max_page_batches = Some(PAGE as u32));
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    b.run("INSERT INTO settings VALUES ('b', 'x')").await;
    b.sync(&server).await.unwrap();
    let one = server.log(id)[0].clone();
    for _ in 1..LOG {
        server.inject(id, one.clone());
    }
    while a.sync(&server).await.unwrap().more {}
    assert_eq!(a.cursor().await, LOG);
    let whole = chain(id.0, &server.log(id));
    for failing in 1..=LOG / PAGE {
        as_layout_two(&mut a).await;
        // The capabilities, then one read per page of the log.
        server.fail_at(1 + failing as usize);
        assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));
        server.behave();
        let part = fingerprint(&mut a.conn, id).await.unwrap();
        assert_eq!(part.seq, (failing - 1) * PAGE, "failing at page {failing}");
        let from = server.with(|s| s.requests.len());
        a.sync(&server).await.unwrap();
        let left = (LOG - part.seq) / PAGE;
        assert_eq!(
            page_reads(&server, from),
            left as usize + 1,
            "failing at {failing}"
        );
        let done = fingerprint(&mut a.conn, id).await.unwrap();
        assert_eq!((done.seq, done.digest), (LOG, whole));
    }
}

/// Counts the calls to `caught_up`.
#[derive(Default)]
struct Counting {
    caught_up: usize,
}

impl Apply for Counting {
    async fn caught_up(
        &mut self,
        _: &mut sqlx::SqliteConnection,
        _: &fongo_sync_client::sqlite::Replica,
    ) -> Result<Option<fongo_sync_client::sqlite::Applied>, Error> {
        self.caught_up += 1;
        Ok(None)
    }
}

/// Tables of layout 2 over a log longer than one sync hashes. `a` has read all of it and has a
/// write of its own to send.
async fn upgraded_over_a_long_log(server: &Server) -> [Peer; 2] {
    server.with(|s| s.caps.max_page_batches = Some(4));
    let [mut a, mut b] = pair(server).await;
    let id = a.credentials.library_id();
    b.run("INSERT INTO settings VALUES ('b0', 'x')").await;
    b.sync(server).await.unwrap();
    let one = server.log(id)[0].clone();
    for _ in 1..5000 {
        server.inject(id, one.clone());
    }
    while a.sync(server).await.unwrap().more {}
    as_layout_two(&mut a).await;
    a.run("INSERT INTO settings VALUES ('a1', 'x')").await;
    [a, b]
}

/// Until the log is hashed to the cursor, a sync sends nothing, does not call `caught_up`, and
/// says more is to read; the sync that finishes the hash does both.
#[tokio::test]
async fn syncs_hashing_the_log_after_an_upgrade_send_and_settle_nothing() {
    let server = Server::new();
    let [mut a, _b] = upgraded_over_a_long_log(&server).await;
    let id = a.credentials.library_id();
    let mut counting = Counting::default();
    let first = sync_with(
        &mut a.conn,
        &a.replica,
        &server,
        &a.credentials,
        now,
        &mut counting,
    )
    .await
    .unwrap();
    assert!(first.more);
    assert_eq!((first.sent, counting.caught_up), (0, 0));
    assert_eq!(server.log(id).len(), 5000);
    assert_eq!(fingerprint(&mut a.conn, id).await.unwrap().seq, 4000);
    let second = sync_with(
        &mut a.conn,
        &a.replica,
        &server,
        &a.credentials,
        now,
        &mut counting,
    )
    .await
    .unwrap();
    assert!(!second.more);
    assert!(second.sent > 0);
    assert_eq!(counting.caught_up, 1);
    assert_eq!(fingerprint(&mut a.conn, id).await.unwrap().seq, 5001);
}

/// Rolled back before the cursor, past what the first sync after the upgrade hashes: that sync
/// finds it, sending nothing and settling nothing.
#[tokio::test]
async fn a_rollback_beyond_the_first_hash_after_an_upgrade_is_found_at_once() {
    let server = Server::new();
    let [mut a, _b] = upgraded_over_a_long_log(&server).await;
    let id = a.credentials.library_id();
    server.roll_back(id, 4500);
    let mut counting = Counting::default();
    let found = sync_with(
        &mut a.conn,
        &a.replica,
        &server,
        &a.credentials,
        now,
        &mut counting,
    )
    .await;
    assert!(
        matches!(
            found,
            Err(Error::LogFault(LogFault::Behind { cursor: 5000 }))
        ),
        "{found:?}"
    );
    assert_eq!(counting.caught_up, 0);
    assert_eq!(server.log(id).len(), 4500);
}

/// After an upgrade from layout 2, with pages of one envelope, the log grows between syncs by
/// as many envelopes as a sync could read and more. The hash reaches the cursor of the upgrade in
/// the syncs its length takes, whatever the growth, and the outbox then goes out.
#[tokio::test]
async fn hashing_after_an_upgrade_ends_however_fast_the_log_grows() {
    for grow in [100usize, 499, 500, 600, 1500] {
        let server = Server::new();
        server.with(|s| s.caps.max_page_batches = Some(1));
        let [mut a, mut b] = pair(&server).await;
        let id = a.credentials.library_id();
        b.run("INSERT INTO settings VALUES ('b0', 'x')").await;
        b.sync(&server).await.unwrap();
        let one = server.log(id)[0].clone();
        for _ in 1..1500 {
            server.inject(id, one.clone());
        }
        while a.sync(&server).await.unwrap().more {}
        as_layout_two(&mut a).await;
        a.run("INSERT INTO settings VALUES ('a1', 'x')").await;
        let mut sent_at = None;
        for syncs in 1..=4 {
            if a.sync(&server).await.unwrap().sent > 0 {
                sent_at = Some(syncs);
                break;
            }
            for _ in 0..grow {
                server.inject(id, one.clone());
            }
        }
        // 1,500 pages of one envelope: a whole sync's pages, then the rest.
        assert_eq!(sent_at, Some(2), "growing by {grow} a sync");
    }
}

/// A sync of a layout-2 build that loaded its state before another connection upgraded the
/// tables goes on with its own statements: it moves the cursor and leaves the digest of the
/// envelope at the old cursor, with that position. This build reads on from there, checking that
/// digest, and requires the log to reach the moved cursor.
#[tokio::test]
async fn an_older_build_running_through_the_upgrade_moves_the_cursor_past_the_anchor() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    for key in ["b0", "b1", "b2"] {
        b.run(&format!("INSERT INTO settings VALUES ('{key}', 'x')"))
            .await;
        b.sync(&server).await.unwrap();
    }
    a.sync(&server).await.unwrap();
    b.run("INSERT INTO settings VALUES ('b3', 'x')").await;
    b.sync(&server).await.unwrap();
    // What a layout-2 build's advance writes, having read and applied envelope 4.
    a.run("UPDATE fongo_client_state SET cursor = max(cursor, 4)")
        .await;
    a.sync(&server).await.unwrap();
    assert_eq!(fault_of(&mut a).await, None);
    let print = fingerprint(&mut a.conn, id).await.unwrap();
    assert_eq!((print.seq, print.digest), (4, chain(id.0, &server.log(id))));
    // A log that no longer reaches a cursor moved so is still found.
    a.run("UPDATE fongo_client_state SET cursor = max(cursor, 5)")
        .await;
    server.roll_back(id, 4);
    assert!(matches!(
        a.sync(&server).await,
        Err(Error::LogFault(LogFault::Behind { cursor: 5 }))
    ));
}

async fn fault_of(peer: &mut Peer) -> Option<LogFault> {
    fongo_sync_client::log_fault(&mut peer.conn, peer.credentials.library_id())
        .await
        .unwrap()
}

/// A sync of a layout-2 build that read to 3 before the device resolved a fault writes its
/// advance after the resolution set the cursor to 0. The device must still read the log from 0,
/// where other devices have since refilled positions with envelopes it never applied.
#[tokio::test]
async fn an_older_build_raising_the_cursor_after_a_resolution_loses_nothing() {
    let server = Server::new();
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    let mut c = Peer::new(
        join_library(&server, &a.credentials.recovery_code(), now)
            .await
            .unwrap(),
    )
    .await;
    for key in ["b1", "b2", "b3"] {
        b.run(&format!("INSERT INTO settings VALUES ('{key}', 'x')"))
            .await;
        b.sync(&server).await.unwrap();
    }
    a.sync(&server).await.unwrap();
    assert_eq!(a.cursor().await, 3);
    server.roll_back(id, 1);
    c.sync(&server).await.unwrap();
    for key in ["c1", "c2"] {
        c.run(&format!("INSERT INTO settings VALUES ('{key}', 'x')"))
            .await;
        c.sync(&server).await.unwrap();
    }
    assert!(matches!(a.sync(&server).await, Err(Error::LogFault(_))));
    fongo_sync_client::resend_all(&mut a.conn, &a.replica, id)
        .await
        .unwrap();
    a.run("UPDATE fongo_client_state SET cursor = max(cursor, 3)")
        .await;
    for _ in 0..10 {
        for p in [&mut a, &mut b, &mut c] {
            match p.sync(&server).await {
                Ok(_) => {}
                Err(Error::LogFault(_)) => {
                    fongo_sync_client::resend_all(&mut p.conn, &p.replica, id)
                        .await
                        .unwrap();
                }
                Err(e) => panic!("{e:?}"),
            }
        }
    }
    let mut d = Peer::new(
        join_library(&server, &a.credentials.recovery_code(), now)
            .await
            .unwrap(),
    )
    .await;
    while d.sync(&server).await.unwrap().more {}
    let want = d.settings().await;
    assert_eq!(want.len(), 5, "{want:?}");
    for p in [&mut a, &mut b, &mut c] {
        assert_eq!(fault_of(p).await, None);
        assert_eq!(p.settings().await, want);
    }
}

async fn sending(peer: &mut Peer, server: &Server, key: &str) {
    peer.run(&format!(
        "INSERT OR REPLACE INTO settings VALUES ('{key}', 'x')"
    ))
    .await;
    peer.sync(server).await.unwrap();
}

/// Syncs until a sync reads the whole log, at most `n` times; whether one did.
async fn caught_up(peer: &mut Peer, server: &Server, n: usize) -> bool {
    for _ in 0..n {
        if !peer.sync(server).await.unwrap().more {
            return true;
        }
    }
    false
}

/// Pages of one envelope, and an older build moves the cursor across more of the log than one
/// sync reads: the syncs after it read on from where the last one got, and cross it.
#[tokio::test]
async fn a_gap_an_older_build_left_longer_than_one_sync_reads_is_crossed() {
    let server = Server::new();
    server.with(|s| s.caps.max_page_batches = Some(1));
    let [mut a, mut b] = pair(&server).await;
    let id = a.credentials.library_id();
    sending(&mut b, &server, "b0").await;
    a.sync(&server).await.unwrap();
    let one = server.log(id)[0].clone();
    for _ in 0..1500 {
        server.inject(id, one.clone());
    }
    sending(&mut b, &server, "b9").await;
    let len = server.log(id).len();
    a.run(&format!(
        "UPDATE fongo_client_state SET cursor = max(cursor, {len})"
    ))
    .await;
    assert!(caught_up(&mut a, &server, 4).await);
    assert_eq!(fault_of(&mut a).await, None);
    let print = fingerprint(&mut a.conn, id).await.unwrap();
    assert_eq!(
        (print.seq, print.digest),
        (len as u64, chain(id.0, &server.log(id)))
    );
}

/// After a resolution, the log refilled by another device ends exactly where an older build had
/// read, more than one sync reads past where the resolution put the cursor; the device's first
/// append lands right after that. The device must still read and apply all of the refill.
async fn append_right_after_a_raised_cursor(connections: usize) {
    let dir = support::scratch(&format!("append-after-raised-{connections}"));
    let path = dir.join("x.db");
    let server = Server::new();
    server.with(|s| s.caps.max_page_batches = Some(1));
    let made = create_library(&server, now).await.unwrap();
    let code = made.recovery_code();
    let join = || join_library(&server, &code, now);
    let mut b = Peer::new(join().await.unwrap()).await;
    let mut c = Peer::new(join().await.unwrap()).await;
    let mut a = Peer::new_at(&path, made.clone()).await;
    a.run("PRAGMA journal_mode = WAL").await;
    a.run("PRAGMA busy_timeout = 10000").await;
    let id = made.library_id();
    sending(&mut b, &server, "b1").await;
    let one = server.log(id)[0].clone();
    for _ in 0..1200 {
        server.inject(id, one.clone());
    }
    assert!(caught_up(&mut a, &server, 4).await);
    let stored = a.cursor().await;
    server.roll_back(id, 1);
    c.sync(&server).await.unwrap();
    sending(&mut c, &server, "c1").await;
    let theirs = server.log(id)[1].clone();
    while (server.log(id).len() as u64) < stored - 1 {
        server.inject(id, theirs.clone());
    }
    sending(&mut c, &server, "c-last").await;
    assert_eq!(server.log(id).len() as u64, stored);
    assert!(matches!(a.sync(&server).await, Err(Error::LogFault(_))));
    fongo_sync_client::resend_all(&mut a.conn, &a.replica, id)
        .await
        .unwrap();
    a.run(&format!(
        "UPDATE fongo_client_state SET cursor = max(cursor, {stored})"
    ))
    .await;
    if connections == 1 {
        for _ in 0..6 {
            a.sync(&server).await.unwrap();
        }
    } else {
        let mut other = Peer::open(&path, made.clone()).await;
        other.run("PRAGMA busy_timeout = 10000").await;
        for _ in 0..4 {
            let (x, y) = tokio::join!(a.sync(&server), other.sync(&server));
            x.unwrap();
            y.unwrap();
        }
        for _ in 0..2 {
            a.sync(&server).await.unwrap();
        }
    }
    assert_eq!(fault_of(&mut a).await, None);
    let mut d = Peer::new(join().await.unwrap()).await;
    assert!(caught_up(&mut d, &server, 4).await);
    assert_eq!(a.settings().await, d.settings().await);
    assert!(a.settings().await.contains(&"c-last=x".to_string()));
}

#[tokio::test]
async fn an_append_right_after_a_raised_cursor_does_not_skip_what_lies_before() {
    append_right_after_a_raised_cursor(1).await;
}

#[tokio::test]
async fn two_connections_appending_right_after_a_raised_cursor_skip_nothing() {
    append_right_after_a_raised_cursor(2).await;
}

/// Crossing a gap longer than one sync reads, the second sync is dropped part-way and the third
/// fails part-way: what the first read is kept, nothing goes back, and the syncs after them go on
/// from there.
#[tokio::test]
async fn crossing_a_long_gap_goes_on_after_a_crash() {
    let dir = support::scratch("long-gap-crash");
    let path = dir.join("x.db");
    let server = Server::new();
    server.with(|s| s.caps.max_page_batches = Some(1));
    let made = create_library(&server, now).await.unwrap();
    let mut b = Peer::new(
        join_library(&server, &made.recovery_code(), now)
            .await
            .unwrap(),
    )
    .await;
    let mut a = Peer::new_at(&path, made.clone()).await;
    let id = made.library_id();
    sending(&mut b, &server, "b0").await;
    a.sync(&server).await.unwrap();
    let one = server.log(id)[0].clone();
    for _ in 0..1500 {
        server.inject(id, one.clone());
    }
    sending(&mut b, &server, "b9").await;
    let len = server.log(id).len() as u64;
    a.run(&format!(
        "UPDATE fongo_client_state SET cursor = max(cursor, {len})"
    ))
    .await;
    let position = async |a: &mut Peer| fingerprint(&mut a.conn, id).await.unwrap().seq;
    assert!(a.sync(&server).await.unwrap().more);
    let first = position(&mut a).await;
    assert!(first > 900 && first < len, "the first sync kept {first}");
    // Dropped while its 300th request hangs.
    server.hang_at(300, false);
    {
        let syncing = a.sync(&server);
        tokio::select! {
            _ = syncing => panic!("the sync did not hang"),
            () = server.hung() => {}
        }
    }
    server.behave();
    sqlx::Connection::close(a.conn).await.unwrap();
    let mut a = Peer::open(&path, made.clone()).await;
    assert_eq!(position(&mut a).await, first);
    server.fail_at(200);
    assert!(matches!(a.sync(&server).await, Err(Error::Transport(_))));
    server.behave();
    assert!(position(&mut a).await >= first);
    assert!(caught_up(&mut a, &server, 3).await);
    assert_eq!(fault_of(&mut a).await, None);
    let print = fingerprint(&mut a.conn, id).await.unwrap();
    assert_eq!(
        (print.seq, print.digest),
        (len, chain(id.0, &server.log(id)))
    );
}

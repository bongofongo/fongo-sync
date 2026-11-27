//! The client's own tables: layouts 1, which recorded no layout, and 2 are upgraded in place; a
//! build of layout 1 fails on later tables, and one of layout 2 refuses them by their recorded
//! layout; and this build refuses a later layout.

#[allow(dead_code)]
mod support;

use fongo_sync_client::protocol::api::CAPABILITIES_PATH;
use fongo_sync_client::{Error, LogFault, Method, create_library, fingerprint, join_library};
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

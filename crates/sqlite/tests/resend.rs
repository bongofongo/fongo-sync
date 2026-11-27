//! What `Replica::resend` sends: a replica given only that shows what the sender shows.

mod support;

use fongo_sync_sqlite::core::{Change, Strategy};
use fongo_sync_sqlite::{Collection, Registry};
use support::peer::{Peer, later};

const SHELVES: &[&str] = &[
    "CREATE TABLE shelves (id INTEGER PRIMARY KEY, name TEXT UNIQUE, note TEXT)",
    "CREATE TABLE settings (key TEXT NOT NULL PRIMARY KEY, value TEXT)",
    "CREATE TABLE books (id INTEGER PRIMARY KEY, status TEXT, finished TEXT, pages INTEGER)",
];

fn shelves() -> Registry {
    Registry::new(vec![
        Collection::minted("shelves", "shelves", "id")
            .field("name")
            .field("note")
            .identity(&["name"]),
        Collection::natural("settings", "settings", &["key"])
            .field("key")
            .field("value"),
        Collection::minted("books", "books", "id")
            .field("status")
            .field("finished")
            .field_with("pages", Strategy::Max)
            .together(&["status", "finished"]),
    ])
    .unwrap()
}

async fn resent(peer: &mut Peer) -> Vec<Change> {
    let n = peer.replica.resend(&mut peer.conn).await.unwrap();
    let outbox = peer.replica.outbox(&mut peer.conn, u32::MAX).await.unwrap();
    assert_eq!(outbox.changes.len(), n);
    outbox.changes
}

/// Two shelves made apart with one name become one; the merged shelf is then renamed. A replica
/// that takes only what the survivor resends shows two shelves.
#[tokio::test]
async fn entities_found_to_be_one_stay_one_after_a_resend() {
    let mut b = Peer::new(SHELVES, shelves()).await;
    let mut c = Peer::new(SHELVES, shelves()).await;
    let mut a = Peer::new(SHELVES, shelves()).await;
    let mut hub = Vec::new();
    b.run("INSERT INTO shelves (name, note) VALUES ('fiction', 'b')")
        .await;
    b.push(&mut hub).await;
    later();
    c.run("INSERT INTO shelves (name, note) VALUES ('fiction', 'c')")
        .await;
    c.push(&mut hub).await;
    a.pull(&hub).await;
    a.push(&mut hub).await;
    assert_eq!(a.lines("SELECT name FROM shelves").await, ["fiction"]);
    later();
    a.run("UPDATE shelves SET name = 'novels'").await;
    a.push(&mut hub).await;
    let shown = a
        .lines("SELECT name || ':' || coalesce(note, '') FROM shelves ORDER BY 1")
        .await;

    // The server lost everything; B and C are gone.
    let changes = resent(&mut a).await;
    let mut d = Peer::new(SHELVES, shelves()).await;
    d.checking = false;
    d.apply(&changes).await;
    let on_d = d
        .lines("SELECT name || ':' || coalesce(note, '') FROM shelves ORDER BY 1")
        .await;
    assert_eq!(
        on_d, shown,
        "a fresh replica shows other shelves than the survivor"
    );
}

/// Greatest-value fields, together sets, deletions and natural keys round-trip.
#[tokio::test]
async fn other_kinds_round_trip_through_a_resend() {
    let mut b = Peer::new(SHELVES, shelves()).await;
    let mut a = Peer::new(SHELVES, shelves()).await;
    let mut hub = Vec::new();
    b.run("INSERT INTO books (status, finished, pages) VALUES ('read', '2020', 300)")
        .await;
    b.run("INSERT INTO books (status, finished, pages) VALUES ('gone', NULL, 1)")
        .await;
    b.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    b.push(&mut hub).await;
    a.pull(&hub).await;
    later();
    a.run("UPDATE books SET pages = 200, status = 'reading', finished = NULL WHERE id = 1")
        .await;
    a.run("DELETE FROM books WHERE status = 'gone'").await;
    a.run("UPDATE settings SET value = 'light'").await;
    a.push(&mut hub).await;
    later();
    b.pull(&hub).await;
    b.run("UPDATE books SET pages = 250 WHERE id = 1").await;
    b.push(&mut hub).await;
    a.pull(&hub).await;
    let rows = "SELECT 'b ' || coalesce(status,'-') || ' ' || coalesce(finished,'-') || ' ' || pages FROM books
                UNION ALL SELECT 's ' || key || '=' || value FROM settings ORDER BY 1";
    let shown = a.lines(rows).await;
    let changes = resent(&mut a).await;
    let mut d = Peer::new(SHELVES, shelves()).await;
    d.checking = false;
    d.apply(&changes).await;
    assert_eq!(d.lines(rows).await, shown);
}

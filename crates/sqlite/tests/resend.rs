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

const ALL: &[&str] = &[
    "CREATE TABLE shelves (id INTEGER PRIMARY KEY, name TEXT UNIQUE, note TEXT)",
    "CREATE TABLE settings (key TEXT NOT NULL PRIMARY KEY, value TEXT)",
    "CREATE TABLE books (id INTEGER PRIMARY KEY, status TEXT, finished TEXT, pages INTEGER)",
    "CREATE TABLE tags (id INTEGER PRIMARY KEY, label TEXT)",
];

fn full() -> Registry {
    let mut r = vec![
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
    ];
    r.push(Collection::minted("tags", "tags", "id").field("label"));
    Registry::new(r).unwrap()
}

const ROWS: &str = "SELECT 'shelf ' || name || ':' || coalesce(note, '-') FROM shelves
    UNION ALL SELECT 'book ' || coalesce(status,'-') || ' ' || coalesce(finished,'-') || ' ' || coalesce(pages,'-') FROM books
    UNION ALL SELECT 's ' || key || '=' || coalesce(value,'-') FROM settings ORDER BY 1";
const TAGS: &str = "SELECT 'tag ' || label FROM tags ORDER BY 1";

/// A, whose version lacks `tags`, merged B's and C's work: entities found to be one, deletions,
/// a re-created natural row, together sets, a greatest-value field and an unknown collection. The
/// log is lost; A resends. A fresh D of A's version must show A's rows; a fresh E of the newer
/// version must show what an oracle that took everything shows.
#[tokio::test]
async fn entities_merged_deleted_and_unknown_round_trip_through_a_resend() {
    let mut b = Peer::new(ALL, full()).await;
    let mut c = Peer::new(ALL, full()).await;
    let mut a = Peer::new(ALL, shelves()).await;
    let mut oracle = Peer::new(ALL, full()).await;
    let mut hub = Vec::new();
    b.run("INSERT INTO shelves (name, note) VALUES ('fiction', 'b')")
        .await;
    b.run("INSERT INTO books (status, finished, pages) VALUES ('read', '2020', 300)")
        .await;
    b.run("INSERT INTO books (status, finished, pages) VALUES ('gone', NULL, 1)")
        .await;
    b.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    b.run("INSERT INTO tags (label) VALUES ('t1')").await;
    b.push(&mut hub).await;
    later();
    c.run("INSERT INTO shelves (name, note) VALUES ('fiction', 'c')")
        .await;
    c.run("INSERT INTO shelves (name, note) VALUES ('poetry', 'c')")
        .await;
    c.push(&mut hub).await;
    c.pull(&hub).await;
    later();
    c.run("UPDATE shelves SET name = 'novels' WHERE name = 'fiction'")
        .await;
    c.run("UPDATE books SET pages = 250, status = 'reading', finished = NULL WHERE pages = 300")
        .await;
    c.run("DELETE FROM books WHERE status = 'gone'").await;
    c.run("DELETE FROM settings WHERE key = 'theme'").await;
    c.run("UPDATE tags SET label = 't1c'").await;
    c.push(&mut hub).await;
    later();
    b.pull(&hub).await;
    b.run("INSERT INTO settings VALUES ('theme', 'light')")
        .await;
    b.run("UPDATE books SET pages = 280").await;
    b.run("DELETE FROM shelves WHERE name = 'poetry'").await;
    b.push(&mut hub).await;
    later();
    a.pull(&hub).await;
    a.run("INSERT INTO shelves (name, note) VALUES ('fiction', 'a')")
        .await;
    a.push(&mut hub).await;
    oracle.pull(&hub).await;
    let shown = a.lines(ROWS).await;
    let truth = (oracle.lines(ROWS).await, oracle.lines(TAGS).await);
    assert_eq!(shown, truth.0, "A and the oracle agree before the loss");

    let changes = resent(&mut a).await;
    let mut d = Peer::new(ALL, shelves()).await;
    d.checking = false;
    d.apply(&changes).await;
    assert_eq!(d.lines(ROWS).await, shown, "D (A's version)");
    let mut e = Peer::new(ALL, full()).await;
    e.checking = false;
    e.apply(&changes).await;
    assert_eq!(
        (e.lines(ROWS).await, e.lines(TAGS).await),
        truth,
        "E (newer version)"
    );
    // A upgraded to the newer version afterwards shows the tags too.
    a.upgrade(full()).await;
    assert_eq!(a.lines(TAGS).await, truth.1);
}

/// Two devices each found a different pair of three shelves to be one, so one shelf carries two
/// aliases. Resent, they reach a replica of a version that lacks the collection, which keeps
/// changes it cannot merge one per clock: both must be kept, and neither may look like a second
/// database writing as one node.
#[tokio::test]
async fn two_aliases_on_one_entity_are_resent_at_two_clocks() {
    for _ in 0..30 {
        let mut p: Vec<Peer> = Vec::new();
        for _ in 0..3 {
            p.push(Peer::new(SHELVES, shelves()).await);
        }
        let mut sent = Vec::new();
        for (i, peer) in p.iter_mut().enumerate() {
            peer.run(&format!(
                "INSERT INTO shelves (name, note) VALUES ('fiction', 'p{i}')"
            ))
            .await;
            let mut out = Vec::new();
            peer.push(&mut out).await;
            sent.push(out);
            later();
        }
        // D1 sees p0 and p1; D2 sees p1 and p2.
        let mut d1 = Peer::new(SHELVES, shelves()).await;
        let mut d2 = Peer::new(SHELVES, shelves()).await;
        d1.checking = false;
        d2.checking = false;
        d1.apply(&[sent[0].clone(), sent[1].clone()].concat()).await;
        d2.apply(&[sent[1].clone(), sent[2].clone()].concat()).await;
        let mut o1 = Vec::new();
        let mut o2 = Vec::new();
        d1.push(&mut o1).await;
        d2.push(&mut o2).await;
        let mut a = Peer::new(SHELVES, shelves()).await;
        a.checking = false;
        a.apply(&[sent.concat(), o1, o2].concat()).await;
        let twice = a
            .int("SELECT count(*) FROM (SELECT a FROM fongo_alias GROUP BY a HAVING count(*) > 1)")
            .await;
        if twice == 0 {
            continue;
        }
        let changes = resent(&mut a).await;
        let aliases: Vec<&Change> = changes.iter().filter(|c| c.field == "$alias").collect();
        // A replica of a version without `shelves` keeps them as foreign.
        let without = Registry::new(vec![
            Collection::natural("settings", "settings", &["key"])
                .field("key")
                .field("value"),
        ])
        .unwrap();
        let mut old = Peer::new(SHELVES, without).await;
        old.checking = false;
        let applied = old.apply(&changes).await;
        let kept = old
            .int("SELECT count(*) FROM fongo_foreign WHERE field = '$alias'")
            .await;
        let mut fresh = Peer::new(SHELVES, shelves()).await;
        fresh.checking = false;
        let applied_fresh = fresh.apply(&changes).await;
        assert!(
            applied.shared.is_empty()
                && kept as usize == aliases.len()
                && applied_fresh.shared.is_empty(),
            "old version kept {kept} of {} aliases; shared reported: old {:?}, fresh {:?}",
            aliases.len(),
            applied.shared,
            applied_fresh.shared
        );
        return;
    }
    panic!("never made an entity with two aliases");
}

/// A value the application kept as this replica's own on resuming is never sent, so a resend does
/// not carry it: a replica given what was resent shows the merged value there, as every other
/// replica does, and the rest of the row as this one does.
#[tokio::test]
async fn a_value_kept_as_this_replicas_own_is_not_resent() {
    let mut a = Peer::new(SHELVES, shelves()).await;
    let mut hub = Vec::new();
    a.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    a.run("INSERT INTO settings VALUES ('font', 'serif')").await;
    a.push(&mut hub).await;
    a.suspend().await;
    a.run("UPDATE settings SET value = 'local' WHERE key = 'theme'")
        .await;
    a.resume(fongo_sync_sqlite::Resume::Baseline).await;
    let rows = "SELECT key || '=' || value FROM settings ORDER BY 1";
    assert_eq!(a.lines(rows).await, ["font=serif", "theme=local"]);
    let changes = resent(&mut a).await;
    let mut d = Peer::new(SHELVES, shelves()).await;
    d.checking = false;
    d.apply(&changes).await;
    assert_eq!(d.lines(rows).await, ["font=serif", "theme=dark"]);
}

/// A field that refers to a file holds the file's reference as a blob value, and is resent as any
/// other value. The kernel keeps nothing else of files: claiming and retiring them is the
/// client's, and no client here does either yet.
#[tokio::test]
async fn a_file_reference_is_resent_as_its_value() {
    let mut b = Peer::new(SHELVES, shelves()).await;
    let mut a = Peer::new(SHELVES, shelves()).await;
    let mut hub = Vec::new();
    b.run("INSERT INTO settings VALUES ('cover', x'0102030405060708')")
        .await;
    b.push(&mut hub).await;
    a.pull(&hub).await;
    let rows = "SELECT key || '=' || hex(value) || ':' || typeof(value) FROM settings";
    assert_eq!(a.lines(rows).await, ["cover=0102030405060708:blob"]);
    let changes = resent(&mut a).await;
    let mut d = Peer::new(SHELVES, shelves()).await;
    d.checking = false;
    d.apply(&changes).await;
    assert_eq!(d.lines(rows).await, a.lines(rows).await);
}

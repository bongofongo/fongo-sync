//! Which replica made an entity and which wrote the value a field shows: `Replica::made` and
//! `Replica::written_here`, in the cases their definitions name, and the clash they exist for.
//! Every apply and collect also checks both against the reference model (see `support::midway`).

mod support;

use fongo_sync_sqlite::core::{Change, Gid, Hlc, Value};
use fongo_sync_sqlite::{Collection, Held, Made, Registry};
use support::peer::{Peer, later, make, same_everywhere, settle};

const SHELVES: &[&str] = &[
    "CREATE TABLE shelves (id INTEGER PRIMARY KEY, name TEXT UNIQUE, note TEXT)",
    "CREATE TABLE settings (key TEXT NOT NULL PRIMARY KEY, value TEXT)",
    "CREATE TABLE books (id INTEGER PRIMARY KEY, status TEXT, finished TEXT)",
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
            .together(&["status", "finished"]),
    ])
    .unwrap()
}

/// At most one open item under a parent: a rule of the application's own the kernel cannot
/// resolve.
const ITEMS: &[&str] = &[
    "CREATE TABLE items (id INTEGER PRIMARY KEY, parent TEXT, open INTEGER, title TEXT)",
    "CREATE UNIQUE INDEX one_open ON items (parent) WHERE open = 1",
];

fn items() -> Registry {
    Registry::new(vec![
        Collection::minted("items", "items", "id")
            .field("parent")
            .field("open")
            .field("title"),
    ])
    .unwrap()
}

async fn made(peer: &mut Peer, collection: &str, gid: Gid) -> Option<Made> {
    peer.replica
        .made(&mut peer.conn, collection, gid)
        .await
        .unwrap()
}

async fn written(peer: &mut Peer, collection: &str, gid: Gid, field: &str) -> Option<bool> {
    latest(peer, collection, gid, &[field]).await
}

async fn latest(peer: &mut Peer, collection: &str, gid: Gid, fields: &[&str]) -> Option<bool> {
    peer.replica
        .written_here(&mut peer.conn, collection, gid, fields)
        .await
        .unwrap()
}

fn theme() -> Gid {
    Gid::derive("settings", &[Value::Text("theme".into())])
}

/// A change from a replica outside the test, at a chosen clock.
fn from_elsewhere(collection: &str, gid: Gid, field: &str, value: Value, wall_ms: u64) -> Change {
    Change {
        collection: collection.into(),
        gid,
        field: field.into(),
        value,
        clock: Hlc {
            wall_ms,
            counter: 1,
            node: Gid([7; 16]),
        },
    }
}

async fn gid_at(peer: &mut Peer, collection: &str, key: &str) -> Gid {
    peer.replica
        .gid_for_key(&mut peer.conn, collection, key)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn a_row_inserted_here_was_made_here_and_not_where_it_was_received() {
    let mut peers = make(2, SHELVES, shelves).await;
    let mut hub = Vec::new();
    peers[0]
        .run("INSERT INTO shelves (name) VALUES ('fiction')")
        .await;
    peers[0].push(&mut hub).await;
    let gid = peers[0].gid("shelves", 1).await;
    assert_eq!(made(&mut peers[0], "shelves", gid).await, Some(Made::Here));
    assert_eq!(
        written(&mut peers[0], "shelves", gid, "name").await,
        Some(true)
    );
    // A field left NULL when the row was made has no write behind it.
    assert_eq!(written(&mut peers[0], "shelves", gid, "note").await, None);

    peers[1].pull(&hub).await;
    assert_eq!(
        made(&mut peers[1], "shelves", gid).await,
        Some(Made::Elsewhere)
    );
    assert_eq!(
        written(&mut peers[1], "shelves", gid, "name").await,
        Some(false)
    );

    // An edit elsewhere moves the field's writer, never the maker.
    later();
    peers[1].run("UPDATE shelves SET note = 'tall'").await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(made(&mut peers[0], "shelves", gid).await, Some(Made::Here));
    assert_eq!(
        made(&mut peers[1], "shelves", gid).await,
        Some(Made::Elsewhere)
    );
    assert_eq!(
        written(&mut peers[0], "shelves", gid, "note").await,
        Some(false)
    );
    assert_eq!(
        written(&mut peers[1], "shelves", gid, "note").await,
        Some(true)
    );
    assert_eq!(
        written(&mut peers[0], "shelves", gid, "name").await,
        Some(true)
    );

    // A field the registry does not declare is an error, even among declared ones; nothing is
    // known of an unknown entity.
    for fields in [&["$deleted"][..], &["name", "width"], &[]] {
        let peer = &mut peers[0];
        let asked = peer
            .replica
            .written_here(&mut peer.conn, "shelves", gid, fields)
            .await;
        match fields {
            [] => assert_eq!(asked.unwrap(), None),
            _ => assert!(asked.is_err(), "{fields:?}"),
        }
    }
    assert_eq!(made(&mut peers[0], "shelves", Gid([7; 16])).await, None);
    assert_eq!(made(&mut peers[0], "books", gid).await, None);

    // Nor once it is removed.
    peers[0].run("DELETE FROM shelves").await;
    settle(&mut peers, &mut hub).await;
    for peer in peers.iter_mut() {
        assert_eq!(made(peer, "shelves", gid).await, None);
        assert_eq!(written(peer, "shelves", gid, "name").await, None);
    }
}

#[tokio::test]
async fn rows_merged_by_identity_were_made_by_the_maker_of_the_earliest() {
    let mut peers = make(3, SHELVES, shelves).await;
    let mut hub = Vec::new();
    peers[0]
        .run("INSERT INTO shelves (name, note) VALUES ('fiction', 'first')")
        .await;
    peers[0].push(&mut hub).await;
    later();
    peers[1]
        .run("INSERT INTO shelves (name, note) VALUES ('fiction', 'second')")
        .await;
    let mut late = Vec::new();
    peers[1].push(&mut late).await;
    // Before they meet, each made its own.
    for i in [0, 1] {
        let own = gid_at(&mut peers[i], "shelves", "[1]").await;
        assert_eq!(made(&mut peers[i], "shelves", own).await, Some(Made::Here));
    }
    // A replica that has the later member's making and only an edit of the earlier one cannot
    // tell which came first.
    let first = peers[0].gid("shelves", 1).await;
    let edit: Vec<Change> = hub.iter().filter(|c| c.field == "name").cloned().collect();
    peers[2].apply(&late).await;
    peers[2].apply(&edit).await;
    let second = late[0].gid;
    assert_eq!(
        made(&mut peers[2], "shelves", second).await,
        Some(Made::Elsewhere)
    );
    peers[1].apply(&edit).await;
    assert_eq!(
        made(&mut peers[1], "shelves", second).await,
        Some(Made::Elsewhere)
    );

    hub.extend(late);
    settle(&mut peers, &mut hub).await;
    same_everywhere(&mut peers, "SELECT name || ' ' || note FROM shelves").await;
    // Each asks about its own row, and only the maker of the earlier member says it made it.
    let mut answers = Vec::new();
    for peer in peers.iter_mut() {
        let own = gid_at(peer, "shelves", "[1]").await;
        assert_eq!(own, first.min(second));
        answers.push(made(peer, "shelves", own).await);
        assert_eq!(
            made(peer, "shelves", first).await,
            answers.last().copied().unwrap()
        );
        assert_eq!(
            made(peer, "shelves", second).await,
            answers.last().copied().unwrap()
        );
    }
    assert_eq!(
        answers,
        [
            Some(Made::Here),
            Some(Made::Elsewhere),
            Some(Made::Elsewhere)
        ]
    );
    // The note shows the later write, from the second replica.
    let notes = [
        written(&mut peers[0], "shelves", first, "note").await,
        written(&mut peers[1], "shelves", first, "note").await,
        written(&mut peers[2], "shelves", first, "note").await,
    ];
    assert_eq!(notes, [Some(false), Some(true), Some(false)]);
}

#[tokio::test]
async fn a_row_found_at_install_was_made_by_no_replica() {
    let mut first = Peer::bare(SHELVES, shelves()).await;
    first
        .run("INSERT INTO shelves (name, note) VALUES ('fiction', 'old')")
        .await;
    first.install().await;
    let mut peers = vec![first, Peer::new(SHELVES, shelves()).await];
    let mut hub = Vec::new();
    settle(&mut peers, &mut hub).await;
    let gid = peers[0].gid("shelves", 1).await;
    for peer in peers.iter_mut() {
        assert_eq!(made(peer, "shelves", gid).await, Some(Made::Adopted));
    }
    // Its values were written by the replica that found them.
    assert_eq!(
        written(&mut peers[0], "shelves", gid, "note").await,
        Some(true)
    );
    assert_eq!(
        written(&mut peers[1], "shelves", gid, "note").await,
        Some(false)
    );

    // A row made since elsewhere that the identity merges with it is adopted too.
    later();
    let mut late = Peer::new(SHELVES, shelves()).await;
    late.run("INSERT INTO shelves (name, note) VALUES ('fiction', 'new')")
        .await;
    late.push(&mut hub).await;
    let new = late.gid("shelves", 1).await;
    assert_eq!(made(&mut late, "shelves", new).await, Some(Made::Here));
    peers.push(late);
    settle(&mut peers, &mut hub).await;
    for peer in peers.iter_mut() {
        assert_eq!(made(peer, "shelves", new).await, Some(Made::Adopted));
        assert_eq!(made(peer, "shelves", gid).await, Some(Made::Adopted));
    }
}

#[tokio::test]
async fn a_field_the_registry_gains_later_leaves_the_maker_as_it_was() {
    let before = || {
        Registry::new(vec![
            Collection::minted("shelves", "shelves", "id")
                .field("name")
                .identity(&["name"]),
        ])
        .unwrap()
    };
    let mut peers = make(2, SHELVES, before).await;
    let mut hub = Vec::new();
    peers[0]
        .run("INSERT INTO shelves (name) VALUES ('fiction')")
        .await;
    settle(&mut peers, &mut hub).await;
    let gid = peers[0].gid("shelves", 1).await;
    later();
    peers[1].run("UPDATE shelves SET note = 'kept'").await;
    for peer in peers.iter_mut() {
        peer.upgrade(shelves()).await;
    }
    settle(&mut peers, &mut hub).await;
    let answers = [
        made(&mut peers[0], "shelves", gid).await,
        made(&mut peers[1], "shelves", gid).await,
    ];
    assert_eq!(answers, [Some(Made::Here), Some(Made::Elsewhere)]);
    assert_eq!(
        written(&mut peers[0], "shelves", gid, "note").await,
        Some(false)
    );
    assert_eq!(
        written(&mut peers[1], "shelves", gid, "note").await,
        Some(true)
    );
}

#[tokio::test]
async fn identical_seeded_rows_were_made_by_no_replica_and_each_field_by_one() {
    let mut peers = Vec::new();
    for _ in 0..3 {
        let mut peer = Peer::bare(SHELVES, shelves()).await;
        peer.run("INSERT INTO shelves (name, note) VALUES ('fiction', 'seed')")
            .await;
        peer.run("INSERT INTO settings VALUES ('theme', 'dark')")
            .await;
        peer.install().await;
        peers.push(peer);
    }
    let mut hub = Vec::new();
    settle(&mut peers, &mut hub).await;
    same_everywhere(&mut peers, "SELECT name || ' ' || note FROM shelves").await;
    let theme = Value::Text("theme".into());
    let setting = Gid::derive("settings", &[theme]);
    for (collection, field) in [("shelves", "note"), ("settings", "value")] {
        let mut writers = 0;
        for peer in peers.iter_mut() {
            let gid = match collection {
                "shelves" => gid_at(peer, "shelves", "[1]").await,
                _ => setting,
            };
            assert_eq!(made(peer, collection, gid).await, Some(Made::Adopted));
            if written(peer, collection, gid, field).await == Some(true) {
                writers += 1;
            }
        }
        assert_eq!(writers, 1, "{collection}.{field}");
    }
}

#[tokio::test]
async fn a_natural_row_removed_and_made_again_was_made_by_who_made_it_again() {
    let mut peers = make(3, SHELVES, shelves).await;
    let mut hub = Vec::new();
    let gid = Gid::derive("settings", &[Value::Text("theme".into())]);
    peers[0]
        .run("INSERT INTO settings VALUES ('theme', 'dark')")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(made(&mut peers[0], "settings", gid).await, Some(Made::Here));
    later();
    peers[1].run("DELETE FROM settings").await;
    settle(&mut peers, &mut hub).await;
    for peer in peers.iter_mut() {
        assert_eq!(made(peer, "settings", gid).await, None);
    }
    later();
    peers[2]
        .run("INSERT INTO settings VALUES ('theme', 'light')")
        .await;
    settle(&mut peers, &mut hub).await;
    let answers = [
        made(&mut peers[0], "settings", gid).await,
        made(&mut peers[1], "settings", gid).await,
        made(&mut peers[2], "settings", gid).await,
    ];
    assert_eq!(
        answers,
        [
            Some(Made::Elsewhere),
            Some(Made::Elsewhere),
            Some(Made::Here)
        ]
    );
    assert_eq!(
        written(&mut peers[2], "settings", gid, "key").await,
        Some(true)
    );
    assert_eq!(
        written(&mut peers[0], "settings", gid, "key").await,
        Some(false)
    );
}

#[tokio::test]
async fn a_local_id_taken_again_names_the_new_entity_and_its_maker() {
    let mut peers = make(2, SHELVES, shelves).await;
    let mut hub = Vec::new();
    peers[1]
        .run("INSERT INTO shelves (name) VALUES ('fiction')")
        .await;
    settle(&mut peers, &mut hub).await;
    let theirs = peers[0].gid("shelves", 1).await;
    assert_eq!(
        made(&mut peers[0], "shelves", theirs).await,
        Some(Made::Elsewhere)
    );
    later();
    peers[0].run("DELETE FROM shelves WHERE id = 1").await;
    peers[0]
        .run("INSERT INTO shelves (id, name) VALUES (1, 'poetry')")
        .await;
    settle(&mut peers, &mut hub).await;
    let mine = peers[0].gid("shelves", 1).await;
    assert_ne!(mine, theirs);
    assert_eq!(made(&mut peers[0], "shelves", mine).await, Some(Made::Here));
    assert_eq!(made(&mut peers[0], "shelves", theirs).await, None);
    assert_eq!(
        made(&mut peers[1], "shelves", mine).await,
        Some(Made::Elsewhere)
    );
}

#[tokio::test]
async fn a_member_removed_before_its_group_formed_does_not_count_as_its_maker() {
    let mut peers = make(3, SHELVES, shelves).await;
    let mut hub = Vec::new();
    peers[0]
        .run("INSERT INTO shelves (name) VALUES ('fiction')")
        .await;
    let first = peers[0].push(&mut hub).await[0].gid;
    peers[2].pull(&hub).await;
    later();
    peers[1]
        .run("INSERT INTO shelves (name) VALUES ('fiction')")
        .await;
    let second = peers[1].push(&mut hub).await[0].gid;
    peers[1].sync(&mut hub).await;
    // The third replica removes the first shelf without knowing of the second, so the group
    // keeps a member that is not live.
    later();
    peers[2].run("DELETE FROM shelves").await;
    peers[2].push(&mut hub).await;
    settle(&mut peers, &mut hub).await;
    same_everywhere(&mut peers, "SELECT name FROM shelves").await;
    let mut answers = Vec::new();
    for peer in peers.iter_mut() {
        answers.push(made(peer, "shelves", first).await);
        assert_eq!(
            made(peer, "shelves", second).await,
            *answers.last().unwrap()
        );
    }
    assert_eq!(
        answers,
        [
            Some(Made::Elsewhere),
            Some(Made::Here),
            Some(Made::Elsewhere)
        ]
    );
}

#[tokio::test]
async fn a_field_in_a_set_was_written_by_the_sets_latest_write_even_half_delivered() {
    let mut peers = make(2, SHELVES, shelves).await;
    let mut hub = Vec::new();
    peers[0]
        .run("INSERT INTO books (status, finished) VALUES ('done', 'monday')")
        .await;
    settle(&mut peers, &mut hub).await;
    let gid = peers[0].gid("books", 1).await;
    later();
    peers[1]
        .run("UPDATE books SET status = 'again', finished = 'friday'")
        .await;
    let sent = peers[1].push(&mut hub).await;
    let (status, finished): (Vec<Change>, Vec<Change>) =
        sent.into_iter().partition(|c| c.field == "status");
    assert_eq!(finished.len(), 1);
    peers[0].apply(&status).await;
    // `merged` picks the field alone and still shows the first write; the row and the answer
    // follow the set, whose latest write is the second replica's.
    let merged = {
        let peer = &mut peers[0];
        peer.replica
            .merged(&mut peer.conn, "books", gid)
            .await
            .unwrap()
    };
    assert_eq!(merged["finished"], Value::Text("monday".into()));
    assert_eq!(
        peers[0]
            .lines("SELECT status || ' ' || COALESCE(finished, 'null') FROM books")
            .await,
        ["again null"]
    );
    assert_eq!(
        written(&mut peers[0], "books", gid, "status").await,
        Some(false)
    );
    assert_eq!(
        written(&mut peers[0], "books", gid, "finished").await,
        Some(false)
    );
    assert_eq!(
        written(&mut peers[1], "books", gid, "finished").await,
        Some(true)
    );
    peers[0].apply(&finished).await;
    assert_eq!(
        written(&mut peers[0], "books", gid, "finished").await,
        Some(false)
    );
    assert_eq!(
        peers[0]
            .lines("SELECT status || ' ' || finished FROM books")
            .await,
        ["again friday"]
    );
}

#[tokio::test]
async fn a_natural_row_found_at_install_and_made_since_elsewhere_was_made_there() {
    let mut first = Peer::bare(SHELVES, shelves()).await;
    first
        .run("INSERT INTO settings VALUES ('theme', 'dark')")
        .await;
    first.install().await;
    let mut peers = vec![first, Peer::new(SHELVES, shelves()).await];
    later();
    peers[1]
        .run("INSERT INTO settings VALUES ('theme', 'light')")
        .await;
    let mut hub = Vec::new();
    settle(&mut peers, &mut hub).await;
    let answers = [
        made(&mut peers[0], "settings", theme()).await,
        made(&mut peers[1], "settings", theme()).await,
    ];
    assert_eq!(answers, [Some(Made::Elsewhere), Some(Made::Here)]);
}

#[tokio::test]
async fn a_natural_key_made_five_times_names_its_last_maker_and_an_earlier_key_writer() {
    let mut peers = make(5, SHELVES, shelves).await;
    let mut hub = Vec::new();
    for i in 0..5 {
        later();
        peers[i]
            .run("INSERT INTO settings VALUES ('theme', 'dark')")
            .await;
        settle(&mut peers, &mut hub).await;
        if i < 4 {
            later();
            peers[i].run("DELETE FROM settings").await;
            settle(&mut peers, &mut hub).await;
        }
    }
    let (mut makers, mut values, mut keys) = (Vec::new(), Vec::new(), Vec::new());
    for peer in peers.iter_mut() {
        makers.push(made(peer, "settings", theme()).await == Some(Made::Here));
        values.push(written(peer, "settings", theme(), "value").await == Some(true));
        keys.push(written(peer, "settings", theme(), "key").await == Some(true));
    }
    assert_eq!(makers, [false, false, false, false, true]);
    assert_eq!(values, [false, false, false, false, true]);
    // Only the earliest four writes to a key field count.
    assert_eq!(keys, [false, false, false, true, false]);
}

#[tokio::test]
async fn a_maker_that_installs_again_leaves_no_replica_that_made_its_rows() {
    let mut peers = make(2, SHELVES, shelves).await;
    let mut hub = Vec::new();
    peers[0]
        .run("INSERT INTO settings VALUES ('theme', 'dark')")
        .await;
    peers[0]
        .run("INSERT INTO shelves (name) VALUES ('fiction')")
        .await;
    settle(&mut peers, &mut hub).await;
    let shelf = peers[0].gid("shelves", 1).await;
    assert_eq!(
        made(&mut peers[0], "settings", theme()).await,
        Some(Made::Here)
    );
    let replica = peers[0].replica.clone();
    replica.uninstall(&mut peers[0].conn).await.unwrap();
    // A fresh join: what it held before is unknown to its new self.
    peers[0].taken.clear();
    peers[0].cursor = 0;
    peers[0].writes = Default::default();
    later();
    peers[0].install().await;
    settle(&mut peers, &mut hub).await;
    for peer in peers.iter_mut() {
        assert_eq!(made(peer, "settings", theme()).await, Some(Made::Elsewhere));
        assert_eq!(made(peer, "shelves", shelf).await, Some(Made::Adopted));
    }
}

#[tokio::test]
async fn a_making_at_wall_time_zero_reads_as_adopted() {
    let mut peer = Peer::new(SHELVES, shelves()).await;
    let gid = Gid([5; 16]);
    peer.apply(&[
        from_elsewhere("shelves", gid, "$deleted", Value::Int(0), 0),
        from_elsewhere("shelves", gid, "name", Value::Text("x".into()), 0),
    ])
    .await;
    assert_eq!(made(&mut peer, "shelves", gid).await, Some(Made::Adopted));
}

#[tokio::test]
async fn the_reads_write_nothing() {
    let mut peers = make(2, SHELVES, shelves).await;
    let mut hub = Vec::new();
    peers[0]
        .run("INSERT INTO shelves (name) VALUES ('fiction')")
        .await;
    settle(&mut peers, &mut hub).await;
    let gid = peers[0].gid("shelves", 1).await;
    let changes = "SELECT total_changes()";
    let before = peers[1].int(changes).await;
    for _ in 0..3 {
        made(&mut peers[1], "shelves", gid).await;
        written(&mut peers[1], "shelves", gid, "name").await;
        made(&mut peers[1], "nothing", gid).await;
        made(&mut peers[1], "settings", gid).await;
    }
    assert_eq!(peers[1].int(changes).await, before);
}

/// The open items under a parent, in id order.
async fn open_under(peer: &mut Peer, parent: &Value) -> Vec<i64> {
    let Value::Text(parent) = parent else {
        return Vec::new();
    };
    sqlx::query_scalar("SELECT id FROM items WHERE parent = ?1 AND open = 1 ORDER BY id")
        .bind(parent)
        .fetch_all(&mut peer.conn)
        .await
        .unwrap()
}

#[tokio::test]
async fn a_held_entity_with_no_row_here_is_answered_by_its_gid() {
    let mut peers = make(3, ITEMS, items).await;
    let mut hub = Vec::new();
    for (i, title) in [(0, "a"), (1, "b")] {
        peers[i]
            .run(&format!(
                "INSERT INTO items (parent, open, title) VALUES ('p', 1, '{title}')"
            ))
            .await;
        peers[i].push(&mut hub).await;
        later();
    }
    let applied = peers[2].pull(&hub).await;
    assert_eq!(applied.held.len(), 1);
    let Held { gid, key, .. } = applied.held[0].clone();
    assert_eq!(key, None);
    assert_eq!(
        made(&mut peers[2], "items", gid).await,
        Some(Made::Elsewhere)
    );
    assert_eq!(
        written(&mut peers[2], "items", gid, "title").await,
        Some(false)
    );
    let maker = if peers[0].gid("items", 1).await == gid {
        0
    } else {
        1
    };
    assert_eq!(
        made(&mut peers[maker], "items", gid).await,
        Some(Made::Here)
    );
    assert_eq!(made(&mut peers[1 - maker], "items", gid).await, None);
}

/// Who acts on two open items under one parent.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Rule {
    /// The replica that made the item that loses closes it.
    Maker,
    /// Any replica that holds a row for the item that loses closes it.
    Holder,
}

/// The application's resolution of a held item: of two open items under one parent the one with
/// the larger gid loses and is closed, by the replicas `rule` names. Returns whether it wrote.
async fn resolve(peer: &mut Peer, held: &[Held], rule: Rule, hub: &mut Vec<Change>) -> bool {
    let mut wrote = false;
    for h in held {
        let merged = peer
            .replica
            .merged(&mut peer.conn, "items", h.gid)
            .await
            .unwrap();
        for id in open_under(peer, &merged["parent"]).await {
            let other = peer.gid("items", id).await;
            if other < h.gid {
                continue;
            }
            let decides = match rule {
                Rule::Maker => made(peer, "items", other).await == Some(Made::Here),
                Rule::Holder => true,
            };
            if decides {
                peer.run(&format!("UPDATE items SET open = 0 WHERE id = {id}"))
                    .await;
                wrote = true;
            }
        }
    }
    if wrote {
        peer.push(hub).await;
        let retried = peer.apply(&[]).await;
        assert!(retried.held.is_empty());
    }
    wrote
}

fn orders(n: usize) -> Vec<Vec<usize>> {
    if n == 0 {
        return vec![Vec::new()];
    }
    let mut out = Vec::new();
    for rest in orders(n - 1) {
        for at in 0..=rest.len() {
            let mut order = rest.clone();
            order.insert(at, n - 1);
            out.push(order);
        }
    }
    out
}

/// Two replicas each make an open item under one parent. The maker of the item that loses
/// closes it, and its user then moves it to another parent and opens it again. A third replica
/// that receives everything in parts and in any order, some of them twice, also sees the clash;
/// if it closes the item too, its later write undoes the user's, and if only the maker decides
/// it writes nothing and the user's edit stands.
#[tokio::test]
async fn only_the_maker_of_the_losing_row_decides_and_the_users_edit_stands() {
    let mut peers = make(2, ITEMS, items).await;
    let mut hub = Vec::new();
    let mut made_by = Vec::new();
    for (i, title) in [(0, "a"), (1, "b")] {
        peers[i]
            .run(&format!(
                "INSERT INTO items (parent, open, title) VALUES ('p', 1, '{title}')"
            ))
            .await;
        made_by.push(peers[i].push(&mut hub).await);
        later();
    }
    let gids = [
        peers[0].gid("items", 1).await,
        peers[1].gid("items", 1).await,
    ];
    let decider = if gids[0] > gids[1] { 0 } else { 1 };
    let other = 1 - decider;
    let loser = gids[decider];

    let applied = peers[decider].apply(&made_by[other]).await;
    assert!(resolve(&mut peers[decider], &applied.held, Rule::Maker, &mut hub).await);
    let fix = hub[hub.len() - 1..].to_vec();
    let applied = peers[other].apply(&made_by[decider]).await;
    assert!(!resolve(&mut peers[other], &applied.held, Rule::Maker, &mut hub).await);
    peers[other].apply(&fix).await;
    later();
    peers[decider]
        .run("UPDATE items SET parent = 'q', open = 1 WHERE id = 1")
        .await;
    let edit = peers[decider].push(&mut hub).await;
    later();
    settle(&mut peers, &mut hub).await;
    let rows = "SELECT title || ' ' || parent || ' ' || open FROM items ORDER BY title";
    let expected = same_everywhere(&mut peers, rows).await;
    assert!(expected.contains(&format!("{} q 1", ["a", "b"][decider])));

    let mut parts = Vec::new();
    for batch in &made_by {
        let (head, tail) = batch.split_at(batch.len() / 2);
        parts.push(head.to_vec());
        parts.push(tail.to_vec());
    }
    parts.push(fix);
    parts.push(edit.clone());
    let mut lost = 0;
    let mut cases = 0;
    for order in orders(parts.len()) {
        // The rule that lets any holder decide is run until it is seen to lose the edit.
        let rules: &[Rule] = if lost == 0 {
            &[Rule::Maker, Rule::Holder]
        } else {
            &[Rule::Maker]
        };
        for &rule in rules {
            let mut third = Peer::new(ITEMS, items()).await;
            // Checked once settled: checking every step of every order costs more than it adds.
            third.checking = false;
            let mut sent = Vec::new();
            let mut wrote = false;
            for &i in &order {
                let applied = third.apply(&parts[i]).await;
                wrote |= resolve(&mut third, &applied.held, rule, &mut sent).await;
                if i % 2 == 0 {
                    let applied = third.apply(&parts[i]).await;
                    wrote |= resolve(&mut third, &applied.held, rule, &mut sent).await;
                }
            }
            let applied = third.apply(&hub).await;
            wrote |= resolve(&mut third, &applied.held, rule, &mut sent).await;
            third.push(&mut sent).await;
            assert_eq!(
                made(&mut third, "items", loser).await,
                Some(Made::Elsewhere)
            );
            if rule == Rule::Maker {
                cases += 1;
                assert!(!wrote && sent.is_empty(), "order {order:?} wrote {sent:?}");
                third.settled().await;
                assert_eq!(third.lines(rows).await, expected, "order {order:?}");
            } else if wrote {
                // Its close is later than the user's edit, so every replica that takes it
                // loses the edit.
                assert!(sent.iter().any(|c| c.gid == loser && c.field == "open"));
                assert!(sent.iter().all(|c| edit.iter().all(|e| c.clock > e.clock)));
                third.settled().await;
                assert_ne!(third.lines(rows).await, expected, "order {order:?}");
                lost += 1;
            }
        }
    }
    assert_eq!(cases, 720);
    assert!(
        lost > 0,
        "the rule that lets any holder decide never lost the edit"
    );
}

/// At most one open task under a parent. Whether a task falls under the index depends on its
/// parent and on its state, a set of two fields.
const TASKS: &[&str] = &[
    "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT UNIQUE)",
    "CREATE TABLE tasks (
        id INTEGER PRIMARY KEY,
        parent INTEGER REFERENCES parents(id) ON DELETE SET NULL,
        open INTEGER,
        since TEXT,
        title TEXT)",
    "CREATE UNIQUE INDEX one_open_task ON tasks (parent) WHERE open = 1",
];

fn tasks() -> Registry {
    Registry::new(vec![
        Collection::minted("parents", "parents", "id")
            .field("name")
            .identity(&["name"]),
        Collection::minted("tasks", "tasks", "id")
            .reference("parent", "parents")
            .field("open")
            .field("since")
            .field("title")
            .together(&["open", "since"]),
    ])
    .unwrap()
}

/// The fields the index reads, and the state set alone.
const INDEXED: &[&str] = &["parent", "open", "since"];
const STATE: &[&str] = &["open", "since"];

/// Each peer's answer for `gid` and `fields`.
async fn answers(peers: &mut [Peer], gid: Gid, fields: &[&str]) -> Vec<Option<bool>> {
    let mut out = Vec::new();
    for peer in peers.iter_mut() {
        out.push(latest(peer, "tasks", gid, fields).await);
    }
    out
}

/// Three replicas that hold parents `p` and `q`, made by the first.
async fn three_with_parents() -> (Vec<Peer>, Vec<Change>) {
    let mut peers = make(3, TASKS, tasks).await;
    let mut hub = Vec::new();
    peers[0]
        .run("INSERT INTO parents (name) VALUES ('p'), ('q')")
        .await;
    settle(&mut peers, &mut hub).await;
    (peers, hub)
}

#[tokio::test]
async fn the_replica_that_moved_a_task_made_the_latest_write_its_index_reads() {
    let (mut peers, mut hub) = three_with_parents().await;
    later();
    peers[0]
        .run("INSERT INTO tasks (parent, open) SELECT id, 1 FROM parents WHERE name = 'p'")
        .await;
    settle(&mut peers, &mut hub).await;
    let task = peers[0].gid("tasks", 1).await;
    later();
    peers[2]
        .run("UPDATE tasks SET parent = (SELECT id FROM parents WHERE name = 'q')")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        answers(&mut peers, task, INDEXED).await,
        [Some(false), Some(false), Some(true)]
    );
    assert_eq!(
        answers(&mut peers, task, STATE).await,
        [Some(true), Some(false), Some(false)]
    );
    // The order of the names does not matter.
    assert_eq!(
        answers(&mut peers, task, &["since", "parent", "open"]).await,
        [Some(false), Some(false), Some(true)]
    );

    // The state written after the move is the latest.
    later();
    peers[1]
        .run("UPDATE tasks SET open = 0, since = 'monday'")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        answers(&mut peers, task, INDEXED).await,
        [Some(false), Some(true), Some(false)]
    );
    assert_eq!(
        answers(&mut peers, task, &["parent"]).await,
        [Some(false), Some(false), Some(true)]
    );
}

#[tokio::test]
async fn a_field_of_a_set_counts_the_sets_latest_write_and_unwritten_fields_count_nothing() {
    let (mut peers, mut hub) = three_with_parents().await;
    later();
    // Only `open` of the state set is written, and no title.
    peers[0].run("INSERT INTO tasks (open) VALUES (0)").await;
    settle(&mut peers, &mut hub).await;
    let task = peers[0].gid("tasks", 1).await;
    assert_eq!(
        answers(&mut peers, task, &["since"]).await,
        [Some(true), Some(false), Some(false)]
    );
    for none in [&["title"][..], &["title", "parent"], &[]] {
        assert_eq!(answers(&mut peers, task, none).await, [None, None, None]);
    }
    later();
    peers[1].run("UPDATE tasks SET title = 'plan'").await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        answers(&mut peers, task, &["since", "title"]).await,
        [Some(false), Some(true), Some(false)]
    );
    later();
    peers[2].run("UPDATE tasks SET since = 'friday'").await;
    let mut state = Vec::new();
    peers[2].push(&mut state).await;
    // Half of the set's write is enough to make it the latest for every field of the set: the
    // title's writer, holding only `open` of it, no longer made the latest write.
    let open: Vec<Change> = state
        .iter()
        .filter(|c| c.field == "open")
        .cloned()
        .collect();
    assert_eq!(open.len(), 1);
    peers[1].apply(&open).await;
    assert_eq!(
        latest(&mut peers[1], "tasks", task, &["title", "since"]).await,
        Some(false)
    );
    hub.extend(state);
    settle(&mut peers, &mut hub).await;
    // The set's write is the latest, though it was `since` that changed and `open` is named.
    assert_eq!(
        answers(&mut peers, task, &["title", "open"]).await,
        [Some(false), Some(false), Some(true)]
    );
    assert_eq!(
        answers(&mut peers, task, &["title"]).await,
        [Some(false), Some(true), Some(false)]
    );
}

#[tokio::test]
async fn an_adopted_value_takes_part_like_any_other_write() {
    let mut first = Peer::bare(TASKS, tasks()).await;
    first
        .run("INSERT INTO tasks (open, title) VALUES (1, 'old')")
        .await;
    first.install().await;
    let mut peers = vec![first, Peer::new(TASKS, tasks()).await];
    let mut hub = Vec::new();
    settle(&mut peers, &mut hub).await;
    let task = peers[0].gid("tasks", 1).await;
    assert_eq!(
        answers(&mut peers, task, &["open", "title"]).await,
        [Some(true), Some(false)]
    );
    later();
    peers[1].run("UPDATE tasks SET title = 'new'").await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        answers(&mut peers, task, &["open", "title"]).await,
        [Some(false), Some(true)]
    );
    assert_eq!(
        answers(&mut peers, task, STATE).await,
        [Some(true), Some(false)]
    );
}

#[tokio::test]
async fn replicas_that_wrote_concurrently_both_answer_until_each_holds_the_others_write() {
    let (mut peers, mut hub) = three_with_parents().await;
    later();
    peers[0].run("INSERT INTO tasks (open) VALUES (0)").await;
    settle(&mut peers, &mut hub).await;
    let task = peers[0].gid("tasks", 1).await;
    later();
    peers[0].run("UPDATE tasks SET title = 'first'").await;
    later();
    peers[1].run("UPDATE tasks SET title = 'second'").await;
    let fields = &["title", "open"];
    for i in [0, 1] {
        // Writes are collected, and so counted, when the replica next steps.
        let peer = &mut peers[i];
        peer.replica.collect(&mut peer.conn).await.unwrap();
    }
    assert_eq!(
        answers(&mut peers, task, fields).await,
        [Some(true), Some(true), Some(false)]
    );
    // Once the first holds the second's write, only the second answers; the second has not
    // seen the first's and still does too.
    let mut sent = Vec::new();
    peers[1].push(&mut sent).await;
    peers[0].apply(&sent).await;
    assert_eq!(
        answers(&mut peers, task, fields).await,
        [Some(false), Some(true), Some(false)]
    );
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        answers(&mut peers, task, fields).await,
        [Some(false), Some(true), Some(false)]
    );
}

#[tokio::test]
async fn a_reference_to_a_merged_parent_counts_the_write_that_set_it() {
    let mut peers = make(3, TASKS, tasks).await;
    let mut hub = Vec::new();
    peers[0]
        .run("INSERT INTO parents (name) VALUES ('p')")
        .await;
    peers[0]
        .run("INSERT INTO tasks (parent, title) VALUES (1, 'a')")
        .await;
    later();
    peers[1]
        .run("INSERT INTO parents (name) VALUES ('p')")
        .await;
    peers[1]
        .run("INSERT INTO tasks (parent, title) VALUES (1, 'b')")
        .await;
    settle(&mut peers, &mut hub).await;
    let first = peers[0].gid("tasks", 1).await;
    let second = peers[1].gid("tasks", 1).await;
    same_everywhere(
        &mut peers,
        "SELECT t.title || ' ' || p.name FROM tasks t JOIN parents p ON p.id = t.parent
          ORDER BY t.title",
    )
    .await;
    // One parent row is given up on each replica; both tasks name the group.
    for peer in peers.iter_mut() {
        assert_eq!(peer.int("SELECT count(*) FROM parents").await, 1);
    }
    assert_eq!(
        answers(&mut peers, first, &["parent"]).await,
        [Some(true), Some(false), Some(false)]
    );
    assert_eq!(
        answers(&mut peers, second, &["parent", "open"]).await,
        [Some(false), Some(true), Some(false)]
    );
    later();
    peers[2]
        .run("UPDATE tasks SET parent = NULL WHERE title = 'a'")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        answers(&mut peers, first, &["parent", "title"]).await,
        [Some(false), Some(false), Some(true)]
    );
}

/// Who gives way when a task moves under a parent that already has an open task, and how.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Decider {
    /// The replica that made the latest write among the fields the index reads closes the task
    /// where it finds it open under that parent in its own table.
    Latest,
    /// The replica that wrote the state set, likewise.
    State,
    /// The replica that made the latest write among the fields the index reads closes its own
    /// row of the task, wherever that row stands.
    Own,
}

/// The application's resolution: of two open tasks under one parent the one with the larger gid
/// gives way and is closed, by the replica and in the way `decider` names. Returns how many it
/// closed.
async fn give_way(
    peer: &mut Peer,
    held: &[Held],
    decider: Decider,
    log: &mut Vec<Change>,
) -> usize {
    let fields = match decider {
        Decider::Latest | Decider::Own => INDEXED,
        Decider::State => STATE,
    };
    let mut closed = 0;
    for h in held.iter().filter(|h| h.collection == "tasks") {
        let merged = peer
            .replica
            .merged(&mut peer.conn, "tasks", h.gid)
            .await
            .unwrap();
        let Some(Value::Ref(parent)) = merged.get("parent") else {
            continue;
        };
        let Some(key) = peer
            .replica
            .key_for_gid(&mut peer.conn, "parents", *parent)
            .await
            .unwrap()
        else {
            continue;
        };
        let local = key.trim_matches(|c| c == '[' || c == ']');
        let open: Vec<i64> = peer
            .lines(&format!(
                "SELECT CAST(id AS TEXT) FROM tasks WHERE parent = {local} AND open = 1"
            ))
            .await
            .iter()
            .map(|id| id.parse().unwrap())
            .collect();
        for id in open {
            let other = peer.gid("tasks", id).await;
            let loser = other.max(h.gid);
            let row = match decider {
                Decider::Own => peer
                    .replica
                    .key_for_gid(&mut peer.conn, "tasks", loser)
                    .await
                    .unwrap()
                    .map(|key| key.trim_matches(|c| c == '[' || c == ']').to_string()),
                _ => (loser == other).then(|| id.to_string()),
            };
            if let Some(row) = row
                && latest(peer, "tasks", loser, fields).await == Some(true)
            {
                peer.run(&format!(
                    "UPDATE tasks SET open = 0, since = 'gave way' WHERE id = {row}"
                ))
                .await;
                closed += 1;
            }
        }
    }
    if closed > 0 {
        peer.push(log).await;
        peer.apply(&[]).await;
    }
    closed
}

/// Two replicas each make an open task under its own parent; a third, holding only the task with
/// the larger gid, moves it under the other's parent. Every replica that sees the clash runs the
/// application's resolution, in every order the five deliveries the move leaves owing can take,
/// and then everything is exchanged. Only the mover holds the moved task open under the new
/// parent, and only the latest writer among the index's fields finds it so: it gives way once
/// and every replica ends alike. Asking who wrote the state set names the task's maker, which
/// can give way only where the move reached it before the other task; in the other orders the
/// move is held there and the clash is never resolved. The latest writer closing its own row of
/// the task gives way once too.
#[tokio::test]
async fn the_latest_writer_among_the_indexed_fields_gives_way_in_every_order() {
    let mut stuck = 0;
    let mut runs = 0;
    for order in orders(5) {
        for decider in [Decider::Latest, Decider::State, Decider::Own] {
            runs += 1;
            let (mut peers, mut hub) = three_with_parents().await;
            for peer in peers.iter_mut() {
                peer.checking = false;
            }
            let mut made_by = Vec::new();
            for (i, parent) in [(0, "p"), (1, "q")] {
                later();
                peers[i]
                    .run(&format!(
                        "INSERT INTO tasks (parent, open, title)
                         SELECT id, 1, 'from {i}' FROM parents WHERE name = '{parent}'"
                    ))
                    .await;
                made_by.push(peers[i].push(&mut hub).await);
            }
            let gids = [made_by[0][0].gid, made_by[1][0].gid];
            let maker = if gids[0] > gids[1] { 0 } else { 1 };
            let other = 1 - maker;
            let to = ["q", "p"][maker];
            peers[2].apply(&made_by[maker]).await;
            later();
            peers[2]
                .run(&format!(
                    "UPDATE tasks SET parent = (SELECT id FROM parents WHERE name = '{to}')"
                ))
                .await;
            let moved = peers[2].push(&mut hub).await;
            let owed = [
                (maker, made_by[other].clone()),
                (maker, moved.clone()),
                (other, made_by[maker].clone()),
                (other, moved.clone()),
                (2, made_by[other].clone()),
            ];
            let mut gave_way = [0; 3];
            for &i in &order {
                let (to, batch) = &owed[i];
                let applied = peers[*to].apply(batch).await;
                gave_way[*to] += give_way(&mut peers[*to], &applied.held, decider, &mut hub).await;
            }
            for peer in peers.iter_mut() {
                peer.cursor = 0;
            }
            for _ in 0..6 {
                for (i, peer) in peers.iter_mut().enumerate() {
                    let applied = peer.pull(&hub).await;
                    gave_way[i] += give_way(peer, &applied.held, decider, &mut hub).await;
                    peer.push(&mut hub).await;
                }
            }
            let mut held = 0;
            for peer in peers.iter_mut() {
                held += peer.apply(&[]).await.held.len();
            }
            match decider {
                Decider::Latest | Decider::Own => {
                    assert_eq!(gave_way, [0, 0, 1], "order {order:?}");
                    assert_eq!(held, 0, "order {order:?}");
                    let rows = same_everywhere(
                        &mut peers,
                        "SELECT t.title || ' ' || p.name || ' ' || t.open FROM tasks t
                           JOIN parents p ON p.id = t.parent ORDER BY t.title",
                    )
                    .await;
                    let moved_title = format!("from {maker} {to} 0");
                    assert!(rows.contains(&moved_title), "order {order:?}: {rows:?}");
                }
                Decider::State => {
                    // The maker gives way where the move reached it before the other task.
                    let total: usize = gave_way.iter().sum();
                    assert_eq!(gave_way[2], 0, "order {order:?}");
                    assert_eq!(total + usize::from(held > 0), 1, "order {order:?}");
                    let at = |e| order.iter().position(|&i| i == e);
                    assert_eq!(held > 0, at(0) < at(1), "order {order:?}");
                    stuck += usize::from(held > 0);
                }
            }
        }
    }
    assert_eq!(runs, 360);
    assert!(
        stuck > 0,
        "deciding by the state set never left the clash held"
    );
}

/// The latest writer of a task's indexed fields reopened it, under its old parent, while a move
/// that makes it clash was on its way: there the move is held, and its own row of the task stands
/// open under the old parent, where nothing clashes. Closing only what it finds in the clash, no
/// replica gives way and the task stays held everywhere; closing its own row of the task, by key,
/// the latest writer gives way and every replica ends alike.
#[tokio::test]
async fn the_latest_writer_gives_way_on_its_own_row_held_at_an_older_state() {
    for decider in [Decider::Latest, Decider::Own] {
        let (mut peers, mut hub) = three_with_parents().await;
        later();
        for (i, title) in [(0, "a"), (1, "b")] {
            peers[i]
                .run(&format!(
                    "INSERT INTO tasks (parent, open, title)
                     SELECT id, 0, '{title}' FROM parents WHERE name = 'p'"
                ))
                .await;
        }
        settle(&mut peers, &mut hub).await;
        let gids = [
            peers[0].gid("tasks", 1).await,
            peers[1].gid("tasks", 1).await,
        ];
        let (loser, winner) = if gids[0] > gids[1] {
            ("a", "b")
        } else {
            ("b", "a")
        };
        let lost = gids[0].max(gids[1]);
        let under_q = "(SELECT id FROM parents WHERE name = 'q')";
        // The second replica opens the winner under q, and only the third receives it now.
        later();
        peers[1]
            .run(&format!(
                "UPDATE tasks SET parent = {under_q}, open = 1, since = 'm' WHERE title = '{winner}'"
            ))
            .await;
        let opened_winner = peers[1].push(&mut Vec::new()).await;
        peers[2].apply(&opened_winner).await;
        // The first moves the loser, closed, under q.
        later();
        peers[0]
            .run(&format!(
                "UPDATE tasks SET parent = {under_q} WHERE title = '{loser}'"
            ))
            .await;
        let moved = peers[0].push(&mut Vec::new()).await;
        // The third, without the move, opens the loser under p: the latest write to its fields.
        later();
        peers[2]
            .run(&format!(
                "UPDATE tasks SET open = 1, since = 'w' WHERE title = '{loser}'"
            ))
            .await;
        let opened_loser = peers[2].push(&mut Vec::new()).await;
        let mut gave_way = [0; 3];
        let mut log = Vec::new();
        for (to, batch) in [
            (2, &moved),
            (0, &opened_winner),
            (0, &opened_loser),
            (1, &moved),
            (1, &opened_loser),
        ] {
            let applied = peers[to].apply(batch).await;
            gave_way[to] += give_way(&mut peers[to], &applied.held, decider, &mut log).await;
        }
        hub.extend(
            opened_winner
                .into_iter()
                .chain(moved)
                .chain(opened_loser)
                .chain(log),
        );
        for peer in peers.iter_mut() {
            peer.cursor = 0;
        }
        for _ in 0..6 {
            for (i, peer) in peers.iter_mut().enumerate() {
                let applied = peer.pull(&hub).await;
                gave_way[i] += give_way(peer, &applied.held, decider, &mut hub).await;
                peer.push(&mut hub).await;
            }
        }
        assert_eq!(
            answers(&mut peers, lost, INDEXED).await,
            [Some(false), Some(false), Some(true)]
        );
        let mut held = Vec::new();
        for peer in peers.iter_mut() {
            held.push(peer.apply(&[]).await.held.len());
        }
        let rows = "SELECT t.title || ' ' || p.name || ' ' || t.open FROM tasks t
                      JOIN parents p ON p.id = t.parent ORDER BY t.title";
        match decider {
            Decider::Latest => {
                assert_eq!(gave_way, [0, 0, 0]);
                assert_eq!(held, [1, 1, 1]);
                // The latest writer's own row still stands at the state it wrote.
                assert_eq!(
                    peers[2]
                        .lines(&format!(
                            "SELECT p.name || ' ' || t.open FROM tasks t
                               JOIN parents p ON p.id = t.parent WHERE t.title = '{loser}'"
                        ))
                        .await,
                    ["p 1"]
                );
            }
            _ => {
                assert_eq!(gave_way, [0, 0, 1]);
                assert_eq!(held, [0, 0, 0]);
                let shown = same_everywhere(&mut peers, rows).await;
                assert!(shown.contains(&format!("{loser} q 0")), "{shown:?}");
                assert!(shown.contains(&format!("{winner} q 1")), "{shown:?}");
            }
        }
    }
}

/// The latest writer keeps a row of the group when a merge gives its own row up; a replica that
/// removes its row while a member it has not seen lives on no longer counts, and takes the live
/// member's row.
#[tokio::test]
async fn the_latest_writer_keeps_a_row_of_the_group_its_row_merged_into() {
    let mut peers = make(2, TASKS, tasks).await;
    let mut hub = Vec::new();
    peers[0]
        .run("INSERT INTO parents (name) VALUES ('p')")
        .await;
    let first = peers[0].push(&mut hub).await[0].gid;
    later();
    peers[1]
        .run("INSERT INTO parents (name) VALUES ('p')")
        .await;
    let second = peers[1].push(&mut hub).await[0].gid;
    settle(&mut peers, &mut hub).await;
    let mut written = Vec::new();
    for peer in peers.iter_mut() {
        written.push(latest(peer, "parents", first, &["name"]).await);
        let row = peer
            .replica
            .key_for_gid(&mut peer.conn, "parents", second)
            .await
            .unwrap();
        assert!(row.is_some());
        assert_eq!(peer.int("SELECT count(*) FROM parents").await, 1);
    }
    assert_eq!(written, [Some(false), Some(true)]);

    let mut peers = make(2, TASKS, tasks).await;
    let mut hub = Vec::new();
    peers[0]
        .run("INSERT INTO parents (name) VALUES ('p')")
        .await;
    let first = peers[0].push(&mut hub).await[0].gid;
    later();
    peers[1]
        .run("INSERT INTO parents (name) VALUES ('p')")
        .await;
    peers[1].push(&mut hub).await;
    later();
    peers[1].run("DELETE FROM parents").await;
    peers[1].push(&mut hub).await;
    settle(&mut peers, &mut hub).await;
    let mut written = Vec::new();
    for peer in peers.iter_mut() {
        written.push(latest(peer, "parents", first, &["name"]).await);
        assert_eq!(peer.lines("SELECT name FROM parents").await, ["p"]);
    }
    assert_eq!(written, [Some(true), Some(false)]);
}

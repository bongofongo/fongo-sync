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
    peer.replica
        .written_here(&mut peer.conn, collection, gid, field)
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

    // Nothing to answer for a field the registry does not declare, or an unknown entity.
    assert_eq!(
        written(&mut peers[0], "shelves", gid, "$deleted").await,
        None
    );
    assert_eq!(written(&mut peers[0], "shelves", gid, "width").await, None);
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

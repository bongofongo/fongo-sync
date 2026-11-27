//! Two databases that share one identity: a copied file, or a backup restored over a database
//! that went on without it. Both issue the same clocks, so two changes to one field can share a
//! clock and differ. Every replica must still show the model's view, in which the greater value
//! wins such a tie, whatever order the changes arrive in; and an apply reports what it finds of
//! a shared identity, never reporting one where a database alone holds its own.

mod support;

use fongo_sync_sqlite::core::{Change, DELETED, Gid, Strategy, Value};
use fongo_sync_sqlite::{Applied, Collection, Registry, Resume, SharedNode};
use support::peer::{Peer, same_everywhere, settle};
use support::world::World;

const SCHEMA: &[&str] = &[
    "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, pages INTEGER)",
    "CREATE TABLE tags (
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        tag TEXT NOT NULL,
        note TEXT,
        PRIMARY KEY (book_id, tag))",
    "CREATE TABLE settings (key TEXT NOT NULL PRIMARY KEY, value TEXT)",
];

fn books_only() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field_with("pages", Strategy::Max),
    ])
    .unwrap()
}

fn registry() -> Registry {
    registry_with(Strategy::Max)
}

fn registry_with(pages: Strategy) -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field_with("pages", pages),
        Collection::natural("tags", "tags", &["book_id", "tag"])
            .owner("book_id", "books")
            .field("tag")
            .field("note"),
        Collection::natural("settings", "settings", &["key"])
            .field("key")
            .field("value"),
    ])
    .unwrap()
}

const ROWS: &str = "
    SELECT 'book ' || COALESCE(title, '-') || ' ' || COALESCE(pages, '-') FROM books
    UNION ALL SELECT 'tag ' || tag || ' ' || COALESCE(note, '-') FROM tags
    UNION ALL SELECT 'setting ' || key || ' ' || COALESCE(value, '-') FROM settings
    ORDER BY 1";

/// Two peers of `world` that share one book, titled `start`.
async fn two(world: &World, hub: &mut Vec<Change>) -> Vec<Peer> {
    let mut peers = vec![
        Peer::in_world(world, 0, SCHEMA, registry()).await,
        Peer::in_world(world, 1, SCHEMA, registry()).await,
    ];
    peers[0]
        .run("INSERT INTO books (id, title, pages) VALUES (1, 'start', 10)")
        .await;
    settle(&mut peers, hub).await;
    peers
}

/// The changes `sent` holds to `field`, each with its clock.
fn to(sent: &[Change], field: &str) -> Vec<Change> {
    sent.iter().filter(|c| c.field == field).cloned().collect()
}

#[tokio::test]
async fn a_copy_and_its_original_titling_a_book_at_one_clock_both_show_the_greater_title() {
    for (first, second) in [("apple", "zebra"), ("zebra", "apple")] {
        let world = World::new(7);
        let mut hub = Vec::new();
        let mut peers = two(&world, &mut hub).await;
        world.hold(true);
        let copy = peers[0].copy(&world, 2).await;
        peers.push(copy);
        peers[0]
            .run(&format!("UPDATE books SET title = '{first}'"))
            .await;
        peers[2]
            .run(&format!("UPDATE books SET title = '{second}'"))
            .await;
        let a = to(&peers[0].push(&mut hub).await, "title");
        let b = to(&peers[2].push(&mut hub).await, "title");
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);
        assert_eq!(a[0].clock, b[0].clock, "the copies issue one clock");
        world.hold(false);
        settle(&mut peers, &mut hub).await;
        assert_eq!(
            same_everywhere(&mut peers, ROWS).await,
            ["book zebra 10"],
            "{first} first"
        );
    }
}

#[tokio::test]
async fn a_backup_restored_over_a_database_that_went_on_converges() {
    let world = World::new(11);
    let mut hub = Vec::new();
    let mut peers = two(&world, &mut hub).await;
    world.hold(true);
    let mut backup = peers[0].copy(&world, 2).await;
    peers[0].run("UPDATE books SET title = 'one'").await;
    peers[0].push(&mut hub).await;
    peers[0]
        .run("INSERT INTO tags (book_id, tag, note) VALUES (1, 'red', 'lost')")
        .await;
    peers[0].push(&mut hub).await;
    // The database is lost and its backup restored: it issues again the clocks it issued since.
    peers[0] = backup.copy(&world, 3).await;
    peers[0].run("UPDATE books SET title = 'aaa'").await;
    peers[0].run("UPDATE books SET pages = 20").await;
    peers[0].push(&mut hub).await;
    world.hold(false);
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, ROWS).await,
        ["book one 20", "tag red lost"]
    );
}

/// A change at another change's clock with another value.
fn twin(change: &Change, value: Value) -> Change {
    Change {
        value,
        ..change.clone()
    }
}

#[tokio::test]
async fn a_tombstone_tie_deletes_in_either_order() {
    let world = World::new(13);
    let mut hub = Vec::new();
    let mut peers = two(&world, &mut hub).await;
    let made = hub
        .iter()
        .find(|c| c.collection == "books" && c.field == DELETED)
        .unwrap()
        .clone();
    let mut gone = made.clone();
    gone.clock.wall_ms += 1_000_000;
    gone.value = Value::Int(1);
    let back = twin(&gone, Value::Int(0));
    peers[0].apply(&[gone.clone(), back.clone()]).await;
    peers[1].apply(&[back, gone]).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, ROWS).await,
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn a_tie_kept_for_a_collection_the_registry_lacks_is_settled_by_value_once_it_has_it() {
    let world = World::new(17);
    let mut hub = Vec::new();
    let mut peers = vec![
        Peer::in_world(&world, 0, SCHEMA, registry()).await,
        Peer::in_world(&world, 1, SCHEMA, books_only()).await,
        Peer::in_world(&world, 2, SCHEMA, books_only()).await,
    ];
    peers[0]
        .run("INSERT INTO books (id, title) VALUES (1, 'one')")
        .await;
    peers[0]
        .run("INSERT INTO tags (book_id, tag, note) VALUES (1, 'red', 'n')")
        .await;
    let sent = peers[0].push(&mut hub).await;
    let note = to(&sent, "note").remove(0);
    let greater = twin(&note, Value::Text("zz".into()));
    let smaller = twin(&note, Value::Text("a".into()));
    // Each of the two keeps the tie in the order it arrives; the greater value must win.
    peers[1].apply(&hub).await;
    peers[1].apply(&[smaller.clone(), greater.clone()]).await;
    peers[2].apply(&[greater.clone(), smaller.clone()]).await;
    peers[2].apply(&hub).await;
    peers[0].apply(&[smaller, greater]).await;
    for peer in &mut peers[1..] {
        peer.upgrade(registry()).await;
        peer.apply(&[]).await;
    }
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, ROWS).await,
        ["book one -", "tag red zz"]
    );
}

#[tokio::test]
async fn two_key_values_at_one_clock_are_read_back_as_the_greater_in_either_order() {
    let world = World::new(19);
    let mut hub = Vec::new();
    let mut peers = two(&world, &mut hub).await;
    peers[0]
        .run("INSERT INTO settings (key, value) VALUES ('k', 'v')")
        .await;
    let sent = peers[0].push(&mut hub).await;
    let key = to(&sent, "key").remove(0);
    let mut low = twin(&key, Value::Text("a".into()));
    low.clock.wall_ms += 1_000_000;
    let high = twin(&low, Value::Text("z".into()));
    peers[0].apply(&[low.clone(), high.clone()]).await;
    peers[1].apply(&hub).await;
    peers[1].apply(&[high, low]).await;
    let mut read = Vec::new();
    for peer in &mut peers {
        let merged = peer
            .replica
            .merged(&mut peer.conn, "settings", key.gid)
            .await
            .unwrap();
        read.push(merged);
    }
    assert_eq!(read[0], read[1]);
    assert_eq!(read[0]["key"], Value::Text("z".into()));
    settle(&mut peers, &mut hub).await;
    same_everywhere(&mut peers, ROWS).await;
}

/// The node each peer's clocks carry.
async fn node(peer: &mut Peer) -> Gid {
    let inspection = peer.replica.inspect(&mut peer.conn).await.unwrap();
    inspection.node.unwrap()
}

/// The nodes `applied` reports, each with whether it is the applying database's own.
fn reported(applied: &Applied) -> Vec<(Gid, bool)> {
    applied
        .shared
        .iter()
        .map(|s: &SharedNode| (s.node, s.here))
        .collect()
}

#[tokio::test]
async fn a_copy_and_its_original_each_find_the_other_and_a_third_database_finds_the_node() {
    let world = World::new(23);
    let mut hub = Vec::new();
    let mut peers = two(&world, &mut hub).await;
    let mine = node(&mut peers[0]).await;
    world.hold(true);
    let copy = peers[0].copy(&world, 2).await;
    peers.push(copy);
    peers[0].run("UPDATE books SET title = 'apple'").await;
    peers[2].run("UPDATE books SET title = 'zebra'").await;
    peers[0].push(&mut hub).await;
    peers[2].push(&mut hub).await;
    world.hold(false);
    for (i, here) in [(0, true), (1, false), (2, true)] {
        let applied = peers[i].pull(&hub).await;
        assert_eq!(reported(&applied), [(mine, here)], "peer {i}");
        assert_eq!(
            applied.shared[0].clock,
            to(&hub, "title").last().unwrap().clock
        );
        let again = peers[i].apply(&hub).await;
        assert_eq!(reported(&again), [(mine, here)], "peer {i}, again");
    }
}

#[tokio::test]
async fn a_book_a_copy_makes_shows_its_original_the_copy() {
    let world = World::new(29);
    let mut hub = Vec::new();
    let mut peers = two(&world, &mut hub).await;
    let mine = node(&mut peers[0]).await;
    let mut copy = peers[0].copy(&world, 2).await;
    copy.run("INSERT INTO books (id, title) VALUES (2, 'two')")
        .await;
    let made = copy.push(&mut hub).await;
    world.later(1_000);
    peers[0].run("UPDATE books SET pages = 30").await;
    let edited = peers[0].push(&mut hub).await;
    // The original's clock has passed the copy's: only the new book gives the copy away.
    assert!(made.iter().all(|c| c.clock < edited[0].clock));
    let applied = peers[0].pull(&hub).await;
    assert_eq!(reported(&applied), [(mine, true)]);
    let applied = peers[1].pull(&hub).await;
    assert_eq!(reported(&applied), []);
}

#[tokio::test]
async fn a_restored_backup_finds_what_the_database_it_replaced_sent() {
    let world = World::new(31);
    let mut hub = Vec::new();
    let mut peers = two(&world, &mut hub).await;
    let mine = node(&mut peers[0]).await;
    let mut backup = peers[0].copy(&world, 2).await;
    peers[0].run("UPDATE books SET title = 'one'").await;
    peers[0].push(&mut hub).await;
    peers[0] = backup.copy(&world, 3).await;
    let applied = peers[0].pull(&hub).await;
    assert_eq!(reported(&applied), [(mine, true)]);
    settle(&mut peers, &mut hub).await;
    assert_eq!(same_everywhere(&mut peers, ROWS).await, ["book one 10"]);
}

/// Applies everything `hub` holds to every peer, its own changes among them, and requires that
/// none reports a shared node.
async fn none_shared(peers: &mut [Peer], hub: &[Change], when: &str) {
    for (i, peer) in peers.iter_mut().enumerate() {
        let applied = peer.apply(hub).await;
        assert_eq!(reported(&applied), [], "peer {i}, {when}");
    }
}

#[tokio::test]
async fn a_database_alone_with_its_node_reports_nothing_whatever_it_applies_again() {
    let world = World::new(37);
    let mut hub = Vec::new();
    let mut peers = two(&world, &mut hub).await;
    none_shared(&mut peers, &hub, "at the start").await;
    for round in 0..3 {
        peers[0]
            .run(&format!(
                "UPDATE books SET title = 'a{round}', pages = pages + 1"
            ))
            .await;
        peers[1]
            .run(&format!(
                "INSERT INTO tags (book_id, tag, note) VALUES (1, 't{round}', 'n')"
            ))
            .await;
        for peer in peers.iter_mut() {
            peer.push(&mut hub).await;
        }
        none_shared(&mut peers, &hub, "after sending").await;
        peers[1].run("DELETE FROM tags").await;
        peers[1].push(&mut hub).await;
        none_shared(&mut peers, &hub, "after deleting").await;
    }
    // Resumed with what was written meanwhile as its own baseline.
    peers[0].suspend().await;
    peers[0].run("UPDATE books SET title = 'quiet'").await;
    peers[0].resume(Resume::Baseline).await;
    none_shared(&mut peers, &hub, "after a baseline resume").await;
    peers[0].push(&mut hub).await;
    none_shared(&mut peers, &hub, "after sending past a baseline").await;
    // Installed again: a new node, which adopts the rows afresh.
    let old = node(&mut peers[0]).await;
    let replica = peers[0].replica.clone();
    replica.uninstall(&mut peers[0].conn).await.unwrap();
    peers[0].taken.clear();
    peers[0].writes = Default::default();
    peers[0].install().await;
    assert_ne!(node(&mut peers[0]).await, old);
    none_shared(&mut peers, &hub, "after installing again").await;
    peers[0].push(&mut hub).await;
    none_shared(&mut peers, &hub, "after sending what it adopted").await;
    settle(&mut peers, &mut hub).await;
    same_everywhere(&mut peers, ROWS).await;
}

#[tokio::test]
async fn rows_found_at_install_are_adopted_without_a_report_whole_or_in_steps() {
    for step in [None, Some(1)] {
        let world = World::new(41);
        let mut hub = Vec::new();
        let mut peers = Vec::new();
        for n in 0..2 {
            let mut conn = world.connect(n).await;
            for sql in SCHEMA {
                sqlx::query(sql).execute(&mut conn).await.unwrap();
            }
            for sql in [
                "INSERT INTO books (id, title, pages) VALUES (1, 'found', 3)",
                "INSERT INTO tags (book_id, tag, note) VALUES (1, 'red', 'n')",
                "INSERT INTO settings (key, value) VALUES ('k', 'v')",
            ] {
                sqlx::query(sql).execute(&mut conn).await.unwrap();
            }
            let mut peer = Peer {
                conn,
                replica: fongo_sync_sqlite::Replica::new(registry()),
                cursor: 0,
                taken: Vec::new(),
                checking: true,
                suspended: false,
                writes: Default::default(),
            };
            match step {
                None => peer.install().await,
                Some(rows) => {
                    let replica = peer.replica.clone();
                    while replica
                        .install_step(&mut peer.conn, rows)
                        .await
                        .unwrap()
                        .more
                    {
                        while replica
                            .collect_step(&mut peer.conn, rows)
                            .await
                            .unwrap()
                            .more
                        {}
                    }
                }
            }
            peers.push(peer);
        }
        for peer in peers.iter_mut() {
            let sent = peer.push(&mut hub).await;
            assert!(sent.iter().any(|c| c.clock.wall_ms == 0), "{step:?}");
        }
        none_shared(&mut peers, &hub, &format!("adopted, steps {step:?}")).await;
        peers[1].run("UPDATE books SET title = 'edited'").await;
        peers[1].push(&mut hub).await;
        none_shared(&mut peers, &hub, &format!("edited, steps {step:?}")).await;
        settle(&mut peers, &mut hub).await;
        same_everywhere(&mut peers, ROWS).await;
    }
}

#[tokio::test]
async fn a_field_whose_strategy_changed_reports_nothing_when_its_own_writes_arrive_again() {
    let world = World::new(43);
    let mut hub = Vec::new();
    let mut peers = two(&world, &mut hub).await;
    // `pages` merges by clock here, so the later, smaller value wins.
    for peer in peers.iter_mut() {
        peer.upgrade(registry_with(Strategy::Lww)).await;
    }
    peers[0].run("UPDATE books SET pages = 50").await;
    peers[0].push(&mut hub).await;
    world.later(10);
    peers[0].run("UPDATE books SET pages = 5").await;
    peers[0].push(&mut hub).await;
    // Under the greatest value the earlier write would win: applied again, it changes what is
    // kept, though this database made it.
    peers[0].checking = false;
    peers[0].upgrade(registry()).await;
    let applied = peers[0].apply(&hub).await;
    assert_eq!(reported(&applied), []);
    peers[0].checking = true;
}

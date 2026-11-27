//! Configurations of the application's schema, connection and registry outside the shape the
//! other tests use, each in the smallest setting that shows whether replicas still converge: every
//! peer checks itself against the model after each apply and collect, every write the
//! application made must be sent, nothing it did not write may be sent, and the end of each test
//! requires every replica to show the model's view of all changes.

mod support;

use std::ffi::{CString, c_int, c_void};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

use fongo_sync_sqlite::core::model;
use fongo_sync_sqlite::core::{Change, DELETED, Strategy};
use fongo_sync_sqlite::{
    Collection, Error, LocalDefault, Made, Registry, Replica, Resume, installed,
};
use libsqlite3_sys as ffi;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};
use support::peer::{Peer, same_everywhere, settle};
use support::world::World;

const LIBRARY: &[&str] = &[
    "CREATE TABLE shelves (id INTEGER PRIMARY KEY, name TEXT)",
    "CREATE TABLE books (
        id INTEGER PRIMARY KEY,
        shelf_id INTEGER NOT NULL REFERENCES shelves(id) ON DELETE CASCADE,
        title TEXT,
        status TEXT,
        archived INTEGER,
        isbn TEXT UNIQUE)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        body TEXT)",
];

fn library() -> Registry {
    Registry::new(vec![
        Collection::minted("shelves", "shelves", "id").field("name"),
        Collection::minted("books", "books", "id")
            .owner("shelf_id", "shelves")
            .field("title")
            .field("status")
            .field("archived")
            .field("isbn")
            .identity(&["isbn"]),
        Collection::minted("notes", "notes", "id")
            .owner("book_id", "books")
            .field("body"),
    ])
    .unwrap()
}

const LIBRARY_ROWS: &str = "
    SELECT 'shelf ' || name FROM shelves
    UNION ALL SELECT 'book ' || b.title || ' on ' || s.name || ' ' || COALESCE(b.status, '-')
           || ' ' || COALESCE(b.archived, '-')
      FROM books b JOIN shelves s ON s.id = b.shelf_id
    UNION ALL SELECT 'note ' || n.body || ' on ' || b.title
      FROM notes n JOIN books b ON b.id = n.book_id
    ORDER BY 1";

const ITEMS: &[&str] =
    &["CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT UNIQUE, title TEXT)"];

fn items() -> Registry {
    Registry::new(vec![
        Collection::minted("items", "items", "id")
            .field("code")
            .field("title"),
    ])
    .unwrap()
}

const ITEM_ROWS: &str =
    "SELECT COALESCE(code, '-') || ' ' || COALESCE(title, '-') FROM items ORDER BY 1";

async fn peers_in(world: &World, n: u64, schema: &[&str], registry: fn() -> Registry) -> Vec<Peer> {
    let mut peers = Vec::new();
    for i in 0..n {
        peers.push(Peer::in_world(world, i, schema, registry()).await);
    }
    peers
}

/// A shelf `S` holding book `A` with note `n1`, on every peer.
async fn shelved(world: &World, hub: &mut Vec<Change>) -> Vec<Peer> {
    let mut peers = peers_in(world, 2, LIBRARY, library).await;
    peers[0]
        .run("INSERT INTO shelves (id, name) VALUES (1, 'S')")
        .await;
    peers[0]
        .run("INSERT INTO books (id, shelf_id, title, archived) VALUES (1, 1, 'A', 0)")
        .await;
    peers[0]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'n1')")
        .await;
    settle(&mut peers, hub).await;
    world.later(5);
    peers
}

// REPLACE that removes a row at another key.

async fn replace_at_another_key(statement: &str, recursive: bool) {
    let world = World::new(1);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, ITEMS, items).await;
    if recursive {
        for peer in &mut peers {
            peer.run("PRAGMA recursive_triggers = ON").await;
        }
    }
    peers[0]
        .run("INSERT INTO items (id, code, title) VALUES (1, 'X', 'a'), (2, 'Y', 'b')")
        .await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[0].run(statement).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, ITEM_ROWS).await,
        ["X b2"],
        "{statement}, recursive_triggers {recursive}"
    );
}

const REPLACING_INSERTS: [&str; 2] = [
    "INSERT OR REPLACE INTO items (id, code, title) VALUES (2, 'X', 'b2')",
    "REPLACE INTO items (id, code, title) VALUES (2, 'X', 'b2')",
];

const REPLACING_UPDATE: &str = "UPDATE OR REPLACE items SET code = 'X', title = 'b2' WHERE id = 2";

#[tokio::test]
#[ignore = "known fault: a row REPLACE removes at another key is never marked without recursive_triggers, so its delete is never sent"]
async fn an_insert_that_replaces_a_row_at_another_key_sends_its_delete() {
    for statement in REPLACING_INSERTS {
        replace_at_another_key(statement, false).await;
    }
}

#[tokio::test]
#[ignore = "known fault: a row REPLACE removes at another key is never marked without recursive_triggers, so its delete is never sent"]
async fn an_update_that_replaces_a_row_at_another_key_sends_its_delete() {
    replace_at_another_key(REPLACING_UPDATE, false).await;
}

#[tokio::test]
async fn with_recursive_triggers_an_insert_that_replaces_a_row_at_another_key_sends_its_delete() {
    for statement in REPLACING_INSERTS {
        replace_at_another_key(statement, true).await;
    }
}

#[tokio::test]
async fn with_recursive_triggers_an_update_that_replaces_a_row_at_another_key_sends_its_delete() {
    replace_at_another_key(REPLACING_UPDATE, true).await;
}

// Natural keys holding a blob or a real.

#[tokio::test]
#[ignore = "known fault: the triggers put a natural key in json_array, which refuses a blob, so the application's own insert fails"]
async fn a_natural_key_holding_a_blob_is_written_and_synced() {
    let schema = &["CREATE TABLE devices (uuid BLOB NOT NULL PRIMARY KEY, name TEXT)"];
    let registry = || {
        Registry::new(vec![
            Collection::natural("devices", "devices", &["uuid"])
                .field("uuid")
                .field("name"),
        ])
        .unwrap()
    };
    let world = World::new(2);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, registry).await;
    peers[0]
        .run("INSERT INTO devices (uuid, name) VALUES (x'00ff10', 'phone')")
        .await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[1].run("UPDATE devices SET name = 'tablet'").await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, "SELECT hex(uuid) || ' ' || name FROM devices").await,
        ["00FF10 tablet"]
    );
}

#[tokio::test]
#[ignore = "known fault: json_array writes a real natural key with 15 digits, so a key needing more reads back as no row and is sent deleted"]
async fn a_natural_key_holding_a_real_keeps_every_digit() {
    let schema = &["CREATE TABLE points (x REAL NOT NULL PRIMARY KEY, label TEXT)"];
    let registry = || {
        Registry::new(vec![
            Collection::natural("points", "points", &["x"])
                .field("x")
                .field("label"),
        ])
        .unwrap()
    };
    let world = World::new(3);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, registry).await;
    peers[0]
        .run("INSERT INTO points (x, label) VALUES (0.1 + 0.2, 'a'), (1.0 / 3, 'b'), (0.5, 'c')")
        .await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[1].run("UPDATE points SET label = label || '!'").await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(
            &mut peers,
            "SELECT printf('%!.17g', x) || ' ' || label FROM points ORDER BY x"
        )
        .await,
        ["0.30000000000000004 a!", "0.33333333333333331 b!", "0.5 c!"]
    );
}

// The rowid changed through its own name.

#[tokio::test]
#[ignore = "known fault: UPDATE ... SET rowid does not name the key column, so the UPDATE OF trigger does not fire and the move is never sent"]
async fn a_key_changed_through_the_rowid_is_sent() {
    for name in ["rowid", "oid", "_rowid_"] {
        let world = World::new(4);
        let mut hub = Vec::new();
        let mut peers = peers_in(&world, 2, ITEMS, items).await;
        peers[0]
            .run("INSERT INTO items (id, code, title) VALUES (1, 'X', 'a')")
            .await;
        settle(&mut peers, &mut hub).await;
        world.later(5);
        peers[0]
            .run(&format!("UPDATE items SET {name} = 10 WHERE id = 1"))
            .await;
        settle(&mut peers, &mut hub).await;
        assert_eq!(
            same_everywhere(&mut peers, ITEM_ROWS).await,
            ["X a"],
            "{name}"
        );
        assert_eq!(
            peers[0].lines("SELECT CAST(id AS TEXT) FROM items").await,
            ["10"]
        );
    }
}

// What the application's triggers write while the kernel writes.

#[tokio::test]
#[ignore = "known fault: capture is silenced for the whole apply, so a delete the application's trigger makes in response is never sent"]
async fn a_delete_an_application_trigger_makes_during_an_apply_is_sent() {
    let world = World::new(5);
    let mut hub = Vec::new();
    let mut peers = shelved(&world, &mut hub).await;
    for peer in &mut peers {
        peer.run(
            "CREATE TRIGGER archive_notes AFTER UPDATE OF archived ON books
             WHEN NEW.archived = 1 BEGIN DELETE FROM notes WHERE book_id = NEW.id; END",
        )
        .await;
    }
    peers[1]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'n2')")
        .await;
    peers[1].push(&mut hub).await;
    peers[0].run("UPDATE books SET archived = 1").await;
    peers[0].push(&mut hub).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        ["book A on S - 1", "shelf S"]
    );
}

#[tokio::test]
#[ignore = "known fault: capture is silenced for the whole apply, so a field the application's trigger sets in response is never sent"]
async fn a_field_an_application_trigger_sets_during_an_apply_is_sent() {
    let world = World::new(6);
    let mut hub = Vec::new();
    let mut peers = shelved(&world, &mut hub).await;
    for peer in &mut peers {
        peer.run(
            "CREATE TRIGGER archive_closes AFTER UPDATE OF archived ON books
             WHEN NEW.archived = 1 BEGIN UPDATE books SET status = 'closed' WHERE id = NEW.id; END",
        )
        .await;
    }
    peers[0].run("UPDATE books SET archived = 1").await;
    peers[0].push(&mut hub).await;
    world.later(5);
    peers[1].run("UPDATE books SET status = 'reading'").await;
    peers[1].push(&mut hub).await;
    world.later(5);
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        ["book A on S closed 1", "note n1 on A", "shelf S"]
    );
}

// A field a replica's registry lacks, declared later.

#[tokio::test]
#[ignore = "known fault: a replica whose registry lacks a field keeps only its latest write, so a later registry merging it by greatest value shows that one"]
async fn a_field_unknown_when_its_changes_arrived_is_merged_by_its_later_strategy() {
    let schema = &["CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, pages INTEGER)"];
    let wide = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id")
                .field("title")
                .field_with("pages", Strategy::Max),
        ])
        .unwrap()
    };
    let narrow = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id").field("title"),
        ])
        .unwrap()
    };
    let world = World::new(7);
    let mut hub = Vec::new();
    let mut a = Peer::in_world(&world, 0, schema, wide()).await;
    let mut b = Peer::in_world(&world, 1, schema, narrow()).await;
    world.hold(true);
    a.run("INSERT INTO books (id, title, pages) VALUES (1, 'T', 500)")
        .await;
    a.push(&mut hub).await;
    a.run("UPDATE books SET pages = 700").await;
    a.push(&mut hub).await;
    world.hold(false);
    world.later(5);
    a.run("UPDATE books SET pages = 300").await;
    a.push(&mut hub).await;
    b.pull(&hub).await;
    b.upgrade(wide()).await;
    let mut peers = vec![a, b];
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, "SELECT title || ' ' || pages FROM books").await,
        ["T 700"]
    );
}

#[tokio::test]
async fn a_field_unknown_when_its_changes_arrived_can_join_the_key() {
    let schema = &["CREATE TABLE marks (
        code TEXT NOT NULL, kind TEXT NOT NULL, label TEXT, PRIMARY KEY (code, kind))"];
    let wide = || {
        Registry::new(vec![
            Collection::natural("marks", "marks", &["code", "kind"])
                .field("code")
                .field("kind")
                .field("label"),
        ])
        .unwrap()
    };
    let narrow = || {
        Registry::new(vec![
            Collection::natural("marks", "marks", &["code"])
                .field("code")
                .field("label"),
        ])
        .unwrap()
    };
    let world = World::new(8);
    let mut hub = Vec::new();
    let mut a = Peer::in_world(&world, 0, schema, wide()).await;
    let mut b = Peer::in_world(&world, 1, schema, narrow()).await;
    world.hold(true);
    a.run("INSERT INTO marks VALUES ('x', 'a', 'one'), ('x', 'b', 'two')")
        .await;
    a.push(&mut hub).await;
    a.run("UPDATE marks SET label = 'three' WHERE kind = 'b'")
        .await;
    a.push(&mut hub).await;
    world.hold(false);
    b.pull(&hub).await;
    b.upgrade(wide()).await;
    let mut peers = vec![a, b];
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(
            &mut peers,
            "SELECT code || ' ' || kind || ' ' || label FROM marks ORDER BY 1"
        )
        .await,
        ["x a one", "x b three"]
    );
}

// AUTOINCREMENT.

#[tokio::test]
#[ignore = "known fault: the kernel picks a local id from the largest in use, so it gives a row from elsewhere an id AUTOINCREMENT retired"]
async fn an_autoincrement_id_is_not_given_again() {
    let schema = &[
        "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT UNIQUE, title TEXT)",
        "CREATE TABLE opened (item_id INTEGER)",
    ];
    let world = World::new(9);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, items).await;
    peers[1]
        .run("INSERT INTO items (title) VALUES ('a'), ('b'), ('c')")
        .await;
    peers[1].run("INSERT INTO opened VALUES (3)").await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[1].run("DELETE FROM items WHERE id = 3").await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[0]
        .run("INSERT INTO items (title) VALUES ('new')")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, ITEM_ROWS).await,
        ["- a", "- b", "- new"]
    );
    assert_eq!(
        peers[1]
            .lines("SELECT i.title FROM opened o JOIN items i ON i.id = o.item_id")
            .await,
        Vec::<String>::new(),
        "a row from elsewhere took id 3, which this replica's own table still names"
    );
}

// A natural key compared without case.

const NOCASE: &[&str] =
    &["CREATE TABLE tags (tag TEXT NOT NULL COLLATE NOCASE PRIMARY KEY, n INTEGER)"];

fn tags() -> Registry {
    Registry::new(vec![
        Collection::natural("tags", "tags", &["tag"])
            .field("tag")
            .field("n"),
    ])
    .unwrap()
}

#[tokio::test]
#[ignore = "known fault: the kernel finds a natural key through the column's collation, so a NOCASE key renamed in case loses its row"]
async fn a_natural_key_compared_without_case_renamed_in_case_converges() {
    let world = World::new(10);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, NOCASE, tags).await;
    peers[0]
        .run("INSERT INTO tags (tag, n) VALUES ('Rust', 1)")
        .await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[0].run("UPDATE tags SET tag = 'rust'").await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, "SELECT tag || ' ' || n FROM tags").await,
        ["rust 1"]
    );
}

#[tokio::test]
#[ignore = "known fault: two keys differing only in case are two entities to the kernel and one row to a NOCASE table, so one waits for ever"]
async fn natural_keys_differing_only_in_case_made_on_two_replicas_converge() {
    let world = World::new(11);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, NOCASE, tags).await;
    peers[0]
        .run("INSERT INTO tags (tag, n) VALUES ('Rust', 1)")
        .await;
    peers[1]
        .run("INSERT INTO tags (tag, n) VALUES ('rust', 2)")
        .await;
    settle(&mut peers, &mut hub).await;
    same_everywhere(&mut peers, "SELECT tag || ' ' || n FROM tags ORDER BY 1").await;
}

// Type affinity converting a key value.

#[tokio::test]
async fn text_written_to_an_integer_natural_key_is_one_entity() {
    let schema = &["CREATE TABLE codes (code INTEGER NOT NULL UNIQUE, label TEXT)"];
    let world = World::new(12);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, codes).await;
    peers[0]
        .run("INSERT INTO codes (code, label) VALUES ('42', 'a')")
        .await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[1].run("UPDATE codes SET label = 'b'").await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(
            &mut peers,
            "SELECT typeof(code) || ' ' || code || ' ' || label FROM codes"
        )
        .await,
        ["integer 42 b"]
    );
}

fn codes() -> Registry {
    Registry::new(vec![
        Collection::natural("codes", "codes", &["code"])
            .field("code")
            .field("label"),
    ])
    .unwrap()
}

/// Every live row of `collection` in the model of everything the peers have taken in.
fn model_rows(peers: &[Peer], collection: &str) -> Vec<model::Row> {
    let everything: Vec<Change> = peers.iter().flat_map(|p| p.taken.iter().cloned()).collect();
    let schema = peers[0].replica.registry().model_schema();
    let view = model::view(&schema, &everything);
    view.rows(collection).map(|(_, row)| row.clone()).collect()
}

/// A key written as text where another replica's column holds integers. The model compares the
/// key exactly, so the check is on what each table shows and what the model makes live.
#[tokio::test]
#[ignore = "known fault: the kernel records a natural key from the value it binds, the triggers from the value the column stores, so an edit makes a second entity"]
async fn a_key_one_replica_holds_as_text_and_another_as_an_integer_is_one_entity() {
    let world = World::new(13);
    let mut hub = Vec::new();
    let mut peers = vec![
        Peer::in_world(
            &world,
            0,
            &["CREATE TABLE codes (code TEXT NOT NULL UNIQUE, label TEXT)"],
            codes(),
        )
        .await,
        Peer::in_world(
            &world,
            1,
            &["CREATE TABLE codes (code INTEGER NOT NULL UNIQUE, label TEXT)"],
            codes(),
        )
        .await,
    ];
    for peer in &mut peers {
        peer.checking = false;
    }
    peers[0]
        .run("INSERT INTO codes (code, label) VALUES ('42', 'a')")
        .await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[1].run("UPDATE codes SET label = 'b'").await;
    settle(&mut peers, &mut hub).await;
    for (i, peer) in peers.iter_mut().enumerate() {
        assert_eq!(
            peer.lines("SELECT code || ' ' || label FROM codes").await,
            ["42 b"],
            "peer {i}"
        );
    }
    let live = model_rows(&peers, "codes");
    assert_eq!(live.len(), 1, "the model shows {live:?}");
}

// The application's triggers during its own writes.

#[tokio::test]
async fn what_an_application_trigger_writes_during_a_local_write_is_sent() {
    let world = World::new(14);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, LIBRARY, library).await;
    for sql in [
        "CREATE TRIGGER first_note AFTER INSERT ON books BEGIN
             INSERT INTO notes (book_id, body) VALUES (NEW.id, 'first');
             UPDATE books SET status = 'new' WHERE id = NEW.id; END",
        "CREATE TRIGGER put_away AFTER UPDATE OF archived ON books WHEN NEW.archived = 1 BEGIN
             DELETE FROM notes WHERE book_id = NEW.id;
             UPDATE shelves SET name = name || '*' WHERE id = NEW.shelf_id; END",
    ] {
        peers[0].run(sql).await;
    }
    peers[0]
        .run("INSERT INTO shelves (id, name) VALUES (1, 'S')")
        .await;
    peers[0]
        .run("INSERT INTO books (id, shelf_id, title, archived) VALUES (1, 1, 'A', 0)")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        ["book A on S new 0", "note first on A", "shelf S"]
    );
    world.later(5);
    peers[0].run("UPDATE books SET archived = 1").await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        ["book A on S* new 1", "shelf S*"]
    );
}

// A synced table in an attached database.

#[tokio::test]
#[ignore = "known fault: install refuses a table in an attached database only with an untyped SQLite error from creating its triggers"]
async fn a_table_in_an_attached_database_is_refused_at_install() {
    let world = World::new(15);
    let mut conn = world.connect(0).await;
    for sql in [
        "ATTACH ':memory:' AS aux",
        "CREATE TABLE aux.items (id INTEGER PRIMARY KEY, code TEXT UNIQUE, title TEXT)",
    ] {
        sqlx::query(sql).execute(&mut conn).await.unwrap();
    }
    let replica = Replica::new(items());
    let result = replica.install(&mut conn).await;
    assert!(result.is_err());
    assert!(!installed(&mut conn).await.unwrap());
    let triggers: i64 =
        sqlx::query_scalar("SELECT count(*) FROM aux.sqlite_master WHERE type = 'trigger'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(triggers, 0);
    assert!(
        matches!(result, Err(Error::Registry(_))),
        "install answered {result:?}"
    );
}

// Write-ahead logging.

struct Wall(Arc<AtomicI64>);

unsafe extern "C" fn wall_seconds(
    ctx: *mut ffi::sqlite3_context,
    _: c_int,
    _: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite passes the user data this function was registered with.
    let wall = unsafe { &*ffi::sqlite3_user_data(ctx).cast::<Wall>() };
    let seconds = (wall.0.load(Ordering::SeqCst) as f64 + 0.5) / 1000.0;
    unsafe { ffi::sqlite3_result_double(ctx, seconds) };
}

unsafe extern "C" fn drop_wall(wall: *mut c_void) {
    // SAFETY: SQLite passes back the pointer it was given, once.
    drop(unsafe { Box::from_raw(wall.cast::<Wall>()) });
}

/// Replaces `unixepoch` on `conn` with a clock that reads `wall`, in milliseconds.
async fn set_wall(conn: &mut SqliteConnection, wall: &Arc<AtomicI64>) {
    let mut handle = support::lock(conn).await;
    let name = CString::new("unixepoch").unwrap();
    let source = Box::into_raw(Box::new(Wall(wall.clone())));
    // SAFETY: the handle is open and locked; SQLite owns the source from here.
    let status = unsafe {
        ffi::sqlite3_create_function_v2(
            handle.as_raw_handle().as_ptr(),
            name.as_ptr(),
            1,
            ffi::SQLITE_UTF8 | ffi::SQLITE_INNOCUOUS,
            source.cast::<c_void>(),
            Some(wall_seconds),
            None,
            None,
            Some(drop_wall),
        )
    };
    assert_eq!(status, ffi::SQLITE_OK);
}

/// A replica over a database file in `journal` mode, whose wall clock stands still at `wall`
/// until the test moves it, so every write between two moves shares a millisecond.
async fn file_peer(path: &PathBuf, journal: &str, wall: &Arc<AtomicI64>) -> Peer {
    let mut conn = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .foreign_keys(true)
        .connect()
        .await
        .unwrap();
    set_wall(&mut conn, wall).await;
    let mode: String = sqlx::query_scalar(&format!("PRAGMA journal_mode = {journal}"))
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(mode.to_lowercase(), journal.to_lowercase());
    for sql in LIBRARY {
        sqlx::query(sql).execute(&mut conn).await.unwrap();
    }
    let mut peer = Peer {
        conn,
        replica: Replica::new(library()),
        cursor: 0,
        taken: Vec::new(),
        checking: true,
        suspended: false,
        writes: Default::default(),
    };
    peer.install().await;
    peer
}

async fn converge_in(journal: &str) -> Vec<String> {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "configs-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let wall = Arc::new(AtomicI64::new(1_700_000_000_000));
    let later = || {
        wall.fetch_add(5, Ordering::SeqCst);
    };
    let mut peers = Vec::new();
    for n in 0..3 {
        peers.push(file_peer(&dir.join(format!("{n}.db")), journal, &wall).await);
    }
    let mut hub = Vec::new();
    peers[0]
        .run("INSERT INTO shelves (id, name) VALUES (1, 'S')")
        .await;
    peers[0]
        .run("INSERT INTO books (id, shelf_id, title) VALUES (1, 1, 'A'), (2, 1, 'B')")
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[0]
        .run("UPDATE books SET title = 'A0' WHERE title = 'A'")
        .await;
    peers[1]
        .run("UPDATE books SET status = 'reading' WHERE title = 'A'")
        .await;
    later();
    peers[1]
        .run("UPDATE books SET title = 'A1' WHERE title = 'A'")
        .await;
    peers[2].run("DELETE FROM books WHERE title = 'B'").await;
    peers[0]
        .run("INSERT INTO books (shelf_id, title, isbn) VALUES (1, 'C0', '9')")
        .await;
    later();
    peers[1]
        .run("INSERT INTO books (shelf_id, title, isbn) VALUES (1, 'C1', '9')")
        .await;
    settle(&mut peers, &mut hub).await;
    let rows = same_everywhere(&mut peers, LIBRARY_ROWS).await;
    drop(peers);
    std::fs::remove_dir_all(&dir).unwrap();
    rows
}

#[tokio::test]
async fn replicas_in_write_ahead_logging_converge_as_in_the_default_mode() {
    let wal = converge_in("WAL").await;
    let default = converge_in("DELETE").await;
    assert_eq!(wal, default);
    assert_eq!(
        wal,
        ["book A1 on S reading -", "book C1 on S - -", "shelf S"]
    );
}

// recursive_triggers on, with deletes cascading through owners.

#[tokio::test]
async fn with_recursive_triggers_a_cascading_delete_is_sent_once_and_whole() {
    let world = World::new(16);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 3, LIBRARY, library).await;
    for peer in &mut peers {
        peer.run("PRAGMA recursive_triggers = ON").await;
    }
    peers[0]
        .run("INSERT INTO shelves (id, name) VALUES (1, 'S'), (2, 'T')")
        .await;
    peers[0]
        .run("INSERT INTO books (id, shelf_id, title) VALUES (1, 1, 'A'), (2, 1, 'B'), (3, 2, 'C')")
        .await;
    peers[0]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'a1'), (1, 'a2'), (2, 'b1'), (3, 'c1')")
        .await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[1]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'a3')")
        .await;
    peers[1].push(&mut hub).await;
    peers[0].run("DELETE FROM shelves WHERE name = 'S'").await;
    let sent = peers[0].push(&mut hub).await;
    let mut deletes: Vec<_> = sent
        .iter()
        .filter(|c| c.field == DELETED)
        .map(|c| (c.collection.clone(), c.gid))
        .collect();
    let all = deletes.len();
    deletes.sort();
    deletes.dedup();
    assert_eq!(deletes.len(), all, "a delete was sent twice: {sent:?}");
    assert!(
        sent.iter()
            .any(|c| c.collection == "shelves" && c.field == DELETED)
    );
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        ["book C on T - -", "note c1 on C", "shelf T"]
    );
}

// A panic inside a kernel call.

/// Counts the progress handler's calls on `conn`, panicking at the `at`th, as a callback the
/// application gave SQLite may; sqlx catches the panic and SQLite interrupts the statement.
async fn arm(conn: &mut SqliteConnection, at: usize) -> Arc<AtomicUsize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let mut handle = support::lock(conn).await;
    handle.set_progress_handler(100, move || {
        assert_ne!(
            counted.fetch_add(1, Ordering::SeqCst) + 1,
            at,
            "the handler panics"
        );
        true
    });
    calls
}

async fn disarm(conn: &mut SqliteConnection) {
    support::lock(conn).await.remove_progress_handler();
}

/// Applies with a panic at the `at`th progress call, in the application's own transaction or in
/// none, then writes and syncs. Returns how many progress calls the apply made.
async fn apply_stopped_at(at: usize, in_transaction: bool) -> usize {
    let world = World::new(17);
    let mut hub = Vec::new();
    let mut peers = shelved(&world, &mut hub).await;
    peers[0]
        .run("INSERT INTO books (shelf_id, title, isbn) VALUES (1, 'B', '7')")
        .await;
    peers[0].run("UPDATE notes SET body = 'n1!'").await;
    peers[0].push(&mut hub).await;
    world.later(5);
    let p = &mut peers[1];
    if in_transaction {
        p.run("BEGIN IMMEDIATE").await;
    }
    let batch = hub[p.cursor..].to_vec();
    p.writes.before(&mut p.conn, &p.replica, &p.taken).await;
    let calls = arm(&mut p.conn, at).await;
    let result = p.replica.apply(&mut p.conn, &batch).await;
    disarm(&mut p.conn).await;
    let calls = calls.load(Ordering::SeqCst);
    match &result {
        Ok(_) => {
            p.taken.extend(batch);
            p.cursor = hub.len();
            p.writes.after(&mut p.conn, &p.replica).await;
        }
        Err(e) => assert!(calls >= at, "the apply failed before the panic: {e:?}"),
    }
    p.run("INSERT INTO shelves (name) VALUES ('mine')").await;
    p.run("UPDATE books SET status = 'kept' WHERE title = 'A'")
        .await;
    if in_transaction {
        let committed = sqlx::query("COMMIT").execute(&mut p.conn).await;
        if let Err(e) = committed {
            assert!(
                result.is_err() && e.to_string().contains("no transaction is active"),
                "{e}"
            );
        }
    }
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        [
            "book A on S kept 0",
            "book B on S - -",
            "note n1! on A",
            "shelf S",
            "shelf mine"
        ],
        "stopped at progress call {at} of {calls}, in a transaction: {in_transaction}"
    );
    calls
}

async fn a_panic_inside_an_apply_leaves_capture_working(in_transaction: bool) {
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|info| {
        if !info.to_string().contains("the handler panics") {
            eprintln!("{info}");
        }
    }));
    let total = apply_stopped_at(usize::MAX, in_transaction).await;
    assert!(total > 10, "the apply made only {total} progress calls");
    for at in (1..=total).step_by(total.div_ceil(24)) {
        apply_stopped_at(at, in_transaction).await;
    }
    std::panic::set_hook(hook);
}

#[tokio::test]
async fn a_panic_inside_an_apply_leaves_capture_working_outside_a_transaction() {
    a_panic_inside_an_apply_leaves_capture_working(false).await;
}

#[tokio::test]
async fn a_panic_inside_an_apply_leaves_capture_working_inside_the_applications_transaction() {
    a_panic_inside_an_apply_leaves_capture_working(true).await;
}

// A wall clock at or before 1970.

async fn writes_before_the_epoch(start: i64) {
    let wall = Arc::new(AtomicI64::new(start));
    let mut conn = SqliteConnectOptions::from_str("sqlite::memory:")
        .unwrap()
        .foreign_keys(true)
        .connect()
        .await
        .unwrap();
    set_wall(&mut conn, &wall).await;
    for sql in LIBRARY {
        sqlx::query(sql).execute(&mut conn).await.unwrap();
    }
    let mut early = Peer {
        conn,
        replica: Replica::new(library()),
        cursor: 0,
        taken: Vec::new(),
        checking: true,
        suspended: false,
        writes: Default::default(),
    };
    early.install().await;
    let world = World::new(40);
    let mut honest = Peer::in_world(&world, 1, LIBRARY, library()).await;
    let mut hub = Vec::new();
    early
        .run("INSERT INTO shelves (id, name) VALUES (1, 'S')")
        .await;
    early
        .run("INSERT INTO books (id, shelf_id, title) VALUES (1, 1, 'A')")
        .await;
    let mut sent = early.push(&mut hub).await;
    early.run("UPDATE books SET title = 'B'").await;
    sent.extend(early.push(&mut hub).await);
    let clocks: Vec<_> = sent
        .iter()
        .filter(|c| c.collection == "books" && c.field == "title")
        .map(|c| c.clock)
        .collect();
    assert!(
        clocks.len() == 2 && clocks[0] < clocks[1],
        "wall {start}: two writes of one field were sent at {clocks:?}"
    );
    let gid = early.gid("books", 1).await;
    let made = early
        .replica
        .made(&mut early.conn, "books", gid)
        .await
        .unwrap();
    honest.pull(&hub).await;
    world.later(5);
    honest.run("UPDATE books SET title = 'H'").await;
    honest.push(&mut hub).await;
    early.pull(&hub).await;
    early.run("UPDATE books SET title = 'E'").await;
    let mut peers = vec![early, honest];
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        ["book E on S - -", "shelf S"],
        "wall {start}"
    );
    assert_eq!(made, Some(Made::Here), "wall {start}");
}

#[tokio::test]
#[ignore = "known fault: a trigger reading of exactly 0 ms is taken for a found row, so the write is sent at an adoption clock and loses to every honest write"]
async fn writes_made_at_the_epoch_are_made_here_and_ordered() {
    writes_before_the_epoch(0).await;
}

#[tokio::test]
#[ignore = "known fault: a making stamped before the epoch is reported as adopted, as Made::Adopted documents"]
async fn writes_made_before_the_epoch_are_made_here_and_ordered() {
    writes_before_the_epoch(-5000).await;
}

// Further configurations: what install does not check of the tables against the registry.

#[tokio::test]
#[ignore = "known fault: install accepts a minted key column that holds text, and every collect then fails as corrupt"]
async fn a_minted_key_column_holding_text_is_synced() {
    let schema = &["CREATE TABLE devices (id TEXT PRIMARY KEY, name TEXT)"];
    let registry = || {
        Registry::new(vec![
            Collection::minted("devices", "devices", "id").field("name"),
        ])
        .unwrap()
    };
    let world = World::new(20);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, registry).await;
    peers[0]
        .run("INSERT INTO devices (id, name) VALUES ('abc', 'phone')")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, "SELECT name FROM devices").await,
        ["phone"]
    );
}

#[tokio::test]
#[ignore = "known fault: install accepts a natural key no unique index holds, and two rows at one key are one entity whose second value is never sent"]
async fn a_natural_key_no_index_holds_unique_is_one_row() {
    let schema = &["CREATE TABLE prefs (key TEXT NOT NULL, value TEXT)"];
    let registry = || {
        Registry::new(vec![
            Collection::natural("prefs", "prefs", &["key"])
                .field("key")
                .field("value"),
        ])
        .unwrap()
    };
    let world = World::new(21);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, registry).await;
    peers[0]
        .run("INSERT INTO prefs (key, value) VALUES ('a', '1'), ('a', '2')")
        .await;
    settle(&mut peers, &mut hub).await;
    same_everywhere(&mut peers, "SELECT key || value FROM prefs ORDER BY 1").await;
}

#[tokio::test]
#[ignore = "known fault: install accepts two collections over one table, and each replica makes a row of each entity of both"]
async fn two_collections_over_one_table_keep_one_row() {
    let schema = &["CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT)"];
    let registry = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id").field("title"),
            Collection::minted("titles", "books", "id").field("title"),
        ])
        .unwrap()
    };
    let world = World::new(22);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, registry).await;
    peers[0].run("INSERT INTO books (title) VALUES ('A')").await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, "SELECT title FROM books").await,
        ["A"]
    );
}

#[tokio::test]
#[ignore = "known fault: install accepts a local dependent whose table does not exist"]
async fn a_local_dependent_naming_no_table_is_refused_at_install() {
    let world = World::new(23);
    let mut conn = world.connect(0).await;
    sqlx::query("CREATE TABLE books (id INTEGER PRIMARY KEY, isbn TEXT UNIQUE)")
        .execute(&mut conn)
        .await
        .unwrap();
    let replica = Replica::new(
        Registry::new(vec![
            Collection::minted("books", "books", "id")
                .field("isbn")
                .identity(&["isbn"])
                .local_dependent("readings", "book_id"),
        ])
        .unwrap(),
    );
    let result = replica.install(&mut conn).await;
    assert!(
        matches!(result, Err(Error::Registry(_))),
        "install answered {result:?}"
    );
}

#[tokio::test]
#[ignore = "known fault: install accepts a local default that does not prepare, and every row from elsewhere then waits for ever"]
async fn a_local_default_that_does_not_prepare_does_not_hold_rows_for_ever() {
    let schema =
        &["CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, seen INTEGER NOT NULL)"];
    let registry = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id")
                .field("title")
                .local_default("seen", LocalDefault::Sql("SELEC 1".into())),
        ])
        .unwrap()
    };
    let world = World::new(24);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, registry).await;
    peers[0]
        .run("INSERT INTO books (title, seen) VALUES ('A', 0)")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, "SELECT title FROM books").await,
        ["A"]
    );
}

#[tokio::test]
async fn references_whose_foreign_key_acts_otherwise_than_the_registry_says_converge() {
    let schema = &[
        "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT)",
        "CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            book_id INTEGER REFERENCES books(id) ON DELETE SET NULL,
            body TEXT)",
        "CREATE TABLE marks (
            id INTEGER PRIMARY KEY,
            book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
            body TEXT)",
    ];
    let registry = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id").field("title"),
            Collection::minted("notes", "notes", "id")
                .owner("book_id", "books")
                .field("body"),
            Collection::minted("marks", "marks", "id")
                .reference("book_id", "books")
                .field("body"),
        ])
        .unwrap()
    };
    let world = World::new(25);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, registry).await;
    peers[0]
        .run("INSERT INTO books (id, title) VALUES (1, 'A')")
        .await;
    peers[0]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'n')")
        .await;
    peers[0]
        .run("INSERT INTO marks (book_id, body) VALUES (1, 'm')")
        .await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[0].run("DELETE FROM books").await;
    settle(&mut peers, &mut hub).await;
    same_everywhere(
        &mut peers,
        "SELECT 'note ' || body FROM notes UNION ALL SELECT 'mark ' || body FROM marks ORDER BY 1",
    )
    .await;
}

#[tokio::test]
#[ignore = "known fault: text that is not UTF-8 is read lossily, so other replicas receive replacement characters"]
async fn text_that_is_not_utf8_is_sent_as_written() {
    let world = World::new(26);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, LIBRARY, library).await;
    for peer in &mut peers {
        peer.checking = false;
    }
    peers[0]
        .run("INSERT INTO shelves (name) VALUES (CAST(x'ff41' AS TEXT))")
        .await;
    settle(&mut peers, &mut hub).await;
    for (i, peer) in peers.iter_mut().enumerate() {
        assert_eq!(
            peer.lines("SELECT hex(name) FROM shelves").await,
            ["FF41"],
            "peer {i}"
        );
    }
}

#[tokio::test]
#[ignore = "known fault: the kernel names tables unqualified, so a temporary table of the same name takes the rows it writes"]
async fn a_temporary_table_named_as_a_synced_one_is_not_written_in_its_place() {
    let world = World::new(27);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, ITEMS, items).await;
    peers[1]
        .run("CREATE TEMP TABLE items (id INTEGER PRIMARY KEY, code TEXT UNIQUE, title TEXT)")
        .await;
    peers[0]
        .run("INSERT INTO items (code, title) VALUES ('X', 'a')")
        .await;
    settle(&mut peers, &mut hub).await;
    peers[1].run("DROP TABLE temp.items").await;
    assert_eq!(same_everywhere(&mut peers, ITEM_ROWS).await, ["X a"]);
}

#[tokio::test]
#[ignore = "known fault: the kernel picks the largest local id plus one, which overflows past i64::MAX, so a row from elsewhere waits for ever"]
async fn a_row_from_elsewhere_finds_an_id_when_the_largest_is_taken() {
    let world = World::new(28);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, ITEMS, items).await;
    peers[1]
        .run("INSERT INTO items (id, title) VALUES (1, 'one'), (9223372036854775807, 'max')")
        .await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[0]
        .run("INSERT INTO items (title) VALUES ('new')")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, ITEM_ROWS).await,
        ["- max", "- new", "- one"]
    );
}

#[tokio::test]
#[ignore = "known fault: a row moved into a natural key another row left is sent as edits of the old entity, so a concurrent delete of that key removes it"]
async fn a_row_moved_into_a_natural_key_another_row_left_converges() {
    let schema = &["CREATE TABLE tags (tag TEXT NOT NULL PRIMARY KEY, n INTEGER)"];
    let world = World::new(29);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, tags).await;
    peers[0]
        .run("INSERT INTO tags (tag, n) VALUES ('a', 1), ('c', 3)")
        .await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[1].run("DELETE FROM tags WHERE tag = 'a'").await;
    peers[1].push(&mut hub).await;
    world.later(5);
    peers[0]
        .run("UPDATE tags SET tag = 'b' WHERE tag = 'a'")
        .await;
    peers[0]
        .run("UPDATE tags SET tag = 'a' WHERE tag = 'c'")
        .await;
    settle(&mut peers, &mut hub).await;
    same_everywhere(&mut peers, "SELECT tag || ' ' || n FROM tags ORDER BY 1").await;
}

// Further configurations: a registry that changes between installs.

fn without_notes() -> Registry {
    let mut collections = library().collections().to_vec();
    collections.retain(|c| c.name != "notes");
    Registry::new(collections).unwrap()
}

fn without_status() -> Registry {
    let mut collections = library().collections().to_vec();
    for c in &mut collections {
        c.fields.retain(|f| f.name != "status");
    }
    Registry::new(collections).unwrap()
}

#[tokio::test]
async fn writes_to_a_collection_left_out_of_the_registry_are_sent_on_resuming() {
    let world = World::new(30);
    let mut hub = Vec::new();
    let mut peers = shelved(&world, &mut hub).await;
    peers[1].checking = false;
    peers[1].upgrade(without_notes()).await;
    peers[1].run("UPDATE notes SET body = 'edited'").await;
    peers[1]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'n2')")
        .await;
    world.later(5);
    peers[1].replica = Replica::new(library());
    let p = &mut peers[1];
    let result = p.replica.install(&mut p.conn).await;
    assert!(matches!(result, Err(Error::Suspended)), "{result:?}");
    peers[1].resume(Resume::Send).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        [
            "book A on S - 0",
            "note edited on A",
            "note n2 on A",
            "shelf S"
        ]
    );
}

#[tokio::test]
#[ignore = "known fault: a write to a field a registry left out is adopted at the oldest clock when the field returns, so it is lost"]
async fn a_write_to_a_field_left_out_of_the_registry_is_sent_when_it_returns() {
    let world = World::new(31);
    let mut hub = Vec::new();
    let mut peers = shelved(&world, &mut hub).await;
    peers[0].run("UPDATE books SET status = 'old'").await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[1].checking = false;
    peers[1].upgrade(without_status()).await;
    peers[1].run("UPDATE books SET status = 'new'").await;
    world.later(5);
    peers[1].upgrade(library()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        ["book A on S new 0", "note n1 on A", "shelf S"]
    );
}

#[tokio::test]
#[ignore = "known fault: a plain field declared a reference later keeps integer values, which no replica reads as the row they named"]
async fn a_plain_field_that_becomes_a_reference_converges() {
    let schema = &[
        "CREATE TABLE shelves (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, shelf INTEGER)",
    ];
    let plain = || {
        Registry::new(vec![
            Collection::minted("shelves", "shelves", "id").field("name"),
            Collection::minted("books", "books", "id")
                .field("title")
                .field("shelf"),
        ])
        .unwrap()
    };
    let referring = || {
        Registry::new(vec![
            Collection::minted("shelves", "shelves", "id").field("name"),
            Collection::minted("books", "books", "id")
                .field("title")
                .reference("shelf", "shelves"),
        ])
        .unwrap()
    };
    let world = World::new(32);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, plain).await;
    peers[0]
        .run("INSERT INTO shelves (id, name) VALUES (5, 'S')")
        .await;
    peers[0]
        .run("INSERT INTO books (title, shelf) VALUES ('A', 5)")
        .await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    for peer in &mut peers {
        peer.checking = false;
        peer.upgrade(referring()).await;
    }
    peers[0].run("UPDATE books SET title = 'A2'").await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(
            &mut peers,
            "SELECT b.title || ' ' || COALESCE(s.name, '-') FROM books b
               LEFT JOIN shelves s ON s.id = b.shelf"
        )
        .await,
        ["A2 S"]
    );
}

#[tokio::test]
#[ignore = "known fault: a reference declared an owner later leaves rows whose owner is gone until something touches them"]
async fn a_reference_that_becomes_an_owner_takes_rows_whose_owner_is_gone() {
    let schema = &[
        "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT)",
        "CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
            body TEXT)",
    ];
    fn notes(owned: bool) -> Registry {
        let notes = Collection::minted("notes", "notes", "id").field("body");
        let notes = if owned {
            notes.owner("book_id", "books")
        } else {
            notes.reference("book_id", "books")
        };
        Registry::new(vec![
            Collection::minted("books", "books", "id").field("title"),
            notes,
        ])
        .unwrap()
    }
    let world = World::new(33);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, || notes(false)).await;
    peers[0]
        .run("INSERT INTO books (id, title) VALUES (1, 'A')")
        .await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    peers[1]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'n')")
        .await;
    peers[1].push(&mut hub).await;
    peers[0].run("DELETE FROM books").await;
    peers[0].push(&mut hub).await;
    settle(&mut peers, &mut hub).await;
    for peer in &mut peers {
        peer.checking = false;
        peer.upgrade(notes(true)).await;
    }
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, "SELECT body FROM notes").await,
        Vec::<String>::new()
    );
}

#[tokio::test]
#[ignore = "known fault: an identity declared later does not join rows that already share it until something touches them"]
async fn an_identity_added_later_joins_rows_that_already_share_it() {
    let schema = &["CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, isbn TEXT)"];
    fn books(identity: bool) -> Registry {
        let books = Collection::minted("books", "books", "id")
            .field("title")
            .field("isbn");
        let books = if identity {
            books.identity(&["isbn"])
        } else {
            books
        };
        Registry::new(vec![books]).unwrap()
    }
    let world = World::new(34);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, || books(false)).await;
    peers[0]
        .run("INSERT INTO books (title, isbn) VALUES ('C0', '9')")
        .await;
    world.later(5);
    peers[1]
        .run("INSERT INTO books (title, isbn) VALUES ('C1', '9')")
        .await;
    settle(&mut peers, &mut hub).await;
    for peer in &mut peers {
        peer.checking = false;
        peer.upgrade(books(true)).await;
    }
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, "SELECT title FROM books").await,
        ["C1"]
    );
}

#[tokio::test]
#[ignore = "known fault: a collect under a registry lacking a collection drops its marks, so writes to it are never sent"]
async fn a_collect_under_a_narrower_registry_loses_no_write() {
    let world = World::new(35);
    let mut hub = Vec::new();
    let mut peers = shelved(&world, &mut hub).await;
    peers[0]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'n2')")
        .await;
    let narrow = Replica::new(without_notes());
    match narrow.collect(&mut peers[0].conn).await {
        Ok(_) | Err(Error::Registry(_)) => {}
        Err(other) => panic!("{other:?}"),
    }
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        ["book A on S - 0", "note n1 on A", "note n2 on A", "shelf S"]
    );
}

#[tokio::test]
#[ignore = "known fault: a natural key that gains a field leaves rows mapped under the old key, which then wait for ever"]
async fn a_natural_key_that_gains_a_field_converges() {
    let schema = &["CREATE TABLE marks (
        code TEXT NOT NULL, kind TEXT NOT NULL, label TEXT, PRIMARY KEY (code, kind))"];
    fn marks(key: &[&str]) -> Registry {
        Registry::new(vec![
            Collection::natural("marks", "marks", key)
                .field("code")
                .field("kind")
                .field("label"),
        ])
        .unwrap()
    }
    let world = World::new(36);
    let mut hub = Vec::new();
    let mut peers = peers_in(&world, 2, schema, || marks(&["code"])).await;
    peers[0]
        .run("INSERT INTO marks VALUES ('x', 'a', 'one')")
        .await;
    settle(&mut peers, &mut hub).await;
    world.later(5);
    for peer in &mut peers {
        peer.checking = false;
        peer.upgrade(marks(&["code", "kind"])).await;
    }
    peers[0].run("UPDATE marks SET label = 'two'").await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(
            &mut peers,
            "SELECT code || ' ' || kind || ' ' || label FROM marks"
        )
        .await,
        ["x a two"]
    );
}

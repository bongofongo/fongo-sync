//! Ways the SQLite layer has been seen to depart from the reference model, each in the smallest
//! setting that shows it. Every peer checks itself against the model after each apply and
//! collect, and the end of each test requires every replica to show the model's view. A test
//! marked `#[ignore]` is one the kernel still fails, with the reason.

mod support;

use fongo_sync_sqlite::core::{Change, DELETED, Gid, Hlc, Value};
use fongo_sync_sqlite::{Collection, Registry, Resume};
use support::peer::{Peer, later, make, same_everywhere, settle};

const SHELVES: &[&str] = &[
    "CREATE TABLE shelves (id INTEGER PRIMARY KEY, name TEXT)",
    "CREATE TABLE books (
        id INTEGER PRIMARY KEY,
        shelf_id INTEGER NOT NULL REFERENCES shelves(id) ON DELETE CASCADE,
        title TEXT,
        status TEXT,
        isbn TEXT UNIQUE)",
];

fn shelves() -> Registry {
    Registry::new(vec![
        Collection::minted("shelves", "shelves", "id").field("name"),
        Collection::minted("books", "books", "id")
            .owner("shelf_id", "shelves")
            .field("title")
            .field("status")
            .field("isbn")
            .identity(&["isbn"]),
    ])
    .unwrap()
}

const SHELF_ROWS: &str = "
    SELECT 'book ' || b.title || ' on ' || s.name || ' ' || COALESCE(b.status, '-')
      FROM books b JOIN shelves s ON s.id = b.shelf_id
    UNION ALL SELECT 'shelf ' || name FROM shelves ORDER BY 1";

/// Two peers that share a shelf with one book.
async fn shelved(hub: &mut Vec<Change>) -> Vec<Peer> {
    let mut peers = make(2, SHELVES, shelves).await;
    peers[0]
        .run("INSERT INTO shelves (id, name) VALUES (1, 'S')")
        .await;
    peers[0]
        .run("INSERT INTO books (id, shelf_id, title) VALUES (1, 1, 'A')")
        .await;
    settle(&mut peers, hub).await;
    later();
    peers
}

/// The second peer takes a book's move to a new shelf, and its new title, without the shelf.
async fn moved_without_its_shelf(peers: &mut [Peer], hub: &mut Vec<Change>) -> Vec<Change> {
    peers[0]
        .run("INSERT INTO shelves (id, name) VALUES (2, 'T')")
        .await;
    peers[0]
        .run("UPDATE books SET shelf_id = 2, title = 'B'")
        .await;
    let sent = peers[0].push(hub).await;
    let (books, shelves): (Vec<Change>, Vec<Change>) =
        sent.into_iter().partition(|c| c.collection == "books");
    peers[1].apply(&books).await;
    later();
    shelves
}

// What the kernel holds back is not the application's to send.

/// While the book's new shelf has not arrived, the row still shows the old shelf and title. An
/// unrelated edit there must not send them as fresh writes, or every replica moves the book back.
#[tokio::test]
#[ignore = "the kernel sends a held row's stale fields as edits, reverting the move everywhere"]
async fn an_edit_to_a_row_whose_new_owner_has_not_arrived_sends_only_the_edit() {
    let mut hub = Vec::new();
    let mut peers = shelved(&mut hub).await;
    let shelf = moved_without_its_shelf(&mut peers, &mut hub).await;
    peers[1].run("UPDATE books SET status = 'reading'").await;
    peers[1].push(&mut hub).await;
    peers[1].apply(&shelf).await;
    peers[1].cursor = hub.len();
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, SHELF_ROWS).await,
        ["book B on T reading", "shelf S", "shelf T"]
    );
}

/// The same, with the book deleted where its move is held: the delete must reach everyone.
#[tokio::test]
#[ignore = "the kernel sends nothing for a held row the application deletes, so the delete is lost"]
async fn a_delete_of_a_row_whose_new_owner_has_not_arrived_is_sent() {
    let mut hub = Vec::new();
    let mut peers = shelved(&mut hub).await;
    let shelf = moved_without_its_shelf(&mut peers, &mut hub).await;
    peers[1].run("DELETE FROM books").await;
    peers[1].push(&mut hub).await;
    peers[1].apply(&shelf).await;
    peers[1].cursor = hub.len();
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, SHELF_ROWS).await,
        ["shelf S", "shelf T"]
    );
}

/// The same with whole logs, one per device, read in different orders: the third device makes a
/// shelf, the first moves a book onto it, and the second reads the first's log before the
/// third's. All replicas agree on the end state, so only the model shows it is wrong.
#[tokio::test]
#[ignore = "the kernel sends a held row's stale fields as edits, reverting the move everywhere"]
async fn logs_read_one_device_before_another_do_not_revert_a_move() {
    let mut peers = make(3, SHELVES, shelves).await;
    let mut shared = Vec::new();
    peers[0]
        .run("INSERT INTO shelves (id, name) VALUES (1, 'S')")
        .await;
    peers[0]
        .run("INSERT INTO books (id, shelf_id, title) VALUES (1, 1, 'A')")
        .await;
    settle(&mut peers, &mut shared).await;
    later();
    let (mut first, mut second, mut third) = (Vec::new(), Vec::new(), Vec::new());
    peers[2]
        .run("INSERT INTO shelves (name) VALUES ('T')")
        .await;
    peers[2].push(&mut third).await;
    peers[0].apply(&third).await;
    later();
    peers[0]
        .run("UPDATE books SET shelf_id = (SELECT id FROM shelves WHERE name = 'T'), title = 'B'")
        .await;
    peers[0].push(&mut first).await;
    peers[1].apply(&first).await;
    later();
    peers[1].run("UPDATE books SET status = 'reading'").await;
    peers[1].push(&mut second).await;
    peers[1].apply(&third).await;
    peers[0].apply(&second).await;
    peers[2].apply(&first).await;
    peers[2].apply(&second).await;
    for peer in peers.iter_mut() {
        peer.cursor = shared.len();
    }
    settle(&mut peers, &mut shared).await;
    assert_eq!(
        same_everywhere(&mut peers, SHELF_ROWS).await,
        ["book B on T reading", "shelf S", "shelf T"]
    );
}

// Applying while the triggers are off.

/// A write made while suspended is the application's, sent on resuming as asked; an apply in
/// the meantime must neither overwrite it nor fail.
#[tokio::test]
#[ignore = "an apply while suspended overwrites the application's write to the row"]
async fn an_apply_while_suspended_keeps_the_applications_write() {
    for how in [Resume::Send, Resume::Baseline] {
        let mut hub = Vec::new();
        let mut peers = shelved(&mut hub).await;
        peers[1].suspend().await;
        peers[1].run("UPDATE books SET title = 'quiet'").await;
        peers[0].run("UPDATE books SET status = 'done'").await;
        peers[0].push(&mut hub).await;
        peers[1].pull(&hub).await;
        assert_eq!(
            peers[1].lines("SELECT title FROM books").await,
            ["quiet"],
            "{how:?}"
        );
        peers[1].resume(how).await;
        settle(&mut peers, &mut hub).await;
        let rows = same_everywhere(&mut peers, SHELF_ROWS).await;
        if how == Resume::Send {
            assert_eq!(rows, ["book quiet on S done", "shelf S"]);
        }
    }
}

/// While suspended, the application deletes a book; a book made elsewhere arrives and must not
/// be mistaken for the one deleted.
#[tokio::test]
#[ignore = "an apply while suspended can put a new entity at a local id the application freed"]
async fn an_apply_while_suspended_into_a_freed_id() {
    let mut hub = Vec::new();
    let mut peers = make(2, SHELVES, shelves).await;
    peers[0]
        .run("INSERT INTO shelves (id, name) VALUES (1, 'S')")
        .await;
    peers[0]
        .run("INSERT INTO books (id, shelf_id, title) VALUES (1, 1, 'A'), (2, 1, 'B')")
        .await;
    settle(&mut peers, &mut hub).await;
    peers[1].suspend().await;
    peers[1].run("DELETE FROM books WHERE id = 2").await;
    peers[0]
        .run("INSERT INTO books (id, shelf_id, title) VALUES (3, 1, 'C')")
        .await;
    peers[0].push(&mut hub).await;
    peers[1].pull(&hub).await;
    peers[1].resume(Resume::Send).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, SHELF_ROWS).await,
        ["book A on S -", "book C on S -", "shelf S"]
    );
}

// Registries that gain a collection.

const TAGGED: &[&str] = &[
    "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT)",
    "CREATE TABLE tags (
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        tag TEXT NOT NULL,
        note TEXT,
        PRIMARY KEY (book_id, tag))",
    "CREATE TABLE marks (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        place INTEGER)",
];

fn books_only() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id").field("title"),
    ])
    .unwrap()
}

fn tagged() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id").field("title"),
        Collection::natural("tags", "tags", &["book_id", "tag"])
            .owner("book_id", "books")
            .field("tag")
            .field("note"),
        Collection::minted("marks", "marks", "id")
            .owner("book_id", "books")
            .field("place"),
    ])
    .unwrap()
}

const TAGGED_ROWS: &str = "
    SELECT 'book ' || title FROM books
    UNION ALL SELECT 'tag ' || tag || ' ' || COALESCE(note, '-') FROM tags
    UNION ALL SELECT 'mark ' || place FROM marks ORDER BY 1";

/// One peer knows tags and marks; the other learns of them later, having held their changes.
async fn before_tags(hub: &mut Vec<Change>) -> Vec<Peer> {
    let mut peers = vec![
        Peer::new(TAGGED, tagged()).await,
        Peer::new(TAGGED, books_only()).await,
    ];
    peers[0]
        .run("INSERT INTO books (id, title) VALUES (1, 'one')")
        .await;
    peers[0]
        .run("INSERT INTO tags (book_id, tag, note) VALUES (1, 'red', 'n')")
        .await;
    peers[0]
        .run("INSERT INTO marks (book_id, place) VALUES (1, 7)")
        .await;
    settle(&mut peers, hub).await;
    peers
}

#[tokio::test]
#[ignore = "a collection the registry gains never reaches the rows of what arrived before"]
async fn a_collection_added_later_reaches_the_rows() {
    let mut hub = Vec::new();
    let mut peers = before_tags(&mut hub).await;
    peers[1].upgrade(tagged()).await;
    peers[1].apply(&[]).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, TAGGED_ROWS).await,
        ["book one", "mark 7", "tag red n"]
    );
}

#[tokio::test]
#[ignore = "resuming with a collection the registry gained deletes its natural-key entities"]
async fn a_collection_added_while_suspended_reaches_the_rows() {
    for how in [Resume::Send, Resume::Baseline] {
        let mut hub = Vec::new();
        let mut peers = before_tags(&mut hub).await;
        peers[1].suspend().await;
        peers[1].replica = fongo_sync_sqlite::Replica::new(tagged());
        peers[1].resume(how).await;
        settle(&mut peers, &mut hub).await;
        assert_eq!(
            same_everywhere(&mut peers, TAGGED_ROWS).await,
            ["book one", "mark 7", "tag red n"],
            "{how:?}"
        );
    }
}

// Rows a merge gave up.

const PINNED: &[&str] = &[
    "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, isbn TEXT UNIQUE)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        body TEXT)",
    "CREATE TABLE pins (book_id INTEGER NOT NULL UNIQUE REFERENCES books(id))",
];

fn pinned() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("isbn")
            .identity(&["isbn"])
            .local_dependent("pins", "book_id"),
        Collection::minted("notes", "notes", "id")
            .owner("book_id", "books")
            .field("body"),
    ])
    .unwrap()
}

const PINNED_ROWS: &str = "
    SELECT 'book ' || title || ' ' || COALESCE(isbn, '-') FROM books
    UNION ALL SELECT 'note ' || n.body || ' on ' || b.title
      FROM notes n JOIN books b ON b.id = n.book_id ORDER BY 1";

/// Two books take one isbn on two replicas; a third replica, which pins both books in a table
/// the kernel does not know, merges them and must keep the row it gives up. Returns that row's
/// local id.
async fn leftover(peers: &mut [Peer], hub: &mut Vec<Change>) -> i64 {
    peers[0]
        .run("INSERT INTO books (title, isbn) VALUES ('one', 'x'), ('two', 'y')")
        .await;
    settle(peers, hub).await;
    peers[1].run("INSERT INTO pins SELECT id FROM books").await;
    later();
    peers[0]
        .run("UPDATE books SET isbn = 'w' WHERE title = 'one'")
        .await;
    peers[0].push(hub).await;
    peers[2]
        .run("UPDATE books SET isbn = 'w' WHERE title = 'two'")
        .await;
    peers[2].push(hub).await;
    let applied = peers[1].pull(hub).await;
    let inspection = peers[1].replica.inspect(&mut peers[1].conn).await.unwrap();
    let (_, key) = inspection
        .leftover
        .first()
        .expect("a row is left over")
        .clone();
    assert!(applied.held.iter().any(|h| h.key.as_deref() == Some(&key)));
    key.trim_matches(|c| c == '[' || c == ']').parse().unwrap()
}

#[tokio::test]
#[ignore = "a new row at a left-over row's freed local id is never sent, then deleted"]
async fn a_new_row_at_a_left_over_rows_id_is_an_entity_of_its_own() {
    let mut hub = Vec::new();
    let mut peers = make(3, PINNED, pinned).await;
    let left = leftover(&mut peers, &mut hub).await;
    peers[1]
        .run(&format!("DELETE FROM pins WHERE book_id = {left}"))
        .await;
    peers[1]
        .run(&format!("DELETE FROM books WHERE id = {left}"))
        .await;
    peers[1]
        .run(&format!(
            "INSERT INTO books (id, title, isbn) VALUES ({left}, 'new', 'q')"
        ))
        .await;
    settle(&mut peers, &mut hub).await;
    let rows = same_everywhere(&mut peers, PINNED_ROWS).await;
    assert!(rows.contains(&"book new q".to_string()), "{rows:?}");
}

#[tokio::test]
#[ignore = "a left-over row whose group is deleted is held for ever"]
async fn a_left_over_row_whose_group_is_deleted_goes_once_let_go() {
    let mut hub = Vec::new();
    let mut peers = make(3, PINNED, pinned).await;
    let left = leftover(&mut peers, &mut hub).await;
    peers[1]
        .run(&format!("DELETE FROM pins WHERE book_id <> {left}"))
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[0].run("DELETE FROM books WHERE isbn = 'w'").await;
    settle(&mut peers, &mut hub).await;
    peers[1]
        .run(&format!("DELETE FROM pins WHERE book_id = {left}"))
        .await;
    let applied = peers[1].apply(&[]).await;
    assert!(applied.held.is_empty(), "{:#?}", applied.held);
    assert!(same_everywhere(&mut peers, PINNED_ROWS).await.is_empty());
}

// A database written by an earlier kernel.

const CURRENT: &[&str] = &[
    "CREATE TABLE books (
        id INTEGER PRIMARY KEY, title TEXT, isbn TEXT NOT NULL UNIQUE, current INTEGER)",
    "CREATE UNIQUE INDEX one_current ON books (current) WHERE current = 1",
];

fn current() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("isbn")
            .field("current")
            .identity(&["isbn"]),
    ])
    .unwrap()
}

/// A kernel that did not record what it left in rows held one with a stand-in for its isbn. The
/// previous layout is made here from the present one by dropping that record, which is all the
/// upgrade has to recover from. An edit after the upgrade must send only the edit.
#[tokio::test]
#[ignore = "after an upgrade from the previous layout, a held row's stand-in is sent"]
async fn a_row_held_by_an_earlier_kernel_sends_only_its_edit() {
    for _ in 0..40 {
        let mut hub = Vec::new();
        let mut peers = make(3, CURRENT, current).await;
        peers[0]
            .run(
                "INSERT INTO books (id, title, isbn)
                 VALUES (1, 'one', 'x'), (2, 'two', 'y'), (3, 'three', 'z')",
            )
            .await;
        settle(&mut peers, &mut hub).await;
        later();
        peers[0]
            .run("UPDATE books SET isbn = 'p', current = 1 WHERE title = 'one'")
            .await;
        peers[0]
            .run("UPDATE books SET isbn = 'x' WHERE title = 'two'")
            .await;
        peers[0].push(&mut hub).await;
        peers[2]
            .run("UPDATE books SET current = 1 WHERE title = 'three'")
            .await;
        peers[2].push(&mut hub).await;
        let applied = peers[1].pull(&hub).await;
        let parked = peers[1]
            .int("SELECT count(*) FROM books WHERE isbn LIKE 'fongo:%'")
            .await;
        if applied.held.is_empty() || parked == 0 {
            continue;
        }
        peers[1].run("DELETE FROM fongo_deviations").await;
        peers[1]
            .run("UPDATE fongo_meta SET value = 1 WHERE key = 'layout'")
            .await;
        peers[1].install().await;
        later();
        peers[1]
            .run("UPDATE books SET title = title || '!' WHERE isbn LIKE 'fongo:%'")
            .await;
        let sent = peers[1].push(&mut hub).await;
        assert!(sent.iter().all(|c| c.field == "title"), "{sent:#?}");
        settle(&mut peers, &mut hub).await;
        same_everywhere(&mut peers, "SELECT title FROM books ORDER BY 1").await;
        return;
    }
    panic!("no row was ever held with a stand-in");
}

// Changes no honest replica makes.

const LIBRARY: &[&str] = &[
    "CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)",
    "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER,
                         isbn TEXT UNIQUE)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        body TEXT)",
];

fn library() -> Registry {
    Registry::new(vec![
        Collection::minted("authors", "authors", "id").field("name"),
        Collection::minted("books", "books", "id")
            .field("title")
            .reference("author_id", "authors")
            .field("isbn")
            .identity(&["isbn"]),
        Collection::minted("notes", "notes", "id")
            .owner("book_id", "books")
            .field("body"),
    ])
    .unwrap()
}

const LIBRARY_ROWS: &str = "
    SELECT 'author ' || a.name FROM authors a
    UNION ALL SELECT 'book ' || b.title || ' by ' || COALESCE(a.name, '-')
      FROM books b LEFT JOIN authors a ON a.id = b.author_id
    UNION ALL SELECT 'note ' || n.body FROM notes n
    ORDER BY 1";

fn hostile(collection: &str, gid: Gid, field: &str, value: Value) -> Change {
    Change {
        collection: collection.into(),
        gid,
        field: field.into(),
        value,
        clock: Hlc {
            wall_ms: 1_900_000_000_000,
            counter: 0,
            node: Gid([9; 16]),
        },
    }
}

async fn library_with_a_note(hub: &mut Vec<Change>) -> (Vec<Peer>, Gid, Gid) {
    let mut peers = make(2, LIBRARY, library).await;
    peers[0]
        .run("INSERT INTO authors (id, name) VALUES (1, 'Eliot')")
        .await;
    peers[0]
        .run("INSERT INTO books (id, title, author_id) VALUES (1, 'Middlemarch', 1)")
        .await;
    peers[0]
        .run("INSERT INTO notes (id, book_id, body) VALUES (1, 1, 'slow')")
        .await;
    settle(&mut peers, hub).await;
    let book = peers[0].gid("books", 1).await;
    let note = peers[0].gid("notes", 1).await;
    (peers, book, note)
}

/// A gid belongs to the collection that used it first, so a reference to it from a field that
/// names another collection names nothing: the reference reads as null, and a row owned through
/// it is gone, on a replica that had the row and on one that joins later alike.
#[tokio::test]
#[ignore = "the kernel does not give gids to collections, so it waits on such references for ever"]
async fn a_reference_to_a_gid_of_another_collection_names_nothing() {
    let mut hub = Vec::new();
    let (mut peers, book, note) = library_with_a_note(&mut hub).await;
    hub.push(hostile("books", book, "author_id", Value::Ref(note)));
    hub.push(hostile("notes", note, "book_id", Value::Ref(note)));
    settle(&mut peers, &mut hub).await;
    peers.push(Peer::new(LIBRARY, library()).await);
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        ["author Eliot", "book Middlemarch by -"]
    );
}

/// The model counts an entity live unless its tombstone says otherwise, so an entity known only
/// from a field change shows, with that field. Every replica writes a tombstone when it makes an
/// entity, so only a replica that does not can send this; the kernel waits for the tombstone.
#[tokio::test]
#[ignore = "the kernel waits for a tombstone the model does not require"]
async fn an_entity_known_only_from_a_field_shows() {
    let mut hub = Vec::new();
    let (mut peers, _, _) = library_with_a_note(&mut hub).await;
    hub.push(hostile(
        "authors",
        Gid([3; 16]),
        "name",
        Value::Text("Gaskell".into()),
    ));
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        [
            "author Eliot",
            "author Gaskell",
            "book Middlemarch by Eliot",
            "note slow"
        ]
    );
}

/// A tombstone written without the entity's other fields still deletes it, and a restore with
/// nothing else brings back the fields it had.
#[tokio::test]
async fn a_tombstone_alone_deletes_and_restores() {
    let mut hub = Vec::new();
    let (mut peers, book, _) = library_with_a_note(&mut hub).await;
    let mut tombstone = hostile("books", book, DELETED, Value::Int(1));
    hub.push(tombstone.clone());
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        ["author Eliot"]
    );
    tombstone.value = Value::Int(0);
    tombstone.clock.counter = 1;
    hub.push(tombstone);
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        ["author Eliot", "book Middlemarch by Eliot", "note slow"]
    );
}

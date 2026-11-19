//! Ways replicas once parted or lost what was live, each in the smallest setting that showed it.
//! Every apply and collect is checked against the reference model, and every settled replica
//! must show its view.

mod support;

use fongo_sync_sqlite::core::{ALIAS, Change, DELETED, Gid, Hlc, Value};
use fongo_sync_sqlite::{Collection, Registry, Resume};
use support::peer::{Peer, later, make, same_everywhere, settle};

fn change(collection: &str, gid: Gid, field: &str, value: Value, wall_ms: u64) -> Change {
    Change {
        collection: collection.into(),
        gid,
        field: field.into(),
        value,
        clock: Hlc {
            wall_ms,
            counter: 0,
            node: Gid([9; 16]),
        },
    }
}

/// A clock far ahead of any real one, but within what the text form holds.
const SOON: u64 = 1_900_000_000_000;

// What the kernel leaves in a row is not an edit.

fn shelf_books(not_null: bool) -> Vec<String> {
    let isbn = if not_null {
        "isbn TEXT NOT NULL UNIQUE"
    } else {
        "isbn TEXT UNIQUE"
    };
    vec![
        format!("CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, {isbn}, current INTEGER)"),
        "CREATE UNIQUE INDEX one_current ON books (current) WHERE current = 1".into(),
    ]
}

fn current_books() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("isbn")
            .field("current")
            .identity(&["isbn"]),
    ])
    .unwrap()
}

#[tokio::test]
async fn a_held_row_edited_here_sends_only_the_edit() {
    for not_null in [false, true] {
        let schema = shelf_books(not_null);
        let schema: Vec<&str> = schema.iter().map(String::as_str).collect();
        let mut held = false;
        for _ in 0..40 {
            let mut hub = Vec::new();
            let mut peers = make(3, &schema, current_books).await;
            peers[0]
                .run(
                    "INSERT INTO books (id, title, isbn)
                     VALUES (1, 'one', 'x'), (2, 'two', 'y'), (3, 'three', 'z')",
                )
                .await;
            settle(&mut peers, &mut hub).await;
            later();
            // The first book takes a new isbn and becomes current, the second takes its old
            // isbn; meanwhile another replica makes the third book current.
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
            let one = format!(
                "[{}]",
                peers[1]
                    .int("SELECT id FROM books WHERE title = 'one'")
                    .await
            );
            if !applied
                .held
                .iter()
                .any(|h| h.key.as_deref() == Some(one.as_str()))
            {
                continue;
            }
            held = true;
            later();
            peers[1]
                .run("UPDATE books SET title = 'one!' WHERE title = 'one'")
                .await;
            let sent = peers[1].push(&mut hub).await;
            let fields: Vec<&str> = sent.iter().map(|c| c.field.as_str()).collect();
            assert_eq!(fields, ["title"], "{sent:#?}");
            break;
        }
        assert!(held, "the first book was never held");
    }
}

#[tokio::test]
async fn half_of_a_set_written_together_is_neither_written_nor_sent() {
    let schema = [
        "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, status TEXT, finished INTEGER,
                             isbn TEXT UNIQUE)",
    ];
    let registry = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id")
                .field("title")
                .field("status")
                .field("finished")
                .field("isbn")
                .together(&["status", "finished"])
                .identity(&["isbn"]),
        ])
        .unwrap()
    };
    let rows = "SELECT COALESCE(title, '-') || ' ' || COALESCE(status, '-') || ' '
                       || COALESCE(finished, '-') FROM books ORDER BY 1";
    let mut hub = Vec::new();
    let mut peers = make(2, &schema, registry).await;
    peers[0]
        .run("INSERT INTO books (title) VALUES ('one')")
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[0]
        .run("UPDATE books SET status = 'done', finished = 2")
        .await;
    let sent = peers[0].push(&mut hub).await;
    let half: Vec<Change> = sent.into_iter().filter(|c| c.field == "finished").collect();
    peers[1].apply(&half).await;
    assert_eq!(peers[1].lines(rows).await, ["one - -"]);
    later();
    peers[1].run("UPDATE books SET title = 'one!'").await;
    let sent = peers[1].push(&mut hub).await;
    assert_eq!(
        sent.iter().map(|c| c.field.as_str()).collect::<Vec<_>>(),
        ["title"]
    );
    settle(&mut peers, &mut hub).await;
    assert_eq!(same_everywhere(&mut peers, rows).await, ["one! done 2"]);

    // The same with the half arriving at a replica that merges two books.
    let mut hub = Vec::new();
    let mut peers = make(3, &schema, registry).await;
    peers[0]
        .run("INSERT INTO books (title, isbn) VALUES ('one', 'i')")
        .await;
    peers[0].push(&mut hub).await;
    later();
    peers[0]
        .run("UPDATE books SET status = 'done', finished = 2")
        .await;
    peers[0].push(&mut hub).await;
    peers[2]
        .run("INSERT INTO books (title, isbn) VALUES ('two', 'i')")
        .await;
    peers[2].push(&mut hub).await;
    let half: Vec<Change> = hub
        .iter()
        .filter(|c| c.field != "status")
        .cloned()
        .collect();
    peers[1].apply(&half).await;
    later();
    peers[1].run("UPDATE books SET title = 'three'").await;
    let sent = peers[1].push(&mut hub).await;
    assert!(
        sent.iter()
            .all(|c| c.field != "status" && c.field != "finished"),
        "{sent:#?}"
    );
    settle(&mut peers, &mut hub).await;
    assert_eq!(same_everywhere(&mut peers, rows).await, ["three done 2"]);
}

// One meaning of alive.

#[tokio::test]
async fn a_book_on_a_shelf_deleted_meanwhile_does_not_take_its_live_twin_with_it() {
    let schema = [
        "CREATE TABLE shelves (id INTEGER PRIMARY KEY, name TEXT)",
        "CREATE TABLE books (
            id INTEGER PRIMARY KEY,
            shelf_id INTEGER NOT NULL REFERENCES shelves(id) ON DELETE CASCADE,
            title TEXT,
            isbn TEXT UNIQUE)",
        "CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
            body TEXT)",
    ];
    let registry = || {
        Registry::new(vec![
            Collection::minted("shelves", "shelves", "id").field("name"),
            Collection::minted("books", "books", "id")
                .owner("shelf_id", "shelves")
                .field("title")
                .field("isbn")
                .identity(&["isbn"]),
            Collection::minted("notes", "notes", "id")
                .owner("book_id", "books")
                .field("body"),
        ])
        .unwrap()
    };
    let rows = "SELECT 'book ' || b.title || ' on ' || s.name FROM books b
                  JOIN shelves s ON s.id = b.shelf_id
                UNION ALL SELECT 'note ' || body FROM notes
                UNION ALL SELECT 'shelf ' || name FROM shelves ORDER BY 1";
    let mut hub = Vec::new();
    let mut peers = make(3, &schema, registry).await;
    peers[0]
        .run("INSERT INTO shelves (name) VALUES ('S'), ('T')")
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[2]
        .run(
            "INSERT INTO books (shelf_id, title, isbn)
             SELECT id, 'Y', 'i' FROM shelves WHERE name = 'T'",
        )
        .await;
    peers[2]
        .run("INSERT INTO notes (book_id, body) SELECT id, 'on Y' FROM books")
        .await;
    later();
    peers[2].run("DELETE FROM shelves WHERE name = 'S'").await;
    let made = peers[2].push(&mut hub).await;
    later();
    peers[0]
        .run(
            "INSERT INTO books (shelf_id, title, isbn)
             SELECT id, 'X', 'i' FROM shelves WHERE name = 'S'",
        )
        .await;
    let doomed = peers[0].push(&mut hub).await;
    // One replica meets the book on the doomed shelf first.
    peers[1].apply(&doomed).await;
    peers[1].apply(&made).await;
    peers[1].cursor = hub.len();
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, rows).await,
        ["book Y on T", "note on Y", "shelf T"]
    );
}

#[tokio::test]
async fn a_tag_completed_by_a_local_write_joins_its_twin_everywhere() {
    let schema = [
        "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, isbn TEXT UNIQUE)",
        "CREATE TABLE tags (
            book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
            tag TEXT NOT NULL,
            note TEXT,
            PRIMARY KEY (book_id, tag))",
    ];
    let registry = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id")
                .field("title")
                .field("isbn")
                .identity(&["isbn"]),
            Collection::natural("tags", "tags", &["book_id", "tag"])
                .owner("book_id", "books")
                .field("tag")
                .field("note"),
        ])
        .unwrap()
    };
    let rows = "SELECT tag || ' ' || COALESCE(note, '-') FROM tags ORDER BY 1";
    for _ in 0..10 {
        let mut hub = Vec::new();
        let mut peers = make(3, &schema, registry).await;
        peers[1]
            .run("INSERT INTO books (id, title, isbn) VALUES (1, 'Y', 'i')")
            .await;
        peers[1]
            .run("INSERT INTO tags (book_id, tag) VALUES (1, 't')")
            .await;
        peers[1].push(&mut hub).await;
        let theirs = hub.len();
        peers[0]
            .run("INSERT INTO books (id, title, isbn) VALUES (1, 'X', 'i')")
            .await;
        peers[0]
            .run("INSERT INTO tags (book_id, tag) VALUES (1, 't')")
            .await;
        peers[0].push(&mut hub).await;
        // The second replica takes the other book, then that book's tag without its tag field.
        let books: Vec<Change> = hub[theirs..]
            .iter()
            .filter(|c| c.collection == "books")
            .cloned()
            .collect();
        let partial: Vec<Change> = hub[theirs..]
            .iter()
            .filter(|c| c.collection == "tags" && c.field != "tag")
            .cloned()
            .collect();
        peers[1].apply(&books).await;
        peers[1].apply(&partial).await;
        later();
        peers[1].run("UPDATE tags SET note = 'mine'").await;
        peers[1].push(&mut hub).await;
        peers[1].cursor = theirs;
        settle(&mut peers, &mut hub).await;
        assert_eq!(same_everywhere(&mut peers, rows).await, ["t mine"]);
    }
}

// What another device sends is data, and is weighed the same everywhere.

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

const LIBRARY: &[&str] = &[
    "CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)",
    "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER,
                         isbn TEXT UNIQUE)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        body TEXT)",
];

const LIBRARY_ROWS: &str = "
    SELECT 'author ' || a.name FROM authors a
    UNION ALL SELECT 'book ' || b.title || ' by ' || COALESCE(a.name, '-')
      FROM books b LEFT JOIN authors a ON a.id = b.author_id
    UNION ALL SELECT 'note ' || n.body || ' on ' || b.title
      FROM notes n JOIN books b ON b.id = n.book_id
    ORDER BY 1";

async fn library_peers(hub: &mut Vec<Change>) -> (Vec<Peer>, [Gid; 4]) {
    let mut peers = make(2, LIBRARY, library).await;
    peers[0]
        .run("INSERT INTO authors (id, name) VALUES (1, 'Eliot'), (2, 'Gaskell')")
        .await;
    peers[0]
        .run(
            "INSERT INTO books (id, title, author_id, isbn)
             VALUES (1, 'Middlemarch', 1, 'x'), (2, 'Cranford', 2, 'y')",
        )
        .await;
    peers[0]
        .run("INSERT INTO notes (id, book_id, body) VALUES (1, 1, 'slow')")
        .await;
    settle(&mut peers, hub).await;
    let ids = [
        peers[0].gid("authors", 1).await,
        peers[0].gid("books", 1).await,
        peers[0].gid("books", 2).await,
        peers[0].gid("notes", 1).await,
    ];
    (peers, ids)
}

#[tokio::test]
async fn changes_that_cross_collections_change_nothing_here() {
    let mut hub = Vec::new();
    let (mut peers, [author, book, other, _]) = library_peers(&mut hub).await;
    let before = peers[1].lines(LIBRARY_ROWS).await;
    let hostile = [
        // An alias under a collection the registry does not have, between two books.
        change("nonsense", book, ALIAS, Value::Ref(other), SOON),
        // An alias that names an author as the same as a book.
        change("books", book, ALIAS, Value::Ref(author), SOON),
        // Tombstones that are neither 0 nor 1.
        change("books", book, DELETED, Value::Int(5), SOON),
        change("books", other, DELETED, Value::Text("yes".into()), SOON),
    ];
    let mut alone = hub.clone();
    for change in &hostile {
        peers[1].apply(std::slice::from_ref(change)).await;
        alone.push(change.clone());
    }
    assert_eq!(
        peers[1].lines(LIBRARY_ROWS).await,
        [
            "author Eliot",
            "author Gaskell",
            "book Cranford by Gaskell",
            "book Middlemarch by Eliot",
            "note slow on Middlemarch",
        ]
    );
    // The same changes in the other order leave the other replica where this one is.
    let mut reversed: Vec<Change> = hostile.to_vec();
    reversed.reverse();
    peers[0].apply(&reversed).await;
    later();
    peers[1].run("UPDATE books SET title = title || '!'").await;
    hub.extend(hostile);
    settle(&mut peers, &mut hub).await;
    let rows = same_everywhere(&mut peers, LIBRARY_ROWS).await;
    assert_eq!(rows.len(), before.len());
}

#[tokio::test]
async fn a_clock_beyond_what_can_be_stored_is_ignored_everywhere() {
    let mut hub = Vec::new();
    let (mut peers, [_, book, _, _]) = library_peers(&mut hub).await;
    hub.push(Change {
        collection: "books".into(),
        gid: book,
        field: "title".into(),
        value: Value::Text("far".into()),
        clock: Hlc {
            wall_ms: u64::MAX,
            counter: u32::MAX,
            node: Gid([7; 16]),
        },
    });
    settle(&mut peers, &mut hub).await;
    peers[1]
        .run("UPDATE books SET title = 'mine' WHERE title = 'Middlemarch'")
        .await;
    settle(&mut peers, &mut hub).await;
    let rows = same_everywhere(&mut peers, LIBRARY_ROWS).await;
    assert!(
        rows.contains(&"book mine by Eliot".to_string()),
        "{rows:#?}"
    );
}

/// A change stamped with the last reading the text form holds is kept, but its clock is not
/// adopted: a replica that adopted it could issue no later reading the text form holds, and
/// would send readings others read back as other ones.
#[tokio::test]
async fn a_clock_at_the_last_reading_is_kept_and_leaves_room_to_tick() {
    let mut hub = Vec::new();
    let (mut peers, [_, book, _, _]) = library_peers(&mut hub).await;
    hub.push(Change {
        collection: "books".into(),
        gid: book,
        field: "title".into(),
        value: Value::Text("last".into()),
        clock: Hlc {
            wall_ms: 0xFFFF_FFFF_FFFF,
            counter: u32::MAX,
            node: Gid([7; 16]),
        },
    });
    settle(&mut peers, &mut hub).await;
    later();
    peers[1]
        .run("UPDATE books SET title = 'mine' WHERE isbn = 'y'")
        .await;
    let first = peers[1].push(&mut hub).await;
    later();
    peers[1]
        .run("UPDATE books SET title = 'mine again' WHERE isbn = 'y'")
        .await;
    let second = peers[1].push(&mut hub).await;
    let clocks: Vec<Hlc> = first.iter().chain(&second).map(|c| c.clock).collect();
    assert!(
        clocks
            .iter()
            .all(|c| c.encodable() && c.wall_ms < 0xFFFF_FFFF_FFFF)
    );
    assert!(clocks.windows(2).all(|w| w[0] < w[1]), "{clocks:?}");
    settle(&mut peers, &mut hub).await;
    let rows = same_everywhere(&mut peers, LIBRARY_ROWS).await;
    assert!(
        rows.contains(&"book last by Eliot".to_string()),
        "{rows:#?}"
    );
    assert!(
        rows.contains(&"book mine again by Gaskell".to_string()),
        "{rows:#?}"
    );
}

// What happens while the triggers are off.

#[tokio::test]
async fn rows_removed_and_added_while_suspended_are_deleted_and_made() {
    let schema = ["CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, isbn TEXT UNIQUE)"];
    let registry = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id")
                .field("title")
                .field("isbn")
                .identity(&["isbn"]),
        ])
        .unwrap()
    };
    let rows = "SELECT title || ' ' || COALESCE(isbn, '-') FROM books ORDER BY 1";
    for how in [Resume::Send, Resume::Baseline] {
        let mut hub = Vec::new();
        let mut peers = make(2, &schema, registry).await;
        peers[0]
            .run("INSERT INTO books (id, title, isbn) VALUES (1, 'one', 'x'), (2, 'two', 'y')")
            .await;
        settle(&mut peers, &mut hub).await;
        let two = peers[0].gid("books", 2).await;
        peers[0].suspend().await;
        peers[0].run("DELETE FROM books WHERE id = 2").await;
        peers[0]
            .run("INSERT INTO books (id, title, isbn) VALUES (3, 'three', 't')")
            .await;
        peers[0].resume(how).await;
        // A row made after resuming takes the freed local id, and is a new entity.
        peers[0]
            .run("INSERT INTO books (id, title, isbn) VALUES (2, 'new', 'n')")
            .await;
        settle(&mut peers, &mut hub).await;
        assert_eq!(
            same_everywhere(&mut peers, rows).await,
            ["new n", "one x", "three t"]
        );
        assert_ne!(peers[0].gid("books", 2).await, two);
    }
}

// Work that grows with what an apply touches, not with its square.

/// The work SQLite does on the replica that takes a book's delete, the book owning `notes`.
async fn work_to_delete_an_owner_of(notes: i64) -> u64 {
    let schema = [
        "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT)",
        "CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
            body TEXT)",
    ];
    let registry = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id").field("title"),
            Collection::minted("notes", "notes", "id")
                .owner("book_id", "books")
                .field("body"),
        ])
        .unwrap()
    };
    let mut hub = Vec::new();
    let mut peers = make(2, &schema, registry).await;
    for peer in peers.iter_mut() {
        peer.checking = false;
    }
    peers[0]
        .run("INSERT INTO books (id, title) VALUES (1, 'Middlemarch')")
        .await;
    peers[0]
        .run(&format!(
            "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < {notes})
             INSERT INTO notes (book_id, body) SELECT 1, 'note ' || x FROM n"
        ))
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(peers[1].int("SELECT count(*) FROM notes").await, notes);
    peers[0].run("DELETE FROM books").await;
    peers[0].push(&mut hub).await;
    let work = peers[1].count_work().await;
    peers[1].pull(&hub).await;
    let done = work.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(peers[1].int("SELECT count(*) FROM notes").await, 0);
    for peer in peers.iter_mut() {
        peer.settled().await;
    }
    done
}

/// Removing the rows one by one against all the others once took minutes: work that grows with
/// the square of the rows grows sixteenfold when they grow fourfold.
#[tokio::test]
async fn deleting_an_owner_of_many_rows_takes_work_in_proportion() {
    let small = work_to_delete_an_owner_of(1_000).await;
    let large = work_to_delete_an_owner_of(4_000).await;
    assert!(large < small * 8, "{small} then {large}");
}

// A merge whose given-up row another table holds.

#[tokio::test]
async fn a_given_up_row_held_by_another_table_waits_and_is_never_sent() {
    let schema = [
        "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, isbn TEXT UNIQUE)",
        "CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
            body TEXT)",
        "CREATE TABLE pins (book_id INTEGER NOT NULL UNIQUE REFERENCES books(id))",
    ];
    let registry = || {
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
    };
    let rows = "SELECT 'book ' || isbn FROM books
                UNION ALL SELECT 'note ' || n.body || ' on ' || b.isbn
                  FROM notes n JOIN books b ON b.id = n.book_id ORDER BY 1";
    let mut hub = Vec::new();
    let mut peers = make(3, &schema, registry).await;
    peers[0]
        .run("INSERT INTO books (title, isbn) VALUES ('one', 'x'), ('two', 'y')")
        .await;
    settle(&mut peers, &mut hub).await;
    // The second replica pins both books, so the one a merge gives up cannot be deleted there.
    peers[1].run("INSERT INTO pins SELECT id FROM books").await;
    later();
    peers[0]
        .run("UPDATE books SET isbn = 'w' WHERE title = 'one'")
        .await;
    peers[0].push(&mut hub).await;
    peers[2]
        .run("UPDATE books SET isbn = 'w' WHERE title = 'two'")
        .await;
    peers[2].push(&mut hub).await;
    let applied = peers[1].pull(&hub).await;
    let left = peers[1]
        .int("SELECT json_extract(key, '$[0]') FROM fongo_leftover")
        .await;
    assert!(
        applied
            .held
            .iter()
            .any(|h| h.key.as_deref() == Some(format!("[{left}]").as_str())),
        "{:#?}",
        applied.held
    );

    // Edited, or given a note, the row is still not an entity of its own.
    peers[1]
        .run(&format!(
            "UPDATE books SET title = 'kept?' WHERE id = {left}"
        ))
        .await;
    peers[1]
        .run(&format!(
            "INSERT INTO notes (book_id, body) VALUES ({left}, 'late')"
        ))
        .await;
    let sent = peers[1].push(&mut hub).await;
    assert!(sent.iter().all(|c| c.field == ALIAS), "{sent:#?}");

    // Let go, the merge completes and the note follows the row kept.
    peers[1]
        .run(&format!("DELETE FROM pins WHERE book_id = {left}"))
        .await;
    let applied = peers[1].apply(&[]).await;
    assert!(applied.held.is_empty(), "{:#?}", applied.held);
    assert_eq!(peers[1].int("SELECT count(*) FROM books").await, 1);
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, rows).await,
        ["book w", "note late on w"]
    );
}

// Registries that change over time.

const RATED: &[&str] =
    &["CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, status TEXT, finished INTEGER)"];

fn before() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("status"),
    ])
    .unwrap()
}

fn after() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("status")
            .field("finished"),
    ])
    .unwrap()
}

fn grouped() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("status")
            .field("finished")
            .together(&["status", "finished"]),
    ])
    .unwrap()
}

const RATED_ROWS: &str = "SELECT title || ' ' || COALESCE(status, '-') || ' '
                                 || COALESCE(finished, '-') FROM books ORDER BY 1";

async fn upgrade(peer: &mut Peer, registry: Registry) {
    peer.upgrade(registry).await;
}

#[tokio::test]
async fn a_field_reaches_rows_whenever_each_replica_learns_of_it() {
    let mut hub = Vec::new();
    let mut peers = make(3, RATED, before).await;
    peers[0]
        .run("INSERT INTO books (title, finished) VALUES ('one', 7)")
        .await;
    settle(&mut peers, &mut hub).await;
    // One replica upgrades and writes the field; another hears of it before it upgrades.
    upgrade(&mut peers[0], after()).await;
    later();
    peers[0]
        .run("INSERT INTO books (title, finished) VALUES ('two', 3)")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(peers[1].lines(RATED_ROWS).await, ["one - -", "two - -"]);
    upgrade(&mut peers[1], after()).await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[2], after()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, RATED_ROWS).await,
        ["one - 7", "two - 3"]
    );
}

#[tokio::test]
async fn a_database_installed_before_fields_were_recorded_adopts_a_new_field() {
    let mut hub = Vec::new();
    let mut peers = make(2, RATED, after).await;
    upgrade(&mut peers[0], before()).await;
    peers[0]
        .run("INSERT INTO books (title, finished) VALUES ('one', 5)")
        .await;
    settle(&mut peers, &mut hub).await;
    peers[0]
        .run("DELETE FROM fongo_meta WHERE key LIKE 'fields:%' OR key LIKE 'together:%'")
        .await;
    upgrade(&mut peers[0], after()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(same_everywhere(&mut peers, RATED_ROWS).await, ["one - 5"]);
}

#[tokio::test]
async fn a_field_that_joins_a_set_already_written_is_recorded_with_it() {
    let mut hub = Vec::new();
    let mut peers = make(2, RATED, before).await;
    peers[0]
        .run("INSERT INTO books (title, status, finished) VALUES ('one', 'done', 7)")
        .await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[0], grouped()).await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[1], grouped()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, RATED_ROWS).await,
        ["one done 7"]
    );
}

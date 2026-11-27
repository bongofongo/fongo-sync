//! Ways replicas once parted or lost what was live, each in the smallest setting that showed it.
//! Every apply and collect is checked against the reference model, and every settled replica
//! must show its view.

mod support;

use std::collections::BTreeSet;

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
async fn half_of_a_set_written_together_is_not_sent() {
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
    // The model shows a set from its latest write, NULL where that write has not arrived.
    assert_eq!(peers[1].lines(rows).await, ["one - 2"]);
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
async fn a_given_up_row_held_by_another_table_waits_and_its_edits_are_sent() {
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
    let replica = peers[1].replica.clone();
    let inspection = replica.inspect(&mut peers[1].conn).await.unwrap();
    let (_, left) = inspection.leftover.first().expect("a row is left over");
    let left: i64 = left.trim_matches(|c| c == '[' || c == ']').parse().unwrap();
    assert!(
        applied
            .held
            .iter()
            .any(|h| h.key.as_deref() == Some(format!("[{left}]").as_str())),
        "{:#?}",
        applied.held
    );

    // Edited, or given a note, the row still stands for the entity it stood for, a member of the
    // group it joined: the edit is that entity's and the note names it, and both are sent.
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
    assert!(
        sent.iter()
            .any(|c| c.field == "title" && c.value == Value::Text("kept?".into())),
        "{sent:#?}"
    );
    assert!(
        sent.iter()
            .any(|c| c.collection == "notes" && c.value == Value::Text("late".into())),
        "{sent:#?}"
    );

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
    assert_eq!(
        same_everywhere(&mut peers, "SELECT title FROM books").await,
        ["kept?"]
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

// A set redrawn by an upgrade is completed at a clock just past its latest write, with the
// values the replica's registers hold, so an edit made elsewhere and not yet pulled still wins.

const WIDE: &[&str] = &[
    "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, status TEXT,
                                             finished INTEGER, rating INTEGER)",
];

fn paired() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("status")
            .field("finished")
            .together(&["status", "finished"]),
    ])
    .unwrap()
}

fn widened() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("status")
            .field("finished")
            .field("rating")
            .together(&["status", "finished", "rating"]),
    ])
    .unwrap()
}

fn reordered() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("status")
            .field("finished")
            .together(&["finished", "status"]),
    ])
    .unwrap()
}

const WIDE_ROWS: &str = "SELECT title || ' ' || COALESCE(status, '-') || ' '
                                || COALESCE(finished, '-') || ' ' || COALESCE(rating, '-')
                           FROM books ORDER BY 1";

#[tokio::test]
async fn widening_a_set_keeps_an_edit_the_installer_has_not_pulled() {
    let mut hub = Vec::new();
    let mut peers = make(2, WIDE, paired).await;
    peers[0]
        .run("INSERT INTO books (title, status, finished, rating) VALUES ('one', 'reading', 1, 4)")
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[1]
        .run("UPDATE books SET status = 'done', finished = 2")
        .await;
    peers[1].push(&mut hub).await;
    later();
    upgrade(&mut peers[0], widened()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(peers[1].lines(WIDE_ROWS).await, ["one done 2 -"]);
    upgrade(&mut peers[1], widened()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, WIDE_ROWS).await,
        ["one done 2 4"]
    );
}

#[tokio::test]
async fn replicas_that_widen_a_set_apart_agree_and_keep_an_older_replicas_edit() {
    let mut hub = Vec::new();
    let mut peers = make(3, WIDE, paired).await;
    peers[0]
        .run("INSERT INTO books (title, status, finished) VALUES ('one', 'reading', 1)")
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[2]
        .run("UPDATE books SET status = 'done', finished = 2")
        .await;
    peers[2].push(&mut hub).await;
    later();
    peers[0].run("UPDATE books SET rating = 4").await;
    upgrade(&mut peers[0], widened()).await;
    peers[0].push(&mut hub).await;
    later();
    peers[1].run("UPDATE books SET rating = 5").await;
    upgrade(&mut peers[1], widened()).await;
    peers[1].push(&mut hub).await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[2], widened()).await;
    settle(&mut peers, &mut hub).await;
    let rows = same_everywhere(&mut peers, WIDE_ROWS).await;
    assert!(
        rows == ["one done 2 4"] || rows == ["one done 2 5"],
        "{rows:?}"
    );
}

#[tokio::test]
async fn grouping_fields_written_apart_keeps_an_edit_the_installer_has_not_pulled() {
    let mut hub = Vec::new();
    let mut peers = make(2, RATED, after).await;
    peers[0]
        .run("INSERT INTO books (title, status, finished) VALUES ('one', 'reading', 1)")
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[0].run("UPDATE books SET status = 'started'").await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[1].run("UPDATE books SET finished = 2").await;
    peers[1].push(&mut hub).await;
    later();
    upgrade(&mut peers[0], grouped()).await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[1], grouped()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, RATED_ROWS).await,
        ["one started 2"]
    );
}

#[tokio::test]
async fn redrawing_sets_leaves_a_partly_arrived_write_alone() {
    let mut hub = Vec::new();
    let mut peers = make(2, RATED, grouped).await;
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
    upgrade(&mut peers[1], reordered()).await;
    let sent = peers[1].push(&mut hub).await;
    assert!(
        sent.iter()
            .all(|c| c.field != "status" && c.field != "finished"),
        "{sent:#?}"
    );
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, RATED_ROWS).await,
        ["one done 2"]
    );
}

/// A set written in another order is the same set: an upgrade that only reorders it sends
/// nothing, even where a write that has partly arrived leaves the set's fields at two clocks over
/// an older write that holds a value for the part still on its way.
#[tokio::test]
async fn a_reorder_leaves_a_partly_arrived_write_over_an_older_one_alone() {
    let mut hub = Vec::new();
    let mut peers = make(2, WIDE, paired).await;
    peers[0]
        .run("INSERT INTO books (title, status, finished) VALUES ('one', 'reading', 1)")
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[0]
        .run("UPDATE books SET status = 'done', finished = 2")
        .await;
    let sent = peers[0].push(&mut hub).await;
    let half: Vec<Change> = sent.into_iter().filter(|c| c.field == "finished").collect();
    peers[1].apply(&half).await;
    upgrade(&mut peers[1], reordered()).await;
    let sent = peers[1].push(&mut hub).await;
    assert_eq!(sent, []);
    upgrade(&mut peers[0], reordered()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, WIDE_ROWS).await,
        ["one done 2 -"]
    );
}

#[tokio::test]
async fn a_redraw_sends_what_the_merge_holds_not_what_a_held_row_holds() {
    let mut hub = Vec::new();
    let mut peers = make(2, WIDE, paired).await;
    peers[0]
        .run("INSERT INTO books (title, status, finished) VALUES ('one', 'reading', 1)")
        .await;
    settle(&mut peers, &mut hub).await;
    peers[1]
        .run(
            "CREATE TRIGGER refuse AFTER UPDATE OF status ON books WHEN NEW.status = 'lost'
             BEGIN SELECT json('{'); END",
        )
        .await;
    later();
    peers[0]
        .run("UPDATE books SET status = 'lost', finished = 9")
        .await;
    peers[0].push(&mut hub).await;
    let applied = peers[1].pull(&hub).await;
    assert_eq!(applied.waiting, 1, "{:#?}", applied.held);
    peers[1].run("UPDATE books SET rating = 3").await;
    later();
    upgrade(&mut peers[1], widened()).await;
    let sent = peers[1].push(&mut hub).await;
    assert!(
        sent.iter()
            .all(|c| c.value != Value::Text("reading".into())),
        "{sent:#?}"
    );
    peers[0].pull(&hub).await;
    assert_eq!(peers[0].lines(WIDE_ROWS).await, ["one lost 9 -"]);
    upgrade(&mut peers[0], widened()).await;
    peers[1].run("DROP TRIGGER refuse").await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, WIDE_ROWS).await,
        ["one lost 9 3"]
    );
}

#[tokio::test]
async fn an_edit_not_yet_collected_when_a_set_is_widened_is_sent_after_as_an_edit() {
    let mut hub = Vec::new();
    let mut peers = make(2, WIDE, paired).await;
    peers[0]
        .run("INSERT INTO books (title, status, finished) VALUES ('one', 'reading', 1)")
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[1]
        .run("UPDATE books SET status = 'done', finished = 2")
        .await;
    peers[1].push(&mut hub).await;
    later();
    peers[0]
        .run("UPDATE books SET status = 'dropped', finished = NULL, rating = 4")
        .await;
    upgrade(&mut peers[0], widened()).await;
    let sent = peers[0].push(&mut hub).await;
    let status: Vec<(&Value, Hlc)> = sent
        .iter()
        .filter(|c| c.field == "status")
        .map(|c| (&c.value, c.clock))
        .collect();
    assert_eq!(status.len(), 2, "{sent:#?}");
    assert_eq!(*status[0].0, Value::Text("reading".into()));
    assert_eq!(*status[1].0, Value::Text("dropped".into()));
    assert!(status[0].1 < status[1].1);
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[1], widened()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, WIDE_ROWS).await,
        ["one dropped - 4"]
    );
}

#[tokio::test]
async fn a_completion_may_share_a_clock_with_another_row_written_in_its_millisecond() {
    let mut hub = Vec::new();
    let mut peers = make(2, WIDE, paired).await;
    peers[0]
        .run("INSERT INTO books (title) VALUES ('one'), ('two')")
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[0]
        .run("UPDATE books SET status = 'reading', finished = 1, rating = 4")
        .await;
    let written = peers[0].push(&mut hub).await;
    later();
    peers[1]
        .run("UPDATE books SET status = 'done', finished = 2 WHERE title = 'two'")
        .await;
    peers[1].push(&mut hub).await;
    upgrade(&mut peers[0], widened()).await;
    let completed = peers[0].push(&mut hub).await;
    // Both rows were written in one millisecond, at successive clocks of one node: the first
    // row's set is completed at the second's clock.
    let clocks: BTreeSet<Hlc> = written.iter().map(|c| c.clock).collect();
    assert_eq!(clocks.len(), 2, "{written:#?}");
    assert!(
        completed.iter().any(|c| clocks.contains(&c.clock)),
        "{completed:#?}"
    );
    later();
    peers[0]
        .run("UPDATE books SET title = 'one!' WHERE title = 'one'")
        .await;
    let edited = peers[0].push(&mut hub).await;
    assert!(
        edited
            .iter()
            .all(|e| completed.iter().all(|c| c.clock < e.clock))
    );
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[1], widened()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, WIDE_ROWS).await,
        ["one! reading 1 4", "two done 2 4"]
    );
}

#[tokio::test]
async fn widening_after_a_downgrade_keeps_an_edit_the_installer_has_not_pulled() {
    let mut hub = Vec::new();
    let mut peers = make(2, WIDE, paired).await;
    peers[0]
        .run("INSERT INTO books (title, status, finished, rating) VALUES ('one', 'reading', 1, 4)")
        .await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[0], widened()).await;
    upgrade(&mut peers[1], widened()).await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[0], paired()).await;
    later();
    peers[0]
        .run("UPDATE books SET status = 'done', finished = 2")
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[1]
        .run("UPDATE books SET status = 'paused', finished = NULL, rating = 5")
        .await;
    peers[1].push(&mut hub).await;
    upgrade(&mut peers[0], widened()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, WIDE_ROWS).await,
        ["one paused - 5"]
    );
}

const WIDER: &[&str] = &[
    "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, status TEXT,
                                              finished INTEGER, rating INTEGER, note TEXT)",
];

fn wider() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("status")
            .field("finished")
            .field("rating")
            .field("note")
            .together(&["status", "finished", "rating", "note"]),
    ])
    .unwrap()
}

fn noted() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("status")
            .field("finished")
            .field("rating")
            .field("note")
            .together(&["status", "finished"])
            .together(&["rating", "note"]),
    ])
    .unwrap()
}

const WIDER_ROWS: &str = "SELECT title || ' ' || COALESCE(status, '-') || ' '
                                 || COALESCE(finished, '-') || ' ' || COALESCE(rating, '-')
                                 || ' ' || COALESCE(note, '-')
                            FROM books ORDER BY 1";

#[tokio::test]
async fn a_second_redraw_keeps_an_edit_the_installer_has_not_pulled() {
    let mut hub = Vec::new();
    let mut peers = make(2, WIDER, paired).await;
    peers[0]
        .run(
            "INSERT INTO books (title, status, finished, rating, note)
             VALUES ('one', 'reading', 1, 4, 'n')",
        )
        .await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[0], widened()).await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[1]
        .run("UPDATE books SET status = 'done', finished = 2")
        .await;
    peers[1].push(&mut hub).await;
    later();
    upgrade(&mut peers[0], wider()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(peers[0].lines(WIDER_ROWS).await, ["one done 2 - -"]);
    upgrade(&mut peers[1], wider()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, WIDER_ROWS).await,
        ["one done 2 4 n"]
    );
}

#[tokio::test]
async fn a_set_made_only_of_new_fields_is_adopted_from_the_rows() {
    let mut hub = Vec::new();
    let mut peers = make(2, WIDER, paired).await;
    peers[0]
        .run("INSERT INTO books (title, status) VALUES ('one', 'reading'), ('two', NULL)")
        .await;
    settle(&mut peers, &mut hub).await;
    peers[0]
        .run("UPDATE books SET rating = 4, note = 'n' WHERE title = 'one'")
        .await;
    upgrade(&mut peers[0], noted()).await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[1], noted()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, WIDER_ROWS).await,
        ["one reading - 4 n", "two - - - -"]
    );
}

#[tokio::test]
async fn a_stepped_install_widens_a_set_without_beating_an_edit_it_has_not_pulled() {
    let mut hub = Vec::new();
    let mut peers = make(2, WIDE, paired).await;
    peers[0]
        .run(
            "INSERT INTO books (title, status, finished, rating)
             VALUES ('one', 'reading', 1, 4), ('two', 'reading', 1, 5), ('three', NULL, NULL, 6)",
        )
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[1]
        .run("UPDATE books SET status = 'done', finished = 2 WHERE title = 'one'")
        .await;
    peers[1].push(&mut hub).await;
    later();
    peers[0]
        .run("UPDATE books SET title = title || '!' WHERE title <> 'one'")
        .await;
    later();
    peers[0]
        .run("UPDATE books SET status = 'done', finished = 3 WHERE title = 'two!'")
        .await;
    peers[0].upgrade_in_steps(widened(), 1).await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[1], widened()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, WIDE_ROWS).await,
        ["one done 2 4", "three! - - 6", "two! done 3 5"]
    );
}

#[tokio::test]
async fn a_set_widened_on_resuming_keeps_an_edit_the_installer_has_not_pulled() {
    for how in [Resume::Send, Resume::Baseline] {
        let mut hub = Vec::new();
        let mut peers = make(2, WIDE, paired).await;
        peers[0]
            .run(
                "INSERT INTO books (title, status, finished)
                 VALUES ('one', 'reading', 1), ('two', 'reading', 1)",
            )
            .await;
        settle(&mut peers, &mut hub).await;
        later();
        peers[1]
            .run("UPDATE books SET status = 'done', finished = 2 WHERE title = 'one'")
            .await;
        peers[1].push(&mut hub).await;
        later();
        peers[0].suspend().await;
        peers[0]
            .run("UPDATE books SET status = 'dropped', finished = NULL WHERE title = 'two'")
            .await;
        peers[0].run("UPDATE books SET rating = 4").await;
        peers[0].replica = fongo_sync_sqlite::Replica::new(widened());
        peers[0].resume(how).await;
        settle(&mut peers, &mut hub).await;
        upgrade(&mut peers[1], widened()).await;
        settle(&mut peers, &mut hub).await;
        if how == Resume::Send {
            assert_eq!(
                same_everywhere(&mut peers, WIDE_ROWS).await,
                ["one done 2 4", "two dropped - 4"]
            );
            continue;
        }
        // The baseline stays in the row it was written to, and only there.
        for (peer, two) in peers.iter_mut().zip(["two dropped - 4", "two reading 1 4"]) {
            peer.settled().await;
            assert_eq!(peer.lines(WIDE_ROWS).await, ["one done 2 4", two]);
        }
    }
}

const SHARED: &[&str] = &["CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT,
                                               isbn TEXT UNIQUE, status TEXT, finished INTEGER,
                                               rating INTEGER)"];

fn shared(set: &[&str]) -> Registry {
    let mut books = Collection::minted("books", "books", "id")
        .field("title")
        .field("isbn")
        .field("status")
        .field("finished");
    if set.contains(&"rating") {
        books = books.field("rating");
    }
    Registry::new(vec![books.together(set).identity(&["isbn"])]).unwrap()
}

#[tokio::test]
async fn a_merged_group_widens_its_set_without_beating_an_edit_the_installer_has_not_pulled() {
    let (old, new) = (
        || shared(&["status", "finished"]),
        || shared(&["status", "finished", "rating"]),
    );
    let mut hub = Vec::new();
    let mut peers = make(2, SHARED, old).await;
    peers[0]
        .run("INSERT INTO books (title, isbn, status, finished) VALUES ('one', 'i', 'reading', 1)")
        .await;
    peers[0].push(&mut hub).await;
    later();
    peers[1]
        .run("INSERT INTO books (title, isbn, status, finished) VALUES ('uno', 'i', 'started', 3)")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(peers[0].int("SELECT count(*) FROM books").await, 1);
    later();
    peers[1]
        .run("UPDATE books SET status = 'done', finished = 2")
        .await;
    peers[1].push(&mut hub).await;
    later();
    peers[0].run("UPDATE books SET rating = 4").await;
    upgrade(&mut peers[0], new()).await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[1], new()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, WIDE_ROWS).await,
        ["uno done 2 4"]
    );
}

/// Accepted, and stated with `Collection::together`: where a replica still on the old registry
/// wrote the set after the redraw's clock, the field the set gained reads NULL until that replica
/// upgrades; a write to the set meanwhile records that NULL, and the value adopted is gone.
#[tokio::test]
async fn a_value_a_set_gains_is_lost_to_an_edit_made_before_every_replica_upgrades() {
    let mut hub = Vec::new();
    let mut peers = make(2, WIDE, paired).await;
    peers[0]
        .run("INSERT INTO books (title, status, finished, rating) VALUES ('one', 'reading', 1, 4)")
        .await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[0], widened()).await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[1]
        .run("UPDATE books SET status = 'done', finished = 2")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(peers[0].lines(WIDE_ROWS).await, ["one done 2 -"]);
    later();
    peers[0].run("UPDATE books SET finished = 3").await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[1], widened()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, WIDE_ROWS).await,
        ["one done 3 -"]
    );
}

/// Applies all of `hub` to every peer, its own changes among them, and requires that none
/// reports a node another database writes as.
async fn none_shared(peers: &mut [Peer], hub: &[Change]) {
    for (i, peer) in peers.iter_mut().enumerate() {
        let applied = peer.apply(hub).await;
        assert!(applied.shared.is_empty(), "peer {i}: {:?}", applied.shared);
    }
}

async fn node_of(peer: &mut Peer) -> Gid {
    let inspection = peer.replica.inspect(&mut peer.conn).await.unwrap();
    inspection.node.unwrap()
}

/// A completion's clock is one this node may never have ticked to, or may have given another
/// row in the same millisecond, or is the first reading of wall 1: it is still one this
/// database issued, so neither it nor another database takes it for a copy's.
#[tokio::test]
async fn a_completion_applied_again_anywhere_reports_no_shared_node() {
    let mut hub = Vec::new();
    let mut found = Peer::bare(WIDE, paired()).await;
    found
        .run(
            "INSERT INTO books (title, status, finished, rating) VALUES ('found', 'reading', 1, 5)",
        )
        .await;
    found.install().await;
    let mut peers = vec![found, Peer::new(WIDE, paired()).await];
    peers[0]
        .run("INSERT INTO books (title, status) VALUES ('one', 'reading'), ('two', 'reading')")
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[1]
        .run("UPDATE books SET status = 'done', finished = 2 WHERE title <> 'found'")
        .await;
    settle(&mut peers, &mut hub).await;
    // The latest write peer 0 knows of is peer 1's, so its completions lie past its own clock.
    let theirs = hub.last().unwrap().clock;
    peers[0]
        .run("UPDATE books SET rating = 4 WHERE title <> 'found'")
        .await;
    upgrade(&mut peers[0], widened()).await;
    let sent = peers[0].push(&mut hub).await;
    let mine = node_of(&mut peers[0]).await;
    let clocks: BTreeSet<Hlc> = sent.iter().map(|c| c.clock).collect();
    assert!(clocks.iter().all(|c| c.node == mine), "{sent:#?}");
    assert!(clocks.iter().any(|c| c.wall_ms == 1), "{sent:#?}");
    assert!(
        clocks
            .iter()
            .any(|c| c.wall_ms == theirs.wall_ms && c.counter > theirs.counter),
        "{sent:#?}"
    );
    none_shared(&mut peers, &hub).await;
    upgrade(&mut peers[1], widened()).await;
    settle(&mut peers, &mut hub).await;
    none_shared(&mut peers, &hub).await;
    assert_eq!(
        same_everywhere(&mut peers, WIDE_ROWS).await,
        ["found reading 1 5", "one done 2 4", "two done 2 4"]
    );
}

const LAST_MS: u64 = 0xFFFF_FFFF_FFFF;

/// One book per clock, each made with its set written by another node at that clock, applied to
/// a fresh replica one at a time in order; the replica then widens the set. Returns the clock it
/// completed each book's set at, `None` where it sent nothing, and its rows.
async fn completed_at(at: &[(u64, u32)]) -> (Vec<Option<Hlc>>, Vec<String>) {
    let other = Gid([9; 16]);
    let mut peer = Peer::new(WIDE, paired()).await;
    let mut hub = Vec::new();
    let mut books = Vec::new();
    for (i, &(wall_ms, counter)) in at.iter().enumerate() {
        let gid = Gid([i as u8 + 1; 16]);
        let clock = Hlc {
            wall_ms,
            counter,
            node: other,
        };
        let made: Vec<Change> = [
            (DELETED, Value::Int(0)),
            ("title", Value::Text(format!("b{i}"))),
            ("status", Value::Text("done".into())),
            ("finished", Value::Int(i as i64)),
        ]
        .into_iter()
        .map(|(field, value)| Change {
            collection: "books".into(),
            gid,
            field: field.into(),
            value,
            clock,
        })
        .collect();
        hub.extend(made.iter().cloned());
        peer.apply(&made).await;
        books.push(gid);
    }
    peer.run("UPDATE books SET rating = 4").await;
    upgrade(&mut peer, widened()).await;
    // What a set left as it was holds of the field it gained is never sent.
    peer.checking = !at.iter().any(|&(wall_ms, _)| wall_ms == LAST_MS);
    let sent = peer.push(&mut hub).await;
    peer.checking = true;
    let mine = node_of(&mut peer).await;
    let clocks = books
        .iter()
        .map(|g| {
            let of: BTreeSet<Hlc> = sent
                .iter()
                .filter(|c| c.gid == *g)
                .map(|c| c.clock)
                .collect();
            assert!(
                of.len() <= 1 && of.iter().all(|c| c.node == mine),
                "{sent:#?}"
            );
            of.into_iter().next()
        })
        .collect();
    let mut peers = vec![peer, Peer::new(WIDE, widened()).await];
    none_shared(&mut peers, &hub).await;
    peers[0].settled().await;
    let rows = peers[0].lines(WIDE_ROWS).await;
    (clocks, rows)
}

#[tokio::test]
async fn a_completion_past_the_last_counter_of_a_millisecond_takes_the_next_millisecond() {
    let (clocks, rows) = completed_at(&[(SOON, u32::MAX)]).await;
    let at = clocks[0].unwrap();
    assert_eq!((at.wall_ms, at.counter), (SOON + 1, 0));
    assert_eq!(rows, ["b0 done 0 4"]);
}

/// Past a set written at wall 0, where counters number adoptions, a completion takes the first
/// reading of wall 1, so a counter from elsewhere never moves this node's own numbering.
#[tokio::test]
async fn a_completion_of_a_set_written_at_wall_zero_takes_the_first_reading_of_wall_one() {
    for at in [
        &[(0, 5), (0, 3)][..],
        &[(0, u32::MAX - 1), (0, 7), (0, u32::MAX)][..],
    ] {
        let (clocks, rows) = completed_at(at).await;
        let at: Vec<(u64, u32)> = clocks
            .iter()
            .map(|c| c.map(|c| (c.wall_ms, c.counter)).unwrap())
            .collect();
        assert!(at.iter().all(|&c| c == (1, 0)), "{at:?}");
        assert_eq!(rows.len(), at.len());
        assert!(rows.iter().all(|r| r.ends_with(" 4")), "{rows:?}");
    }
}

/// A set last written in the last millisecond the text form holds cannot be recorded again at a
/// later clock: it is left as that write left it, and the field it gained reads NULL. Only a
/// device writing against the application's intent stamps such a clock, and no later write can
/// change the set either.
#[tokio::test]
async fn a_set_written_in_the_last_millisecond_is_not_completed() {
    let (clocks, rows) = completed_at(&[(LAST_MS, 3), (SOON, 0)]).await;
    assert_eq!(clocks[0], None);
    assert!(clocks[1].is_some());
    assert_eq!(rows, ["b0 done 0 -", "b1 done 1 4"]);
}

#[tokio::test]
async fn a_database_without_a_record_of_its_sets_widens_them_as_any_other() {
    let mut hub = Vec::new();
    let mut peers = make(2, WIDE, paired).await;
    peers[0]
        .run("INSERT INTO books (title, status, finished) VALUES ('one', 'reading', 1)")
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[1]
        .run("UPDATE books SET status = 'done', finished = 2")
        .await;
    peers[1].push(&mut hub).await;
    let forget = "DELETE FROM fongo_meta WHERE key = 'together:books'";
    peers[0].run(forget).await;
    upgrade(&mut peers[0], paired()).await;
    assert_eq!(peers[0].push(&mut hub).await, []);
    peers[0].run(forget).await;
    peers[0].run("UPDATE books SET rating = 4").await;
    upgrade(&mut peers[0], widened()).await;
    settle(&mut peers, &mut hub).await;
    upgrade(&mut peers[1], widened()).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, WIDE_ROWS).await,
        ["one done 2 4"]
    );
}

const FOUND: &[&str] = &[
    "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, status TEXT,
                                             finished INTEGER, rating INTEGER, extra TEXT)",
];

/// `paired`, `widened`, or `widened` with a plain field more.
fn found(version: u8) -> Registry {
    let books = Collection::minted("books", "books", "id")
        .field("title")
        .field("status")
        .field("finished");
    let books = match version {
        0 => books.together(&["status", "finished"]),
        1 => books
            .field("rating")
            .together(&["status", "finished", "rating"]),
        _ => books
            .field("rating")
            .field("extra")
            .together(&["status", "finished", "rating"]),
    };
    Registry::new(vec![books]).unwrap()
}

/// Requires that no two of `sent` give one register one clock.
fn one_clock_per_write(sent: &[Change]) {
    for (i, a) in sent.iter().enumerate() {
        for b in &sent[..i] {
            assert!(
                !(a.gid == b.gid && a.field == b.field && a.clock == b.clock),
                "{a:?}\n{b:?}"
            );
        }
    }
}

/// A found row's set is written at wall 0; a change from elsewhere at wall 0 with a counter near
/// the last becomes the set's latest write. Completing the set past it must not spend this
/// node's adoption sequence, or every later adoption would take one clock, and a field adopted
/// twice with two values would carry two values at one clock.
#[tokio::test]
async fn a_foreign_adoption_counter_never_makes_a_clock_reused() {
    let mut a = Peer::bare(FOUND, found(0)).await;
    a.run(
        "INSERT INTO books (title, status, finished, rating, extra)
         VALUES ('one', 'r', 1, 4, 'e1'), ('two', 'r', 2, 5, 'e2')",
    )
    .await;
    a.install().await;
    let mut peers = vec![a, Peer::new(FOUND, found(0)).await];
    let mut hub = Vec::new();
    settle(&mut peers, &mut hub).await;
    let gid = peers[0].gid("books", 1).await;
    hub.push(Change {
        collection: "books".into(),
        gid,
        field: "status".into(),
        value: Value::Text("f".into()),
        clock: Hlc {
            wall_ms: 0,
            counter: u32::MAX - 1,
            node: Gid([7; 16]),
        },
    });
    settle(&mut peers, &mut hub).await;
    let mut sent = Vec::new();
    for version in [1, 2, 1, 2] {
        if version == 1 {
            peers[0].run("UPDATE books SET extra = extra || '+'").await;
        }
        upgrade(&mut peers[0], found(version)).await;
        sent.extend(peers[0].push(&mut hub).await);
    }
    let mine = node_of(&mut peers[0]).await;
    assert!(sent.iter().all(|c| c.clock.node == mine), "{sent:#?}");
    assert!(
        sent.iter()
            .all(|c| c.clock.wall_ms != 0 || c.clock.counter < 100),
        "{sent:#?}"
    );
    one_clock_per_write(&sent);
    none_shared(&mut peers, &hub).await;
    upgrade(&mut peers[1], found(2)).await;
    settle(&mut peers, &mut hub).await;
    none_shared(&mut peers, &hub).await;
    assert_eq!(
        same_everywhere(
            &mut peers,
            "SELECT title || ' ' || status || ' ' || rating || ' ' || extra FROM books ORDER BY 1"
        )
        .await,
        ["one f 4 e1++", "two r 5 e2++"]
    );
}

/// Widening, narrowing and widening again over a set whose latest write is a change from
/// elsewhere at wall 0 with a counter near the last, with the fields it gains written while not
/// declared, so each widening adopts them anew:
/// every clock this node gives one register is its own, and no replica takes it for another
/// database's.
#[tokio::test]
async fn widening_again_over_a_foreign_adoption_counter_reuses_no_clock() {
    let mut a = Peer::bare(FOUND, found(0)).await;
    a.run(
        "INSERT INTO books (title, status, finished, rating, extra) VALUES ('one', 'r', 1, 4, 'e')",
    )
    .await;
    a.install().await;
    let mut peers = vec![a, Peer::new(FOUND, found(0)).await];
    let mut hub = Vec::new();
    settle(&mut peers, &mut hub).await;
    let gid = peers[0].gid("books", 1).await;
    for (field, value) in [
        ("status", Value::Text("f".into())),
        ("finished", Value::Int(9)),
    ] {
        hub.push(Change {
            collection: "books".into(),
            gid,
            field: field.into(),
            value,
            clock: Hlc {
                wall_ms: 0,
                counter: u32::MAX - 1,
                node: Gid([7; 16]),
            },
        });
    }
    settle(&mut peers, &mut hub).await;
    let mut sent = Vec::new();
    for n in 0..5 {
        let version = if n % 2 == 0 { 2 } else { 0 };
        if version == 2 && n > 0 {
            peers[0]
                .run(&format!("UPDATE books SET rating = {n}, extra = 'e{n}'"))
                .await;
        }
        upgrade(&mut peers[0], found(version)).await;
        sent.extend(peers[0].push(&mut hub).await);
    }
    one_clock_per_write(&sent);
    none_shared(&mut peers, &hub).await;
    upgrade(&mut peers[1], found(2)).await;
    settle(&mut peers, &mut hub).await;
    none_shared(&mut peers, &hub).await;
    same_everywhere(&mut peers, WIDE_ROWS).await;
}

const NOTED: &[&str] = &[
    "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, status TEXT,
                                              note TEXT, rating INTEGER)",
];

fn noting(widened: bool) -> Registry {
    let books = Collection::minted("books", "books", "id")
        .field("title")
        .field("status")
        .field_reporting_conflicts("note");
    let books = if widened {
        books
            .field("rating")
            .together(&["status", "note", "rating"])
    } else {
        books.together(&["status", "note"])
    };
    Registry::new(vec![books]).unwrap()
}

/// A set recorded again by an upgrade carries the value an edit elsewhere replaced. The edit
/// wins, and the record is reported as a conflict whose remote value is the base, so a three-way
/// merge gives the local value back. A write that does change the field is reported too.
#[tokio::test]
async fn a_completion_is_reported_with_the_base_and_the_edit_kept() {
    let rows = "SELECT title || ' ' || COALESCE(status, '-') || ' ' || COALESCE(note, '-') || ' '
                       || COALESCE(rating, '-') FROM books";
    let mut hub = Vec::new();
    let mut peers = make(2, NOTED, || noting(false)).await;
    peers[0]
        .run("INSERT INTO books (title, status, note) VALUES ('one', 'reading', 'n0')")
        .await;
    settle(&mut peers, &mut hub).await;
    later();
    peers[1].run("UPDATE books SET note = 'mine'").await;
    peers[1].push(&mut hub).await;
    later();
    peers[0].run("UPDATE books SET rating = 4").await;
    upgrade(&mut peers[0], noting(true)).await;
    peers[0].push(&mut hub).await;
    let applied = peers[1].pull(&hub).await;
    assert_eq!(told(&applied), [conflict("n0", "mine", "n0", false)]);
    assert_eq!(peers[1].lines("SELECT note FROM books").await, ["mine"]);
    later();
    peers[0].run("UPDATE books SET note = 'theirs'").await;
    peers[0].push(&mut hub).await;
    let applied = peers[1].pull(&hub).await;
    assert_eq!(told(&applied), [conflict("n0", "mine", "theirs", true)]);
    upgrade(&mut peers[1], noting(true)).await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, rows).await,
        ["one reading theirs 4"]
    );
}

fn conflict(base: &str, local: &str, remote: &str, won: bool) -> (String, String, String, bool) {
    (base.into(), local.into(), remote.into(), won)
}

/// Base, local and remote text of each conflict reported, and whether the remote edit won.
fn told(applied: &fongo_sync_sqlite::Applied) -> Vec<(String, String, String, bool)> {
    fn text(v: &Value) -> String {
        match v {
            Value::Text(t) => t.clone(),
            _ => panic!("{v:?}"),
        }
    }
    applied
        .conflicts
        .iter()
        .map(|c| {
            (
                c.base.as_ref().map_or("-".into(), text),
                text(&c.local),
                text(&c.remote),
                c.remote_won,
            )
        })
        .collect()
}

/// Peer 2 changes the note from x to z; peer 1, having seen z, sets it back to x; peer 0, having
/// seen neither, edits it to y and then takes both, in one apply or in two. Peer 0 is told the
/// remote side's latest value, x, and never z alone as the last word. The world's clock puts
/// many writes in one millisecond and steps back.
async fn set_back(seed: u64, apart: bool) -> Vec<Vec<(String, String, String, bool)>> {
    let world = support::world::World::stepping_back(seed, 4, 3);
    let mut peers = Vec::new();
    for n in 0..3 {
        peers.push(Peer::in_world(&world, n, NOTED, noting(false)).await);
    }
    let mut hub = Vec::new();
    peers[0]
        .run("INSERT INTO books (title, status, note) VALUES ('one', 'reading', 'x')")
        .await;
    settle(&mut peers, &mut hub).await;
    peers[2].run("UPDATE books SET note = 'z'").await;
    peers[2].push(&mut hub).await;
    peers[1].pull(&hub).await;
    peers[1].run("UPDATE books SET note = 'x'").await;
    peers[1].push(&mut hub).await;
    peers[0].run("UPDATE books SET note = 'y'").await;
    let theirs = hub[peers[0].cursor..].to_vec();
    let mut reports = Vec::new();
    let applies: Vec<&[Change]> = if apart {
        let z = theirs
            .iter()
            .position(|c| c.value == Value::Text("x".into()))
            .unwrap();
        vec![&theirs[..z], &theirs[z..]]
    } else {
        vec![&theirs[..]]
    };
    for changes in applies {
        let applied = peers[0].apply(changes).await;
        reports.push(told(&applied));
    }
    peers[0].cursor = hub.len();
    let shown = peers[0].lines("SELECT note FROM books").await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, "SELECT note FROM books").await,
        shown
    );
    reports
}

#[tokio::test]
async fn a_remote_set_back_in_one_apply_is_reported_as_the_latest_remote_value() {
    for seed in 0..4 {
        let reports = set_back(seed, false).await;
        assert_eq!(reports.len(), 1);
        let [(base, local, remote, _)] = &reports[0][..] else {
            panic!("seed {seed}: {reports:?}");
        };
        assert_eq!(
            (&base[..], &local[..], &remote[..]),
            ("x", "y", "x"),
            "seed {seed}"
        );
    }
}

#[tokio::test]
async fn a_remote_set_back_after_its_change_was_reported_is_reported_again() {
    for seed in 0..4 {
        let reports = set_back(seed, true).await;
        let remotes: Vec<Vec<&str>> = reports
            .iter()
            .map(|r| r.iter().map(|(_, _, remote, _)| remote.as_str()).collect())
            .collect();
        assert_eq!(remotes, [vec!["z"], vec!["x"]], "seed {seed}: {reports:?}");
    }
}

// What a table refuses, or retries, never stops an apply.

/// Applies to a fresh replica `n` books whose titles, unique in the table but not an identity,
/// all move one place along at once, in the order that lets one write succeed per round. Returns
/// the statements the second apply ran and how many books it left waiting.
async fn statements_to_shift_titles(n: u64) -> (u64, usize) {
    let schema = ["CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT UNIQUE)"];
    let registry = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id").field("title"),
        ])
        .unwrap()
    };
    let mut peer = Peer::new(&schema, registry()).await;
    peer.checking = false;
    let gid = |i: u64| {
        let mut bytes = [0u8; 16];
        bytes[8..].copy_from_slice(&i.to_be_bytes());
        Gid(bytes)
    };
    let title = |i: u64| Value::Text(format!("t{i:05}"));
    let mut made = Vec::new();
    for i in 0..n {
        made.push(change("books", gid(i), DELETED, Value::Int(0), SOON));
        made.push(change("books", gid(i), "title", title(i), SOON));
    }
    peer.apply(&made).await;
    let shifted: Vec<Change> = (0..n)
        .map(|i| change("books", gid(i), "title", title(i + 1), SOON + 1))
        .collect();
    let statements = support::statements::Statements::watch(&mut peer.conn).await;
    let applied = peer.apply(&shifted).await;
    (statements.count(), applied.waiting)
}

/// Each round of retrying what a table refused writes one book, so without a bound on the rounds
/// the work would grow as the square of the books.
#[tokio::test]
async fn retrying_refused_writes_takes_a_bounded_number_of_rounds() {
    let (small, waiting) = statements_to_shift_titles(40).await;
    assert!(waiting > 0, "every book was written in one apply");
    let (large, _) = statements_to_shift_titles(160).await;
    assert!(large < small * 6, "{small} then {large}");
}

#[tokio::test]
async fn a_value_a_table_or_its_triggers_refuse_is_held_and_everything_else_applies() {
    let schema = [
        "CREATE TABLE books (id INTEGER PRIMARY KEY,
                             title TEXT CHECK (title IS NULL OR typeof(title) = 'text'))",
        "CREATE TRIGGER boom AFTER UPDATE OF title ON books WHEN NEW.title = 'boom'
         BEGIN SELECT json('{'); END",
        "CREATE TABLE tags (
            book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
            tag TEXT NOT NULL,
            PRIMARY KEY (book_id, tag))",
    ];
    let registry = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id").field("title"),
            Collection::natural("tags", "tags", &["book_id", "tag"])
                .owner("book_id", "books")
                .field("tag"),
        ])
        .unwrap()
    };
    let mut hub = Vec::new();
    let mut peers = make(2, &schema, registry).await;
    peers[0]
        .run("INSERT INTO books (id, title) VALUES (1, 'one'), (2, 'two'), (3, 'three')")
        .await;
    settle(&mut peers, &mut hub).await;
    let [one, two, three] = [
        peers[0].gid("books", 1).await,
        peers[0].gid("books", 2).await,
        peers[0].gid("books", 3).await,
    ];
    let blob = Value::Blob(vec![1, 2, 3]);
    let tag = Gid::derive("tags", &[Value::Ref(three), blob.clone()]);
    let batch = [
        change("books", one, "title", blob, SOON),
        change("books", two, "title", Value::Text("boom".into()), SOON),
        change("books", one, "title", Value::Real(f64::NAN), SOON + 1),
        change("tags", tag, DELETED, Value::Int(0), SOON),
        change("tags", tag, "book_id", Value::Ref(three), SOON),
        change("tags", tag, "tag", Value::Blob(vec![1, 2, 3]), SOON),
        change("books", three, "title", Value::Text("fine".into()), SOON),
    ];
    let applied = peers[1].apply(&batch).await;
    assert_eq!(applied.waiting, 3, "{:#?}", applied.held);
    assert_eq!(
        peers[1]
            .lines("SELECT title FROM books ORDER BY title")
            .await,
        ["fine", "one", "two"]
    );
    let later = [change(
        "books",
        three,
        "title",
        Value::Text("later".into()),
        SOON + 2,
    )];
    let applied = peers[1].apply(&later).await;
    assert_eq!(applied.waiting, 3);
    assert_eq!(
        peers[1]
            .lines("SELECT title FROM books ORDER BY title")
            .await,
        ["later", "one", "two"]
    );
    peers[1]
        .run("UPDATE books SET title = 'mine' WHERE title = 'later'")
        .await;
    let sent = peers[1].push(&mut hub).await;
    assert_eq!(sent.len(), 1, "{sent:#?}");
}

// What a column's affinity converts.

#[tokio::test]
async fn a_value_the_column_converts_is_not_sent_back() {
    let schema = ["CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, year INTEGER)"];
    let registry = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id")
                .field("title")
                .field("year"),
        ])
        .unwrap()
    };
    let mut hub = Vec::new();
    let mut peers = make(2, &schema, registry).await;
    peers[0]
        .run("INSERT INTO books (title, year) VALUES ('Middlemarch', 1871)")
        .await;
    settle(&mut peers, &mut hub).await;
    // The row shows the integer SQLite stores, the model the text that arrived.
    peers[1].checking = false;
    let gid = peers[1].gid("books", 1).await;
    peers[1]
        .apply(&[change(
            "books",
            gid,
            "year",
            Value::Text("1872".into()),
            SOON,
        )])
        .await;
    assert_eq!(
        peers[1]
            .lines("SELECT typeof(year) || ' ' || year FROM books")
            .await,
        ["integer 1872"]
    );
    peers[1]
        .run("UPDATE books SET title = 'Middlemarch!'")
        .await;
    let sent = peers[1].push(&mut hub).await;
    let fields: Vec<&str> = sent.iter().map(|c| c.field.as_str()).collect();
    assert_eq!(fields, ["title"], "{sent:#?}");
}

// A row that waits keeps what names it.

#[tokio::test]
async fn a_row_still_named_by_a_row_whose_write_was_refused_is_not_removed() {
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
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch'), ('Romola')")
        .await;
    peers[0]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'first')")
        .await;
    settle(&mut peers, &mut hub).await;
    // Here every write to the note is refused, so it cannot move off the book being deleted.
    peers[1]
        .run("CREATE TRIGGER refuse BEFORE UPDATE ON notes BEGIN SELECT RAISE(ABORT, 'refused'); END")
        .await;
    peers[0]
        .run("UPDATE notes SET book_id = 2, body = 'moved'")
        .await;
    peers[0].run("DELETE FROM books WHERE id = 1").await;
    peers[0].push(&mut hub).await;
    let applied = peers[1].pull(&hub).await;
    assert!(!applied.held.is_empty(), "{applied:#?}");
    assert_eq!(
        peers[1]
            .lines(
                "SELECT n.body || ' on ' || b.title FROM notes n JOIN books b ON b.id = n.book_id"
            )
            .await,
        ["first on Middlemarch"]
    );
}

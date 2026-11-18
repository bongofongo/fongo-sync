//! Ways replicas once parted or lost what was live, each in the smallest setting that showed it.
//! Every apply and collect is checked against the reference model, and every settled replica
//! must show its view.

mod support;

use std::str::FromStr;

use fongo_sync_sqlite::core::{ALIAS, Change, DELETED, Gid, Hlc, Value};
use fongo_sync_sqlite::{Applied, Collection, Registry, Replica, Resume};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};

struct Peer {
    conn: SqliteConnection,
    replica: Replica,
    cursor: usize,
    /// Every change applied here or sent from here.
    taken: Vec<Change>,
}

impl Peer {
    async fn new(schema: &[&str], registry: Registry) -> Peer {
        let mut conn = SqliteConnectOptions::from_str("sqlite::memory:")
            .unwrap()
            .foreign_keys(true)
            .connect()
            .await
            .unwrap();
        for sql in schema {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }
        let replica = Replica::new(registry);
        replica.install(&mut conn).await.unwrap();
        Peer {
            conn,
            replica,
            cursor: 0,
            taken: Vec::new(),
        }
    }

    async fn midway(&mut self) {
        if let Err(e) = support::midway(&mut self.conn, &self.replica, &self.taken).await {
            panic!("{e}");
        }
    }

    async fn settled(&mut self) {
        if let Err(e) = support::settled(&mut self.conn, &self.replica, &self.taken).await {
            panic!("{e}");
        }
    }

    async fn run(&mut self, sql: &str) {
        sqlx::query(sql).execute(&mut self.conn).await.unwrap();
    }

    async fn lines(&mut self, sql: &str) -> Vec<String> {
        sqlx::query_scalar(sql)
            .fetch_all(&mut self.conn)
            .await
            .unwrap()
    }

    async fn int(&mut self, sql: &str) -> i64 {
        sqlx::query_scalar(sql)
            .fetch_one(&mut self.conn)
            .await
            .unwrap()
    }

    async fn gid(&mut self, collection: &str, id: i64) -> Gid {
        let gid = self.replica.gid_of(&mut self.conn, collection, id).await;
        gid.unwrap().unwrap()
    }

    /// Collects and sends, returning what was sent.
    async fn push(&mut self, hub: &mut Vec<Change>) -> Vec<Change> {
        self.replica.collect(&mut self.conn).await.unwrap();
        let outbox = self
            .replica
            .outbox(&mut self.conn, 1_000_000)
            .await
            .unwrap();
        hub.extend(outbox.changes.iter().cloned());
        self.taken.extend(outbox.changes.iter().cloned());
        self.replica
            .acknowledge(&mut self.conn, outbox.through)
            .await
            .unwrap();
        self.midway().await;
        outbox.changes
    }

    async fn apply(&mut self, changes: &[Change]) -> Applied {
        self.taken.extend(changes.iter().cloned());
        let applied = self.replica.apply(&mut self.conn, changes).await.unwrap();
        self.midway().await;
        applied
    }

    async fn pull(&mut self, hub: &[Change]) -> Applied {
        let applied = self.apply(&hub[self.cursor..]).await;
        self.cursor = hub.len();
        applied
    }

    async fn sync(&mut self, hub: &mut Vec<Change>) -> Applied {
        self.push(hub).await;
        self.pull(hub).await
    }
}

async fn make(n: usize, schema: &[&str], registry: impl Fn() -> Registry) -> Vec<Peer> {
    let mut all = Vec::new();
    for _ in 0..n {
        all.push(Peer::new(schema, registry()).await);
    }
    all
}

/// Every peer sends and takes everything, until a round sends nothing.
async fn settle(peers: &mut [Peer], hub: &mut Vec<Change>) {
    for _ in 0..20 {
        let before = hub.len();
        for peer in peers.iter_mut() {
            peer.sync(hub).await;
        }
        if hub.len() == before && peers.iter().all(|p| p.cursor == hub.len()) {
            return;
        }
    }
    panic!("the peers never stopped sending");
}

/// The rows, the shadow and the alias groups are the same on every peer, and each shows the
/// model's view of what it has seen.
async fn same_everywhere(peers: &mut [Peer], rows: &str) -> Vec<String> {
    for peer in peers.iter_mut() {
        peer.settled().await;
    }
    let shadow = "SELECT collection || hex(gid) || field || quote(value) || clock
                    FROM fongo_shadow ORDER BY 1";
    let aliases = "SELECT collection || hex(gid) || hex(canon) FROM fongo_alias ORDER BY 1";
    let first = (
        peers[0].lines(rows).await,
        peers[0].lines(shadow).await,
        peers[0].lines(aliases).await,
    );
    for peer in peers.iter_mut().skip(1) {
        assert_eq!(peer.lines(rows).await, first.0);
        assert_eq!(peer.lines(shadow).await, first.1);
        assert_eq!(peer.lines(aliases).await, first.2);
    }
    first.0
}

fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

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

/// A gid belongs to the collection that used it first, so a reference to it from a field that
/// names another collection names nothing: the reference reads as null, and a row owned through
/// it is gone. The kernel keys entities by collection and gid and never decides which collection
/// a gid belongs to, so it waits for the referenced entity forever, keeping the row as it was.
#[tokio::test]
#[ignore = "the kernel does not claim gids for collections, so it waits on such references"]
async fn a_reference_to_a_gid_of_another_collection_names_nothing() {
    let mut hub = Vec::new();
    let (mut peers, [_, _, other, note]) = library_peers(&mut hub).await;
    hub.push(change("books", other, "author_id", Value::Ref(note), SOON));
    hub.push(change("notes", note, "book_id", Value::Ref(note), SOON));
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        same_everywhere(&mut peers, LIBRARY_ROWS).await,
        [
            "author Eliot",
            "author Gaskell",
            "book Cranford by -",
            "book Middlemarch by Eliot",
        ]
    );
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
        let replica = peers[0].replica.clone();
        replica.suspend(&mut peers[0].conn).await.unwrap();
        peers[0].run("DELETE FROM books WHERE id = 2").await;
        peers[0]
            .run("INSERT INTO books (id, title, isbn) VALUES (3, 'three', 't')")
            .await;
        replica.resume(&mut peers[0].conn, how).await.unwrap();
        peers[0].midway().await;
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

#[tokio::test]
async fn deleting_an_owner_of_many_rows_takes_time_in_proportion() {
    const NOTES: i64 = 20_000;
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
        .run("INSERT INTO books (id, title) VALUES (1, 'Middlemarch')")
        .await;
    peers[0]
        .run(&format!(
            "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < {NOTES})
             INSERT INTO notes (book_id, body) SELECT 1, 'note ' || x FROM n"
        ))
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(peers[1].int("SELECT count(*) FROM notes").await, NOTES);

    peers[0].run("DELETE FROM books").await;
    peers[0].push(&mut hub).await;
    let started = std::time::Instant::now();
    peers[1].pull(&hub).await;
    let took = started.elapsed();
    assert_eq!(peers[1].int("SELECT count(*) FROM notes").await, 0);
    // Removing the rows one by one against all the others took minutes.
    assert!(took < std::time::Duration::from_secs(60), "{took:?}");
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
    peer.replica = Replica::new(registry);
    peer.replica.install(&mut peer.conn).await.unwrap();
    peer.midway().await;
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

use std::str::FromStr;

use fongo_sync_sqlite::core::{Change, Gid, Strategy as Merge, Value};
use fongo_sync_sqlite::{Applied, Collection, Conflict, Registry, Replica};
use proptest::prelude::*;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};

const SCHEMA: &[&str] = &[
    "CREATE TABLE authors (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        born INTEGER NOT NULL DEFAULT 0,
        UNIQUE (name, born))",
    "CREATE TABLE books (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        year INTEGER,
        pages INTEGER,
        author_id INTEGER REFERENCES authors(id) ON DELETE SET NULL,
        isbn TEXT UNIQUE,
        isbn10 TEXT UNIQUE)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
        body TEXT)",
    "CREATE TABLE marks (
        id INTEGER PRIMARY KEY,
        note_id INTEGER NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
        place INTEGER,
        UNIQUE (note_id, place))",
    "CREATE TABLE tags (
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        tag TEXT NOT NULL,
        note TEXT,
        PRIMARY KEY (book_id, tag))",
    "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE loans (
        id INTEGER PRIMARY KEY,
        book_id INTEGER REFERENCES books(id) ON DELETE SET NULL,
        borrower TEXT)",
];

fn registry() -> Registry {
    Registry::new(vec![
        Collection::minted("authors", "authors", "id")
            .field("name")
            .field("born")
            .identity(&["name", "born"]),
        Collection::minted("books", "books", "id")
            .field("title")
            .field("year")
            .field_with("pages", Merge::Max)
            .reference("author_id", "authors")
            .field("isbn")
            .field("isbn10")
            .identity(&["isbn"])
            .identity(&["isbn10"]),
        Collection::minted("notes", "notes", "id")
            .owner("book_id", "books")
            .field_reporting_conflicts("body"),
        Collection::minted("marks", "marks", "id")
            .owner("note_id", "notes")
            .field("place")
            .identity(&["note_id", "place"]),
        Collection::natural("tags", "tags", &["book_id", "tag"])
            .owner("book_id", "books")
            .field("tag")
            .field("note"),
        Collection::natural("settings", "settings", &["key"])
            .field("key")
            .field("value"),
        Collection::minted("loans", "loans", "id")
            .reference("book_id", "books")
            .field("borrower"),
    ])
    .unwrap()
}

const DUMP: &str = "
    SELECT 'author ' || COALESCE(hex(i.gid), 'UNMAPPED') || ' ' || a.name || ' ' || a.born
      FROM authors a LEFT JOIN fongo_ids i ON i.collection = 'authors' AND i.key = json_array(a.id)
    UNION ALL
    SELECT 'book ' || COALESCE(hex(i.gid), 'UNMAPPED')
           || ' i=' || COALESCE(b.isbn, '-') || ' j=' || COALESCE(b.isbn10, '-') || ' ' || b.title
           || ' y=' || COALESCE(b.year, '-') || ' p=' || COALESCE(b.pages, '-') || ' a='
           || COALESCE((SELECT hex(gid) FROM fongo_ids
                        WHERE collection = 'authors' AND key = json_array(b.author_id)), '-')
      FROM books b LEFT JOIN fongo_ids i ON i.collection = 'books' AND i.key = json_array(b.id)
    UNION ALL
    SELECT 'note ' || COALESCE(hex(i.gid), 'UNMAPPED') || ' ' || COALESCE(n.body, '-') || ' b='
           || COALESCE((SELECT hex(gid) FROM fongo_ids
                        WHERE collection = 'books' AND key = json_array(n.book_id)), '-')
      FROM notes n LEFT JOIN fongo_ids i ON i.collection = 'notes' AND i.key = json_array(n.id)
    UNION ALL
    SELECT 'mark ' || COALESCE(hex(i.gid), 'UNMAPPED') || ' ' || COALESCE(m.place, '-') || ' n='
           || COALESCE((SELECT hex(gid) FROM fongo_ids
                        WHERE collection = 'notes' AND key = json_array(m.note_id)), '-')
      FROM marks m LEFT JOIN fongo_ids i ON i.collection = 'marks' AND i.key = json_array(m.id)
    UNION ALL
    SELECT 'tag ' || COALESCE((SELECT hex(gid) FROM fongo_ids
                               WHERE collection = 'books' AND key = json_array(t.book_id)), 'UNMAPPED')
           || ' ' || t.tag || ' ' || COALESCE(t.note, '-')
      FROM tags t
    UNION ALL
    SELECT 'setting ' || key || '=' || value FROM settings
    UNION ALL
    SELECT 'loan ' || COALESCE(hex(i.gid), 'UNMAPPED') || ' ' || COALESCE(l.borrower, '-') || ' b='
           || COALESCE((SELECT hex(gid) FROM fongo_ids
                        WHERE collection = 'books' AND key = json_array(l.book_id)), '-')
      FROM loans l LEFT JOIN fongo_ids i ON i.collection = 'loans' AND i.key = json_array(l.id)
    ORDER BY 1";

const SHADOW: &str = "
    SELECT hex(gid) || ' ' || field || ' ' || quote(value) || ' ' || is_ref || ' ' || clock
      FROM fongo_shadow ORDER BY 1";

const ALIASES: &str = "SELECT hex(gid) || ' ' || hex(canon) FROM fongo_alias ORDER BY 1";

struct Peer {
    conn: SqliteConnection,
    replica: Replica,
    cursor: usize,
}

impl Peer {
    async fn bare() -> Peer {
        Peer::bare_with(SCHEMA, registry()).await
    }

    async fn bare_with(schema: &[&str], registry: Registry) -> Peer {
        let mut conn = SqliteConnectOptions::from_str("sqlite::memory:")
            .unwrap()
            .foreign_keys(true)
            .connect()
            .await
            .unwrap();
        for sql in schema {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }
        Peer {
            conn,
            replica: Replica::new(registry),
            cursor: 0,
        }
    }

    async fn with(schema: &[&str], registry: Registry) -> Peer {
        let mut peer = Peer::bare_with(schema, registry).await;
        peer.install().await;
        peer
    }

    async fn new() -> Peer {
        let mut peer = Peer::bare().await;
        peer.install().await;
        peer
    }

    async fn install(&mut self) {
        self.replica.install(&mut self.conn).await.unwrap();
    }

    async fn run(&mut self, sql: &str) {
        sqlx::query(sql).execute(&mut self.conn).await.unwrap();
    }

    async fn ids(&mut self, table: &str) -> Vec<i64> {
        sqlx::query_scalar(&format!("SELECT rowid FROM {table} ORDER BY rowid"))
            .fetch_all(&mut self.conn)
            .await
            .unwrap()
    }

    async fn pick(&mut self, table: &str, n: usize) -> Option<i64> {
        let ids = self.ids(table).await;
        (!ids.is_empty()).then(|| ids[n % ids.len()])
    }

    /// Sends local changes to the hub, then takes everything the hub has that this peer has not
    /// seen, its own changes included.
    async fn sync(&mut self, hub: &mut Vec<Change>) -> Applied {
        self.replica.collect(&mut self.conn).await.unwrap();
        let outbox = self.replica.outbox(&mut self.conn, 100_000).await.unwrap();
        hub.extend(outbox.changes);
        self.replica
            .acknowledge(&mut self.conn, outbox.through)
            .await
            .unwrap();
        let applied = self
            .replica
            .apply(&mut self.conn, &hub[self.cursor..])
            .await
            .unwrap();
        self.cursor = hub.len();
        applied
    }

    /// Takes what the hub has without sending first, so local edits are still unsent when the
    /// incoming changes are applied.
    async fn pull(&mut self, hub: &[Change]) -> Applied {
        let applied = self
            .replica
            .apply(&mut self.conn, &hub[self.cursor..])
            .await
            .unwrap();
        self.cursor = hub.len();
        applied
    }

    async fn gid(&mut self, collection: &str, id: i64) -> Gid {
        let gid = self.replica.gid_of(&mut self.conn, collection, id).await;
        gid.unwrap().unwrap()
    }

    async fn lines(&mut self, sql: &str) -> Vec<String> {
        sqlx::query_scalar(sql)
            .fetch_all(&mut self.conn)
            .await
            .unwrap()
    }

    async fn dump(&mut self) -> Vec<String> {
        self.lines(DUMP).await
    }
}

/// Syncs every peer in turn until a whole round sends nothing new, so each has seen everything.
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

async fn assert_converged(peers: &mut [Peer]) -> Vec<String> {
    let first = peers[0].dump().await;
    let shadow = peers[0].lines(SHADOW).await;
    assert!(
        !first.iter().any(|line| line.contains("UNMAPPED")),
        "a row has no global id: {first:#?}"
    );
    let aliases = peers[0].lines(ALIASES).await;
    for peer in peers.iter_mut().skip(1) {
        assert_eq!(peer.dump().await, first);
        assert_eq!(peer.lines(SHADOW).await, shadow);
        assert_eq!(peer.lines(ALIASES).await, aliases);
    }
    first
}

#[tokio::test]
async fn rows_and_their_references_reach_another_replica() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    let a = &mut peers[0];
    a.run("INSERT INTO authors (name) VALUES ('Eliot')").await;
    a.run("INSERT INTO books (title, year, author_id) VALUES ('Middlemarch', 1871, 1)")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'slow start')")
        .await;
    a.run("INSERT INTO tags (book_id, tag, note) VALUES (1, 'classic', 'yes')")
        .await;
    a.run("INSERT INTO settings VALUES ('theme', 'dark')").await;
    // Local ids differ between the replicas, so a reference has to be translated.
    peers[1]
        .run("INSERT INTO authors (name) VALUES ('Woolf')")
        .await;
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    assert_eq!(state.len(), 6, "{state:#?}");
    let author: String =
        sqlx::query_scalar("SELECT a.name FROM books b JOIN authors a ON a.id = b.author_id")
            .fetch_one(&mut peers[1].conn)
            .await
            .unwrap();
    assert_eq!(author, "Eliot");
}

#[tokio::test]
async fn concurrent_edits_to_different_fields_both_survive() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    settle(&mut peers, &mut hub).await;

    peers[0].run("UPDATE books SET year = 1871").await;
    peers[1]
        .run("UPDATE books SET title = 'Middlemarch: A Study'")
        .await;
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    assert!(
        state[0].contains("Middlemarch: A Study y=1871"),
        "{state:#?}"
    );
}

#[tokio::test]
async fn an_edit_made_after_seeing_another_wins() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title) VALUES ('one')")
        .await;
    settle(&mut peers, &mut hub).await;
    peers[0].run("UPDATE books SET title = 'two'").await;
    settle(&mut peers, &mut hub).await;
    peers[1].run("UPDATE books SET title = 'three'").await;
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    assert!(state[0].contains(" three "), "{state:#?}");
}

#[tokio::test]
async fn a_delete_takes_what_the_row_owned_even_if_added_elsewhere_meanwhile() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[0]
        .run("INSERT INTO tags (book_id, tag) VALUES (1, 'classic')")
        .await;
    settle(&mut peers, &mut hub).await;

    peers[0].run("DELETE FROM books").await;
    peers[1]
        .run("INSERT INTO tags (book_id, tag) VALUES (1, 'unread')")
        .await;
    peers[1]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'late')")
        .await;
    settle(&mut peers, &mut hub).await;

    assert_eq!(assert_converged(&mut peers).await, Vec::<String>::new());
}

#[tokio::test]
async fn a_reused_local_id_is_a_new_entity() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title) VALUES ('old')")
        .await;
    settle(&mut peers, &mut hub).await;
    let before = peers[0].dump().await;

    peers[0].run("DELETE FROM books").await;
    peers[0]
        .run("INSERT INTO books (title) VALUES ('new')")
        .await;
    assert_eq!(peers[0].ids("books").await, vec![1]);
    settle(&mut peers, &mut hub).await;

    let after = assert_converged(&mut peers).await;
    assert_eq!(after.len(), 1);
    assert!(after[0].ends_with("new y=- p=- a=-"), "{after:#?}");
    assert_ne!(before[0].split(' ').nth(1), after[0].split(' ').nth(1));
}

#[tokio::test]
async fn the_same_natural_key_made_twice_is_one_entity() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO settings VALUES ('theme', 'dark')")
        .await;
    peers[1]
        .run("INSERT INTO settings VALUES ('theme', 'light')")
        .await;
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    assert_eq!(state.len(), 1, "{state:#?}");

    peers[0].run("DELETE FROM settings").await;
    settle(&mut peers, &mut hub).await;
    assert!(assert_converged(&mut peers).await.is_empty());

    peers[1]
        .run("INSERT INTO settings VALUES ('theme', 'sepia')")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        assert_converged(&mut peers).await,
        vec!["setting theme=sepia"]
    );
}

#[tokio::test]
async fn the_greater_value_wins_whenever_it_was_written() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title, pages) VALUES ('Middlemarch', 300)")
        .await;
    settle(&mut peers, &mut hub).await;

    peers[1].run("UPDATE books SET pages = 120").await;
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    assert!(state[0].contains("p=300"), "{state:#?}");
}

#[tokio::test]
async fn rows_that_existed_before_install_fill_gaps_without_overwriting() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::bare().await];
    peers[0]
        .run("INSERT INTO settings VALUES ('theme', 'dark')")
        .await;
    peers[0].sync(&mut hub).await;

    peers[1]
        .run("INSERT INTO settings VALUES ('theme', 'light')")
        .await;
    peers[1]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[1].install().await;
    peers[1].install().await;
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    assert_eq!(state.len(), 2, "{state:#?}");
    assert_eq!(state[1], "setting theme=dark");
}

#[tokio::test]
async fn applying_remote_changes_queues_nothing_to_send_back() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[0].sync(&mut hub).await;
    let sent = hub.len();
    peers[1].sync(&mut hub).await;
    peers[1].sync(&mut hub).await;
    assert_eq!(hub.len(), sent);
}

async fn count(peer: &mut Peer, sql: &str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(&mut peer.conn)
        .await
        .unwrap()
}

fn gid_in(line: &str) -> &str {
    line.split(' ').nth(1).unwrap()
}

#[tokio::test]
async fn the_same_identity_made_on_two_replicas_is_one_entity_with_all_its_children() {
    let mut hub = Vec::new();
    // The third peer makes nothing, so it meets the two books only as remote changes.
    let mut peers = [Peer::new().await, Peer::new().await, Peer::new().await];
    let a = &mut peers[0];
    a.run("INSERT INTO authors (name) VALUES ('Eliot')").await;
    a.run(
        "INSERT INTO books (title, year, author_id, isbn)
         VALUES ('Middlemarch', 1871, 1, '9780141439549')",
    )
    .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'from a')")
        .await;
    a.run("INSERT INTO tags (book_id, tag, note) VALUES (1, 'classic', 'a'), (1, 'shared', 'a')")
        .await;
    a.run("INSERT INTO loans (book_id, borrower) VALUES (1, 'Dorothea')")
        .await;
    let b = &mut peers[1];
    b.run("INSERT INTO authors (name) VALUES ('Woolf')").await;
    b.run(
        "INSERT INTO books (title, pages, isbn, isbn10)
         VALUES ('Middlemarch: A Study', 880, '9780141439549', '0141439548')",
    )
    .await;
    b.run("INSERT INTO notes (book_id, body) VALUES (1, 'from b')")
        .await;
    b.run("INSERT INTO tags (book_id, tag, note) VALUES (1, 'victorian', 'b'), (1, 'shared', 'b')")
        .await;
    b.run("INSERT INTO loans (book_id, borrower) VALUES (1, 'Celia')")
        .await;
    let mut made = Vec::new();
    for peer in &mut peers[..2] {
        peer.replica.collect(&mut peer.conn).await.unwrap();
        let gid = peer.replica.gid_of(&mut peer.conn, "books", 1).await;
        made.push(gid.unwrap().unwrap());
    }
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    let books: Vec<&String> = state.iter().filter(|l| l.starts_with("book ")).collect();
    assert_eq!(books.len(), 1, "{state:#?}");
    let book = gid_in(books[0]);
    assert!(
        books[0].contains("i=9780141439549 j=0141439548 "),
        "{state:#?}"
    );
    assert!(books[0].contains(" y=1871 p=880 "), "{state:#?}");
    for kind in ["note ", "loan "] {
        let children: Vec<&String> = state.iter().filter(|l| l.starts_with(kind)).collect();
        assert_eq!(children.len(), 2, "{state:#?}");
        assert!(
            children.iter().all(|l| l.ends_with(&format!(" b={book}"))),
            "{state:#?}"
        );
    }
    let tags: Vec<&String> = state.iter().filter(|l| l.starts_with("tag ")).collect();
    assert_eq!(tags.len(), 3, "{state:#?}");
    assert!(tags.iter().all(|l| gid_in(l) == book), "{state:#?}");

    // One group for the books, and one for the two "shared" tags, which are now the same row.
    let aliases = peers[0].lines(ALIASES).await;
    assert_eq!(aliases.len(), 4, "{aliases:#?}");
    assert_eq!(
        aliases.iter().filter(|l| l.ends_with(book)).count(),
        2,
        "{aliases:#?}"
    );
    for peer in peers.iter_mut() {
        assert_eq!(count(peer, "SELECT count(*) FROM fongo_pending").await, 0);
        assert_eq!(count(peer, "SELECT count(*) FROM books").await, 1);
    }
    // The group is known by the smallest of its ids, on every replica.
    assert_eq!(book, made.iter().min().unwrap().to_hex().to_uppercase());

    // The merged book is edited and deleted like any other.
    peers[2]
        .run("UPDATE books SET title = 'Middlemarch, again'")
        .await;
    settle(&mut peers, &mut hub).await;
    let state = assert_converged(&mut peers).await;
    assert!(
        state.iter().any(|l| l.contains(" Middlemarch, again y=")),
        "{state:#?}"
    );
    peers[1].run("DELETE FROM books").await;
    settle(&mut peers, &mut hub).await;
    let state = assert_converged(&mut peers).await;
    assert!(
        state
            .iter()
            .all(|l| l.starts_with("author ") || l.starts_with("loan ")),
        "{state:#?}"
    );
    assert!(
        state
            .iter()
            .filter(|l| l.starts_with("loan "))
            .all(|l| l.ends_with(" b=-"))
    );
}

#[tokio::test]
async fn identities_found_on_different_replicas_at_once_make_one_group() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title, isbn) VALUES ('a', '9780141439549')")
        .await;
    peers[1]
        .run("INSERT INTO books (title, isbn, isbn10) VALUES ('b', '9780141439549', '0141439548')")
        .await;
    peers[2]
        .run("INSERT INTO books (title, isbn10) VALUES ('c', '0141439548')")
        .await;
    peers[0]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'a')")
        .await;
    peers[2]
        .run("INSERT INTO tags (book_id, tag) VALUES (1, 'c')")
        .await;

    // The first and the third each see only the second's book, and find a different match.
    peers[1].sync(&mut hub).await;
    for who in [0, 2] {
        let peer = &mut peers[who];
        peer.replica.apply(&mut peer.conn, &hub).await.unwrap();
        peer.cursor = hub.len();
        assert_eq!(peer.lines(ALIASES).await.len(), 2);
        assert_eq!(count(peer, "SELECT count(*) FROM books").await, 1);
    }
    assert_ne!(peers[0].lines(ALIASES).await, peers[2].lines(ALIASES).await);
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    assert_eq!(peers[0].lines(ALIASES).await.len(), 3);
    let books: Vec<&String> = state.iter().filter(|l| l.starts_with("book ")).collect();
    assert_eq!(books.len(), 1, "{state:#?}");
    let book = gid_in(books[0]);
    assert!(
        state
            .iter()
            .any(|l| l.starts_with("note ") && l.ends_with(&format!(" b={book}")))
    );
    assert!(
        state.iter().any(|l| *l == format!("tag {book} c -")),
        "{state:#?}"
    );
}

#[tokio::test]
async fn a_row_that_is_only_out_of_date_is_not_mistaken_for_the_same_entity() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title, isbn) VALUES ('one', 'x'), ('two', 'y')")
        .await;
    settle(&mut peers, &mut hub).await;

    // The isbns are swapped by way of a third value, so they never clash on the first replica.
    peers[0]
        .run("UPDATE books SET isbn = 'z' WHERE id = 1")
        .await;
    peers[0]
        .run("UPDATE books SET isbn = 'x' WHERE id = 2")
        .await;
    peers[0]
        .run("UPDATE books SET isbn = 'y' WHERE id = 1")
        .await;
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    assert_eq!(state.len(), 2, "{state:#?}");
    assert!(peers[0].lines(ALIASES).await.is_empty());
}

fn touched(applied: &Applied) -> Vec<(&str, &str, Gid, bool)> {
    applied
        .touched
        .iter()
        .map(|t| (t.collection.as_str(), t.key.as_str(), t.gid, t.deleted))
        .collect()
}

#[tokio::test]
async fn apply_reports_each_row_it_wrote_or_removed() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[0]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'slow start')")
        .await;
    peers[0]
        .run("INSERT INTO tags (book_id, tag) VALUES (1, 'classic')")
        .await;
    assert!(touched(&peers[0].sync(&mut hub).await).is_empty());
    let book = peers[0].gid("books", 1).await;
    let note = peers[0].gid("notes", 1).await;
    let tag = Gid::derive("tags", &[Value::Ref(book), Value::Text("classic".into())]);

    let applied = peers[1].sync(&mut hub).await;
    assert_eq!(
        touched(&applied),
        vec![
            ("books", "[1]", book, false),
            ("notes", "[1]", note, false),
            ("tags", "[1,\"classic\"]", tag, false),
        ]
    );

    peers[0].run("UPDATE books SET year = 1871").await;
    peers[0].sync(&mut hub).await;
    let applied = peers[1].sync(&mut hub).await;
    assert_eq!(touched(&applied), vec![("books", "[1]", book, false)]);

    // What the database removes along with the book is reported too.
    peers[0].run("DELETE FROM books").await;
    peers[0].sync(&mut hub).await;
    let applied = peers[1].sync(&mut hub).await;
    assert_eq!(
        touched(&applied),
        vec![
            ("books", "[1]", book, true),
            ("notes", "[1]", note, true),
            ("tags", "[1,\"classic\"]", tag, true),
        ]
    );
    assert!(touched(&peers[0].sync(&mut hub).await).is_empty());
}

#[tokio::test]
async fn a_merge_reports_the_row_given_up_and_the_rows_moved_off_it() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await, Peer::new().await];
    peers[1].run("INSERT INTO books (title) VALUES ('b')").await;
    peers[1].sync(&mut hub).await;
    peers[0]
        .run("INSERT INTO books (title, isbn) VALUES ('a', '9780141439549')")
        .await;
    peers[0].sync(&mut hub).await;
    // The third replica holds both books as two rows, and lends the one without an isbn.
    peers[2].sync(&mut hub).await;
    peers[2]
        .run("INSERT INTO loans (book_id, borrower) SELECT id, 'Celia' FROM books WHERE isbn IS NULL")
        .await;

    // The second replica gives its book the same isbn before it has seen the first's.
    peers[1]
        .run("UPDATE books SET isbn = '9780141439549'")
        .await;
    peers[1].sync(&mut hub).await;
    peers[1].sync(&mut hub).await;
    let applied = peers[2].sync(&mut hub).await;

    let rows = touched(&applied);
    let books: Vec<_> = rows.iter().filter(|r| r.0 == "books").collect();
    assert_eq!(books.len(), 2, "{rows:#?}");
    assert_eq!(books[0].2, books[1].2, "{rows:#?}");
    assert_ne!(books[0].3, books[1].3, "{rows:#?}");
    let kept: i64 = count(&mut peers[2], "SELECT id FROM books").await;
    assert!(rows.contains(&("books", &format!("[{kept}]"), books[0].2, false)));
    let loan = peers[2].gid("loans", 1).await;
    assert!(rows.contains(&("loans", "[1]", loan, false)), "{rows:#?}");
    assert_eq!(
        count(&mut peers[2], "SELECT book_id FROM loans").await,
        kept
    );

    settle(&mut peers, &mut hub).await;
    let state = assert_converged(&mut peers).await;
    assert_eq!(state.len(), 2, "{state:#?}");
}

#[tokio::test]
async fn a_row_moved_off_a_parent_deleted_in_the_same_batch_keeps_what_it_owns() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    let a = &mut peers[0];
    a.run("INSERT INTO books (title) VALUES ('Middlemarch'), ('Daniel Deronda')")
        .await;
    a.run("INSERT INTO notes (book_id, body) VALUES (1, 'moved')")
        .await;
    a.run("INSERT INTO marks (note_id, place) VALUES (1, 7)")
        .await;
    a.run("INSERT INTO loans (book_id, borrower) VALUES (1, 'Celia')")
        .await;
    settle(&mut peers, &mut hub).await;
    let note_row: i64 = count(&mut peers[1], "SELECT id FROM notes").await;

    peers[0].run("UPDATE notes SET book_id = 2").await;
    peers[0].run("DELETE FROM books WHERE id = 1").await;
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    let book = peers[0].gid("books", 2).await.to_hex().to_uppercase();
    let note = peers[0].gid("notes", 1).await.to_hex().to_uppercase();
    assert!(
        state.contains(&format!("note {note} moved b={book}")),
        "{state:#?}"
    );
    assert!(
        state
            .iter()
            .any(|l| l.starts_with("mark ") && l.ends_with(&format!(" 7 n={note}"))),
        "{state:#?}"
    );
    assert!(
        state
            .iter()
            .any(|l| l.starts_with("loan ") && l.ends_with(" b=-")),
        "{state:#?}"
    );
    assert_eq!(count(&mut peers[1], "SELECT id FROM notes").await, note_row);
}

#[tokio::test]
async fn a_not_null_identity_merges_and_is_given_up_by_an_out_of_date_row() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO authors (name) VALUES ('Eliot'), ('Gaskell')")
        .await;
    peers[1]
        .run("INSERT INTO authors (name) VALUES ('Eliot')")
        .await;
    settle(&mut peers, &mut hub).await;
    let state = assert_converged(&mut peers).await;
    assert_eq!(state.len(), 2, "{state:#?}");
    assert_eq!(peers[0].lines(ALIASES).await.len(), 2);

    // The names are swapped by way of a third, so they never clash on the first replica.
    peers[0]
        .run("UPDATE authors SET name = 'x' WHERE id = 1")
        .await;
    peers[0]
        .run("UPDATE authors SET name = 'Eliot' WHERE id = 2")
        .await;
    peers[0]
        .run("UPDATE authors SET name = 'Gaskell' WHERE id = 1")
        .await;
    settle(&mut peers, &mut hub).await;
    let state = assert_converged(&mut peers).await;
    assert_eq!(state.len(), 2, "{state:#?}");
    assert_eq!(peers[0].lines(ALIASES).await.len(), 2);
    for peer in peers.iter_mut() {
        assert_eq!(count(peer, "SELECT count(*) FROM fongo_pending").await, 0);
    }
}

#[tokio::test]
async fn an_identity_of_several_fields_with_an_owner_is_matched_under_the_same_owner() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[0]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'one'), (1, 'two')")
        .await;
    settle(&mut peers, &mut hub).await;

    peers[0]
        .run("INSERT INTO marks (note_id, place) VALUES (1, 3), (2, 3)")
        .await;
    peers[1]
        .run("INSERT INTO marks (note_id, place) VALUES (1, 3), (1, 4)")
        .await;
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    let marks: Vec<&String> = state.iter().filter(|l| l.starts_with("mark ")).collect();
    assert_eq!(marks.len(), 3, "{state:#?}");
    assert_eq!(peers[0].lines(ALIASES).await.len(), 2);
}

#[tokio::test]
async fn an_owner_graph_with_many_paths_is_walked_once() {
    // Each part is owned by both parts of the level above, so a part deep down has 2^depth
    // chains of owners up to the top.
    const LEVELS: i64 = 40;
    let schema = ["CREATE TABLE parts (
        id INTEGER PRIMARY KEY,
        a INTEGER REFERENCES parts(id) ON DELETE CASCADE,
        b INTEGER REFERENCES parts(id) ON DELETE CASCADE)"];
    let registry = || {
        Registry::new(vec![
            Collection::minted("parts", "parts", "id")
                .owner("a", "parts")
                .owner("b", "parts"),
        ])
        .unwrap()
    };
    let mut hub = Vec::new();
    let mut peers = [
        Peer::with(&schema, registry()).await,
        Peer::with(&schema, registry()).await,
    ];
    peers[0].run("INSERT INTO parts (id) VALUES (1), (2)").await;
    for level in 1..LEVELS {
        let (top, up) = (2 * level + 1, 2 * level - 1);
        peers[0]
            .run(&format!(
                "INSERT INTO parts (id, a, b) VALUES ({top}, {up}, {}), ({}, {up}, {})",
                up + 1,
                top + 1,
                up + 1
            ))
            .await;
    }
    let started = std::time::Instant::now();
    settle(&mut peers, &mut hub).await;
    assert_eq!(
        count(&mut peers[1], "SELECT count(*) FROM parts").await,
        2 * LEVELS
    );

    // Deleting one part at the top takes every part below it, since each is owned by it.
    peers[0].run("DELETE FROM parts WHERE id = 1").await;
    settle(&mut peers, &mut hub).await;
    for peer in peers.iter_mut() {
        assert_eq!(count(peer, "SELECT count(*) FROM parts").await, 1);
        assert_eq!(count(peer, "SELECT count(*) FROM fongo_pending").await, 0);
    }
    let kept = peers[0].gid("parts", 2).await;
    let row = count(&mut peers[1], "SELECT id FROM parts").await;
    assert_eq!(peers[1].gid("parts", row).await, kept);
    assert!(started.elapsed() < std::time::Duration::from_secs(60));
}

/// Lets the wall clock move on, so the next edit is stamped later than the last.
fn later() {
    std::thread::sleep(std::time::Duration::from_millis(5));
}

async fn note_body(peer: &mut Peer) -> String {
    sqlx::query_scalar("SELECT body FROM notes")
        .fetch_one(&mut peer.conn)
        .await
        .unwrap()
}

#[tokio::test]
async fn concurrent_edits_to_a_field_that_reports_conflicts_are_reported_whichever_wins() {
    let text = |s: &str| Value::Text(s.into());
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[0]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
        .await;
    settle(&mut peers, &mut hub).await;
    let note = peers[0].gid("notes", 1).await;
    let conflict = |base: &str, local: &str, remote: &str, remote_won| Conflict {
        collection: "notes".into(),
        gid: note,
        key: Some("[1]".into()),
        field: "body".into(),
        base: Some(text(base)),
        local: text(local),
        remote: text(remote),
        remote_won,
    };

    // The second replica's edit is the later, and it learns of the first while holding it.
    peers[0].run("UPDATE notes SET body = 'one'").await;
    later();
    peers[1].run("UPDATE notes SET body = 'two'").await;
    assert!(peers[0].sync(&mut hub).await.conflicts.is_empty());
    let applied = peers[1].pull(&hub).await;
    assert_eq!(
        applied.conflicts,
        vec![conflict("base", "two", "one", false)]
    );
    settle(&mut peers, &mut hub).await;
    assert_converged(&mut peers).await;
    assert_eq!(note_body(&mut peers[0]).await, "two");

    // Now the replica holding its edit made the earlier one.
    peers[0].run("UPDATE notes SET body = 'three'").await;
    later();
    peers[1].run("UPDATE notes SET body = 'four'").await;
    assert!(peers[1].sync(&mut hub).await.conflicts.is_empty());
    let applied = peers[0].pull(&hub).await;
    assert_eq!(
        applied.conflicts,
        vec![conflict("two", "three", "four", true)]
    );
    settle(&mut peers, &mut hub).await;
    assert_converged(&mut peers).await;
    assert_eq!(note_body(&mut peers[1]).await, "four");
}

#[tokio::test]
async fn an_edit_made_after_seeing_another_is_not_a_conflict() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[0]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
        .await;
    settle(&mut peers, &mut hub).await;

    peers[0].run("UPDATE notes SET body = 'one'").await;
    peers[0].sync(&mut hub).await;
    peers[1].sync(&mut hub).await;
    peers[1].run("UPDATE notes SET body = 'two'").await;
    let mut reports = Vec::new();
    for _ in 0..2 {
        for peer in peers.iter_mut() {
            reports.extend(peer.pull(&hub).await.conflicts);
            reports.extend(peer.sync(&mut hub).await.conflicts);
        }
    }
    assert!(reports.is_empty(), "{reports:#?}");
    assert_converged(&mut peers).await;
    assert_eq!(note_body(&mut peers[0]).await, "two");
}

#[derive(Debug, Clone)]
enum Op {
    AddAuthor(u8),
    RenameAuthor(usize, u8),
    SetBorn(usize, u8),
    AddBook(u8, usize),
    SetTitle(usize, u8),
    SetYear(usize, u8),
    SetPages(usize, u8),
    SetAuthor(usize, usize),
    DeleteBook(usize),
    DeleteAuthor(usize),
    AddNote(usize, u8),
    SetNote(usize, u8),
    DeleteNote(usize),
    AddTag(usize, u8),
    AddMark(usize, u8),
    MoveNoteOffDeletedBook(usize, usize),
    MoveMarkOffDeletedNote(usize, usize),
    SetTagNote(usize, u8),
    DeleteTag(usize),
    SetSetting(u8, u8),
    DeleteSetting(u8),
    AddIsbnBook(u8, u8),
    SetIsbn(usize, u8),
    SetIsbn10(usize, u8),
    AddLoan(usize),
    SetLoanBook(usize, usize),
    DeleteLoan(usize),
    Sync,
}

fn arb_op() -> impl Strategy<Value = Op> {
    let n = 0usize..8;
    let v = 0u8..4;
    prop_oneof![
        v.clone().prop_map(Op::AddAuthor),
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::RenameAuthor(a, b)),
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::SetBorn(a, b)),
        (v.clone(), n.clone()).prop_map(|(a, b)| Op::AddBook(a, b)),
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::SetTitle(a, b)),
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::SetYear(a, b)),
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::SetPages(a, b)),
        (n.clone(), n.clone()).prop_map(|(a, b)| Op::SetAuthor(a, b)),
        n.clone().prop_map(Op::DeleteBook),
        n.clone().prop_map(Op::DeleteAuthor),
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::AddNote(a, b)),
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::SetNote(a, b)),
        n.clone().prop_map(Op::DeleteNote),
        (n.clone(), 0u8..2).prop_map(|(a, b)| Op::AddTag(a, b)),
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::AddMark(a, b)),
        (n.clone(), n.clone()).prop_map(|(a, b)| Op::MoveNoteOffDeletedBook(a, b)),
        (n.clone(), n.clone()).prop_map(|(a, b)| Op::MoveMarkOffDeletedNote(a, b)),
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::AddMark(a, b)),
        (n.clone(), n.clone()).prop_map(|(a, b)| Op::MoveNoteOffDeletedBook(a, b)),
        (n.clone(), n.clone()).prop_map(|(a, b)| Op::MoveMarkOffDeletedNote(a, b)),
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::AddMark(a, b)),
        (n.clone(), n.clone()).prop_map(|(a, b)| Op::MoveNoteOffDeletedBook(a, b)),
        (n.clone(), n.clone()).prop_map(|(a, b)| Op::MoveMarkOffDeletedNote(a, b)),
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::SetTagNote(a, b)),
        n.clone().prop_map(Op::DeleteTag),
        (0u8..2, v.clone()).prop_map(|(a, b)| Op::SetSetting(a, b)),
        (0u8..2).prop_map(Op::DeleteSetting),
        (v.clone(), 0u8..3).prop_map(|(a, b)| Op::AddIsbnBook(a, b)),
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::SetIsbn(a, b)),
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::SetIsbn10(a, b)),
        n.clone().prop_map(Op::AddLoan),
        (n.clone(), n.clone()).prop_map(|(a, b)| Op::SetLoanBook(a, b)),
        n.clone().prop_map(Op::DeleteLoan),
        Just(Op::Sync),
        Just(Op::Sync),
    ]
}

async fn perform(peer: &mut Peer, op: Op, hub: &mut Vec<Change>) {
    let sql = match op {
        Op::AddMark(n, v) => match peer.pick("notes", n).await {
            Some(id) => {
                format!("INSERT OR IGNORE INTO marks (note_id, place) VALUES ({id}, {v})")
            }
            None => return,
        },
        Op::MoveNoteOffDeletedBook(n, m) | Op::MoveMarkOffDeletedNote(n, m) => {
            let (child, parent, column) = match op {
                Op::MoveNoteOffDeletedBook(..) => ("notes", "books", "book_id"),
                _ => ("marks", "notes", "note_id"),
            };
            let (Some(id), Some(to)) = (peer.pick(child, n).await, peer.pick(parent, m).await)
            else {
                return;
            };
            let from: Option<i64> =
                sqlx::query_scalar(&format!("SELECT {column} FROM {child} WHERE id = {id}"))
                    .fetch_one(&mut peer.conn)
                    .await
                    .unwrap();
            let Some(from) = from.filter(|from| *from != to) else {
                return;
            };
            peer.run(&format!(
                "UPDATE OR IGNORE {child} SET {column} = {to} WHERE id = {id}"
            ))
            .await;
            format!("DELETE FROM {parent} WHERE id = {from}")
        }
        Op::Sync => {
            peer.sync(hub).await;
            return;
        }
        Op::AddAuthor(v) => format!("INSERT OR IGNORE INTO authors (name) VALUES ('author{v}')"),
        Op::RenameAuthor(n, v) | Op::SetBorn(n, v) => {
            let Some(id) = peer.pick("authors", n).await else {
                return;
            };
            let set = match op {
                Op::RenameAuthor(..) => format!("name = 'author{v}'"),
                _ => format!("born = {}", v % 2),
            };
            format!("UPDATE OR IGNORE authors SET {set} WHERE id = {id}")
        }
        Op::AddBook(v, n) => match peer.pick("authors", n).await {
            Some(a) => format!("INSERT INTO books (title, author_id) VALUES ('title{v}', {a})"),
            None => format!("INSERT INTO books (title) VALUES ('title{v}')"),
        },
        Op::SetAuthor(n, m) => {
            let (Some(b), Some(a)) = (peer.pick("books", n).await, peer.pick("authors", m).await)
            else {
                return;
            };
            format!("UPDATE books SET author_id = {a} WHERE id = {b}")
        }
        Op::SetSetting(k, v) => format!(
            "INSERT INTO settings VALUES ('key{k}', 'value{v}')
             ON CONFLICT (key) DO UPDATE SET value = excluded.value"
        ),
        Op::DeleteSetting(k) => format!("DELETE FROM settings WHERE key = 'key{k}'"),
        // Values 0 to 2 clash with other books holding the same; 3 clears the identity.
        Op::AddIsbnBook(v, i) => {
            format!("INSERT OR IGNORE INTO books (title, isbn) VALUES ('title{v}', 'isbn{i}')")
        }
        Op::SetIsbn(n, v) | Op::SetIsbn10(n, v) => {
            let Some(id) = peer.pick("books", n).await else {
                return;
            };
            let column = if matches!(op, Op::SetIsbn(..)) {
                "isbn"
            } else {
                "isbn10"
            };
            let value = if v == 3 {
                "NULL".to_string()
            } else {
                format!("'{column}{v}'")
            };
            format!("UPDATE OR IGNORE books SET {column} = {value} WHERE id = {id}")
        }
        Op::AddLoan(n) => match peer.pick("books", n).await {
            Some(b) => format!("INSERT INTO loans (book_id, borrower) VALUES ({b}, 'friend')"),
            None => "INSERT INTO loans (borrower) VALUES ('friend')".to_string(),
        },
        Op::SetLoanBook(n, m) => {
            let (Some(l), Some(b)) = (peer.pick("loans", n).await, peer.pick("books", m).await)
            else {
                return;
            };
            format!("UPDATE loans SET book_id = {b} WHERE id = {l}")
        }
        Op::DeleteLoan(n) => match peer.pick("loans", n).await {
            Some(id) => format!("DELETE FROM loans WHERE id = {id}"),
            None => return,
        },
        Op::DeleteAuthor(n) => match peer.pick("authors", n).await {
            Some(id) => format!("DELETE FROM authors WHERE id = {id}"),
            None => return,
        },
        Op::SetNote(n, v) => match peer.pick("notes", n).await {
            Some(id) => format!("UPDATE notes SET body = 'body{v}' WHERE id = {id}"),
            None => return,
        },
        Op::DeleteNote(n) => match peer.pick("notes", n).await {
            Some(id) => format!("DELETE FROM notes WHERE id = {id}"),
            None => return,
        },
        Op::SetTagNote(n, v) => match peer.pick("tags", n).await {
            Some(id) => format!("UPDATE tags SET note = 'note{v}' WHERE rowid = {id}"),
            None => return,
        },
        Op::DeleteTag(n) => match peer.pick("tags", n).await {
            Some(id) => format!("DELETE FROM tags WHERE rowid = {id}"),
            None => return,
        },
        Op::SetTitle(n, _)
        | Op::SetYear(n, _)
        | Op::SetPages(n, _)
        | Op::DeleteBook(n)
        | Op::AddNote(n, _)
        | Op::AddTag(n, _) => {
            let Some(id) = peer.pick("books", n).await else {
                return;
            };
            match op {
                Op::SetTitle(_, v) => {
                    format!("UPDATE books SET title = 'title{v}' WHERE id = {id}")
                }
                Op::SetYear(_, v) => format!("UPDATE books SET year = {v} WHERE id = {id}"),
                Op::SetPages(_, v) => format!("UPDATE books SET pages = {v} WHERE id = {id}"),
                Op::DeleteBook(_) => format!("DELETE FROM books WHERE id = {id}"),
                Op::AddNote(_, v) => {
                    format!("INSERT INTO notes (book_id, body) VALUES ({id}, 'body{v}')")
                }
                _ => {
                    let Op::AddTag(_, v) = op else { return };
                    format!("INSERT OR IGNORE INTO tags (book_id, tag) VALUES ({id}, 'tag{v}')")
                }
            }
        }
    };
    peer.run(&sql).await;
}

proptest! {
    #[test]
    fn replicas_that_have_exchanged_everything_are_identical(
        script in proptest::collection::vec((0usize..3, arb_op()), 0..60)
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(async {
            let mut hub = Vec::new();
            let mut peers = [Peer::new().await, Peer::new().await, Peer::new().await];
            for (who, op) in script {
                perform(&mut peers[who], op, &mut hub).await;
            }
            settle(&mut peers, &mut hub).await;
            assert_converged(&mut peers).await;
        });
    }
}

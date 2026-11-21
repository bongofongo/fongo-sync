//! What replicas show, case by case. Every apply and collect is checked against the reference
//! model, and every settled replica must show its view.

mod support;

use std::str::FromStr;

use fongo_sync_sqlite::core::{Change, Gid, Strategy as Merge, Value};
use fongo_sync_sqlite::{
    Applied, Collection, Conflict, Held, LocalDefault, Registry, Replica, Resume,
};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};

const SCHEMA: &[&str] = &[
    "CREATE TABLE authors (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        born INTEGER NOT NULL DEFAULT 0,
        favourite INTEGER REFERENCES books(id) ON DELETE SET NULL,
        UNIQUE (name, born))",
    "CREATE TABLE books (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        year INTEGER,
        pages INTEGER,
        author_id INTEGER REFERENCES authors(id) ON DELETE SET NULL,
        isbn TEXT UNIQUE,
        isbn10 TEXT UNIQUE,
        current INTEGER,
        status TEXT,
        finished INTEGER)",
    "CREATE UNIQUE INDEX one_current_book ON books (current) WHERE current = 1",
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
    "CREATE TABLE settings (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE positions (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        page INTEGER)",
    "CREATE TABLE shelf (book_id INTEGER NOT NULL REFERENCES books(id))",
    "CREATE TABLE covers (
        book_id INTEGER NOT NULL UNIQUE REFERENCES books(id) ON DELETE CASCADE,
        path TEXT)",
    "CREATE TABLE loans (
        id INTEGER PRIMARY KEY,
        book_id INTEGER REFERENCES books(id) ON DELETE SET NULL,
        borrower TEXT,
        lent_seq INTEGER NOT NULL,
        origin TEXT NOT NULL)",
];

fn registry() -> Registry {
    Registry::new(vec![
        Collection::minted("authors", "authors", "id")
            .field("name")
            .field("born")
            .reference("favourite", "books")
            .identity(&["name", "born"]),
        Collection::minted("books", "books", "id")
            .field("title")
            .field("year")
            .field_with("pages", Merge::Max)
            .reference("author_id", "authors")
            .field("isbn")
            .field("isbn10")
            .field("current")
            .field("status")
            .field("finished")
            .together(&["status", "finished"])
            .identity(&["isbn"])
            .identity(&["isbn10"])
            .local_dependent("positions", "book_id")
            .local_dependent("covers", "book_id"),
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
            .field("borrower")
            .local_default(
                "lent_seq",
                LocalDefault::Sql("SELECT COALESCE(max(lent_seq), 0) + 1 FROM loans".into()),
            )
            .local_default(
                "origin",
                LocalDefault::Value(Value::Text("elsewhere".into())),
            ),
    ])
    .unwrap()
}

const DUMP: &str = "
    SELECT 'author ' || COALESCE(hex(i.gid), 'UNMAPPED') || ' ' || a.name || ' ' || a.born
           || ' f=' || COALESCE((SELECT hex(gid) FROM fongo_ids
                                 WHERE collection = 'books' AND key = json_array(a.favourite)), '-')
      FROM authors a LEFT JOIN fongo_ids i ON i.collection = 'authors' AND i.key = json_array(a.id)
    UNION ALL
    SELECT 'book ' || COALESCE(hex(i.gid), 'UNMAPPED')
           || ' i=' || COALESCE(b.isbn, '-') || ' j=' || COALESCE(b.isbn10, '-')
           || ' s=' || COALESCE(b.status, '-') || ' f=' || COALESCE(b.finished, '-') || ' ' || b.title
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

struct Peer {
    conn: SqliteConnection,
    replica: Replica,
    cursor: usize,
    /// Where this peer's last sent changes end in the hub, and their last outbox sequence.
    sent: Option<(usize, i64)>,
    /// Every change applied here or sent from here.
    taken: Vec<Change>,
    writes: support::writes::Writes,
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
            sent: None,
            taken: Vec::new(),
            writes: Default::default(),
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
        let waiting = self
            .writes
            .installing(&mut self.conn, &self.replica, &self.taken)
            .await;
        self.replica.install(&mut self.conn).await.unwrap();
        self.writes
            .installed(&mut self.conn, &self.replica, waiting)
            .await;
        self.midway().await;
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

    async fn apply(&mut self, changes: &[Change]) -> Applied {
        self.taken.extend(changes.iter().cloned());
        self.writes
            .before(&mut self.conn, &self.replica, &self.taken)
            .await;
        let applied = self.replica.apply(&mut self.conn, changes).await.unwrap();
        self.writes.touched(&applied.touched);
        self.writes.after(&mut self.conn, &self.replica).await;
        self.midway().await;
        applied
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

    /// Sends local changes to the hub, then takes everything the hub has that this peer has not
    /// seen, its own changes included.
    async fn sync(&mut self, hub: &mut Vec<Change>) -> Applied {
        self.push(hub).await;
        self.pull(hub).await
    }

    /// Once this peer has read the hub past what it sent, edits it sent are known to everyone
    /// who sends after them.
    async fn release(&mut self, read_through: impl Fn(usize) -> bool) {
        if let Some((end, through)) = self.sent
            && read_through(end)
        {
            self.replica
                .release_bases(&mut self.conn, through)
                .await
                .unwrap();
            self.sent = None;
        }
    }

    /// Takes what the hub has without sending first, so local edits are still unsent when the
    /// incoming changes are applied.
    async fn pull(&mut self, hub: &[Change]) -> Applied {
        let applied = self.apply(&hub[self.cursor..]).await;
        self.cursor = hub.len();
        let cursor = self.cursor;
        self.release(|end| end <= cursor).await;
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

    /// Each member of a group of more than one entity, and the group's canonical gid.
    async fn aliases(&mut self) -> Vec<String> {
        let inspection = self.replica.inspect(&mut self.conn).await.unwrap();
        let mut lines: Vec<String> = inspection
            .aliases
            .iter()
            .map(|(_, g, c)| format!("{} {}", g.to_hex(), c.to_hex()).to_uppercase())
            .collect();
        lines.sort();
        lines
    }

    /// How many entities wait.
    async fn waiting(&mut self) -> usize {
        self.replica
            .inspect(&mut self.conn)
            .await
            .unwrap()
            .held
            .len()
    }
}

impl Peer {
    async fn push(&mut self, hub: &mut Vec<Change>) {
        self.writes
            .before(&mut self.conn, &self.replica, &self.taken)
            .await;
        self.replica.collect(&mut self.conn).await.unwrap();
        self.writes.after(&mut self.conn, &self.replica).await;
        let outbox = self
            .replica
            .outbox(&mut self.conn, 1_000_000)
            .await
            .unwrap();
        let unwritten = self
            .writes
            .unwritten(&mut self.conn, &self.replica, &self.taken, &outbox.changes)
            .await;
        assert!(unwritten.is_empty(), "{}", unwritten.join("\n"));
        if !outbox.changes.is_empty() {
            self.taken.extend(outbox.changes.iter().cloned());
            hub.extend(outbox.changes);
            self.sent = Some((hub.len(), outbox.through));
        }
        self.replica
            .acknowledge(&mut self.conn, outbox.through)
            .await
            .unwrap();
        self.midway().await;
    }
}

/// Syncs every peer in turn until a whole round sends nothing new, so each has seen everything.
async fn settle(peers: &mut [Peer], hub: &mut Vec<Change>) {
    for _ in 0..20 {
        let before = hub.len();
        for peer in peers.iter_mut() {
            peer.sync(hub).await;
        }
        let mut quiet = hub.len() == before && peers.iter().all(|p| p.cursor == hub.len());
        // A change made by the last apply of a round is not sent until the next.
        for peer in peers.iter_mut() {
            peer.replica.collect(&mut peer.conn).await.unwrap();
            let left = peer.replica.outbox(&mut peer.conn, 1).await.unwrap();
            quiet &= left.changes.is_empty();
        }
        if quiet {
            return;
        }
    }
    panic!("the peers never stopped sending");
}

async fn assert_converged(peers: &mut [Peer]) -> Vec<String> {
    for peer in peers.iter_mut() {
        peer.settled().await;
    }
    let everything: Vec<Change> = peers.iter().flat_map(|p| p.taken.iter().cloned()).collect();
    for (i, peer) in peers.iter().enumerate() {
        if let Err(e) = peer.writes.kept(peer.replica.registry(), &everything) {
            panic!("peer {i}: {e}");
        }
    }
    let first = peers[0].dump().await;
    let shadow = peers[0].lines(SHADOW).await;
    assert!(
        !first.iter().any(|line| line.contains("UNMAPPED")),
        "a row has no global id: {first:#?}"
    );
    let aliases = peers[0].aliases().await;
    for peer in peers.iter_mut().skip(1) {
        assert_eq!(peer.dump().await, first);
        assert_eq!(peer.lines(SHADOW).await, shadow);
        assert_eq!(peer.aliases().await, aliases);
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
    a.run(
        "INSERT INTO loans (book_id, borrower, lent_seq, origin) VALUES (1, 'Dorothea', 0, 'here')",
    )
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
    b.run("INSERT INTO loans (book_id, borrower, lent_seq, origin) VALUES (1, 'Celia', 0, 'here')")
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
    let aliases = peers[0].aliases().await;
    assert_eq!(aliases.len(), 4, "{aliases:#?}");
    assert_eq!(
        aliases.iter().filter(|l| l.ends_with(book)).count(),
        2,
        "{aliases:#?}"
    );
    for peer in peers.iter_mut() {
        assert_eq!(peer.waiting().await, 0);
        assert_eq!(count(peer, "SELECT count(*) FROM books").await, 1);
    }
    // The group is known by the smallest of its ids, on every replica.
    assert_eq!(book, made.iter().min().unwrap().to_hex().to_uppercase());
    // Either id finds the row, and the row names the group.
    let replica = peers[0].replica.clone();
    let conn = &mut peers[0].conn;
    let key = replica.key_for_gid(conn, "books", made[1]).await.unwrap();
    let key = key.unwrap();
    assert_eq!(
        replica.key_for_gid(conn, "books", made[0]).await.unwrap(),
        Some(key.clone())
    );
    assert_eq!(
        replica.gid_for_key(conn, "books", &key).await.unwrap(),
        made.iter().min().copied()
    );

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
        peer.apply(&hub).await;
        peer.cursor = hub.len();
        assert_eq!(peer.aliases().await.len(), 2);
        assert_eq!(count(peer, "SELECT count(*) FROM books").await, 1);
    }
    assert_ne!(peers[0].aliases().await, peers[2].aliases().await);
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    assert_eq!(peers[0].aliases().await.len(), 3);
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
    assert!(peers[0].aliases().await.is_empty());
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
        .run("INSERT INTO loans (book_id, borrower, lent_seq, origin) SELECT id, 'Celia', 0, 'here' FROM books WHERE isbn IS NULL")
        .await;
    let loan_book: i64 = count(&mut peers[2], "SELECT book_id FROM loans").await;

    // The second replica gives its book the same isbn before it has seen the first's.
    peers[1]
        .run("UPDATE books SET isbn = '9780141439549'")
        .await;
    peers[1].sync(&mut hub).await;
    peers[1].sync(&mut hub).await;
    let applied = peers[2].sync(&mut hub).await;

    let rows = touched(&applied);
    let removed: Vec<_> = rows.iter().filter(|r| r.0 == "books" && r.3).collect();
    assert_eq!(removed.len(), 1, "{rows:#?}");
    let kept: i64 = count(&mut peers[2], "SELECT id FROM books").await;
    assert_ne!(removed[0].1, format!("[{kept}]"), "{rows:#?}");
    assert_eq!(removed[0].2, peers[2].gid("books", kept).await);
    // The loan is reported if it had to be moved off the row given up.
    let loan = peers[2].gid("loans", 1).await;
    assert_eq!(
        rows.contains(&("loans", "[1]", loan, false)),
        removed[0].1 == format!("[{loan_book}]"),
        "{rows:#?}"
    );
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
    a.run("INSERT INTO loans (book_id, borrower, lent_seq, origin) VALUES (1, 'Celia', 0, 'here')")
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
#[ignore = "plan::order writes no swap of NOT NULL unique values; see a_swap_of_not_null_unique_values_can_be_written in the core"]
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
    assert_eq!(peers[0].aliases().await.len(), 2);

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
    assert_eq!(peers[0].aliases().await.len(), 2);
    for peer in peers.iter_mut() {
        assert_eq!(peer.waiting().await, 0);
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
    assert_eq!(peers[0].aliases().await.len(), 2);
}

#[tokio::test]
async fn an_owner_graph_with_many_paths_is_walked_once() {
    // Each level is a collection whose two rows are both owned by both rows of the level above,
    // so a row at the bottom has 2^depth chains of owners up to the top.
    const LEVELS: usize = 30;
    let mut schema = vec!["CREATE TABLE level0 (id INTEGER PRIMARY KEY, label TEXT)".to_string()];
    for level in 1..LEVELS {
        let up = level - 1;
        schema.push(format!(
            "CREATE TABLE level{level} (
                id INTEGER PRIMARY KEY,
                a INTEGER REFERENCES level{up}(id) ON DELETE CASCADE,
                b INTEGER REFERENCES level{up}(id) ON DELETE CASCADE)"
        ));
    }
    let schema: Vec<&str> = schema.iter().map(String::as_str).collect();
    let registry = || {
        let mut collections = vec![Collection::minted("level0", "level0", "id").field("label")];
        for level in 1..LEVELS {
            let (name, up) = (format!("level{level}"), format!("level{}", level - 1));
            collections.push(
                Collection::minted(&name, &name, "id")
                    .owner("a", &up)
                    .owner("b", &up),
            );
        }
        Registry::new(collections).unwrap()
    };
    let mut hub = Vec::new();
    let mut peers = [
        Peer::with(&schema, registry()).await,
        Peer::with(&schema, registry()).await,
    ];
    peers[0]
        .run("INSERT INTO level0 (id, label) VALUES (1, 'a'), (2, 'b')")
        .await;
    for level in 1..LEVELS {
        peers[0]
            .run(&format!(
                "INSERT INTO level{level} (id, a, b) VALUES (1, 1, 2), (2, 1, 2)"
            ))
            .await;
    }
    let started = std::time::Instant::now();
    settle(&mut peers, &mut hub).await;
    let bottom = format!("SELECT count(*) FROM level{}", LEVELS - 1);
    assert_eq!(count(&mut peers[1], &bottom).await, 2);

    // Deleting one row at the top takes every row below it, since each is owned by it.
    peers[0].run("DELETE FROM level0 WHERE id = 1").await;
    settle(&mut peers, &mut hub).await;
    for peer in peers.iter_mut() {
        assert_eq!(count(peer, "SELECT count(*) FROM level0").await, 1);
        assert_eq!(count(peer, "SELECT count(*) FROM level1").await, 0);
        assert_eq!(count(peer, &bottom).await, 0);
        assert_eq!(peer.waiting().await, 0);
    }
    let kept = peers[0].gid("level0", 2).await;
    let row = count(&mut peers[1], "SELECT id FROM level0").await;
    assert_eq!(peers[1].gid("level0", row).await, kept);
    assert!(started.elapsed() < std::time::Duration::from_secs(60));
}

#[tokio::test]
async fn a_reference_held_while_its_group_had_no_row_is_restored_when_the_row_returns() {
    let mut hub = Vec::new();
    let mut peers = [
        Peer::new().await,
        Peer::new().await,
        Peer::new().await,
        Peer::new().await,
    ];
    let take = |peer: &mut Peer, hub: &Vec<Change>| {
        let batch = hub[peer.cursor..].to_vec();
        peer.cursor = hub.len();
        batch
    };
    peers[0]
        .run("INSERT INTO books (title, isbn) VALUES ('a', 'v')")
        .await;
    peers[0].push(&mut hub).await;
    let created = take(&mut peers[0], &hub);
    peers[1].apply(&created).await;
    peers[1]
        .run("INSERT INTO loans (book_id, borrower, lent_seq, origin) VALUES (1, 'Celia', 0, 'here')")
        .await;
    peers[1].cursor = hub.len();
    peers[1].push(&mut hub).await;
    let loan = take(&mut peers[1], &hub);
    later();
    peers[0].run("DELETE FROM books").await;
    peers[0].cursor = hub.len();
    peers[0].push(&mut hub).await;
    let deleted = take(&mut peers[0], &hub);
    later();
    // Made after the delete, so the group the two books form is alive.
    peers[2]
        .run("INSERT INTO books (title, isbn) VALUES ('b', 'v')")
        .await;
    peers[2].cursor = hub.len();
    peers[2].push(&mut hub).await;
    let remade = take(&mut peers[2], &hub);
    // Still holding the first book alive, the second replica meets the new one and merges them.
    peers[1].apply(&remade).await;
    peers[1].cursor = hub.len();
    peers[1].push(&mut hub).await;
    let alias = take(&mut peers[1], &hub);

    for batch in [&created, &loan, &deleted, &alias, &remade] {
        peers[3].apply(batch).await;
    }
    for peer in peers.iter_mut() {
        peer.cursor = 0;
    }
    settle(&mut peers, &mut hub).await;
    let state = assert_converged(&mut peers).await;
    let book = state.iter().find(|l| l.starts_with("book ")).unwrap();
    let loan = state.iter().find(|l| l.starts_with("loan ")).unwrap();
    assert!(
        loan.ends_with(&format!(" b={}", book.split(' ').nth(1).unwrap())),
        "{state:#?}"
    );
}

#[tokio::test]
async fn a_deleted_member_lends_nothing_to_its_group() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title, isbn, year) VALUES ('old', 'v', 1871)")
        .await;
    settle(&mut peers, &mut hub).await;
    peers[0].run("DELETE FROM books").await;
    peers[0].push(&mut hub).await;
    let deleted = hub[peers[2].cursor..].to_vec();
    peers[1].pull(&hub).await;
    later();
    peers[1]
        .run("INSERT INTO books (title, isbn) VALUES ('new', 'v')")
        .await;
    let from = hub.len();
    peers[1].push(&mut hub).await;
    // The third replica meets the new book before the delete, so merges the two.
    let made = hub[from..].to_vec();
    peers[2].apply(&made).await;
    peers[2].apply(&deleted).await;
    peers[2].cursor = hub.len();
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    let books: Vec<&String> = state.iter().filter(|l| l.starts_with("book ")).collect();
    assert_eq!(books.len(), 1, "{state:#?}");
    assert!(books[0].contains(" new y=- "), "{state:#?}");
}

#[tokio::test]
async fn deleting_one_member_keeps_another_made_without_seeing_it() {
    for third_saw_both in [false, true] {
        let mut hub = Vec::new();
        let mut peers = [Peer::new().await, Peer::new().await, Peer::new().await];
        peers[0]
            .run("INSERT INTO books (title, isbn) VALUES ('a', 'v')")
            .await;
        peers[1]
            .run("INSERT INTO books (title, isbn) VALUES ('b', 'v')")
            .await;
        peers[1]
            .run("INSERT INTO notes (book_id, body) VALUES (1, 'kept')")
            .await;
        peers[0].push(&mut hub).await;
        peers[1].push(&mut hub).await;
        if third_saw_both {
            peers[2].pull(&hub).await;
        }
        later();
        peers[0].run("DELETE FROM books").await;
        peers[0].push(&mut hub).await;
        settle(&mut peers, &mut hub).await;

        let state = assert_converged(&mut peers).await;
        assert_eq!(
            state.iter().filter(|l| l.starts_with("book ")).count(),
            1,
            "{state:#?}"
        );
        assert!(
            state
                .iter()
                .any(|l| l.starts_with("note ") && l.contains(" kept b=")),
            "{state:#?}"
        );
    }
}

#[tokio::test]
#[ignore = "plan::order writes no swap of NOT NULL unique values; see a_swap_of_not_null_unique_values_can_be_written in the core"]
async fn a_swap_of_not_null_unique_identities_converges_and_one_that_cannot_waits() {
    for check in ["", " CHECK (length(isbn) = 1)"] {
        let schema = [format!(
            "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, isbn TEXT NOT NULL UNIQUE{check})"
        )];
        let schema: Vec<&str> = schema.iter().map(String::as_str).collect();
        let registry = || {
            Registry::new(vec![
                Collection::minted("books", "books", "id")
                    .field("title")
                    .field("isbn")
                    .identity(&["isbn"]),
            ])
            .unwrap()
        };
        let mut hub = Vec::new();
        let mut peers = [
            Peer::with(&schema, registry()).await,
            Peer::with(&schema, registry()).await,
        ];
        peers[0]
            .run("INSERT INTO books (title, isbn) VALUES ('one', 'x'), ('two', 'y')")
            .await;
        settle(&mut peers, &mut hub).await;
        peers[0]
            .run("UPDATE books SET isbn = 'z' WHERE id = 1")
            .await;
        peers[0]
            .run("UPDATE books SET isbn = 'x' WHERE id = 2")
            .await;
        peers[0]
            .run("UPDATE books SET isbn = 'y' WHERE id = 1")
            .await;
        peers[0].push(&mut hub).await;
        let mut batch = hub[peers[1].cursor..].to_vec();
        batch.reverse();
        let applied = peers[1].apply(&batch).await;
        peers[1].cursor = hub.len();
        let rows = peers[1]
            .lines("SELECT title || ' ' || isbn FROM books ORDER BY 1")
            .await;
        if check.is_empty() {
            assert_eq!(applied.waiting, 0);
            assert_eq!(rows, ["one y", "two x"]);
            assert!(peers[1].aliases().await.is_empty());
        } else {
            // No stand-in passes the check, so the rows wait rather than spin.
            assert_eq!(applied.waiting, 2);
            assert_eq!(rows, ["one x", "two y"]);
        }
    }
}

#[tokio::test]
async fn a_local_id_given_to_another_entity_in_the_same_apply_reports_both() {
    let schema = ["CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT UNIQUE)"];
    let registry = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id").field("title"),
        ])
        .unwrap()
    };
    let mut hub = Vec::new();
    let mut peers = [
        Peer::with(&schema, registry()).await,
        Peer::with(&schema, registry()).await,
    ];
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    settle(&mut peers, &mut hub).await;
    let old = peers[0].gid("books", 1).await;
    // The new row cannot be written until the old one is gone, then takes its local id.
    peers[0].run("DELETE FROM books").await;
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[0].push(&mut hub).await;
    let new = peers[0].gid("books", 1).await;
    let applied = peers[1].pull(&hub).await;
    assert_eq!(
        touched(&applied),
        vec![("books", "[1]", old, true), ("books", "[1]", new, false)]
    );
}

#[tokio::test]
async fn a_row_left_waiting_is_not_reported_again_until_it_changes() {
    let schema = ["CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT UNIQUE, year INTEGER)"];
    let registry = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id")
                .field("title")
                .field("year"),
        ])
        .unwrap()
    };
    let mut hub = Vec::new();
    let mut peers = [
        Peer::with(&schema, registry()).await,
        Peer::with(&schema, registry()).await,
    ];
    peers[0]
        .run("INSERT INTO books (title, year) VALUES ('Middlemarch', 1871)")
        .await;
    peers[1]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[0].push(&mut hub).await;
    let applied = peers[1].sync(&mut hub).await;
    assert_eq!(applied.waiting, 1);
    let again = peers[1].apply(&[]).await;
    assert_eq!(again.waiting, 1);
    assert!(again.touched.is_empty(), "{:#?}", again.touched);
}

#[tokio::test]
async fn a_conflict_is_found_in_a_later_page_reported_once_and_not_after_release() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peers[0]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'base')")
        .await;
    settle(&mut peers, &mut hub).await;

    peers[0]
        .run("INSERT INTO settings VALUES ('theme', 'dark')")
        .await;
    peers[0].run("UPDATE notes SET body = 'one'").await;
    later();
    peers[1].run("UPDATE notes SET body = 'two'").await;
    peers[0].push(&mut hub).await;
    let (first, second): (Vec<Change>, Vec<Change>) = hub[peers[1].cursor..]
        .iter()
        .cloned()
        .partition(|c| c.collection == "settings");
    peers[1].cursor = hub.len();
    peers[1].taken.extend(first.iter().chain(&second).cloned());
    let replica = peers[1].replica.clone();
    let conn = &mut peers[1].conn;
    assert!(
        replica
            .apply(conn, &first)
            .await
            .unwrap()
            .conflicts
            .is_empty()
    );
    let applied = replica.apply(conn, &second).await.unwrap();
    assert_eq!(applied.conflicts.len(), 1, "{:#?}", applied.conflicts);
    assert_eq!(applied.conflicts[0].local, Value::Text("two".into()));
    assert_eq!(applied.conflicts[0].remote, Value::Text("one".into()));
    assert!(
        replica
            .apply(conn, &second)
            .await
            .unwrap()
            .conflicts
            .is_empty()
    );

    // Released, the edit no longer counts, however late another arrives.
    replica.release_bases(conn, i64::MAX).await.unwrap();
    peers[0].run("UPDATE notes SET body = 'three'").await;
    let from = hub.len();
    peers[0].push(&mut hub).await;
    let late = hub[from..].to_vec();
    peers[1].taken.extend(late.iter().cloned());
    let applied = replica.apply(&mut peers[1].conn, &late).await.unwrap();
    assert!(applied.conflicts.is_empty(), "{:#?}", applied.conflicts);
    peers[1].midway().await;
}

#[tokio::test]
async fn installing_again_changes_nothing() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title, isbn) VALUES ('a', 'v')")
        .await;
    peers[1]
        .run("INSERT INTO books (title, isbn) VALUES ('b', 'v')")
        .await;
    settle(&mut peers, &mut hub).await;
    let before = peers[1].dump().await;
    peers[1].install().await;
    let produced = peers[1].replica.collect(&mut peers[1].conn).await.unwrap();
    assert_eq!(produced, 0);
    assert_eq!(peers[1].dump().await, before);
}

#[tokio::test]
async fn two_local_rows_with_one_identity_become_one_without_a_unique_constraint() {
    let schema = [
        "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, isbn TEXT)",
        "CREATE TABLE notes (
            id INTEGER PRIMARY KEY,
            book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
            body TEXT)",
    ];
    let registry = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id")
                .field("title")
                .field("isbn")
                .identity(&["isbn"]),
            Collection::minted("notes", "notes", "id")
                .owner("book_id", "books")
                .field("body"),
        ])
        .unwrap()
    };
    let mut hub = Vec::new();
    let mut peers = [
        Peer::with(&schema, registry()).await,
        Peer::with(&schema, registry()).await,
    ];
    peers[0]
        .run("INSERT INTO books (title, isbn) VALUES ('a', 'v'), ('b', 'v')")
        .await;
    peers[0]
        .run("INSERT INTO notes (book_id, body) VALUES (1, 'one'), (2, 'two')")
        .await;
    settle(&mut peers, &mut hub).await;
    let rows = "SELECT n.body || ' ' || b.isbn FROM notes n JOIN books b ON b.id = n.book_id
                ORDER BY 1";
    for peer in peers.iter_mut() {
        assert_eq!(count(peer, "SELECT count(*) FROM books").await, 1);
        assert_eq!(peer.lines(rows).await, ["one v", "two v"]);
    }
    assert_eq!(peers[0].lines(SHADOW).await, peers[1].lines(SHADOW).await);
}

#[tokio::test]
async fn rows_outside_the_registry_move_to_the_row_a_merge_keeps() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await, Peer::new().await];
    peers[1].run("INSERT INTO books (title) VALUES ('b')").await;
    peers[1].sync(&mut hub).await;
    peers[0]
        .run("INSERT INTO books (title, isbn) VALUES ('a', '9780141439549')")
        .await;
    peers[0].sync(&mut hub).await;
    // The third replica holds the two books as two rows, each with local rows of its own.
    peers[2].sync(&mut hub).await;
    peers[2]
        .run("INSERT INTO positions (book_id, page) SELECT id, id * 10 FROM books")
        .await;
    peers[2]
        .run("INSERT INTO covers (book_id, path) SELECT id, 'cover' || id FROM books")
        .await;
    peers[1]
        .run("UPDATE books SET isbn = '9780141439549'")
        .await;
    peers[1].sync(&mut hub).await;
    peers[1].sync(&mut hub).await;
    peers[2].sync(&mut hub).await;

    let third = &mut peers[2];
    let kept: i64 = count(third, "SELECT id FROM books").await;
    assert_eq!(count(third, "SELECT count(*) FROM books").await, 1);
    assert_eq!(
        third
            .lines("SELECT book_id || ' ' || page FROM positions ORDER BY page")
            .await,
        [format!("{kept} 10"), format!("{kept} 20")]
    );
    // Two covers cannot share a book: the kept row's own stays, the other went with its row.
    assert_eq!(
        third
            .lines("SELECT book_id || ' ' || path FROM covers")
            .await,
        [format!("{kept} cover{kept}")]
    );
}

#[tokio::test]
async fn a_delete_another_table_refuses_waits_until_it_is_let_go() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    settle(&mut peers, &mut hub).await;
    peers[1].run("INSERT INTO shelf (book_id) VALUES (1)").await;
    peers[0].run("DELETE FROM books").await;
    peers[0].push(&mut hub).await;

    let applied = peers[1].pull(&hub).await;
    assert_eq!(applied.waiting, 1);
    assert_eq!(count(&mut peers[1], "SELECT count(*) FROM books").await, 1);
    peers[1].run("DELETE FROM shelf").await;
    let applied = peers[1].apply(&[]).await;
    assert_eq!(applied.waiting, 0);
    settle(&mut peers, &mut hub).await;
    assert!(assert_converged(&mut peers).await.is_empty());
}

#[tokio::test]
async fn a_column_kept_by_each_replica_is_filled_when_the_kernel_inserts() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[1]
        .run("INSERT INTO loans (borrower, lent_seq, origin) VALUES ('Mary', 5, 'here')")
        .await;
    peers[0]
        .run("INSERT INTO loans (borrower, lent_seq, origin) VALUES ('Celia', 1, 'here')")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_converged(&mut peers).await;
    assert_eq!(
        peers[1]
            .lines("SELECT borrower || ' ' || lent_seq || ' ' || origin FROM loans ORDER BY 1")
            .await,
        ["Celia 6 elsewhere", "Mary 5 here"]
    );
}

#[tokio::test]
async fn the_application_resolves_what_the_kernel_reports_waiting() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title, current) VALUES ('Middlemarch', 1)")
        .await;
    peers[1]
        .run("INSERT INTO books (title, current) VALUES ('Romola', 1)")
        .await;
    peers[0].push(&mut hub).await;
    let theirs = peers[0].gid("books", 1).await;

    // Each replica has a current book, and the index allows one.
    let applied = peers[1].pull(&hub).await;
    assert_eq!(
        applied.held,
        [Held {
            collection: "books".into(),
            gid: theirs,
            key: None,
        }]
    );
    let replica = peers[1].replica.clone();
    let conn = &mut peers[1].conn;
    let merged = replica.merged(conn, "books", theirs).await.unwrap();
    assert_eq!(merged["current"], Value::Int(1));
    assert_eq!(merged["title"], Value::Text("Middlemarch".into()));
    let ours = replica.gid_of(conn, "books", 1).await.unwrap().unwrap();
    assert_eq!(
        replica.gid_for_key(conn, "books", "[1]").await.unwrap(),
        Some(ours)
    );
    assert_eq!(
        replica
            .key_for_gid(conn, "books", ours)
            .await
            .unwrap()
            .as_deref(),
        Some("[1]")
    );

    peers[1].run("UPDATE books SET current = 0").await;
    let applied = peers[1].apply(&[]).await;
    let replica = peers[1].replica.clone();
    let conn = &mut peers[1].conn;
    assert_eq!(applied.waiting, 0);
    assert!(applied.held.is_empty());
    assert_eq!(
        replica
            .key_for_gid(conn, "books", theirs)
            .await
            .unwrap()
            .as_deref(),
        Some("[2]")
    );
    settle(&mut peers, &mut hub).await;
    assert_converged(&mut peers).await;
    for peer in peers.iter_mut() {
        assert_eq!(peer.waiting().await, 0);
        assert_eq!(
            count(peer, "SELECT count(*) FROM books WHERE current = 1").await,
            1
        );
    }
}

#[tokio::test]
async fn two_tables_that_refer_to_each_other_arrive_together() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO authors (name) VALUES ('Eliot')")
        .await;
    peers[0]
        .run("INSERT INTO books (title, author_id) VALUES ('Middlemarch', 1)")
        .await;
    peers[0].run("UPDATE authors SET favourite = 1").await;
    settle(&mut peers, &mut hub).await;

    let state = assert_converged(&mut peers).await;
    let book = peers[0].gid("books", 1).await.to_hex().to_uppercase();
    let author = peers[0].gid("authors", 1).await.to_hex().to_uppercase();
    assert!(
        state.contains(&format!("author {author} Eliot 0 f={book}")),
        "{state:#?}"
    );
    assert!(
        state.iter().any(|l| l.ends_with(&format!(" a={author}"))),
        "{state:#?}"
    );
    for peer in peers.iter_mut() {
        assert_eq!(peer.waiting().await, 0);
    }
}

#[tokio::test]
async fn rows_changed_while_suspended_are_sent_or_kept_as_asked() {
    for how in [Resume::Send, Resume::Baseline] {
        let mut hub = Vec::new();
        let mut peers = [Peer::new().await, Peer::new().await];
        peers[0]
            .run("INSERT INTO books (title) VALUES ('Middlemarch')")
            .await;
        settle(&mut peers, &mut hub).await;
        let replica = peers[0].replica.clone();
        replica.suspend(&mut peers[0].conn).await.unwrap();
        peers[0].run("UPDATE books SET title = upper(title)").await;
        replica.resume(&mut peers[0].conn, how).await.unwrap();
        peers[0].midway().await;
        // Resuming collects what changed meanwhile; settling left the outbox empty.
        let sent = replica
            .outbox(&mut peers[0].conn, u32::MAX)
            .await
            .unwrap()
            .changes
            .len();
        assert_eq!(replica.collect(&mut peers[0].conn).await.unwrap(), 0);
        let titles = "SELECT title FROM books";
        match how {
            Resume::Send => {
                assert_eq!(sent, 1);
                settle(&mut peers, &mut hub).await;
                assert_eq!(peers[1].lines(titles).await, ["MIDDLEMARCH"]);
            }
            Resume::Baseline => {
                assert_eq!(sent, 0);
                // Another field's edit sends only that field, and an apply keeps the title.
                peers[0].run("UPDATE books SET year = 1871").await;
                assert_eq!(replica.collect(&mut peers[0].conn).await.unwrap(), 1);
                settle(&mut peers, &mut hub).await;
                assert_eq!(peers[0].lines(titles).await, ["MIDDLEMARCH"]);
                assert_eq!(peers[1].lines(titles).await, ["Middlemarch"]);
                // Until someone writes the field.
                peers[1]
                    .run("UPDATE books SET title = 'Middlemarch!'")
                    .await;
                settle(&mut peers, &mut hub).await;
                assert_eq!(peers[0].lines(titles).await, ["Middlemarch!"]);
            }
        }
    }
}

#[tokio::test]
async fn uninstalling_leaves_only_the_application_tables() {
    let mut peer = Peer::new().await;
    peer.run("INSERT INTO books (title) VALUES ('Middlemarch')")
        .await;
    peer.replica.collect(&mut peer.conn).await.unwrap();
    let replica = peer.replica.clone();
    replica.uninstall(&mut peer.conn).await.unwrap();
    let kernel = "SELECT name FROM sqlite_master WHERE name LIKE 'fongo!_%' ESCAPE '!'";
    assert!(peer.lines(kernel).await.is_empty());
    peer.run("INSERT INTO books (title) VALUES ('Romola')")
        .await;
    assert_eq!(count(&mut peer, "SELECT count(*) FROM books").await, 2);
}

#[tokio::test]
async fn a_field_added_later_takes_what_rows_hold_and_one_removed_is_ignored() {
    let schema =
        ["CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, year INTEGER, pages INTEGER)"];
    let before = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id")
                .field("title")
                .field("pages"),
        ])
        .unwrap()
    };
    let after = || {
        Registry::new(vec![
            Collection::minted("books", "books", "id")
                .field("title")
                .field("year"),
        ])
        .unwrap()
    };
    let mut hub = Vec::new();
    let mut peers = [
        Peer::with(&schema, before()).await,
        Peer::with(&schema, before()).await,
    ];
    peers[0]
        .run("INSERT INTO books (title, year, pages) VALUES ('Middlemarch', 1871, 880)")
        .await;
    settle(&mut peers, &mut hub).await;
    let years = "SELECT COALESCE(year, '-') || ' ' || COALESCE(pages, '-') FROM books";
    assert_eq!(peers[1].lines(years).await, ["- 880"]);

    // The replicas upgrade one after the other, with an exchange between.
    for who in 0..2 {
        peers[who].replica = Replica::new(after());
        peers[who].install().await;
        settle(&mut peers, &mut hub).await;
    }
    assert_eq!(peers[1].lines(years).await, ["1871 880"]);
    assert_eq!(peers[0].lines(SHADOW).await, peers[1].lines(SHADOW).await);

    // A value adopted that way gives way to any real write.
    peers[1]
        .run("UPDATE books SET year = 1872, pages = 1")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_eq!(peers[0].lines(years).await, ["1872 880"]);
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

#[tokio::test]
async fn merged_shows_what_the_model_shows_for_key_identity_and_grouped_fields() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title, isbn) VALUES ('Middlemarch', '978-0141439549')")
        .await;
    peers[0]
        .run("INSERT INTO tags (book_id, tag, note) VALUES (1, 'classic', 'first')")
        .await;
    peers[0]
        .run("INSERT INTO settings (key, value) VALUES ('theme', 'dark')")
        .await;
    peers[0].push(&mut hub).await;
    later();
    peers[1]
        .run("INSERT INTO books (title, isbn) VALUES ('Middlemarch, a study', '978-0141439549')")
        .await;
    peers[1]
        .run("INSERT INTO tags (book_id, tag, note) VALUES (1, 'classic', 'second')")
        .await;
    peers[1]
        .run("INSERT INTO settings (key, value) VALUES ('theme', 'light')")
        .await;
    settle(&mut peers, &mut hub).await;
    assert_converged(&mut peers).await;

    let schema = registry().model_schema();
    let merge = fongo_sync_sqlite::core::model::merge(&schema, &hub);
    let view = fongo_sync_sqlite::core::model::view(&schema, &hub);
    for peer in &mut peers {
        let mut compared = 0;
        for (gid, entity) in &merge.entities {
            let Some(row) = view.row(&entity.collection, entity.canonical) else {
                continue;
            };
            let merged = peer
                .replica
                .merged(&mut peer.conn, &entity.collection, *gid)
                .await
                .unwrap();
            for (field, value) in row {
                let shown = merged.get(field).unwrap_or(&Value::Null);
                // A reference shows as the group its entity joined; the model names the canonical.
                let shown = match (shown, value) {
                    (Value::Ref(a), Value::Ref(b)) if merge.entities[a].canonical == *b => value,
                    _ => shown,
                };
                assert_eq!(
                    shown.total_cmp(value),
                    std::cmp::Ordering::Equal,
                    "{} {gid:?} {field}: merged() shows {shown:?}, the model {value:?}",
                    entity.collection
                );
            }
            compared += 1;
        }
        assert_eq!(compared, 5, "two books, two tags and one setting");
    }
}

#[tokio::test]
async fn a_tag_moved_to_the_book_a_merge_keeps_is_reported_removed_at_its_old_key() {
    let mut hub = Vec::new();
    let mut peers = [Peer::new().await, Peer::new().await];
    peers[0]
        .run("INSERT INTO books (title, isbn) VALUES ('Middlemarch', 'x')")
        .await;
    peers[0].push(&mut hub).await;
    peers[1].pull(&hub).await;
    peers[0].run("UPDATE books SET isbn = 'y'").await;
    peers[0].push(&mut hub).await;
    // Made here and not yet collected: the apply collects them, then merges the two books.
    peers[1]
        .run("INSERT INTO books (title, isbn) VALUES ('Middlemarch, a study', 'y')")
        .await;
    peers[1]
        .run("INSERT INTO tags (book_id, tag) VALUES (2, 'classic')")
        .await;
    let tags = "SELECT json_array(book_id, tag) FROM tags";
    let before = peers[1].lines(tags).await;
    let applied = peers[1].pull(&hub).await;
    let after = peers[1].lines(tags).await;
    assert_eq!(after.len(), 1);
    for key in before.iter().filter(|k| !after.contains(k)) {
        assert!(
            applied
                .touched
                .iter()
                .any(|t| t.collection == "tags" && &t.key == key && t.deleted),
            "{key} left the table unreported: {:?}",
            applied.touched
        );
    }
    for key in &after {
        assert!(
            applied
                .touched
                .iter()
                .any(|t| t.collection == "tags" && &t.key == key && !t.deleted)
                || before.contains(key),
            "{key} came unreported"
        );
    }
}

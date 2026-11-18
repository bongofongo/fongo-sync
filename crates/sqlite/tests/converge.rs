use std::str::FromStr;

use fongo_sync_sqlite::core::{Change, Strategy as Merge};
use fongo_sync_sqlite::{Collection, Registry, Replica};
use proptest::prelude::*;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};

const SCHEMA: &[&str] = &[
    "CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
    "CREATE TABLE books (
        id INTEGER PRIMARY KEY,
        title TEXT NOT NULL,
        year INTEGER,
        pages INTEGER,
        author_id INTEGER REFERENCES authors(id) ON DELETE SET NULL)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER REFERENCES books(id) ON DELETE CASCADE,
        body TEXT)",
    "CREATE TABLE tags (
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        tag TEXT NOT NULL,
        note TEXT,
        PRIMARY KEY (book_id, tag))",
    "CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
];

fn registry() -> Registry {
    Registry::new(vec![
        Collection::minted("authors", "authors", "id").field("name"),
        Collection::minted("books", "books", "id")
            .field("title")
            .field("year")
            .field_with("pages", Merge::Max)
            .reference("author_id", "authors"),
        Collection::minted("notes", "notes", "id")
            .owner("book_id", "books")
            .field("body"),
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

const DUMP: &str = "
    SELECT 'author ' || COALESCE(hex(i.gid), 'UNMAPPED') || ' ' || a.name
      FROM authors a LEFT JOIN fongo_ids i ON i.collection = 'authors' AND i.key = json_array(a.id)
    UNION ALL
    SELECT 'book ' || COALESCE(hex(i.gid), 'UNMAPPED') || ' ' || b.title
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
    SELECT 'tag ' || COALESCE((SELECT hex(gid) FROM fongo_ids
                               WHERE collection = 'books' AND key = json_array(t.book_id)), 'UNMAPPED')
           || ' ' || t.tag || ' ' || COALESCE(t.note, '-')
      FROM tags t
    UNION ALL
    SELECT 'setting ' || key || '=' || value FROM settings
    ORDER BY 1";

const SHADOW: &str = "
    SELECT hex(gid) || ' ' || field || ' ' || quote(value) || ' ' || is_ref || ' ' || clock
      FROM fongo_shadow ORDER BY 1";

struct Peer {
    conn: SqliteConnection,
    replica: Replica,
    cursor: usize,
}

impl Peer {
    async fn bare() -> Peer {
        let mut conn = SqliteConnectOptions::from_str("sqlite::memory:")
            .unwrap()
            .foreign_keys(true)
            .connect()
            .await
            .unwrap();
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }
        Peer {
            conn,
            replica: Replica::new(registry()),
            cursor: 0,
        }
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
    async fn sync(&mut self, hub: &mut Vec<Change>) {
        self.replica.collect(&mut self.conn).await.unwrap();
        let outbox = self.replica.outbox(&mut self.conn, 100_000).await.unwrap();
        hub.extend(outbox.changes);
        self.replica
            .acknowledge(&mut self.conn, outbox.through)
            .await
            .unwrap();
        self.replica
            .apply(&mut self.conn, &hub[self.cursor..])
            .await
            .unwrap();
        self.cursor = hub.len();
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

async fn settle(peers: &mut [Peer], hub: &mut Vec<Change>) {
    for _ in 0..3 {
        for peer in peers.iter_mut() {
            peer.sync(hub).await;
        }
    }
}

async fn assert_converged(peers: &mut [Peer]) -> Vec<String> {
    let first = peers[0].dump().await;
    let shadow = peers[0].lines(SHADOW).await;
    assert!(
        !first.iter().any(|line| line.contains("UNMAPPED")),
        "a row has no global id: {first:#?}"
    );
    for peer in peers.iter_mut().skip(1) {
        assert_eq!(peer.dump().await, first);
        assert_eq!(peer.lines(SHADOW).await, shadow);
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

#[derive(Debug, Clone)]
enum Op {
    AddAuthor(u8),
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
    SetTagNote(usize, u8),
    DeleteTag(usize),
    SetSetting(u8, u8),
    DeleteSetting(u8),
    Sync,
}

fn arb_op() -> impl Strategy<Value = Op> {
    let n = 0usize..8;
    let v = 0u8..4;
    prop_oneof![
        v.clone().prop_map(Op::AddAuthor),
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
        (n.clone(), v.clone()).prop_map(|(a, b)| Op::SetTagNote(a, b)),
        n.clone().prop_map(Op::DeleteTag),
        (0u8..2, v.clone()).prop_map(|(a, b)| Op::SetSetting(a, b)),
        (0u8..2).prop_map(Op::DeleteSetting),
        Just(Op::Sync),
        Just(Op::Sync),
    ]
}

async fn perform(peer: &mut Peer, op: Op, hub: &mut Vec<Change>) {
    let sql = match op {
        Op::Sync => {
            peer.sync(hub).await;
            return;
        }
        Op::AddAuthor(v) => format!("INSERT INTO authors (name) VALUES ('author{v}')"),
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

//! Replicas that use every feature at once, exchange changes in any order, part by part and more
//! than once, and change their registry midway, agree with the reference model after every step
//! and show exactly its view once everything is exchanged.

mod support;

use fongo_sync_sqlite::core::Change;
use fongo_sync_sqlite::{Collection, LocalDefault, Registry, Replica, Resume};
use proptest::prelude::*;
use sqlx::SqliteConnection;
use support::world::World;

const SCHEMA: &[&str] = &[
    "CREATE TABLE shelves (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    "CREATE TABLE authors (
        id INTEGER PRIMARY KEY,
        name TEXT,
        favourite INTEGER REFERENCES books(id) ON DELETE SET NULL)",
    "CREATE TABLE books (
        id INTEGER PRIMARY KEY,
        shelf_id INTEGER NOT NULL REFERENCES shelves(id) ON DELETE CASCADE,
        title TEXT,
        isbn TEXT UNIQUE,
        author_id INTEGER REFERENCES authors(id) ON DELETE SET NULL,
        status TEXT,
        finished INTEGER,
        extra TEXT,
        seen INTEGER NOT NULL)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
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
    "CREATE TABLE covers (
        book_id INTEGER NOT NULL UNIQUE REFERENCES books(id) ON DELETE CASCADE,
        path TEXT)",
];

/// The registry before and after an upgrade that adds a field.
fn registry(upgraded: bool) -> Registry {
    let mut books = Collection::minted("books", "books", "id")
        .owner("shelf_id", "shelves")
        .field("title")
        .field("isbn")
        .reference("author_id", "authors")
        .field("status")
        .field("finished")
        .together(&["status", "finished"])
        .identity(&["isbn"])
        .local_default(
            "seen",
            LocalDefault::Sql("SELECT COALESCE(max(seen), 0) + 1 FROM books".into()),
        )
        .local_dependent("covers", "book_id");
    if upgraded {
        books = books.field("extra");
    }
    Registry::new(vec![
        Collection::minted("shelves", "shelves", "id")
            .field("name")
            .identity(&["name"]),
        Collection::minted("authors", "authors", "id")
            .field("name")
            .reference("favourite", "books"),
        books,
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
    ])
    .unwrap()
}

/// What must hold on every replica once everything is exchanged, of the application's own
/// tables: a book holds a finish date exactly when it is done.
const MIXED: &str = "
    SELECT 'mixed ' || id FROM books
     WHERE (COALESCE(status, '') = 'done') <> (finished IS NOT NULL)";

struct Peer {
    conn: SqliteConnection,
    replica: Replica,
    upgraded: bool,
    seen: Vec<bool>,
    sent: Option<(usize, i64)>,
    /// Every change applied here or sent from here.
    taken: Vec<Change>,
    writes: support::writes::Writes,
}

impl Peer {
    async fn new(world: &World, n: u64) -> Peer {
        let mut conn = world.connect(n).await;
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }
        let replica = Replica::new(registry(false));
        replica.install(&mut conn).await.unwrap();
        Peer {
            conn,
            replica,
            upgraded: false,
            seen: Vec::new(),
            sent: None,
            taken: Vec::new(),
            writes: Default::default(),
        }
    }

    async fn midway(&mut self) -> Result<(), String> {
        support::midway(&mut self.conn, &self.replica, &self.taken).await
    }

    async fn upgrade(&mut self) -> Result<(), String> {
        if !self.upgraded {
            self.upgraded = true;
            self.replica = Replica::new(registry(true));
            let waiting = self.writes.installing(&mut self.conn, &self.replica).await;
            self.replica.install(&mut self.conn).await.unwrap();
            self.writes
                .installed(&mut self.conn, &self.replica, waiting)
                .await;
            self.midway().await?;
        }
        Ok(())
    }

    /// Runs the application's own write, which a constraint may refuse.
    async fn run(&mut self, sql: &str) {
        let _ = sqlx::query(sql).execute(&mut self.conn).await;
    }

    async fn lines(&mut self, sql: &str) -> Vec<String> {
        sqlx::query_scalar(sql)
            .fetch_all(&mut self.conn)
            .await
            .unwrap()
    }

    async fn one(&mut self, sql: &str) -> Option<i64> {
        sqlx::query_scalar(sql)
            .fetch_optional(&mut self.conn)
            .await
            .unwrap()
            .flatten()
    }

    async fn pick(&mut self, table: &str, n: usize) -> Option<i64> {
        let ids: Vec<i64> = sqlx::query_scalar(&format!("SELECT rowid FROM {table} ORDER BY 1"))
            .fetch_all(&mut self.conn)
            .await
            .unwrap();
        (!ids.is_empty()).then(|| ids[n % ids.len()])
    }

    /// The rows and what the replica holds.
    async fn state(&mut self) -> String {
        let kernel = support::read(&mut self.conn, &self.replica).await;
        let inspection = self.replica.inspect(&mut self.conn).await.unwrap();
        format!("{:?}\n{:?}", kernel.view, inspection)
    }

    async fn apply(&mut self, batch: &[Change]) -> Result<(), String> {
        self.taken.extend(batch.iter().cloned());
        self.writes.before(&mut self.conn, &self.replica).await;
        let applied = self.replica.apply(&mut self.conn, batch).await.unwrap();
        self.writes.touched(&applied.touched);
        self.writes.after(&mut self.conn, &self.replica).await;
        self.midway().await
    }

    async fn push(&mut self, hub: &mut Vec<Change>) -> Result<(), String> {
        self.writes.before(&mut self.conn, &self.replica).await;
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
        if !unwritten.is_empty() {
            return Err(unwritten.join("\n"));
        }
        if !outbox.changes.is_empty() {
            self.taken.extend(outbox.changes.iter().cloned());
            hub.extend(outbox.changes);
            self.sent = Some((hub.len(), outbox.through));
        }
        self.replica
            .acknowledge(&mut self.conn, outbox.through)
            .await
            .unwrap();
        self.midway().await
    }

    /// Takes the changes `pick` chooses among those not yet seen, in order or reversed. `twice`
    /// takes the same batch again, which must change nothing, and then a slice of what was
    /// already seen.
    async fn deliver(
        &mut self,
        hub: &[Change],
        pick: impl Fn(usize) -> bool,
        reverse: bool,
        twice: bool,
    ) -> Result<(), String> {
        self.seen.resize(hub.len(), false);
        let mut chosen: Vec<usize> = (0..hub.len())
            .filter(|&i| !self.seen[i] && pick(i))
            .collect();
        if reverse {
            chosen.reverse();
        }
        let batch: Vec<Change> = chosen.iter().map(|&i| hub[i].clone()).collect();
        for i in chosen {
            self.seen[i] = true;
        }
        self.apply(&batch).await?;
        if let Some((end, through)) = self.sent
            && self.seen[..end].iter().all(|s| *s)
        {
            self.replica
                .release_bases(&mut self.conn, through)
                .await
                .unwrap();
            self.sent = None;
        }
        if twice {
            let before = self.state().await;
            self.replica.apply(&mut self.conn, &batch).await.unwrap();
            assert_eq!(
                before,
                self.state().await,
                "a batch applied again changed something"
            );
            let start = hub.len() / 3;
            let again: Vec<Change> = (start..hub.len())
                .filter(|&i| self.seen[i])
                .map(|i| hub[i].clone())
                .collect();
            self.apply(&again).await?;
        }
        Ok(())
    }
}

fn mix(seed: u64, i: usize) -> u64 {
    let mut x = seed ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^ (x >> 33)
}

#[derive(Debug, Clone)]
enum Op {
    AddShelf(u8),
    RenameShelf(usize, u8),
    DeleteShelf(usize),
    AddBook(usize, u8, u8),
    SetTitle(usize, u8),
    SetIsbn(usize, u8),
    SetStatus(usize, u8),
    SetAuthor(usize, usize),
    SetExtra(usize, u8),
    DeleteBook(usize),
    MoveBookDeleteShelf(usize, usize),
    MoveNoteDeleteShelf(usize, usize),
    MoveNoteDeleteBook(usize, usize),
    MoveMarkDeleteBook(usize, usize),
    AddAuthor(u8),
    DeleteAuthor(usize),
    SetFavourite(usize, usize),
    AddNote(usize, u8),
    SetNote(usize, u8),
    DeleteNote(usize),
    AddMark(usize, u8),
    DeleteMark(usize),
    AddTag(usize, u8),
    SetTagNote(usize, u8),
    DeleteTag(usize),
    SetSetting(u8, u8),
    DeleteSetting(u8),
    AddCover(usize),
    /// A write with the triggers suspended, sent on resuming.
    Suspended(usize, u8),
    Upgrade,
    Sync,
    Push,
    Deliver(u64, bool, bool),
    /// Takes everything one device has sent that this peer has not seen, as a device that reads
    /// each device's log in turn does.
    Log(usize, bool),
}

fn arb_op() -> impl Strategy<Value = Op> {
    let n = 0usize..6;
    let v = 0u8..3;
    prop_oneof![
        3 => v.clone().prop_map(Op::AddShelf),
        2 => (n.clone(), v.clone()).prop_map(|(a, b)| Op::RenameShelf(a, b)),
        1 => n.clone().prop_map(Op::DeleteShelf),
        8 => (n.clone(), v.clone(), 0u8..4).prop_map(|(a, b, c)| Op::AddBook(a, b, c)),
        3 => (n.clone(), v.clone()).prop_map(|(a, b)| Op::SetTitle(a, b)),
        5 => (n.clone(), 0u8..4).prop_map(|(a, b)| Op::SetIsbn(a, b)),
        3 => (n.clone(), 0u8..4).prop_map(|(a, b)| Op::SetStatus(a, b)),
        2 => (n.clone(), n.clone()).prop_map(|(a, b)| Op::SetAuthor(a, b)),
        3 => (n.clone(), v.clone()).prop_map(|(a, b)| Op::SetExtra(a, b)),
        1 => n.clone().prop_map(Op::DeleteBook),
        1 => (n.clone(), n.clone()).prop_map(|(a, b)| Op::MoveBookDeleteShelf(a, b)),
        1 => (n.clone(), n.clone()).prop_map(|(a, b)| Op::MoveNoteDeleteShelf(a, b)),
        1 => (n.clone(), n.clone()).prop_map(|(a, b)| Op::MoveNoteDeleteBook(a, b)),
        1 => (n.clone(), n.clone()).prop_map(|(a, b)| Op::MoveMarkDeleteBook(a, b)),
        2 => v.clone().prop_map(Op::AddAuthor),
        1 => n.clone().prop_map(Op::DeleteAuthor),
        2 => (n.clone(), n.clone()).prop_map(|(a, b)| Op::SetFavourite(a, b)),
        5 => (n.clone(), v.clone()).prop_map(|(a, b)| Op::AddNote(a, b)),
        3 => (n.clone(), v.clone()).prop_map(|(a, b)| Op::SetNote(a, b)),
        1 => n.clone().prop_map(Op::DeleteNote),
        5 => (n.clone(), v.clone()).prop_map(|(a, b)| Op::AddMark(a, b)),
        1 => n.clone().prop_map(Op::DeleteMark),
        4 => (n.clone(), 0u8..2).prop_map(|(a, b)| Op::AddTag(a, b)),
        2 => (n.clone(), v.clone()).prop_map(|(a, b)| Op::SetTagNote(a, b)),
        1 => n.clone().prop_map(Op::DeleteTag),
        2 => (0u8..2, v.clone()).prop_map(|(a, b)| Op::SetSetting(a, b)),
        1 => (0u8..2).prop_map(Op::DeleteSetting),
        2 => n.clone().prop_map(Op::AddCover),
        1 => (n.clone(), v.clone()).prop_map(|(a, b)| Op::Suspended(a, b)),
        1 => Just(Op::Upgrade),
        4 => Just(Op::Sync),
        2 => Just(Op::Push),
        6 => (any::<u64>(), any::<bool>(), any::<bool>()).prop_map(|(a, b, c)| Op::Deliver(a, b, c)),
        4 => (0usize..5, any::<bool>()).prop_map(|(a, b)| Op::Log(a, b)),
    ]
}

async fn perform(peer: &mut Peer, op: Op, hub: &mut Vec<Change>) -> Result<(), String> {
    let isbn = |v: u8| {
        if v == 3 {
            "NULL".to_string()
        } else {
            format!("'i{}'", v % 2)
        }
    };
    let sql = match op {
        Op::Upgrade => return peer.upgrade().await,
        Op::Suspended(n, v) => {
            let replica = peer.replica.clone();
            replica.suspend(&mut peer.conn).await.unwrap();
            if let Some(id) = peer.pick("books", n).await {
                peer.run(&format!(
                    "UPDATE books SET title = 'quiet{v}' WHERE id = {id}"
                ))
                .await;
            }
            let waiting = peer.writes.installing(&mut peer.conn, &peer.replica).await;
            replica.resume(&mut peer.conn, Resume::Send).await.unwrap();
            peer.writes
                .installed(&mut peer.conn, &peer.replica, waiting)
                .await;
            return peer.midway().await;
        }
        Op::Sync => {
            peer.push(hub).await?;
            return peer.deliver(hub, |_| true, false, false).await;
        }
        Op::Push => return peer.push(hub).await,
        Op::Deliver(seed, reverse, twice) => {
            return peer
                .deliver(hub, |i| !mix(seed, i).is_multiple_of(3), reverse, twice)
                .await;
        }
        Op::Log(device, reverse) => {
            let mut devices = Vec::new();
            for change in hub.iter() {
                if !devices.contains(&change.clock.node) {
                    devices.push(change.clock.node);
                }
            }
            let Some(&node) = devices.get(device % devices.len().max(1)) else {
                return Ok(());
            };
            return peer
                .deliver(hub, |i| hub[i].clock.node == node, reverse, false)
                .await;
        }
        Op::AddShelf(v) => format!("INSERT OR IGNORE INTO shelves (name) VALUES ('shelf{v}')"),
        Op::RenameShelf(n, v) => match peer.pick("shelves", n).await {
            Some(id) => format!("UPDATE OR IGNORE shelves SET name = 'shelf{v}' WHERE id = {id}"),
            None => return Ok(()),
        },
        Op::DeleteShelf(n) => match peer.pick("shelves", n).await {
            Some(id) => format!("DELETE FROM shelves WHERE id = {id}"),
            None => return Ok(()),
        },
        Op::AddBook(n, v, i) => match peer.pick("shelves", n).await {
            Some(s) => format!(
                "INSERT OR IGNORE INTO books (shelf_id, title, isbn, seen)
                 VALUES ({s}, 'title{v}', {}, 0)",
                isbn(i)
            ),
            None => return Ok(()),
        },
        Op::SetTitle(n, v) => match peer.pick("books", n).await {
            Some(id) => format!("UPDATE books SET title = 'title{v}' WHERE id = {id}"),
            None => return Ok(()),
        },
        Op::SetIsbn(n, v) => match peer.pick("books", n).await {
            Some(id) => format!(
                "UPDATE OR IGNORE books SET isbn = {} WHERE id = {id}",
                isbn(v)
            ),
            None => return Ok(()),
        },
        Op::SetStatus(n, v) => match peer.pick("books", n).await {
            // A book holds a finish date exactly when it is done.
            Some(id) => {
                let set = match v {
                    0 => "status = 'reading', finished = NULL".to_string(),
                    1 => "status = NULL, finished = NULL".to_string(),
                    _ => format!("status = 'done', finished = {v}"),
                };
                format!("UPDATE books SET {set} WHERE id = {id}")
            }
            None => return Ok(()),
        },
        Op::SetAuthor(n, m) => match (peer.pick("books", n).await, peer.pick("authors", m).await) {
            (Some(b), Some(a)) => format!("UPDATE books SET author_id = {a} WHERE id = {b}"),
            _ => return Ok(()),
        },
        Op::SetExtra(n, v) => match peer.pick("books", n).await {
            Some(id) => format!("UPDATE books SET extra = 'x{v}' WHERE id = {id}"),
            None => return Ok(()),
        },
        Op::DeleteBook(n) => match peer.pick("books", n).await {
            Some(id) => format!("DELETE FROM books WHERE id = {id}"),
            None => return Ok(()),
        },
        Op::MoveBookDeleteShelf(n, m) => {
            let (Some(b), Some(s)) = (peer.pick("books", n).await, peer.pick("shelves", m).await)
            else {
                return Ok(());
            };
            let Some(from) = peer
                .one(&format!("SELECT shelf_id FROM books WHERE id = {b}"))
                .await
                .filter(|from| *from != s)
            else {
                return Ok(());
            };
            peer.run(&format!("UPDATE books SET shelf_id = {s} WHERE id = {b}"))
                .await;
            format!("DELETE FROM shelves WHERE id = {from}")
        }
        Op::MoveNoteDeleteShelf(n, m) => {
            let (Some(note), Some(b)) = (peer.pick("notes", n).await, peer.pick("books", m).await)
            else {
                return Ok(());
            };
            let from = peer
                .one(&format!(
                    "SELECT b.shelf_id FROM notes n JOIN books b ON b.id = n.book_id
                      WHERE n.id = {note}"
                ))
                .await;
            let to = peer
                .one(&format!("SELECT shelf_id FROM books WHERE id = {b}"))
                .await;
            let (Some(from), Some(to)) = (from, to) else {
                return Ok(());
            };
            if from == to {
                return Ok(());
            }
            peer.run(&format!("UPDATE notes SET book_id = {b} WHERE id = {note}"))
                .await;
            format!("DELETE FROM shelves WHERE id = {from}")
        }
        Op::MoveNoteDeleteBook(n, m) | Op::MoveMarkDeleteBook(n, m) => {
            let marks = matches!(op, Op::MoveMarkDeleteBook(..));
            let (child, parent, column) = if marks {
                ("marks", "notes", "note_id")
            } else {
                ("notes", "books", "book_id")
            };
            let (Some(id), Some(to)) = (peer.pick(child, n).await, peer.pick(parent, m).await)
            else {
                return Ok(());
            };
            let Some(from) = peer
                .one(&format!("SELECT {column} FROM {child} WHERE id = {id}"))
                .await
                .filter(|from| *from != to)
            else {
                return Ok(());
            };
            peer.run(&format!(
                "UPDATE OR IGNORE {child} SET {column} = {to} WHERE id = {id}"
            ))
            .await;
            let book = if marks {
                match peer
                    .one(&format!("SELECT book_id FROM notes WHERE id = {from}"))
                    .await
                {
                    Some(book) => book,
                    None => return Ok(()),
                }
            } else {
                from
            };
            format!("DELETE FROM books WHERE id = {book}")
        }
        Op::AddAuthor(v) => format!("INSERT INTO authors (name) VALUES ('author{v}')"),
        Op::DeleteAuthor(n) => match peer.pick("authors", n).await {
            Some(id) => format!("DELETE FROM authors WHERE id = {id}"),
            None => return Ok(()),
        },
        Op::SetFavourite(n, m) => {
            match (peer.pick("authors", n).await, peer.pick("books", m).await) {
                (Some(a), Some(b)) => format!("UPDATE authors SET favourite = {b} WHERE id = {a}"),
                _ => return Ok(()),
            }
        }
        Op::AddNote(n, v) => match peer.pick("books", n).await {
            Some(b) => format!("INSERT INTO notes (book_id, body) VALUES ({b}, 'body{v}')"),
            None => return Ok(()),
        },
        Op::SetNote(n, v) => match peer.pick("notes", n).await {
            Some(id) => format!("UPDATE notes SET body = 'body{v}' WHERE id = {id}"),
            None => return Ok(()),
        },
        Op::DeleteNote(n) => match peer.pick("notes", n).await {
            Some(id) => format!("DELETE FROM notes WHERE id = {id}"),
            None => return Ok(()),
        },
        Op::AddMark(n, v) => match peer.pick("notes", n).await {
            Some(id) => format!("INSERT OR IGNORE INTO marks (note_id, place) VALUES ({id}, {v})"),
            None => return Ok(()),
        },
        Op::DeleteMark(n) => match peer.pick("marks", n).await {
            Some(id) => format!("DELETE FROM marks WHERE id = {id}"),
            None => return Ok(()),
        },
        Op::AddTag(n, v) => match peer.pick("books", n).await {
            Some(b) => format!("INSERT OR IGNORE INTO tags (book_id, tag) VALUES ({b}, 'tag{v}')"),
            None => return Ok(()),
        },
        Op::SetTagNote(n, v) => match peer.pick("tags", n).await {
            Some(id) => format!("UPDATE tags SET note = 'note{v}' WHERE rowid = {id}"),
            None => return Ok(()),
        },
        Op::DeleteTag(n) => match peer.pick("tags", n).await {
            Some(id) => format!("DELETE FROM tags WHERE rowid = {id}"),
            None => return Ok(()),
        },
        Op::SetSetting(k, v) => format!(
            "INSERT INTO settings VALUES ('key{k}', 'value{v}')
             ON CONFLICT (key) DO UPDATE SET value = excluded.value"
        ),
        Op::DeleteSetting(k) => format!("DELETE FROM settings WHERE key = 'key{k}'"),
        Op::AddCover(n) => match peer.pick("books", n).await {
            Some(b) => format!("INSERT OR IGNORE INTO covers (book_id, path) VALUES ({b}, 'c')"),
            None => return Ok(()),
        },
    };
    peer.run(&sql).await;
    Ok(())
}

/// Upgrades every peer, then exchanges until a round sends nothing and every peer has seen
/// everything, and a last collect on each finds nothing left to send.
async fn finish(peers: &mut [Peer], hub: &mut Vec<Change>) -> Result<(), String> {
    for (i, peer) in peers.iter_mut().enumerate() {
        peer.upgrade()
            .await
            .map_err(|e| format!("peer {i}, upgrading: {e}"))?;
    }
    for _ in 0..40 {
        let before = hub.len();
        for (i, peer) in peers.iter_mut().enumerate() {
            peer.push(hub)
                .await
                .map_err(|e| format!("peer {i}, sending: {e}"))?;
            peer.deliver(hub, |_| true, false, false)
                .await
                .map_err(|e| format!("peer {i}, taking: {e}"))?;
        }
        let mut quiet = hub.len() == before
            && peers
                .iter()
                .all(|p| p.seen.len() == hub.len() && p.seen.iter().all(|s| *s));
        for peer in peers.iter_mut() {
            peer.replica.collect(&mut peer.conn).await.unwrap();
            let left = peer.replica.outbox(&mut peer.conn, 1).await.unwrap();
            quiet &= left.changes.is_empty();
        }
        if quiet {
            return Ok(());
        }
    }
    Err("the peers never stopped sending".into())
}

async fn check(peers: &mut [Peer]) -> Result<(), String> {
    let mut first: Option<fongo_sync_sqlite::core::model::View> = None;
    for (i, peer) in peers.iter_mut().enumerate() {
        support::settled(&mut peer.conn, &peer.replica, &peer.taken)
            .await
            .map_err(|e| format!("peer {i}: {e}"))?;
        let mixed = peer.lines(MIXED).await;
        if !mixed.is_empty() {
            return Err(format!("peer {i}: {mixed:#?}"));
        }
        let view = support::read(&mut peer.conn, &peer.replica).await.view;
        match &first {
            None => first = Some(view),
            Some(first) if *first != view => {
                return Err(format!("rows differ, 0 and {i}:\n{first:#?}\n{view:#?}"));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// Runs a script on five peers of a world made from `seed`. The same seed and script always do
/// the same thing.
fn run(seed: u64, script: &[(usize, Op)]) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let outcome = runtime.block_on(async {
        let world = World::new(seed);
        let mut hub = Vec::new();
        let mut peers = Vec::new();
        for n in 0..5 {
            peers.push(Peer::new(&world, n).await);
        }
        for (step, (who, op)) in script.iter().cloned().enumerate() {
            perform(&mut peers[who], op, &mut hub)
                .await
                .map_err(|e| format!("after step {step}, peer {who}: {e}"))?;
        }
        finish(&mut peers, &mut hub)
            .await
            .map_err(|e| format!("while exchanging everything: {e}"))?;
        check(&mut peers).await
    });
    outcome.map_err(|e| format!("{e}\n\n{}", reproduction(seed, script)))
}

/// A failing case as a test to paste below.
fn reproduction(seed: u64, script: &[(usize, Op)]) -> String {
    let steps: Vec<String> = script
        .iter()
        .map(|(who, op)| format!("        ({who}, {op:?}),"))
        .collect();
    format!(
        "#[test]\nfn found() {{\n    use Op::*;\n    let script = vec![\n{}\n    ];\n    run({seed}, &script).unwrap();\n}}",
        steps.join("\n")
    )
}

/// A modest number of cases unless `PROPTEST_CASES` asks for more; `make deep` does.
fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(32)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: cases(),
        failure_persistence: None,
        ..ProptestConfig::default()
    })]
    #[test]
    fn replicas_agree_with_the_model_at_every_step_and_show_its_view_at_the_end(
        seed in any::<u64>(),
        script in proptest::collection::vec((0usize..5, arb_op()), 0..300)
    ) {
        if let Err(error) = run(seed, &script) {
            prop_assert!(false, "{}", error);
        }
    }
}

/// Two books on one shelf both take a status, and an unrelated edit to one of them on a replica
/// that holds only half of a status write must not send the half it holds.
#[test]
fn a_half_delivered_set_is_not_sent_by_an_unrelated_edit() {
    use Op::*;
    let script = vec![
        (0, AddAuthor(2)),
        (0, Push),
        (0, AddShelf(0)),
        (2, AddShelf(2)),
        (2, AddBook(1, 1, 1)),
        (2, Sync),
        (2, SetStatus(0, 2)),
        (0, AddAuthor(1)),
        (3, AddShelf(2)),
        (3, Deliver(9312422101023782502, true, true)),
        (2, SetAuthor(0, 4)),
        (0, AddBook(2, 1, 1)),
        (3, Sync),
        (2, Sync),
        (0, Sync),
        (1, Deliver(7153892181697962845, false, false)),
        (1, SetTitle(1, 2)),
    ];
    for seed in 0..10 {
        run(seed, &script).unwrap();
    }
}

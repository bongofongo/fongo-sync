//! Replicas that use every feature at once, some holding rows before the kernel is installed,
//! exchange changes in any order, part by part and more than once, and change their registry
//! midway, agree with the reference model after every step and show exactly its view once
//! everything is exchanged. Each replica collects whole or in bounded steps of a size its world
//! picks, with the application writing between some steps; every other time it collects in steps
//! with no writes between, a copy of its database made at the start collects whole, and the two
//! must send, hold and show the same. `PROPERTY_STEP` (`whole` or a number of rows) makes every
//! replica collect one way. In a second property a replica's database can be replaced by a copy
//! of another's, or by a backup of its own, which then issues clocks already issued, so changes
//! to one field share a clock and differ. Until that first happens in a run, no apply may report
//! a shared node.
//!
//! Books name a cover file, and the upgrade marks a field of notes and a new collection as naming
//! files, so that a replica not yet upgraded holds references it cannot interpret. After every
//! step the files a replica reports are checked against the protocol's definition computed from
//! the changes it holds (`support::files`), under its registry and the upgraded one, and what each
//! apply, collect and purge reports doing to them against what it did.

mod support;

use std::collections::BTreeSet;

use fongo_sync_sqlite::core::{Change, Gid};
use fongo_sync_sqlite::{COLLECT_ROWS, Collection, LocalDefault, Made, Registry, Replica, Resume};
use proptest::prelude::*;
use proptest::test_runner::RngSeed;
use sqlx::SqliteConnection;
use support::world::World;

/// A shelf's name is its identity. It accepts NULL: where an identity column refuses NULL,
/// `plan::order` cannot write some sets of rows that wait on each other (see
/// `a_shelf_renamed_onto_one_its_book_moved_to`), and they wait for the application.
const SCHEMA: &[&str] = &[
    "CREATE TABLE shelves (id INTEGER PRIMARY KEY, name TEXT UNIQUE)",
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
        seen INTEGER NOT NULL,
        cover BLOB)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        body TEXT,
        scan BLOB)",
    "CREATE TABLE attachments (
        id INTEGER PRIMARY KEY,
        note_id INTEGER NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
        file BLOB)",
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
    "CREATE TABLE covers (
        book_id INTEGER NOT NULL UNIQUE REFERENCES books(id) ON DELETE CASCADE,
        path TEXT)",
    // An identity no index enforces, so one replica can find two of its own rows to be one.
    "CREATE TABLE series (id INTEGER PRIMARY KEY, code TEXT, name TEXT)",
    "CREATE TABLE volumes (
        id INTEGER PRIMARY KEY,
        series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
        title TEXT)",
];

/// The registry before and after an upgrade that adds a field, a field naming files and a
/// collection whose field names files.
fn registry(upgraded: bool) -> Registry {
    let mut books = Collection::minted("books", "books", "id")
        .owner("shelf_id", "shelves")
        .field("title")
        .field("isbn")
        .reference("author_id", "authors")
        .field("status")
        .field("finished")
        .file("cover")
        .together(&["status", "finished"])
        .identity(&["isbn"])
        .local_default(
            "seen",
            LocalDefault::Sql("SELECT COALESCE(max(seen), 0) + 1 FROM books".into()),
        )
        .local_dependent("covers", "book_id");
    let mut notes = Collection::minted("notes", "notes", "id")
        .owner("book_id", "books")
        .field_reporting_conflicts("body");
    let mut later = Vec::new();
    if upgraded {
        books = books.field("extra");
        notes = notes.file("scan");
        later.push(
            Collection::minted("attachments", "attachments", "id")
                .owner("note_id", "notes")
                .file("file"),
        );
    }
    let mut collections = vec![
        Collection::minted("shelves", "shelves", "id")
            .field("name")
            .identity(&["name"]),
        Collection::minted("authors", "authors", "id")
            .field("name")
            .reference("favourite", "books"),
        books,
        notes,
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
        Collection::minted("series", "series", "id")
            .field("code")
            .field("name")
            .identity(&["code"]),
        Collection::minted("volumes", "volumes", "id")
            .owner("series_id", "series")
            .field("title"),
    ];
    collections.extend(later);
    Registry::new(collections).unwrap()
}

/// A file reference as an SQL literal: the `n`th file, or NULL for 3.
fn file_sql(n: u8) -> String {
    if n == 3 {
        return "NULL".into();
    }
    let bytes = fongo_sync_sqlite::FileRef::of(&[n; 3]).encode();
    let hex: String = bytes.iter().map(|b| format!("{b:02X}")).collect();
    format!("X'{hex}'")
}

/// What must hold on every replica once everything is exchanged, of the application's own
/// tables: a book holds a finish date exactly when it is done.
const MIXED: &str = "
    SELECT 'mixed ' || id FROM books
     WHERE (COALESCE(status, '') = 'done') <> (finished IS NOT NULL)";

struct Peer {
    conn: SqliteConnection,
    replica: Replica,
    /// Rows a bounded step of collecting takes, or `None` to collect whole.
    step: Option<usize>,
    /// Picks the writes made between steps.
    seed: u64,
    steps: usize,
    upgraded: bool,
    seen: Vec<bool>,
    sent: Option<(usize, i64)>,
    /// Every change applied here or sent from here.
    taken: Vec<Change>,
    writes: support::writes::Writes,
    /// What this replica answered of who made each live group and wrote each field, and the
    /// changes and registry it answered from, as of its last step.
    origins: Option<(BTreeSet<String>, support::Origins)>,
    /// The world and number of this replica's database, to collect a copy of it whole.
    world: World,
    n: u64,
    pushes: usize,
    /// The node the replica's clocks carry, which a copy of its database shares.
    node: Gid,
    /// The files it reported at its last check.
    files: support::files::Reported,
}

impl Peer {
    /// A replica whose tables hold `rows` before the kernel is installed.
    /// `step` is the rows each step collects, `None` to collect whole; left out, the world picks.
    async fn new(
        world: &World,
        n: u64,
        schema: &[&str],
        rows: &[&str],
        seed: u64,
        step: Option<Option<usize>>,
    ) -> Peer {
        let mut conn = world.connect(n).await;
        for sql in schema.iter().chain(rows) {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }
        let mut peer = Peer {
            conn,
            replica: Replica::new(registry(false)),
            step: step.unwrap_or(match std::env::var("PROPERTY_STEP").as_deref() {
                Ok("whole") => None,
                Ok(rows) => rows.parse().ok(),
                Err(_) => [None, Some(1), Some(2), Some(3), Some(COLLECT_ROWS)]
                    [(mix(seed, n as usize) % 5) as usize],
            }),
            seed: mix(seed, 1_000 + n as usize),
            steps: 0,
            world: world.clone(),
            n,
            pushes: 0,
            upgraded: false,
            seen: Vec::new(),
            sent: None,
            taken: Vec::new(),
            writes: Default::default(),
            origins: None,
            node: Gid([0; 16]),
            files: Default::default(),
        };
        peer.install().await.unwrap();
        let node = peer.replica.inspect(&mut peer.conn).await.unwrap().node;
        peer.node = node.expect("an installed replica has a node");
        peer
    }

    /// A copy of this replica's database as database `n` of its world, with everything this
    /// replica knew: a second database sharing its identity, as a copied file or a restored
    /// backup is.
    async fn copy(&mut self, n: u64) -> Peer {
        let conn = self.world.copy(&mut self.conn, n).await;
        Peer {
            conn,
            replica: self.replica.clone(),
            step: self.step,
            seed: self.seed,
            steps: self.steps,
            upgraded: self.upgraded,
            seen: self.seen.clone(),
            sent: self.sent,
            taken: self.taken.clone(),
            writes: self.writes.clone(),
            origins: None,
            world: self.world.clone(),
            n,
            pushes: self.pushes,
            node: self.node,
            files: self.files.clone(),
        }
    }

    /// Installs whole, or in steps: an install that collects one step, then steps of collecting,
    /// checking the model after each.
    async fn install(&mut self) -> Result<(), String> {
        let waiting = self
            .writes
            .installing(&mut self.conn, &self.replica, &self.taken)
            .await;
        let Some(rows) = self.step else {
            self.replica.install(&mut self.conn).await.unwrap();
            self.writes
                .installed(&mut self.conn, &self.replica, waiting)
                .await;
            return Ok(());
        };
        let first = self
            .replica
            .install_step(&mut self.conn, rows)
            .await
            .unwrap();
        self.writes.touched(&first.touched);
        let mut more = first.more;
        if more {
            self.writes
                .installed_step(&mut self.conn, &self.replica, waiting)
                .await;
        } else {
            self.writes
                .installed(&mut self.conn, &self.replica, waiting)
                .await;
        }
        while more {
            self.between().await;
            self.before_step().await;
            let collected = self
                .replica
                .collect_step(&mut self.conn, rows)
                .await
                .unwrap();
            self.writes.touched(&collected.touched);
            more = collected.more;
            if more {
                self.writes.after_step(&mut self.conn, &self.replica).await;
            } else {
                self.writes.after(&mut self.conn, &self.replica).await;
            }
            self.collected().await?;
        }
        Ok(())
    }

    /// What the replica has taken in, with its outbox, which is not yet in `taken`.
    async fn holding(&mut self) -> Vec<Change> {
        let outbox = self.replica.outbox(&mut self.conn, u32::MAX).await.unwrap();
        let mut seen = self.taken.clone();
        seen.extend(outbox.changes);
        seen
    }

    /// Checks a replica that collected part of what was written against the model of what it
    /// holds.
    async fn collected(&mut self) -> Result<(), String> {
        let seen = self.holding().await;
        support::midway(&mut self.conn, &self.replica, &seen).await?;
        self.files = self.files_now().await?;
        Ok(())
    }

    /// The files the replica reports, checked against the changes it holds under its registry
    /// and, before it upgrades, the upgraded one.
    async fn files_now(&mut self) -> Result<support::files::Reported, String> {
        let seen = self.holding().await;
        let later = if self.upgraded {
            Vec::new()
        } else {
            vec![registry(true)]
        };
        support::files::check(&mut self.conn, &self.replica, &seen, &later)
            .await
            .map_err(|e| format!("files: {e}"))
    }

    /// Clears the files of every deleted entity, in steps of a few, each checked: a local write
    /// the application asks for, though no row of its own changes.
    async fn purge(&mut self) -> Result<(), String> {
        let mut from = None;
        loop {
            let before = self.files_now().await?;
            let waiting = self
                .replica
                .outbox(&mut self.conn, u32::MAX)
                .await
                .unwrap()
                .changes
                .len();
            let step = self
                .replica
                .purge_step(&mut self.conn, u64::MAX, from.as_ref(), 2)
                .await
                .unwrap();
            let outbox = self.replica.outbox(&mut self.conn, u32::MAX).await.unwrap();
            let cleared = &outbox.changes[waiting..];
            if cleared.len() != step.cleared
                || cleared
                    .iter()
                    .any(|c| c.value != fongo_sync_sqlite::core::Value::Null)
            {
                return Err(format!("a purge step sent {cleared:?}, reporting {step:?}"));
            }
            self.writes.asked(cleared);
            self.collected().await?;
            support::files::reported(&before, &self.files, &step.files, None)
                .map_err(|e| format!("purging: {e}"))?;
            match step.next {
                Some(next) => from = Some(next),
                None => return Ok(()),
            }
        }
    }

    /// Before a step of collecting that follows another.
    async fn before_step(&mut self) {
        let seen = self.holding().await;
        self.writes
            .before(&mut self.conn, &self.replica, &seen)
            .await;
    }

    /// Between two steps of collecting, now and then, a write of the application's.
    async fn between(&mut self) {
        self.steps += 1;
        let pick = mix(self.seed, self.steps);
        if !pick.is_multiple_of(3) {
            return;
        }
        let n = (pick >> 8) % 7;
        let row = |table: &str| {
            format!(
                "(SELECT rowid FROM {table} ORDER BY rowid LIMIT 1
                  OFFSET {n} % max(1, (SELECT count(*) FROM {table})))"
            )
        };
        let sql = match (pick >> 4) % 8 {
            0 => format!(
                "UPDATE books SET title = 'between{n}' WHERE id = {}",
                row("books")
            ),
            1 => format!(
                "INSERT INTO notes (book_id, body) SELECT {}, 'between' WHERE EXISTS (SELECT 1 FROM books)",
                row("books")
            ),
            2 => format!("DELETE FROM tags WHERE rowid = {}", row("tags")),
            3 => format!("DELETE FROM books WHERE id = {}", row("books")),
            4 => format!(
                "INSERT OR IGNORE INTO shelves (name) VALUES ('shelf{}')",
                n % 3
            ),
            5 => format!(
                "INSERT OR IGNORE INTO tags (book_id, tag) SELECT {}, 'tag{}' WHERE EXISTS (SELECT 1 FROM books)",
                row("books"),
                n % 2
            ),
            6 => format!(
                "UPDATE OR IGNORE books SET isbn = 'i{}' WHERE id = {}",
                n % 2,
                row("books")
            ),
            _ => format!(
                "UPDATE notes SET body = 'between{n}' WHERE id = {}",
                row("notes")
            ),
        };
        self.run(&sql).await;
    }

    /// Checks the replica against the model, and records what it answers of who made each live
    /// group and wrote each field, with the changes and registry it answers from.
    async fn midway(&mut self) -> Result<(), String> {
        let (seen, answers) =
            support::midway_answering(&mut self.conn, &self.replica, &self.taken).await?;
        let mut held: BTreeSet<String> = seen.iter().map(|c| format!("{c:?}")).collect();
        held.insert(format!("upgraded {}", self.upgraded));
        self.origins = Some((held, answers));
        self.files = self.files_now().await?;
        Ok(())
    }

    async fn upgrade(&mut self) -> Result<(), String> {
        if !self.upgraded {
            self.upgraded = true;
            self.replica = Replica::new(registry(true));
            self.install().await?;
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

    /// The rows, registers and alias groups.
    async fn state(&mut self) -> String {
        let kernel = support::read(&mut self.conn, &self.replica).await;
        let inspection = self.replica.inspect(&mut self.conn).await.unwrap();
        format!(
            "{:?}\n{:?}\n{:?}",
            kernel.view, inspection.registers, inspection.aliases
        )
    }

    async fn apply(&mut self, batch: &[Change]) -> Result<(), String> {
        let before = self.files_now().await?;
        let waiting = self
            .replica
            .outbox(&mut self.conn, u32::MAX)
            .await
            .unwrap()
            .changes
            .len();
        self.taken.extend(batch.iter().cloned());
        self.writes
            .before(&mut self.conn, &self.replica, &self.taken)
            .await;
        let applied = self.replica.apply(&mut self.conn, batch).await.unwrap();
        self.writes.touched(&applied.touched);
        self.writes.after(&mut self.conn, &self.replica).await;
        if !self.world.shared() && !applied.shared.is_empty() {
            return Err(format!(
                "no two databases share a node, yet an apply reports {:?}",
                applied.shared
            ));
        }
        self.midway().await?;
        let outbox = self.replica.outbox(&mut self.conn, u32::MAX).await.unwrap();
        let mut brought = support::files::carried(batch);
        brought.extend(support::files::carried(&outbox.changes[waiting..]));
        support::files::reported(&before, &self.files, &applied.files, Some(&brought))
            .map_err(|e| format!("applying: {e}"))
    }

    async fn push(&mut self, hub: &mut Vec<Change>) -> Result<(), String> {
        self.writes
            .before(&mut self.conn, &self.replica, &self.taken)
            .await;
        // Every other push collects with no writes between its steps, and a copy of the database
        // collects whole: the two must send, hold and show the same.
        self.pushes += 1;
        let quiet = self.step.is_some() && mix(self.seed, 10_000 + self.pushes).is_multiple_of(2);
        let whole = if quiet {
            let (_, mut copy) = self.world.twin(self.n, &mut self.conn).await;
            let start = self
                .replica
                .outbox(&mut copy, 1_000_000)
                .await
                .unwrap()
                .changes
                .len();
            self.replica.collect(&mut copy).await.unwrap();
            Some((start, collected_whole(&mut copy, &self.replica).await))
        } else {
            None
        };
        match self.step {
            None => {
                self.replica.collect(&mut self.conn).await.unwrap();
            }
            Some(rows) => loop {
                let before = self.files_now().await?;
                let waiting = self
                    .replica
                    .outbox(&mut self.conn, u32::MAX)
                    .await
                    .unwrap()
                    .changes
                    .len();
                let collected = self
                    .replica
                    .collect_step(&mut self.conn, rows)
                    .await
                    .unwrap();
                self.writes.touched(&collected.touched);
                if collected.more {
                    self.writes.after_step(&mut self.conn, &self.replica).await;
                } else {
                    self.writes.after(&mut self.conn, &self.replica).await;
                }
                self.collected().await?;
                let outbox = self.replica.outbox(&mut self.conn, u32::MAX).await.unwrap();
                let brought = support::files::carried(&outbox.changes[waiting..]);
                support::files::reported(&before, &self.files, &collected.files, Some(&brought))
                    .map_err(|e| format!("collecting a step: {e}"))?;
                if !collected.more {
                    break;
                }
                if !quiet {
                    self.between().await;
                }
                self.before_step().await;
            },
        }
        if let Some((start, whole)) = whole {
            let stepped = collected_whole(&mut self.conn, &self.replica).await;
            same_as_whole(start, &whole, &stepped)?;
        }
        self.writes.after(&mut self.conn, &self.replica).await;
        let outbox = self
            .replica
            .outbox(&mut self.conn, 1_000_000)
            .await
            .unwrap();
        self.midway().await?;
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

/// What a collect leaves: the outbox, the application's rows and the registers' values.
struct Left {
    outbox: Vec<Change>,
    rows: Vec<String>,
    values: Vec<String>,
}

async fn collected_whole(conn: &mut SqliteConnection, replica: &Replica) -> Left {
    let mut rows = Vec::new();
    for table in [
        "shelves",
        "authors",
        "books",
        "notes",
        "marks",
        "tags",
        "settings",
        "covers",
        "series",
        "volumes",
        "attachments",
    ] {
        let sql = format!("SELECT * FROM {table} ORDER BY rowid");
        for row in support::rows(conn, &sql, &[]).await {
            rows.push(format!("{table} {row:?}"));
        }
    }
    let inspection = replica.inspect(conn).await.unwrap();
    // A reference is held as the group it names: see `same_as_whole`.
    let canonical: std::collections::BTreeMap<fongo_sync_sqlite::core::Gid, _> = inspection
        .aliases
        .iter()
        .map(|(_, g, k)| (*g, *k))
        .collect();
    Left {
        outbox: replica.outbox(conn, 1_000_000).await.unwrap().changes,
        rows,
        values: inspection
            .registers
            .iter()
            .map(|(c, g, f, v, _)| {
                let v = match v {
                    fongo_sync_sqlite::core::Value::Ref(r) => {
                        fongo_sync_sqlite::core::Value::Ref(*canonical.get(r).unwrap_or(r))
                    }
                    v => v.clone(),
                };
                format!("{c} {g:?} {f} {v:?}")
            })
            .chain(inspection.aliases.iter().map(|a| format!("{a:?}")))
            .collect(),
    }
}

/// Collecting in steps sent, from `start` of the outbox, what collecting whole did: the same
/// changes in clock order, aliases joining the same groups, and the same rows and values.
fn same_as_whole(start: usize, whole: &Left, stepped: &Left) -> Result<(), String> {
    use fongo_sync_sqlite::core::{ALIAS, Gid, Value};
    // A reference may name another member of a group the steps join, as it may across two
    // collects: references are compared by the group they name, each side's groups joined by
    // the aliases it sent.
    let groups = |left: &Left| {
        let mut parent: std::collections::BTreeMap<Gid, Gid> = std::collections::BTreeMap::new();
        fn root(parent: &std::collections::BTreeMap<Gid, Gid>, mut g: Gid) -> Gid {
            while let Some(&p) = parent.get(&g) {
                if p == g {
                    break;
                }
                g = p;
            }
            g
        }
        for c in left.outbox[start..].iter().filter(|c| c.field == ALIAS) {
            if let Value::Ref(b) = c.value {
                let (ra, rb) = (root(&parent, c.gid), root(&parent, b));
                if ra != rb {
                    parent.insert(ra.max(rb), ra.min(rb));
                }
            }
        }
        move |g: Gid| root(&parent, g)
    };
    let plain = |left: &Left| {
        let group = groups(left);
        let mut changes: Vec<&Change> = left.outbox[start..]
            .iter()
            .filter(|c| c.field != ALIAS)
            .collect();
        changes.sort_by_key(|c| c.clock);
        changes
            .iter()
            .map(|c| {
                let value = match &c.value {
                    Value::Ref(g) => Value::Ref(group(*g)),
                    v => v.clone(),
                };
                format!("{} {:?} {} {:?}", c.collection, c.gid, c.field, value)
            })
            .collect::<Vec<_>>()
    };
    if plain(whole) != plain(stepped) {
        return Err(format!(
            "collected in steps, sent otherwise than whole:\n{:#?}\n{:#?}",
            plain(whole),
            plain(stepped)
        ));
    }
    if whole.rows != stepped.rows {
        return Err(format!(
            "collected in steps, rows differ from whole:\n{:#?}\n{:#?}",
            whole.rows, stepped.rows
        ));
    }
    if whole.values != stepped.values {
        return Err(format!(
            "collected in steps, values or groups differ from whole:\n{:#?}\n{:#?}",
            whole.values, stepped.values
        ));
    }
    Ok(())
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
    AddSeries(u8, u8),
    SetSeriesCode(usize, u8),
    AddVolume(usize, u8),
    SetCover(usize, u8),
    SetScan(usize, u8),
    AddAttachment(usize, u8),
    SetAttachment(usize, u8),
    DeleteAttachment(usize),
    /// Clears the files of every deleted entity.
    Purge,
    /// A write with the triggers suspended, sent on resuming.
    Suspended(usize, u8),
    Upgrade,
    Sync,
    Push,
    Deliver(u64, bool, bool),
    /// Takes everything one device has sent that this peer has not seen, as a device that reads
    /// each device's log in turn does.
    Log(usize, bool),
    /// This peer's database is replaced by a copy of the database of peer `.0`, which issues the
    /// clocks that one issues. With the clock held still, each of the two titles its `.1`th book
    /// with a title of its own and sends it, so the two changes share a clock.
    Copy(usize, usize),
    /// This peer keeps a copy of its database.
    Backup,
    /// This peer's database goes back to the copy it kept, if any, and issues again the clocks
    /// it issued since.
    Restore,
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
        3 => (0u8..2, v.clone()).prop_map(|(a, b)| Op::AddSeries(a, b)),
        2 => (n.clone(), 0u8..2).prop_map(|(a, b)| Op::SetSeriesCode(a, b)),
        2 => (n.clone(), v.clone()).prop_map(|(a, b)| Op::AddVolume(a, b)),
        4 => (n.clone(), 0u8..4).prop_map(|(a, b)| Op::SetCover(a, b)),
        3 => (n.clone(), 0u8..4).prop_map(|(a, b)| Op::SetScan(a, b)),
        3 => (n.clone(), 0u8..4).prop_map(|(a, b)| Op::AddAttachment(a, b)),
        2 => (n.clone(), 0u8..4).prop_map(|(a, b)| Op::SetAttachment(a, b)),
        1 => n.clone().prop_map(Op::DeleteAttachment),
        1 => Just(Op::Purge),
        1 => (n.clone(), v.clone()).prop_map(|(a, b)| Op::Suspended(a, b)),
        1 => Just(Op::Upgrade),
        4 => Just(Op::Sync),
        2 => Just(Op::Push),
        6 => (any::<u64>(), any::<bool>(), any::<bool>()).prop_map(|(a, b, c)| Op::Deliver(a, b, c)),
        4 => (0usize..5, any::<bool>()).prop_map(|(a, b)| Op::Log(a, b)),
    ]
}

/// [`arb_op`], and now and then a database copied over another, backed up or restored.
fn arb_op_copying() -> impl Strategy<Value = Op> {
    prop_oneof![
        30 => arb_op(),
        1 => (0usize..5, 0usize..6).prop_map(|(a, b)| Op::Copy(a, b)),
        1 => Just(Op::Backup),
        1 => Just(Op::Restore),
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
        Op::Copy(..) | Op::Backup | Op::Restore => unreachable!("done by the run"),
        Op::Purge => return peer.purge().await,
        Op::Suspended(n, v) => {
            let replica = peer.replica.clone();
            replica.suspend(&mut peer.conn).await.unwrap();
            if let Some(id) = peer.pick("books", n).await {
                peer.run(&format!(
                    "UPDATE books SET title = 'quiet{v}' WHERE id = {id}"
                ))
                .await;
            }
            let waiting = peer
                .writes
                .installing(&mut peer.conn, &peer.replica, &peer.taken)
                .await;
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
        Op::AddSeries(c, v) => {
            format!("INSERT INTO series (code, name) VALUES ('c{c}', 'name{v}')")
        }
        Op::SetSeriesCode(n, c) => match peer.pick("series", n).await {
            Some(id) => format!("UPDATE series SET code = 'c{c}' WHERE id = {id}"),
            None => return Ok(()),
        },
        Op::AddVolume(n, v) => match peer.pick("series", n).await {
            Some(id) => format!("INSERT INTO volumes (series_id, title) VALUES ({id}, 'v{v}')"),
            None => return Ok(()),
        },
        Op::SetCover(n, v) => match peer.pick("books", n).await {
            Some(id) => format!("UPDATE books SET cover = {} WHERE id = {id}", file_sql(v)),
            None => return Ok(()),
        },
        Op::SetScan(n, v) => match peer.pick("notes", n).await {
            Some(id) => format!("UPDATE notes SET scan = {} WHERE id = {id}", file_sql(v)),
            None => return Ok(()),
        },
        Op::AddAttachment(n, v) => match peer.pick("notes", n).await {
            Some(id) => format!(
                "INSERT INTO attachments (note_id, file) VALUES ({id}, {})",
                file_sql(if v == 3 { 3 } else { 4 + v })
            ),
            None => return Ok(()),
        },
        Op::SetAttachment(n, v) => match peer.pick("attachments", n).await {
            Some(id) => format!(
                "UPDATE attachments SET file = {} WHERE id = {id}",
                file_sql(if v == 3 { 3 } else { 4 + v })
            ),
            None => return Ok(()),
        },
        Op::DeleteAttachment(n) => match peer.pick("attachments", n).await {
            Some(id) => format!("DELETE FROM attachments WHERE id = {id}"),
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

/// Replicas that hold the same changes under the same registry agree on who made each live
/// group and wrote each field: at most one node says it made a group, and at most one that it
/// wrote a field; databases that share a node, being copies, each say so. Returns how many pairs
/// of replicas were compared.
fn agree(peers: &[Peer], with: usize) -> Result<usize, String> {
    let Some((held, mine)) = &peers[with].origins else {
        return Ok(0);
    };
    let alike: Vec<usize> = (0..peers.len())
        .filter(|&i| peers[i].origins.as_ref().is_some_and(|(h, _)| h == held))
        .collect();
    if alike.len() < 2 {
        return Ok(0);
    }
    for (group, (_, fields, _)) in mine {
        let mut makers = BTreeSet::new();
        let mut writers: std::collections::BTreeMap<&String, BTreeSet<Gid>> = Default::default();
        for &i in &alike {
            let theirs = peers[i].origins.as_ref().map(|(_, a)| a).unwrap();
            let Some((made, written, _)) = theirs.get(group) else {
                return Err(format!(
                    "peers {with} and {i} hold the same changes; {i} has no {group:?}"
                ));
            };
            if *made == Made::Here {
                makers.insert(peers[i].node);
            }
            for (f, here) in written {
                if here.is_some() != fields[f].is_some() {
                    return Err(format!(
                        "peers {with} and {i} disagree on whether {group:?}.{f} was written"
                    ));
                }
                let nodes = writers.entry(f).or_default();
                if *here == Some(true) {
                    nodes.insert(peers[i].node);
                }
            }
        }
        if makers.len() > 1 {
            return Err(format!(
                "{} replicas holding the same changes made {group:?}",
                makers.len()
            ));
        }
        if let Some((f, n)) = writers.iter().find(|(_, n)| n.len() > 1) {
            return Err(format!(
                "{} replicas holding the same changes wrote {group:?}.{f}",
                n.len()
            ));
        }
    }
    Ok(alike.len() - 1)
}

/// `copied` says whether two databases shared an identity: their writes at one clock to a set of
/// fields that changes together can mix, so a book's status and finish date need not match.
async fn check(peers: &mut [Peer], copied: bool) -> Result<(), String> {
    for peer in peers.iter_mut() {
        peer.midway().await?;
    }
    if agree(peers, 0)? != peers.len() - 1 {
        return Err("after exchanging everything, replicas hold different changes".into());
    }
    let everything: Vec<Change> = peers.iter().flat_map(|p| p.taken.iter().cloned()).collect();
    for (i, peer) in peers.iter().enumerate() {
        peer.writes
            .kept(peer.replica.registry(), &everything)
            .map_err(|e| format!("peer {i}: {e}"))?;
    }
    let mut first: Option<fongo_sync_sqlite::core::model::View> = None;
    for (i, peer) in peers.iter_mut().enumerate() {
        support::settled(&mut peer.conn, &peer.replica, &peer.taken)
            .await
            .map_err(|e| format!("peer {i}: {e}"))?;
        let mixed = peer.lines(MIXED).await;
        if !mixed.is_empty() && !copied {
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

/// Rows each replica holds before the kernel is installed: the same shelf, setting and book on
/// several, so identities and natural keys join rows no replica made, and others on one.
fn found_at_install(n: u64) -> &'static [&'static str] {
    match n {
        0 | 1 => &[
            "INSERT INTO shelves (id, name) VALUES (1, 'shelf0')",
            "INSERT INTO settings VALUES ('key0', 'value0')",
            "INSERT INTO books (id, shelf_id, title, isbn, seen) VALUES (1, 1, 'title0', 'i0', 1)",
        ],
        2 => &[
            "INSERT INTO shelves (id, name) VALUES (1, 'shelf0')",
            "INSERT INTO settings VALUES ('key0', 'value1')",
        ],
        3 => &[
            "INSERT INTO shelves (id, name) VALUES (1, 'shelf1')",
            "INSERT INTO books (id, shelf_id, title, isbn, seen) VALUES (1, 1, 'title1', NULL, 1)",
        ],
        _ => &[],
    }
}

/// Runs a script on five peers of a world made from `seed`. The same seed and script always do
/// the same thing.
fn run(seed: u64, script: &[(usize, Op)]) -> Result<(), String> {
    run_on(SCHEMA, false, seed, 0, script, None)
}

/// [`run`] on replicas that hold [`found_at_install`] before they install.
fn run_found(seed: u64, script: &[(usize, Op)]) -> Result<(), String> {
    run_on(SCHEMA, true, seed, 0, script, None)
}

/// [`run`] or [`run_found`] in a world whose clock moves on one reading in `every` (see
/// [`World::clustered`]), and with every peer collecting in steps of `step` rows (`Some(None)`
/// for whole) where given.
fn run_on(
    schema: &[&str],
    found: bool,
    seed: u64,
    every: u64,
    script: &[(usize, Op)],
    step: Option<Option<usize>>,
) -> Result<(), String> {
    run_back(schema, found, seed, every, 0, script, step)
}

/// [`run_on`] on a clock that also steps back `back` milliseconds now and then.
fn run_back(
    schema: &[&str],
    found: bool,
    seed: u64,
    every: u64,
    back: u64,
    script: &[(usize, Op)],
    step: Option<Option<usize>>,
) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let outcome = runtime.block_on(async {
        let world = World::stepping_back(seed, every, back);
        let mut hub = Vec::new();
        let mut peers = Vec::new();
        for n in 0..5 {
            let rows = if found { found_at_install(n) } else { &[] };
            peers.push(Peer::new(&world, n, schema, rows, seed, step).await);
        }
        let mut backups: Vec<Option<Peer>> = (0..5).map(|_| None).collect();
        let mut databases = 5;
        let mut copied = false;
        for (step, (who, op)) in script.iter().cloned().enumerate() {
            let done = match op {
                Op::Copy(from, book) => {
                    copied |= from != who;
                    copy(
                        &world,
                        &mut peers,
                        &mut hub,
                        from,
                        who,
                        book,
                        &mut databases,
                    )
                    .await
                }
                Op::Backup => {
                    databases += 1;
                    backups[who] = Some(peers[who].copy(databases).await);
                    Ok(())
                }
                Op::Restore => match &mut backups[who] {
                    Some(backup) => {
                        copied = true;
                        world.share();
                        databases += 1;
                        peers[who] = backup.copy(databases).await;
                        Ok(())
                    }
                    None => Ok(()),
                },
                op => perform(&mut peers[who], op, &mut hub).await,
            };
            done.map_err(|e| format!("after step {step}, peer {who}: {e}"))?;
            agree(&peers, who).map_err(|e| format!("after step {step}, peer {who}: {e}"))?;
        }
        finish(&mut peers, &mut hub)
            .await
            .map_err(|e| format!("while exchanging everything: {e}"))?;
        check(&mut peers, copied).await
    });
    outcome.map_err(|e| format!("{e}\n\n{}", reproduction(found, seed, every, back, script)))
}

/// Replaces peer `to`'s database with a copy of peer `from`'s; then, with the clock held, each
/// titles its `book`th book with a title of its own and sends it.
async fn copy(
    world: &World,
    peers: &mut [Peer],
    hub: &mut Vec<Change>,
    from: usize,
    to: usize,
    book: usize,
    databases: &mut u64,
) -> Result<(), String> {
    if from == to {
        return Ok(());
    }
    world.share();
    world.hold(true);
    *databases += 1;
    peers[to] = peers[from].copy(*databases).await;
    let mut outcome = Ok(());
    for i in [from, to] {
        let peer = &mut peers[i];
        if let Some(id) = peer.pick("books", book).await {
            peer.run(&format!(
                "UPDATE books SET title = 'copy{i}' WHERE id = {id}"
            ))
            .await;
        }
        outcome = outcome.and(peer.push(hub).await);
    }
    world.hold(false);
    outcome
}

/// A failing case as a test to paste below.
fn reproduction(found: bool, seed: u64, every: u64, back: u64, script: &[(usize, Op)]) -> String {
    let run = match (found, every, back) {
        (false, 0, 0) => format!("run({seed}, &script)"),
        (true, 0, 0) => format!("run_found({seed}, &script)"),
        (found, every, 0) => format!("run_on(SCHEMA, {found}, {seed}, {every}, &script, None)"),
        (found, every, back) => {
            format!("run_back(SCHEMA, {found}, {seed}, {every}, {back}, &script, None)")
        }
    };
    let steps: Vec<String> = script
        .iter()
        .map(|(who, op)| format!("        ({who}, {op:?}),"))
        .collect();
    format!(
        "#[test]\nfn found() {{\n    use Op::*;\n    let script = vec![\n{}\n    ];\n    {run}.unwrap();\n}}",
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

/// Half as many cases for databases that share a node, unless `PROPTEST_CASES` asks for more.
fn sharing_cases() -> u32 {
    match std::env::var("PROPTEST_CASES") {
        Ok(_) => cases(),
        Err(_) => cases() / 2,
    }
}

/// One fixed run unless `PROPTEST_RNG_SEED` asks for another, so `make check` is repeatable;
/// `make deep` picks a new seed each time and prints it.
fn rng_seed() -> RngSeed {
    match std::env::var("PROPTEST_RNG_SEED") {
        Ok(_) => ProptestConfig::default().rng_seed,
        Err(_) => RngSeed::Fixed(17),
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: cases(),
        rng_seed: rng_seed(),
        failure_persistence: None,
        ..ProptestConfig::default()
    })]
    #[test]
    fn replicas_agree_with_the_model_at_every_step_and_show_its_view_at_the_end(
        seed in any::<u64>(),
        found in any::<bool>(),
        every in prop_oneof![Just(0u64), Just(1), Just(3), Just(8), Just(40)],
        back in prop_oneof![Just(0u64), Just(1), Just(2), Just(40)],
        script in proptest::collection::vec((0usize..5, arb_op()), 0..300)
    ) {
        let outcome = run_back(SCHEMA, found, seed, every, back, &script, None);
        if let Err(error) = outcome {
            prop_assert!(false, "{}", error);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: sharing_cases(),
        rng_seed: rng_seed(),
        failure_persistence: None,
        ..ProptestConfig::default()
    })]
    /// The same, with databases copied over others, backed up and restored, so that two
    /// databases write as one node and their changes share clocks.
    #[test]
    fn replicas_sharing_a_node_agree_with_the_model_at_every_step_and_show_its_view_at_the_end(
        seed in any::<u64>(),
        every in prop_oneof![Just(0u64), Just(1), Just(3), Just(8), Just(40)],
        back in prop_oneof![Just(0u64), Just(1), Just(2), Just(40)],
        script in proptest::collection::vec((0usize..5, arb_op_copying()), 0..300)
    ) {
        let outcome = run_back(SCHEMA, false, seed, every, back, &script, None);
        if let Err(error) = outcome {
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

/// Found by a long run. A tag the application deletes belongs to a book whose group has a deleted
/// member and one whose tombstone has not arrived. Its owner's fate is undecided here, so the
/// kernel sends no tombstone for the tag, which comes back once the book's tombstone arrives.
#[test]
fn a_deleted_tag_whose_books_fate_is_undecided_stays_deleted() {
    use Op::*;
    let script = vec![
        (0, AddShelf(0)),
        (0, AddBook(0, 0, 1)),
        (4, AddShelf(0)),
        (0, AddBook(0, 0, 2)),
        (0, DeleteBook(0)),
        (0, AddShelf(0)),
        (0, AddBook(0, 0, 2)),
        (0, SetExtra(0, 0)),
        (0, AddShelf(1)),
        (0, Upgrade),
        (0, Sync),
        (0, SetTitle(0, 0)),
        (4, AddBook(0, 0, 2)),
        (2, AddShelf(0)),
        (0, MoveBookDeleteShelf(0, 0)),
        (0, AddShelf(0)),
        (0, MoveBookDeleteShelf(0, 0)),
        (0, MoveBookDeleteShelf(0, 0)),
        (1, Log(0, false)),
        (0, MoveBookDeleteShelf(0, 0)),
        (4, Deliver(17723131892693417154, false, false)),
        (0, AddShelf(0)),
        (0, MoveBookDeleteShelf(0, 0)),
        (1, DeleteBook(0)),
        (0, MoveBookDeleteShelf(0, 0)),
        (4, SetSetting(0, 0)),
        (0, AddTag(0, 0)),
        (4, Sync),
        (0, AddBook(0, 0, 0)),
        (0, AddShelf(0)),
        (1, Sync),
        (0, Deliver(16183820145487851227, false, false)),
        (0, DeleteTag(0)),
    ];
    run(488962404733629702, &script).unwrap();
}

/// Found by a long run with shelf names that refuse NULL. One replica renames a shelf to the name
/// of another shelf a book moved to, while elsewhere the first shelf is deleted. To write the
/// result the first shelf's row must go before the other is inserted (one name), the book must
/// leave it before it goes (a NOT NULL reference), and the other shelf must exist before the book
/// can move. That is a NOT NULL cycle, not a swap, and the same accepted limit as the swaps:
/// `plan::order` parks only by writing NULL, which no column of the cycle accepts, so the rows
/// wait for the application.
#[test]
#[ignore = "accepted limit: plan::order parks only by writing NULL; this NOT NULL cycle waits"]
fn a_shelf_renamed_onto_one_its_book_moved_to() {
    use Op::*;
    let schema: Vec<String> = SCHEMA
        .iter()
        .map(|sql| sql.replace("name TEXT UNIQUE", "name TEXT NOT NULL UNIQUE"))
        .collect();
    let schema: Vec<&str> = schema.iter().map(String::as_str).collect();
    let script = vec![
        (3, AddShelf(0)),
        (4, AddShelf(2)),
        (3, Sync),
        (1, Deliver(1279435572691482015, false, false)),
        (1, AddBook(0, 0, 0)),
        (1, Sync),
        (4, Deliver(9884803012250758789, false, false)),
        (4, MoveBookDeleteShelf(0, 0)),
        (1, RenameShelf(0, 2)),
    ];
    run_on(&schema, false, 0, 0, &script, None).unwrap();
}

/// A title written while suspended is sent on resuming, so writing it back afterwards is an edit
/// of its own.
#[test]
fn a_title_written_while_suspended_and_then_written_back_is_sent() {
    use Op::*;
    let script = vec![
        (2, AddShelf(0)),
        (2, AddBook(0, 0, 0)),
        (2, Sync),
        (1, Sync),
        (1, Suspended(0, 0)),
        (1, SetTitle(0, 0)),
    ];
    run(0, &script).unwrap();
}

/// A tag made on a book the same apply merges into another moves to the kept book's row: the
/// apply reports the row removed at its old key, so the tag is known as the application's write.
#[test]
fn a_natural_row_the_kernel_moves_to_another_key_is_reported_removed_at_the_old_one() {
    use Op::*;
    let script = vec![
        (3, AddShelf(1)),
        (3, AddBook(0, 1, 1)),
        (3, Sync),
        (3, SetIsbn(0, 0)),
        (4, AddAuthor(0)),
        (4, Sync),
        (4, AddBook(0, 0, 0)),
        (4, AddTag(1, 0)),
        (3, Sync),
        (4, Deliver(9274842016017877865, false, false)),
    ];
    run(267643275035851679, &script).unwrap();
}

/// A book whose new shelf has no row yet waits on its old shelf's row, which the plan removes: the
/// removal waits too, rather than taking the book and its tags with it by ON DELETE CASCADE.
#[test]
fn a_row_a_held_row_still_names_is_not_removed() {
    use Op::*;
    let script = vec![
        (1, AddShelf(0)),
        (4, AddShelf(0)),
        (1, Sync),
        (4, AddBook(0, 0, 3)),
        (3, Deliver(9599366878112, false, false)),
        (4, AddBook(0, 0, 1)),
        (4, AddBook(0, 0, 2)),
        (2, SetSetting(0, 0)),
        (1, AddBook(0, 0, 0)),
        (1, AddTag(0, 0)),
        (1, Sync),
        (1, DeleteShelf(0)),
        (4, Push),
        (2, Sync),
        (1, Deliver(8650076728648463482, false, false)),
        (3, AddBook(0, 0, 1)),
        (3, AddShelf(0)),
        (1, AddNote(0, 0)),
        (1, AddBook(0, 0, 0)),
        (4, DeleteShelf(0)),
        (1, AddMark(0, 0)),
        (1, AddNote(0, 0)),
        (3, Sync),
        (1, Sync),
        (4, Sync),
        (2, Deliver(3211387209849894402, false, false)),
    ];
    run(13014502829616582930, &script).unwrap();
}

/// A value an earlier collect recorded is carried by the next send, though the application has
/// since written the row again and that write waits: the value is not owed a second time.
#[test]
fn a_value_sent_while_its_row_waits_on_a_later_write_counts_as_sent() {
    use Op::*;
    let script = vec![
        (1, SetSetting(0, 0)),
        (1, AddAuthor(0)),
        (1, Sync),
        (4, AddShelf(0)),
        (2, AddShelf(1)),
        (4, AddBook(0, 0, 2)),
        (2, AddBook(0, 0, 3)),
        (2, Sync),
        (3, Deliver(1821843754366452306, false, false)),
        (3, SetIsbn(0, 2)),
        (2, DeleteShelf(0)),
        (4, Sync),
        (3, Deliver(12548034056379798152, false, false)),
        (2, Sync),
        (3, Log(1, false)),
        (3, SetTitle(0, 0)),
    ];
    run(9069292581118449834, &script).unwrap();
}

/// As `a_value_sent_while_its_row_waits_on_a_later_write_counts_as_sent`, for a reference: the
/// cell holds a local id and the change the entity that row stands for.
#[test]
fn a_reference_sent_while_its_row_waits_on_a_later_write_counts_as_sent() {
    use Op::*;
    let script = vec![
        (1, SetSetting(0, 0)),
        (3, AddShelf(0)),
        (1, AddAuthor(0)),
        (1, Sync),
        (0, AddShelf(0)),
        (4, AddShelf(0)),
        (2, AddShelf(1)),
        (4, AddBook(0, 0, 2)),
        (2, AddBook(0, 0, 3)),
        (2, Sync),
        (3, AddAuthor(0)),
        (0, AddShelf(0)),
        (0, Deliver(0, false, false)),
        (3, Deliver(1821843754366452306, false, false)),
        (0, AddShelf(0)),
        (3, SetIsbn(0, 2)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (4, Deliver(482915997399765560, false, false)),
        (3, SetAuthor(0, 0)),
        (2, DeleteShelf(0)),
        (4, Sync),
        (3, Deliver(12548007102727212835, false, false)),
        (2, Sync),
        (3, Log(1, false)),
        (3, SetTitle(0, 0)),
    ];
    run(9069292581118449834, &script).unwrap();
}

/// Found at 4,000 cases with seed 20261119. The application changes a book's author while the
/// book's row names a shelf row a merge gave up, so collect leaves the row waiting; the next apply
/// rewrites the row onto the kept shelf and its author with it, and the write is lost. Refusing
/// that rewrite instead deadlocks: the row waits on the shelf, whose removal waits on the row.
/// Collect would have to record a row that names a given-up row as the entity it stood for.
#[test]
fn an_uncollected_write_to_a_row_naming_a_given_up_row_is_kept() {
    use Op::*;
    let script = vec![
        (1, SetSetting(0, 0)),
        (3, AddShelf(0)),
        (1, AddAuthor(0)),
        (1, Sync),
        (0, AddShelf(0)),
        (4, AddShelf(0)),
        (2, AddShelf(1)),
        (4, AddBook(0, 0, 2)),
        (2, AddBook(0, 0, 3)),
        (2, Sync),
        (3, AddAuthor(0)),
        (0, AddShelf(0)),
        (0, Deliver(0, false, false)),
        (3, Deliver(1821843754366452306, false, false)),
        (0, AddShelf(0)),
        (3, SetIsbn(0, 2)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (4, Deliver(482915997399765560, false, false)),
        (4, SetAuthor(5, 0)),
        (2, DeleteShelf(0)),
        (4, Sync),
        (3, Deliver(12548034056711065972, false, false)),
        (2, Sync),
        (3, Log(1, false)),
        (3, SetAuthor(0, 0)),
    ];
    run(9069292581118449834, &script).unwrap();
}

/// Found at 4,000 cases with seed 20261121: a settled replica keeps a given-up shelf row and an
/// uncollected book. Not investigated; likely the same as the case above.
#[test]
fn a_book_naming_a_given_up_shelf_settles_to_the_model() {
    use Op::*;
    let script = vec![
        (4, SetSetting(0, 0)),
        (0, AddShelf(1)),
        (3, AddAuthor(0)),
        (3, AddShelf(0)),
        (4, AddShelf(0)),
        (0, SetSetting(0, 0)),
        (4, Sync),
        (0, Sync),
        (3, Log(0, false)),
        (4, Sync),
        (0, AddCover(0)),
        (0, AddShelf(0)),
        (4, AddBook(1, 0, 3)),
        (3, AddBook(0, 1, 0)),
        (4, AddNote(0, 0)),
        (3, Sync),
        (3, SetIsbn(0, 3)),
        (3, DeleteShelf(1)),
        (3, SetAuthor(0, 0)),
        (3, AddBook(0, 0, 1)),
        (0, AddShelf(0)),
        (3, SetSetting(0, 1)),
        (0, AddShelf(0)),
        (3, AddNote(0, 0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (4, AddBook(2, 0, 0)),
        (3, SetTitle(2, 0)),
        (2, Sync),
        (4, SetIsbn(5, 1)),
        (4, DeleteSetting(0)),
        (3, AddNote(2, 0)),
        (2, SetSetting(1, 0)),
        (3, Suspended(0, 0)),
        (3, SetTitle(0, 0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (2, DeleteBook(0)),
        (3, SetAuthor(1, 0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (3, Sync),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (2, Sync),
        (4, Sync),
        (3, Deliver(10212369816065641680, false, false)),
        (0, AddShelf(0)),
        (2, Log(0, false)),
        (2, DeleteShelf(0)),
        (2, Sync),
        (3, Deliver(3224780953315842759, false, false)),
        (3, AddBook(0, 0, 2)),
    ];
    run(27569572497282000, &script).unwrap();
}

/// Found at 4,000 cases with seed 20261119: a book row a merge gave up, kept because a note
/// still names it, holds a value for a field an upgrade then adopts. The row stands for the
/// entity it stood for, so the adoption records the value for that entity.
#[test]
fn a_field_adopted_from_a_given_up_row_is_sent() {
    use Op::*;
    let script = vec![
        (1, AddShelf(0)),
        (1, AddBook(0, 0, 3)),
        (1, Sync),
        (1, AddTag(0, 1)),
        (4, AddShelf(1)),
        (0, Deliver(1488990945943347925, false, false)),
        (1, AddNote(0, 0)),
        (4, Sync),
        (0, AddShelf(2)),
        (0, SetStatus(0, 2)),
        (4, AddTag(0, 0)),
        (4, SetStatus(0, 2)),
        (1, SetNote(0, 2)),
        (4, Sync),
        (1, Sync),
        (0, AddAuthor(0)),
        (0, Deliver(629250560763980496, false, false)),
        (1, AddNote(0, 0)),
        (0, AddBook(0, 0, 1)),
        (1, SetNote(0, 1)),
        (0, AddBook(1, 0, 0)),
        (0, MoveNoteDeleteShelf(0, 2)),
        (1, SetExtra(0, 0)),
        (0, Sync),
        (1, Deliver(15365404531894706616, false, false)),
    ];
    run(0, &script).unwrap();
}

/// Found by a long run. A book takes an isbn that a member of another book's group once held.
/// The collect that mints the new book's entity merges it into that group and removes its row,
/// so the entity's creation, which the application wrote, is sent while the group's row names it.
#[test]
fn a_new_book_whose_collect_merges_it_into_another_row() {
    use Op::*;
    let script = vec![
        (0, AddShelf(0)),
        (3, AddShelf(1)),
        (3, Suspended(0, 0)),
        (3, AddBook(0, 0, 2)),
        (3, Sync),
        (0, AddBook(0, 0, 0)),
        (0, Deliver(10397343180266276659, false, false)),
        (0, SetIsbn(0, 1)),
        (0, Sync),
        (0, DeleteShelf(5)),
        (0, AddBook(0, 0, 0)),
        (0, Sync),
    ];
    run(2777986060137981915, &script).unwrap();
}

/// Found by a long run. Peer 1 deletes its book and makes another at the same local id on a shelf
/// row a merge gave up; the collect sends the new book's entity and the step removes its row, so
/// only the key it was made at, which the deleted book's entity also held, names the write.
#[test]
fn a_new_book_at_a_reused_id_whose_row_goes_in_the_same_step() {
    use Op::*;
    let script = vec![
        (0, AddAuthor(0)),
        (3, AddShelf(0)),
        (1, AddAuthor(0)),
        (3, Sync),
        (2, AddShelf(1)),
        (0, DeleteSetting(0)),
        (0, AddShelf(0)),
        (0, AddBook(0, 0, 2)),
        (2, Sync),
        (2, AddShelf(0)),
        (0, AddBook(0, 0, 2)),
        (0, MoveBookDeleteShelf(0, 0)),
        (0, AddNote(0, 0)),
        (1, Upgrade),
        (1, DeleteAuthor(0)),
        (4, AddAuthor(0)),
        (0, SetTitle(0, 0)),
        (4, Sync),
        (1, Deliver(2, false, false)),
        (4, AddBook(0, 0, 0)),
        (3, AddAuthor(0)),
        (1, AddBook(0, 0, 0)),
        (1, Sync),
        (0, AddAuthor(0)),
        (3, DeleteShelf(0)),
        (0, AddNote(0, 0)),
        (4, SetIsbn(0, 1)),
        (3, Sync),
        (2, AddShelf(0)),
        (0, Sync),
        (0, AddNote(0, 0)),
        (0, AddMark(0, 0)),
        (1, MoveBookDeleteShelf(0, 0)),
        (0, AddShelf(0)),
        (1, MoveBookDeleteShelf(0, 0)),
        (1, AddShelf(0)),
        (0, SetTitle(0, 0)),
        (0, SetTitle(0, 1)),
        (4, Log(3, false)),
        (1, MoveBookDeleteShelf(0, 0)),
        (1, AddShelf(0)),
        (0, SetIsbn(0, 2)),
        (4, Deliver(2034325994777331782, false, false)),
        (1, MoveBookDeleteShelf(0, 0)),
        (1, MoveBookDeleteShelf(0, 0)),
        (0, AddAuthor(0)),
        (1, MoveBookDeleteShelf(0, 0)),
        (0, SetStatus(0, 1)),
        (1, MoveBookDeleteShelf(0, 0)),
        (1, AddBook(0, 0, 0)),
        (0, AddMark(0, 0)),
        (2, AddShelf(0)),
        (0, AddNote(0, 0)),
        (0, Sync),
        (2, AddShelf(0)),
        (0, AddShelf(0)),
        (4, Sync),
        (2, AddShelf(0)),
        (1, Deliver(13447369747061139288, false, false)),
        (1, DeleteBook(0)),
        (1, AddBook(2, 0, 0)),
    ];
    run(4378100637463862824, &script).unwrap();
}

/// Found by a long run (seed 31337). Peer 4 writes a title while suspended, with a shelf deletion
/// not yet collected. Resuming must mark the title before the install's step brings the book to
/// its group's target, or the write is neither sent nor kept.
#[test]
fn a_title_written_while_suspended_as_its_book_merges() {
    use Op::*;
    let script = vec![
        (4, AddShelf(0)),
        (4, Sync),
        (2, AddShelf(0)),
        (4, AddShelf(2)),
        (1, AddShelf(2)),
        (0, AddAuthor(0)),
        (0, AddAuthor(0)),
        (2, AddShelf(2)),
        (0, AddShelf(0)),
        (3, Sync),
        (1, AddBook(0, 0, 2)),
        (4, AddBook(2, 0, 1)),
        (2, DeleteShelf(0)),
        (2, Sync),
        (2, SetTitle(0, 0)),
        (2, SetTitle(0, 0)),
        (2, SetTitle(0, 0)),
        (4, Suspended(0, 0)),
        (2, SetTitle(0, 0)),
        (2, SetTitle(0, 0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (1, AddAuthor(0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (0, Deliver(0, false, false)),
        (1, AddBook(0, 0, 1)),
        (1, AddBook(0, 0, 0)),
        (0, Deliver(0, false, false)),
        (0, AddShelf(0)),
        (0, Deliver(0, false, false)),
        (0, AddShelf(0)),
        (1, Sync),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (4, Suspended(0, 2)),
        (0, AddShelf(0)),
        (4, Sync),
        (4, DeleteShelf(2)),
        (4, Suspended(2, 0)),
    ];
    run(0, &script).unwrap();
}

/// Found by a long run (seed 31337). A book and a tag are made again at the local ids and key of
/// ones removed; the tag's key names the new book, so it is a new entity and its NULL note is
/// not owed.
#[test]
fn a_tag_made_again_with_a_null_note_at_seed_31337() {
    use Op::*;
    let script = vec![
        (0, AddShelf(0)),
        (4, SetSetting(0, 0)),
        (0, Sync),
        (2, Deliver(1567813854110382274, false, false)),
        (2, AddBook(0, 0, 2)),
        (2, SetTagNote(0, 0)),
        (3, AddShelf(0)),
        (2, AddTag(0, 1)),
        (1, AddShelf(0)),
        (0, SetTitle(0, 0)),
        (2, SetTagNote(0, 0)),
        (1, AddBook(0, 0, 0)),
        (0, DeleteSetting(0)),
        (0, AddBook(0, 0, 0)),
        (2, MoveBookDeleteShelf(0, 0)),
        (4, Sync),
        (1, Sync),
        (0, Deliver(5598430481767046809, false, false)),
        (1, AddShelf(0)),
        (0, Sync),
        (2, Sync),
        (3, Deliver(774416090256426437, false, false)),
        (3, DeleteShelf(5)),
        (0, AddShelf(0)),
        (3, AddBook(0, 0, 0)),
        (3, AddTag(0, 1)),
    ];
    run(8729899457635566058, &script).unwrap();
}

/// Found by a long run (seed 271828). A book and a tag are made again at the local ids and key of
/// ones removed; the tag's key names the new book, so it is a new entity and its NULL note is
/// not owed.
#[test]
fn a_tag_made_again_with_a_null_note_at_seed_271828() {
    use Op::*;
    let script = vec![
        (2, AddShelf(0)),
        (2, AddBook(0, 0, 0)),
        (0, RenameShelf(0, 0)),
        (0, RenameShelf(0, 0)),
        (2, AddTag(0, 0)),
        (2, Sync),
        (0, Deliver(9791640996397213198, false, false)),
        (0, SetTagNote(0, 0)),
        (0, Suspended(0, 0)),
        (0, DeleteShelf(0)),
        (0, AddShelf(0)),
        (0, AddBook(0, 0, 0)),
        (0, AddTag(0, 0)),
    ];
    run(0, &script).unwrap();
}

/// Found by a long run (seed 99). Peer 3 collects a row at a time while its application writes
/// between steps: an author's favourite is set and, before a step reaches the author, set back
/// by removing the book. Collected together, the two writes are none, as between two collects.
#[test]
fn a_reference_written_between_steps_and_back_sends_nothing() {
    use Op::*;
    let script = vec![
        (0, AddShelf(0)),
        (1, AddCover(0)),
        (0, AddShelf(1)),
        (0, AddBook(0, 1, 1)),
        (1, Sync),
        (0, Upgrade),
        (0, AddAuthor(0)),
        (0, AddCover(0)),
        (0, SetTitle(0, 0)),
        (0, Sync),
        (1, AddBook(0, 0, 0)),
        (0, DeleteAuthor(0)),
        (1, AddTag(0, 0)),
        (0, SetTitle(0, 0)),
        (3, SetNote(0, 0)),
        (0, DeleteShelf(0)),
        (0, Deliver(0, false, false)),
        (0, DeleteShelf(0)),
        (0, RenameShelf(0, 0)),
        (0, DeleteAuthor(0)),
        (0, AddShelf(0)),
        (0, SetTitle(0, 0)),
        (1, AddAuthor(0)),
        (2, AddShelf(0)),
        (0, AddShelf(0)),
        (1, Sync),
        (0, AddAuthor(0)),
        (0, SetFavourite(0, 0)),
        (1, Sync),
        (3, Sync),
        (0, SetTitle(0, 0)),
        (0, SetTitle(0, 0)),
        (0, Sync),
        (0, Upgrade),
        (0, AddBook(2, 0, 3)),
        (1, Sync),
        (0, Sync),
        (4, Log(4, true)),
        (3, AddNote(0, 0)),
        (4, AddTag(4, 0)),
        (2, AddNote(3, 1)),
        (4, Deliver(17504071961584740370, false, true)),
        (1, SetStatus(4, 2)),
        (3, SetStatus(5, 3)),
        (1, Push),
        (4, AddTag(4, 0)),
        (3, SetTitle(3, 2)),
        (3, Sync),
        (2, DeleteAuthor(4)),
        (2, AddMark(4, 1)),
        (0, AddAuthor(2)),
        (0, SetNote(3, 0)),
        (1, DeleteShelf(4)),
        (2, Sync),
        (0, Sync),
        (4, SetNote(2, 0)),
        (0, SetFavourite(4, 4)),
        (2, AddNote(4, 2)),
        (1, Upgrade),
        (0, AddNote(1, 0)),
        (2, AddBook(2, 2, 3)),
        (4, AddNote(0, 0)),
        (0, SetSetting(0, 2)),
        (0, AddMark(0, 1)),
        (0, AddNote(4, 1)),
        (0, Push),
        (1, SetAuthor(2, 5)),
        (4, SetNote(0, 2)),
        (1, DeleteBook(0)),
        (4, Deliver(16045125935950652839, false, true)),
        (3, DeleteShelf(4)),
        (1, AddBook(2, 1, 3)),
        (3, AddBook(4, 2, 1)),
        (1, SetStatus(5, 2)),
        (4, AddNote(4, 2)),
        (1, DeleteBook(4)),
        (4, SetFavourite(2, 2)),
        (0, SetFavourite(3, 5)),
        (4, DeleteMark(3)),
        (0, AddBook(5, 0, 3)),
        (3, SetIsbn(2, 3)),
        (1, AddBook(5, 2, 2)),
        (4, Push),
        (3, Deliver(7089252144508200401, false, true)),
        (3, AddNote(2, 2)),
        (3, RenameShelf(4, 1)),
        (4, DeleteBook(2)),
        (3, SetFavourite(5, 4)),
        (3, AddBook(1, 0, 1)),
    ];
    run_found(9882191972837468972, &script).unwrap();
}

/// The case behind the next test, by itself: a replica makes two series with one code, and an
/// upgrade's install or a collect finds them to be one and removes a row; another replica's series
/// then arrives, before or after the next collect, and may take the removed row's key. Both
/// replicas end with one series of each code, holding what was written.
#[tokio::test]
async fn two_series_made_with_one_code_reach_another_replica_as_one() {
    for upgrade in [true, false] {
        for deliver_first in [true, false] {
            let world = World::new(1);
            let mut a = world.connect(2).await;
            let mut c = world.connect(0).await;
            for sql in SCHEMA {
                sqlx::query(sql).execute(&mut a).await.unwrap();
                sqlx::query(sql).execute(&mut c).await.unwrap();
            }
            let (before, after) = (Replica::new(registry(false)), Replica::new(registry(true)));
            before.install(&mut a).await.unwrap();
            before.install(&mut c).await.unwrap();
            for _ in 0..2 {
                world.later(10);
                sqlx::query("INSERT INTO series (code, name) VALUES ('c1', 'name1')")
                    .execute(&mut a)
                    .await
                    .unwrap();
            }
            world.later(10);
            let on_a = if upgrade {
                after.install(&mut a).await.unwrap();
                &after
            } else {
                before.collect(&mut a).await.unwrap();
                &before
            };
            world.later(10);
            sqlx::query("INSERT INTO series (code, name) VALUES ('c0', 'name1')")
                .execute(&mut c)
                .await
                .unwrap();
            before.collect(&mut c).await.unwrap();
            let from_c = before.outbox(&mut c, u32::MAX).await.unwrap().changes;
            if deliver_first {
                on_a.apply(&mut a, &from_c).await.unwrap();
            }
            on_a.collect(&mut a).await.unwrap();
            let from_a = on_a.outbox(&mut a, u32::MAX).await.unwrap().changes;
            if !deliver_first {
                on_a.apply(&mut a, &from_c).await.unwrap();
            }
            before.apply(&mut c, &from_a).await.unwrap();
            for conn in [&mut a, &mut c] {
                let held =
                    support::rows(conn, "SELECT code, name FROM series ORDER BY code", &[]).await;
                assert_eq!(
                    format!("{held:?}"),
                    r#"[[Text("c0"), Text("name1")], [Text("c1"), Text("name1")]]"#,
                    "upgrade {upgrade}, delivered first {deliver_first}"
                );
            }
        }
    }
}

/// Found by a long run once series, an identity no index enforces, joined the test. An upgrade
/// install collects two series a replica made with one code and merges them, removing one row;
/// a delivery then gives that row's key to another series. What the install sent carries the
/// application's writes; this checks the test's own account of them, with the replicas
/// collecting in the steps the world picks and with every replica collecting whole.
#[test]
fn series_found_to_be_one_on_one_replica_keep_its_writes() {
    use Op::*;
    let script = vec![
        (2, AddShelf(2)),
        (4, AddBook(3, 1, 1)),
        (3, Log(3, true)),
        (4, SetSeriesCode(5, 0)),
        (3, AddShelf(1)),
        (1, AddCover(4)),
        (1, SetIsbn(1, 3)),
        (4, SetTitle(4, 2)),
        (4, SetIsbn(4, 0)),
        (4, Log(3, true)),
        (0, DeleteShelf(4)),
        (0, AddNote(5, 2)),
        (1, AddBook(1, 2, 2)),
        (1, AddCover(3)),
        (0, SetFavourite(4, 5)),
        (2, AddSeries(1, 1)),
        (4, Deliver(12070878064311819280, false, true)),
        (2, AddMark(1, 2)),
        (2, AddTag(1, 0)),
        (3, SetNote(1, 0)),
        (2, AddSeries(1, 1)),
        (3, SetTagNote(4, 2)),
        (4, AddTag(1, 0)),
        (4, AddBook(1, 0, 1)),
        (1, RenameShelf(1, 2)),
        (4, SetSeriesCode(1, 1)),
        (4, AddBook(5, 0, 3)),
        (0, Deliver(18220861876318342081, true, false)),
        (3, Upgrade),
        (1, AddAuthor(1)),
        (0, Upgrade),
        (2, Upgrade),
        (1, Sync),
        (4, AddBook(4, 0, 3)),
        (3, AddCover(0)),
        (2, Deliver(14353264754466172533, false, false)),
        (4, Upgrade),
        (0, SetAuthor(0, 5)),
        (3, AddShelf(1)),
        (4, AddNote(2, 2)),
        (0, AddSeries(0, 1)),
        (2, SetIsbn(3, 0)),
        (1, AddCover(3)),
        (2, Deliver(14648138576390885823, false, false)),
        (3, Push),
        (4, AddMark(5, 0)),
        (3, DeleteShelf(3)),
        (3, SetNote(0, 2)),
        (2, SetStatus(5, 3)),
        (1, SetStatus(5, 2)),
        (3, DeleteNote(0)),
        (0, AddBook(4, 1, 1)),
        (4, RenameShelf(3, 2)),
        (1, AddBook(2, 1, 3)),
        (4, MoveBookDeleteShelf(1, 3)),
        (3, Sync),
        (0, Suspended(3, 2)),
        (0, DeleteTag(3)),
        (1, AddCover(1)),
        (2, AddTag(2, 0)),
        (2, Log(4, true)),
        (1, AddBook(0, 0, 2)),
        (2, AddShelf(1)),
        (2, AddBook(0, 1, 1)),
        (0, AddNote(4, 1)),
        (2, AddBook(2, 1, 3)),
        (4, SetFavourite(0, 3)),
        (4, AddTag(2, 0)),
        (2, AddMark(2, 1)),
        (1, AddSeries(1, 2)),
        (4, MoveMarkDeleteBook(2, 4)),
        (2, Log(0, true)),
        (3, Deliver(17844019385685927557, true, true)),
        (0, Push),
        (3, Deliver(9135296489156821608, false, false)),
        (1, DeleteAuthor(0)),
        (2, Deliver(14972710878109861610, false, true)),
        (3, SetExtra(0, 1)),
        (0, AddMark(1, 1)),
        (3, SetIsbn(5, 1)),
        (2, AddMark(1, 1)),
        (1, Sync),
        (0, AddSeries(1, 1)),
        (1, AddShelf(1)),
        (3, AddVolume(2, 2)),
        (1, DeleteMark(4)),
        (0, AddAuthor(1)),
        (2, MoveMarkDeleteBook(1, 2)),
        (3, MoveBookDeleteShelf(2, 0)),
        (1, SetTitle(3, 1)),
        (0, SetExtra(5, 0)),
        (0, AddNote(1, 0)),
        (1, SetAuthor(3, 5)),
        (4, AddCover(1)),
        (0, AddSeries(0, 2)),
        (4, AddShelf(1)),
        (2, RenameShelf(4, 0)),
        (4, AddSeries(0, 2)),
        (1, Log(2, false)),
        (2, MoveNoteDeleteShelf(2, 2)),
        (4, SetSetting(0, 2)),
        (4, AddNote(4, 1)),
        (2, DeleteAuthor(3)),
        (4, AddBook(4, 1, 1)),
        (1, RenameShelf(4, 2)),
        (1, AddMark(1, 2)),
        (2, AddCover(1)),
        (0, AddSeries(0, 1)),
        (3, AddMark(1, 2)),
        (1, AddTag(1, 1)),
        (4, AddTag(0, 1)),
        (0, Log(4, true)),
        (1, AddSeries(1, 1)),
        (2, AddBook(3, 0, 0)),
        (3, SetNote(3, 0)),
        (4, Deliver(15985184485851326179, true, true)),
        (3, AddBook(4, 0, 0)),
        (3, MoveNoteDeleteShelf(0, 1)),
        (3, AddCover(0)),
        (1, DeleteBook(0)),
        (1, AddBook(0, 1, 1)),
        (2, AddVolume(1, 0)),
    ];
    run(5694626446472720197, &script).unwrap();
    run_on(SCHEMA, false, 5694626446472720197, 0, &script, Some(None)).unwrap();
}
/// Found at 500 cases once series joined the test. A series the replica received is found to be
/// one with another of its own and its row is removed; two new series then share one code, the
/// first at the removed row's key, and the push's collect merges that one away too. What it wrote
/// is carried under the entity minted for it, which the test never saw at that key.
#[test]
fn a_series_made_at_a_key_a_merge_freed_and_merged_away_again_is_sent() {
    use Op::*;
    let script = vec![
        (0, AddShelf(0)),
        (0, SetTitle(0, 0)),
        (0, AddShelf(0)),
        (0, SetTitle(0, 0)),
        (0, AddShelf(0)),
        (0, AddSeries(1, 0)),
        (0, Sync),
        (0, RenameShelf(0, 0)),
        (0, AddSeries(0, 0)),
        (0, AddShelf(0)),
        (2, AddAuthor(0)),
        (0, DeleteShelf(0)),
        (0, AddShelf(0)),
        (0, Sync),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (0, AddShelf(0)),
        (2, Deliver(99234427557325516, false, false)),
        (2, SetSeriesCode(2, 0)),
        (2, AddVolume(0, 0)),
        (2, AddShelf(1)),
        (2, AddVolume(0, 0)),
        (2, Suspended(0, 0)),
        (0, AddShelf(0)),
        (2, AddSeries(1, 0)),
        (2, AddSeries(1, 0)),
    ];
    run(6847074269335888592, &script).unwrap();
    run_on(SCHEMA, false, 6847074269335888592, 0, &script, Some(None)).unwrap();
}
/// Found at 400 cases with seed 424242. Two series with one code and a volume on the second;
/// resuming collects them whole and merges the second away, and a new series then takes its id.
/// The volume's reference is carried as the entity minted for the merged row, which the test
/// never saw at that id.
#[test]
fn a_volume_naming_a_series_merged_away_at_a_resume_is_sent() {
    use Op::*;
    let script = vec![
        (1, AddSeries(1, 0)),
        (1, AddShelf(2)),
        (1, AddShelf(0)),
        (1, AddSeries(1, 0)),
        (0, Deliver(0, false, true)),
        (1, AddVolume(5, 0)),
        (1, Suspended(0, 0)),
        (1, AddSeries(0, 0)),
    ];
    run(1416172913542871530, &script).unwrap();
    run_on(SCHEMA, false, 1416172913542871530, 0, &script, Some(None)).unwrap();
}
/// Found at 400 cases with seed 99. A book found to be one with a delivered book, and a note on
/// it, collected in steps: the note names the book's own entity, where one collect names the
/// entity it joins. Both name one group; the test compares references by the group they name.
#[test]
fn a_note_on_a_book_found_to_be_one_names_its_group_in_steps() {
    use Op::*;
    let script = vec![
        (1, AddShelf(1)),
        (1, Sync),
        (0, AddShelf(0)),
        (3, Deliver(0, false, false)),
        (0, AddShelf(0)),
        (0, Sync),
        (2, AddBook(0, 0, 1)),
        (2, Deliver(11898942371205919333, false, false)),
        (2, AddBook(0, 0, 0)),
        (2, MoveBookDeleteShelf(1, 0)),
        (2, AddNote(2, 0)),
        (2, Sync),
    ];
    run_found(3778721728584034636, &script).unwrap();
}

/// Found at 128 cases: rows found at a first install made in steps are adopted by the steps
/// after it, whose adoptions carry the adoption clock; the test's account took only what the
/// install call itself sent for adoptions.
#[test]
fn rows_a_first_install_in_steps_adopts_after_it_are_its_adoptions() {
    run_found(8319302663633078794, &[]).unwrap();
}

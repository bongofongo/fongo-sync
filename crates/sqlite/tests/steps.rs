//! Collecting in bounded steps records what one collect of the same rows records, in every case
//! tried here. The same world is built again for each step size, from one seed, so every run
//! reaches the collect under test in the same state; that collect is then made whole, or in steps
//! of a few rows until none are left, and the outcomes are compared: the changes sent, what the
//! replica holds, its rows and images, and what another replica shows once it applies them.
//! Worlds are run on clocks that move on most readings or only now and then, and with the writes
//! under test in one millisecond or spread out.
//!
//! Gids and clocks are compared exactly, but where a step aliases entities found to be one: an
//! alias takes the clock of the step that found it, so later clocks move on. Then the changes are
//! compared in clock order, aliases as a set, and registers by value.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_sqlite::core::{ALIAS, Change, Gid, Value};
use fongo_sync_sqlite::{COLLECT_ROWS, Collection, Inspection, LocalDefault, Registry, Replica};
use proptest::prelude::*;
use proptest::test_runner::RngSeed;
use sqlx::SqliteConnection;
use support::world::World;

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
    "CREATE TABLE settings (key TEXT NOT NULL PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE covers (
        book_id INTEGER NOT NULL UNIQUE REFERENCES books(id) ON DELETE CASCADE,
        path TEXT)",
    // An identity no index enforces, so a collect can find two of its own rows to be one.
    "CREATE TABLE series (id INTEGER PRIMARY KEY, code TEXT, name TEXT)",
    "CREATE TABLE volumes (
        id INTEGER PRIMARY KEY,
        series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
        title TEXT)",
];

const TABLES: &[&str] = &[
    "shelves", "authors", "books", "notes", "marks", "tags", "settings", "covers", "series",
    "volumes",
];

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
        Collection::minted("series", "series", "id")
            .field("code")
            .field("name")
            .identity(&["code"]),
        Collection::minted("volumes", "volumes", "id")
            .owner("series_id", "series")
            .field("title"),
    ])
    .unwrap()
}

/// A row of `table` chosen by `n`, as SQL.
fn pick(table: &str, n: u8) -> String {
    format!(
        "(SELECT rowid FROM {table} ORDER BY rowid LIMIT 1
          OFFSET {n} % max(1, (SELECT count(*) FROM {table})))"
    )
}

/// One write of the application, of a kind chosen by `kind`, with arguments `a` and `b`.
fn write(kind: u8, a: u8, b: u8) -> Vec<String> {
    let book = pick("books", a);
    let isbn = |v: u8| match v % 4 {
        3 => "NULL".to_string(),
        v => format!("'i{}'", v % 2),
    };
    let one = |s: String| vec![s];
    match kind % 36 {
        0 => one(format!(
            "INSERT OR IGNORE INTO shelves (name) VALUES ('shelf{}')",
            b % 3
        )),
        1 => one(format!(
            "UPDATE OR IGNORE shelves SET name = 'shelf{}' WHERE id = {}",
            b % 3,
            pick("shelves", a)
        )),
        2 => one(format!("DELETE FROM shelves WHERE id = {}", pick("shelves", a))),
        3 => one(format!(
            "INSERT OR IGNORE INTO books (shelf_id, title, isbn, seen)
             SELECT {}, 'title{}', {}, 0 WHERE EXISTS (SELECT 1 FROM shelves)",
            pick("shelves", a),
            b % 3,
            isbn(b / 3)
        )),
        4 => one(format!(
            "UPDATE books SET title = 'title{}' WHERE id = {book}",
            b % 3
        )),
        5 => one(format!(
            "UPDATE OR IGNORE books SET isbn = {} WHERE id = {book}",
            isbn(b)
        )),
        6 => one(match b % 3 {
            0 => format!("UPDATE books SET status = 'reading', finished = NULL WHERE id = {book}"),
            1 => format!("UPDATE books SET status = NULL, finished = NULL WHERE id = {book}"),
            _ => format!("UPDATE books SET status = 'done', finished = {b} WHERE id = {book}"),
        }),
        7 => one(format!(
            "UPDATE books SET author_id = {} WHERE id = {book}",
            pick("authors", b)
        )),
        8 => one(format!("DELETE FROM books WHERE id = {book}")),
        // A delete and a new row at the same id.
        9 => vec![
            "CREATE TEMP TABLE IF NOT EXISTS chosen (id INTEGER)".into(),
            "DELETE FROM temp.chosen".into(),
            format!("INSERT INTO temp.chosen SELECT {book}"),
            "DELETE FROM books WHERE id = (SELECT id FROM temp.chosen)".into(),
            format!(
                "INSERT OR IGNORE INTO books (id, shelf_id, title, isbn, seen)
                 SELECT id, (SELECT min(id) FROM shelves), 'again{}', {}, 0 FROM temp.chosen
                  WHERE id IS NOT NULL AND EXISTS (SELECT 1 FROM shelves)",
                b % 2,
                isbn(b)
            ),
        ],
        10 => one(format!(
            "INSERT INTO authors (name) VALUES ('author{}')",
            b % 3
        )),
        11 => one(format!("DELETE FROM authors WHERE id = {}", pick("authors", a))),
        12 => one(format!(
            "UPDATE authors SET favourite = {book} WHERE id = {}",
            pick("authors", b)
        )),
        // A row that names one written after it: the author is written again later.
        13 => vec![
            format!("INSERT INTO authors (name) VALUES ('fresh{}')", b % 3),
            format!("UPDATE books SET author_id = (SELECT max(id) FROM authors) WHERE id = {book}"),
            "UPDATE authors SET name = name || '!' WHERE id = (SELECT max(id) FROM authors)".into(),
        ],
        14 => one(format!(
            "INSERT INTO notes (book_id, body) SELECT {book}, 'body{}'
              WHERE EXISTS (SELECT 1 FROM books)",
            b % 3
        )),
        15 => one(format!(
            "UPDATE notes SET body = 'body{}' WHERE id = {}",
            b % 3,
            pick("notes", a)
        )),
        16 => one(format!("DELETE FROM notes WHERE id = {}", pick("notes", a))),
        // A note moved to another book, and the book it left removed.
        17 => vec![
            "CREATE TEMP TABLE IF NOT EXISTS chosen (id INTEGER)".into(),
            "DELETE FROM temp.chosen".into(),
            format!(
                "INSERT INTO temp.chosen SELECT book_id FROM notes WHERE id = {}",
                pick("notes", a)
            ),
            format!(
                "UPDATE notes SET book_id = {} WHERE id = {} AND {} <> (SELECT id FROM temp.chosen)",
                pick("books", b),
                pick("notes", a),
                pick("books", b)
            ),
            "DELETE FROM books WHERE id = (SELECT id FROM temp.chosen)
               AND NOT EXISTS (SELECT 1 FROM notes WHERE id IN (SELECT id FROM temp.chosen))"
                .into(),
        ],
        18 => one(format!(
            "INSERT OR IGNORE INTO marks (note_id, place) SELECT {}, {}
              WHERE EXISTS (SELECT 1 FROM notes)",
            pick("notes", a),
            b % 3
        )),
        19 => one(format!("DELETE FROM marks WHERE id = {}", pick("marks", a))),
        20 => one(format!(
            "INSERT OR IGNORE INTO tags (book_id, tag) SELECT {book}, 'tag{}'
              WHERE EXISTS (SELECT 1 FROM books)",
            b % 2
        )),
        21 => one(format!(
            "UPDATE tags SET note = 'note{}' WHERE rowid = {}",
            b % 3,
            pick("tags", a)
        )),
        22 => one(format!("DELETE FROM tags WHERE rowid = {}", pick("tags", a))),
        // A tag removed and made again at its key.
        23 => vec![
            "CREATE TEMP TABLE IF NOT EXISTS tag (book_id INTEGER, tag TEXT)".into(),
            "DELETE FROM temp.tag".into(),
            format!(
                "INSERT INTO temp.tag SELECT book_id, tag FROM tags WHERE rowid = {}",
                pick("tags", a)
            ),
            "DELETE FROM tags WHERE (book_id, tag) IN (SELECT book_id, tag FROM temp.tag)".into(),
            format!(
                "INSERT INTO tags (book_id, tag, note) SELECT book_id, tag, {} FROM temp.tag",
                if b.is_multiple_of(2) { "NULL" } else { "'again'" }
            ),
        ],
        24 => one(format!(
            "INSERT INTO settings VALUES ('key{}', 'value{}')
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            a % 2,
            b % 3
        )),
        25 => one(format!("DELETE FROM settings WHERE key = 'key{}'", a % 2)),
        // A parent and its children, made together.
        26 => vec![
            format!(
                "INSERT OR IGNORE INTO books (shelf_id, title, isbn, seen)
                 SELECT {}, 'family{}', {}, 0 WHERE EXISTS (SELECT 1 FROM shelves)",
                pick("shelves", a),
                b % 2,
                isbn(b)
            ),
            "INSERT INTO notes (book_id, body) SELECT max(id), 'first' FROM books HAVING max(id) IS NOT NULL".into(),
            "INSERT INTO notes (book_id, body) SELECT max(id), 'second' FROM books HAVING max(id) IS NOT NULL".into(),
            "INSERT OR IGNORE INTO marks (note_id, place) SELECT max(id), 1 FROM notes HAVING max(id) IS NOT NULL".into(),
            "INSERT OR IGNORE INTO tags (book_id, tag) SELECT max(id), 'new' FROM books HAVING max(id) IS NOT NULL".into(),
        ],
        // A parent removed with its children.
        27 => one(format!(
            "DELETE FROM books WHERE id = (SELECT book_id FROM notes WHERE id = {})",
            pick("notes", a)
        )),
        28 => one(format!(
            "INSERT OR IGNORE INTO covers (book_id, path) SELECT {book}, 'c'
              WHERE EXISTS (SELECT 1 FROM books)"
        )),
        29 => one(format!(
            "UPDATE books SET extra = 'x{}' WHERE id = {book}",
            b % 3
        )),
        30 => one(format!(
            "INSERT INTO series (code, name) VALUES ('c{}', 'series{}')",
            b % 2,
            b % 3
        )),
        31 => one(format!(
            "UPDATE series SET code = 'c{}' WHERE id = {}",
            b % 2,
            pick("series", a)
        )),
        32 => one(format!(
            "UPDATE series SET name = 'name{}' WHERE id = {}",
            b % 3,
            pick("series", a)
        )),
        33 => one(format!(
            "INSERT INTO volumes (series_id, title) SELECT {}, 'v{}'
              WHERE EXISTS (SELECT 1 FROM series)",
            pick("series", a),
            b % 3
        )),
        34 => one(format!("DELETE FROM series WHERE id = {}", pick("series", a))),
        // Two series found to be one, and a volume on the later.
        _ => vec![
            format!("INSERT INTO series (code, name) VALUES ('dup', 'first{}')", b % 2),
            format!("INSERT INTO series (code, name) VALUES ('dup', 'second{}')", b % 2),
            "INSERT INTO volumes (series_id, title) SELECT max(id), 'on the second' FROM series"
                .into(),
        ],
    }
}

/// A small library: rows held before the first install, which adopts them, or written and sent
/// before the history.
const LIBRARY: &[&str] = &[
    "INSERT INTO shelves (id, name) VALUES (1, 'shelf0'), (2, 'shelf1'), (3, NULL)",
    "INSERT INTO authors (id, name) VALUES (1, 'author0'), (2, 'author1')",
    "INSERT INTO books (id, shelf_id, title, isbn, author_id, status, finished, seen) VALUES
        (1, 1, 'title0', 'i0', 1, 'done', 3, 1), (2, 2, 'title1', NULL, NULL, NULL, NULL, 2),
        (3, 1, 'title2', 'i1', 2, 'reading', NULL, 3), (4, 3, 'title0', NULL, 1, NULL, NULL, 4),
        (5, 2, 'title1', 'i2', NULL, NULL, NULL, 5)",
    "UPDATE authors SET favourite = 3 WHERE id = 1",
    "INSERT INTO notes (id, book_id, body) VALUES (1, 1, 'a'), (2, 3, 'b'), (3, 3, 'c'), (4, 5, 'd')",
    "INSERT INTO marks (id, note_id, place) VALUES (1, 1, 0), (2, 1, 1), (3, 3, 0)",
    "INSERT INTO tags (book_id, tag, note) VALUES (1, 'tag0', NULL), (3, 'tag1', 'n'), (3, 'tag0', NULL)",
    "INSERT INTO settings VALUES ('key0', 'value0'), ('key1', 'value1')",
    "INSERT INTO covers (book_id, path) VALUES (1, 'c')",
    "INSERT INTO series (id, code, name) VALUES (1, 'c0', 'one'), (2, 'c1', 'two')",
    "INSERT INTO volumes (id, series_id, title) VALUES (1, 1, 'v'), (2, 2, 'w')",
];

#[derive(Debug, Clone, Copy, PartialEq)]
enum Mode {
    /// Rows found and written before the first install, which is made in steps.
    First,
    /// A collect after some history, in steps.
    Collect,
    /// An install of a registry that adds a field, in steps.
    Upgrade,
}

#[derive(Debug, Clone)]
struct Case {
    seed: u64,
    mode: Mode,
    /// Whether the databases enforce foreign keys. Without, removing a row leaves rows naming it.
    fk: bool,
    /// Writes on replica 0 or 1 (by the flag), or with `None` an exchange between the two.
    history: Vec<Option<(bool, u8, u8, u8)>>,
    /// Writes on replica 0 not yet collected when the operation under test runs.
    burst: Vec<(u8, u8, u8)>,
    /// The clock moves on one reading in this many, so many writes share a millisecond; 0 for
    /// three in four.
    every: u64,
    /// Whether the burst is written in one millisecond.
    still: bool,
}

async fn run(conn: &mut SqliteConnection, statements: &[String]) {
    for sql in statements {
        let _ = sqlx::query(sql).execute(&mut *conn).await;
    }
}

async fn send(replica: &Replica, from: &mut SqliteConnection, to: &mut SqliteConnection) {
    replica.collect(from).await.unwrap();
    let outbox = replica.outbox(from, u32::MAX).await.unwrap();
    replica.acknowledge(from, outbox.through).await.unwrap();
    replica.apply(to, &outbox.changes).await.unwrap();
}

#[derive(Debug, PartialEq)]
struct Outcome {
    outbox: Vec<Change>,
    inspection: Inspection,
    ids: Vec<Vec<Value>>,
    tables: Vec<Vec<Vec<Value>>>,
    /// The other replica's rows once it has applied what was sent.
    elsewhere: Vec<Vec<Vec<Value>>>,
    /// Steps taken, and changes the last step reported.
    steps: usize,
}

async fn tables(conn: &mut SqliteConnection) -> Vec<Vec<Vec<Value>>> {
    let mut out = Vec::new();
    for t in TABLES {
        out.push(support::rows(conn, &format!("SELECT * FROM {t} ORDER BY rowid"), &[]).await);
    }
    out
}

/// Builds the case's world and runs the operation under test whole (`step` of `None`) or in
/// steps of `step` rows.
async fn outcome(case: &Case, step: Option<usize>) -> Outcome {
    let world = World::clustered(case.seed, case.every);
    let mut a = world.connect(0).await;
    let mut b = world.connect(1).await;
    for sql in SCHEMA {
        sqlx::query(sql).execute(&mut a).await.unwrap();
        sqlx::query(sql).execute(&mut b).await.unwrap();
    }
    if !case.fk {
        for conn in [&mut a, &mut b] {
            sqlx::query("PRAGMA foreign_keys = OFF")
                .execute(&mut *conn)
                .await
                .unwrap();
        }
    }
    let plain = Replica::new(registry(false));
    let upgraded = Replica::new(registry(true));
    let burst: Vec<String> = case
        .burst
        .iter()
        .flat_map(|&(k, x, y)| write(k, x, y))
        .collect();
    let mut steps = 0;
    let mut stepped = |more: bool| {
        steps += 1;
        assert!(steps < 100_000, "the steps never end");
        more
    };
    if case.mode == Mode::First {
        run(
            &mut a,
            &LIBRARY.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
        .await;
        if case.still {
            world.later(7);
            world.hold(true);
        }
        run(&mut a, &burst).await;
        world.hold(false);
        plain.install(&mut b).await.unwrap();
        match step {
            None => plain.install(&mut a).await.unwrap(),
            Some(rows) => {
                let mut more = stepped(plain.install_step(&mut a, rows).await.unwrap().more);
                while more {
                    more = stepped(plain.collect_step(&mut a, rows).await.unwrap().more);
                }
            }
        }
    } else {
        plain.install(&mut a).await.unwrap();
        plain.install(&mut b).await.unwrap();
        run(
            &mut a,
            &LIBRARY.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
        .await;
        send(&plain, &mut a, &mut b).await;
        for entry in &case.history {
            match *entry {
                None => {
                    send(&plain, &mut a, &mut b).await;
                    send(&plain, &mut b, &mut a).await;
                }
                Some((on_b, k, x, y)) => {
                    let conn = if on_b { &mut b } else { &mut a };
                    run(conn, &write(k, x, y)).await;
                }
            }
        }
        send(&plain, &mut a, &mut b).await;
        send(&plain, &mut b, &mut a).await;
        if case.still {
            world.later(7);
            world.hold(true);
        }
        run(&mut a, &burst).await;
        world.hold(false);
        let replica = if case.mode == Mode::Upgrade {
            &upgraded
        } else {
            &plain
        };
        match (case.mode, step) {
            (Mode::Upgrade, None) => replica.install(&mut a).await.unwrap(),
            (_, None) => {
                replica.collect(&mut a).await.unwrap();
            }
            (mode, Some(rows)) => {
                let mut more = stepped(if mode == Mode::Upgrade {
                    replica.install_step(&mut a, rows).await.unwrap().more
                } else {
                    replica.collect_step(&mut a, rows).await.unwrap().more
                });
                while more {
                    more = stepped(replica.collect_step(&mut a, rows).await.unwrap().more);
                }
            }
        }
    }
    let replica = if case.mode == Mode::Upgrade {
        &upgraded
    } else {
        &plain
    };
    let outbox = replica.outbox(&mut a, u32::MAX).await.unwrap().changes;
    let inspection = replica.inspect(&mut a).await.unwrap();
    let ids = support::rows(
        &mut a,
        "SELECT collection, key, gid, local, image, doomed FROM fongo_ids ORDER BY collection, key",
        &[],
    )
    .await;
    let tables_a = tables(&mut a).await;
    replica.install(&mut b).await.unwrap();
    replica.apply(&mut b, &outbox).await.unwrap();
    Outcome {
        outbox,
        inspection,
        ids,
        tables: tables_a,
        elsewhere: tables(&mut b).await,
        steps,
    }
}

/// Fails unless `stepped` records what `whole` does, as the module documentation says.
fn same(whole: &Outcome, stepped: &Outcome) -> Result<(), String> {
    let aliased = whole.outbox.iter().any(|c| c.field == ALIAS);
    let mut differ = Vec::new();
    if aliased {
        let plain = |o: &Outcome| {
            let mut changes: Vec<&Change> = o.outbox.iter().filter(|c| c.field != ALIAS).collect();
            changes.sort_by_key(|c| c.clock);
            changes
                .into_iter()
                .map(|c| {
                    (
                        c.collection.clone(),
                        c.gid,
                        c.field.clone(),
                        c.value.clone(),
                    )
                })
                .collect::<Vec<_>>()
        };
        // By the groups they join: entities found to be one in two steps take other pairs than
        // when found at once, as they would in two collects.
        let aliases = |o: &Outcome| {
            let mut group: BTreeMap<(String, Gid), (String, Gid)> = BTreeMap::new();
            fn root(
                group: &BTreeMap<(String, Gid), (String, Gid)>,
                mut at: (String, Gid),
            ) -> (String, Gid) {
                while let Some(up) = group.get(&at) {
                    at = up.clone();
                }
                at
            }
            for c in o.outbox.iter().filter(|c| c.field == ALIAS) {
                let Value::Ref(to) = c.value else { continue };
                let a = root(&group, (c.collection.clone(), c.gid));
                let b = root(&group, (c.collection.clone(), to));
                if a != b {
                    group.insert(a.clone().max(b.clone()), a.min(b));
                }
            }
            let mut parts: BTreeMap<(String, Gid), BTreeSet<Gid>> = BTreeMap::new();
            let all: Vec<(String, Gid)> = group
                .iter()
                .flat_map(|(a, b)| [a.clone(), b.clone()])
                .collect();
            for at in all {
                parts
                    .entry(root(&group, at.clone()))
                    .or_default()
                    .insert(at.1);
            }
            parts
                .into_iter()
                .map(|((c, _), members)| format!("{c} {members:?}"))
                .collect::<BTreeSet<_>>()
        };
        if plain(whole) != plain(stepped) {
            differ.push(format!(
                "changes in clock order:\n{:#?}\n{:#?}",
                plain(whole),
                plain(stepped)
            ));
        }
        if aliases(whole) != aliases(stepped) {
            differ.push(format!(
                "aliases:\n{:#?}\n{:#?}",
                aliases(whole),
                aliases(stepped)
            ));
        }
        let values = |i: &Inspection| {
            i.registers
                .iter()
                .map(|(c, g, f, v, _)| (c.clone(), *g, f.clone(), v.clone()))
                .collect::<Vec<_>>()
        };
        if values(&whole.inspection) != values(&stepped.inspection) {
            differ.push("register values".into());
        }
        let mut a = whole.inspection.clone();
        let mut b = stepped.inspection.clone();
        a.registers.clear();
        b.registers.clear();
        if a != b {
            differ.push(format!("inspections:\n{a:#?}\n{b:#?}"));
        }
    } else {
        if whole.outbox != stepped.outbox {
            let only = |a: &[Change], b: &[Change]| {
                a.iter()
                    .filter(|c| !b.contains(c))
                    .map(|c| {
                        format!(
                            "{} {:?} {} {:?} {:?}",
                            c.collection, c.gid, c.field, c.value, c.clock
                        )
                    })
                    .collect::<Vec<_>>()
            };
            differ.push(format!(
                "outboxes, {} and {} changes; only whole:\n{:#?}\nonly stepped:\n{:#?}",
                whole.outbox.len(),
                stepped.outbox.len(),
                only(&whole.outbox, &stepped.outbox),
                only(&stepped.outbox, &whole.outbox)
            ));
        }
        if whole.inspection != stepped.inspection {
            differ.push("inspections".into());
        }
    }
    if whole.ids != stepped.ids {
        differ.push("ids and images".into());
    }
    if whole.tables != stepped.tables {
        differ.push(format!("rows:\n{:?}\n{:?}", whole.tables, stepped.tables));
    }
    if whole.elsewhere != stepped.elsewhere {
        differ.push("rows elsewhere".into());
    }
    if differ.is_empty() {
        Ok(())
    } else {
        Err(differ.join("\n\n"))
    }
}

const SIZES: &[usize] = &[1, 2, 3, 7, COLLECT_ROWS];

fn check(case: &Case) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let whole = outcome(case, None).await;
        for &rows in SIZES {
            let stepped = outcome(case, Some(rows)).await;
            same(&whole, &stepped)
                .map_err(|e| format!("steps of {rows} ({} steps): {e}\n{case:?}", stepped.steps))?;
        }
        Ok(())
    })
}

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(48)
}

fn rng_seed() -> RngSeed {
    match std::env::var("PROPTEST_RNG_SEED") {
        Ok(_) => ProptestConfig::default().rng_seed,
        Err(_) => RngSeed::Fixed(23),
    }
}

fn arb_write() -> impl Strategy<Value = (u8, u8, u8)> {
    (any::<u8>(), 0u8..8, any::<u8>())
}

fn arb_case() -> impl Strategy<Value = Case> {
    (
        any::<u64>(),
        prop_oneof![Just(Mode::First), Just(Mode::Collect), Just(Mode::Upgrade)],
        any::<bool>(),
        proptest::collection::vec(
            prop_oneof![
                1 => Just(None),
                6 => (any::<bool>(), any::<u8>(), 0u8..8, any::<u8>())
                    .prop_map(|(b, k, x, y)| Some((b, k, x, y))),
            ],
            0..40,
        ),
        proptest::collection::vec(arb_write(), 0..40),
        prop_oneof![Just(0), Just(1), Just(3), Just(8), Just(40)],
        any::<bool>(),
    )
        .prop_map(|(seed, mode, fk, history, burst, every, still)| Case {
            seed,
            mode,
            fk,
            history,
            burst,
            every,
            still,
        })
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: cases(),
        rng_seed: rng_seed(),
        failure_persistence: None,
        ..ProptestConfig::default()
    })]
    #[test]
    fn collecting_in_steps_records_what_collecting_at_once_does(case in arb_case()) {
        if let Err(error) = check(&case) {
            prop_assert!(false, "{}", error);
        }
    }
}

/// Every kind of write, each alone and all together, in each mode.
#[test]
fn every_kind_of_write_collects_alike_in_steps() {
    for (mode, fk) in [
        (Mode::First, true),
        (Mode::Collect, true),
        (Mode::Upgrade, true),
        (Mode::Collect, false),
    ] {
        let history: Vec<_> = (0..36u8)
            .flat_map(|k| [Some((k % 3 == 0, k, k, k + 1)), Some((false, 3, k, k))])
            .chain([None])
            .collect();
        for kind in 0..36u8 {
            let burst = vec![(kind, 1, 2), (kind, 2, 5), (kind, 0, 7)];
            check(&Case {
                every: 0,
                still: false,
                seed: u64::from(kind),
                mode,
                fk,
                history: history.clone(),
                burst,
            })
            .unwrap();
        }
        let burst: Vec<_> = (0..108u8).map(|i| (i % 36, i / 36, i)).collect();
        check(&Case {
            every: 0,
            still: false,
            seed: 7,
            mode,
            fk,
            history,
            burst,
        })
        .unwrap();
    }
}

/// Rows that do not depend on one another, each written in a millisecond of its own, are
/// collected `rows` at a time, the earliest written first, and the last step says no more are
/// left.
#[tokio::test]
async fn a_step_takes_as_many_independent_rows_as_asked() {
    let world = World::new(3);
    let mut conn = world.connect(0).await;
    for sql in SCHEMA {
        sqlx::query(sql).execute(&mut conn).await.unwrap();
    }
    let replica = Replica::new(registry(false));
    replica.install(&mut conn).await.unwrap();
    for i in 1..=95 {
        world.later(1);
        sqlx::query(&format!("INSERT INTO settings VALUES ('key {i}', 'value')"))
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let mut left = Vec::new();
    loop {
        let collected = replica.collect_step(&mut conn, 10).await.unwrap();
        left.push((
            replica.uncollected_rows(&mut conn, 1_000).await.unwrap(),
            collected.produced,
            collected.more,
        ));
        if !collected.more {
            break;
        }
    }
    let want: Vec<(usize, usize, bool)> = (1..=10)
        .map(|i| {
            let left = 95usize.saturating_sub(10 * i);
            let taken = 95 - left - 10 * (i - 1);
            (left, 3 * taken, left > 0)
        })
        .collect();
    assert_eq!(left, want);
}

/// Rows written in one millisecond, as one statement writes many, are taken by one step: a step
/// never ends inside a millisecond, however few rows it was asked for.
#[tokio::test]
async fn rows_written_in_one_millisecond_are_one_step() {
    let world = World::new(3);
    let mut conn = world.connect(0).await;
    for sql in SCHEMA {
        sqlx::query(sql).execute(&mut conn).await.unwrap();
    }
    let replica = Replica::new(registry(false));
    replica.install(&mut conn).await.unwrap();
    world.hold(true);
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 95)
         INSERT INTO settings SELECT 'key ' || i, 'value' FROM n",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    world.hold(false);
    world.later(1);
    sqlx::query("INSERT INTO settings VALUES ('later', 'value')")
        .execute(&mut conn)
        .await
        .unwrap();
    let first = replica.collect_step(&mut conn, 10).await.unwrap();
    assert_eq!((first.rows, first.more), (95, true));
    let second = replica.collect_step(&mut conn, 10).await.unwrap();
    assert_eq!((second.rows, second.more), (1, false));
}

/// Hard shapes, each collected a row at a time: a book removed and made again at its id with
/// notes and a tag naming it written after (rows that name a row given a new entity); a book
/// moved to a shelf made after it, and the shelf written again (a row that names a later one);
/// a shelf removed with its books (an owner and what it owns); and a book given the isbn of
/// another, then that one's changed (entities found to be one).
#[test]
fn hard_shapes_collect_alike_in_steps() {
    let shapes: [&[(u8, u8, u8)]; 5] = [
        &[
            (9, 0, 1),
            (24, 0, 1),
            (24, 1, 2),
            (10, 0, 1),
            (14, 0, 1),
            (20, 0, 1),
            (14, 0, 2),
            (21, 0, 1),
        ],
        &[(9, 0, 0), (26, 0, 1), (9, 0, 2), (14, 0, 0)],
        &[(13, 0, 0), (13, 1, 1), (0, 0, 2), (1, 2, 1)],
        &[(2, 0, 0), (14, 1, 1), (2, 1, 0)],
        &[(5, 1, 0), (5, 0, 1), (5, 2, 0), (4, 0, 1)],
    ];
    for (i, burst) in shapes.iter().enumerate() {
        for mode in [Mode::First, Mode::Collect, Mode::Upgrade] {
            check(&Case {
                every: 0,
                still: false,
                seed: i as u64,
                mode,
                fk: true,
                history: vec![None],
                burst: burst.to_vec(),
            })
            .unwrap();
        }
    }
    check(&Case {
        every: 0,
        still: false,
        seed: 2143016055923739986,
        mode: Mode::Collect,
        fk: true,
        history: vec![Some((false, 38, 5, 0))],
        burst: vec![
            (2, 1, 0),
            (122, 5, 0),
            (181, 0, 0),
            (70, 0, 0),
            (146, 0, 38),
        ],
    })
    .unwrap();
}

/// An alias found by an early step is stamped as by a collect made just after the rows that step
/// took, not when the step runs: a write collected by a later step keeps its own time, so it
/// still loses to another replica's later write, as it would after any collects. Two series are
/// found to be one while, later, this replica renames another series and the other replica then
/// renames it too.
#[tokio::test]
async fn an_alias_found_by_an_early_step_leaves_later_writes_their_times() {
    async fn names(step: Option<usize>) -> (Vec<Vec<Value>>, Vec<Vec<Value>>, bool) {
        let world = World::new(1);
        let mut a = world.connect(0).await;
        let mut b = world.connect(1).await;
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut a).await.unwrap();
            sqlx::query(sql).execute(&mut b).await.unwrap();
        }
        let replica = Replica::new(registry(false));
        replica.install(&mut a).await.unwrap();
        replica.install(&mut b).await.unwrap();
        run(
            &mut a,
            &["INSERT INTO series (id, code, name) VALUES (1, NULL, 'Middlemarch')".into()],
        )
        .await;
        send(&replica, &mut a, &mut b).await;
        world.later(10_000);
        run(
            &mut a,
            &[
                "INSERT INTO series (id, code, name) VALUES (2, 'dup', 'one')".into(),
                "INSERT INTO series (id, code, name) VALUES (3, 'dup', 'two')".into(),
            ],
        )
        .await;
        world.later(10_000);
        run(
            &mut a,
            &["UPDATE series SET name = 'from a' WHERE id = 1".into()],
        )
        .await;
        world.later(10_000);
        run(
            &mut b,
            &["UPDATE series SET name = 'from b' WHERE id = 1".into()],
        )
        .await;
        replica.collect(&mut b).await.unwrap();
        world.later(10_000);
        match step {
            None => {
                replica.collect(&mut a).await.unwrap();
            }
            Some(rows) => while replica.collect_step(&mut a, rows).await.unwrap().more {},
        }
        let aliased = replica
            .outbox(&mut a, u32::MAX)
            .await
            .unwrap()
            .changes
            .iter()
            .any(|c| c.field == ALIAS);
        send(&replica, &mut a, &mut b).await;
        send(&replica, &mut b, &mut a).await;
        let sql = "SELECT code, name FROM series ORDER BY code, name";
        (
            support::rows(&mut a, sql, &[]).await,
            support::rows(&mut b, sql, &[]).await,
            aliased,
        )
    }
    let whole = names(None).await;
    assert!(whole.2, "no alias was found");
    assert_eq!(whole.0, whole.1);
    assert!(format!("{:?}", whole.0).contains("from b"));
    for rows in [1, 2, 3] {
        assert_eq!(names(Some(rows)).await, whole, "steps of {rows}");
    }
}

/// Entities found to be one while rows that name the one given up wait past the prefix: the
/// rows are collected with the step, before the merge points them at the row kept.
#[test]
fn rows_naming_a_row_a_step_gives_up_are_collected_with_it() {
    for seed in 0..8 {
        for fk in [true, false] {
            check(&Case {
                every: 0,
                still: false,
                seed,
                mode: Mode::Collect,
                fk,
                history: vec![None],
                burst: vec![
                    (35, 0, seed as u8),
                    (24, 0, 1),
                    (24, 1, 2),
                    (33, 3, 0),
                    (35, 1, 1),
                ],
            })
            .unwrap();
        }
    }
}

/// A prefix that must grow because what its changes affect has rows past it is given up before
/// any change is kept: with foreign keys off, a book removed and then a note it owned edited,
/// collected a row at a time, store each fact once, as collecting at once does.
#[tokio::test]
async fn a_prefix_that_must_grow_keeps_nothing_before_it_does() {
    async fn kept(step: Option<usize>) -> (u64, usize) {
        let world = World::new(2);
        let mut a = world.connect(0).await;
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut a).await.unwrap();
        }
        let replica = Replica::new(registry(false));
        replica.install(&mut a).await.unwrap();
        run(
            &mut a,
            &LIBRARY.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
        .await;
        replica.collect(&mut a).await.unwrap();
        run(
            &mut a,
            &[
                "PRAGMA foreign_keys = OFF".into(),
                "DELETE FROM books WHERE id = 3".into(),
                "UPDATE notes SET body = 'still' WHERE id = 2".into(),
                "INSERT INTO settings VALUES ('key2', 'v'), ('key3', 'v'), ('key4', 'v'),
                    ('key5', 'v'), ('key6', 'v'), ('key7', 'v')"
                    .into(),
            ],
        )
        .await;
        let statements = support::statements::Statements::watch(&mut a).await;
        let mut steps = 1;
        match step {
            None => {
                replica.collect(&mut a).await.unwrap();
            }
            Some(rows) => {
                while replica.collect_step(&mut a, rows).await.unwrap().more {
                    steps += 1;
                }
            }
        }
        (statements.started("INSERT INTO fongo_shadow"), steps)
    }
    let (whole, _) = kept(None).await;
    let (stepped, steps) = kept(Some(1)).await;
    assert!(whole > 0);
    assert!(steps > 1, "one step");
    assert_eq!(stepped, whole);
}

/// A row found through a book made again at its id is stamped with the book's time, so it ticks
/// after every row marked in that millisecond, whole or in steps. With foreign keys off, the
/// author naming the book is not marked, and settings share the book's millisecond.
#[tokio::test]
async fn a_found_row_ticks_after_the_rows_of_its_millisecond() {
    async fn sent(step: Option<usize>, fk: bool) -> Vec<Change> {
        let world = World::new(5);
        let mut a = world.connect(0).await;
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut a).await.unwrap();
        }
        if !fk {
            run(&mut a, &["PRAGMA foreign_keys = OFF".into()]).await;
        }
        let replica = Replica::new(registry(false));
        replica.install(&mut a).await.unwrap();
        run(
            &mut a,
            &LIBRARY.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
        )
        .await;
        replica.collect(&mut a).await.unwrap();
        let o = replica.outbox(&mut a, u32::MAX).await.unwrap();
        replica.acknowledge(&mut a, o.through).await.unwrap();
        world.later(1000);
        run(
            &mut a,
            &[
                "DELETE FROM books WHERE id = 3".into(),
                "INSERT INTO books (id, shelf_id, title, seen) VALUES (3, 1, 'again', 0)".into(),
                "INSERT INTO settings VALUES ('k1', 'v'), ('k2', 'v'), ('k3', 'v')".into(),
                "UPDATE fongo_dirty SET at = (SELECT max(at) FROM fongo_dirty)".into(),
            ],
        )
        .await;
        match step {
            None => {
                replica.collect(&mut a).await.unwrap();
            }
            Some(rows) => while replica.collect_step(&mut a, rows).await.unwrap().more {},
        }
        replica.outbox(&mut a, u32::MAX).await.unwrap().changes
    }
    for fk in [false, true] {
        let whole = sent(None, fk).await;
        for rows in 1..=4 {
            assert_eq!(
                sent(Some(rows), fk).await,
                whole,
                "steps of {rows}, foreign keys {fk}"
            );
        }
    }
}

/// The application writes rows found at a first install between that install's steps: they were
/// adopted by the install, and the writes are edits after them, as after one install.
#[tokio::test]
async fn rows_found_at_a_first_install_are_adopted_before_its_next_step() {
    async fn shown(step: Option<usize>) -> (Vec<Vec<Value>>, Vec<Vec<Value>>) {
        let world = World::new(11);
        let mut a = world.connect(0).await;
        let mut b = world.connect(1).await;
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut a).await.unwrap();
            sqlx::query(sql).execute(&mut b).await.unwrap();
        }
        let replica = Replica::new(registry(false));
        replica.install(&mut b).await.unwrap();
        run(
            &mut b,
            &[
                "INSERT INTO settings VALUES ('theme', 'light')".into(),
                "INSERT INTO series (code, name) VALUES ('mm', 'Middlemarch, from b')".into(),
            ],
        )
        .await;
        replica.collect(&mut b).await.unwrap();
        world.later(1000);
        run(
            &mut a,
            &[
                "INSERT INTO settings VALUES ('font', 'serif'), ('size', '12'), ('theme', 'dark')"
                    .into(),
                "INSERT INTO series (code, name) VALUES ('mm', 'Middlemarch, from a')".into(),
                "INSERT INTO series (code, name) VALUES ('x', 'Romola')".into(),
            ],
        )
        .await;
        let between = [
            "UPDATE settings SET value = value WHERE key = 'theme'".to_string(),
            "UPDATE series SET name = name".into(),
            "UPDATE settings SET value = '14' WHERE key = 'size'".into(),
        ];
        match step {
            None => {
                replica.install(&mut a).await.unwrap();
                run(&mut a, &between).await;
                replica.collect(&mut a).await.unwrap();
            }
            Some(rows) => {
                let mut more = replica.install_step(&mut a, rows).await.unwrap().more;
                run(&mut a, &between).await;
                while more {
                    more = replica.collect_step(&mut a, rows).await.unwrap().more;
                }
                while replica.collect_step(&mut a, rows).await.unwrap().more {}
            }
        }
        send(&replica, &mut a, &mut b).await;
        send(&replica, &mut b, &mut a).await;
        let sql =
            "SELECT key, value FROM settings UNION ALL SELECT code, name FROM series ORDER BY 1, 2";
        (
            support::rows(&mut a, sql, &[]).await,
            support::rows(&mut b, sql, &[]).await,
        )
    }
    let whole = shown(None).await;
    assert!(format!("{whole:?}").contains("light"));
    for rows in [1, 2] {
        assert_eq!(shown(Some(rows)).await, whole, "steps of {rows}");
    }
}

/// How far steps grow past what is asked over shapes of writes that depend on each other: books
/// removed and made again at their ids with a new note on each, as an import that replaces its
/// records does; every book edited, then every note; half the books moved to a new shelf, and the
/// shelf they left renamed. No step takes more than twice what is asked. (A new row written again
/// after the rows naming it is collected with all of them: until then it has no entity they can
/// name.)
#[tokio::test]
async fn steps_over_rows_that_depend_on_each_other_stay_near_their_size() {
    async fn largest(writes: &[String], rows: usize) -> (usize, usize) {
        let world = World::new(21);
        let mut a = world.connect(0).await;
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut a).await.unwrap();
        }
        let replica = Replica::new(registry(false));
        replica.install(&mut a).await.unwrap();
        run(
            &mut a,
            &[
                "INSERT INTO shelves (id, name) VALUES (1, 's1'), (2, 's2')".into(),
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300)
                 INSERT INTO books (id, shelf_id, title, seen) SELECT i, 1, 't' || i, 0 FROM n"
                    .into(),
                "INSERT INTO notes (book_id, body) SELECT id, 'b' FROM books".into(),
            ],
        )
        .await;
        replica.collect(&mut a).await.unwrap();
        run(&mut a, writes).await;
        let (mut largest, mut total) = (0, 0);
        loop {
            let before = replica.uncollected_rows(&mut a, usize::MAX).await.unwrap();
            let more = replica.collect_step(&mut a, rows).await.unwrap().more;
            let taken = before - replica.uncollected_rows(&mut a, usize::MAX).await.unwrap();
            largest = largest.max(taken);
            total += taken;
            if !more {
                return (largest, total);
            }
        }
    }
    let replaced: Vec<String> = (1..=300)
        .flat_map(|i| {
            [
                format!("DELETE FROM books WHERE id = {i}"),
                format!(
                    "INSERT INTO books (id, shelf_id, title, seen) VALUES ({i}, 1, 'again', 0)"
                ),
                format!("INSERT INTO notes (book_id, body) VALUES ({i}, 'new')"),
            ]
        })
        .collect();
    let edited = vec![
        "UPDATE books SET title = title || '!'".to_string(),
        "UPDATE notes SET body = body || '!'".into(),
    ];
    let moved = vec![
        "INSERT INTO shelves (id, name) VALUES (3, 's3')".to_string(),
        "UPDATE books SET shelf_id = 3 WHERE id % 2 = 0".into(),
        "UPDATE shelves SET name = 's1!' WHERE id = 1".into(),
    ];
    for (what, writes) in [("replaced", replaced), ("edited", edited), ("moved", moved)] {
        for rows in [10, 100] {
            let (largest, total) = largest(&writes, rows).await;
            assert!(total > rows, "{what}: {total} rows");
            assert!(
                largest <= 2 * rows,
                "{what}, steps of {rows}: a step of {largest}"
            );
        }
    }
}

/// What one replica's collects, made wherever between its writes, send for an alias found and
/// then an earlier edit of a field another replica edits later: the later edit wins in every
/// placement, as it does when the same writes are collected in steps.
#[tokio::test]
async fn no_placing_of_collects_lets_an_earlier_edit_win() {
    for placing in 0u32..8 {
        let world = World::new(1);
        let mut a = world.connect(0).await;
        let mut b = world.connect(1).await;
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut a).await.unwrap();
            sqlx::query(sql).execute(&mut b).await.unwrap();
        }
        let replica = Replica::new(registry(false));
        replica.install(&mut a).await.unwrap();
        replica.install(&mut b).await.unwrap();
        run(
            &mut a,
            &["INSERT INTO series (id, code, name) VALUES (1, NULL, 'Middlemarch')".into()],
        )
        .await;
        send(&replica, &mut a, &mut b).await;
        world.later(10_000);
        let writes = [
            "INSERT INTO series (id, code, name) VALUES (2, 'dup', 'one')",
            "INSERT INTO series (id, code, name) VALUES (3, 'dup', 'two')",
            "UPDATE series SET name = 'from a' WHERE id = 1",
        ];
        for (i, write) in writes.iter().enumerate() {
            if i == 2 {
                world.later(10_000);
            }
            run(&mut a, &[write.to_string()]).await;
            if placing & (1 << i) != 0 {
                replica.collect(&mut a).await.unwrap();
            }
        }
        world.later(10_000);
        run(
            &mut b,
            &["UPDATE series SET name = 'from b' WHERE id = 1".into()],
        )
        .await;
        replica.collect(&mut b).await.unwrap();
        send(&replica, &mut a, &mut b).await;
        send(&replica, &mut b, &mut a).await;
        let names = support::rows(&mut a, "SELECT name FROM series WHERE code IS NULL", &[]).await;
        assert_eq!(
            names,
            vec![vec![Value::Text("from b".into())]],
            "placing {placing:03b}"
        );
    }
}

/// A row written back to what it held between two steps, while another written before the first
/// step is collected by it: the steps send what a collect made just after the first step's rows,
/// and another at the end, send.
#[tokio::test]
async fn a_row_written_back_between_steps_sends_what_two_collects_would() {
    async fn sent(first_by_step: bool) -> Vec<String> {
        let world = World::new(3);
        let mut a = world.connect(0).await;
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut a).await.unwrap();
        }
        let replica = Replica::new(registry(false));
        replica.install(&mut a).await.unwrap();
        run(
            &mut a,
            &["INSERT INTO settings VALUES ('q', 'q0'), ('r', 'r0')".into()],
        )
        .await;
        replica.collect(&mut a).await.unwrap();
        let outbox = replica.outbox(&mut a, u32::MAX).await.unwrap();
        replica.acknowledge(&mut a, outbox.through).await.unwrap();
        world.later(100);
        run(
            &mut a,
            &["UPDATE settings SET value = 'q1' WHERE key = 'q'".into()],
        )
        .await;
        if !first_by_step {
            replica.collect(&mut a).await.unwrap();
        }
        world.later(100);
        run(
            &mut a,
            &["UPDATE settings SET value = 'r1' WHERE key = 'r'".into()],
        )
        .await;
        if first_by_step {
            assert!(replica.collect_step(&mut a, 1).await.unwrap().more);
        }
        world.later(100);
        run(
            &mut a,
            &[
                "UPDATE settings SET value = 'r0' WHERE key = 'r'".into(),
                "UPDATE settings SET value = 'q0' WHERE key = 'q'".into(),
            ],
        )
        .await;
        if first_by_step {
            while replica.collect_step(&mut a, 1).await.unwrap().more {}
        } else {
            replica.collect(&mut a).await.unwrap();
        }
        replica
            .outbox(&mut a, u32::MAX)
            .await
            .unwrap()
            .changes
            .iter()
            .map(|c| format!("{:?} {} {:?}", c.gid, c.field, c.value))
            .collect()
    }
    assert_eq!(sent(true).await, sent(false).await);
}

/// Found by a long run (seed 41). With foreign keys off, tags whose books are removed derive one
/// key; the first of them makes its entity and the next finds it known.
#[test]
fn tags_whose_books_are_gone_collect_alike_in_steps() {
    check(&Case {
        every: 0,
        still: false,
        seed: 1882020332708727243,
        mode: Mode::Collect,
        fk: false,
        history: vec![
            Some((false, 80, 4, 0)),
            None,
            Some((false, 16, 0, 0)),
            Some((false, 86, 0, 0)),
            Some((false, 232, 0, 0)),
        ],
        burst: vec![
            (17, 1, 130),
            (57, 3, 0),
            (98, 0, 42),
            (152, 1, 0),
            (20, 3, 2),
            (8, 3, 0),
        ],
    })
    .unwrap();
}

/// Found by a long run (seed 42). With foreign keys off, a shelf is removed while a book on it
/// stays, and an author the book names is removed and made again at its id later: one collect
/// finds the book through the author and clears its shelf before the merge, so the book lives;
/// a step that removes the shelf first must take the author too, or it removes the book.
#[test]
fn a_row_a_later_step_would_find_is_not_written_first() {
    check(&Case {
        every: 0,
        still: false,
        seed: 2121431186798033,
        mode: Mode::Collect,
        fk: false,
        history: vec![Some((false, 254, 2, 0)), Some((true, 191, 5, 0))],
        burst: vec![
            (17, 6, 1),
            (63, 4, 0),
            (38, 2, 0),
            (28, 0, 0),
            (83, 0, 0),
            (88, 1, 0),
            (118, 0, 0),
            (89, 7, 0),
            (46, 0, 0),
        ],
    })
    .unwrap();
}

/// SQLite's work on a connection, in units of 64 virtual machine instructions.
async fn count_work(conn: &mut SqliteConnection) -> std::sync::Arc<std::sync::atomic::AtomicU64> {
    let work = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counter = work.clone();
    let mut handle = support::lock(conn).await;
    handle.set_progress_handler(64, move || {
        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        true
    });
    work
}

/// Rows that each depend on one written after them: books, each naming its author, and authors,
/// each naming the next book as a favourite, written in turn (`books`), highlights, each naming
/// the next (`highlights`), or books that all name one new author written again after them
/// (`fan`): no closed prefix is shorter than all of them. Or rows a step finds it needs only once
/// it merges: every other book removed with its notes and tags (`owners`), or new series each
/// found to be one with a series edited after the next new one is made (`found`).
async fn chained(world: &World, shape: &str, n: usize) -> (SqliteConnection, Replica) {
    let mut a = world.connect(0).await;
    for sql in SCHEMA {
        sqlx::query(sql).execute(&mut a).await.unwrap();
    }
    sqlx::query(
        "CREATE TABLE highlights (id INTEGER PRIMARY KEY, body TEXT,
            next INTEGER REFERENCES highlights(id) ON DELETE SET NULL)",
    )
    .execute(&mut a)
    .await
    .unwrap();
    let mut collections: Vec<Collection> = registry(false).collections().to_vec();
    collections.push(
        Collection::minted("highlights", "highlights", "id")
            .field("body")
            .reference("next", "highlights"),
    );
    let replica = Replica::new(Registry::new(collections).unwrap());
    replica.install(&mut a).await.unwrap();
    run(
        &mut a,
        &["INSERT INTO shelves (id, name) VALUES (1, 's1')".into()],
    )
    .await;
    replica.collect(&mut a).await.unwrap();
    let seq =
        format!("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {n})");
    if shape == "books" {
        run(
            &mut a,
            &[
                format!("{seq} INSERT INTO authors (id, name) SELECT i, 'a' || i FROM n"),
                format!("{seq} INSERT INTO books (id, shelf_id, title, seen) SELECT i, 1, 't' || i, 0 FROM n"),
            ],
        )
        .await;
        for i in 1..=n {
            world.later(1);
            run(
                &mut a,
                &[format!("UPDATE books SET author_id = {i} WHERE id = {i}")],
            )
            .await;
            if i < n {
                world.later(1);
                run(
                    &mut a,
                    &[format!(
                        "UPDATE authors SET favourite = {} WHERE id = {i}",
                        i + 1
                    )],
                )
                .await;
            }
        }
    } else if shape == "fan" {
        run(
            &mut a,
            &[
                "INSERT INTO authors (id, name) VALUES (1, 'a')".into(),
                format!("{seq} INSERT INTO books (id, shelf_id, title, seen) SELECT i, 1, 't' || i, 0 FROM n"),
            ],
        )
        .await;
        for i in 1..=n {
            world.later(1);
            run(
                &mut a,
                &[format!("UPDATE books SET author_id = 1 WHERE id = {i}")],
            )
            .await;
        }
        world.later(1);
        run(
            &mut a,
            &["UPDATE authors SET name = 'b' WHERE id = 1".into()],
        )
        .await;
    } else if shape == "found" {
        run(
            &mut a,
            &[format!(
                "{seq} INSERT INTO series (id, code, name) SELECT i, 'c' || i, 's' || i FROM n"
            )],
        )
        .await;
        replica.collect(&mut a).await.unwrap();
        for i in 1..=n {
            world.later(1);
            run(
                &mut a,
                &[format!(
                    "INSERT INTO series (id, code, name) VALUES ({}, 'c{i}', 'again')",
                    n + i
                )],
            )
            .await;
            if i > 1 {
                world.later(1);
                run(
                    &mut a,
                    &[format!(
                        "UPDATE series SET name = 'edited' WHERE id = {}",
                        i - 1
                    )],
                )
                .await;
            }
        }
        world.later(1);
        run(
            &mut a,
            &[format!("UPDATE series SET name = 'edited' WHERE id = {n}")],
        )
        .await;
    } else if shape == "owners" {
        run(
            &mut a,
            &[
                format!("{seq} INSERT INTO books (id, shelf_id, title, seen) SELECT i, 1, 't' || i, 0 FROM n"),
                "INSERT INTO notes (book_id, body) SELECT id, 'n' FROM books".into(),
                "INSERT INTO tags (book_id, tag) SELECT id, 'x' FROM books".into(),
            ],
        )
        .await;
        replica.collect(&mut a).await.unwrap();
        world.later(1);
        run(&mut a, &["DELETE FROM books WHERE id % 2 = 0".into()]).await;
    } else {
        run(
            &mut a,
            &[format!(
                "{seq} INSERT INTO highlights (id, body) SELECT i, 'h' || i FROM n"
            )],
        )
        .await;
        for i in 1..n {
            world.later(1);
            run(
                &mut a,
                &[format!(
                    "UPDATE highlights SET next = {} WHERE id = {i}",
                    i + 1
                )],
            )
            .await;
        }
    }
    (a, replica)
}

/// A step never gives up more rows than it then takes, at any step size, so it costs at most
/// about twice what collecting its rows at once costs; a chain is found before anything is tried
/// and taken in one step, as large as it must be. In SQLite's work, steps of ten rows or more of
/// a backlog cost at most twice one collect of it.
#[tokio::test]
async fn steps_cost_at_most_about_twice_one_collect() {
    for shape in ["books", "highlights", "fan", "owners", "found"] {
        let n = 600;
        let whole = {
            let world = World::new(3);
            let (mut a, replica) = chained(&world, shape, n).await;
            let work = count_work(&mut a).await;
            replica.collect(&mut a).await.unwrap();
            work.load(std::sync::atomic::Ordering::Relaxed)
        };
        for rows in [1, 10, 100, 1_000] {
            let world = World::new(3);
            let (mut a, replica) = chained(&world, shape, n).await;
            let marked = replica.uncollected_rows(&mut a, usize::MAX).await.unwrap();
            let work = count_work(&mut a).await;
            let (mut taken, mut tried) = (0, 0);
            loop {
                let step = replica.collect_step(&mut a, rows).await.unwrap();
                assert!(
                    step.abandoned == 0 || step.abandoned < step.rows,
                    "{shape}, steps of {rows}: gave up {} rows to take {}",
                    step.abandoned,
                    step.rows
                );
                taken += step.rows;
                tried += step.abandoned;
                if !step.more {
                    break;
                }
            }
            let work = work.load(std::sync::atomic::Ordering::Relaxed);
            eprintln!(
                "{shape}, steps of {rows}: {taken} rows, {tried} given up, work {work} against {whole}"
            );
            assert_eq!(taken, marked, "{shape}, steps of {rows}");
            if !["owners", "found"].contains(&shape) {
                assert_eq!(tried, 0, "{shape}, steps of {rows}: a chain was tried");
            }
            assert!(
                tried <= marked,
                "{shape}, steps of {rows}: {tried} rows given up"
            );
            // A step is a transaction, with what any collect costs besides its rows: steps of a
            // row or two cost that many times over.
            if rows >= 10 {
                assert!(
                    work <= 2 * whole,
                    "{shape}, steps of {rows}: work {work} against {whole} at once"
                );
            }
        }
    }
}

/// Removed owners, whose rows a step finds it needs only once it merges, in backlogs small enough
/// that its tries grow until it takes every row: the tries it gave up still took fewer rows than
/// it takes in the end.
#[tokio::test]
async fn a_step_that_grows_to_every_row_gave_up_fewer_than_it_takes() {
    for n in [16, 20, 28, 32] {
        for rows in 1..12 {
            let world = World::new(3);
            let (mut a, replica) = chained(&world, "owners", n).await;
            loop {
                let step = replica.collect_step(&mut a, rows).await.unwrap();
                assert!(
                    step.abandoned == 0 || step.abandoned < step.rows,
                    "{n} books, steps of {rows}: gave up {} rows to take {}",
                    step.abandoned,
                    step.rows
                );
                if !step.more {
                    break;
                }
            }
        }
    }
}

/// A book naming an author the kernel has not given an entity waits, and is first of the rows to
/// collect: a step whose rows all wait takes more, so steps of one go on past it to the rest.
#[tokio::test]
async fn steps_go_on_past_a_row_that_waits() {
    let world = World::new(3);
    let mut a = world.connect(0).await;
    for sql in SCHEMA {
        sqlx::query(sql).execute(&mut a).await.unwrap();
    }
    let replica = Replica::new(registry(false));
    replica.install(&mut a).await.unwrap();
    run(
        &mut a,
        &["INSERT INTO shelves (id, name) VALUES (1, 's1')".into()],
    )
    .await;
    replica.collect(&mut a).await.unwrap();
    // As the kernel's own writes are, so the author is neither marked nor mapped.
    run(
        &mut a,
        &[
            "INSERT OR REPLACE INTO fongo_meta (key, value) VALUES ('applying', 1)".into(),
            "INSERT INTO authors (id, name) VALUES (1, 'unknown')".into(),
            "INSERT OR REPLACE INTO fongo_meta (key, value) VALUES ('applying', 0)".into(),
        ],
    )
    .await;
    world.later(1);
    run(
        &mut a,
        &[
            "INSERT INTO books (id, shelf_id, title, author_id, seen) VALUES (1, 1, 't1', 1, 0)"
                .into(),
        ],
    )
    .await;
    for i in 2..=5 {
        world.later(1);
        run(
            &mut a,
            &[format!(
                "INSERT INTO books (id, shelf_id, title, seen) VALUES ({i}, 1, 't{i}', 0)"
            )],
        )
        .await;
    }
    let mut steps = 0;
    while replica.collect_step(&mut a, 1).await.unwrap().more {
        steps += 1;
        assert!(steps < 20, "steps of one never got past the row that waits");
    }
    let left = replica.inspect(&mut a).await.unwrap().uncollected;
    assert_eq!(left, [("books".to_string(), "[1]".to_string())]);
}

/// A book on a shelf a merge gave up, and another shelf, removed in one millisecond, the shelf
/// first. Removing the book lets the given-up row go and gives its entity a new row, at the next
/// id SQLite picks; a step that took the book's removal without the shelf's, which sorts after it
/// though written before, saw the shelf's id still taken and wrote the row at another id than any
/// collect made between the writes would.
#[test]
fn a_shelf_and_a_book_removed_in_one_millisecond_are_one_step() {
    check(&Case {
        seed: 4788079860073119813,
        mode: Mode::Collect,
        fk: false,
        history: vec![
            Some((false, 80, 4, 0)),
            None,
            Some((false, 8, 0, 0)),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some((true, 110, 7, 0)),
            Some((true, 4, 4, 0)),
            None,
            None,
            None,
            None,
            Some((true, 180, 0, 70)),
            None,
            Some((false, 80, 2, 0)),
            None,
            None,
            None,
        ],
        burst: vec![(218, 2, 0), (116, 6, 0)],
        every: 40,
        still: true,
    })
    .unwrap();
}

/// With foreign keys off, an author removed while a book still names its id; then, in one
/// millisecond, a new author at that id, the book pointed at it and the author renamed. A step
/// that took the authors first compared the book with its image through the new author's
/// mapping and never sent its reference.
#[test]
fn an_author_made_again_with_its_book_in_one_millisecond_is_one_step() {
    for still in [true, false] {
        check(&Case {
            seed: 1382359649355028263,
            mode: Mode::Collect,
            fk: false,
            history: vec![
                None,
                Some((false, 43, 1, 5)),
                Some((false, 227, 1, 0)),
                Some((false, 26, 0, 43)),
            ],
            burst: vec![(13, 7, 0)],
            every: 3,
            still,
        })
        .unwrap();
    }
}

/// Books each written alone, `n` of them, and `wait` books naming authors the kernel has not
/// given an entity, which cannot be collected yet, written before them (`head`), among them
/// (`middle`) or after them (`tail`).
async fn waiting_and_new(
    world: &World,
    n: usize,
    wait: usize,
    place: &str,
) -> (SqliteConnection, Replica) {
    let mut a = world.connect(0).await;
    for sql in SCHEMA {
        sqlx::query(sql).execute(&mut a).await.unwrap();
    }
    let replica = Replica::new(registry(false));
    replica.install(&mut a).await.unwrap();
    run(
        &mut a,
        &["INSERT INTO shelves (id, name) VALUES (1, 's1')".into()],
    )
    .await;
    replica.collect(&mut a).await.unwrap();
    // As the kernel's own writes are, so the authors are neither marked nor mapped.
    run(
        &mut a,
        &[
            "INSERT OR REPLACE INTO fongo_meta (key, value) VALUES ('applying', 1)".into(),
            format!(
                "WITH RECURSIVE k(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM k WHERE i < {})
                 INSERT INTO authors (id, name) SELECT i, 'unknown' FROM k",
                wait.max(1)
            ),
            "INSERT OR REPLACE INTO fongo_meta (key, value) VALUES ('applying', 0)".into(),
        ],
    )
    .await;
    let at = match place {
        "head" => 0,
        "middle" => n / 2,
        _ => n,
    };
    sqlx::query("BEGIN").execute(&mut a).await.unwrap();
    for i in 0..=n {
        if i == at {
            for w in 1..=wait {
                world.later(1);
                run(
                    &mut a,
                    &[format!(
                        "INSERT INTO books (id, shelf_id, title, author_id, seen)
                         VALUES ({w}, 1, 'waits', {w}, 0)"
                    )],
                )
                .await;
            }
        }
        if i < n {
            world.later(1);
            run(
                &mut a,
                &[format!(
                    "INSERT INTO books (id, shelf_id, title, seen) VALUES ({}, 1, 'new', 0)",
                    1_000_000 + i
                )],
            )
            .await;
        }
    }
    sqlx::query("COMMIT").execute(&mut a).await.unwrap();
    (a, replica)
}

/// Steps of 100 and 1,000 rows over many rows each written alone, and over rows behind, among or
/// after many that wait, cost in SQLite's work at most twice one collect of them all: the marks
/// are read in order through their index, and a step goes on past rows that wait until it has
/// collected at least as many. Every step but the last collects at least what was asked, and the
/// last says none are left once only rows that wait remain.
#[tokio::test]
async fn steps_over_many_rows_and_rows_that_wait_cost_at_most_twice_one_collect() {
    for (n, wait, place) in [
        (20_000, 0, "head"),
        (5_000, 999, "head"),
        (5_000, 999, "middle"),
        (5_000, 999, "tail"),
        (2_000, 2_047, "head"),
    ] {
        let whole = {
            let world = World::new(3);
            let (mut a, replica) = waiting_and_new(&world, n, wait, place).await;
            let work = count_work(&mut a).await;
            replica.collect(&mut a).await.unwrap();
            work.load(std::sync::atomic::Ordering::Relaxed)
        };
        for rows in [100, 1_000] {
            let world = World::new(3);
            let (mut a, replica) = waiting_and_new(&world, n, wait, place).await;
            let work = count_work(&mut a).await;
            let mut taken = 0;
            loop {
                let step = replica.collect_step(&mut a, rows).await.unwrap();
                taken += step.rows;
                if !step.more {
                    break;
                }
                assert!(
                    step.rows >= rows,
                    "{n} rows, {wait} waiting at the {place}, steps of {rows}: a step took {}",
                    step.rows
                );
            }
            let work = work.load(std::sync::atomic::Ordering::Relaxed);
            eprintln!(
                "{n} rows, {wait} waiting at the {place}, steps of {rows}: work {work} against {whole}"
            );
            assert_eq!(
                taken, n,
                "{n} rows, {wait} waiting at the {place}, steps of {rows}"
            );
            assert_eq!(
                replica.uncollected_rows(&mut a, usize::MAX).await.unwrap(),
                wait
            );
            assert!(
                work <= 2 * whole,
                "{n} rows, {wait} waiting at the {place}, steps of {rows}: work {work} against {whole}"
            );
        }
    }
}

/// A new series found to be one with a series edited a hundred rows later: the step finds that
/// only once it merges, and its next try reaches the edited series at once, giving up one row.
#[tokio::test]
async fn a_try_given_up_is_followed_by_one_reaching_the_row_it_needed() {
    let world = World::new(3);
    let mut a = world.connect(0).await;
    for sql in SCHEMA {
        sqlx::query(sql).execute(&mut a).await.unwrap();
    }
    let replica = Replica::new(registry(false));
    replica.install(&mut a).await.unwrap();
    run(
        &mut a,
        &["INSERT INTO series (id, code, name) VALUES (1, 'c1', 'first')".into()],
    )
    .await;
    replica.collect(&mut a).await.unwrap();
    world.later(1);
    run(
        &mut a,
        &["INSERT INTO series (id, code, name) VALUES (2, 'c1', 'again')".into()],
    )
    .await;
    for i in 0..100 {
        world.later(1);
        run(
            &mut a,
            &[format!("INSERT INTO settings VALUES ('key {i}', 'value')")],
        )
        .await;
    }
    world.later(1);
    run(
        &mut a,
        &["UPDATE series SET name = 'edited' WHERE id = 1".into()],
    )
    .await;
    let step = replica.collect_step(&mut a, 1).await.unwrap();
    assert_eq!((step.abandoned, step.rows), (1, 102));
}

/// A series whose code is changed to another's, so its entity joins that one's group, and the
/// other series edited later: the step that merges them keeps the other's row and writes it, so
/// it takes the later edit with it rather than write over it.
#[tokio::test]
async fn a_row_a_merge_keeps_takes_its_later_edit_with_the_step() {
    for rows in [None, Some(1)] {
        let world = World::new(3);
        let mut a = world.connect(0).await;
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut a).await.unwrap();
        }
        let replica = Replica::new(registry(false));
        replica.install(&mut a).await.unwrap();
        run(
            &mut a,
            &["INSERT INTO series (id, code, name) VALUES (1, 'c1', 'first'), (2, 'c2', 'second')".into()],
        )
        .await;
        replica.collect(&mut a).await.unwrap();
        world.later(1);
        run(
            &mut a,
            &["UPDATE series SET code = 'c1' WHERE id = 2".into()],
        )
        .await;
        for i in 0..5 {
            world.later(1);
            run(
                &mut a,
                &[format!("INSERT INTO settings VALUES ('key {i}', 'value')")],
            )
            .await;
        }
        world.later(1);
        run(
            &mut a,
            &["UPDATE series SET name = 'edited' WHERE id = 1".into()],
        )
        .await;
        match rows {
            None => {
                replica.collect(&mut a).await.unwrap();
            }
            Some(rows) => while replica.collect_step(&mut a, rows).await.unwrap().more {},
        }
        let names = support::rows(&mut a, "SELECT id, name FROM series ORDER BY id", &[]).await;
        let sent: Vec<_> = replica
            .outbox(&mut a, u32::MAX)
            .await
            .unwrap()
            .changes
            .into_iter()
            .filter(|c| c.collection == "series" && c.field == "name")
            .map(|c| c.value)
            .collect();
        eprintln!("ZZM {rows:?}: {names:?} sent {sent:?}");
        assert!(
            sent.contains(&fongo_sync_sqlite::core::Value::Text("edited".into())),
            "{rows:?}: {names:?} {sent:?}"
        );
    }
}

//! Collecting in bounded steps records what one collect of the same rows records. The same world
//! is built again for each step size, from one seed, so every run reaches the collect under test
//! in the same state; that collect is then made whole, or in steps of a few rows until none are
//! left, and the outcomes are compared: the changes sent, what the replica holds, its rows and
//! images, and what another replica shows once it applies them.
//!
//! Gids and clocks are compared exactly, but where a step aliases entities found to be one: an
//! alias takes the clock of the step that found it, so later clocks move on. Then the changes are
//! compared in clock order, aliases as a set, and registers by value.

mod support;

use std::collections::BTreeSet;

use fongo_sync_sqlite::core::{ALIAS, Change, Value};
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
];

const TABLES: &[&str] = &[
    "shelves", "authors", "books", "notes", "marks", "tags", "settings", "covers",
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
    match kind % 30 {
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
        _ => one(format!(
            "UPDATE books SET extra = 'x{}' WHERE id = {book}",
            b % 3
        )),
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
    /// Writes on replica 0 or 1 (by the flag), or with `None` an exchange between the two.
    history: Vec<Option<(bool, u8, u8, u8)>>,
    /// Writes on replica 0 not yet collected when the operation under test runs.
    burst: Vec<(u8, u8, u8)>,
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
    let world = World::new(case.seed);
    let mut a = world.connect(0).await;
    let mut b = world.connect(1).await;
    for sql in SCHEMA {
        sqlx::query(sql).execute(&mut a).await.unwrap();
        sqlx::query(sql).execute(&mut b).await.unwrap();
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
        run(&mut a, &burst).await;
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
        run(&mut a, &burst).await;
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
        let aliases = |o: &Outcome| {
            o.outbox
                .iter()
                .filter(|c| c.field == ALIAS)
                .map(|c| format!("{} {:?} {:?}", c.collection, c.gid, c.value))
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
        proptest::collection::vec(
            prop_oneof![
                1 => Just(None),
                6 => (any::<bool>(), any::<u8>(), 0u8..8, any::<u8>())
                    .prop_map(|(b, k, x, y)| Some((b, k, x, y))),
            ],
            0..40,
        ),
        proptest::collection::vec(arb_write(), 0..40),
    )
        .prop_map(|(seed, mode, history, burst)| Case {
            seed,
            mode,
            history,
            burst,
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
    for mode in [Mode::First, Mode::Collect, Mode::Upgrade] {
        let history: Vec<_> = (0..30u8)
            .flat_map(|k| [Some((k % 3 == 0, k, k, k + 1)), Some((false, 3, k, k))])
            .chain([None])
            .collect();
        for kind in 0..30u8 {
            let burst = vec![(kind, 1, 2), (kind, 2, 5), (kind, 0, 7)];
            check(&Case {
                seed: u64::from(kind),
                mode,
                history: history.clone(),
                burst,
            })
            .unwrap();
        }
        let burst: Vec<_> = (0..90u8).map(|i| (i % 30, i / 30, i)).collect();
        check(&Case {
            seed: 7,
            mode,
            history,
            burst,
        })
        .unwrap();
    }
}

/// Rows that do not depend on one another are collected `rows` at a time, the earliest written
/// first, and the last step says no more are left.
#[tokio::test]
async fn a_step_takes_as_many_independent_rows_as_asked() {
    let world = World::new(3);
    let mut conn = world.connect(0).await;
    for sql in SCHEMA {
        sqlx::query(sql).execute(&mut conn).await.unwrap();
    }
    let replica = Replica::new(registry(false));
    replica.install(&mut conn).await.unwrap();
    sqlx::query(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 95)
         INSERT INTO settings SELECT 'key ' || i, 'value' FROM n",
    )
    .execute(&mut conn)
    .await
    .unwrap();
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
                seed: i as u64,
                mode,
                history: vec![None],
                burst: burst.to_vec(),
            })
            .unwrap();
        }
    }
    check(&Case {
        seed: 2143016055923739986,
        mode: Mode::Collect,
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

//! How long the kernel takes over a large library, printed as a table so runs can be compared:
//! `make bench`. Ignored by default; meaningful only in a release build. One test that is not
//! ignored checks, by counting statements and SQLite's work rather than time, that a small batch
//! costs no more in a large library than in a small one.

mod support;

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use fongo_sync_sqlite::core::{Change, DELETED, Gid, Hlc, Value};
use fongo_sync_sqlite::{Collection, Registry, Replica};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};
use support::statements::Statements;

const ROWS: i64 = 50_000;
const DIRTY: i64 = 3_000;
const GROUP: u8 = 200;

const SCHEMA: &[&str] = &[
    "CREATE TABLE books (
        id INTEGER PRIMARY KEY, title TEXT, isbn TEXT UNIQUE, year INTEGER, pages INTEGER)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        body TEXT)",
    "CREATE INDEX notes_book ON notes (book_id)",
    "CREATE TABLE tags (
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        tag TEXT NOT NULL,
        note TEXT,
        PRIMARY KEY (book_id, tag))",
];

fn registry() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("isbn")
            .field("year")
            .field("pages")
            .identity(&["isbn"]),
        Collection::minted("notes", "notes", "id")
            .owner("book_id", "books")
            .field("body"),
        Collection::natural("tags", "tags", &["book_id", "tag"])
            .owner("book_id", "books")
            .field("tag")
            .field("note"),
    ])
    .unwrap()
}

async fn database() -> SqliteConnection {
    let mut conn = SqliteConnectOptions::from_str("sqlite::memory:")
        .unwrap()
        .foreign_keys(true)
        .connect()
        .await
        .unwrap();
    for sql in SCHEMA {
        sqlx::query(sql).execute(&mut conn).await.unwrap();
    }
    conn
}

async fn run(conn: &mut SqliteConnection, sql: &str) {
    sqlx::query(sql).execute(conn).await.unwrap();
}

async fn drain(replica: &Replica, conn: &mut SqliteConnection) -> Vec<Change> {
    let outbox = replica.outbox(conn, u32::MAX).await.unwrap();
    replica.acknowledge(conn, outbox.through).await.unwrap();
    outbox.changes
}

struct Table(Vec<(String, Duration)>);

impl Table {
    fn time(&mut self, what: &str, started: Instant) {
        self.0.push((what.to_string(), started.elapsed()));
    }
}

/// Two replicas of a library of `books` books, each with a note and a tag, the second made by
/// applying what the first sent.
async fn library(replica: &Replica, books: i64) -> (SqliteConnection, SqliteConnection) {
    let mut a = database().await;
    run(
        &mut a,
        &format!(
            "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < {books})
             INSERT INTO books (title, isbn, year, pages) SELECT 'title ' || x, 'isbn ' || x, x, x FROM n"
        ),
    )
    .await;
    run(
        &mut a,
        "INSERT INTO notes (book_id, body) SELECT id, 'note' FROM books",
    )
    .await;
    run(
        &mut a,
        "INSERT INTO tags (book_id, tag) SELECT id, 'read' FROM books",
    )
    .await;
    replica.install(&mut a).await.unwrap();
    replica.collect(&mut a).await.unwrap();
    let all = drain(replica, &mut a).await;
    let mut b = database().await;
    replica.install(&mut b).await.unwrap();
    replica.apply(&mut b, &all).await.unwrap();
    (a, b)
}

/// A few edits of every kind the first replica makes, as one batch.
async fn small_batch(replica: &Replica, a: &mut SqliteConnection) -> Vec<Change> {
    run(
        a,
        "UPDATE books SET title = title || '!' WHERE id IN (2, 3)",
    )
    .await;
    run(
        a,
        "INSERT INTO notes (book_id, body) VALUES (4, 'more'), (5, 'more')",
    )
    .await;
    run(a, "DELETE FROM books WHERE id = 6").await;
    run(a, "INSERT INTO tags (book_id, tag) VALUES (7, 'new')").await;
    run(a, "UPDATE tags SET note = 'n' WHERE book_id = 8").await;
    replica.collect(a).await.unwrap();
    drain(replica, a).await
}

/// Counts SQLite's virtual machine work on a connection, in units of 64 instructions.
async fn count_work(conn: &mut SqliteConnection) -> Arc<AtomicU64> {
    let work = Arc::new(AtomicU64::new(0));
    let counter = work.clone();
    let mut handle = support::lock(conn).await;
    handle.set_progress_handler(64, move || {
        counter.fetch_add(1, Ordering::Relaxed);
        true
    });
    work
}

/// Statements and work of applying a small batch, then collecting a one-row edit, in a library
/// of `books` books.
async fn small_costs(books: i64) -> [(u64, u64); 2] {
    let replica = Replica::new(registry());
    let (mut a, mut b) = library(&replica, books).await;
    let batch = small_batch(&replica, &mut a).await;
    let statements = Statements::watch(&mut b).await;
    let work = count_work(&mut b).await;
    replica.apply(&mut b, &batch).await.unwrap();
    let applied = (statements.count(), work.swap(0, Ordering::Relaxed));
    let mut texts = statements.texts();
    run(&mut b, "UPDATE books SET year = 1871 WHERE id = 9").await;
    statements.reset();
    replica.collect(&mut b).await.unwrap();
    let collected = (statements.count(), work.load(Ordering::Relaxed));
    texts.extend(statements.texts());
    // Every statement looks kernel tables up by an index, but for the marks of what to collect
    // and what waits, which are as long as the work they stand for, and a test for any pin.
    for text in texts.iter().filter(|t| !t.starts_with("--")) {
        let detail: Vec<String> = support::rows(&mut b, &format!("EXPLAIN QUERY PLAN {text}"), &[])
            .await
            .into_iter()
            .map(|row| format!("{:?}", row[3]))
            .collect();
        let scans: Vec<&String> = detail
            .iter()
            .filter(|d| d.contains("SCAN") && d.contains("fongo_"))
            .filter(|d| !d.contains("fongo_dirty") && !d.contains("fongo_held"))
            .filter(|_| !text.starts_with("SELECT 1 FROM fongo_pins LIMIT 1"))
            .collect();
        assert!(
            scans.is_empty(),
            "{text} reads a whole kernel table: {detail:?}"
        );
    }
    [applied, collected]
}

#[tokio::test]
async fn a_small_batch_costs_no_more_in_a_larger_library() {
    let small = small_costs(500).await;
    let large = small_costs(4_000).await;
    for ((what, s), l) in ["apply", "collect"].iter().zip(small).zip(large) {
        eprintln!(
            "{what}: {} statements, work {}; then {} and {}",
            s.0, s.1, l.0, l.1
        );
        assert!(l.0 <= s.0 + 10, "{what}: {} statements, then {}", s.0, l.0);
        assert!(
            l.1 <= s.1 + s.1 / 2 + 10,
            "{what}: work {}, then {}",
            s.1,
            l.1
        );
    }
}

/// Statements and work of an operation, with the statements it ran most for a failure message.
struct Cost {
    statements: u64,
    work: u64,
    busiest: String,
}

impl Cost {
    fn of(statements: &Statements, work: &AtomicU64) -> Cost {
        let cost = Cost {
            statements: statements.count(),
            work: work.swap(0, Ordering::Relaxed),
            busiest: statements.busiest(8),
        };
        statements.reset();
        cost
    }
}

/// Fails unless `large`, for `times` as much, costs about `times` as much as `small`.
fn near_linear(what: &str, times: u64, small: &Cost, large: &Cost) {
    eprintln!(
        "{what}: {} statements, work {}; {times} times as much: {} and {}",
        small.statements, small.work, large.statements, large.work
    );
    assert!(
        large.statements <= small.statements * times * 11 / 10 + 50,
        "{what}: {} statements, then {} for {times} times as much. The most run:\n{}",
        small.statements,
        large.statements,
        large.busiest
    );
    assert!(
        large.work <= small.work * times * 3 / 2 + 50,
        "{what}: work {}, then {} for {times} times as much. The most run:\n{}",
        small.work,
        large.work,
        large.busiest
    );
}

/// Collecting and applying `n` natural children of one parent, which share the first part of
/// their key.
async fn children_costs(n: u32) -> [Cost; 2] {
    let replica = Replica::new(registry());
    let (mut a, mut b) = library(&replica, 10).await;
    run(
        &mut a,
        &format!(
            "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < {n})
             INSERT INTO tags (book_id, tag) SELECT 2, 'tag ' || x FROM n"
        ),
    )
    .await;
    let (sa, wa) = (Statements::watch(&mut a).await, count_work(&mut a).await);
    replica.collect(&mut a).await.unwrap();
    let collected = Cost::of(&sa, &wa);
    let tags = drain(&replica, &mut a).await;
    let (sb, wb) = (Statements::watch(&mut b).await, count_work(&mut b).await);
    replica.apply(&mut b, &tags).await.unwrap();
    [collected, Cost::of(&sb, &wb)]
}

#[tokio::test]
async fn natural_children_of_one_parent_cost_in_proportion_to_their_number() {
    let small = children_costs(150).await;
    let large = children_costs(600).await;
    for ((what, s), l) in ["collect", "apply"].iter().zip(&small).zip(&large) {
        near_linear(what, 4, s, l);
    }
}

/// Applying one change to a member of `size` entities found to be one, then collecting an edit to
/// the group's row. Forming the group is not counted: each entity's clashes are asked of every
/// other, so the rows read grow as the square of the entities that clash at once.
async fn group_costs(size: u8) -> [Cost; 2] {
    let replica = Replica::new(registry());
    let mut c = database().await;
    replica.install(&mut c).await.unwrap();
    let at = |n: u32| Hlc {
        wall_ms: 1_700_000_000_000,
        counter: n,
        node: Gid([1; 16]),
    };
    let mut group = Vec::new();
    for i in 0..size {
        let n = u32::from(i) * 3;
        for (field, value, k) in [
            (DELETED, Value::Int(0), n),
            ("title", Value::Text(format!("copy {i}")), n + 1),
            ("isbn", Value::Text("one".into()), n + 2),
        ] {
            group.push(Change {
                collection: "books".into(),
                gid: Gid([i; 16]),
                field: field.into(),
                value,
                clock: at(k),
            });
        }
    }
    replica.apply(&mut c, &group).await.unwrap();
    let (st, work) = (Statements::watch(&mut c).await, count_work(&mut c).await);
    let one = Change {
        collection: "books".into(),
        gid: Gid([size / 2; 16]),
        field: "title".into(),
        value: Value::Text("the one".into()),
        clock: at(10_000),
    };
    replica.apply(&mut c, &[one]).await.unwrap();
    let applied = Cost::of(&st, &work);
    run(&mut c, "UPDATE books SET year = 1871").await;
    st.reset();
    replica.collect(&mut c).await.unwrap();
    [applied, Cost::of(&st, &work)]
}

#[tokio::test]
async fn a_group_costs_in_proportion_to_its_members() {
    let small = group_costs(40).await;
    let large = group_costs(160).await;
    for ((what, s), l) in ["apply to a member", "collect the row"]
        .iter()
        .zip(&small)
        .zip(&large)
    {
        near_linear(what, 4, s, l);
    }
}

#[tokio::test]
#[ignore = "a benchmark: make bench"]
async fn timings() {
    let mut table = Table(Vec::new());
    let replica = Replica::new(registry());

    // A library of books adopted by a first install.
    let mut a = database().await;
    run(
        &mut a,
        &format!(
            "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < {ROWS})
             INSERT INTO books (title, isbn, year, pages) SELECT 'title ' || x, 'isbn ' || x, x, x FROM n"
        ),
    )
    .await;
    let started = Instant::now();
    replica.install(&mut a).await.unwrap();
    table.time("install adopting 50,000 rows", started);
    let started = Instant::now();
    replica.collect(&mut a).await.unwrap();
    table.time("first collect of 50,000 rows", started);
    let adopted = drain(&replica, &mut a).await;

    let mut b = database().await;
    replica.install(&mut b).await.unwrap();
    let started = Instant::now();
    replica.apply(&mut b, &adopted).await.unwrap();
    table.time(
        &format!("apply of {} changes to a fresh replica", adopted.len()),
        started,
    );

    // Edits to some of the rows.
    run(
        &mut a,
        &format!("UPDATE books SET title = title || '!' WHERE id <= {DIRTY}"),
    )
    .await;
    let started = Instant::now();
    replica.collect(&mut a).await.unwrap();
    table.time("collect of 3,000 edited rows", started);
    let edits = drain(&replica, &mut a).await;
    let started = Instant::now();
    replica.apply(&mut b, &edits).await.unwrap();
    table.time("apply of those edits", started);

    // One book owning many notes, deleted.
    run(
        &mut a,
        &format!(
            "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < {ROWS})
             INSERT INTO notes (book_id, body) SELECT 1, 'note ' || x FROM n"
        ),
    )
    .await;
    replica.collect(&mut a).await.unwrap();
    let notes = drain(&replica, &mut a).await;
    replica.apply(&mut b, &notes).await.unwrap();
    run(&mut a, "DELETE FROM books WHERE id = 1").await;
    let started = Instant::now();
    replica.collect(&mut a).await.unwrap();
    table.time("collect of a delete of an owner of 50,000 rows", started);
    let delete = drain(&replica, &mut a).await;
    let started = Instant::now();
    replica.apply(&mut b, &delete).await.unwrap();
    table.time("apply of that delete", started);

    // Many tags of one book: natural keys that share their first part.
    run(
        &mut a,
        "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 10000)
             INSERT INTO tags (book_id, tag) SELECT 2, 'tag ' || x FROM n",
    )
    .await;
    replica.collect(&mut a).await.unwrap();
    let tags = drain(&replica, &mut a).await;
    let started = Instant::now();
    replica.apply(&mut b, &tags).await.unwrap();
    table.time("apply of 10,000 tags of one book", started);
    run(
        &mut a,
        "INSERT INTO tags (book_id, tag) VALUES (2, 'one more')",
    )
    .await;
    run(
        &mut a,
        "UPDATE tags SET note = 'n' WHERE book_id = 2 AND tag = 'tag 7'",
    )
    .await;
    let started = Instant::now();
    replica.collect(&mut a).await.unwrap();
    table.time("collect of two tag edits beside 10,000 tags", started);
    let tag_edits = drain(&replica, &mut a).await;
    let started = Instant::now();
    replica.apply(&mut b, &tag_edits).await.unwrap();
    table.time("apply of those edits", started);

    // Many entities found to be one.
    let mut c = database().await;
    replica.install(&mut c).await.unwrap();
    let at = |n: u32| Hlc {
        wall_ms: 1_700_000_000_000,
        counter: n,
        node: Gid([1; 16]),
    };
    let mut group = Vec::new();
    for i in 0..GROUP {
        let gid = Gid([i; 16]);
        let n = u32::from(i) * 3;
        for (field, value, k) in [
            (DELETED, Value::Int(0), n),
            ("title", Value::Text(format!("copy {i}")), n + 1),
            ("isbn", Value::Text("one".into()), n + 2),
        ] {
            group.push(Change {
                collection: "books".into(),
                gid,
                field: field.into(),
                value,
                clock: at(k),
            });
        }
    }
    let started = Instant::now();
    replica.apply(&mut c, &group).await.unwrap();
    table.time("apply of 200 entities with one identity", started);
    let joined = replica.inspect(&mut c).await.unwrap().aliases.len();
    assert_eq!(
        joined,
        usize::from(GROUP),
        "the entities did not become one group"
    );
    let one = Change {
        collection: "books".into(),
        gid: Gid([GROUP / 2; 16]),
        field: "title".into(),
        value: Value::Text("the one".into()),
        clock: at(10_000),
    };
    let started = Instant::now();
    replica.apply(&mut c, &[one]).await.unwrap();
    table.time("apply of one change to a member of that group", started);
    run(&mut c, "UPDATE books SET year = 1871").await;
    let started = Instant::now();
    replica.collect(&mut c).await.unwrap();
    table.time("collect of an edit to that group's row", started);

    // A small batch, and a one-row edit, in libraries of two sizes.
    for books in [10_000, 100_000] {
        let started = Instant::now();
        let (mut a, mut b) = library(&replica, books).await;
        table.time(
            &format!("first sync of {books} books with a note and a tag each"),
            started,
        );
        let batch = small_batch(&replica, &mut a).await;
        let started = Instant::now();
        replica.apply(&mut b, &batch).await.unwrap();
        table.time(
            &format!("apply of {} changes to {books} books", batch.len()),
            started,
        );
        run(&mut b, "UPDATE books SET year = 1871 WHERE id = 9").await;
        let started = Instant::now();
        replica.collect(&mut b).await.unwrap();
        table.time(
            &format!("collect of a one-row edit in {books} books"),
            started,
        );
    }

    println!("\n{:<60} {:>12}", "operation", "ms");
    for (what, took) in &table.0 {
        println!("{what:<60} {:>12.1}", took.as_secs_f64() * 1000.0);
    }
}

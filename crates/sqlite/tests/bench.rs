//! How long the kernel takes over a large library, printed as a table so runs can be compared:
//! `make bench`. Ignored by default; meaningful only in a release build.

use std::str::FromStr;
use std::time::{Duration, Instant};

use fongo_sync_sqlite::core::{Change, DELETED, Gid, Hlc, Value};
use fongo_sync_sqlite::{Collection, Registry, Replica};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};

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

#[tokio::test]
#[ignore = "a benchmark: make bench"]
async fn timings() {
    let mut table = Table(Vec::new());
    let replica = Replica::new(registry());

    // A library of books, each with one note, adopted by a first install.
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
    let library = drain(&replica, &mut a).await;

    let mut b = database().await;
    replica.install(&mut b).await.unwrap();
    let started = Instant::now();
    replica.apply(&mut b, &library).await.unwrap();
    table.time(
        &format!("apply of {} changes to a fresh replica", library.len()),
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

    println!("\n{:<50} {:>12}", "operation", "ms");
    for (what, took) in &table.0 {
        println!("{what:<50} {:>12.1}", took.as_secs_f64() * 1000.0);
    }
}

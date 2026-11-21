//! An apply or a collect ended at any statement leaves the database as it was, and doing it again
//! gives what it would have given had it never been interrupted. Each run is made from one seed,
//! so the run interrupted at the `n`th statement starts from the same state as every other.

mod support;

use fongo_sync_sqlite::core::Change;
use fongo_sync_sqlite::{Collection, Registry, Replica};
use sqlx::SqliteConnection;
use support::statements::Statements;
use support::world::World;

const SCHEMA: &[&str] = &[
    "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT NOT NULL, isbn TEXT UNIQUE)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        body TEXT)",
    "CREATE TABLE tags (
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        tag TEXT NOT NULL,
        PRIMARY KEY (book_id, tag))",
];

fn registry() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("isbn")
            .identity(&["isbn"]),
        Collection::minted("notes", "notes", "id")
            .owner("book_id", "books")
            .field("body"),
        Collection::natural("tags", "tags", &["book_id", "tag"])
            .owner("book_id", "books")
            .field("tag"),
    ])
    .unwrap()
}

async fn run(conn: &mut SqliteConnection, sql: &str) {
    sqlx::query(sql).execute(conn).await.unwrap();
}

/// Every row of every table, the kernel's included.
async fn dump(conn: &mut SqliteConnection) -> Vec<String> {
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY 1",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
    let mut out = Vec::new();
    for t in tables {
        for row in support::rows(conn, &format!("SELECT * FROM \"{t}\""), &[]).await {
            out.push(format!("{t} {row:?}"));
        }
    }
    out.sort();
    out
}

/// The application's rows.
async fn shown(conn: &mut SqliteConnection) -> Vec<String> {
    let mut out = Vec::new();
    for t in ["books", "notes", "tags"] {
        for row in support::rows(conn, &format!("SELECT * FROM {t}"), &[]).await {
            out.push(format!("{t} {row:?}"));
        }
    }
    out.sort();
    out
}

/// Two replicas: the first has made a library and sent it; the second has made a book with the
/// same isbn, edited and removed rows, and collected none of it. Returns the second and what the
/// first sent.
async fn setting(world: &World) -> (SqliteConnection, Replica, Vec<Change>) {
    let replica = Replica::new(registry());
    let mut a = world.connect(0).await;
    let mut b = world.connect(1).await;
    for conn in [&mut a, &mut b] {
        for sql in SCHEMA {
            run(conn, sql).await;
        }
        replica.install(conn).await.unwrap();
    }
    run(
        &mut a,
        "INSERT INTO books (title, isbn) VALUES ('Middlemarch', 'x'), ('Romola', 'y')",
    )
    .await;
    run(
        &mut a,
        "INSERT INTO notes (book_id, body) VALUES (1, 'slow'), (2, 'quick')",
    )
    .await;
    run(
        &mut a,
        "INSERT INTO tags (book_id, tag) VALUES (1, 'classic'), (2, 'italy')",
    )
    .await;
    replica.collect(&mut a).await.unwrap();
    let first = replica.outbox(&mut a, u32::MAX).await.unwrap().changes;
    replica.apply(&mut b, &first).await.unwrap();
    run(&mut a, "UPDATE books SET title = 'Romola!' WHERE id = 2").await;
    run(&mut a, "DELETE FROM notes WHERE id = 1").await;
    replica.collect(&mut a).await.unwrap();
    let all = replica.outbox(&mut a, u32::MAX).await.unwrap().changes;
    let second = all[first.len()..].to_vec();
    run(
        &mut b,
        "INSERT INTO books (title, isbn) VALUES ('Felix Holt', 'z')",
    )
    .await;
    run(
        &mut b,
        "INSERT INTO notes (book_id, body) VALUES (3, 'mine')",
    )
    .await;
    run(
        &mut b,
        "INSERT INTO tags (book_id, tag) VALUES (3, 'classic')",
    )
    .await;
    run(
        &mut b,
        "UPDATE books SET isbn = 'w' WHERE title = 'Middlemarch'",
    )
    .await;
    run(
        &mut b,
        "UPDATE books SET isbn = 'x' WHERE title = 'Felix Holt'",
    )
    .await;
    run(&mut b, "DELETE FROM tags WHERE tag = 'italy'").await;
    (b, replica, second)
}

#[derive(Clone, Copy, Debug)]
enum Op {
    Apply,
    Collect,
}

async fn perform(op: Op, replica: &Replica, conn: &mut SqliteConnection, batch: &[Change]) -> bool {
    match op {
        Op::Apply => replica.apply(conn, batch).await.is_ok(),
        Op::Collect => replica.collect(conn).await.is_ok(),
    }
}

async fn every_statement(op: Op) -> u64 {
    let (mut conn, replica, batch) = setting(&World::new(7)).await;
    assert!(perform(op, &replica, &mut conn, &batch).await);
    let expected = shown(&mut conn).await;
    let mut n = 1;
    loop {
        let (mut conn, replica, batch) = setting(&World::new(7)).await;
        let before = dump(&mut conn).await;
        let statements = Statements::watch(&mut conn).await;
        statements.stop_at(n);
        if perform(op, &replica, &mut conn, &batch).await {
            assert!(n > 1, "{op:?} ran no statement");
            return n - 1;
        }
        statements.stop_at(0);
        assert_eq!(
            dump(&mut conn).await,
            before,
            "{op:?} stopped at statement {n} left a change"
        );
        assert!(
            perform(op, &replica, &mut conn, &batch).await,
            "{op:?} again after {n}"
        );
        assert_eq!(
            shown(&mut conn).await,
            expected,
            "{op:?} again after statement {n}"
        );
        n += 1;
    }
}

#[tokio::test]
async fn an_apply_stopped_at_any_statement_changes_nothing_and_completes_when_run_again() {
    let statements = every_statement(Op::Apply).await;
    eprintln!("apply: {statements} statements");
    assert!(statements > 50, "{statements}");
}

#[tokio::test]
async fn a_collect_stopped_at_any_statement_changes_nothing_and_completes_when_run_again() {
    let statements = every_statement(Op::Collect).await;
    eprintln!("collect: {statements} statements");
    assert!(statements > 20, "{statements}");
}

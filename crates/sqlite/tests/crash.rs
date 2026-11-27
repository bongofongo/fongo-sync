//! An apply, a collect, a suspend or a resume ended at any statement leaves the database as it
//! was, and doing it again gives what it would have given had it never been interrupted. Each run is made from one seed,
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

/// Every object in the schema and every row of every table, the kernel's included.
async fn dump(conn: &mut SqliteConnection) -> Vec<String> {
    let mut out: Vec<String> = support::rows(conn, "SELECT * FROM sqlite_master", &[])
        .await
        .into_iter()
        .map(|row| format!("sqlite_master {row:?}"))
        .collect();
    let tables: Vec<String> = sqlx::query_scalar(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY 1",
    )
    .fetch_all(&mut *conn)
    .await
    .unwrap();
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

/// A replica whose tables hold rows written before its first install, with the triggers
/// suspended and rows changed meanwhile, as [`Steps::Resume`] finds it, or not yet installed.
async fn found(world: &World, install: bool) -> (SqliteConnection, Replica) {
    let replica = Replica::new(registry());
    let mut c = world.connect(2).await;
    for sql in SCHEMA {
        run(&mut c, sql).await;
    }
    run(
        &mut c,
        "INSERT INTO books (title, isbn) VALUES ('Middlemarch', 'x'), ('Romola', NULL), ('Felix Holt', 'z')",
    )
    .await;
    run(
        &mut c,
        "INSERT INTO notes (book_id, body) VALUES (1, 'slow'), (2, 'quick'), (3, 'mine')",
    )
    .await;
    run(
        &mut c,
        "INSERT INTO tags (book_id, tag) VALUES (1, 'classic'), (3, 'classic')",
    )
    .await;
    if install {
        replica.install(&mut c).await.unwrap();
        replica.suspend(&mut c).await.unwrap();
        run(&mut c, "UPDATE books SET title = 'Romola!' WHERE id = 2").await;
        run(&mut c, "DELETE FROM notes WHERE id = 1").await;
        run(
            &mut c,
            "INSERT INTO tags (book_id, tag) VALUES (2, 'italy')",
        )
        .await;
    }
    (c, replica)
}

/// Operations made in bounded steps of one row, or whole where they have none.
#[derive(Clone, Copy, Debug)]
enum Steps {
    /// Steps of collecting what [`setting`] left uncollected.
    Collect,
    /// An install over [`setting`], then steps of collecting.
    Install,
    /// A first install over rows found in the tables, then steps of collecting.
    First,
    /// A resume, which collects whole.
    Resume,
    /// A suspend while a first install in steps has records waiting, which it adopts first.
    Suspend,
    /// A suspend after a table was renamed away and written, whose marks it sets aside.
    SuspendAway,
    /// A resume after a table was renamed away and written, with no suspend: it sets the marks
    /// aside and compares the table.
    ResumeAway,
}

async fn begin(op: Steps) -> (SqliteConnection, Replica) {
    let world = World::new(7);
    match op {
        Steps::Collect | Steps::Install => {
            let (conn, replica, _) = setting(&world).await;
            (conn, replica)
        }
        Steps::First => found(&world, false).await,
        Steps::Resume => found(&world, true).await,
        Steps::Suspend => {
            let (mut conn, replica) = found(&world, false).await;
            assert!(replica.install_step(&mut conn, 1).await.unwrap().more);
            (conn, replica)
        }
        Steps::SuspendAway | Steps::ResumeAway => away(&world).await,
    }
}

/// A replica whose `books` were collected, then edited without a collect, then renamed away and
/// copied into a new table of the name, the old table kept and written: its capture triggers,
/// gone with it, marked those writes as the collection's.
async fn away(world: &World) -> (SqliteConnection, Replica) {
    let (mut c, replica) = found(world, false).await;
    replica.install(&mut c).await.unwrap();
    replica.collect(&mut c).await.unwrap();
    for sql in [
        "UPDATE books SET title = 'Daniel Deronda' WHERE id = 1",
        "PRAGMA foreign_keys = OFF",
        "PRAGMA legacy_alter_table = ON",
        "ALTER TABLE books RENAME TO books_old",
        "PRAGMA legacy_alter_table = OFF",
        "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT NOT NULL, isbn TEXT UNIQUE)",
        "INSERT INTO books SELECT id, upper(title), isbn FROM books_old",
        "UPDATE books_old SET isbn = 'old' WHERE id = 1",
        "DELETE FROM books_old WHERE id = 3",
        "PRAGMA foreign_keys = ON",
    ] {
        run(&mut c, sql).await;
    }
    (c, replica)
}

/// The `i`th call of `op`, and whether more follow it.
async fn call(op: Steps, i: usize, replica: &Replica, conn: &mut SqliteConnection) -> Option<bool> {
    let done = match (op, i) {
        (Steps::Install | Steps::First, 0) => replica.install_step(conn, 1).await,
        (Steps::Resume, _) => {
            return replica
                .resume(conn, fongo_sync_sqlite::Resume::Send)
                .await
                .ok()
                .map(|()| false);
        }
        (Steps::Suspend | Steps::SuspendAway, _) => {
            return replica.suspend(conn).await.ok().map(|()| false);
        }
        (Steps::ResumeAway, _) => {
            return replica
                .resume(conn, fongo_sync_sqlite::Resume::Send)
                .await
                .ok()
                .map(|()| false);
        }
        _ => replica.collect_step(conn, 1).await,
    };
    done.ok().map(|collected| collected.more)
}

/// What was sent, without the gids and clocks a call made again mints anew.
async fn sent(replica: &Replica, conn: &mut SqliteConnection) -> Vec<String> {
    let mut out: Vec<String> = replica
        .outbox(conn, u32::MAX)
        .await
        .unwrap()
        .changes
        .iter()
        .map(|c| {
            let value = match &c.value {
                fongo_sync_sqlite::core::Value::Ref(_) => "a reference".to_string(),
                other => format!("{other:?}"),
            };
            format!("{} {} {value}", c.collection, c.field)
        })
        .collect();
    out.sort();
    out
}

/// Runs `op`'s calls from the `from`th until none is left; the number of calls.
async fn finish(op: Steps, from: usize, replica: &Replica, conn: &mut SqliteConnection) -> usize {
    let mut i = from;
    loop {
        let more = call(op, i, replica, conn).await.expect("a call fails");
        i += 1;
        if !more {
            return i;
        }
    }
}

/// Whether capture is suspended as `op` leaves it once finished.
async fn suspended(
    op: Steps,
    replica: &Replica,
    conn: &mut SqliteConnection,
) -> Result<(), String> {
    let suspended = replica.suspended(conn).await.map_err(|e| e.to_string())?;
    if suspended != matches!(op, Steps::Suspend | Steps::SuspendAway) {
        return Err(format!("{op:?} left capture suspended: {suspended}"));
    }
    Ok(())
}

/// Stops each call of `op` at each of its statements in turn: a call stopped changes nothing,
/// and the calls that follow, that one first, finish with the rows and sent changes of a run never
/// stopped, nothing left to collect, and capture suspended only by a suspend. Returns the
/// statements counted.
async fn every_statement_of_every_step(op: Steps) -> u64 {
    let (mut conn, replica) = begin(op).await;
    let calls = finish(op, 0, &replica, &mut conn).await;
    let expected = (shown(&mut conn).await, sent(&replica, &mut conn).await);
    assert!(
        replica
            .inspect(&mut conn)
            .await
            .unwrap()
            .uncollected
            .is_empty(),
        "{op:?} left rows to collect"
    );
    suspended(op, &replica, &mut conn).await.unwrap();
    let mut total = 0;
    for i in 0..calls {
        let (mut conn, replica) = begin(op).await;
        for j in 0..i {
            call(op, j, &replica, &mut conn).await.unwrap();
        }
        let statements = Statements::watch(&mut conn).await;
        call(op, i, &replica, &mut conn).await.unwrap();
        let count = statements.count();
        total += count;
        for n in 1..=count {
            let (mut conn, replica) = begin(op).await;
            for j in 0..i {
                call(op, j, &replica, &mut conn).await.unwrap();
            }
            let before = dump(&mut conn).await;
            let statements = Statements::watch(&mut conn).await;
            statements.stop_at(n);
            let stopped = call(op, i, &replica, &mut conn).await.is_none();
            statements.stop_at(0);
            let from = if stopped {
                assert_eq!(
                    dump(&mut conn).await,
                    before,
                    "{op:?} call {i} stopped at statement {n} left a change"
                );
                i
            } else {
                i + 1
            };
            if from < calls {
                finish(op, from, &replica, &mut conn).await;
            }
            assert_eq!(
                (shown(&mut conn).await, sent(&replica, &mut conn).await),
                expected,
                "{op:?} call {i} stopped at statement {n}, then finished"
            );
            assert!(
                replica
                    .inspect(&mut conn)
                    .await
                    .unwrap()
                    .uncollected
                    .is_empty(),
                "{op:?} call {i} stopped at statement {n} left rows to collect"
            );
            suspended(op, &replica, &mut conn)
                .await
                .unwrap_or_else(|e| panic!("call {i} stopped at statement {n}: {e}"));
        }
    }
    eprintln!("{op:?}: {calls} calls, {total} statements");
    assert!(
        calls > 1
            || matches!(
                op,
                Steps::Resume
                    | Steps::First
                    | Steps::Suspend
                    | Steps::SuspendAway
                    | Steps::ResumeAway
            ),
        "{op:?} took one step"
    );
    total
}

#[tokio::test]
async fn a_step_of_collecting_stopped_at_any_statement_changes_nothing_and_the_steps_finish() {
    every_statement_of_every_step(Steps::Collect).await;
}

#[tokio::test]
async fn a_step_of_installing_stopped_at_any_statement_changes_nothing_and_the_steps_finish() {
    every_statement_of_every_step(Steps::Install).await;
    every_statement_of_every_step(Steps::First).await;
}

#[tokio::test]
async fn a_resume_stopped_at_any_statement_changes_nothing_and_completes_when_run_again() {
    every_statement_of_every_step(Steps::Resume).await;
}

#[tokio::test]
async fn a_suspend_stopped_at_any_statement_changes_nothing_and_completes_when_run_again() {
    every_statement_of_every_step(Steps::Suspend).await;
}

#[tokio::test]
async fn setting_marks_aside_stopped_at_any_statement_changes_nothing_and_completes_when_run_again()
{
    every_statement_of_every_step(Steps::SuspendAway).await;
    every_statement_of_every_step(Steps::ResumeAway).await;
}

/// Polls `future` until it has been pending `polls` times, then drops it. Whether it finished.
async fn dropped_after<F: std::future::Future>(future: F, polls: usize) -> Option<F::Output> {
    let mut future = std::pin::pin!(future);
    let mut pending = 0;
    std::future::poll_fn(|cx| match future.as_mut().poll(cx) {
        std::task::Poll::Ready(out) => std::task::Poll::Ready(Some(out)),
        std::task::Poll::Pending if pending == polls => std::task::Poll::Ready(None),
        std::task::Poll::Pending => {
            pending += 1;
            std::task::Poll::Pending
        }
    })
    .await
}

/// Each call of `op` dropped at each point it waits: the database is as before the call or as
/// after it, the connection still serves, and the calls that follow finish the job.
async fn every_suspension_of_every_step(op: Steps) {
    let (mut conn, replica) = begin(op).await;
    let calls = finish(op, 0, &replica, &mut conn).await;
    let expected = (shown(&mut conn).await, sent(&replica, &mut conn).await);
    for i in 0..calls {
        for polls in 0.. {
            let (mut conn, replica) = begin(op).await;
            for j in 0..i {
                call(op, j, &replica, &mut conn).await.unwrap();
            }
            let before = dump(&mut conn).await;
            if dropped_after(call(op, i, &replica, &mut conn), polls)
                .await
                .is_some()
            {
                assert!(polls > 0, "{op:?} call {i} never waits");
                break;
            }
            let now = dump(&mut conn).await;
            let from = if now == before { i } else { i + 1 };
            if from > i && calls > i + 1 {
                finish(op, from, &replica, &mut conn).await;
            } else if from == i {
                finish(op, i, &replica, &mut conn).await;
            }
            assert_eq!(
                (shown(&mut conn).await, sent(&replica, &mut conn).await),
                expected,
                "{op:?} call {i} dropped after {polls} polls"
            );
            suspended(op, &replica, &mut conn)
                .await
                .unwrap_or_else(|e| panic!("call {i} dropped after {polls} polls: {e}"));
        }
    }
}

#[tokio::test]
async fn a_step_dropped_where_it_waits_is_whole_or_not_done_and_the_steps_finish() {
    for op in [
        Steps::Collect,
        Steps::Install,
        Steps::First,
        Steps::Resume,
        Steps::Suspend,
        Steps::SuspendAway,
        Steps::ResumeAway,
    ] {
        every_suspension_of_every_step(op).await;
    }
}

/// No transaction or savepoint of a step is left open, whether it ran with no transaction open
/// or inside the caller's, and stopped at any statement or not.
async fn no_transaction_left(conn: &mut SqliteConnection) -> Result<(), String> {
    use sqlx::Connection;
    if conn.is_in_transaction() {
        return Err("sqlx counts a transaction".into());
    }
    sqlx::query("BEGIN")
        .execute(&mut *conn)
        .await
        .map_err(|e| format!("BEGIN: {e}"))?;
    for savepoint in ["fongo", "fongo_step"] {
        if sqlx::query(&format!("RELEASE {savepoint}"))
            .execute(&mut *conn)
            .await
            .is_ok()
        {
            return Err(format!("savepoint {savepoint} left"));
        }
    }
    sqlx::query("ROLLBACK")
        .execute(&mut *conn)
        .await
        .map_err(|e| format!("ROLLBACK: {e}"))?;
    Ok(())
}

#[tokio::test]
async fn a_step_stopped_at_any_statement_leaves_no_transaction_open() {
    let (mut conn, replica) = begin(Steps::Collect).await;
    let statements = Statements::watch(&mut conn).await;
    replica.collect_step(&mut conn, 1).await.unwrap();
    let count = statements.count();
    for n in 1..=count {
        let (mut conn, replica) = begin(Steps::Collect).await;
        let statements = Statements::watch(&mut conn).await;
        statements.stop_at(n);
        let step = replica.collect_step(&mut conn, 1).await;
        statements.stop_at(0);
        no_transaction_left(&mut conn)
            .await
            .unwrap_or_else(|e| panic!("statement {n}: {e}; {step:?}"));
        let (mut conn, replica) = begin(Steps::Collect).await;
        let statements = Statements::watch(&mut conn).await;
        sqlx::query("BEGIN").execute(&mut conn).await.unwrap();
        statements.stop_at(n + 1);
        let step = replica.collect_step(&mut conn, 1).await;
        statements.stop_at(0);
        for savepoint in ["fongo", "fongo_step"] {
            assert!(
                sqlx::query(&format!("RELEASE {savepoint}"))
                    .execute(&mut conn)
                    .await
                    .is_err(),
                "in a transaction, statement {n}: savepoint {savepoint} left; {step:?}"
            );
        }
        let _ = sqlx::query("ROLLBACK").execute(&mut conn).await;
    }
}

/// A step that cannot take the write lock, or cannot commit while another connection reads,
/// fails with nothing collected and the connection sound, in either journal mode; the steps
/// then finish.
#[tokio::test]
async fn a_step_kept_from_the_lock_leaves_the_connection_sound() {
    use sqlx::ConnectOptions;
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("crash-busy-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    for journal in ["delete", "wal"] {
        let path = dir.join(format!("{journal}.db"));
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(std::time::Duration::from_millis(100));
        let mut a = options.connect().await.unwrap();
        run(&mut a, &format!("PRAGMA journal_mode = {journal}")).await;
        for sql in SCHEMA {
            run(&mut a, sql).await;
        }
        let replica = Replica::new(registry());
        replica.install(&mut a).await.unwrap();
        run(
            &mut a,
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 300)
             INSERT INTO books (title) SELECT 'Middlemarch ' || i FROM n",
        )
        .await;
        let before = replica.uncollected_rows(&mut a, usize::MAX).await.unwrap();
        let mut other = options.connect().await.unwrap();
        run(&mut other, "BEGIN IMMEDIATE").await;
        assert!(replica.collect_step(&mut a, 100).await.is_err());
        no_transaction_left(&mut a).await.unwrap();
        run(&mut other, "ROLLBACK").await;
        assert_eq!(
            replica.uncollected_rows(&mut a, usize::MAX).await.unwrap(),
            before
        );
        run(&mut other, "BEGIN").await;
        run(&mut other, "SELECT count(*) FROM books").await;
        let step = replica.collect_step(&mut a, 100).await;
        no_transaction_left(&mut a).await.unwrap();
        run(&mut other, "COMMIT").await;
        if step.is_err() {
            assert_eq!(
                replica.uncollected_rows(&mut a, usize::MAX).await.unwrap(),
                before
            );
        }
        while replica.collect_step(&mut a, 100).await.unwrap().more {}
        assert_eq!(
            replica.uncollected_rows(&mut a, usize::MAX).await.unwrap(),
            0
        );
    }
    let _ = std::fs::remove_dir_all(dir);
}

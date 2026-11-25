//! A first install in steps over an existing library ends as a whole install made when the
//! stepped one began would, followed by the same writes and an ordinary collect. The library is
//! the shape the kernel is first used with (see `support::library`). Its database is copied when
//! the install begins; the copy installs whole, the original in steps, and between its steps the
//! application writes, removes and makes rows again at their ids, on a clock that may move on,
//! stand still or go back, and a remote replica's changes may arrive. Each side then writes the
//! same and collects, and the outcomes are compared: the changes sent, in clock order, the
//! application's rows with their local ids, the kernel's mapping and images, what it holds, and
//! another replica's rows once it applies what was sent.

mod support;

use fongo_sync_sqlite::core::{Change, Value};
use fongo_sync_sqlite::{Inspection, Replica};
use proptest::prelude::*;
use proptest::test_runner::RngSeed;
use sqlx::SqliteConnection;
use support::library::{self, existing, registry, write};
use support::world::World;

#[derive(Debug, Clone)]
struct Case {
    seed: u64,
    books: u64,
    /// Rows each step adopts.
    step: usize,
    /// The application's writes, each after a step: a time to move the clock (0 stands still,
    /// negative goes back), and the write.
    writes: Vec<(i8, u64, u64, u64)>,
    /// After which write a remote replica's changes arrive, if at all.
    remote: Option<usize>,
}

#[derive(Debug, PartialEq)]
struct Outcome {
    outbox: Vec<Change>,
    tables: Vec<Vec<Vec<Value>>>,
    ids: Vec<Vec<Value>>,
    inspection: Inspection,
    elsewhere: Vec<Vec<Vec<Value>>>,
}

async fn tables(conn: &mut SqliteConnection) -> Vec<Vec<Vec<Value>>> {
    let mut out = Vec::new();
    for t in library::TABLES {
        out.push(support::rows(conn, &format!("SELECT * FROM {t} ORDER BY rowid"), &[]).await);
    }
    out
}

async fn database(world: &World, n: u64) -> SqliteConnection {
    let mut conn = world.connect(n).await;
    for sql in library::SCHEMA {
        sqlx::query(sql).execute(&mut conn).await.unwrap();
    }
    conn
}

/// Changes a replica that has a library of its own sends: some of its books, one with the isbn
/// of one here, and a highlight on it.
async fn remote(world: &World) -> Vec<Change> {
    let replica = Replica::new(registry());
    let mut c = database(world, 9).await;
    replica.install(&mut c).await.unwrap();
    for sql in [
        "INSERT INTO books (id, title, isbn_13) VALUES (1, 'theirs', '9781'), (2, 'other', '555')",
        "INSERT INTO highlights (book_id, text, hash) VALUES (1, 'their passage', 'h1')",
        "INSERT INTO book_tags (book_id, tag, source) VALUES (2, 'read', 'shelf')",
    ] {
        library::run(&mut c, sql).await;
    }
    replica.collect(&mut c).await.unwrap();
    replica.outbox(&mut c, u32::MAX).await.unwrap().changes
}

async fn finish(world: &World, replica: &Replica, conn: &mut SqliteConnection) -> Outcome {
    replica.collect(conn).await.unwrap();
    let mut outbox = replica.outbox(conn, u32::MAX).await.unwrap().changes;
    outbox.sort_by_key(|c| c.clock);
    let inspection = replica.inspect(conn).await.unwrap();
    let ids = support::rows(
        conn,
        "SELECT collection, key, gid, local, image, doomed FROM fongo_ids ORDER BY collection, key",
        &[],
    )
    .await;
    let mut other = database(world, 7).await;
    replica.install(&mut other).await.unwrap();
    replica.apply(&mut other, &outbox).await.unwrap();
    Outcome {
        outbox,
        tables: tables(conn).await,
        ids,
        inspection,
        elsewhere: tables(&mut other).await,
    }
}

async fn found_left(conn: &mut SqliteConnection) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM fongo_found WHERE batch >= 0")
        .fetch_one(&mut *conn)
        .await
        .unwrap()
}

/// The stepped install and the whole one on a copy made when it began.
async fn both(case: &Case) -> (Outcome, Outcome, usize) {
    let world = World::new(case.seed);
    world.hold(true);
    let mut a = database(&world, 0).await;
    existing(&mut a, case.books).await;
    let changes = remote(&world).await;
    let (twin, mut b) = world.twin(0, &mut a).await;
    let replica = Replica::new(registry());
    let mut steps = 1;
    let mut more = replica.install_step(&mut a, case.step).await.unwrap().more;
    replica.install(&mut b).await.unwrap();
    for (i, &(move_by, kind, x, y)) in case.writes.iter().enumerate() {
        for (w, conn) in [(&world, &mut a), (&twin, &mut b)] {
            match move_by {
                m if m > 0 => w.later(m as u64),
                m if m < 0 => w.earlier(u64::from(m.unsigned_abs())),
                _ => {}
            }
            for sql in write(kind, x, y) {
                library::run(conn, &sql).await;
            }
        }
        if case.remote == Some(i) {
            replica.apply(&mut a, &changes).await.unwrap();
            replica.apply(&mut b, &changes).await.unwrap();
        }
        // Steps adopt between the writes; collecting what they wrote is left to the end.
        if more && found_left(&mut a).await > 0 {
            more = replica.collect_step(&mut a, case.step).await.unwrap().more;
            steps += 1;
        }
    }
    while found_left(&mut a).await > 0 {
        replica.collect_step(&mut a, case.step).await.unwrap();
        steps += 1;
    }
    let records: i64 = sqlx::query_scalar("SELECT count(*) FROM fongo_found")
        .fetch_one(&mut a)
        .await
        .unwrap();
    assert_eq!(records, 0, "records left once every one is adopted");
    (
        finish(&world, &replica, &mut a).await,
        finish(&twin, &replica, &mut b).await,
        steps,
    )
}

fn check(case: &Case) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let (stepped, whole, steps) = runtime.block_on(both(case));
    let mut differ = Vec::new();
    if stepped.outbox != whole.outbox {
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
            "outboxes; only stepped: {:#?}\nonly whole: {:#?}",
            only(&stepped.outbox, &whole.outbox),
            only(&whole.outbox, &stepped.outbox)
        ));
    }
    if stepped.tables != whole.tables {
        differ.push(format!("rows:\n{:?}\n{:?}", stepped.tables, whole.tables));
    }
    if stepped.ids != whole.ids {
        differ.push("ids and images".into());
    }
    if stepped.inspection != whole.inspection {
        differ.push(format!(
            "inspections:\n{:?}\n{:?}",
            stepped.inspection, whole.inspection
        ));
    }
    if stepped.elsewhere != whole.elsewhere {
        differ.push("rows elsewhere".into());
    }
    if differ.is_empty() {
        Ok(())
    } else {
        Err(format!("{steps} steps: {}\n{case:?}", differ.join("\n\n")))
    }
}

fn cases() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(32)
}

fn rng_seed() -> RngSeed {
    match std::env::var("PROPTEST_RNG_SEED") {
        Ok(_) => ProptestConfig::default().rng_seed,
        Err(_) => RngSeed::Fixed(41),
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
    fn an_install_in_steps_ends_as_a_whole_one_made_when_it_began(
        seed in any::<u64>(),
        books in 1u64..30,
        step in prop_oneof![Just(1usize), Just(2), Just(5), Just(17), Just(1_000)],
        writes in proptest::collection::vec(
            (prop_oneof![Just(0i8), Just(1), Just(1), Just(5), Just(-3)], any::<u64>(), 0u64..40, any::<u64>()),
            0..30,
        ),
        remote in proptest::option::of(0usize..30),
    ) {
        let case = Case { seed, books, step, writes, remote };
        if let Err(error) = check(&case) {
            prop_assert!(false, "{}", error);
        }
    }
}

/// What the application writes while the kernel is suspended between install steps, removing a
/// found row and editing another before either is adopted, is collected on resuming as after a
/// whole install.
#[test]
fn writes_while_suspended_between_install_steps() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let world = World::new(5);
        world.hold(true);
        let mut a = database(&world, 0).await;
        existing(&mut a, 12).await;
        let (twin, mut b) = world.twin(0, &mut a).await;
        let replica = Replica::new(registry());
        replica.install_step(&mut a, 2).await.unwrap();
        replica.install(&mut b).await.unwrap();
        for (w, conn) in [(&world, &mut a), (&twin, &mut b)] {
            replica.suspend(conn).await.unwrap();
            w.later(1);
            for sql in [
                "DELETE FROM books WHERE id = 11",
                "UPDATE books SET title = 'quiet' WHERE id = 12",
                "UPDATE note_bodies SET body = 'quiet' WHERE note_id = 10",
            ] {
                library::run(conn, sql).await;
            }
            replica
                .resume(conn, fongo_sync_sqlite::Resume::Send)
                .await
                .unwrap();
        }
        assert_eq!(found_left(&mut a).await, 0);
        assert_eq!(
            finish(&world, &replica, &mut a).await,
            finish(&twin, &replica, &mut b).await
        );
    });
}

/// An install that registers a new collection while an earlier one's records wait finds those
/// records adopted first and the new collection's rows marked, as a whole install would leave
/// them: the outcome is a whole install of each registry in turn.
#[test]
fn a_second_registry_installed_while_records_wait() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let world = World::new(6);
        world.hold(true);
        let mut a = database(&world, 0).await;
        existing(&mut a, 12).await;
        let (twin, mut b) = world.twin(0, &mut a).await;
        let all = registry();
        let books_only = fongo_sync_sqlite::Registry::new(
            all.collections()
                .iter()
                .filter(|c| ["books", "scales", "book_tags"].contains(&c.name.as_str()))
                .cloned()
                .collect(),
        )
        .unwrap();
        let first = Replica::new(books_only);
        let second = Replica::new(all);
        assert!(first.install_step(&mut a, 2).await.unwrap().more);
        assert!(found_left(&mut a).await > 0);
        // One install, then steps of collecting, as an application makes them: each install
        // reads the node's id afresh.
        let mut more = second.install_step(&mut a, 2).await.unwrap().more;
        while more {
            more = second.collect_step(&mut a, 2).await.unwrap().more;
        }
        first.install(&mut b).await.unwrap();
        second.install(&mut b).await.unwrap();
        assert_eq!(found_left(&mut a).await, 0);
        let (x, y) = (
            finish(&world, &second, &mut a).await,
            finish(&twin, &second, &mut b).await,
        );
        if x != y {
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
            panic!(
                "only stepped {:#?}\nonly whole {:#?}\nrows {} ids {} inspection {} elsewhere {}",
                only(&x.outbox, &y.outbox),
                only(&y.outbox, &x.outbox),
                x.tables == y.tables,
                x.ids == y.ids,
                x.inspection == y.inspection,
                x.elsewhere == y.elsewhere
            );
        }
    });
}

/// Outside the library's shape: series whose code is an identity no index enforces, two found
/// with one code, so adopting them gives one row up. Before their batch is adopted the given-up
/// row is removed and another made at its id; adopting leaves the new row to the collect of its
/// mark, as after a whole install.
#[test]
#[ignore = "outside the supported shape: an identity no unique index enforces; rows agree, the changes sent do not"]
fn a_found_row_given_up_after_its_id_was_taken_again() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let registry = || {
            fongo_sync_sqlite::Registry::new(vec![
                fongo_sync_sqlite::Collection::minted("albums", "albums", "id").field("name"),
                fongo_sync_sqlite::Collection::minted("series", "series", "id")
                    .field("code")
                    .field("name")
                    .identity(&["code"]),
            ])
            .unwrap()
        };
        let world = World::new(8);
        world.hold(true);
        let mut a = world.connect(0).await;
        for sql in [
            "CREATE TABLE albums (id INTEGER PRIMARY KEY, name TEXT)",
            "CREATE TABLE series (id INTEGER PRIMARY KEY, code TEXT, name TEXT)",
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 6)
             INSERT INTO albums (id, name) SELECT i, 'album ' || i FROM n",
            "INSERT INTO series (id, code, name) VALUES (1, 'c1', 'first'), (2, 'c1', 'second')",
        ] {
            library::run(&mut a, sql).await;
        }
        let (twin, mut b) = world.twin(0, &mut a).await;
        let replica = Replica::new(registry());
        assert!(replica.install_step(&mut a, 1).await.unwrap().more);
        replica.install(&mut b).await.unwrap();
        for (w, conn) in [(&world, &mut a), (&twin, &mut b)] {
            w.later(1);
            for sql in [
                "DELETE FROM series WHERE id = 2",
                "INSERT INTO series (id, code, name) VALUES (2, 'c9', 'made again')",
            ] {
                library::run(conn, sql).await;
            }
        }
        while found_left(&mut a).await > 0 {
            replica.collect_step(&mut a, 1).await.unwrap();
        }
        let mut out = Vec::new();
        for conn in [&mut a, &mut b] {
            replica.collect(conn).await.unwrap();
            let mut outbox = replica.outbox(conn, u32::MAX).await.unwrap().changes;
            outbox.sort_by_key(|c| c.clock);
            out.push((
                outbox,
                support::rows(conn, "SELECT * FROM series ORDER BY id", &[]).await,
                replica.inspect(conn).await.unwrap(),
            ));
        }
        let (stepped, whole) = (&out[0], &out[1]);
        assert_eq!(stepped.1, whole.1, "rows");
        assert_eq!(stepped.0, whole.0, "changes");
        assert_eq!(stepped.2, whole.2, "inspections");
    });
}

/// A database of layout 3, as origin/main left it or as it was with the index of marks, is
/// upgraded in place by its next install, whole or in steps: what it held is kept and it goes on
/// collecting.
#[test]
fn layout_three_is_upgraded_in_place() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        for (indexed, stepped) in [(false, false), (true, false), (false, true), (true, true)] {
            let world = World::new(10);
            let mut a = database(&world, 0).await;
            existing(&mut a, 8).await;
            let replica = Replica::new(registry());
            replica.install(&mut a).await.unwrap();
            library::run(&mut a, "UPDATE books SET title = 'kept' WHERE id = 3").await;
            // As layout 3 laid its tables down.
            let mut downgrade = vec![
                "DROP TABLE fongo_found",
                "UPDATE fongo_meta SET value = 3 WHERE key = 'layout'",
            ];
            if !indexed {
                downgrade.push("DROP INDEX fongo_dirty_order");
            }
            for sql in downgrade {
                sqlx::query(sql).execute(&mut a).await.unwrap();
            }
            let before = (
                replica.outbox(&mut a, u32::MAX).await.unwrap().changes,
                replica.inspect(&mut a).await.unwrap(),
            );
            if stepped {
                replica.install_step(&mut a, 3).await.unwrap();
            } else {
                replica.install(&mut a).await.unwrap();
            }
            let layout: i64 =
                sqlx::query_scalar("SELECT value FROM fongo_meta WHERE key = 'layout'")
                    .fetch_one(&mut a)
                    .await
                    .unwrap();
            assert_eq!(layout, 4);
            let objects: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM sqlite_master WHERE name IN ('fongo_found', 'fongo_found_batch', 'fongo_dirty_order')",
            )
            .fetch_one(&mut a)
            .await
            .unwrap();
            assert_eq!(objects, 3);
            replica.collect(&mut a).await.unwrap();
            let after = (
                replica.outbox(&mut a, u32::MAX).await.unwrap().changes,
                replica.inspect(&mut a).await.unwrap(),
            );
            assert!(after.0.starts_with(&before.0), "indexed {indexed}, stepped {stepped}");
            assert!(
                after.0[before.0.len()..]
                    .iter()
                    .any(|c| c.value == Value::Text("kept".into())),
                "the write made before the upgrade is collected after it"
            );
            assert!(after.1.uncollected.is_empty());
        }
    });
}

/// Uninstalled while records wait, the kernel leaves nothing behind, and installing again is a
/// fresh whole install.
#[test]
fn uninstalled_while_records_wait() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let world = World::new(11);
        world.hold(true);
        let mut a = database(&world, 0).await;
        existing(&mut a, 10).await;
        let replica = Replica::new(registry());
        assert!(replica.install_step(&mut a, 2).await.unwrap().more);
        replica.uninstall(&mut a).await.unwrap();
        let kernel: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM sqlite_master WHERE name LIKE 'fongo!_%' ESCAPE '!'",
        )
        .fetch_one(&mut a)
        .await
        .unwrap();
        assert_eq!(kernel, 0);
        let (twin, mut b) = world.twin(0, &mut a).await;
        replica.install(&mut a).await.unwrap();
        replica.install(&mut b).await.unwrap();
        assert_eq!(
            finish(&world, &replica, &mut a).await,
            finish(&twin, &replica, &mut b).await
        );
    });
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

/// In SQLite's work, a first install in steps of 100 or 1,000 rows over a library of books, each
/// with a highlight and a note, costs at most twice a whole install, and the step that records
/// what it finds is a small part of that.
#[tokio::test]
async fn an_install_in_steps_costs_at_most_twice_a_whole_one() {
    let books = std::env::var("INSTALL_BOOKS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(2_000);
    let whole = {
        let world = World::new(3);
        let mut a = database(&world, 0).await;
        library::books_with_notes(&mut a, books).await;
        let work = count_work(&mut a).await;
        let began = std::time::Instant::now();
        Replica::new(registry()).install(&mut a).await.unwrap();
        let w = work.load(std::sync::atomic::Ordering::Relaxed);
        eprintln!(
            "{books} books, whole install: work {w}, {:?}",
            began.elapsed()
        );
        w
    };
    for rows in [100, 1_000] {
        let world = World::new(3);
        let mut a = database(&world, 0).await;
        library::books_with_notes(&mut a, books).await;
        let replica = Replica::new(registry());
        let work = count_work(&mut a).await;
        let began = std::time::Instant::now();
        let first = replica.install_step(&mut a, rows).await.unwrap();
        let (first_work, first_took) = (
            work.load(std::sync::atomic::Ordering::Relaxed),
            began.elapsed(),
        );
        let (mut more, mut steps, mut longest) = (first.more, 1, first_took);
        while more {
            let t = std::time::Instant::now();
            more = replica.collect_step(&mut a, rows).await.unwrap().more;
            longest = longest.max(t.elapsed());
            steps += 1;
        }
        let w = work.load(std::sync::atomic::Ordering::Relaxed);
        eprintln!(
            "{books} books, steps of {rows}: {steps} steps, work {w} against {whole}, first step {first_work} ({first_took:?}), longest other {longest:?}, all {:?}",
            began.elapsed()
        );
        assert!(w <= 2 * whole, "steps of {rows}: work {w} against {whole}");
    }
}

/// The step that records what a first install finds is short beside a whole install of the same
/// library, references among the rows and cycles of them included: recording reads each row
/// once and walks each reference once.
#[tokio::test]
async fn recording_takes_a_small_part_of_a_whole_install() {
    let books = std::env::var("INSTALL_BOOKS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(3_000);
    let whole = {
        let world = World::new(3);
        let mut a = database(&world, 0).await;
        existing(&mut a, books).await;
        let began = std::time::Instant::now();
        Replica::new(registry()).install(&mut a).await.unwrap();
        began.elapsed()
    };
    let world = World::new(3);
    let mut a = database(&world, 0).await;
    existing(&mut a, books).await;
    let replica = Replica::new(registry());
    let began = std::time::Instant::now();
    replica.install_step(&mut a, 1).await.unwrap();
    let recording = began.elapsed();
    let largest: i64 = sqlx::query_scalar(
        "SELECT max(n) FROM (SELECT count(*) AS n FROM fongo_found GROUP BY batch)",
    )
    .fetch_one(&mut a)
    .await
    .unwrap();
    eprintln!(
        "{books} books: recording {recording:?}, whole install {whole:?}, largest batch {largest}"
    );
    assert!(largest <= 3, "a batch of {largest} rows");
    assert!(
        recording * 2 <= whole,
        "recording {recording:?} against a whole install's {whole:?}"
    );
}

/// The library's registry as it was before a change, and as it is after: a field added, a
/// collection added, a field removed, an identity added, a reference added.
fn registries(change: &str) -> (fongo_sync_sqlite::Registry, fongo_sync_sqlite::Registry) {
    let full = registry();
    let mut fewer: Vec<fongo_sync_sqlite::Collection> = full.collections().to_vec();
    let books = fewer.iter().position(|c| c.name == "books").unwrap();
    let highlights = fewer.iter().position(|c| c.name == "highlights").unwrap();
    match change {
        "field" | "removed" => fewer[books].fields.retain(|f| f.name != "title"),
        "collection" => fewer.retain(|c| c.name != "book_tags"),
        "identity" => fewer[books]
            .identity
            .retain(|set| set != &["isbn_10".to_string()]),
        "reference" => fewer[highlights].fields.retain(|f| f.name != "copy_id"),
        _ => unreachable!(),
    }
    let fewer = fongo_sync_sqlite::Registry::new(fewer).unwrap();
    if change == "removed" {
        (full, fewer)
    } else {
        (fewer, full)
    }
}

/// A registry that changes while a first install's records wait, by an install or by resuming
/// after a migration: the records are adopted under the registry they were recorded with, and
/// what the new one adds is adopted of the rows as they stand, as installs of the two
/// registries one after the other would.
#[test]
fn a_registry_changed_while_records_wait() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        for change in ["field", "collection", "removed", "identity", "reference"] {
            for migration in [false, true] {
                let (before, after) = registries(change);
                let (before, after) = (Replica::new(before), Replica::new(after));
                let world = World::new(12);
                world.hold(true);
                let mut a = database(&world, 0).await;
                existing(&mut a, 12).await;
                let (twin, mut b) = world.twin(0, &mut a).await;
                assert!(before.install_step(&mut a, 2).await.unwrap().more);
                before.install(&mut b).await.unwrap();
                for (stepped, w, conn) in [(true, &world, &mut a), (false, &twin, &mut b)] {
                    w.later(1);
                    if migration {
                        before.suspend(conn).await.unwrap();
                        library::run(conn, "UPDATE books SET title = 'migrated' WHERE id = 4").await;
                        after
                            .resume(conn, fongo_sync_sqlite::Resume::Baseline)
                            .await
                            .unwrap();
                    } else if stepped {
                        let mut more = after.install_step(conn, 2).await.unwrap().more;
                        while more {
                            more = after.collect_step(conn, 2).await.unwrap().more;
                        }
                    } else {
                        after.install(conn).await.unwrap();
                    }
                }
                while found_left(&mut a).await > 0 {
                    after.collect_step(&mut a, 2).await.unwrap();
                }
                let (x, y) = (
                    finish(&world, &after, &mut a).await,
                    finish(&twin, &after, &mut b).await,
                );
                assert!(
                    x == y,
                    "{change}, migration {migration}: rows {} ids {} inspection {} elsewhere {} outbox {}",
                    x.tables == y.tables,
                    x.ids == y.ids,
                    x.inspection == y.inspection,
                    x.elsewhere == y.elsewhere,
                    x.outbox == y.outbox
                );
            }
        }
    });
}

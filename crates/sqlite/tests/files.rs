//! Fields that name files: what a replica refuses to collect into one, which files the changes it
//! holds refer to, by the protocol's definition and conservatively beyond it for data its
//! registry cannot interpret, what each apply, collect and purge does to that set, and the
//! clearing of deleted rows' files.

mod support;

use fongo_sync_sqlite::core::{ALIAS, Change, DELETED, Gid, Hlc, Strategy, Value};
use fongo_sync_sqlite::{
    Collection, ContentHash, Error, Field, FileChanges, FileRef, FilesFrom, Kind, Registry, Replica,
};
use sqlx::SqliteConnection;
use support::world::World;

const SCHEMA: &[&str] = &[
    "CREATE TABLE books (
        id INTEGER PRIMARY KEY,
        title TEXT,
        isbn TEXT UNIQUE,
        slot INTEGER UNIQUE,
        cover BLOB)",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        body TEXT,
        scan BLOB)",
];

/// Books with a cover, and notes, which name a scan from the second version on.
fn registry(version: u8) -> Registry {
    let books = Collection::minted("books", "books", "id")
        .field("title")
        .field("isbn")
        .field("slot")
        .file("cover")
        .identity(&["isbn"]);
    let notes = Collection::minted("notes", "notes", "id")
        .owner("book_id", "books")
        .field("body");
    match version {
        0 => Registry::new(vec![books]),
        1 => Registry::new(vec![books, notes]),
        _ => Registry::new(vec![books, notes.file("scan")]),
    }
    .unwrap()
}

fn file(n: u8) -> FileRef {
    FileRef::of(&[n; 3])
}

async fn run(conn: &mut SqliteConnection, sql: &str) {
    sqlx::query(sql).execute(conn).await.unwrap();
}

/// Runs `sql` with `?1` bound to the encoded reference of `f`.
async fn with_file(conn: &mut SqliteConnection, sql: &str, f: FileRef) {
    sqlx::query(sql)
        .bind(f.encode())
        .execute(conn)
        .await
        .unwrap();
}

async fn database(world: &World, n: u64, replica: &Replica) -> SqliteConnection {
    let mut conn = world.connect(n).await;
    for sql in SCHEMA {
        run(&mut conn, sql).await;
    }
    replica.install(&mut conn).await.unwrap();
    conn
}

async fn sent(replica: &Replica, conn: &mut SqliteConnection) -> Vec<Change> {
    let outbox = replica.outbox(conn, u32::MAX).await.unwrap();
    replica.acknowledge(conn, outbox.through).await.unwrap();
    outbox.changes
}

async fn refers(replica: &Replica, conn: &mut SqliteConnection, f: FileRef) -> bool {
    replica.refers(conn, &[f.hash]).await.unwrap()[0]
}

/// The content of the `n`th file.
fn hash(n: u8) -> ContentHash {
    file(n).hash
}

/// Every file `files` lists, paging `limit` at a time, with whether it is declared.
async fn every_file(
    replica: &Replica,
    conn: &mut SqliteConnection,
    limit: usize,
) -> Vec<(FileRef, bool)> {
    let mut out = Vec::new();
    let mut from: Option<FilesFrom> = None;
    loop {
        let page = replica.files(conn, from.as_ref(), limit).await.unwrap();
        assert!(page.files.len() <= limit);
        out.extend(page.files.iter().map(|r| (r.file, r.declared)));
        match page.next {
            Some(next) => from = Some(next),
            None => return out,
        }
    }
}

fn changes(added: &[FileRef], dropped: &[FileRef]) -> FileChanges {
    let mut files = FileChanges {
        added: added.to_vec(),
        dropped: dropped.iter().map(|f| f.hash).collect(),
    };
    files.added.sort_by_key(|f| f.encode());
    files.dropped.sort();
    files
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

#[test]
fn a_file_field_is_a_plain_field_merged_by_clock_outside_keys_and_sets() {
    let books = || Collection::minted("books", "books", "id").field("title");
    assert!(Registry::new(vec![books().file("cover")]).is_ok());
    assert!(Registry::new(vec![books().file("cover").identity(&["cover"])]).is_ok());
    let tags = Collection::natural("tags", "tags", &["tag"])
        .file("tag")
        .field("note");
    assert!(Registry::new(vec![tags]).is_err());
    let tags = Collection::natural("tags", "tags", &["tag"])
        .field("tag")
        .file("note");
    assert!(Registry::new(vec![tags]).is_ok());
    assert!(
        Registry::new(vec![
            books()
                .file("cover")
                .field("source")
                .together(&["cover", "source"])
        ])
        .is_err()
    );
    for (strategy, reports) in [(Strategy::Max, false), (Strategy::Lww, true)] {
        let mut c = books();
        c.fields.push(Field {
            name: "cover".into(),
            kind: Kind::File,
            strategy,
            reports_conflicts: reports,
        });
        assert!(Registry::new(vec![c]).is_err(), "{strategy:?} {reports}");
    }
    let schema = Registry::new(vec![books().file("cover")])
        .unwrap()
        .model_schema();
    let plain = Registry::new(vec![books().field("cover")])
        .unwrap()
        .model_schema();
    assert_eq!(schema, plain, "to the model a file field is a plain one");
}

/// A local write of anything but a reference or NULL to a file field is refused, naming the
/// row, and leaves the database as it was; once the row holds a reference it is collected.
#[tokio::test]
async fn a_local_write_of_something_else_is_refused_until_the_row_holds_a_reference() {
    let world = World::new(1);
    let replica = Replica::new(registry(1));
    let mut a = database(&world, 0, &replica).await;
    run(&mut a, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
    replica.collect(&mut a).await.unwrap();
    sent(&replica, &mut a).await;
    for bad in ["'cover.jpg'", "x'0102'", "42"] {
        run(&mut a, &format!("UPDATE books SET cover = {bad}")).await;
        let before = dump(&mut a).await;
        for refused in [
            replica.collect(&mut a).await.map(|_| ()),
            replica.collect_step(&mut a, 10).await.map(|_| ()),
            replica.apply(&mut a, &[]).await.map(|_| ()),
        ] {
            match refused {
                Err(Error::NotAFile {
                    collection,
                    field,
                    key,
                }) => {
                    assert_eq!(
                        (collection.as_str(), field.as_str(), key.as_deref()),
                        ("books", "cover", Some("[1]"))
                    );
                }
                other => panic!("{bad}: {other:?}"),
            }
            assert_eq!(dump(&mut a).await, before, "{bad}");
        }
    }
    with_file(&mut a, "UPDATE books SET cover = ?1", file(1)).await;
    replica.collect(&mut a).await.unwrap();
    let out = sent(&replica, &mut a).await;
    assert_eq!(replica.registry().files_set(&out), vec![file(1)]);
    run(&mut a, "UPDATE books SET cover = NULL").await;
    replica.collect(&mut a).await.unwrap();
    let out = sent(&replica, &mut a).await;
    assert_eq!(out.len(), 1);
    assert!(replica.registry().files_set(&out).is_empty());
}

/// A value from elsewhere that is not a reference is kept and shown, and names no file.
#[tokio::test]
async fn a_value_from_elsewhere_that_is_not_a_reference_is_kept_and_names_nothing() {
    let world = World::new(2);
    let replica = Replica::new(registry(1));
    let mut b = database(&world, 1, &replica).await;
    let gid = Gid([3; 16]);
    let at = |counter| Hlc {
        wall_ms: 1_700_000_000_000,
        counter,
        node: Gid([9; 16]),
    };
    let f = file(1);
    let mut longer = f.encode();
    longer.push(0);
    let batch = [
        (DELETED, Value::Int(0)),
        ("title", Value::Text("Romola".into())),
        ("cover", Value::Blob(longer.clone())),
    ]
    .into_iter()
    .enumerate()
    .map(|(i, (field, value))| Change {
        collection: "books".into(),
        gid,
        field: field.into(),
        value,
        clock: at(i as u32),
    })
    .collect::<Vec<_>>();
    let applied = replica.apply(&mut b, &batch).await.unwrap();
    assert_eq!(applied.files, FileChanges::default());
    let shown: Vec<Vec<u8>> = sqlx::query_scalar("SELECT cover FROM books")
        .fetch_all(&mut b)
        .await
        .unwrap();
    assert_eq!(shown, vec![longer]);
    assert!(!refers(&replica, &mut b, f).await);
    assert!(replica.registry().files_set(&batch).is_empty());
    // Editing another field of the row sends nothing of the cover.
    run(&mut b, "UPDATE books SET title = 'Romola!'").await;
    replica.collect(&mut b).await.unwrap();
    let out = sent(&replica, &mut b).await;
    assert!(out.iter().all(|c| c.field != "cover"), "{out:?}");
}

/// What a batch sets is a function of the batch, whenever it is read; what refers follows the
/// winner of each field, a deleted row keeps referring, and each collect and apply reports what
/// it added and dropped.
#[tokio::test]
async fn the_set_follows_each_fields_winner_whatever_the_rows_liveness() {
    let world = World::new(3);
    let replica = Replica::new(registry(1));
    let mut a = database(&world, 0, &replica).await;
    let mut b = database(&world, 1, &replica).await;
    with_file(
        &mut a,
        "INSERT INTO books (title, cover) VALUES ('Middlemarch', ?1)",
        file(1),
    )
    .await;
    with_file(
        &mut a,
        "INSERT INTO books (title, cover) VALUES ('Romola', ?1)",
        file(1),
    )
    .await;
    let collected = replica.collect_step(&mut a, 100).await.unwrap();
    assert_eq!(collected.files, changes(&[file(1)], &[]));
    let outbox = replica.outbox(&mut a, u32::MAX).await.unwrap();
    assert_eq!(replica.registry().files_set(&outbox.changes), vec![file(1)]);
    let first = sent(&replica, &mut a).await;
    assert_eq!(first, outbox.changes);
    assert_eq!(replica.registry().files_set(&first), vec![file(1)]);
    let applied = replica.apply(&mut b, &first).await.unwrap();
    assert_eq!(applied.files, changes(&[file(1)], &[]));
    let applied = replica.apply(&mut b, &first).await.unwrap();
    assert_eq!(
        applied.files,
        FileChanges::default(),
        "again, nothing changes"
    );

    // One book takes another cover: the first is still named by the other book.
    with_file(&mut a, "UPDATE books SET cover = ?1 WHERE id = 1", file(2)).await;
    let collected = replica.collect_step(&mut a, 100).await.unwrap();
    assert_eq!(collected.files, changes(&[file(2)], &[]));
    let second = sent(&replica, &mut a).await;
    let applied = replica.apply(&mut b, &second).await.unwrap();
    assert_eq!(applied.files, changes(&[file(2)], &[]));

    // The other takes it too: the first is named by nothing.
    with_file(&mut a, "UPDATE books SET cover = ?1 WHERE id = 2", file(2)).await;
    let collected = replica.collect_step(&mut a, 100).await.unwrap();
    assert_eq!(collected.files, changes(&[], &[file(1)]));
    let third = sent(&replica, &mut a).await;
    let applied = replica.apply(&mut b, &third).await.unwrap();
    assert_eq!(applied.files, changes(&[], &[file(1)]));
    for conn in [&mut a, &mut b] {
        assert_eq!(
            replica.refers(conn, &[hash(1), hash(2)]).await.unwrap(),
            [false, true]
        );
    }

    // Deleted, both books still refer to their file.
    run(&mut a, "DELETE FROM books").await;
    let collected = replica.collect_step(&mut a, 100).await.unwrap();
    assert_eq!(collected.files, FileChanges::default());
    let fourth = sent(&replica, &mut a).await;
    let applied = replica.apply(&mut b, &fourth).await.unwrap();
    assert_eq!(applied.files, FileChanges::default());
    for conn in [&mut a, &mut b] {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM books")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(count, 0);
        assert!(refers(&replica, conn, file(2)).await);
        assert_eq!(every_file(&replica, conn, 1).await, [(file(2), true)]);
    }
}

/// A replica whose registry lacks a file field, or the whole collection, that a later version
/// declares never reports a file that field names as unreferred.
#[tokio::test]
async fn an_older_registry_counts_what_it_cannot_interpret() {
    let world = World::new(4);
    let newer = Replica::new(registry(2));
    let mut a = database(&world, 0, &newer).await;
    let (one, zero) = (Replica::new(registry(1)), Replica::new(registry(0)));
    let mut b = database(&world, 1, &one).await;
    let mut c = database(&world, 2, &zero).await;
    run(&mut a, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
    with_file(
        &mut a,
        "INSERT INTO notes (book_id, body, scan) VALUES (1, 'slow', ?1)",
        file(7),
    )
    .await;
    newer.collect(&mut a).await.unwrap();
    let first = sent(&newer, &mut a).await;
    assert_eq!(newer.registry().files_set(&first), vec![file(7)]);
    for (replica, conn) in [(&one, &mut b), (&zero, &mut c)] {
        let applied = replica.apply(conn, &first).await.unwrap();
        assert_eq!(applied.files, FileChanges::default(), "not declared here");
        assert!(refers(replica, conn, file(7)).await);
        assert_eq!(every_file(replica, conn, 10).await, [(file(7), false)]);
    }
    with_file(&mut a, "UPDATE notes SET scan = ?1", file(8)).await;
    newer.collect(&mut a).await.unwrap();
    let second = sent(&newer, &mut a).await;
    // An undeclared field of a declared collection keeps its winner by clock; a collection the
    // registry lacks keeps every change.
    one.apply(&mut b, &second).await.unwrap();
    assert_eq!(
        one.refers(&mut b, &[hash(7), hash(8)]).await.unwrap(),
        [false, true]
    );
    zero.apply(&mut c, &second).await.unwrap();
    assert_eq!(
        zero.refers(&mut c, &[hash(7), hash(8)]).await.unwrap(),
        [true, true]
    );
    assert_eq!(every_file(&zero, &mut c, 1).await, {
        let mut both = vec![(file(7), false), (file(8), false)];
        both.sort_by_key(|(f, _)| f.encode());
        both
    });

    // Upgraded, each replica holds what the newer one does. The changes held for a collection
    // the registry lacked are merged at the install, which reports what they bring; a field
    // newly declared is read with `files`.
    for (conn, expected) in [
        (&mut b, FileChanges::default()),
        (&mut c, changes(&[file(8)], &[file(7)])),
    ] {
        let stepped = newer.install_step(conn, 100).await.unwrap();
        assert_eq!(stepped.files, expected);
        assert_eq!(
            newer.refers(conn, &[hash(7), hash(8)]).await.unwrap(),
            [false, true]
        );
        assert_eq!(every_file(&newer, conn, 10).await, [(file(8), true)]);
    }
}

/// A row held back shows the file it held: it refers to it, `held_files` names it, and once
/// the row is written the file drops out.
#[tokio::test]
async fn a_row_held_back_refers_to_the_file_it_shows() {
    let world = World::new(5);
    let replica = Replica::new(registry(1));
    let mut a = database(&world, 0, &replica).await;
    let mut b = database(&world, 1, &replica).await;
    with_file(
        &mut a,
        "INSERT INTO books (title, slot, cover) VALUES ('Middlemarch', 1, ?1)",
        file(1),
    )
    .await;
    run(
        &mut a,
        "INSERT INTO books (title, slot) VALUES ('Romola', 2)",
    )
    .await;
    replica.collect(&mut a).await.unwrap();
    replica
        .apply(&mut b, &sent(&replica, &mut a).await)
        .await
        .unwrap();
    // Elsewhere the first book moves to a slot this replica gives its second book meanwhile.
    with_file(
        &mut a,
        "UPDATE books SET slot = 3, cover = ?1 WHERE id = 1",
        file(2),
    )
    .await;
    replica.collect(&mut a).await.unwrap();
    run(&mut b, "UPDATE books SET slot = 3 WHERE title = 'Romola'").await;
    replica.collect(&mut b).await.unwrap();
    sent(&replica, &mut b).await;
    let applied = replica
        .apply(&mut b, &sent(&replica, &mut a).await)
        .await
        .unwrap();
    assert_eq!(applied.held.len(), 1, "{applied:?}");
    assert_eq!(applied.files, changes(&[file(2)], &[]));
    assert_eq!(replica.files_to_claim(&mut b).await.unwrap(), vec![file(1)]);
    assert_eq!(
        replica.refers(&mut b, &[hash(1), hash(2)]).await.unwrap(),
        [true, true]
    );
    assert!(
        every_file(&replica, &mut b, 10)
            .await
            .contains(&(file(1), true))
    );
    run(&mut b, "UPDATE books SET slot = 4 WHERE title = 'Romola'").await;
    let applied = replica.apply(&mut b, &[]).await.unwrap();
    assert!(applied.held.is_empty(), "{applied:?}");
    assert_eq!(applied.files, changes(&[], &[file(1)]));
    assert!(replica.files_to_claim(&mut b).await.unwrap().is_empty());
    assert!(!refers(&replica, &mut b, file(1)).await);
}

/// Pages of every size list each file held once, in order, and say which are declared.
#[tokio::test]
async fn pages_list_each_file_once_in_order() {
    let world = World::new(6);
    let newer = Replica::new(registry(2));
    let zero = Replica::new(registry(0));
    let mut a = database(&world, 0, &newer).await;
    let mut c = database(&world, 1, &zero).await;
    for i in 0..12u8 {
        with_file(
            &mut a,
            "INSERT INTO books (title, cover) VALUES ('Middlemarch', ?1)",
            file(i % 9),
        )
        .await;
        with_file(
            &mut a,
            &format!(
                "INSERT INTO notes (book_id, body, scan) VALUES ({}, 'n', ?1)",
                i + 1
            ),
            file(5 + i % 11),
        )
        .await;
    }
    newer.collect(&mut a).await.unwrap();
    zero.apply(&mut c, &sent(&newer, &mut a).await)
        .await
        .unwrap();
    let mut expected: Vec<(FileRef, bool)> = (0..16u8).map(|n| (file(n), n < 9)).collect();
    expected.sort_by_key(|(f, _)| f.encode());
    for limit in [1, 2, 3, 5, 16, 100] {
        assert_eq!(every_file(&zero, &mut c, limit).await, expected, "{limit}");
    }
    let declared: Vec<(FileRef, bool)> = expected.iter().map(|(f, _)| (*f, true)).collect();
    for limit in [1, 4, 100] {
        assert_eq!(every_file(&newer, &mut a, limit).await, declared, "{limit}");
    }
}

/// Runs purge steps of `rows` until none is left, returning every step's report.
async fn purge(
    replica: &Replica,
    conn: &mut SqliteConnection,
    before: u64,
    rows: usize,
) -> Vec<fongo_sync_sqlite::Purged> {
    let mut steps = Vec::new();
    let mut from = None;
    loop {
        let step = replica
            .purge_step(conn, before, from.as_ref(), rows)
            .await
            .unwrap();
        assert!(step.examined <= rows, "{step:?}");
        from = step.next.clone();
        steps.push(step);
        if from.is_none() {
            return steps;
        }
    }
}

/// Clearing the files of rows deleted before a time is a local write like any other: sent,
/// merged elsewhere, and reported as dropping the files nothing else names.
#[tokio::test]
async fn purging_clears_the_files_of_rows_deleted_before_a_time() {
    let world = World::new(7);
    let replica = Replica::new(registry(2));
    let mut a = database(&world, 0, &replica).await;
    let mut b = database(&world, 1, &replica).await;
    for i in 1..=6u8 {
        with_file(
            &mut a,
            &format!("INSERT INTO books (title, cover) VALUES ('book {i}', ?1)"),
            file(i),
        )
        .await;
    }
    // The second shares its cover with the first, which stays.
    with_file(&mut a, "UPDATE books SET cover = ?1 WHERE id = 2", file(1)).await;
    with_file(
        &mut a,
        "INSERT INTO notes (book_id, body, scan) VALUES (3, 'slow', ?1)",
        file(9),
    )
    .await;
    replica.collect(&mut a).await.unwrap();
    replica
        .apply(&mut b, &sent(&replica, &mut a).await)
        .await
        .unwrap();
    run(&mut a, "DELETE FROM books WHERE id IN (2, 3, 4)").await;
    replica.collect(&mut a).await.unwrap();
    world.later(60_000);
    let cut = 1_700_000_000_000 + 30_000;
    run(&mut a, "DELETE FROM books WHERE id = 5").await;
    replica.collect(&mut a).await.unwrap();
    replica
        .apply(&mut b, &sent(&replica, &mut a).await)
        .await
        .unwrap();

    // Nothing was deleted before the start of time; nothing is sent.
    let none = purge(&replica, &mut a, 0, 2).await;
    assert!(none.iter().all(|p| p.cleared == 0));
    assert!(sent(&replica, &mut a).await.is_empty());

    let steps = purge(&replica, &mut a, cut, 2).await;
    let examined: usize = steps.iter().map(|p| p.examined).sum();
    // Books 2 to 5 and the note on book 3, deleted with it.
    assert_eq!(examined, 5, "{steps:?}");
    let cleared: usize = steps.iter().map(|p| p.cleared).sum();
    // The second book's cover too, though the first names that file still.
    assert_eq!(cleared, 4, "{steps:?}");
    let mut dropped: Vec<ContentHash> =
        steps.iter().flat_map(|p| p.files.dropped.clone()).collect();
    dropped.sort();
    assert_eq!(dropped, changes(&[], &[file(3), file(4), file(9)]).dropped);
    assert!(steps.iter().all(|p| p.files.added.is_empty()));
    assert_eq!(
        replica
            .refers(&mut a, &[hash(1), hash(3), hash(4), hash(5), hash(9)])
            .await
            .unwrap(),
        [true, false, false, true, false]
    );
    // Once more, nothing is left to clear.
    assert!(
        purge(&replica, &mut a, cut, 2)
            .await
            .iter()
            .all(|p| p.cleared == 0)
    );

    let out = sent(&replica, &mut a).await;
    assert_eq!(out.len(), 4);
    assert!(out.iter().all(|c| c.value == Value::Null));
    assert!(replica.registry().files_set(&out).is_empty());
    let applied = replica.apply(&mut b, &out).await.unwrap();
    assert_eq!(applied.files, changes(&[], &[file(3), file(4), file(9)]));
    assert_eq!(
        replica
            .refers(&mut b, &[hash(1), hash(3), hash(4), hash(5), hash(9)])
            .await
            .unwrap(),
        [true, false, false, true, false]
    );
}

/// A change made elsewhere, at the `n`th tick of one clock.
fn change(collection: &str, gid: u8, field: &str, value: Value, n: u32) -> Change {
    Change {
        collection: collection.into(),
        gid: Gid([gid; 16]),
        field: field.into(),
        value,
        clock: Hlc {
            wall_ms: 1_600_000_000_000,
            counter: n,
            node: Gid([200; 16]),
        },
    }
}

/// A deleted member of a group that still shows a row, and a row gone only with its owner, keep
/// their files.
#[tokio::test]
async fn purging_spares_a_shown_group_and_a_row_gone_only_with_its_owner() {
    let world = World::new(8);
    let replica = Replica::new(registry(2));
    let mut b = database(&world, 1, &replica).await;
    let blob = |n: u8| Value::Blob(file(n).encode());
    let batch = vec![
        change("books", 1, DELETED, Value::Int(0), 1),
        change("books", 1, "isbn", Value::Text("x".into()), 2),
        change("books", 1, "cover", blob(1), 3),
        change("books", 2, DELETED, Value::Int(0), 4),
        change("books", 2, "isbn", Value::Text("x".into()), 5),
        change("books", 2, "cover", blob(2), 6),
        change("books", 2, ALIAS, Value::Ref(Gid([1; 16])), 7),
        change("books", 2, DELETED, Value::Int(1), 8),
        change("books", 3, DELETED, Value::Int(0), 8),
        change("books", 3, "cover", blob(3), 9),
        change("notes", 4, DELETED, Value::Int(0), 10),
        change("notes", 4, "book_id", Value::Ref(Gid([3; 16])), 11),
        change("notes", 4, "scan", blob(4), 12),
        change("books", 3, DELETED, Value::Int(1), 13),
    ];
    let applied = replica.apply(&mut b, &batch).await.unwrap();
    assert_eq!(
        applied.files,
        changes(&[file(1), file(2), file(3), file(4)], &[])
    );
    let shown: i64 = sqlx::query_scalar("SELECT count(*) FROM books")
        .fetch_one(&mut b)
        .await
        .unwrap();
    assert_eq!(shown, 1, "the two books with one isbn show as one");
    let steps = purge(&replica, &mut b, u64::MAX, 10).await;
    let out = sent(&replica, &mut b).await;
    // Only the third book, deleted itself and alone, is cleared.
    assert_eq!(
        steps.iter().map(|p| p.cleared).sum::<usize>(),
        1,
        "{steps:?}"
    );
    assert_eq!(out.len(), 1, "{out:?}");
    assert_eq!((out[0].gid, out[0].field.as_str()), (Gid([3; 16]), "cover"));
    assert_eq!(
        replica
            .refers(&mut b, &[hash(1), hash(2), hash(3), hash(4)])
            .await
            .unwrap(),
        [true, true, false, true]
    );
}

/// A field already cleared, or whose winner a clearing cannot beat, is not written again.
#[tokio::test]
async fn purging_writes_only_where_its_null_wins() {
    let world = World::new(9);
    let replica = Replica::new(registry(1));
    let mut b = database(&world, 1, &replica).await;
    // The last millisecond a clock's text holds, which no replica's clock moves to.
    let far = Hlc {
        wall_ms: 0xFFFF_FFFF_FFFF,
        counter: u32::MAX,
        node: Gid([200; 16]),
    };
    let mut ahead = change("books", 1, "cover", Value::Blob(file(1).encode()), 0);
    ahead.clock = far;
    let batch = vec![
        change("books", 1, DELETED, Value::Int(0), 1),
        ahead,
        change("books", 1, DELETED, Value::Int(1), 2),
    ];
    replica.apply(&mut b, &batch).await.unwrap();
    let steps = purge(&replica, &mut b, u64::MAX, 10).await;
    assert_eq!(
        steps.iter().map(|p| p.cleared).sum::<usize>(),
        0,
        "{steps:?}"
    );
    assert!(sent(&replica, &mut b).await.is_empty());
    assert!(refers(&replica, &mut b, file(1)).await);
}

/// A field kept as this replica's own on resuming names its file in no change, but the row
/// shows it: it counts as referring here, and is among the files to claim.
#[tokio::test]
async fn a_field_kept_as_this_replicas_own_refers_to_its_file() {
    let world = World::new(10);
    let replica = Replica::new(registry(1));
    let mut a = database(&world, 0, &replica).await;
    with_file(
        &mut a,
        "INSERT INTO books (title, cover) VALUES ('Middlemarch', ?1)",
        file(1),
    )
    .await;
    replica.collect(&mut a).await.unwrap();
    sent(&replica, &mut a).await;
    replica.suspend(&mut a).await.unwrap();
    with_file(&mut a, "UPDATE books SET cover = ?1", file(2)).await;
    replica
        .resume(&mut a, fongo_sync_sqlite::Resume::Baseline)
        .await
        .unwrap();
    assert!(sent(&replica, &mut a).await.is_empty());
    assert_eq!(
        replica.refers(&mut a, &[hash(1), hash(2)]).await.unwrap(),
        [true, true]
    );
    let mut both = vec![(file(1), true), (file(2), true)];
    both.sort_by_key(|(f, _)| f.encode());
    assert_eq!(every_file(&replica, &mut a, 1).await, both);
    // A device claims it before it reports that it has settled: other devices may retire it.
    assert_eq!(replica.files_to_claim(&mut a).await.unwrap(), vec![file(2)]);
}

/// A database whose values were written before the indexes of values that may name files
/// existed, at layout 4 or 3: the install that upgrades it indexes them all.
#[tokio::test]
async fn an_upgrade_indexes_the_files_already_held() {
    for layout in [4, 3] {
        let world = World::new(11);
        let newer = Replica::new(registry(2));
        let one = Replica::new(registry(1));
        let mut a = database(&world, 0, &newer).await;
        let mut b = database(&world, 1, &one).await;
        with_file(
            &mut a,
            "INSERT INTO books (title, cover) VALUES ('Middlemarch', ?1)",
            file(1),
        )
        .await;
        with_file(
            &mut a,
            "INSERT INTO notes (book_id, body, scan) VALUES (1, 'slow', ?1)",
            file(2),
        )
        .await;
        newer.collect(&mut a).await.unwrap();
        one.apply(&mut b, &sent(&newer, &mut a).await)
            .await
            .unwrap();
        // A cover kept as this replica's own, which only the row shows.
        one.suspend(&mut b).await.unwrap();
        with_file(&mut b, "UPDATE books SET cover = ?1", file(3)).await;
        one.resume(&mut b, fongo_sync_sqlite::Resume::Baseline)
            .await
            .unwrap();
        let before = every_file(&one, &mut b, 10).await;
        assert_eq!(before.len(), 3);
        let set = format!("UPDATE fongo_meta SET value = {layout} WHERE key = 'layout'");
        let mut downgrade = vec![
            "DROP INDEX fongo_shadow_files",
            "DROP INDEX fongo_foreign_files",
            "DROP INDEX fongo_pins_files",
            "ALTER TABLE fongo_pins DROP COLUMN shown",
            set.as_str(),
        ];
        if layout == 3 {
            downgrade.push("DROP TABLE fongo_found");
        }
        for sql in downgrade {
            run(&mut b, sql).await;
        }
        let refused = one.refers(&mut b, &[hash(1)]).await;
        assert!(
            matches!(refused, Err(Error::OlderLayout { found, known: 5 }) if found == layout),
            "{refused:?}"
        );
        one.install(&mut b).await.unwrap();
        assert_eq!(every_file(&one, &mut b, 1).await, before, "layout {layout}");
        assert_eq!(
            one.refers(&mut b, &[hash(1), hash(2), hash(3)])
                .await
                .unwrap(),
            [true, true, true]
        );
        assert_eq!(one.files_to_claim(&mut b).await.unwrap(), vec![file(3)]);
    }
}

/// The state a purge step meets: rows with files, some deleted long ago, and a write not yet
/// collected.
async fn to_purge(world: &World) -> (SqliteConnection, Replica) {
    let replica = Replica::new(registry(2));
    let mut a = database(world, 0, &replica).await;
    for i in 1..=4u8 {
        with_file(
            &mut a,
            &format!("INSERT INTO books (title, cover) VALUES ('book {i}', ?1)"),
            file(i),
        )
        .await;
        with_file(
            &mut a,
            &format!("INSERT INTO notes (book_id, body, scan) VALUES ({i}, 'n', ?1)"),
            file(10 + i),
        )
        .await;
    }
    replica.collect(&mut a).await.unwrap();
    run(&mut a, "DELETE FROM books WHERE id IN (1, 3)").await;
    replica.collect(&mut a).await.unwrap();
    sent(&replica, &mut a).await;
    run(&mut a, "UPDATE books SET title = 'kept' WHERE id = 2").await;
    (a, replica)
}

/// A purge step stopped at any statement leaves the database as it was, and the steps run again
/// to the end clear what they would have cleared.
#[tokio::test]
async fn a_purge_step_stopped_at_any_statement_changes_nothing_and_the_steps_finish() {
    let purge_all = async |replica: &Replica, conn: &mut SqliteConnection| {
        let mut from = None;
        let mut out = Vec::new();
        loop {
            let step = replica.purge_step(conn, u64::MAX, from.as_ref(), 1).await?;
            out.push(step.clone());
            match step.next {
                Some(next) => from = Some(next),
                None => return Ok::<_, Error>(out),
            }
        }
    };
    let (mut conn, replica) = to_purge(&World::new(12)).await;
    let expected_steps = purge_all(&replica, &mut conn).await.unwrap();
    let expected = sent(&replica, &mut conn).await;
    assert_eq!(expected.len(), 4, "{expected:?}");
    let mut n = 1;
    loop {
        let (mut conn, replica) = to_purge(&World::new(12)).await;
        let before = dump(&mut conn).await;
        let statements = support::statements::Statements::watch(&mut conn).await;
        statements.stop_at(n);
        let first = replica.purge_step(&mut conn, u64::MAX, None, 1).await;
        statements.stop_at(0);
        match first {
            Ok(_) => {
                assert!(n > 1);
                eprintln!("purge step: {} statements", n - 1);
                return;
            }
            Err(_) => assert_eq!(dump(&mut conn).await, before, "stopped at {n}"),
        }
        let steps = purge_all(&replica, &mut conn).await.unwrap();
        assert_eq!(steps, expected_steps, "after {n}");
        let out = sent(&replica, &mut conn).await;
        let fields = |out: &[Change]| -> Vec<(Gid, String, Value)> {
            out.iter()
                .map(|c| (c.gid, c.field.clone(), c.value.clone()))
                .collect()
        };
        assert_eq!(fields(&out), fields(&expected), "after {n}");
        n += 1;
    }
}

/// A server names a file by its content hash alone: one content held at two sizes is one file,
/// referred to while any value of that hash is held.
#[tokio::test]
async fn one_content_held_at_two_sizes_is_one_file() {
    let world = World::new(13);
    let replica = Replica::new(registry(1));
    let mut b = database(&world, 1, &replica).await;
    let right = FileRef::of(b"Middlemarch");
    let wrong = FileRef {
        hash: right.hash,
        size: right.size + 1,
    };
    let blob = |f: FileRef| Value::Blob(f.encode());
    replica
        .apply(
            &mut b,
            &[
                change("books", 1, DELETED, Value::Int(0), 1),
                change("books", 1, "cover", blob(right), 2),
                change("books", 2, DELETED, Value::Int(0), 3),
                change("books", 2, "cover", blob(wrong), 4),
            ],
        )
        .await
        .unwrap();
    let applied = replica
        .apply(&mut b, &[change("books", 2, "cover", blob(file(1)), 5)])
        .await
        .unwrap();
    assert!(applied.files.dropped.is_empty(), "{:?}", applied.files);
    assert!(refers(&replica, &mut b, right).await);
    assert!(refers(&replica, &mut b, wrong).await, "the same content");
    let mut listed = vec![(right, true), (file(1), true)];
    listed.sort_by_key(|(f, _)| f.encode());
    assert_eq!(every_file(&replica, &mut b, 1).await, listed);
    // Once the last value of that content goes, it drops, whatever its size.
    let applied = replica
        .apply(&mut b, &[change("books", 1, "cover", Value::Null, 6)])
        .await
        .unwrap();
    assert_eq!(applied.files.dropped, vec![right.hash]);
    // Two sizes of one content held at once are listed next to each other, one answer for both.
    let applied = replica
        .apply(
            &mut b,
            &[
                change("books", 1, "cover", blob(right), 7),
                change("books", 2, "cover", blob(wrong), 8),
            ],
        )
        .await
        .unwrap();
    assert_eq!(applied.files.added, {
        let mut both = vec![right, wrong];
        both.sort_by_key(|f| f.encode());
        both
    });
    assert_eq!(applied.files.dropped, vec![file(1).hash]);
    let mut both = vec![(right, true), (wrong, true)];
    both.sort_by_key(|(f, _)| f.encode());
    assert_eq!(every_file(&replica, &mut b, 1).await, both);
}

/// Rows found by a stepped install under an older registry are adopted as a whole install would
/// adopt them under the current one: a value that is not a reference in a field now naming files
/// is adopted as data, and the references adopted are reported.
#[tokio::test]
async fn rows_found_under_an_older_registry_are_checked_and_reported_under_the_current_one() {
    let older = Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("cover"),
    ])
    .unwrap();
    for bad in [false, true] {
        let world = World::new(14);
        let mut c = world.connect(0).await;
        run(
            &mut c,
            "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, cover)",
        )
        .await;
        for i in 0..6u8 {
            with_file(
                &mut c,
                &format!("INSERT INTO books (title, cover) VALUES ('t{i}', ?1)"),
                file(i),
            )
            .await;
        }
        if bad {
            run(&mut c, "UPDATE books SET cover = 'a path' WHERE id = 6").await;
        }
        assert!(
            Replica::new(older.clone())
                .install_step(&mut c, 1)
                .await
                .unwrap()
                .more
        );
        let newer = Replica::new(
            Registry::new(vec![
                Collection::minted("books", "books", "id")
                    .field("title")
                    .file("cover"),
            ])
            .unwrap(),
        );
        // A value that is not a reference is adopted as data, as a whole install adopts it.
        let mut added = newer.install_step(&mut c, 1).await.unwrap().files.added;
        let mut more = true;
        while more {
            let step = newer.collect_step(&mut c, 1).await.unwrap();
            added.extend(step.files.added);
            more = step.more;
        }
        added.sort_by_key(|f| f.encode());
        // The first row was adopted under the older registry, before `cover` named files.
        let last = if bad { 5 } else { 6 };
        let mut expected: Vec<FileRef> = (1..last).map(file).collect();
        expected.sort_by_key(|f| f.encode());
        assert_eq!(added, expected);
    }
}

/// A natural key brings the same entity back whenever a row of that key is made again, so a
/// purge leaves natural collections alone: a tag deleted here and made again elsewhere meanwhile
/// keeps the file it was made again with.
#[tokio::test]
async fn purging_leaves_a_natural_row_made_again_elsewhere_its_file() {
    let world = World::new(15);
    let replica = Replica::new(
        Registry::new(vec![
            Collection::natural("tags", "tags", &["name"])
                .field("name")
                .file("icon"),
        ])
        .unwrap(),
    );
    let (mut a, mut b) = (world.connect(0).await, world.connect(1).await);
    for c in [&mut a, &mut b] {
        run(
            c,
            "CREATE TABLE tags (name TEXT NOT NULL PRIMARY KEY, icon BLOB)",
        )
        .await;
        replica.install(c).await.unwrap();
    }
    let (x, y) = (file(1), file(2));
    with_file(
        &mut a,
        "INSERT INTO tags (name, icon) VALUES ('Middlemarch', ?1)",
        x,
    )
    .await;
    replica.collect(&mut a).await.unwrap();
    let made = sent(&replica, &mut a).await;
    replica.apply(&mut b, &made).await.unwrap();
    run(&mut a, "DELETE FROM tags").await;
    replica.collect(&mut a).await.unwrap();
    let from_a = sent(&replica, &mut a).await;
    world.later(60_000);
    run(&mut b, "DELETE FROM tags").await;
    with_file(
        &mut b,
        "INSERT INTO tags (name, icon) VALUES ('Middlemarch', ?1)",
        y,
    )
    .await;
    replica.collect(&mut b).await.unwrap();
    let from_b = sent(&replica, &mut b).await;
    world.later(60_000);
    let steps = purge(&replica, &mut a, u64::MAX, 10).await;
    assert!(
        steps.iter().all(|p| p.cleared == 0 && p.examined == 0),
        "{steps:?}"
    );
    let purged = sent(&replica, &mut a).await;
    assert!(purged.is_empty());
    replica.apply(&mut a, &from_b).await.unwrap();
    replica.apply(&mut b, &from_a).await.unwrap();
    for c in [&mut a, &mut b] {
        let icons: Vec<Vec<u8>> = sqlx::query_scalar("SELECT icon FROM tags")
            .fetch_all(&mut *c)
            .await
            .unwrap();
        assert_eq!(icons, vec![y.encode()]);
        assert!(refers(&replica, c, y).await);
    }
}

/// A minted entity whose own deletion wins never shows again, so neither does a clearing of its
/// files: not through a twin made elsewhere with its identity, which is another entity, nor
/// through an edit made elsewhere before the deletion arrived.
#[tokio::test]
async fn a_purged_minted_entity_never_shows_its_clearing() {
    let world = World::new(16);
    let replica = Replica::new(registry(1));
    let mut a = database(&world, 0, &replica).await;
    let mut b = database(&world, 1, &replica).await;
    with_file(
        &mut a,
        "INSERT INTO books (title, isbn, cover) VALUES ('Middlemarch', 'x', ?1)",
        file(1),
    )
    .await;
    replica.collect(&mut a).await.unwrap();
    let made = sent(&replica, &mut a).await;
    replica.apply(&mut b, &made).await.unwrap();
    // Here the book goes; elsewhere, not knowing, it takes another cover, goes too, and a twin
    // with its isbn is made with a cover of its own.
    run(&mut a, "DELETE FROM books").await;
    replica.collect(&mut a).await.unwrap();
    let deleted = sent(&replica, &mut a).await;
    world.later(60_000);
    with_file(&mut b, "UPDATE books SET cover = ?1", file(2)).await;
    replica.collect(&mut b).await.unwrap();
    run(&mut b, "DELETE FROM books").await;
    with_file(
        &mut b,
        "INSERT INTO books (title, isbn, cover) VALUES ('Middlemarch', 'x', ?1)",
        file(3),
    )
    .await;
    replica.collect(&mut b).await.unwrap();
    let elsewhere = sent(&replica, &mut b).await;
    world.later(60_000);
    replica.apply(&mut a, &elsewhere).await.unwrap();
    let steps = purge(&replica, &mut a, u64::MAX, 10).await;
    assert_eq!(
        steps.iter().map(|p| p.cleared).sum::<usize>(),
        1,
        "{steps:?}"
    );
    let purged = sent(&replica, &mut a).await;
    replica.apply(&mut b, &deleted).await.unwrap();
    replica.apply(&mut b, &purged).await.unwrap();
    for c in [&mut a, &mut b] {
        let covers: Vec<Vec<u8>> = sqlx::query_scalar("SELECT cover FROM books")
            .fetch_all(&mut *c)
            .await
            .unwrap();
        assert_eq!(covers, vec![file(3).encode()]);
        assert_eq!(
            replica
                .refers(c, &[hash(1), hash(2), hash(3)])
                .await
                .unwrap(),
            [false, false, true]
        );
    }
}

/// Every operation that can change the files referred to reports it: whole collects, installs
/// and resumes through their reporting forms too.
#[tokio::test]
async fn whole_collects_installs_and_resumes_report_their_files() {
    let world = World::new(17);
    let replica = Replica::new(registry(1));
    let mut a = world.connect(0).await;
    for sql in SCHEMA {
        run(&mut a, sql).await;
    }
    with_file(
        &mut a,
        "INSERT INTO books (title, cover) VALUES ('Middlemarch', ?1)",
        file(1),
    )
    .await;
    let installed = replica.install_reported(&mut a).await.unwrap();
    assert_eq!(installed.files, changes(&[file(1)], &[]));
    with_file(&mut a, "UPDATE books SET cover = ?1", file(2)).await;
    let collected = replica.collect_reported(&mut a).await.unwrap();
    assert_eq!(collected.files, changes(&[file(2)], &[file(1)]));
    assert!(!collected.more);
    replica.suspend(&mut a).await.unwrap();
    with_file(&mut a, "UPDATE books SET cover = ?1", file(3)).await;
    let resumed = replica
        .resume_reported(&mut a, fongo_sync_sqlite::Resume::Send)
        .await
        .unwrap();
    assert_eq!(resumed.files, changes(&[file(3)], &[file(2)]));
}

/// A device retires only while its state is exactly what it has merged: one call says so.
#[tokio::test]
async fn nothing_pending_holds_only_when_the_state_is_what_was_merged() {
    let world = World::new(18);
    let replica = Replica::new(registry(1));
    let mut a = world.connect(0).await;
    for sql in SCHEMA {
        run(&mut a, sql).await;
    }
    run(
        &mut a,
        "INSERT INTO books (title) VALUES ('Middlemarch'), ('Romola')",
    )
    .await;
    // A first install in steps that has not adopted every row it found.
    assert!(replica.install_step(&mut a, 1).await.unwrap().more);
    assert!(!replica.nothing_pending(&mut a).await.unwrap());
    while replica.collect_step(&mut a, 1).await.unwrap().more {}
    // Changes waiting in the outbox.
    assert!(!replica.nothing_pending(&mut a).await.unwrap());
    sent(&replica, &mut a).await;
    assert!(replica.nothing_pending(&mut a).await.unwrap());
    // A row written and not collected.
    run(&mut a, "UPDATE books SET title = 'Felix Holt' WHERE id = 1").await;
    assert!(!replica.nothing_pending(&mut a).await.unwrap());
    replica.collect(&mut a).await.unwrap();
    sent(&replica, &mut a).await;
    assert!(replica.nothing_pending(&mut a).await.unwrap());
    // Capture suspended: what is written meanwhile is not known.
    replica.suspend(&mut a).await.unwrap();
    assert!(!replica.nothing_pending(&mut a).await.unwrap());
    replica
        .resume(&mut a, fongo_sync_sqlite::Resume::Send)
        .await
        .unwrap();
    assert!(replica.nothing_pending(&mut a).await.unwrap());
}

/// Rows already there when they are adopted are adopted as data whatever their file fields
/// hold, by a whole install and by one in steps alike: a value that is not a reference names no
/// file. Only a write the application makes afterwards is refused, and correcting it clears it.
#[tokio::test]
async fn found_rows_are_adopted_whatever_their_file_fields_hold() {
    for stepped in [false, true] {
        let world = World::new(19);
        let mut c = world.connect(0).await;
        run(
            &mut c,
            "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT, cover BLOB)",
        )
        .await;
        let replica = Replica::new(
            Registry::new(vec![
                Collection::minted("books", "books", "id")
                    .field("title")
                    .file("cover"),
            ])
            .unwrap(),
        );
        for i in 0..6u8 {
            with_file(
                &mut c,
                "INSERT INTO books (title, cover) VALUES ('t', ?1)",
                file(i),
            )
            .await;
        }
        run(&mut c, "UPDATE books SET cover = X'0102' WHERE id = 5").await;
        if stepped {
            assert!(replica.install_step(&mut c, 1).await.unwrap().more);
            while replica.collect_step(&mut c, 1).await.unwrap().more {}
        } else {
            replica.install(&mut c).await.unwrap();
        }
        let out = sent(&replica, &mut c).await;
        assert!(
            out.iter()
                .any(|ch| ch.field == "cover" && ch.value == Value::Blob(vec![1, 2])),
            "stepped {stepped}: {out:?}"
        );
        assert!(
            !refers(&replica, &mut c, file(4)).await,
            "row 5 no longer holds it"
        );
        // The application's own write of the same kind is refused, and its correction taken.
        run(&mut c, "UPDATE books SET cover = X'0103' WHERE id = 4").await;
        assert!(matches!(
            replica.collect(&mut c).await,
            Err(Error::NotAFile { .. })
        ));
        run(&mut c, "UPDATE books SET cover = NULL WHERE id = 4").await;
        replica.collect(&mut c).await.unwrap();
        assert!(!replica.nothing_pending(&mut c).await.unwrap());
    }
}

/// A refused write is always cleared by correcting or deleting the row, whatever happened
/// between: a suspend and resume, an uninstall and install, or a write made while suspended.
#[tokio::test]
async fn a_refused_write_is_cleared_by_correcting_or_deleting_the_row() {
    let replica = Replica::new(registry(1));
    let bad = "UPDATE books SET cover = X'0102' WHERE id = 1";
    for path in 0..5 {
        let world = World::new(20);
        let mut a = database(&world, 0, &replica).await;
        run(&mut a, "INSERT INTO books (title) VALUES ('Middlemarch')").await;
        replica.collect(&mut a).await.unwrap();
        sent(&replica, &mut a).await;
        if path == 4 {
            replica.suspend(&mut a).await.unwrap();
            run(&mut a, bad).await;
            let resumed = replica
                .resume(&mut a, fongo_sync_sqlite::Resume::Send)
                .await;
            assert!(
                matches!(resumed, Err(Error::NotAFile { .. })),
                "{resumed:?}"
            );
            run(&mut a, "UPDATE books SET cover = NULL WHERE id = 1").await;
            replica
                .resume(&mut a, fongo_sync_sqlite::Resume::Send)
                .await
                .unwrap();
            assert!(
                sent(&replica, &mut a)
                    .await
                    .iter()
                    .all(|c| c.field != "cover")
            );
            continue;
        }
        run(&mut a, bad).await;
        assert!(matches!(
            replica.collect_step(&mut a, 10).await,
            Err(Error::NotAFile { .. })
        ));
        match path {
            0 => run(&mut a, "UPDATE books SET cover = NULL WHERE id = 1").await,
            1 => run(&mut a, "DELETE FROM books WHERE id = 1").await,
            2 => {
                replica.suspend(&mut a).await.unwrap();
                replica
                    .resume(&mut a, fongo_sync_sqlite::Resume::Send)
                    .await
                    .unwrap_err();
                replica.suspend(&mut a).await.unwrap();
                with_file(&mut a, "UPDATE books SET cover = ?1 WHERE id = 1", file(1)).await;
                replica
                    .resume(&mut a, fongo_sync_sqlite::Resume::Send)
                    .await
                    .unwrap();
            }
            _ => {
                // Installed again, the row is found and adopted as data.
                replica.uninstall(&mut a).await.unwrap();
                replica.install(&mut a).await.unwrap();
            }
        }
        replica.collect(&mut a).await.unwrap();
        replica.apply(&mut a, &[]).await.unwrap();
        replica.collect_step(&mut a, 10).await.unwrap();
        replica.suspend(&mut a).await.unwrap();
        replica
            .resume(&mut a, fongo_sync_sqlite::Resume::Send)
            .await
            .unwrap();
        sent(&replica, &mut a).await;
        assert!(
            replica.nothing_pending(&mut a).await.unwrap(),
            "path {path}"
        );
    }
}

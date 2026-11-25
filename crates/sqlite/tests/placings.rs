//! Collecting in steps lands where ordinary collects could. For histories drawn from a seed, a
//! replica writes a few rows and collects them in steps of one, two and three rows, by a collect,
//! by an install that adds collections, or around an apply; with each of those writes at a fixed
//! time, often the same millisecond, the outcome, once both replicas have exchanged everything,
//! must be one that collecting after some of the same writes gives, or failing that after some of
//! their statements. The history before them runs on a clock that may move only now and then.
//! `PLACINGS_CASES` sets how many histories (24 by default), `PLACINGS_SEED` the first seed.

mod support;

use std::collections::BTreeSet;
use std::ffi::{CString, c_int, c_void};
use std::str::FromStr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use fongo_sync_sqlite::{Collection, LocalDefault, Registry, Replica};
use libsqlite3_sys as ffi;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};

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

/// The registry before an install that adds series and volumes.
fn small_registry() -> Registry {
    let books = Collection::minted("books", "books", "id")
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

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

struct Source {
    rng: Mutex<u64>,
    wall: Arc<AtomicI64>,
    drift: Arc<Mutex<Drift>>,
}

/// How the clock moves by itself: on one reading in `every`, on average, or never for 0.
struct Drift {
    state: u64,
    every: u64,
}

type Call = unsafe extern "C" fn(*mut ffi::sqlite3_context, c_int, *mut *mut ffi::sqlite3_value);
unsafe extern "C" fn drop_source(source: *mut c_void) {
    drop(unsafe { Box::from_raw(source.cast::<Source>()) });
}
unsafe fn source<'a>(ctx: *mut ffi::sqlite3_context) -> &'a Source {
    unsafe { &*ffi::sqlite3_user_data(ctx).cast::<Source>() }
}
unsafe extern "C" fn randomblob(
    ctx: *mut ffi::sqlite3_context,
    _: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    let n = unsafe { ffi::sqlite3_value_int64(*argv) }.clamp(1, 1 << 20) as usize;
    let source = unsafe { source(ctx) };
    let mut rng = source.rng.lock().unwrap();
    let bytes: Vec<u8> = (0..n.div_ceil(8))
        .flat_map(|_| splitmix(&mut rng).to_le_bytes())
        .take(n)
        .collect();
    unsafe {
        ffi::sqlite3_result_blob(
            ctx,
            bytes.as_ptr().cast::<c_void>(),
            n as c_int,
            ffi::SQLITE_TRANSIENT(),
        )
    };
}
unsafe extern "C" fn random(
    ctx: *mut ffi::sqlite3_context,
    _: c_int,
    _: *mut *mut ffi::sqlite3_value,
) {
    let source = unsafe { source(ctx) };
    let value = splitmix(&mut source.rng.lock().unwrap()) as i64;
    unsafe { ffi::sqlite3_result_int64(ctx, value) };
}
unsafe extern "C" fn unixepoch(
    ctx: *mut ffi::sqlite3_context,
    _: c_int,
    _: *mut *mut ffi::sqlite3_value,
) {
    let source = unsafe { source(ctx) };
    let mut drift = source.drift.lock().unwrap();
    if drift.every > 0 {
        let mut x = drift.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        drift.state = x;
        if x.is_multiple_of(drift.every) {
            source.wall.fetch_add(1, Ordering::SeqCst);
        }
    }
    let ms = source.wall.load(Ordering::SeqCst);
    let seconds = (ms as f64 + 0.5) / 1000.0;
    unsafe { ffi::sqlite3_result_double(ctx, seconds) };
}

/// A database whose randomness comes from `seed` and whose wall clock is `wall`, which moves when
/// the test moves it and as `drift` says.
async fn connect(
    seed: u64,
    n: u64,
    wall: Arc<AtomicI64>,
    drift: Arc<Mutex<Drift>>,
) -> SqliteConnection {
    let mut conn = SqliteConnectOptions::from_str("sqlite::memory:")
        .unwrap()
        .foreign_keys(true)
        .connect()
        .await
        .unwrap();
    let mut rng = seed ^ n.wrapping_mul(0xD1B5_4A32_D192_ED03);
    let rng = splitmix(&mut rng);
    let mut handle = support::lock(&mut conn).await;
    let db = handle.as_raw_handle().as_ptr();
    for (name, args, call) in [
        ("randomblob", 1, randomblob as Call),
        ("random", 0, random as Call),
        ("unixepoch", 1, unixepoch as Call),
    ] {
        let mut start = rng ^ (name.len() as u64).wrapping_mul(0x8CB9_2BA7_2F3D_8DD7);
        let source = Box::new(Source {
            rng: Mutex::new(splitmix(&mut start)),
            wall: wall.clone(),
            drift: drift.clone(),
        });
        let cname = CString::new(name).unwrap();
        let status = unsafe {
            ffi::sqlite3_create_function_v2(
                db,
                cname.as_ptr(),
                args,
                ffi::SQLITE_UTF8 | ffi::SQLITE_INNOCUOUS,
                Box::into_raw(source).cast::<c_void>(),
                Some(call),
                None,
                None,
                Some(drop_source),
            )
        };
        assert_eq!(status, ffi::SQLITE_OK);
    }
    drop(handle);
    conn
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

async fn tables(conn: &mut SqliteConnection) -> String {
    let mut out = String::new();
    for t in TABLES {
        let rows = support::rows(conn, &format!("SELECT * FROM {t} ORDER BY rowid"), &[]).await;
        out.push_str(&format!("{t}: {rows:?}\n"));
    }
    out
}

#[derive(Debug, Clone)]
struct Case {
    seed: u64,
    /// Whether foreign keys are enforced.
    fk: bool,
    /// How A's writes are collected: 0 by a collect, 1 by an install that adds series and
    /// volumes, 2 by a collect with an apply after its first step.
    mode: u8,
    /// Writes on A or B (`true`) before A's, sent both ways where `None`.
    history: Vec<Option<(bool, u8, u8, u8)>>,
    /// A's writes: milliseconds since the last, possibly back, then kind and arguments.
    a: Vec<(i64, u8, u8, u8)>,
    /// B's writes, each after A's write of that number.
    b: Vec<(usize, u8, u8, u8)>,
    /// Before A's writes, the clock moves by itself on one reading in this many, or never for
    /// 0, so many writes of the history share a millisecond.
    every: u64,
}

/// How A collects its writes: after each write whose bit is set, after each statement whose bit
/// is set, or afterwards in steps of so many rows.
#[derive(Debug, Clone, Copy)]
enum How {
    Place(u32),
    Fine(u64),
    Steps(usize),
}

thread_local! {
    /// Statements to run in place of each of the case's writes on A, to pin a case down.
    static RAW: std::cell::RefCell<Option<Vec<Vec<String>>>> =
        const { std::cell::RefCell::new(None) };
}

fn a_write(i: usize, k: u8, x: u8, y: u8) -> Vec<String> {
    RAW.with(|r| r.borrow().as_ref().map(|v| v[i].clone()))
        .unwrap_or_else(|| write(k, x, y))
}
/// Both replicas' rows once A has collected its writes as `how` says and everything is exchanged.
async fn outcome(case: &Case, how: How) -> String {
    let wall = Arc::new(AtomicI64::new(1_700_000_000_000));
    let drift = Arc::new(Mutex::new(Drift {
        state: case.seed | 1,
        every: case.every,
    }));
    let mut a = connect(case.seed, 0, wall.clone(), drift.clone()).await;
    let mut b = connect(case.seed, 1, wall.clone(), drift.clone()).await;
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
    let full = Replica::new(registry(false));
    let small = Replica::new(small_registry());
    let ra = if case.mode == 1 { &small } else { &full };
    ra.install(&mut a).await.unwrap();
    full.install(&mut b).await.unwrap();
    let tick = |w: &Arc<AtomicI64>, d: i64| {
        w.fetch_add(d, Ordering::SeqCst);
    };
    run(
        &mut a,
        &LIBRARY.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
    )
    .await;
    tick(&wall, 7);
    send(ra, &mut a, &mut b).await;
    for entry in &case.history {
        tick(&wall, 3);
        match *entry {
            None => {
                send(ra, &mut a, &mut b).await;
                send(&full, &mut b, &mut a).await;
            }
            Some((on_b, k, x, y)) => {
                let conn = if on_b { &mut b } else { &mut a };
                run(conn, &write(k, x, y)).await;
            }
        }
    }
    tick(&wall, 5);
    send(ra, &mut a, &mut b).await;
    send(&full, &mut b, &mut a).await;
    tick(&wall, 1000);
    // From here every write is at the time the case gives it, however A collects.
    drift.lock().unwrap().every = 0;
    let mut si = 0usize;
    for (i, &(gap, k, x, y)) in case.a.iter().enumerate() {
        tick(&wall, gap);
        for st in a_write(i, k, x, y) {
            run(&mut a, &[st]).await;
            let hit = match how {
                How::Place(bits) => bits & (1 << i) != 0 && fine_last(case, i, si),
                How::Fine(bits) => bits & (1u64 << si) != 0,
                _ => false,
            };
            if hit {
                ra.collect(&mut a).await.unwrap();
            }
            si += 1;
        }
        for &(after, k, x, y) in &case.b {
            if after == i {
                run(&mut b, &write(k, x, y)).await;
            }
        }
    }
    tick(&wall, 10);
    full.collect(&mut b).await.unwrap();
    tick(&wall, 10);
    match how {
        How::Place(_) | How::Fine(_) => {
            if case.mode == 1 {
                full.install(&mut a).await.unwrap();
            }
            if case.mode == 2 {
                send(&full, &mut b, &mut a).await;
            }
        }
        How::Steps(rows) => {
            stepped(case, &full, &mut a, &mut b, rows).await;
        }
    }
    tick(&wall, 10);
    send(&full, &mut a, &mut b).await;
    send(&full, &mut b, &mut a).await;
    send(&full, &mut a, &mut b).await;
    format!("A\n{}B\n{}", loose(&mut a).await, tables(&mut b).await)
}

async fn stepped(
    case: &Case,
    full: &Replica,
    a: &mut SqliteConnection,
    b: &mut SqliteConnection,
    rows: usize,
) {
    let c = if case.mode == 1 {
        full.install_step(a, rows).await.unwrap()
    } else {
        full.collect_step(a, rows).await.unwrap()
    };
    let mut more = c.more;
    if case.mode == 2 {
        send(full, b, a).await;
        more = false;
    }
    let mut n = 0;
    while more {
        n += 1;
        assert!(n < 100_000);
        more = full.collect_step(a, rows).await.unwrap().more;
    }
}

fn make_case(seed: u64) -> Case {
    let mut s = seed;
    let mut r = |n: u64| splitmix(&mut s) % n;
    let mode = r(3) as u8;
    let fk = r(3) != 0;
    let nh = r(12) as usize;
    let history = (0..nh)
        .map(|_| {
            if r(6) == 0 {
                None
            } else {
                Some((r(2) == 0, r(256) as u8, r(8) as u8, r(256) as u8))
            }
        })
        .collect();
    let na = 1 + r(6) as usize;
    let a: Vec<_> = (0..na)
        .map(|_| {
            let gap = [0, 0, 1, 1, 5, 100, -3][r(7) as usize];
            // Often a series, which aliases, or a row removed and made again.
            let k = if r(3) == 0 {
                [30, 31, 35, 9, 13, 17, 34, 33][r(8) as usize]
            } else {
                r(36) as u8
            };
            (gap, k, r(8) as u8, r(256) as u8)
        })
        .collect();
    let nb = r(3) as usize;
    let b = (0..nb)
        .map(|_| {
            (
                r(na as u64) as usize,
                if r(2) == 0 {
                    [31, 32, 4, 15, 24, 35][r(6) as usize]
                } else {
                    r(36) as u8
                },
                r(8) as u8,
                r(256) as u8,
            )
        })
        .collect();
    let every = [0, 1, 3, 8, 40][r(5) as usize];
    Case {
        seed,
        fk,
        mode,
        history,
        a,
        b,
        every,
    }
}

/// Whether statement `si` is the last statement of write unit `i`.
fn fine_last(case: &Case, i: usize, si: usize) -> bool {
    let mut end = 0;
    for (j, &(_, k, x, y)) in case.a.iter().enumerate() {
        end += write(k, x, y).len();
        if j == i {
            return si + 1 == end;
        }
    }
    false
}
fn statements(case: &Case) -> usize {
    case.a
        .iter()
        .map(|&(_, k, x, y)| write(k, x, y).len())
        .sum()
}

/// A's rows without local ids, sorted: an earlier collect can change which rowid an insert gets.
async fn loose(conn: &mut SqliteConnection) -> String {
    let mut out = String::new();
    for (t, cols) in [
        ("shelves", "name"),
        ("authors", "name"),
        ("books", "title, isbn, status, finished, extra"),
        ("notes", "body"),
        ("marks", "place"),
        ("tags", "tag, note"),
        ("settings", "key, value"),
        ("covers", "path"),
        ("series", "code, name"),
        ("volumes", "title"),
    ] {
        let rows = support::rows(
            conn,
            &format!("SELECT {cols} FROM {t} ORDER BY {cols}"),
            &[],
        )
        .await;
        out.push_str(&format!("{t}: {rows:?}\n"));
    }
    out
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
}

/// Every outcome of collecting after some subset of the history's writes.
fn placings(rt: &tokio::runtime::Runtime, case: &Case) -> BTreeSet<String> {
    rt.block_on(async {
        let mut set = BTreeSet::new();
        for bits in 0..(1u32 << case.a.len()) {
            set.insert(outcome(case, How::Place(bits)).await);
        }
        set
    })
}

/// Whether collecting after some subset of the history's statements gives `got`.
fn some_fine_placing(rt: &tokio::runtime::Runtime, case: &Case, got: &str) -> bool {
    let n = statements(case);
    assert!(n <= 16, "{n} statements, too many to place each");
    rt.block_on(async {
        for bits in 0..(1u64 << n) {
            if outcome(case, How::Fine(bits)).await == got {
                return true;
            }
        }
        false
    })
}

#[test]
fn steps_land_where_some_placing_of_collects_does() {
    let env = |name: &str, default: u64| {
        std::env::var(name)
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default)
    };
    let (first, count) = (env("PLACINGS_SEED", 1), env("PLACINGS_CASES", 24));
    let rt = runtime();
    let mut failures = Vec::new();
    for i in 0..count {
        let seed = first.wrapping_mul(1_000_003).wrapping_add(i);
        let case = make_case(seed);
        let places = placings(&rt, &case);
        for rows in [1usize, 2, 3] {
            let got = rt.block_on(outcome(&case, How::Steps(rows)));
            if !places.contains(&got) && !some_fine_placing(&rt, &case, &got) {
                eprintln!("seed {seed}, steps of {rows}: {case:?}\n{got}");
                failures.push((seed, rows));
            }
        }
    }
    assert!(failures.is_empty(), "{failures:?}");
}

/// Two series made with one code in two writes, a third, and the second removed: steps must not
/// join what a collect after the removal would never see.
#[test]
fn series_found_to_be_one_and_then_removed() {
    let case = make_case(99000734);
    let raw = vec![
        write(case.a[0].1, case.a[0].2, case.a[0].3),
        vec!["INSERT INTO series (id, code, name) VALUES (4, 'c0', 'series0')".to_string()],
        vec!["INSERT INTO series (id, code, name) VALUES (5, 'c0', 'series0')".to_string()],
        vec!["INSERT INTO series (id, code, name) VALUES (6, 'c0', 'series2')".to_string()],
        vec!["DELETE FROM series WHERE id = 5".to_string()],
    ];
    RAW.with(|r| *r.borrow_mut() = Some(raw));
    let rt = runtime();
    let mut set = BTreeSet::new();
    for bits in 0..32u64 {
        set.insert(rt.block_on(outcome(&case, How::Fine(bits))));
    }
    for rows in [1usize, 2, 3] {
        let got = rt.block_on(outcome(&case, How::Steps(rows)));
        assert!(set.contains(&got), "steps of {rows}:\n{got}");
    }
}

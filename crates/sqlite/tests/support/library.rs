//! A reading library, the shape of registry and schema the kernel is first used with: foreign
//! keys enforced; minted collections with owners, references, sets written together, `Max` and
//! last-writer-wins fields and local dependents; natural collections owned by minted ones; and
//! identities, each enforced by a unique index. Writes of every kind such a library makes, chosen
//! from a seed, for tests that run it long.

use fongo_sync_sqlite::core::Strategy;
use fongo_sync_sqlite::{Collection, LocalDefault, Registry};
use sqlx::SqliteConnection;

pub const SCHEMA: &[&str] = &[
    "PRAGMA foreign_keys = ON",
    "CREATE TABLE books (
        id INTEGER PRIMARY KEY,
        title TEXT,
        isbn_13 TEXT UNIQUE,
        isbn_10 TEXT UNIQUE,
        series TEXT,
        series_index REAL,
        last_modified INTEGER)",
    "CREATE TABLE book_files (book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE, path TEXT)",
    "CREATE TABLE scales (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, top INTEGER,
        is_default INTEGER NOT NULL DEFAULT 0)",
    "CREATE UNIQUE INDEX one_default_scale ON scales(is_default) WHERE is_default = 1",
    "CREATE TABLE copies (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        form TEXT,
        updated_at INTEGER)",
    "CREATE TABLE readings (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        status TEXT,
        finished_at INTEGER,
        card_id INTEGER REFERENCES highlights(id) ON DELETE SET NULL,
        last_modified INTEGER)",
    "CREATE UNIQUE INDEX one_open_reading ON readings(book_id) WHERE finished_at IS NULL",
    "CREATE TABLE highlights (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        reading_id INTEGER REFERENCES readings(id) ON DELETE SET NULL,
        copy_id INTEGER REFERENCES copies(id) ON DELETE SET NULL,
        text TEXT,
        hash TEXT NOT NULL,
        UNIQUE (book_id, hash))",
    "CREATE TABLE notes (
        id INTEGER PRIMARY KEY,
        book_id INTEGER REFERENCES books(id) ON DELETE SET NULL,
        highlight_id INTEGER REFERENCES highlights(id) ON DELETE SET NULL,
        reading_id INTEGER REFERENCES readings(id) ON DELETE SET NULL,
        kind TEXT,
        path TEXT NOT NULL UNIQUE,
        title TEXT,
        updated INTEGER NOT NULL DEFAULT 0)",
    "CREATE UNIQUE INDEX one_reflection ON notes(reading_id) WHERE kind = 'reflection'",
    "CREATE TABLE external_ids (
        source TEXT NOT NULL,
        external_id TEXT NOT NULL,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        PRIMARY KEY (source, external_id))",
    "CREATE TABLE reviews (
        note_id INTEGER NOT NULL PRIMARY KEY REFERENCES notes(id) ON DELETE CASCADE,
        scale_id INTEGER REFERENCES scales(id),
        stars INTEGER)",
    "CREATE TABLE note_bodies (
        note_id INTEGER NOT NULL PRIMARY KEY REFERENCES notes(id) ON DELETE CASCADE,
        body TEXT)",
    "CREATE TABLE cards (
        id INTEGER PRIMARY KEY,
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        highlight_id INTEGER NOT NULL REFERENCES highlights(id) ON DELETE CASCADE,
        word TEXT NOT NULL,
        exported INTEGER,
        UNIQUE (book_id, word))",
    "CREATE TABLE citations (
        note_id INTEGER NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
        highlight_id INTEGER NOT NULL REFERENCES highlights(id) ON DELETE CASCADE,
        place INTEGER,
        PRIMARY KEY (note_id, highlight_id))",
    "CREATE TABLE book_tags (
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        tag TEXT NOT NULL,
        source TEXT NOT NULL,
        PRIMARY KEY (book_id, tag, source))",
];

/// The tables, in an order a dump reads them.
pub const TABLES: &[&str] = &[
    "books",
    "book_files",
    "scales",
    "copies",
    "readings",
    "highlights",
    "notes",
    "note_bodies",
    "cards",
    "citations",
    "book_tags",
    "external_ids",
    "reviews",
];

pub fn registry() -> Registry {
    Registry::new(vec![
        Collection::minted("books", "books", "id")
            .field("title")
            .field("isbn_13")
            .field("isbn_10")
            .field("series")
            .field("series_index")
            .field_with("last_modified", Strategy::Max)
            .identity(&["isbn_13"])
            .identity(&["isbn_10"])
            .together(&["series", "series_index"])
            .local_dependent("book_files", "book_id"),
        Collection::minted("scales", "scales", "id")
            .field("name")
            .field("top")
            .field("is_default")
            .identity(&["name"]),
        Collection::minted("copies", "copies", "id")
            .owner("book_id", "books")
            .field("form")
            .field_with("updated_at", Strategy::Max),
        Collection::minted("readings", "readings", "id")
            .owner("book_id", "books")
            .field("status")
            .field("finished_at")
            .reference("card_id", "highlights")
            .field_with("last_modified", Strategy::Max)
            .together(&["status", "finished_at"]),
        Collection::minted("highlights", "highlights", "id")
            .owner("book_id", "books")
            .reference("reading_id", "readings")
            .reference("copy_id", "copies")
            .field("text")
            .field("hash")
            .identity(&["book_id", "hash"]),
        Collection::minted("notes", "notes", "id")
            .reference("book_id", "books")
            .reference("highlight_id", "highlights")
            .reference("reading_id", "readings")
            .field("kind")
            .field("path")
            .field("title")
            .identity(&["path"])
            .local_default("updated", LocalDefault::Sql("unixepoch('now')".into())),
        Collection::natural("note_bodies", "note_bodies", &["note_id"])
            .owner("note_id", "notes")
            .field_reporting_conflicts("body"),
        Collection::natural("external_ids", "external_ids", &["source", "external_id"])
            .field("source")
            .field("external_id")
            .owner("book_id", "books"),
        Collection::natural("reviews", "reviews", &["note_id"])
            .owner("note_id", "notes")
            .reference("scale_id", "scales")
            .field("stars"),
        Collection::minted("cards", "cards", "id")
            .owner("book_id", "books")
            .owner("highlight_id", "highlights")
            .field("word")
            .field_with("exported", Strategy::Max)
            .identity(&["book_id", "word"]),
        Collection::natural("citations", "citations", &["note_id", "highlight_id"])
            .owner("note_id", "notes")
            .owner("highlight_id", "highlights")
            .field("place"),
        Collection::natural("book_tags", "book_tags", &["book_id", "tag", "source"])
            .owner("book_id", "books")
            .field("tag")
            .field("source"),
    ])
    .unwrap()
}

pub async fn run(conn: &mut SqliteConnection, sql: &str) {
    let _ = sqlx::query(sql).execute(&mut *conn).await;
}

/// A library of `books` books, each with a highlight, a note on it with its body, a tag and,
/// for every third, a reading, a copy and a card, written in one statement per table.
pub async fn existing(conn: &mut SqliteConnection, books: u64) {
    let n = |to: u64| {
        format!("WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {to})")
    };
    for sql in [
        format!(
            "{} INSERT INTO books (id, title, isbn_13, series, series_index, last_modified)
             SELECT i, 'Middlemarch ' || i, '978' || i, CASE WHEN i % 5 = 0 THEN 'series' END,
                    CASE WHEN i % 5 = 0 THEN i / 5 END, i FROM n",
            n(books)
        ),
        "INSERT INTO scales (id, name, top, is_default) VALUES (1, 'stars', 5, 1), (2, 'tens', 10, 0)".into(),
        format!(
            "{} INSERT INTO highlights (id, book_id, text, hash) SELECT i, i, 'passage ' || i, 'h' || i FROM n",
            n(books)
        ),
        format!(
            "{} INSERT INTO readings (id, book_id, status, finished_at, card_id, last_modified)
             SELECT i, i, 'done', i, i, i FROM n WHERE i % 3 = 0",
            n(books)
        ),
        format!(
            "{} INSERT INTO copies (id, book_id, form, updated_at) SELECT i, i, 'paper', i FROM n WHERE i % 3 = 0",
            n(books)
        ),
        "UPDATE highlights SET reading_id = id, copy_id = id WHERE id % 3 = 0".to_string(),
        format!(
            "{} INSERT INTO notes (id, book_id, highlight_id, path, title)
             SELECT i, i, i, 'notes/' || i || '.md', 'On passage ' || i FROM n",
            n(books)
        ),
        format!(
            "{} INSERT INTO note_bodies (note_id, body) SELECT i, 'Thoughts on ' || i FROM n",
            n(books)
        ),
        format!(
            "{} INSERT INTO cards (id, book_id, highlight_id, word, exported)
             SELECT i, i, i, 'word' || i, 0 FROM n WHERE i % 3 = 0",
            n(books)
        ),
        format!(
            "{} INSERT INTO citations (note_id, highlight_id, place) SELECT i, i, 0 FROM n",
            n(books)
        ),
        format!(
            "{} INSERT INTO book_tags (book_id, tag, source) SELECT i, 'read', 'shelf' FROM n",
            n(books)
        ),
        format!(
            "{} INSERT INTO book_files (book_id, path) SELECT i, 'files/' || i FROM n",
            n(books)
        ),
        format!(
            "{} INSERT INTO external_ids (source, external_id, book_id) SELECT 'catalogue', 'c' || i, i FROM n WHERE i % 2 = 0",
            n(books)
        ),
        "UPDATE notes SET reading_id = id, kind = 'reflection' WHERE id % 3 = 0".to_string(),
        format!(
            "{} INSERT INTO reviews (note_id, scale_id, stars) SELECT i, 1 + i % 2, i % 5 FROM n WHERE i % 4 = 0",
            n(books)
        ),
    ] {
        run(conn, &sql).await;
    }
}

/// A row of `table` chosen by `n`, as SQL.
fn pick(table: &str, n: u64) -> String {
    format!(
        "(SELECT rowid FROM {table} ORDER BY rowid LIMIT 1
          OFFSET {n} % max(1, (SELECT count(*) FROM {table})))"
    )
}

/// One write such a library makes, chosen by `kind` with arguments `a` and `b`: edits, imports,
/// removals that cascade, rows removed and made again at their ids, and sets written together.
pub fn write(kind: u64, a: u64, b: u64) -> Vec<String> {
    let book = pick("books", a);
    let book2 = pick("books", b);
    let one = |s: String| vec![s];
    match kind % 36 {
        0 => one(format!("UPDATE books SET title = 'title {b}' WHERE id = {book}")),
        1 => one(format!(
            "UPDATE books SET last_modified = coalesce(last_modified, 0) + {b} % 7 WHERE id = {book}"
        )),
        2 => one(format!(
            "INSERT INTO books (title, isbn_13) VALUES ('new {b}', '979{}')",
            b % 40
        )),
        3 => one(format!("DELETE FROM books WHERE id = {book}")),
        // A book removed and made again at its id, with a highlight.
        4 => vec![
            "CREATE TEMP TABLE IF NOT EXISTS chosen (id INTEGER)".into(),
            "DELETE FROM temp.chosen".into(),
            format!("INSERT INTO temp.chosen SELECT {book}"),
            "DELETE FROM books WHERE id = (SELECT id FROM temp.chosen)".into(),
            format!(
                "INSERT INTO books (id, title, isbn_13) SELECT id, 'again {b}', '977' || id FROM temp.chosen WHERE id IS NOT NULL"
            ),
            format!(
                "INSERT INTO highlights (book_id, text, hash) SELECT id, 'again', 'g{b}' FROM temp.chosen WHERE id IS NOT NULL"
            ),
        ],
        5 => one(format!(
            "INSERT OR IGNORE INTO highlights (book_id, text, hash) SELECT {book}, 'new {b}', 'n{}' WHERE EXISTS (SELECT 1 FROM books)",
            b % 9
        )),
        6 => one(format!(
            "UPDATE highlights SET text = 'edited {b}' WHERE id = {}",
            pick("highlights", a)
        )),
        7 => one(format!("DELETE FROM highlights WHERE id = {}", pick("highlights", a))),
        8 => vec![
            format!(
                "INSERT OR IGNORE INTO notes (book_id, highlight_id, path, title)
                 SELECT {book}, {}, 'notes/new' || {} || '.md', 'new'
                  WHERE EXISTS (SELECT 1 FROM books) AND EXISTS (SELECT 1 FROM highlights)",
                pick("highlights", b),
                b % 13
            ),
            "INSERT OR IGNORE INTO note_bodies (note_id, body) SELECT max(id), 'body' FROM notes HAVING max(id) IS NOT NULL".into(),
        ],
        9 => one(format!(
            "UPDATE note_bodies SET body = 'edited {b}' WHERE note_id = {}",
            pick("note_bodies", a)
        )),
        10 => one(format!("DELETE FROM notes WHERE id = {}", pick("notes", a))),
        11 => one(match b % 3 {
            0 => format!("UPDATE readings SET status = 'reading', finished_at = NULL WHERE id = {}", pick("readings", a)),
            1 => format!("UPDATE readings SET status = 'done', finished_at = {b} WHERE id = {}", pick("readings", a)),
            _ => format!(
                "INSERT OR IGNORE INTO readings (book_id, status, last_modified) SELECT {book}, 'reading', {b} WHERE EXISTS (SELECT 1 FROM books)"
            ),
        }),
        12 => one(format!(
            "UPDATE books SET series = 'set {}', series_index = {b} WHERE id = {book}",
            b % 3
        )),
        13 => one(format!(
            "INSERT OR IGNORE INTO book_tags (book_id, tag, source) SELECT {book}, 'tag{}', 'shelf' WHERE EXISTS (SELECT 1 FROM books)",
            b % 4
        )),
        14 => one(format!("DELETE FROM book_tags WHERE rowid = {}", pick("book_tags", a))),
        15 => one(format!(
            "INSERT OR IGNORE INTO cards (book_id, highlight_id, word, exported)
             SELECT book_id, id, 'w{}', 0 FROM highlights WHERE id = {}",
            b % 5,
            pick("highlights", a)
        )),
        16 => one(format!(
            "UPDATE cards SET exported = coalesce(exported, 0) + 1 WHERE id = {}",
            pick("cards", a)
        )),
        17 => one(format!(
            "INSERT OR IGNORE INTO citations (note_id, highlight_id, place) SELECT {}, {}, {b}
              WHERE EXISTS (SELECT 1 FROM notes) AND EXISTS (SELECT 1 FROM highlights)",
            pick("notes", a),
            pick("highlights", b)
        )),
        // An identity given to a row that held another, and one moved from row to row.
        18 => one(format!("UPDATE books SET isbn_13 = '555' WHERE id = {book}")),
        19 => vec![
            "CREATE TEMP TABLE IF NOT EXISTS swapped (id INTEGER, isbn TEXT)".into(),
            "DELETE FROM temp.swapped".into(),
            format!("INSERT INTO temp.swapped SELECT id, isbn_13 FROM books WHERE id = {book}"),
            "UPDATE books SET isbn_13 = NULL WHERE id = (SELECT id FROM temp.swapped)".into(),
            format!("UPDATE books SET isbn_13 = (SELECT isbn FROM temp.swapped) WHERE id = {book2}"),
        ],
        20 => one(format!(
            "UPDATE highlights SET reading_id = {} WHERE id = {}",
            pick("readings", b),
            pick("highlights", a)
        )),
        21 => one(format!(
            "UPDATE readings SET card_id = {} WHERE id = {}",
            pick("highlights", b),
            pick("readings", a)
        )),
        22 => one(format!(
            "UPDATE notes SET path = 'notes/{}.md' WHERE id = {}",
            b % 7,
            pick("notes", a)
        )),
        // References pointed at the newest rows.
        23 => one(format!(
            "UPDATE notes SET highlight_id = (SELECT max(id) FROM highlights), book_id = {book2} WHERE id = {}",
            pick("notes", a)
        )),
        // An import of many books at once, with a highlight and a note on each.
        24 => {
            let n = 20 + b % 60;
            vec![
                format!(
                    "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {n})
                     INSERT INTO books (id, title, isbn_13)
                     SELECT (SELECT max(id) FROM books) + i, 'imported ' || i,
                            '96-' || ((SELECT max(id) FROM books) + i) FROM n"
                ),
                "INSERT OR IGNORE INTO highlights (book_id, text, hash)
                 SELECT id, 'imp', 'h' || id FROM books
                  WHERE title LIKE 'imported%' AND id NOT IN (SELECT book_id FROM highlights)"
                    .into(),
                "INSERT OR IGNORE INTO notes (book_id, highlight_id, path, title)
                 SELECT book_id, id, 'imp/' || id || '.md', 'n' FROM highlights WHERE text = 'imp'"
                    .into(),
            ]
        }
        25 => one(format!(
            "UPDATE highlights SET book_id = {book2} WHERE id = {}",
            pick("highlights", a)
        )),
        26 => one(format!("DELETE FROM readings WHERE id = {}", pick("readings", a))),
        27 => vec![
            "UPDATE books SET isbn_13 = NULL WHERE id = 1".into(),
            format!("UPDATE books SET isbn_13 = '9781' WHERE id = {book}"),
        ],
        28 => one(format!(
            "UPDATE highlights SET hash = 'h1' WHERE id = {}",
            pick("highlights", a)
        )),
        29 => one(format!(
            "UPDATE scales SET name = CASE WHEN name = 'stars' THEN 'starsx' ELSE 'stars' END WHERE id = {}",
            pick("scales", a)
        )),
        30 => one(format!(
            "UPDATE cards SET word = 'word{}', exported = exported + 1 WHERE id = {}",
            b % 7,
            pick("cards", a)
        )),
        // A highlight removed and made again at its id, and a reading naming it.
        31 => vec![
            "CREATE TEMP TABLE IF NOT EXISTS remade (id INTEGER)".into(),
            "DELETE FROM temp.remade".into(),
            format!("INSERT INTO temp.remade SELECT {}", pick("highlights", a)),
            "DELETE FROM highlights WHERE id = (SELECT id FROM temp.remade)".into(),
            format!(
                "INSERT INTO highlights (id, book_id, text, hash)
                 SELECT id, {book}, 'remade', 'r{b}' FROM temp.remade WHERE id IS NOT NULL"
            ),
            format!(
                "INSERT OR IGNORE INTO readings (book_id, status, card_id, last_modified)
                 SELECT {book}, 'reading', id, 3 FROM temp.remade WHERE id IS NOT NULL"
            ),
        ],
        // The default scale moved, under its partial unique index.
        32 => vec![
            "UPDATE scales SET is_default = 0".into(),
            format!("UPDATE scales SET is_default = 1 WHERE id = {}", pick("scales", a)),
        ],
        33 => one(format!(
            "INSERT OR IGNORE INTO notes (book_id, reading_id, kind, path, title)
             SELECT book_id, id, 'reflection', 'reflections/' || id || '.md', 'r'
               FROM readings WHERE id = {}",
            pick("readings", a)
        )),
        34 => one(format!(
            "INSERT OR REPLACE INTO reviews (note_id, scale_id, stars) SELECT {}, {}, {}
              WHERE EXISTS (SELECT 1 FROM notes) AND EXISTS (SELECT 1 FROM scales)",
            pick("notes", a),
            pick("scales", b),
            b % 5
        )),
        _ => one(format!(
            "INSERT OR IGNORE INTO external_ids (source, external_id, book_id)
             SELECT 'catalogue', 'x{}', {book} WHERE EXISTS (SELECT 1 FROM books)",
            b % 9
        )),
    }
}

/// A library of `books` books, each with a highlight and a note on it, and nothing else.
pub async fn books_with_notes(conn: &mut SqliteConnection, books: u64) {
    let n = format!(
        "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < {books})"
    );
    for sql in [
        format!(
            "{n} INSERT INTO books (id, title, isbn_13, last_modified)
             SELECT i, 'Middlemarch ' || i, '978' || i, i FROM n"
        ),
        format!(
            "{n} INSERT INTO highlights (id, book_id, text, hash) SELECT i, i, 'passage ' || i, 'h' || i FROM n"
        ),
        format!(
            "{n} INSERT INTO notes (id, book_id, highlight_id, path, title)
             SELECT i, i, i, 'notes/' || i || '.md', 'On passage ' || i FROM n"
        ),
    ] {
        run(conn, &sql).await;
    }
}

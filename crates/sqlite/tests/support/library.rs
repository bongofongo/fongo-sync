//! A reading library, the shape of registry and schema the kernel is first used with: foreign
//! keys enforced; minted collections with owners, references, sets written together, `Max` and
//! last-writer-wins fields and local dependents; natural collections owned by minted ones; and
//! identities, each enforced by a unique index. Writes of every kind such a library makes, chosen
//! from a seed, for tests that run it long.

use fongo_sync_sqlite::core::Strategy;
use fongo_sync_sqlite::{Collection, Registry};
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
    "CREATE TABLE scales (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, top INTEGER)",
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
        path TEXT NOT NULL UNIQUE,
        title TEXT)",
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
            .field("path")
            .field("title")
            .identity(&["path"]),
        Collection::natural("note_bodies", "note_bodies", &["note_id"])
            .owner("note_id", "notes")
            .field("body"),
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
        "INSERT INTO scales (id, name, top) VALUES (1, 'stars', 5), (2, 'tens', 10)".into(),
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
    let one = |s: String| vec![s];
    match kind % 18 {
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
        _ => one(format!(
            "INSERT OR IGNORE INTO citations (note_id, highlight_id, place) SELECT {}, {}, {b}
              WHERE EXISTS (SELECT 1 FROM notes) AND EXISTS (SELECT 1 FROM highlights)",
            pick("notes", a),
            pick("highlights", b)
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

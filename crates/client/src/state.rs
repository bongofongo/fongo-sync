//! What the client keeps in the database, one row per library: its position in the server's log,
//! the envelope it is about to append and the head it knew then, the envelope its pull last
//! stopped at and one the application chose to step past; and where each envelope it appended
//! landed, with a digest of it, until it reads the log past it. No secret is stored here.
//!
//! The tables are of layout 2, recorded in `fongo_client_meta`. Layout 1 kept no record of its
//! layout; it is upgraded in place. A sync of a layout-1 build on these tables asks the server for
//! its capabilities, adds a row with cursor 0 for a library that has none, and then fails reading
//! the column `through`, which layout 2 renames, before it reads the log or writes anything else.

use fongo_sync_protocol::LibraryId;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection};

use crate::error::Error;

/// The layout of the client's tables this build reads and writes.
pub(crate) const LAYOUT: i64 = 2;

const META: &str = "CREATE TABLE IF NOT EXISTS fongo_client_meta (layout INTEGER NOT NULL)";

const STATE: &str = "CREATE TABLE IF NOT EXISTS fongo_client_state (
    library BLOB PRIMARY KEY,
    cursor INTEGER NOT NULL DEFAULT 0,
    sending_through INTEGER,
    sending_envelope BLOB,
    sending_after INTEGER,
    stopped_seq INTEGER,
    stopped_digest BLOB,
    passing_seq INTEGER,
    passing_digest BLOB)";

const APPENDED: &str = "CREATE TABLE IF NOT EXISTS fongo_client_appended (
    library BLOB NOT NULL,
    seq INTEGER NOT NULL,
    through INTEGER NOT NULL,
    digest BLOB,
    PRIMARY KEY (library, seq))";

/// What layout 2 adds to layout 1's `fongo_client_state`, after renaming two of its columns.
const ADDED_TO_STATE: [&str; 5] = [
    "sending_after INTEGER",
    "stopped_seq INTEGER",
    "stopped_digest BLOB",
    "passing_seq INTEGER",
    "passing_digest BLOB",
];

/// A SHA-256 of an envelope's bytes.
pub(crate) type Digest32 = [u8; 32];

pub(crate) fn digest(bytes: &[u8]) -> Digest32 {
    Sha256::digest(bytes).into()
}

pub(crate) struct State {
    pub(crate) cursor: u64,
    pub(crate) sending: Option<Sending>,
    /// Where this database's appends landed in the log, past the cursor, with the digest of each
    /// where it was recorded.
    pub(crate) appended: Vec<(u64, Option<Digest32>)>,
    /// The envelope the last pull stopped at, as a later version's.
    pub(crate) stopped: Option<(u64, Digest32)>,
    /// An envelope the application chose to step past.
    pub(crate) passing: Option<(u64, Digest32)>,
}

/// An envelope recorded before it was appended, with the outbox position it covers and the head
/// of the log the client knew when it recorded it, after which it can only have landed.
pub(crate) struct Sending {
    pub(crate) through: i64,
    pub(crate) envelope: Vec<u8>,
    pub(crate) after: Option<u64>,
}

fn corrupt(what: &str) -> Error {
    Error::Database(fongo_sync_sqlite::Error::Corrupt(what.into()))
}

fn seq_of(value: i64) -> Result<u64, Error> {
    u64::try_from(value).map_err(|_| corrupt("a sequence number is negative"))
}

fn as_i64(seq: u64) -> Result<i64, Error> {
    i64::try_from(seq).map_err(|_| corrupt("a sequence number exceeds i64"))
}

fn digest_of(bytes: Option<Vec<u8>>) -> Result<Option<Digest32>, Error> {
    bytes
        .map(|b| Digest32::try_from(b.as_slice()).map_err(|_| corrupt("a digest is not 32 bytes")))
        .transpose()
}

enum Layout {
    /// No client tables.
    None,
    /// Layout 1's tables, which record no layout.
    One,
    Current,
}

async fn exists(conn: &mut SqliteConnection, table: &str) -> Result<bool, Error> {
    Ok(
        sqlx::query("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(table)
            .fetch_optional(&mut *conn)
            .await?
            .is_some(),
    )
}

async fn has_column(conn: &mut SqliteConnection, table: &str, column: &str) -> Result<bool, Error> {
    Ok(
        sqlx::query("SELECT 1 FROM pragma_table_info(?) WHERE name = ?")
            .bind(table)
            .bind(column)
            .fetch_optional(&mut *conn)
            .await?
            .is_some(),
    )
}

/// The layout of the client's tables, refusing a later one or a record that is not one layout.
async fn layout(conn: &mut SqliteConnection) -> Result<Layout, Error> {
    if !exists(conn, "fongo_client_meta").await? {
        let any = exists(conn, "fongo_client_state").await?
            || exists(conn, "fongo_client_appended").await?;
        return Ok(if any { Layout::One } else { Layout::None });
    }
    let found: Vec<i64> = sqlx::query_scalar("SELECT layout FROM fongo_client_meta")
        .fetch_all(&mut *conn)
        .await?;
    match found[..] {
        [] => Ok(Layout::None),
        [LAYOUT] => Ok(Layout::Current),
        [later] if later > LAYOUT => Err(Error::NewerClientTables {
            found: later,
            known: LAYOUT,
        }),
        _ => Err(corrupt("the client's tables record no single known layout")),
    }
}

/// Refuses client tables of a later layout, reading only.
pub(crate) async fn check(conn: &mut SqliteConnection) -> Result<(), Error> {
    layout(conn).await.map(|_| ())
}

/// Makes the client's tables of [`LAYOUT`], upgrading layout 1 in place, in one transaction.
async fn prepare(conn: &mut SqliteConnection) -> Result<(), Error> {
    let mut tx = conn.begin().await?;
    // Written first, so the transaction holds the write lock before it looks.
    sqlx::query(META).execute(&mut *tx).await?;
    if let Layout::None | Layout::One = layout(&mut tx).await? {
        if has_column(&mut tx, "fongo_client_state", "through").await? {
            for sql in [
                "ALTER TABLE fongo_client_state RENAME COLUMN through TO sending_through",
                "ALTER TABLE fongo_client_state RENAME COLUMN envelope TO sending_envelope",
            ] {
                sqlx::query(sql).execute(&mut *tx).await?;
            }
            for column in ADDED_TO_STATE {
                sqlx::query(&format!(
                    "ALTER TABLE fongo_client_state ADD COLUMN {column}"
                ))
                .execute(&mut *tx)
                .await?;
            }
        }
        sqlx::query(STATE).execute(&mut *tx).await?;
        if exists(&mut tx, "fongo_client_appended").await?
            && !has_column(&mut tx, "fongo_client_appended", "digest").await?
        {
            sqlx::query("ALTER TABLE fongo_client_appended ADD COLUMN digest BLOB")
                .execute(&mut *tx)
                .await?;
        }
        sqlx::query(APPENDED).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO fongo_client_meta (layout) VALUES (?)")
            .bind(LAYOUT)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

pub(crate) async fn load(conn: &mut SqliteConnection, library: LibraryId) -> Result<State, Error> {
    if !matches!(layout(conn).await?, Layout::Current) {
        prepare(conn).await?;
    }
    sqlx::query("INSERT OR IGNORE INTO fongo_client_state (library) VALUES (?)")
        .bind(&library.0[..])
        .execute(&mut *conn)
        .await?;
    let row = sqlx::query(
        "SELECT cursor, sending_through, sending_envelope, sending_after, stopped_seq,
                stopped_digest, passing_seq, passing_digest
         FROM fongo_client_state WHERE library = ?",
    )
    .bind(&library.0[..])
    .fetch_one(&mut *conn)
    .await?;
    let cursor: i64 = row.try_get(0)?;
    let through: Option<i64> = row.try_get(1)?;
    let envelope: Option<Vec<u8>> = row.try_get(2)?;
    let after: Option<i64> = row.try_get(3)?;
    let mark = |seq: Option<i64>, digest: Option<Vec<u8>>| -> Result<_, Error> {
        Ok(match (seq, digest_of(digest)?) {
            (Some(seq), Some(digest)) => Some((seq_of(seq)?, digest)),
            _ => None,
        })
    };
    let stopped = mark(row.try_get(4)?, row.try_get(5)?)?;
    let passing = mark(row.try_get(6)?, row.try_get(7)?)?;
    let appended: Vec<(i64, Option<Vec<u8>>)> =
        sqlx::query_as("SELECT seq, digest FROM fongo_client_appended WHERE library = ?")
            .bind(&library.0[..])
            .fetch_all(&mut *conn)
            .await?;
    Ok(State {
        cursor: u64::try_from(cursor).map_err(|_| corrupt("the cursor is negative"))?,
        sending: match (through, envelope) {
            (Some(through), Some(envelope)) => Some(Sending {
                through,
                envelope,
                after: after.map(seq_of).transpose()?,
            }),
            _ => None,
        },
        appended: appended
            .into_iter()
            .map(|(seq, digest)| Ok((seq_of(seq)?, digest_of(digest)?)))
            .collect::<Result<_, Error>>()?,
        stopped,
        passing,
    })
}

/// Moves the cursor forward to `seq`; never back.
pub(crate) async fn advance(
    conn: &mut SqliteConnection,
    library: LibraryId,
    seq: u64,
) -> Result<(), Error> {
    sqlx::query("UPDATE fongo_client_state SET cursor = max(cursor, ?) WHERE library = ?")
        .bind(as_i64(seq)?)
        .bind(&library.0[..])
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Records that an envelope with `digest`, covering the outbox through `through`, landed at `seq`.
pub(crate) async fn record_appended(
    conn: &mut SqliteConnection,
    library: LibraryId,
    seq: u64,
    through: i64,
    digest: Digest32,
) -> Result<(), Error> {
    sqlx::query(
        "INSERT OR REPLACE INTO fongo_client_appended (library, seq, through, digest)
         VALUES (?, ?, ?, ?)",
    )
    .bind(&library.0[..])
    .bind(as_i64(seq)?)
    .bind(through)
    .bind(&digest[..])
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Records that the envelope with `digest`, recorded where a later copy of it landed, is first in
/// the log at `seq`.
pub(crate) async fn land_at(
    conn: &mut SqliteConnection,
    library: LibraryId,
    digest: Digest32,
    seq: u64,
) -> Result<(), Error> {
    let through: Option<i64> = sqlx::query_scalar(
        "SELECT max(through) FROM fongo_client_appended WHERE library = ? AND digest = ?",
    )
    .bind(&library.0[..])
    .bind(&digest[..])
    .fetch_one(&mut *conn)
    .await?;
    if let Some(through) = through {
        sqlx::query("DELETE FROM fongo_client_appended WHERE library = ? AND digest = ?")
            .bind(&library.0[..])
            .bind(&digest[..])
            .execute(&mut *conn)
            .await?;
        record_appended(conn, library, seq, through, digest).await?;
    }
    Ok(())
}

/// Forgets the appends at or before `read`, returning the furthest outbox position they covered.
pub(crate) async fn read_past(
    conn: &mut SqliteConnection,
    library: LibraryId,
    read: u64,
) -> Result<Option<i64>, Error> {
    let read = as_i64(read)?;
    let through: Option<i64> = sqlx::query_scalar(
        "SELECT max(through) FROM fongo_client_appended WHERE library = ? AND seq <= ?",
    )
    .bind(&library.0[..])
    .bind(read)
    .fetch_one(&mut *conn)
    .await?;
    if through.is_some() {
        sqlx::query("DELETE FROM fongo_client_appended WHERE library = ? AND seq <= ?")
            .bind(&library.0[..])
            .bind(read)
            .execute(&mut *conn)
            .await?;
    }
    Ok(through)
}

pub(crate) async fn record_sending(
    conn: &mut SqliteConnection,
    library: LibraryId,
    sending: &Sending,
) -> Result<(), Error> {
    sqlx::query(
        "UPDATE fongo_client_state
         SET sending_through = ?, sending_envelope = ?, sending_after = ? WHERE library = ?",
    )
    .bind(sending.through)
    .bind(&sending.envelope[..])
    .bind(sending.after.map(as_i64).transpose()?)
    .bind(&library.0[..])
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub(crate) async fn clear_sending(
    conn: &mut SqliteConnection,
    library: LibraryId,
) -> Result<(), Error> {
    sqlx::query(
        "UPDATE fongo_client_state
         SET sending_through = NULL, sending_envelope = NULL, sending_after = NULL
         WHERE library = ?",
    )
    .bind(&library.0[..])
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Records where the pull stopped at a later version's envelope, or that it did not.
pub(crate) async fn record_stop(
    conn: &mut SqliteConnection,
    library: LibraryId,
    stopped: Option<(u64, Digest32)>,
) -> Result<(), Error> {
    let seq = stopped.map(|(seq, _)| as_i64(seq)).transpose()?;
    sqlx::query(
        "UPDATE fongo_client_state SET stopped_seq = ?, stopped_digest = ? WHERE library = ?",
    )
    .bind(seq)
    .bind(stopped.map(|(_, digest)| digest.to_vec()))
    .bind(&library.0[..])
    .execute(&mut *conn)
    .await?;
    Ok(())
}

pub(crate) async fn clear_passing(
    conn: &mut SqliteConnection,
    library: LibraryId,
) -> Result<(), Error> {
    sqlx::query(
        "UPDATE fongo_client_state SET passing_seq = NULL, passing_digest = NULL
         WHERE library = ?",
    )
    .bind(&library.0[..])
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Lets the next sync step past the envelope its pull stopped at, at `seq` with bytes whose
/// SHA-256 is `digest`, as [`crate::Newer`] gives them: that envelope is skipped as
/// [`crate::Refusal::SteppedPast`] and the pull reads on. Fails with [`Error::NotStoppedAt`],
/// changing nothing, unless the last sync stopped at exactly that envelope and nothing has read
/// past it since.
///
/// This gives up that envelope's changes on this device for good: a build that could read them
/// does not go back for them. Offer it only on the user's say, for an envelope no build is
/// expected to read, such as one whose bytes were damaged.
///
/// It applies to a stop of every [`crate::Unknown`] kind. The next sync still reads the envelope
/// first: if it can now read it, because the server holds repaired bytes there or this device has
/// since learned the key of its epoch, it applies it and steps past nothing. Bytes other than
/// those named are never stepped past.
pub async fn step_past(
    conn: &mut SqliteConnection,
    library: LibraryId,
    seq: u64,
    digest: [u8; 32],
) -> Result<(), Error> {
    let not_there = Error::NotStoppedAt { seq };
    if !matches!(layout(conn).await?, Layout::Current) {
        return Err(not_there);
    }
    let mut tx = conn.begin().await?;
    let found = sqlx::query(
        "UPDATE fongo_client_state SET passing_seq = stopped_seq, passing_digest = stopped_digest
         WHERE library = ? AND stopped_seq = ? AND stopped_digest = ? AND cursor = ? - 1",
    )
    .bind(&library.0[..])
    .bind(as_i64(seq)?)
    .bind(&digest[..])
    .bind(as_i64(seq)?)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if found != 1 {
        tx.rollback().await?;
        return Err(not_there);
    }
    tx.commit().await?;
    Ok(())
}

/// The sequence number of the last envelope in `library`'s log that this database has applied or
/// knows it wrote; 0 before the first sync, and where the kernel is not installed. Fails, as the
/// kernel's own calls do, where its tables are of a layout other than the one this build uses, and
/// where the client's own tables are of a later layout than this build's.
pub async fn cursor(conn: &mut SqliteConnection, library: LibraryId) -> Result<u64, Error> {
    // One transaction, so the checks and the read see one state of the database.
    let mut tx = conn.begin().await?;
    let cursor = read_cursor(&mut tx, library).await;
    tx.rollback().await?;
    u64::try_from(cursor?.unwrap_or(0)).map_err(|_| corrupt("the cursor is negative"))
}

async fn read_cursor(
    conn: &mut SqliteConnection,
    library: LibraryId,
) -> Result<Option<i64>, Error> {
    if !fongo_sync_sqlite::installed(conn).await? {
        return Ok(None);
    }
    check(conn).await?;
    if !exists(conn, "fongo_client_state").await? {
        return Ok(None);
    }
    Ok(
        sqlx::query_scalar("SELECT cursor FROM fongo_client_state WHERE library = ?")
            .bind(&library.0[..])
            .fetch_optional(&mut *conn)
            .await?,
    )
}

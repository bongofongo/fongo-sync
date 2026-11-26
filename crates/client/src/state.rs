//! What the client keeps in the database, one row per library: its position in the server's log
//! and the envelope it is about to append; and where each envelope it appended landed, until it
//! reads the log past it. No secret is stored here.

use std::collections::BTreeSet;

use fongo_sync_protocol::LibraryId;
use sqlx::{Row, SqliteConnection};

use crate::error::Error;

const TABLE: &str = "CREATE TABLE IF NOT EXISTS fongo_client_state (
    library BLOB PRIMARY KEY,
    cursor INTEGER NOT NULL DEFAULT 0,
    through INTEGER,
    envelope BLOB)";

const APPENDED: &str = "CREATE TABLE IF NOT EXISTS fongo_client_appended (
    library BLOB NOT NULL,
    seq INTEGER NOT NULL,
    through INTEGER NOT NULL,
    PRIMARY KEY (library, seq))";

pub(crate) struct State {
    pub(crate) cursor: u64,
    pub(crate) sending: Option<Sending>,
    /// Where this database's appends landed in the log, past the cursor.
    pub(crate) appended: BTreeSet<u64>,
}

/// An envelope recorded before it was appended, with the outbox position it covers.
pub(crate) struct Sending {
    pub(crate) through: i64,
    pub(crate) envelope: Vec<u8>,
}

fn corrupt(what: &str) -> Error {
    Error::Database(fongo_sync_sqlite::Error::Corrupt(what.into()))
}

pub(crate) async fn load(conn: &mut SqliteConnection, library: LibraryId) -> Result<State, Error> {
    sqlx::query(TABLE).execute(&mut *conn).await?;
    sqlx::query(APPENDED).execute(&mut *conn).await?;
    sqlx::query("INSERT OR IGNORE INTO fongo_client_state (library) VALUES (?)")
        .bind(&library.0[..])
        .execute(&mut *conn)
        .await?;
    let row =
        sqlx::query("SELECT cursor, through, envelope FROM fongo_client_state WHERE library = ?")
            .bind(&library.0[..])
            .fetch_one(&mut *conn)
            .await?;
    let cursor: i64 = row.try_get(0)?;
    let through: Option<i64> = row.try_get(1)?;
    let envelope: Option<Vec<u8>> = row.try_get(2)?;
    let appended: Vec<i64> =
        sqlx::query_scalar("SELECT seq FROM fongo_client_appended WHERE library = ?")
            .bind(&library.0[..])
            .fetch_all(&mut *conn)
            .await?;
    Ok(State {
        cursor: u64::try_from(cursor).map_err(|_| corrupt("the cursor is negative"))?,
        sending: through
            .zip(envelope)
            .map(|(through, envelope)| Sending { through, envelope }),
        appended: appended
            .into_iter()
            .map(|seq| u64::try_from(seq).map_err(|_| corrupt("a sequence number is negative")))
            .collect::<Result<_, _>>()?,
    })
}

/// Moves the cursor forward to `seq`; never back.
pub(crate) async fn advance(
    conn: &mut SqliteConnection,
    library: LibraryId,
    seq: u64,
) -> Result<(), Error> {
    let seq = i64::try_from(seq).map_err(|_| corrupt("a sequence number exceeds i64"))?;
    sqlx::query("UPDATE fongo_client_state SET cursor = max(cursor, ?) WHERE library = ?")
        .bind(seq)
        .bind(&library.0[..])
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Records that an envelope covering the outbox through `through` landed at `seq`.
pub(crate) async fn record_appended(
    conn: &mut SqliteConnection,
    library: LibraryId,
    seq: u64,
    through: i64,
) -> Result<(), Error> {
    let seq = i64::try_from(seq).map_err(|_| corrupt("a sequence number exceeds i64"))?;
    sqlx::query(
        "INSERT OR REPLACE INTO fongo_client_appended (library, seq, through) VALUES (?, ?, ?)",
    )
    .bind(&library.0[..])
    .bind(seq)
    .bind(through)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Forgets the appends at or before `read`, returning the furthest outbox position they covered.
pub(crate) async fn read_past(
    conn: &mut SqliteConnection,
    library: LibraryId,
    read: u64,
) -> Result<Option<i64>, Error> {
    let read = i64::try_from(read).map_err(|_| corrupt("a sequence number exceeds i64"))?;
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
    sqlx::query("UPDATE fongo_client_state SET through = ?, envelope = ? WHERE library = ?")
        .bind(sending.through)
        .bind(&sending.envelope[..])
        .bind(&library.0[..])
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub(crate) async fn clear_sending(
    conn: &mut SqliteConnection,
    library: LibraryId,
) -> Result<(), Error> {
    sqlx::query("UPDATE fongo_client_state SET through = NULL, envelope = NULL WHERE library = ?")
        .bind(&library.0[..])
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// The sequence number of the last envelope in `library`'s log that this database has applied or
/// knows it wrote; 0 before the first sync, and where the kernel is not installed. Fails, as the
/// kernel's own calls do, where its tables are of a layout other than the one this build uses.
pub async fn cursor(conn: &mut SqliteConnection, library: LibraryId) -> Result<u64, Error> {
    if !fongo_sync_sqlite::installed(conn).await? {
        return Ok(0);
    }
    let exists = sqlx::query(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'fongo_client_state'",
    )
    .fetch_optional(&mut *conn)
    .await?
    .is_some();
    if !exists {
        return Ok(0);
    }
    let cursor: Option<i64> =
        sqlx::query_scalar("SELECT cursor FROM fongo_client_state WHERE library = ?")
            .bind(&library.0[..])
            .fetch_optional(&mut *conn)
            .await?;
    u64::try_from(cursor.unwrap_or(0)).map_err(|_| corrupt("the cursor is negative"))
}

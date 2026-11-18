//! What the client keeps in the database, one row per library: its position in the server's log
//! and the envelope it is about to append. No secret is stored here.

use fongo_sync_protocol::{DeviceId, LibraryId};
use sqlx::{Row, SqliteConnection};

use crate::error::Error;

const TABLE: &str = "CREATE TABLE IF NOT EXISTS fongo_client_state (
    library BLOB PRIMARY KEY,
    cursor INTEGER NOT NULL DEFAULT 0,
    author BLOB,
    through INTEGER,
    envelope BLOB)";

pub(crate) struct State {
    pub(crate) cursor: u64,
    /// The device this database has sent envelopes as, if any.
    pub(crate) author: Option<DeviceId>,
    pub(crate) sending: Option<Sending>,
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
    sqlx::query("INSERT OR IGNORE INTO fongo_client_state (library) VALUES (?)")
        .bind(&library.0[..])
        .execute(&mut *conn)
        .await?;
    let row = sqlx::query(
        "SELECT cursor, author, through, envelope FROM fongo_client_state WHERE library = ?",
    )
    .bind(&library.0[..])
    .fetch_one(&mut *conn)
    .await?;
    let cursor: i64 = row.try_get(0)?;
    let author: Option<Vec<u8>> = row.try_get(1)?;
    let through: Option<i64> = row.try_get(2)?;
    let envelope: Option<Vec<u8>> = row.try_get(3)?;
    let author = match author {
        None => None,
        Some(bytes) => Some(DeviceId(
            bytes
                .try_into()
                .map_err(|_| corrupt("a device id is not 16 bytes"))?,
        )),
    };
    Ok(State {
        cursor: u64::try_from(cursor).map_err(|_| corrupt("the cursor is negative"))?,
        author,
        sending: through
            .zip(envelope)
            .map(|(through, envelope)| Sending { through, envelope }),
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

pub(crate) async fn record_sending(
    conn: &mut SqliteConnection,
    library: LibraryId,
    author: DeviceId,
    sending: &Sending,
) -> Result<(), Error> {
    sqlx::query(
        "UPDATE fongo_client_state SET author = ?, through = ?, envelope = ? WHERE library = ?",
    )
    .bind(&author.0[..])
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
/// knows it wrote; 0 before the first sync.
pub async fn cursor(conn: &mut SqliteConnection, library: LibraryId) -> Result<u64, Error> {
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

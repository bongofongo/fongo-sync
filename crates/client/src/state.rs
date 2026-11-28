//! What the client keeps in the database, one row per library: its position in the server's log,
//! the digest of the envelope there with the position it is for, the log's fingerprint up to some
//! position, the envelope it is about to append and the head it knew then, the envelope its pull
//! last stopped at and one the application chose to step past, a fault found in the log and how
//! many times one was resolved; where each envelope it appended landed, with a digest of it, until
//! it reads the log past it; and where its latest appends that it has read past landed. No secret
//! is stored here.
//!
//! The tables are of layout 3, recorded in `fongo_client_meta`. Layouts 1 and 2 are upgraded in
//! place, in one transaction. Layout 1 kept no record of its layout: a sync of a layout-1 build on
//! these tables asks the server for its capabilities, adds a row with cursor 0 for a library that
//! has none, and then fails reading the column `through`, which layout 2 renamed, before it reads
//! the log or writes anything else. A build of layout 2 reads the layout first, so every sync of it
//! that begins after the upgrade refuses these tables with [`Error::NewerClientTables`].
//!
//! A sync of a layout-2 build already running when another connection upgrades the tables goes
//! on with its own statements, which touch none of layout 3's columns. It can move the cursor:
//! the digest kept for the old position is then not taken for the new one's, and the log is hashed
//! again from where the fingerprint stands up to the cursor, as after an upgrade, so neither a
//! fault nor a wrong fingerprint follows. It can drop records of appends read past without keeping
//! them in `fongo_client_passed`: an append later answered at such a position is then read back
//! before it is taken for reuse. It does not see a stored fault and goes on applying and sending;
//! and a resolution while it runs is undone in part where it moves the cursor up again after the
//! resolution set it to 0, so that this device does not read again what lies before that.

use fongo_sync_protocol::LibraryId;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection};

use crate::error::Error;
use crate::fault::LogFault;

/// The layout of the client's tables this build reads and writes.
pub(crate) const LAYOUT: i64 = 3;

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
    passing_digest BLOB,
    anchor BLOB,
    anchor_seq INTEGER,
    chain_seq INTEGER NOT NULL DEFAULT 0,
    chain BLOB,
    fault_kind INTEGER,
    fault_seq INTEGER,
    fault_other INTEGER,
    resets INTEGER NOT NULL DEFAULT 0,
    tracked_from INTEGER NOT NULL DEFAULT 0)";

const APPENDED: &str = "CREATE TABLE IF NOT EXISTS fongo_client_appended (
    library BLOB NOT NULL,
    seq INTEGER NOT NULL,
    through INTEGER NOT NULL,
    digest BLOB,
    PRIMARY KEY (library, seq))";

const PASSED: &str = "CREATE TABLE IF NOT EXISTS fongo_client_passed (
    library BLOB NOT NULL,
    seq INTEGER NOT NULL,
    digest BLOB,
    PRIMARY KEY (library, seq))";

/// What layout 2 adds to layout 1's `fongo_client_state`, after renaming two of its columns.
const ADDED_IN_TWO: [(&str, &str); 5] = [
    ("sending_after", "INTEGER"),
    ("stopped_seq", "INTEGER"),
    ("stopped_digest", "BLOB"),
    ("passing_seq", "INTEGER"),
    ("passing_digest", "BLOB"),
];

/// What layout 3 adds to layout 2's `fongo_client_state`.
const ADDED_IN_THREE: [(&str, &str); 9] = [
    ("anchor", "BLOB"),
    ("anchor_seq", "INTEGER"),
    ("chain_seq", "INTEGER NOT NULL DEFAULT 0"),
    ("chain", "BLOB"),
    ("fault_kind", "INTEGER"),
    ("fault_seq", "INTEGER"),
    ("fault_other", "INTEGER"),
    ("resets", "INTEGER NOT NULL DEFAULT 0"),
    ("tracked_from", "INTEGER NOT NULL DEFAULT 0"),
];

/// How many of this database's appends it keeps a record of once read past, so that an append
/// answered with the place of an earlier copy of the same envelope is told from reuse.
const PASSED_KEPT: i64 = 1_024;

/// A SHA-256 of an envelope's bytes.
pub(crate) type Digest32 = [u8; 32];

pub(crate) fn digest(bytes: &[u8]) -> Digest32 {
    Sha256::digest(bytes).into()
}

const CHAIN_DOMAIN: &[u8] = b"fongo-sync/v1/log-chain\0";

/// The fingerprint of an empty log: SHA-256 of the domain, a zero byte and the library id.
pub(crate) fn chain_start(library: LibraryId) -> Digest32 {
    let mut hash = Sha256::new();
    hash.update(CHAIN_DOMAIN);
    hash.update([0]);
    hash.update(library.0);
    hash.finalize().into()
}

/// The fingerprint through `seq`, from the one through `seq - 1` and the digest of the envelope
/// at `seq`: SHA-256 of the domain, a one byte, the previous fingerprint, `seq` as eight
/// big-endian bytes and the digest.
pub(crate) fn chain_step(previous: Digest32, seq: u64, envelope: Digest32) -> Digest32 {
    let mut hash = Sha256::new();
    hash.update(CHAIN_DOMAIN);
    hash.update([1]);
    hash.update(previous);
    hash.update(seq.to_be_bytes());
    hash.update(envelope);
    hash.finalize().into()
}

pub(crate) struct State {
    pub(crate) cursor: u64,
    /// The digest of the envelope at the cursor, when known.
    pub(crate) anchor: Option<Digest32>,
    /// The log's fingerprint through a position at or before the cursor.
    pub(crate) chain: (u64, Digest32),
    pub(crate) sending: Option<Sending>,
    /// Where this database's appends landed in the log, past the cursor, with the digest of each
    /// where it was recorded.
    pub(crate) appended: Vec<(u64, Option<Digest32>)>,
    /// The envelope the last pull stopped at, as a later version's.
    pub(crate) stopped: Option<(u64, Digest32)>,
    /// An envelope the application chose to step past.
    pub(crate) passing: Option<(u64, Digest32)>,
    pub(crate) fault: Option<LogFault>,
    /// How many times a fault was resolved; a sync that loaded another count writes nothing.
    pub(crate) resets: i64,
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
    Two,
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
        [2] => Ok(Layout::Two),
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

/// Adds each of `columns` that `table` lacks.
async fn add_columns(
    conn: &mut SqliteConnection,
    table: &str,
    columns: &[(&str, &str)],
) -> Result<(), Error> {
    for (name, kind) in columns {
        if !has_column(conn, table, name).await? {
            sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN {name} {kind}"))
                .execute(&mut *conn)
                .await?;
        }
    }
    Ok(())
}

/// Makes the client's tables of [`LAYOUT`], upgrading layouts 1 and 2 in place, in one
/// transaction.
async fn prepare(conn: &mut SqliteConnection) -> Result<(), Error> {
    // Made on its own first, as an empty table records no layout: creating a table that exists
    // writes nothing, so it cannot be what takes the write lock below.
    sqlx::query(META).execute(&mut *conn).await?;
    let mut tx = conn.begin().await?;
    // A write even when it changes no row, so the transaction holds the write lock before it
    // reads anything, and another connection upgrading at once waits for it in the busy handler.
    sqlx::query("UPDATE fongo_client_meta SET layout = layout")
        .execute(&mut *tx)
        .await?;
    let found = layout(&mut tx).await?;
    if let Layout::None | Layout::One = found {
        if has_column(&mut tx, "fongo_client_state", "through").await? {
            for sql in [
                "ALTER TABLE fongo_client_state RENAME COLUMN through TO sending_through",
                "ALTER TABLE fongo_client_state RENAME COLUMN envelope TO sending_envelope",
            ] {
                sqlx::query(sql).execute(&mut *tx).await?;
            }
            add_columns(&mut tx, "fongo_client_state", &ADDED_IN_TWO).await?;
        }
        if exists(&mut tx, "fongo_client_appended").await?
            && !has_column(&mut tx, "fongo_client_appended", "digest").await?
        {
            sqlx::query("ALTER TABLE fongo_client_appended ADD COLUMN digest BLOB")
                .execute(&mut *tx)
                .await?;
        }
    }
    if !matches!(found, Layout::Current) {
        if exists(&mut tx, "fongo_client_state").await? {
            add_columns(&mut tx, "fongo_client_state", &ADDED_IN_THREE).await?;
            // Appends read past before now left no record, so none is known up to the cursor.
            sqlx::query("UPDATE fongo_client_state SET tracked_from = cursor")
                .execute(&mut *tx)
                .await?;
        }
        for sql in [STATE, APPENDED, PASSED] {
            sqlx::query(sql).execute(&mut *tx).await?;
        }
        sqlx::query("DELETE FROM fongo_client_meta")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO fongo_client_meta (layout) VALUES (?)")
            .bind(LAYOUT)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// The stored fingerprint and its position; none stored at position 0 is the empty log's.
fn chain_of(
    library: LibraryId,
    seq: i64,
    chain: Option<Vec<u8>>,
) -> Result<(u64, Digest32), Error> {
    let seq = seq_of(seq)?;
    match digest_of(chain)? {
        Some(chain) => Ok((seq, chain)),
        None if seq == 0 => Ok((0, chain_start(library))),
        None => Err(corrupt("a fingerprint's position is stored without it")),
    }
}

/// A stored fault, from its columns.
fn fault_of(
    kind: Option<i64>,
    seq: Option<i64>,
    other: Option<i64>,
) -> Result<Option<LogFault>, Error> {
    let Some(kind) = kind else {
        return Ok(None);
    };
    let seq = seq_of(seq.ok_or_else(|| corrupt("a stored fault has no position"))?)?;
    let other = other.map(seq_of).transpose()?;
    Ok(Some(match (kind, other) {
        (1, None) => LogFault::Behind { cursor: seq },
        (2, None) => LogFault::Rewritten { seq },
        (3, None) => LogFault::Diverged { seq },
        (4, Some(furthest)) => LogFault::Reused { seq, furthest },
        (5, None) => LogFault::AppendReplaced { seq },
        (6, None) => LogFault::AppendMissing { seq },
        _ => return Err(corrupt("a stored fault is of no kind this build knows")),
    }))
}

/// A fault's columns: its kind, position and second number.
fn fault_columns(fault: LogFault) -> (i64, u64, Option<u64>) {
    match fault {
        LogFault::Behind { cursor } => (1, cursor, None),
        LogFault::Rewritten { seq } => (2, seq, None),
        LogFault::Diverged { seq } => (3, seq, None),
        LogFault::Reused { seq, furthest } => (4, seq, Some(furthest)),
        LogFault::AppendReplaced { seq } => (5, seq, None),
        LogFault::AppendMissing { seq } => (6, seq, None),
    }
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
                stopped_digest, passing_seq, passing_digest, anchor, chain_seq, chain,
                fault_kind, fault_seq, fault_other, resets, anchor_seq
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
    // A digest stored for another position than the cursor, as when a build that keeps none moved
    // the cursor, is not known to be the cursor's.
    let anchor_seq: Option<i64> = row.try_get(15)?;
    let anchor = digest_of(row.try_get(8)?)?.filter(|_| anchor_seq == Some(cursor));
    let chain = chain_of(library, row.try_get(9)?, row.try_get(10)?)?;
    let fault = fault_of(row.try_get(11)?, row.try_get(12)?, row.try_get(13)?)?;
    let resets: i64 = row.try_get(14)?;
    let appended: Vec<(i64, Option<Vec<u8>>)> =
        sqlx::query_as("SELECT seq, digest FROM fongo_client_appended WHERE library = ?")
            .bind(&library.0[..])
            .fetch_all(&mut *conn)
            .await?;
    Ok(State {
        cursor: u64::try_from(cursor).map_err(|_| corrupt("the cursor is negative"))?,
        anchor,
        chain,
        fault,
        resets,
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

/// Whether another sync of this database stored `seq` as its cursor with another envelope there
/// or another fingerprint through it than `anchor` and `chain`: the server served two logs.
pub(crate) async fn diverges(
    conn: &mut SqliteConnection,
    library: LibraryId,
    seq: u64,
    anchor: Option<Digest32>,
    chain: Option<Digest32>,
) -> Result<Option<LogFault>, Error> {
    let row = sqlx::query(
        "SELECT cursor, anchor, chain_seq, chain, anchor_seq FROM fongo_client_state
         WHERE library = ?",
    )
    .bind(&library.0[..])
    .fetch_one(&mut *conn)
    .await?;
    let at = as_i64(seq)?;
    if row.try_get::<i64, _>(0)? != at {
        return Ok(None);
    }
    let anchor_seq: Option<i64> = row.try_get(4)?;
    let stored_anchor = digest_of(row.try_get(1)?)?.filter(|_| anchor_seq == Some(at));
    let (chain_seq, stored_chain) = chain_of(library, row.try_get(2)?, row.try_get(3)?)?;
    let differs = |stored: Option<Digest32>, mine: Option<Digest32>| {
        stored.zip(mine).is_some_and(|(s, m)| s != m)
    };
    let diverged =
        differs(stored_anchor, anchor) || (chain_seq == seq && differs(Some(stored_chain), chain));
    Ok(diverged.then_some(LogFault::Diverged { seq }))
}

/// Moves the cursor forward to `seq`, with the digest of the envelope there and, when known, the
/// fingerprint through it, all in one statement; never back. Where another sync already stored
/// `seq`, fills in what it lacks; [`diverges`] says first whether the two disagree.
pub(crate) async fn advance(
    conn: &mut SqliteConnection,
    library: LibraryId,
    seq: u64,
    anchor: Option<Digest32>,
    chain: Option<Digest32>,
) -> Result<(), Error> {
    let at = as_i64(seq)?;
    let moved = sqlx::query(
        "UPDATE fongo_client_state SET cursor = ?1, anchor = ?2,
            anchor_seq = CASE WHEN ?2 IS NULL THEN NULL ELSE ?1 END,
            chain_seq = CASE WHEN ?3 IS NULL THEN chain_seq ELSE ?1 END,
            chain = CASE WHEN ?3 IS NULL THEN chain ELSE ?3 END
         WHERE library = ?4 AND cursor < ?1",
    )
    .bind(at)
    .bind(anchor.map(|a| a.to_vec()))
    .bind(chain.map(|c| c.to_vec()))
    .bind(&library.0[..])
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if moved == 1 {
        return Ok(());
    }
    sqlx::query(
        "UPDATE fongo_client_state SET
            anchor = CASE WHEN ?1 IS NOT NULL AND (anchor IS NULL OR anchor_seq IS NOT ?3)
                          THEN ?1 ELSE anchor END,
            anchor_seq = CASE WHEN ?1 IS NOT NULL AND (anchor IS NULL OR anchor_seq IS NOT ?3)
                              THEN ?3 ELSE anchor_seq END,
            chain = CASE WHEN ?2 IS NOT NULL AND chain_seq < ?3 THEN ?2 ELSE chain END,
            chain_seq = CASE WHEN ?2 IS NOT NULL AND chain_seq < ?3 THEN ?3 ELSE chain_seq END
         WHERE library = ?4 AND cursor = ?3",
    )
    .bind(anchor.map(|a| a.to_vec()))
    .bind(chain.map(|c| c.to_vec()))
    .bind(at)
    .bind(&library.0[..])
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Stores the fingerprint through `at`, hashed on from the one stored through `from`, unless
/// another sync stored a fingerprint meanwhile; with `anchor`, the digest of the envelope at
/// `at`, where `at` is the cursor and none is stored.
pub(crate) async fn record_chain(
    conn: &mut SqliteConnection,
    library: LibraryId,
    from: u64,
    at: u64,
    chain: Digest32,
    anchor: Option<Digest32>,
) -> Result<(), Error> {
    sqlx::query(
        "UPDATE fongo_client_state SET chain_seq = ?1, chain = ?2,
            anchor = CASE WHEN cursor = ?1 AND ?3 IS NOT NULL
                               AND (anchor IS NULL OR anchor_seq IS NOT ?1)
                          THEN ?3 ELSE anchor END,
            anchor_seq = CASE WHEN cursor = ?1 AND ?3 IS NOT NULL
                                   AND (anchor IS NULL OR anchor_seq IS NOT ?1)
                              THEN ?1 ELSE anchor_seq END
         WHERE library = ?4 AND chain_seq = ?5 AND cursor >= ?1",
    )
    .bind(as_i64(at)?)
    .bind(&chain[..])
    .bind(anchor.map(|a| a.to_vec()))
    .bind(&library.0[..])
    .bind(as_i64(from)?)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Fails with the stored fault, or with [`Error::ResolvedMeanwhile`] when a fault was resolved
/// since the sync loaded `resets`.
pub(crate) async fn check_turn(
    conn: &mut SqliteConnection,
    library: LibraryId,
    resets: i64,
) -> Result<(), Error> {
    let row = sqlx::query(
        "SELECT fault_kind, fault_seq, fault_other, resets FROM fongo_client_state
         WHERE library = ?",
    )
    .bind(&library.0[..])
    .fetch_one(&mut *conn)
    .await?;
    if let Some(fault) = fault_of(row.try_get(0)?, row.try_get(1)?, row.try_get(2)?)? {
        return Err(Error::LogFault(fault));
    }
    if row.try_get::<i64, _>(3)? != resets {
        return Err(Error::ResolvedMeanwhile);
    }
    Ok(())
}

/// Stores `fault`, found by a sync that loaded `resets`, in a transaction of its own, unless a
/// fault is stored already or one was resolved since; returns the fault stored.
pub(crate) async fn record_fault(
    conn: &mut SqliteConnection,
    library: LibraryId,
    resets: i64,
    fault: LogFault,
) -> Result<LogFault, Error> {
    let mut tx = conn.begin().await?;
    let stored = record_fault_in(&mut tx, library, resets, fault).await?;
    tx.commit().await?;
    stored.ok_or(Error::ResolvedMeanwhile)
}

/// [`record_fault`] in the caller's transaction; `None` when a fault was resolved since.
pub(crate) async fn record_fault_in(
    conn: &mut SqliteConnection,
    library: LibraryId,
    resets: i64,
    fault: LogFault,
) -> Result<Option<LogFault>, Error> {
    let (kind, seq, other) = fault_columns(fault);
    sqlx::query(
        "UPDATE fongo_client_state SET fault_kind = ?1, fault_seq = ?2, fault_other = ?3
         WHERE library = ?4 AND fault_kind IS NULL AND resets = ?5",
    )
    .bind(kind)
    .bind(as_i64(seq)?)
    .bind(other.map(as_i64).transpose()?)
    .bind(&library.0[..])
    .bind(resets)
    .execute(&mut *conn)
    .await?;
    let row = sqlx::query(
        "SELECT fault_kind, fault_seq, fault_other FROM fongo_client_state WHERE library = ?",
    )
    .bind(&library.0[..])
    .fetch_one(&mut *conn)
    .await?;
    fault_of(row.try_get(0)?, row.try_get(1)?, row.try_get(2)?)
}

/// Whether an append of the envelope with `digest`, answered with `seq`, may reuse a position:
/// `seq` is at or before the stored cursor or a recorded append of this database, and no record
/// says this envelope was there. Positions up to where records began cannot be told and pass. It
/// is proof of nothing yet: the server may hold a copy of the envelope there, as when two syncs
/// appended it and their answers came back out of order.
pub(crate) async fn reused(
    conn: &mut SqliteConnection,
    library: LibraryId,
    seq: u64,
    digest: Digest32,
) -> Result<Option<LogFault>, Error> {
    let row = sqlx::query(
        "SELECT max(cursor, coalesce((SELECT max(seq) FROM fongo_client_appended
                                       WHERE library = ?1), 0)),
                tracked_from
         FROM fongo_client_state WHERE library = ?1",
    )
    .bind(&library.0[..])
    .fetch_one(&mut *conn)
    .await?;
    let read = seq_of(row.try_get(0)?)?;
    let tracked_from = seq_of(row.try_get(1)?)?;
    if seq > read || seq <= tracked_from {
        return Ok(None);
    }
    let at = as_i64(seq)?;
    let there = sqlx::query(
        "SELECT 1 FROM fongo_client_appended
          WHERE library = ?1 AND seq = ?2 AND (digest IS NULL OR digest = ?3)
         UNION ALL
         SELECT 1 FROM fongo_client_passed
          WHERE library = ?1 AND seq = ?2 AND (digest IS NULL OR digest = ?3)",
    )
    .bind(&library.0[..])
    .bind(at)
    .bind(&digest[..])
    .fetch_optional(&mut *conn)
    .await?
    .is_some();
    Ok((!there).then_some(LogFault::Reused {
        seq,
        furthest: read,
    }))
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
        for sql in [
            "INSERT OR REPLACE INTO fongo_client_passed (library, seq, digest)
             SELECT library, seq, digest FROM fongo_client_appended
              WHERE library = ?1 AND seq <= ?2",
            "DELETE FROM fongo_client_appended WHERE library = ?1 AND seq <= ?2",
        ] {
            sqlx::query(sql)
                .bind(&library.0[..])
                .bind(read)
                .execute(&mut *conn)
                .await?;
        }
        let oldest_kept: Option<i64> = sqlx::query_scalar(
            "SELECT seq FROM fongo_client_passed WHERE library = ?
             ORDER BY seq DESC LIMIT 1 OFFSET ?",
        )
        .bind(&library.0[..])
        .bind(PASSED_KEPT)
        .fetch_optional(&mut *conn)
        .await?;
        if let Some(forgotten) = oldest_kept {
            sqlx::query(
                "UPDATE fongo_client_state SET tracked_from = max(tracked_from, ?2)
                 WHERE library = ?1",
            )
            .bind(&library.0[..])
            .bind(forgotten)
            .execute(&mut *conn)
            .await?;
            sqlx::query("DELETE FROM fongo_client_passed WHERE library = ? AND seq <= ?")
                .bind(&library.0[..])
                .bind(forgotten)
                .execute(&mut *conn)
                .await?;
        }
    }
    Ok(through)
}

/// Whether an envelope is recorded as being sent, by this sync or another.
pub(crate) async fn sending(
    conn: &mut SqliteConnection,
    library: LibraryId,
) -> Result<bool, Error> {
    Ok(sqlx::query_scalar(
        "SELECT sending_envelope IS NOT NULL FROM fongo_client_state WHERE library = ?",
    )
    .bind(&library.0[..])
    .fetch_optional(&mut *conn)
    .await?
    .unwrap_or(false))
}

/// Whether `envelope` is still the one recorded as being sent.
pub(crate) async fn still_sending(
    conn: &mut SqliteConnection,
    library: LibraryId,
    envelope: &[u8],
) -> Result<bool, Error> {
    Ok(
        sqlx::query("SELECT 1 FROM fongo_client_state WHERE library = ? AND sending_envelope = ?")
            .bind(&library.0[..])
            .bind(envelope)
            .fetch_optional(&mut *conn)
            .await?
            .is_some(),
    )
}

/// What the database knows of an envelope this device signed: where a sync appended it, or the
/// outbox position it covers if it is the one recorded as being sent.
pub(crate) enum Known {
    Appended(Vec<u64>),
    Sending(i64),
}

pub(crate) async fn known(
    conn: &mut SqliteConnection,
    library: LibraryId,
    digest: Digest32,
    envelope: &[u8],
) -> Result<Option<Known>, Error> {
    let seqs: Vec<i64> = sqlx::query_scalar(
        "SELECT seq FROM fongo_client_appended WHERE library = ? AND digest = ? ORDER BY seq",
    )
    .bind(&library.0[..])
    .bind(&digest[..])
    .fetch_all(&mut *conn)
    .await?;
    if !seqs.is_empty() {
        let seqs = seqs.into_iter().map(seq_of).collect::<Result<_, _>>()?;
        return Ok(Some(Known::Appended(seqs)));
    }
    let through: Option<i64> = sqlx::query_scalar(
        "SELECT sending_through FROM fongo_client_state WHERE library = ? AND sending_envelope = ?",
    )
    .bind(&library.0[..])
    .bind(envelope)
    .fetch_optional(&mut *conn)
    .await?
    .flatten();
    Ok(through.map(Known::Sending))
}

/// The stored cursor.
pub(crate) async fn stored_cursor(
    conn: &mut SqliteConnection,
    library: LibraryId,
) -> Result<u64, Error> {
    let cursor: i64 = sqlx::query_scalar("SELECT cursor FROM fongo_client_state WHERE library = ?")
        .bind(&library.0[..])
        .fetch_one(&mut *conn)
        .await?;
    u64::try_from(cursor).map_err(|_| corrupt("the cursor is negative"))
}

/// The stored fingerprint and its position.
pub(crate) async fn stored_chain(
    conn: &mut SqliteConnection,
    library: LibraryId,
) -> Result<(u64, Digest32), Error> {
    let row = sqlx::query("SELECT chain_seq, chain FROM fongo_client_state WHERE library = ?")
        .bind(&library.0[..])
        .fetch_one(&mut *conn)
        .await?;
    chain_of(library, row.try_get(0)?, row.try_get(1)?)
}

/// Records `sending` as being sent, unless an envelope already is.
pub(crate) async fn record_sending(
    conn: &mut SqliteConnection,
    library: LibraryId,
    sending: &Sending,
) -> Result<(), Error> {
    let recorded = sqlx::query(
        "UPDATE fongo_client_state
         SET sending_through = ?, sending_envelope = ?, sending_after = ?
         WHERE library = ? AND sending_envelope IS NULL",
    )
    .bind(sending.through)
    .bind(&sending.envelope[..])
    .bind(sending.after.map(as_i64).transpose()?)
    .bind(&library.0[..])
    .execute(&mut *conn)
    .await?
    .rows_affected();
    if recorded != 1 {
        return Err(corrupt("an envelope was already recorded as being sent"));
    }
    Ok(())
}

/// Forgets `envelope` as being sent; another envelope recorded since stays.
pub(crate) async fn clear_sending(
    conn: &mut SqliteConnection,
    library: LibraryId,
    envelope: &[u8],
) -> Result<(), Error> {
    sqlx::query(
        "UPDATE fongo_client_state
         SET sending_through = NULL, sending_envelope = NULL, sending_after = NULL
         WHERE library = ? AND sending_envelope = ?",
    )
    .bind(&library.0[..])
    .bind(envelope)
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Records where the pull stopped at a later version's envelope, unless the stored cursor is
/// already past it, and forgets a recorded stop the stored cursor has passed. Written after the
/// cursor, so that a stop another sync recorded further on stays.
pub(crate) async fn record_stop(
    conn: &mut SqliteConnection,
    library: LibraryId,
    stopped: Option<(u64, Digest32)>,
) -> Result<(), Error> {
    let seq = stopped.map(|(seq, _)| as_i64(seq)).transpose()?;
    sqlx::query(
        "UPDATE fongo_client_state SET
            stopped_seq = CASE WHEN ?1 > cursor THEN ?1
                               WHEN stopped_seq <= cursor THEN NULL ELSE stopped_seq END,
            stopped_digest = CASE WHEN ?1 > cursor THEN ?2
                                  WHEN stopped_seq <= cursor THEN NULL ELSE stopped_digest END
         WHERE library = ?3",
    )
    .bind(seq)
    .bind(stopped.map(|(_, digest)| digest.to_vec()))
    .bind(&library.0[..])
    .execute(&mut *conn)
    .await?;
    Ok(())
}

/// Forgets an envelope to step past once the pull has read through it.
pub(crate) async fn clear_passing(
    conn: &mut SqliteConnection,
    library: LibraryId,
    read: u64,
) -> Result<(), Error> {
    sqlx::query(
        "UPDATE fongo_client_state SET passing_seq = NULL, passing_digest = NULL
         WHERE library = ? AND passing_seq <= ?",
    )
    .bind(&library.0[..])
    .bind(as_i64(read)?)
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
/// first: if it can now read it, because this device has since learned the key of its epoch, it
/// applies it and steps past nothing. Bytes other than those named are never stepped past: should
/// the server hold other bytes there, against the rule that bytes once served never change, a
/// device stopped before them reads them as any envelope, and one that stepped past the damaged
/// bytes, whose cursor they are then, finds [`crate::LogFault::Rewritten`].
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

/// The stored fault, read in one transaction; `None` with no client tables.
pub(crate) async fn read_fault(
    conn: &mut SqliteConnection,
    library: LibraryId,
) -> Result<Option<LogFault>, Error> {
    let mut tx = conn.begin().await?;
    let read = async {
        if !matches!(layout(&mut tx).await?, Layout::Current) {
            return Ok(None);
        }
        let row = sqlx::query(
            "SELECT fault_kind, fault_seq, fault_other FROM fongo_client_state WHERE library = ?",
        )
        .bind(&library.0[..])
        .fetch_optional(&mut *tx)
        .await?;
        match row {
            Some(row) => fault_of(row.try_get(0)?, row.try_get(1)?, row.try_get(2)?),
            None => Ok(None),
        }
    }
    .await;
    tx.rollback().await?;
    read
}

/// The stored fingerprint and its position, read in one transaction; the empty log's with no
/// client tables, and position 0 on tables of an earlier layout, which kept none.
pub(crate) async fn read_chain(
    conn: &mut SqliteConnection,
    library: LibraryId,
) -> Result<(u64, Digest32), Error> {
    let mut tx = conn.begin().await?;
    let read = async {
        if !matches!(layout(&mut tx).await?, Layout::Current) {
            return Ok((0, chain_start(library)));
        }
        let row = sqlx::query("SELECT chain_seq, chain FROM fongo_client_state WHERE library = ?")
            .bind(&library.0[..])
            .fetch_optional(&mut *tx)
            .await?;
        match row {
            Some(row) => chain_of(library, row.try_get(0)?, row.try_get(1)?),
            None => Ok((0, chain_start(library))),
        }
    }
    .await;
    tx.rollback().await?;
    read
}

/// [`crate::resend_all`] once the connection is checked.
pub(crate) async fn resend_all(
    conn: &mut SqliteConnection,
    replica: &fongo_sync_sqlite::Replica,
    library: LibraryId,
) -> Result<usize, Error> {
    if !matches!(layout(conn).await?, Layout::Current) {
        return Err(Error::NoLogFault);
    }
    let mut tx = conn.begin().await?;
    let reset = sqlx::query(
        "UPDATE fongo_client_state SET
            cursor = 0, anchor = NULL, anchor_seq = NULL, chain_seq = 0, chain = NULL,
            sending_through = NULL, sending_envelope = NULL, sending_after = NULL,
            stopped_seq = NULL, stopped_digest = NULL,
            fault_kind = NULL, fault_seq = NULL, fault_other = NULL,
            resets = resets + 1, tracked_from = 0
         WHERE library = ? AND fault_kind IS NOT NULL",
    )
    .bind(&library.0[..])
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if reset != 1 {
        tx.rollback().await?;
        return Err(Error::NoLogFault);
    }
    for table in ["fongo_client_appended", "fongo_client_passed"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE library = ?"))
            .bind(&library.0[..])
            .execute(&mut *tx)
            .await?;
    }
    let sent = replica.resend(&mut tx).await?;
    tx.commit().await?;
    Ok(sent)
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

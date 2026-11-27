//! What a sync concludes when the server's log is not the log it read before, and what the
//! application can do about it.

use fongo_sync_protocol::LibraryId;
use fongo_sync_sqlite::Replica;
use sqlx::SqliteConnection;

use crate::error::Error;
use crate::state;
use crate::sync::check_connection;

/// Proof that the server's log is not the one this device read or wrote: it was rolled back,
/// restored from a backup, rewritten, or lost an envelope it had acknowledged. Each names the
/// first evidence found; a log can be wrong in more ways than the one named.
///
/// A sync that finds one stores it and fails with [`Error::LogFault`]. From then on every sync on
/// the database, on any connection, fails with it before reaching the server, until the
/// application resolves it with [`resend_all`] or rebuilds the device; nothing is applied, sent or
/// acknowledged meanwhile, and [`crate::Apply::caught_up`] is not called, so the device never
/// reports itself settled on such a log. [`fault`] reads it without syncing.
///
/// What this cannot detect: a server that shows this device a log consistent with everything it
/// read and wrote, as after a rollback to a point after the last envelope this device read or
/// appended, or a server that withholds the newest envelopes, which looks like nothing new; and a
/// split, where the server keeps separate logs for different devices and no envelope crosses
/// between them. Devices that compare [`fingerprint`]s at one position, out of band, detect a
/// split.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LogFault {
    /// The log ends before `cursor`, the last position this device read: the server lost
    /// envelopes it had served.
    Behind { cursor: u64 },
    /// The envelope at `seq`, the last position this device read, now has other bytes: history
    /// was rewritten there, as when the log was restored and then refilled by other devices.
    Rewritten { seq: u64 },
    /// Two syncs of this database read the log to `seq` and found different envelopes on the
    /// way: the server served two logs.
    Diverged { seq: u64 },
    /// The server answered an append of this device with position `seq`, at or before `read`,
    /// a position this device had already read or appended at, and `seq` did not hold this
    /// envelope then: a position was used twice.
    Reused { seq: u64, read: u64 },
    /// The envelope this device appended at `seq`, as the server answered, is not there: `seq`
    /// holds other bytes.
    AppendReplaced { seq: u64 },
    /// The log ends before `seq`, where the server placed an envelope this device appended.
    AppendMissing { seq: u64 },
}

/// The fingerprint of the server's log through position `seq`, as this device read it.
///
/// It is defined by the log alone, whatever this device did with each envelope: applied it,
/// refused it, stepped past it or found it to be its own. With `D` = `"fongo-sync/v1/log-chain\0"`
/// and `H` = SHA-256, the fingerprint at 0 is `H(D ‖ 0x00 ‖ library id)` and the fingerprint at
/// `s` is `H(D ‖ 0x01 ‖ fingerprint at s-1 ‖ s as 8 big-endian bytes ‖ H(envelope at s))`, over
/// the envelope's bytes exactly as the server serves them. An envelope the pull stopped at is not
/// counted until the pull reads past it.
///
/// Two devices whose fingerprints at one `seq` are equal read the same log to that point. Devices
/// read the log at different paces, so compare at a common `seq`: a device keeps only its latest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Fingerprint {
    pub seq: u64,
    pub digest: [u8; 32],
}

/// The fault stored for `library`, if any; `None` too where the client's tables do not exist yet.
/// Fails where they are of a later layout than this build's.
pub async fn fault(
    conn: &mut SqliteConnection,
    library: LibraryId,
) -> Result<Option<LogFault>, Error> {
    state::read_fault(conn, library).await
}

/// The fingerprint of `library`'s log through the furthest position this device has hashed:
/// usually its cursor ([`crate::cursor`]). A database whose client tables were made by a build
/// that kept no fingerprint gives position 0 until a sync has read the log again up to its
/// cursor, which the first sync after the upgrade does, as far as its page limit lets it. Fails
/// where the client's tables are of a later layout than this build's.
pub async fn fingerprint(
    conn: &mut SqliteConnection,
    library: LibraryId,
) -> Result<Fingerprint, Error> {
    let (seq, digest) = state::read_chain(conn, library).await?;
    Ok(Fingerprint { seq, digest })
}

/// Resolves a stored [`LogFault`] by sending again everything this device holds, and reading the
/// server's log again from its start. Fails with [`Error::NoLogFault`], changing nothing, when no
/// fault is stored.
///
/// In one transaction: every change the replica keeps, its own and those it merged from other
/// devices, including changes to collections its registry lacks, is put in its outbox, as
/// [`Replica::resend`] does; the cursor goes back to 0; the record of an envelope being sent, of
/// where its appends landed and of where its pull stopped is dropped; and the fault is cleared. The
/// next syncs read the whole log and apply it again, which changes nothing already held, and send
/// the outbox. Once they have sent it, the server's log holds every change this device held: what
/// the server lost from any device is restored as far as this device had it. Applying a change
/// twice changes nothing, so the copies cost only room. Changes not yet collected, and those in
/// the outbox before, are sent as before. An envelope [`crate::step_past`] chose to step past is
/// still stepped past if found at the same position with the same bytes.
///
/// A crash leaves the transaction whole or undone: with the fault still stored, call this again.
/// Two devices that each resolve so both send everything, which costs room and nothing else. A
/// sync running on another connection meanwhile writes nothing more once this commits, and fails
/// with [`Error::ResolvedMeanwhile`]. Should the server roll back again before the outbox is
/// sent, the next sync finds a new fault, and resolving it sends everything again, the earlier
/// copies included.
///
/// What it does not restore: a change only another device held, and the server lost, waits for
/// that device to resolve too; of a field's history only the change that wins on this device is
/// sent, so the earliest change to an entity may be lost, which can change what
/// [`Replica::made`] answers for it elsewhere; and which entities were found to be one is not
/// sent, since the kernel keeps no clock for it, but every device finds it again from the
/// entities' identities.
///
/// The other resolutions: leave the fault stored, and the device stays stopped while local edits
/// gather; or rebuild the device from the server, giving up what it holds that the server lost:
/// [`Replica::uninstall`] (which also drops the client's tables, and the fault with them), delete
/// the rows of the synced tables, [`Replica::install`], and sync. Offer an export first.
pub async fn resend_all(
    conn: &mut SqliteConnection,
    replica: &Replica,
    library: LibraryId,
) -> Result<usize, Error> {
    check_connection(conn).await?;
    state::resend_all(conn, replica, library).await
}

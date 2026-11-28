//! What a sync concludes when the server's log is not the log it read before, and what the
//! application can do about it.

use fongo_sync_protocol::LibraryId;
use fongo_sync_sqlite::Replica;
use sqlx::SqliteConnection;

use crate::error::Error;
use crate::state;
use crate::sync::check_connection;

/// Proof that the server's log is not the one this device read or wrote: it was rolled back,
/// restored from a backup, rewritten at this device's cursor, or lost an envelope it had
/// acknowledged. Each names the first evidence found; a log can be wrong in more ways than the
/// one named.
///
/// A sync that finds one stores it and fails with [`Error::LogFault`]. From then on every sync on
/// the database, on any connection, fails with it before reaching the server, until the
/// application resolves it with [`resend_all`] or rebuilds the device: nothing more is applied,
/// sealed or acknowledged, and [`crate::Apply::caught_up`] is not called, so the device never
/// reports itself settled on such a log. A sync running on another connection when the fault is
/// stored writes nothing more and fails with it too; but an append whose request it had already
/// given to the transport still reaches the server, and is left unacknowledged, its changes in the
/// outbox. [`log_fault`] reads the stored fault without syncing.
///
/// The log is append-only byte for byte: once a server has served bytes at a position, it serves
/// those bytes there for good, as the protocol's rule that every device reads the same envelope at
/// each position requires. A server that finds an envelope damaged leaves it as it served it,
/// since it cannot know that no device has read past it. A device whose pull stopped at the
/// damaged bytes, and so has not passed them, reads other bytes found there later as any
/// envelope; to one that refused or stepped past them, that position is the cursor or behind it,
/// and other bytes at the cursor are [`LogFault::Rewritten`], even when they are the envelope as
/// first written.
///
/// What this cannot detect: a server that shows this device a log consistent with everything it
/// read and wrote, as after a rollback to a point at or after the last envelope this device read
/// or appended; an envelope rewritten behind this device's cursor, which it does not read again; a
/// server that withholds the newest envelopes, which looks like nothing new; and a split, where
/// the server keeps separate logs for different devices and no envelope crosses between them.
/// Devices that compare [`fingerprint`]s at one position, out of band, detect a split, and a
/// rewrite behind their cursors once one of them has read the log anew.
///
/// Most faults are found again from what the device stores, so one found and not stored, as when
/// the process stops or the database fails before the fault is written, is found by the next sync.
/// Two rest on what the server served one sync alone: [`LogFault::Diverged`], stored in the
/// transaction that finds it, and [`LogFault::Reused`], stored once the envelope is read back.
/// One of these lost so is found again only if the server serves the same again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LogFault {
    /// The log ends before `cursor`, the last position this device read: the server lost
    /// envelopes it had served.
    Behind { cursor: u64 },
    /// The envelope at `seq`, this device's cursor, now has other bytes: history was rewritten
    /// there, as when the log was restored and then refilled by other devices.
    Rewritten { seq: u64 },
    /// Two syncs of this database read the log to `seq` and found different envelopes on the
    /// way: the server served two logs.
    Diverged { seq: u64 },
    /// The server answered an append of this device with position `seq`, at or before
    /// `furthest`, the furthest position this device had read or appended at, and the envelope
    /// read back at `seq` was not the one appended: the answer names a position used by another.
    Reused { seq: u64, furthest: u64 },
    /// The envelope this device appended at `seq`, as the server answered, is not there: `seq`
    /// holds other bytes.
    AppendReplaced { seq: u64 },
    /// The log ends before `seq`, where the server placed an envelope this device appended.
    AppendMissing { seq: u64 },
}

impl LogFault {
    /// The position the evidence is at: the cursor for [`LogFault::Behind`], otherwise `seq`.
    pub fn seq(&self) -> u64 {
        match *self {
            LogFault::Behind { cursor } => cursor,
            LogFault::Rewritten { seq }
            | LogFault::Diverged { seq }
            | LogFault::Reused { seq, .. }
            | LogFault::AppendReplaced { seq }
            | LogFault::AppendMissing { seq } => seq,
        }
    }
}

impl std::fmt::Display for LogFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            LogFault::Behind { cursor } => write!(
                f,
                "the server's log ends before position {cursor}, which this device read"
            ),
            LogFault::Rewritten { seq } => write!(
                f,
                "the server's log holds other bytes at position {seq} than this device read there"
            ),
            LogFault::Diverged { seq } => write!(
                f,
                "two syncs of this device read different logs up to position {seq}"
            ),
            LogFault::Reused { seq, furthest } => write!(
                f,
                "the server placed an append at position {seq}, which holds another envelope; \
                 this device had read or appended up to {furthest}"
            ),
            LogFault::AppendReplaced { seq } => write!(
                f,
                "the envelope this device appended at position {seq} has been replaced"
            ),
            LogFault::AppendMissing { seq } => write!(
                f,
                "the server's log ends before position {seq}, where this device appended"
            ),
        }
    }
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
#[non_exhaustive]
pub struct Fingerprint {
    pub seq: u64,
    pub digest: [u8; 32],
}

/// The fault stored for `library`, if any; `None` too where the client's tables do not exist yet.
/// Fails where they are of a later layout than this build's.
pub async fn log_fault(
    conn: &mut SqliteConnection,
    library: LibraryId,
) -> Result<Option<LogFault>, Error> {
    state::read_fault(conn, library).await
}

/// The fingerprint of `library`'s log through the furthest position this device has hashed:
/// usually its cursor ([`crate::cursor`]). On a database whose client tables were made by a build
/// that kept no fingerprint, the syncs after the upgrade hash the log again from its start and
/// store how far they got after each page: until they reach the cursor this gives the position
/// they reached, 0 before the first. Fails where the client's tables are of a later layout than
/// this build's.
///
/// Two devices whose fingerprints at one position differ were shown different logs. Nothing in
/// this crate acts on that: [`resend_all`] needs a fault this device found itself, and no call
/// records one an application found by comparing fingerprints. What an application can do today
/// is tell the user the server is not keeping one log, choose the log to keep, and rebuild each
/// device that followed another, after an export, as [`resend_all`] describes; what only those
/// devices held is lost unless entered again.
pub async fn fingerprint(
    conn: &mut SqliteConnection,
    library: LibraryId,
) -> Result<Fingerprint, Error> {
    let (seq, digest) = state::read_chain(conn, library).await?;
    Ok(Fingerprint { seq, digest })
}

/// Resolves a stored [`LogFault`] by sending again everything this device holds, and reading the
/// server's log again from its start. Returns how many changes it put in the outbox, which the
/// next syncs send besides what the outbox held already. Fails with [`Error::NoLogFault`],
/// changing nothing, when no fault is stored.
///
/// In one transaction: every change the replica keeps, its own and those it merged from other
/// devices, including changes to collections its registry lacks and which entities are one, is
/// put in its outbox, as [`Replica::resend`] does, an alias at a clock on a node no device owns,
/// derived from the alias, which every device takes as it takes any other; the cursor goes back
/// to 0; the record of an envelope being sent, of where its appends landed and of where its pull
/// stopped is dropped; and the fault is cleared. The
/// next syncs read the whole log and apply it again, which changes nothing already held, and send
/// the outbox. Once they have sent it, the server's log holds every change this device held: what
/// the server lost from any device is restored as far as this device had it. Applying a change
/// twice changes nothing, so the copies cost only room. Changes not yet collected, and those in
/// the outbox before, are sent as before. An envelope [`crate::step_past`] chose to step past is
/// still stepped past if found at the same position with the same bytes.
///
/// A crash leaves the transaction whole or undone: with the fault still stored, call this again.
/// Two devices that each resolve so both send everything, which costs room on the server. Near the
/// library's storage limit ([`fongo_sync_protocol::api::Capabilities::max_library_bytes`]) the
/// server refuses part of it: each sync then reads the log and fails with [`Error::Server`]
/// carrying [`fongo_sync_protocol::api::ErrorCode::LibraryFull`], the fault stays resolved, and
/// nothing is lost on this device, as what was not sent waits in the outbox. The application can
/// tell the user the library is full, so that room is made on the server, and sync again; or
/// rebuild the device instead, which gives up what only it held. A sync running on another
/// connection meanwhile writes nothing more once this commits, and fails with
/// [`Error::ResolvedMeanwhile`]. Should the server roll back again before the outbox is sent, the
/// next sync finds a new fault, and resolving it sends everything again, the earlier copies
/// included.
///
/// A device that then takes the log, of this version or an older one later upgraded to it, shows
/// the rows this device shows, with what follows from the kernel's own record, as
/// [`Replica::resend`] states: of a field's history only the change that wins is sent, which
/// changes nothing a device shows but which collection an id with facts in more than one belongs
/// to; values the application kept as this device's own on resuming, and rows held back for a
/// clash, stay this device's alone. A change only another device held, and the server lost, waits
/// for that device to resolve too. Reading the log again reports no conflict this device was told
/// of before, nor one with an edit it has not sent.
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

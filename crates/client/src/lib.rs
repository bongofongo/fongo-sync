//! A device's side of a synced library: creating and joining a library, and [`sync`], one round
//! of reading what other devices sent, applying it, and sending this device's own changes.
//! [`sync_with`] lets the application do work of its own in the transaction of each apply, and
//! once a pull has read the whole log.
//!
//! What a sync cannot read it skips only when it is damaged or forged. An envelope a later version
//! of the protocol may have written stops the pull where it is, and [`SyncReport::newer`] says so,
//! so that the application can ask for an update; a build that reads it continues from there.
//! [`step_past`] gives up one such envelope, for one that no build will read.
//!
//! The crate picks no HTTP client and no async runtime. An application implements [`Transport`]
//! over whatever HTTP client it uses; every request and response is plain bytes and headers, and
//! every future this crate returns is `Send`.
//!
//! Secrets live in [`Credentials`], which the application keeps wherever it keeps secrets and
//! passes in on each call. This crate never writes them to disk or to the database, and never
//! logs them. What it does keep in the database, in its own `fongo_client_state`,
//! `fongo_client_appended`, `fongo_client_passed` and `fongo_client_meta` tables, is the position
//! in the server's log with the digest of the envelope there and the log's [`Fingerprint`], an
//! envelope waiting to be sent, which is already encrypted, where the envelopes it appended landed
//! in the log and digests of them, where its pull stopped at an envelope it cannot read, a
//! [`LogFault`] found in the log, and the layout of those tables. A build refuses tables of a later
//! layout with [`Error::NewerClientTables`].
//!
//! # A log that is not the one read before
//!
//! The server's log only grows, and a device keeps what it read of it: so each sync first reads
//! again the envelope at its cursor and compares it with the digest it kept, checks that each
//! envelope it appended is where the server placed it, and that the server places a new append
//! after everything it has read. A server restored from a backup, or one that rewrote or dropped
//! envelopes, fails one of these, and the sync stops with [`Error::LogFault`] before applying,
//! sending or settling anything more, as does every sync after it until the application resolves
//! the fault, with [`resend_all`] or by rebuilding the device. Reading the envelope at the cursor
//! costs that envelope's bytes in the first page of each sync, and no request more.
//!
//! What cannot be detected this way: a rollback to a point after the last envelope this device read
//! or appended, which leaves it nothing to miss; a server withholding the newest envelopes, which
//! looks like nothing new; a server keeping separate logs for different devices, where no envelope
//! crosses between them; and, on a device that never read the lost part, the loss itself, until a
//! device that read it syncs and finds the fault. Two devices comparing their [`fingerprint`]s at
//! one position out of band find a split.
//!
//! To create or join a library so that a lost response can be retried, make the credentials
//! first ([`Credentials::for_new_library`], [`Credentials::for_recovery_code`]), store them, then
//! register them with [`create_library_with`] or [`join_library_with`], repeating the call until
//! it gives an answer. Each registers the device's wrap key and a membership proof, which another
//! device checks with [`Credentials::check_member`]. [`create_library`] and [`join_library`] do
//! both steps in one call and return the credentials, which a lost response loses.
//! A device can also be added by another: the new device makes its credentials from the library's
//! keys with [`Credentials::for_new_device`], stores them and hands over
//! [`Credentials::device_offer`], which the other passes to [`add_device`].
//!
//! Native targets only: keys and nonces come from the operating system.
//!
//! # Shaped by the kernel as it stands
//!
//! The outbox reports one position for a whole page of changes, so an envelope smaller than a page
//! is cut by reading the outbox again with a smaller limit; with a position per change it can be
//! cut from one read. The kernel keeps the base of each local edit until told the edit has been
//! read back from the log, after which a change it meets may have been made knowing the edit; so
//! the client records where each envelope it appends lands, and releases the bases of its edits
//! once a pull has applied everything before that point. Envelopes carry no record of what their
//! author had seen, so of two concurrent edits only the device whose envelope lands later in the
//! log is told of the conflict; the other takes the winning value without a report.

mod credentials;
mod error;
mod fault;
mod library;
mod notify;
mod rpc;
mod state;
mod sync;
mod transport;

pub use credentials::{
    CREDENTIALS_FORMAT, CREDENTIALS_FORMAT_WITHOUT_WRAP, Credentials, CredentialsError,
};
pub use error::Error;
pub use fault::{Fingerprint, LogFault, fault, fingerprint, resend_all};
pub use fongo_sync_protocol as protocol;
pub use fongo_sync_sqlite as sqlite;
pub use library::{
    DeviceOffer, Registered, WrapListing, add_device, create_library, create_library_with,
    join_library, join_library_with,
};
pub use notify::{message_worth_syncing, worth_syncing};
pub use state::{cursor, step_past};
pub use sync::{
    Apply, Blocked, Newer, Pace, Refusal, Refused, SyncReport, Unknown, check_connection,
    merge_applied, sync, sync_with,
};
pub use transport::{Method, Request, Response, Transport, TransportError};

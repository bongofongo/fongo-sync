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
//! `fongo_client_appended` and `fongo_client_meta` tables, is the position in the server's log, an
//! envelope waiting to be sent, which is already encrypted, where the envelopes it appended landed
//! in the log, where its pull stopped at an envelope it cannot read, and the layout of those
//! tables. A build refuses tables of a later layout with [`Error::NewerClientTables`].
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
pub use fongo_sync_protocol as protocol;
pub use fongo_sync_sqlite as sqlite;
pub use library::{
    DeviceOffer, add_device, create_library, create_library_with, join_library, join_library_with,
};
pub use notify::{message_worth_syncing, worth_syncing};
pub use state::{cursor, step_past};
pub use sync::{
    Apply, Blocked, Newer, Pace, Refusal, Refused, SyncReport, Unknown, check_connection,
    merge_applied, sync, sync_with,
};
pub use transport::{Method, Request, Response, Transport, TransportError};

//! A device's side of a synced library: creating and joining a library, and [`sync`], one round
//! of reading what other devices sent, applying it, and sending this device's own changes.
//! [`sync_with`] lets the application do work of its own in the transaction of each apply, and
//! once a pull has read the whole log.
//!
//! The crate picks no HTTP client and no async runtime. An application implements [`Transport`]
//! over whatever HTTP client it uses; every request and response is plain bytes and headers, and
//! every future this crate returns is `Send`.
//!
//! Secrets live in [`Credentials`], which the application keeps wherever it keeps secrets and
//! passes in on each call. This crate never writes them to disk or to the database, and never
//! logs them. What it does keep in the database, in its own `fongo_client_state` and
//! `fongo_client_appended` tables, is the position in the server's log, an envelope waiting to be
//! sent, which is already encrypted, and where the envelopes it appended landed in the log.
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

pub use credentials::{CREDENTIALS_FORMAT, Credentials, CredentialsError};
pub use error::Error;
pub use fongo_sync_protocol as protocol;
pub use fongo_sync_sqlite as sqlite;
pub use library::{add_device, create_library, join_library};
pub use notify::{message_worth_syncing, worth_syncing};
pub use state::cursor;
pub use sync::{Apply, Blocked, Refusal, Refused, SyncReport, merge_applied, sync, sync_with};
pub use transport::{Method, Request, Response, Transport, TransportError};

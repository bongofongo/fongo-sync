//! A device's side of a synced library: creating and joining a library, and [`sync`], one round
//! of reading what other devices sent, applying it, and sending this device's own changes.
//!
//! The crate picks no HTTP client and no async runtime. An application implements [`Transport`]
//! over whatever HTTP client it uses; every request and response is plain bytes and headers, and
//! every future this crate returns is `Send`.
//!
//! Secrets live in [`Credentials`], which the application keeps wherever it keeps secrets and
//! passes in on each call. This crate never writes them to disk or to the database, and never
//! logs them. What it does keep in the database, in its own `fongo_client_state` table, is the
//! position in the server's log and an envelope waiting to be sent, which is already encrypted.
//!
//! Native targets only: keys and nonces come from the operating system.
//!
//! # Shaped by the kernel as it stands
//!
//! The outbox reports one position for a whole page of changes, so an envelope smaller than a page
//! is cut by reading the outbox again with a smaller limit; with a position per change it can be
//! cut from one read. And a conflict is found only between a local edit and the remote changes
//! given to the same apply, so received changes are held across pages and applied together; once
//! the kernel keeps a local edit's base across applies, each page can be applied as it arrives.

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
pub use sync::{Blocked, Refusal, Refused, SyncReport, sync};
pub use transport::{Method, Request, Response, Transport, TransportError};

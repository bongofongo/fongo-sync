//! fongo-sync over SQLite. Triggers notice an application's writes, [`Replica::collect`] turns
//! them into field-level changes, and [`Replica::apply`] merges changes from other replicas back
//! into the application's own tables. The application's schema is left as it is.
//!
//! Every decision is the core's (`fongo_sync_core::eval` and `plan`); this crate keeps the facts,
//! the rows' images and the mapping from local rows to entities, and does what the core says.
//!
//! A value a column's type affinity converts, such as the text `"1872"` written to an `INTEGER`
//! column, is shown as SQLite stores it (the integer 1872) while the merged state keeps the value
//! as it arrived. The conversion is not an edit: it is not sent back.
//!
//! Each call works on the connection's own SQLite handle, taken with sqlx's
//! `SqliteConnection::lock_handle` straight after a `ping`. sqlx 0.8 lends the handle by sending
//! the connection's worker thread an unlock command. When one `lock_handle` follows another with
//! no other command between them, it can find the lock free before the worker has taken it back,
//! and its unlock command is then left in the worker's queue; enough of those deadlock the
//! connection. The ping makes every earlier command run first. An application that also takes
//! the handle itself, on a connection it passes to the kernel, should do the same: send any
//! command it waits on, such as `ping` or a query, before each `lock_handle`.

mod collect;
mod db;
mod error;
mod image;
mod reconcile;
mod registry;
mod replica;
mod store;
mod tables;

pub use error::{Error, Result};
pub use fongo_sync_core as core;
pub use registry::{Collection, Field, Key, Kind, LocalDefault, Registry};
pub use replica::{
    Applied, COLLECT_ROWS, Collected, Conflict, Held, Inspection, Made, Outbox, Replica, Resume,
    Touched,
};

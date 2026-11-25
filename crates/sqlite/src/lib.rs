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
//!
//! # What is supported and tested
//!
//! Collecting and installing in bounded steps are held to their documented bar and bounds in
//! the shape of registry and schema tested most: foreign keys enforced, minted collections with
//! owners, references, sets written together, `Max` and last-writer-wins fields and local
//! dependents, natural collections owned by minted ones, and identities each enforced by a
//! unique index. Outside it, ignored tests record what is known not to hold: with foreign keys
//! off, a reference to an id taken again after its row was collected gone can be lost (as with
//! ordinary collects), and natural rows naming a row that is gone make steps cost many times one
//! collect; many rows that cannot be collected yet, among others, make every step read them
//! again; and with an identity no index enforces, a first install in steps can send changes that
//! differ from a whole install's where it gives up a row found to be one with another whose id
//! was taken again before it was adopted.

mod adopt;
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

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
//! # Entry points
//!
//! Every call that reads or writes the database first reads, in one indexed statement inside
//! the transaction or savepoint it works in, whether the kernel's tables are there, of which
//! layout, and whether capture is suspended; what it refuses, it refuses before changing
//! anything.
//!
//! - Tables of a later layout than this build's are refused by every call
//!   ([`Error::NewerLayout`]).
//! - Tables of layout 3 are upgraded by `install`, `install_step` and `resume`, and removed by
//!   `uninstall`; every other call refuses them ([`Error::OlderLayout`]).
//! - Tables of any other layout, or that record none, are removed by `uninstall` and refused by
//!   every other call ([`Error::UnknownLayout`]).
//! - Kernel tables some of which are missing, `fongo_meta` among them, are removed by
//!   `uninstall` and refused by every other call ([`Error::Corrupt`]).
//! - With no kernel tables, `install`, `install_step` and `resume` install, `suspend` and
//!   `uninstall` do nothing, `uncollected_rows` answers 0 and [`installed`] false; every other
//!   call refuses ([`Error::NotInstalled`]). The client's own `fongo_client_` tables are not
//!   the kernel's.
//! - While capture is suspended (a suspend is recorded, or a collection that was installed
//!   lacks a trigger; see [`Replica::resume`]), `install`, `install_step`, `collect`,
//!   `collect_step`, `apply` and `uncollected_rows` refuse ([`Error::Suspended`]); the reads,
//!   `acknowledge` and `release_bases` answer as at any other time.
//!
//! Every call that writes, called with no transaction open, takes SQLite's write lock with its
//! first statement, so while another connection writes it waits in the busy handler. Called
//! inside the caller's transaction, it works in a savepoint of it and cannot take the lock
//! itself: the caller begins that transaction with `BEGIN IMMEDIATE`, since a deferred one that
//! has read fails with `SQLITE_BUSY` at its first write if another connection wrote meanwhile.
//! Every read runs in a savepoint, so it answers from one state of the database; in
//! rollback-journal mode, another connection's commit waits for it while it lasts.
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
    Touched, installed,
};

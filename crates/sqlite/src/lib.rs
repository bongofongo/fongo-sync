//! fongo-sync over SQLite. Triggers notice an application's writes, [`Replica::collect`] turns
//! them into field-level changes, and [`Replica::apply`] merges changes from other replicas back
//! into the application's own tables. The application's schema is left as it is.
//!
//! Every decision is the core's (`fongo_sync_core::eval` and `plan`); this crate keeps the facts,
//! the rows' images and the mapping from local rows to entities, and does what the core says.

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
pub use replica::{Applied, Conflict, Held, Inspection, Outbox, Replica, Resume, Touched};

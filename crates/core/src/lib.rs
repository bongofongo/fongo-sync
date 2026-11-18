//! The storage-free heart of fongo-sync: identities, clocks, change records and the rules by which
//! concurrent changes merge. It performs no I/O and knows nothing about any application.
//!
//! [`model`] states what replicas must show. [`source`] says how merged changes are kept,
//! [`eval`] computes what one entity should look like from them, and [`plan`] turns that into
//! row writes and turns row edits into changes. A storage adapter keeps facts and rows and asks
//! these modules every question whose answer is a decision.

mod affected;
mod change;
mod clock;
pub mod eval;
pub mod model;
pub mod plan;
pub mod source;
mod total;
mod value;

pub use change::{Change, DELETED, State, Strategy, wins};
pub use clock::{Clock, Hlc};
pub use value::{Gid, Value};

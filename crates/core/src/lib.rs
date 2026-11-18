//! The storage-free heart of fongo-sync: identities, clocks, change records and the rules by which
//! concurrent changes merge. It performs no I/O and knows nothing about any application.

mod change;
mod clock;
mod value;

pub use change::{Change, DELETED, State, Strategy, wins};
pub use clock::{Clock, Hlc};
pub use value::{Gid, Value};

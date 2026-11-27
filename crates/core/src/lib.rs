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
#[cfg(feature = "work-counters")]
pub mod work;
#[cfg(not(feature = "work-counters"))]
mod work;

pub use change::{ALIAS, Change, DELETED, State, Strategy, wins};
pub use clock::{Clock, Hlc};
pub use value::{Gid, Value};

/// Settings for this crate's property tests: proptest's own, with the cases drawn from one fixed
/// seed unless `PROPTEST_RNG_SEED` names another, and no failures kept in files between runs.
#[cfg(test)]
fn proptest_config() -> proptest::test_runner::Config {
    let config = proptest::test_runner::Config::default();
    let rng_seed = match std::env::var("PROPTEST_RNG_SEED") {
        Ok(_) => config.rng_seed,
        Err(_) => proptest::test_runner::RngSeed::Fixed(29),
    };
    proptest::test_runner::Config {
        rng_seed,
        failure_persistence: None,
        ..config
    }
}

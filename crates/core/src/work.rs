//! How much work the evaluator, `affected` and the planner have done on this thread: gids
//! fetched into regions, groups joined, keys filed and looked up, members read, gids queued for
//! comparison, and rows, requirements and parkings looked at. Counted only with the
//! `work-counters` feature, for tests that check how work grows.

#[cfg(feature = "work-counters")]
mod counter {
    use std::cell::Cell;

    thread_local! {
        static DONE: Cell<usize> = const { Cell::new(0) };
    }

    /// The work counted since the last call, which resets it.
    pub fn take() -> usize {
        DONE.with(|done| done.replace(0))
    }

    pub(crate) fn add(n: usize) {
        DONE.with(|done| done.set(done.get() + n));
    }
}

#[cfg(feature = "work-counters")]
pub use counter::take;

#[cfg(feature = "work-counters")]
pub(crate) use counter::add;

#[cfg(not(feature = "work-counters"))]
pub(crate) fn add(_: usize) {}

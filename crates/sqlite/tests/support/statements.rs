//! Counts the statements SQLite starts on a connection, and can interrupt the one with a given
//! number, as a crash at that point would end it.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::CStr;
use std::ffi::{c_int, c_uint, c_void};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use libsqlite3_sys as ffi;
use sqlx::SqliteConnection;

pub struct Statements {
    state: Arc<State>,
}

struct State {
    db: *mut ffi::sqlite3,
    count: AtomicU64,
    /// The statement to interrupt, counting from one; zero for none.
    stop_at: AtomicU64,
    /// The text of every statement started, and how many times it was.
    texts: Mutex<BTreeMap<String, u64>>,
}

// SAFETY: the state is only read by the trace callback, on the connection's own thread, and
// `sqlite3_interrupt` may be called from any thread.
unsafe impl Send for State {}
unsafe impl Sync for State {}

unsafe extern "C" fn traced(
    _: c_uint,
    ctx: *mut c_void,
    stmt: *mut c_void,
    _: *mut c_void,
) -> c_int {
    // SAFETY: `ctx` is the state `watch` leaked for the life of the connection, and `stmt` the
    // statement starting, whose text lives as long as it does.
    let state = unsafe { &*ctx.cast::<State>() };
    let text = unsafe { ffi::sqlite3_sql(stmt.cast()) };
    if !text.is_null() {
        let text = unsafe { CStr::from_ptr(text) }
            .to_string_lossy()
            .into_owned();
        *state.texts.lock().unwrap().entry(text).or_default() += 1;
    }
    let n = state.count.fetch_add(1, Ordering::Relaxed) + 1;
    if n == state.stop_at.load(Ordering::Relaxed) {
        // SAFETY: the handle is open; interrupting only sets a flag the running statement reads.
        unsafe { ffi::sqlite3_interrupt(state.db) };
    }
    0
}

impl Statements {
    /// Starts counting on `conn`.
    pub async fn watch(conn: &mut SqliteConnection) -> Statements {
        let mut handle = conn.lock_handle().await.unwrap();
        let db = handle.as_raw_handle().as_ptr();
        let state = Arc::new(State {
            db,
            count: AtomicU64::new(0),
            stop_at: AtomicU64::new(0),
            texts: Mutex::new(BTreeMap::new()),
        });
        let ctx = Arc::into_raw(state.clone()) as *mut c_void;
        // SAFETY: the handle is locked; the state outlives the connection, being leaked here.
        let status = unsafe {
            ffi::sqlite3_trace_v2(db, ffi::SQLITE_TRACE_STMT as c_uint, Some(traced), ctx)
        };
        assert_eq!(status, ffi::SQLITE_OK);
        Statements { state }
    }

    /// Statements started since counting began, or since the last reset.
    pub fn count(&self) -> u64 {
        self.state.count.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.state.count.store(0, Ordering::Relaxed);
        self.state.texts.lock().unwrap().clear();
    }

    /// The text of every statement started since counting began, or since the last reset.
    pub fn texts(&self) -> BTreeSet<String> {
        self.state.texts.lock().unwrap().keys().cloned().collect()
    }

    /// The statements started since counting began, or since the last reset, the most frequent
    /// first: a cost test's message, saying where the statements went.
    pub fn busiest(&self, n: usize) -> String {
        let mut counted: Vec<(u64, String)> = self
            .state
            .texts
            .lock()
            .unwrap()
            .iter()
            .map(|(text, k)| (*k, text.split_whitespace().collect::<Vec<_>>().join(" ")))
            .collect();
        counted.sort_by(|a, b| b.cmp(a));
        counted
            .iter()
            .take(n)
            .map(|(k, text)| format!("{k:>8}  {text}\n"))
            .collect()
    }

    /// Interrupts the `n`th statement from now; zero interrupts none.
    pub fn stop_at(&self, n: u64) {
        self.reset();
        self.state.stop_at.store(n, Ordering::Relaxed);
    }
}

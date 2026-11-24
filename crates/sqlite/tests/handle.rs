//! The kernel's calls stay live however the connection's worker thread is scheduled.
//!
//! sqlx lends out a connection's SQLite handle by sending its worker thread an unlock command:
//! the worker gives up the lock it holds on the handle, then waits to take it back. A caller that
//! takes the handle, uses it and lets it go before the worker is back in line finds the lock free
//! the next time, takes it at once, and leaves the unlock command it sent unanswered. Each such
//! command is answered later by the worker letting go and taking the lock again, which reopens
//! the same gap. Left-over commands fill the worker's queue, and once it is full a caller holds
//! the lock while it waits for room in the queue, and the worker waits for the lock: neither
//! moves again.
//!
//! The gap needs the worker thread to be descheduled at one exact point, which load makes rare
//! and an idle machine makes rarer still. The executor here makes it certain: a waker called on
//! another thread does not return until the woken task has run to its next pause, as if that
//! thread had been descheduled the moment it woke the task.

use std::future::Future;
use std::pin::pin;
use std::str::FromStr;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};

use fongo_sync_sqlite::{Collection, Registry, Replica};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};

/// More calls than sqlx's default command queue holds.
const CALLS: usize = 1_000;
const LIMIT: Duration = Duration::from_secs(5);

struct Slow {
    poller: ThreadId,
    state: Mutex<State>,
    changed: Condvar,
}

#[derive(Default)]
struct State {
    woken: bool,
    polls: u64,
}

impl Wake for Slow {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let mut state = self.state.lock().unwrap();
        state.woken = true;
        self.changed.notify_all();
        if thread::current().id() == self.poller {
            return;
        }
        let seen = state.polls;
        let until = Instant::now() + Duration::from_secs(1);
        while state.polls == seen {
            let left = until.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return;
            }
            state = self.changed.wait_timeout(state, left).unwrap().0;
        }
    }
}

/// Runs `future` on this thread, holding every other thread that wakes it until it has been
/// polled. `None` if it has not finished within `limit`.
fn run_slowly<F: Future>(future: F, limit: Duration) -> Option<F::Output> {
    let slow = Arc::new(Slow {
        poller: thread::current().id(),
        state: Mutex::new(State::default()),
        changed: Condvar::new(),
    });
    let waker = Waker::from(slow.clone());
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    let deadline = Instant::now() + limit;
    loop {
        slow.state.lock().unwrap().woken = false;
        let poll = future.as_mut().poll(&mut cx);
        let mut state = slow.state.lock().unwrap();
        state.polls += 1;
        slow.changed.notify_all();
        if let Poll::Ready(out) = poll {
            return Some(out);
        }
        while !state.woken {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            state = slow.changed.wait_timeout(state, left).unwrap().0;
        }
    }
}

async fn library(replica: &Replica) -> SqliteConnection {
    let mut conn = SqliteConnectOptions::from_str("sqlite::memory:")
        .unwrap()
        .connect()
        .await
        .unwrap();
    for sql in [
        "CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT)",
        "INSERT INTO books (title) VALUES ('Middlemarch')",
    ] {
        sqlx::query(sql).execute(&mut conn).await.unwrap();
    }
    replica.install(&mut conn).await.unwrap();
    replica.collect(&mut conn).await.unwrap();
    conn
}

#[test]
fn back_to_back_calls_finish_when_the_worker_is_slow_to_take_its_lock_back() {
    let replica = Replica::new(
        Registry::new(vec![
            Collection::minted("books", "books", "id").field("title"),
        ])
        .unwrap(),
    );
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut conn = runtime.block_on(library(&replica));
    let started = Instant::now();
    let found = run_slowly(
        async {
            let mut found = 0;
            for _ in 0..CALLS {
                if replica
                    .gid_for_key(&mut conn, "books", "[1]")
                    .await
                    .unwrap()
                    .is_some()
                {
                    found += 1;
                }
            }
            found
        },
        LIMIT,
    );
    assert_eq!(
        found,
        Some(CALLS),
        "{CALLS} lookups did not finish within {LIMIT:?}"
    );
    eprintln!("{CALLS} lookups in {:?}", started.elapsed());
}

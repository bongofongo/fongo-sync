//! Databases whose randomness and wall clock come from one seed, so a failing run can be repeated
//! exactly. The kernel mints node ids and gids with `randomblob`, stands in for identities with
//! `random`, and stamps writes with `unixepoch`; each connection here replaces those three
//! built-ins with functions of the seed. Every connection of one world shares a clock that only
//! moves forward, sometimes not at all between two readings, as a real one does.

use std::ffi::{CString, c_int, c_void};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use libsqlite3_sys as ffi;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, SqliteConnection};

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

struct Wall {
    ms: u64,
    rng: u64,
    /// The clock moves on one reading in this many, on average; 0 for three in four.
    every: u64,
    /// The clock does not move by itself.
    still: bool,
    /// On one reading in sixteen, the clock steps back this many milliseconds; 0 never.
    back: u64,
    back_rng: u64,
}

struct Source {
    rng: Arc<Mutex<u64>>,
    wall: Arc<Mutex<Wall>>,
}

/// The state of each function's stream on a connection, by connection number.
type Streams = Arc<Mutex<std::collections::BTreeMap<u64, Vec<Arc<Mutex<u64>>>>>>;

#[derive(Clone)]
pub struct World {
    seed: u64,
    wall: Arc<Mutex<Wall>>,
    streams: Streams,
}

impl World {
    pub fn new(seed: u64) -> World {
        World::clustered(seed, 0)
    }

    /// A world whose clock moves on, on average, one reading in `every`, so that many writes
    /// share a millisecond; 0 moves it three readings in four, as [`World::new`] does.
    pub fn clustered(seed: u64, every: u64) -> World {
        World::stepping_back(seed, every, 0)
    }

    /// [`World::clustered`] whose clock also steps back `back` milliseconds on one reading in
    /// sixteen, as a wall clock set back does; 0 never.
    pub fn stepping_back(seed: u64, every: u64, back: u64) -> World {
        World {
            seed,
            wall: Arc::new(Mutex::new(Wall {
                ms: 1_700_000_000_000,
                rng: if every == 0 { seed ^ 0x5555 } else { seed | 1 },
                every,
                still: false,
                back,
                back_rng: seed ^ 0xB4CC,
            })),
            streams: Streams::default(),
        }
    }

    /// A copy of database `n`, on a connection whose randomness and wall clock go on from where
    /// `n`'s stand, in a world of its own: what is done to the copy is what would have been done
    /// to `n`, and touches neither `n` nor this world's clock.
    pub async fn twin(&self, n: u64, conn: &mut SqliteConnection) -> (World, SqliteConnection) {
        let wall = {
            let wall = self.wall.lock().unwrap();
            Wall {
                ms: wall.ms,
                rng: wall.rng,
                every: wall.every,
                still: wall.still,
                back: wall.back,
                back_rng: wall.back_rng,
            }
        };
        let states: Vec<u64> = self.streams.lock().unwrap()[&n]
            .iter()
            .map(|s| *s.lock().unwrap())
            .collect();
        let world = World {
            seed: self.seed,
            wall: Arc::new(Mutex::new(wall)),
            streams: Streams::default(),
        };
        let mut copy = world.open(n, Some(states)).await;
        let mut from = super::lock(conn).await;
        let mut to = super::lock(&mut copy).await;
        let (src, dest) = (from.as_raw_handle().as_ptr(), to.as_raw_handle().as_ptr());
        let main = CString::new("main").unwrap();
        // SAFETY: both handles are open and locked for the copy.
        unsafe {
            let backup = ffi::sqlite3_backup_init(dest, main.as_ptr(), src, main.as_ptr());
            assert!(!backup.is_null());
            assert_eq!(ffi::sqlite3_backup_step(backup, -1), ffi::SQLITE_DONE);
            assert_eq!(ffi::sqlite3_backup_finish(backup), ffi::SQLITE_OK);
        }
        drop((from, to));
        (world, copy)
    }

    /// Stops the clock moving by itself, so that every write until it starts again shares one
    /// millisecond, or starts it again.
    pub fn hold(&self, still: bool) {
        self.wall.lock().unwrap().still = still;
    }

    /// Moves the shared clock on by `ms`.
    pub fn later(&self, ms: u64) {
        self.wall.lock().unwrap().ms += ms;
    }

    /// Moves the shared clock back by `ms`, as a clock set back does.
    pub fn earlier(&self, ms: u64) {
        self.wall.lock().unwrap().ms -= ms;
    }

    /// An empty database, numbered `n` within this world, with foreign keys enforced.
    pub async fn connect(&self, n: u64) -> SqliteConnection {
        self.open(n, None).await
    }

    async fn open(&self, n: u64, states: Option<Vec<u64>>) -> SqliteConnection {
        let mut conn = SqliteConnectOptions::from_str("sqlite::memory:")
            .unwrap()
            .foreign_keys(true)
            .connect()
            .await
            .unwrap();
        // Streams that start at mixed states, not at neighbouring ones, never overlap in practice.
        let mut rng = self.seed ^ n.wrapping_mul(0xD1B5_4A32_D192_ED03);
        let rng = splitmix(&mut rng);
        let mut handle = super::lock(&mut conn).await;
        let db = handle.as_raw_handle().as_ptr();
        let mut streams = Vec::new();
        for (i, (name, args, call)) in [
            ("randomblob", 1, randomblob as Call),
            ("random", 0, random as Call),
            ("unixepoch", 1, unixepoch as Call),
        ]
        .into_iter()
        .enumerate()
        {
            let mut start = rng ^ (name.len() as u64).wrapping_mul(0x8CB9_2BA7_2F3D_8DD7);
            let state = match &states {
                Some(states) => states[i],
                None => splitmix(&mut start),
            };
            let stream = Arc::new(Mutex::new(state));
            streams.push(stream.clone());
            let source = Box::new(Source {
                rng: stream,
                wall: self.wall.clone(),
            });
            let name = CString::new(name).unwrap();
            // SAFETY: `db` is the open connection's handle, locked for this call; the source is
            // owned by SQLite from here and freed by `drop_source`.
            let status = unsafe {
                ffi::sqlite3_create_function_v2(
                    db,
                    name.as_ptr(),
                    args,
                    ffi::SQLITE_UTF8 | ffi::SQLITE_INNOCUOUS,
                    Box::into_raw(source).cast::<c_void>(),
                    Some(call),
                    None,
                    None,
                    Some(drop_source),
                )
            };
            assert_eq!(status, ffi::SQLITE_OK);
        }
        drop(handle);
        self.streams.lock().unwrap().insert(n, streams);
        conn
    }
}

type Call = unsafe extern "C" fn(*mut ffi::sqlite3_context, c_int, *mut *mut ffi::sqlite3_value);

unsafe extern "C" fn drop_source(source: *mut c_void) {
    // SAFETY: SQLite passes back the pointer `connect` gave it, once.
    drop(unsafe { Box::from_raw(source.cast::<Source>()) });
}

/// # Safety
/// Called by SQLite with a context whose user data is a `Source`.
unsafe fn source<'a>(ctx: *mut ffi::sqlite3_context) -> &'a Source {
    unsafe { &*ffi::sqlite3_user_data(ctx).cast::<Source>() }
}

unsafe extern "C" fn randomblob(
    ctx: *mut ffi::sqlite3_context,
    _: c_int,
    argv: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: SQLite calls this with one argument, as registered.
    let n = unsafe { ffi::sqlite3_value_int64(*argv) }.clamp(1, 1 << 20) as usize;
    let source = unsafe { source(ctx) };
    let mut rng = source.rng.lock().unwrap();
    let bytes: Vec<u8> = (0..n.div_ceil(8))
        .flat_map(|_| splitmix(&mut rng).to_le_bytes())
        .take(n)
        .collect();
    // SAFETY: SQLite copies the bytes before this returns.
    unsafe {
        ffi::sqlite3_result_blob(
            ctx,
            bytes.as_ptr().cast::<c_void>(),
            n as c_int,
            ffi::SQLITE_TRANSIENT(),
        );
    }
}

unsafe extern "C" fn random(
    ctx: *mut ffi::sqlite3_context,
    _: c_int,
    _: *mut *mut ffi::sqlite3_value,
) {
    let source = unsafe { source(ctx) };
    let value = splitmix(&mut source.rng.lock().unwrap()) as i64;
    unsafe { ffi::sqlite3_result_int64(ctx, value) };
}

/// Seconds, as `unixepoch('subsec')` gives them, read off the shared clock.
unsafe extern "C" fn unixepoch(
    ctx: *mut ffi::sqlite3_context,
    _: c_int,
    _: *mut *mut ffi::sqlite3_value,
) {
    let source = unsafe { source(ctx) };
    let mut wall = source.wall.lock().unwrap();
    let moves = match wall.every {
        0 => !splitmix(&mut wall.rng).is_multiple_of(4),
        every => {
            let mut x = wall.rng;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            wall.rng = x;
            x.is_multiple_of(every)
        }
    };
    if moves && !wall.still {
        wall.ms += 1;
    }
    if wall.back > 0 && !wall.still && splitmix(&mut wall.back_rng).is_multiple_of(16) {
        wall.ms -= wall.back;
    }
    // Half a millisecond on, so `CAST(x * 1000 AS INTEGER)` reads back the same millisecond.
    let seconds = (wall.ms as f64 + 0.5) / 1000.0;
    unsafe { ffi::sqlite3_result_double(ctx, seconds) };
}

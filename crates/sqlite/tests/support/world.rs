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
}

struct Source {
    rng: Mutex<u64>,
    wall: Arc<Mutex<Wall>>,
}

#[derive(Clone)]
pub struct World {
    seed: u64,
    wall: Arc<Mutex<Wall>>,
}

impl World {
    pub fn new(seed: u64) -> World {
        World {
            seed,
            wall: Arc::new(Mutex::new(Wall {
                ms: 1_700_000_000_000,
                rng: seed ^ 0x5555,
            })),
        }
    }

    /// Moves the shared clock on by `ms`.
    pub fn later(&self, ms: u64) {
        self.wall.lock().unwrap().ms += ms;
    }

    /// An empty database, numbered `n` within this world, with foreign keys enforced.
    pub async fn connect(&self, n: u64) -> SqliteConnection {
        let mut conn = SqliteConnectOptions::from_str("sqlite::memory:")
            .unwrap()
            .foreign_keys(true)
            .connect()
            .await
            .unwrap();
        // Streams that start at mixed states, not at neighbouring ones, never overlap in practice.
        let mut rng = self.seed ^ n.wrapping_mul(0xD1B5_4A32_D192_ED03);
        let rng = splitmix(&mut rng);
        let mut handle = conn.lock_handle().await.unwrap();
        let db = handle.as_raw_handle().as_ptr();
        for (name, args, call) in [
            ("randomblob", 1, randomblob as Call),
            ("random", 0, random as Call),
            ("unixepoch", 1, unixepoch as Call),
        ] {
            let mut start = rng ^ (name.len() as u64).wrapping_mul(0x8CB9_2BA7_2F3D_8DD7);
            let source = Box::new(Source {
                rng: Mutex::new(splitmix(&mut start)),
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
    if !splitmix(&mut wall.rng).is_multiple_of(4) {
        wall.ms += 1;
    }
    // Half a millisecond on, so `CAST(x * 1000 AS INTEGER)` reads back the same millisecond.
    let seconds = (wall.ms as f64 + 0.5) / 1000.0;
    unsafe { ffi::sqlite3_result_double(ctx, seconds) };
}

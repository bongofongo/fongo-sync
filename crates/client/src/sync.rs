use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::task::{Poll, Waker};
use std::time::{Duration, Instant};

use fongo_sync_protocol::api::{
    Appended, BatchPage, Devices, ErrorCode, ReadBatches, StoredBatch, batches_path, devices_path,
};
use fongo_sync_protocol::{
    DeviceId, Envelope, EnvelopeError, VerifyingKey, encode_batch, open, random_nonce, seal,
};
use fongo_sync_sqlite::core::{ALIAS, Change, DELETED, Gid, Value};
use fongo_sync_sqlite::{Applied, COLLECT_ROWS, Collected, Conflict, Replica, Touched};
use sqlx::{Connection, Sqlite, SqliteConnection, Transaction};

use crate::credentials::Credentials;
use crate::error::Error;
use crate::rpc::{Body, Rpc, json};
use crate::state::{self, Sending};
use crate::transport::{Method, Transport};

/// Envelopes asked for per page when the server states no maximum.
const DEFAULT_PAGE: u32 = 256;
/// Pages read in one call; [`SyncReport::more`] says when the log held more.
const MAX_PAGES: usize = 1_000;
/// Refusals listed in [`SyncReport::refused`]; [`SyncReport::refused_total`] counts them all.
const REFUSALS_LISTED: usize = 100;
/// The encoded size of a batch of one change with an empty collection and field and a null value.
const SMALLEST_BATCH: usize = 3 + 1 + 16 + 1 + 1 + 8 + 4 + 16;

/// How a sync shares the database with other connections that write to it. SQLite lets one
/// connection write at a time, and a sync takes that turn for each of its transactions: each step
/// of collecting local writes, each apply, `caught_up`, and the record and acknowledgment of each
/// envelope it sends. These settings keep each turn short and leave room between them.
/// [`Apply::pace`] gives them; [`sync`] uses the defaults.
///
/// With the defaults, a writer on another connection that waits in SQLite's busy handler usually
/// waits about `hold`, one apply and `pause`, and longer when several connections compete for the
/// same gap. Joining 200,000 changes on a fast laptop while two writers and a reader each used the
/// database every 20 ms, the worst wait in a run was about a second, with a pause of 150, 200 or
/// 250 ms alike; each 50 ms more pause cost about a second of sync time and did not remove that
/// tail. Readers wait too in rollback-journal mode, up to about 0.3 s there. A busy timeout near
/// 1 s is therefore marginal whatever the pause: give other connections several seconds, such as
/// sqlx's default of 5 s.
///
/// Setting `pause` to zero, without letting writers through in [`Apply::between`], makes a large
/// sync slower than one bounded by nothing and no fairer to writers in the busy handler; it suits
/// an application whose writers all wait on a gate of its own that `between` opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pace {
    /// Most changes one apply transaction is given. Received envelopes are held until the next
    /// would take the count past this, and what is held is then applied and the cursor stored past
    /// it; an envelope is never split, so one holding more than this is applied alone. 0 counts as
    /// 1, one envelope per apply. Default 4,000. An apply takes longer the larger the library: on
    /// a fast laptop, the kernel's work for 4,000 changes took about 50 ms in a new library and
    /// about 180 ms in one of 200,000 changes, before an application's own [`Apply::apply`] work
    /// and on slower devices more.
    pub apply: usize,
    /// About how many rows written since the last collect one transaction collects. Before each
    /// apply, while at least this many rows wait to be collected, the sync collects them through
    /// [`Apply::collect`] in transactions of their own, paced as the others, until those steps
    /// have collected, in all, as many rows as waited when the first of them began, or one finds
    /// that only rows that cannot be collected yet are left; the apply then collects what is
    /// left, which is what was written during the steps, those rows, and fewer than this many
    /// besides. A step takes more where rows it takes depend on later ones, share a millisecond,
    /// or follow rows that wait; see [`Replica::collect_step`]. 0 counts as 1. Default
    /// [`COLLECT_ROWS`].
    ///
    /// This field is new: code that builds a `Pace` naming every field must name it too, or
    /// start from `..Pace::default()`.
    pub collect: usize,
    /// Most changes this device seals into one envelope, so a receiver's single apply of it stays
    /// short; the server's maximum envelope size can make envelopes smaller still. 0 counts as 1.
    /// Default 1,000.
    pub seal: u32,
    /// How long the sync holds the write lock, summed over its transactions, before it pauses.
    /// Default 250 ms.
    pub hold: Duration,
    /// How long it then leaves the write lock free before its next transaction, so that a writer
    /// waiting in SQLite's busy handler, which sleeps up to 100 ms between tries, wakes inside it.
    /// The pause is timed by a thread of the client's own and is taken only when another
    /// transaction follows. Zero never pauses. Default 150 ms.
    pub pause: Duration,
    /// How long syncs that ran just before this one held the write lock since they last paused:
    /// [`SyncReport::held`] of the last of them. The time a sync holds the lock starts from this,
    /// so a caller that syncs again at once, as while [`SyncReport::more`] is set, keeps the
    /// pauses it would have had in one longer sync; when it is already `hold` or more, the sync
    /// pauses before its first write. Default zero.
    pub held: Duration,
}

impl Default for Pace {
    fn default() -> Pace {
        Pace {
            apply: 4_000,
            collect: COLLECT_ROWS,
            seal: 1_000,
            hold: Duration::from_millis(250),
            pause: Duration::from_millis(150),
            held: Duration::ZERO,
        }
    }
}

/// Waits `duration`. A thread of its own sleeps and wakes the task, so a sync needs no particular
/// runtime's timer; if the thread cannot be made, it does not wait.
async fn pause(duration: Duration) {
    let state: Arc<Mutex<(bool, Option<Waker>)>> = Arc::default();
    let mut started = false;
    std::future::poll_fn(|cx| {
        let mut shared = state.lock().unwrap_or_else(|e| e.into_inner());
        if shared.0 {
            return Poll::Ready(());
        }
        shared.1 = Some(cx.waker().clone());
        if !started {
            started = true;
            let state = state.clone();
            let spawned = std::thread::Builder::new().spawn(move || {
                std::thread::sleep(duration);
                let mut shared = state.lock().unwrap_or_else(|e| e.into_inner());
                shared.0 = true;
                if let Some(waker) = shared.1.take() {
                    waker.wake();
                }
            });
            if spawned.is_err() {
                return Poll::Ready(());
            }
        }
        Poll::Pending
    })
    .await
}

/// Why an envelope from the server's log was skipped instead of applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Refusal {
    /// Not an envelope, or what it decrypts to is not a batch.
    Malformed,
    /// It names another library.
    WrongLibrary,
    /// Signed by the library's recovery key, which may add devices but never writes.
    RecoveryKey,
    /// Signed by a key the library's device list does not hold. Not reported by this version: such
    /// an envelope may become valid once the list catches up, so it blocks the pull instead (see
    /// [`SyncReport::blocked`]).
    UnknownDevice,
    /// Its signature does not verify against the key of the device it names.
    BadSignature,
    /// It does not decrypt under the library's key, so whoever wrote it lacks the library secret.
    Undecryptable,
}

/// One envelope skipped and never applied. It does not stop the sync, and the cursor moves past
/// it, so it is not read again.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Refused {
    pub seq: u64,
    /// The device the envelope names, when it could be read.
    pub device: Option<DeviceId>,
    pub reason: Refusal,
}

/// An envelope the pull stopped at: its signer is not in the library's device list, even fetched
/// afresh. An honest server stores only envelopes from listed devices, so the list is behind or
/// the server is faulty; either way the envelope may become valid, so the cursor stays before it
/// and every later sync tries it again. Change notifications keep reporting a sync worth running
/// meanwhile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Blocked {
    pub seq: u64,
    pub device: DeviceId,
}

/// What one [`sync`] did.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SyncReport {
    /// Changes received from other devices and applied, whether or not they won.
    pub received: usize,
    /// Changes this device sent.
    pub sent: usize,
    /// What applying them did, merged over every apply in the sync with [`merge_applied`]:
    /// changes accepted, rows touched, conflicts, one for each local edit with the latest remote
    /// change it met, and entities still waiting. A conflict's `gid` and `key` are as the sync
    /// leaves them, looked up once it is done: `gid` is the id of the entity's group, and `key`
    /// that of the row holding the entity or any entity merged with it, `None` if no row does. A
    /// later apply of the same sync may have merged the entity into a group, removed its row, or
    /// given the row's key to another entity, and a local edit an earlier apply reported under
    /// the entity's own id is reported once, under the group's. Each lookup takes about 75 µs on
    /// a fast laptop; if one fails, the conflict keeps the id and key its apply reported, and the
    /// sync still returns its report. `touched` lists keys in the order the sync
    /// first touched them, each key's entries together. Within a key it holds one entry per
    /// entity, that entity's last, in the order those entries came: the entities that left the
    /// key come before the one it was left holding.
    pub applied: Applied,
    /// The first refusals, at most 100.
    pub refused: Vec<Refused>,
    /// Every refusal in this sync, listed or not.
    pub refused_total: usize,
    /// Set when the pull stopped at an envelope from an unlisted signer. What came before it was
    /// applied and the outbox was still sent.
    pub blocked: Option<Blocked>,
    /// The pull stopped after the most pages one call reads, with more waiting. Everything read
    /// was applied and the outbox was sent; call again to read on.
    pub more: bool,
    /// The last sequence number this sync knows the library's log to hold.
    pub head: u64,
    /// How long this sync held the write lock since it last paused, counting [`Pace::held`] when
    /// it did not pause; give it as [`Pace::held`] to a sync that follows at once.
    pub held: Duration,
}

/// One round with the server: reads the envelopes after the stored cursor, applies what other
/// devices wrote, then seals and appends this device's outbox.
///
/// `now` gives the current unix time in seconds; every request is stamped with it.
///
/// The cursor is stored after every page read. Received changes are held, whole envelopes at a
/// time and across pages, until the next envelope would take them past [`Pace::apply`], and then
/// applied together with the cursor in one transaction, so a crash at any point leaves either both
/// or neither; re-reading changes already applied is harmless. Local writes are collected before
/// each apply, in steps of [`Pace::collect`] rows each committed on its own, and what remains by
/// the apply itself. A local edit to a field that reports conflicts is compared with every remote
/// change to it until the pull has read past the envelope that carried it, and a remote change
/// met before that point is reported in [`SyncReport::applied`].
///
/// Conflicts are reported on one side only. Of two concurrent edits, only the device whose
/// envelope lands later in the log is told; the device whose envelope landed first has released
/// its edit's base by the time it reads the other, and takes the other's value, when that wins,
/// without a report. Envelopes carry no record of what their author had seen, so a change read
/// after one's own envelope cannot be told apart from one made knowing it.
///
/// Whatever is held is also applied before this returns an error, so a failure part-way keeps the
/// progress made.
///
/// Each envelope to send is recorded in the database before it is appended, and the outbox
/// acknowledged and the record cleared in one transaction once the server confirms. When a sync
/// stops between the two, whether or not the server stored the envelope, the next sync appends
/// the same recorded bytes again before anything new, unless its pull found them in the log, in
/// which case they are not sent again; a server that stores an identical envelope
/// once makes that harmless, and otherwise the duplicate is applied idempotently by every device.
/// Changes are not re-sealed into a second, different envelope, with one exception: when the
/// server refuses the recorded envelope itself, as too large or not one it accepts, the record is
/// dropped and its changes, still in the outbox because they were never acknowledged, are sealed
/// afresh under the server's current limits. Should the server in fact hold the earlier envelope,
/// its changes then arrive twice, which every device applies idempotently.
///
/// Envelopes this device signed are applied like any other: applying a change already held is a
/// no-op, and a database restored from a backup, or a second database sharing these credentials,
/// may not hold them. An append the server places right after the cursor moves the cursor past
/// it, so a device does not usually read back what it sent.
///
/// # Other writers
///
/// SQLite lets one connection write at a time. A sync writes in transactions of its own: steps of
/// collecting local writes and each apply, then [`Apply::caught_up`], then a record and an
/// acknowledgment for each envelope it sends. Each takes the write lock with its first statement,
/// waiting for it in the connection's busy handler, and holds it until it commits, so the longest
/// hold is the largest of them. What bounds each: a step of collecting takes about
/// [`Pace::collect`] rows, more where rows depend on later ones, share a millisecond or follow
/// rows that cannot be collected yet, at worst every row waiting; an
/// apply takes at most [`Pace::apply`] received changes unless one envelope holds more, and
/// collects the rows written while the steps before it ran, which nothing bounds, with fewer
/// than [`Pace::collect`] left over besides; [`Apply::caught_up`] holds what the application
/// makes it hold; a record or an acknowledgment is small. Nothing here bounds what the
/// application does outside a sync: [`Replica::resume`], a first [`Replica::install`] over rows
/// already in its tables, and a plain [`Replica::install`] or [`Replica::collect`] each hold the
/// lock for everything they do. A first [`Replica::install_step`] records the rows it finds in
/// one transaction and leaves adopting them to steps, which a sync takes as steps of collecting
/// before its first apply; an apply adopts whatever is left first, whole.
///
/// Between transactions no transaction is open; once the sync has held the lock for
/// [`Pace::hold`] in all, it leaves it free for [`Pace::pause`] before the next, long enough for a
/// writer waiting in SQLite's busy handler on another connection to wake and write. A sync that
/// holds the lock for less than [`Pace::hold`] never pauses. [`sync_with`] also calls
/// [`Apply::between`] in each gap, where an application can let its own writers through.
///
/// # The connection
///
/// A sync needs a connection with no transaction open, as SQLite and sqlx both count them: it
/// commits transactions of its own, and a step given through [`Apply`] that ended its transaction
/// would end a caller's. It does not run inside a caller's transaction. It checks first, with
/// [`check_connection`], and otherwise fails with [`Error::ConnectionUnsound`] before reading or
/// writing anything.
///
/// It needs the kernel installed, of the layout this build uses, with capture running: it asks
/// [`Replica::suspended`] next, before reaching the server, and otherwise fails with
/// [`Error::Database`] carrying the kernel's refusal, such as
/// [`fongo_sync_sqlite::Error::Suspended`] after a suspend that no resume has ended.
///
/// A sync that returns `Ok`, or any error other than [`Error::ConnectionUnsound`], leaves the
/// connection sound. [`Error::ConnectionUnsound`] means it cannot be trusted: sqlx may count a
/// transaction SQLite has ended, or SQLite may hold one nothing will end, and what is written on
/// it then may never reach the file. Close it, awaiting the close, before writing anything more,
/// and open a fresh one: a connection only dropped, or left open, can keep its write transaction
/// on the file, and the fresh one then finds the database locked. A pooled connection is detached
/// and closed rather than given back.
///
/// A sync that panicked or was dropped can leave the connection unsound too, and returns nothing to
/// say so; the next sync reports it, before reading or writing anything.
///
/// # What the server is relied on for
///
/// - Numbering each library's envelopes 1, 2, 3 and so on without gaps, as the protocol states. A
///   page that skips, repeats or reorders a number is refused as malformed.
/// - Nothing else about appends: storing an identical envelope only once is welcome but not
///   relied on, since a copy is applied idempotently.
pub async fn sync<T: Transport>(
    conn: &mut SqliteConnection,
    replica: &Replica,
    transport: &T,
    credentials: &Credentials,
    now: impl Fn() -> u64 + Sync,
) -> Result<SyncReport, Error> {
    let result = run(conn, replica, transport, credentials, now, &mut (), false).await;
    finish(conn, result).await
}

/// How [`sync_with`] applies what it receives, what it does once a pull has read the whole log,
/// what it does between transactions, and how it paces them. [`sync`] uses `()`, which keeps every
/// default.
///
/// [`Apply::apply`] and [`Apply::caught_up`] are given the connection inside a transaction the
/// client opened, and must neither commit nor roll it back; a savepoint of their own is fine, as
/// [`Replica::apply`] makes one. Everything they write there is the application's, and is
/// committed or rolled back with whatever the client writes in that transaction. A write to a
/// synced table is a local edit like any other, collected by the next apply.
///
/// Some statements end the transaction as a side effect: SQLite answers some failures by rolling
/// it back, as with `INSERT OR ROLLBACK`, a trigger's `RAISE(ROLLBACK)` or a full disk. When a
/// method returns with the client's transaction ended, even having caught that error, the sync
/// fails with [`Error::TransactionEnded`] and the cursor does not move; whatever the method wrote
/// after the transaction ended was committed on its own.
///
/// The savepoint `fongo_client_watch`, made before each call, and the temporary table of that
/// name are the client's: a method must not release the savepoint, roll back to it, or write the
/// table. Rolling back to it fails the sync as an ended transaction does; releasing it fails the
/// sync too, though nothing was lost.
///
/// Because a method can end the transaction it is given, a sync never runs inside a caller's
/// transaction, and one that panics can leave the connection unusable; see [`sync`].
///
/// Implement it with `async fn`; the futures must be `Send`.
pub trait Apply: Send {
    /// Applies a batch of received changes and returns what that did, which is merged into
    /// [`SyncReport::applied`]. By default, `replica.apply(conn, changes)`. An implementation
    /// passes `changes` to [`Replica::apply`] once, may apply again with no changes, and returns
    /// those applies combined with [`merge_applied`].
    ///
    /// `changes` may hold this device's own, read back from the log, and may hold changes an
    /// earlier apply was given: the log can hold a second copy of an envelope, from a server that
    /// stores an identical envelope twice or from an envelope sealed again after the server
    /// refused it. Applying a change again is a no-op, so an implementation works from what the
    /// apply did, such as [`Applied::touched`], and not from the changes it is given.
    ///
    /// It is called for every apply the client makes, including the apply with no changes a sync
    /// makes when it receives nothing: that apply collects local writes and may write rows held
    /// back before, so what it touched need not be empty. Rows written by the steps of collecting
    /// before an apply are reported by [`Apply::collect`] instead, in each step's transaction.
    ///
    /// `changes` holds whole envelopes, at most [`Pace::apply`] changes unless one envelope holds
    /// more. A pull can thus make many applies, and a field changed remotely more than once before
    /// this device's own edit of it was read back can be reported as a conflict by more than one
    /// of them, each with a later change; [`merge_applied`] keeps the last. A conflict's `gid`
    /// and `key` here are as this apply leaves them; a later apply of the same sync can merge the
    /// entity into a group, remove its row or give the row's key to another entity, so neither is
    /// kept from one apply to the next. [`SyncReport::applied`] gives them as the sync ends.
    ///
    /// In the same transaction, after this returns, the client stores the cursor past the
    /// envelopes `changes` came in, so the rows applied, everything this wrote and the cursor are
    /// committed together or not at all; once committed, those envelopes are not read again. When
    /// this or anything after it in the transaction fails, the transaction is rolled back, the
    /// cursor stays before them, and the sync returns an error, so there is no report. A failure
    /// part-way through a pull stops the reading, and, as after any failure there, the client
    /// applies what it holds once more, calling this again with the same changes and keeping what
    /// that commits, before it returns the first error. The database is rolled back before that
    /// second call; nothing this kept in `self` during the first is. The next sync reads whatever
    /// was not committed again.
    fn apply(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        changes: &[Change],
    ) -> impl Future<Output = Result<Applied, Error>> + Send {
        async move { Ok(replica.apply(conn, changes).await?) }
    }

    /// Collects a step of about `rows` local writes, before an apply, in a transaction the client
    /// opened, as [`Apply::apply`] applies: it must neither commit nor roll it back, and what it
    /// writes is committed with the step. By default, `replica.collect_step(conn, rows)`. An
    /// implementation calls [`Replica::collect_step`] once and returns what it did.
    ///
    /// A step can write rows besides collecting them: a merge of entities its rows were found to
    /// share, or a row held back before that it can now write. It lists them in
    /// [`Collected::touched`], which is merged into [`SyncReport::applied`]; one apply of
    /// everything would have reported them itself. An application that works from what an apply
    /// touched does the same with these, here, in the step's transaction.
    ///
    /// Before each apply it is called while at least [`Pace::collect`] rows wait to be
    /// collected, until its steps have collected, in all, as many rows as waited when the first
    /// of them began, whichever rows those are, or one reports that no more are left
    /// ([`Collected::more`]); the apply collects what is left. It is not called when fewer wait.
    fn collect(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        rows: usize,
    ) -> impl Future<Output = Result<Collected, Error>> + Send {
        async move { Ok(replica.collect_step(conn, rows).await?) }
    }

    /// Called once in a sync whose pull read to the head of the log, after its last apply and
    /// before the outbox is sent, even when nothing was received. It is not called when the pull
    /// stopped with more waiting ([`SyncReport::more`]), stopped at an envelope it is blocked at
    /// ([`SyncReport::blocked`]), or failed. By default it does nothing and returns `None`.
    ///
    /// It runs in a transaction of its own, begun once the transaction that stored the cursor at
    /// the end of the pull has committed, so nothing it writes is atomic with the cursor. What it
    /// returns is merged into [`SyncReport::applied`]. An apply it makes with no changes,
    /// `replica.apply(conn, &[])`, collects what it wrote to synced tables, and this sync sends
    /// that.
    ///
    /// When it fails, its transaction is rolled back and the sync returns the error without
    /// sending the outbox; what the pull applied, and the cursor, stay. Since a failure or a crash
    /// can come between the pull's commit and this one, the next sync that reads to the head
    /// calls it again: it should act on the database as it stands, not on what one pull touched.
    fn caught_up(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
    ) -> impl Future<Output = Result<Option<Applied>, Error>> + Send {
        let _ = (conn, replica);
        async { Ok(None) }
    }

    /// Called between two of the sync's write transactions: after one has committed and before
    /// the next begins, with no transaction open, so that another writer can take its turn. An
    /// application whose own writes wait on a gate of its own can let them through here, or it
    /// can yield or sleep. By default it does nothing.
    ///
    /// It is called once in each gap: before every step of collecting and every apply but the
    /// sync's first transaction, before `caught_up`, and before each record and acknowledgment of
    /// an envelope sent, whenever a transaction of this sync committed before it; and after the
    /// client's own pause, when [`Pace`] calls for one. It is never called inside a transaction,
    /// before the sync's first write, after its last, or once anything in the sync has failed.
    ///
    /// It may use the connection for statements and transactions of its own, provided it ends
    /// every transaction it begins; what it writes to a synced table is a local edit, collected by
    /// the next apply. If it returns with a transaction open on the connection, the sync writes
    /// nothing more and fails with [`Error::ConnectionUnsound`].
    ///
    /// An application that sends its own writes through a gate, such as a fair async mutex, can
    /// hold the gate for the whole sync and, here, release it and take it again: the writers
    /// queued on it go first and find the database free. The client's pause comes before this
    /// call, with the gate still held, so those writers wait out the pause too; when every writer
    /// goes through the gate, `pause` can be zero. A connection of the application must not wait
    /// on the gate inside a write transaction of its own: it then holds SQLite's lock while the
    /// sync waits for it, and the sync's next write fails when its busy timeout runs out.
    ///
    /// When it fails, the sync stops: as after any failure in a pull, it applies what it holds
    /// once more, without calling this again, and returns the error; nothing is sent. When the
    /// sync is dropped while this runs, no transaction of the client's is open, and the next
    /// sync reads again whatever was not committed.
    fn between(
        &mut self,
        conn: &mut SqliteConnection,
    ) -> impl Future<Output = Result<(), Error>> + Send {
        let _ = conn;
        async { Ok(()) }
    }

    /// How this sync paces its transactions. By default, [`Pace::default`].
    fn pace(&self) -> Pace {
        Pace::default()
    }
}

impl Apply for () {}

/// [`sync`], with the application's own [`Apply`] in place of [`Replica::apply`] alone. Nothing
/// else changes: the order of pull and append, the cursor, the conflict bases, and the connection
/// it needs, with no transaction open, are as `sync` keeps them.
///
/// What the client guarantees about atomicity with its cursor: what [`Apply::apply`] writes is
/// committed in one transaction with the changes it applied and the cursor past them, and with the
/// client's other writes there; what [`Apply::caught_up`] and [`Apply::between`] write is committed
/// on its own.
pub async fn sync_with<T: Transport, A: Apply>(
    conn: &mut SqliteConnection,
    replica: &Replica,
    transport: &T,
    credentials: &Credentials,
    now: impl Fn() -> u64 + Sync,
    apply: &mut A,
) -> Result<SyncReport, Error> {
    let result = run(conn, replica, transport, credentials, now, apply, true).await;
    finish(conn, result).await
}

/// Returns a sync's result, unless it is an error that left the connection unsound: a step that
/// left a savepoint of its own behind, or ended the transaction inside one, or a commit that ended
/// its own transaction. Then the connection cannot be trusted, and the error says so, carrying the
/// original as its source.
async fn finish(
    conn: &mut SqliteConnection,
    result: Result<SyncReport, Error>,
) -> Result<SyncReport, Error> {
    match result {
        Err(error)
            if !matches!(error, Error::ConnectionUnsound { .. })
                && check_connection(conn).await.is_err() =>
        {
            Err(Error::ConnectionUnsound {
                source: Some(Box::new(error)),
            })
        }
        result => result,
    }
}

/// One sync; `settles` says whether to call [`Apply::caught_up`], which `()` leaves empty.
async fn run<T: Transport, A: Apply>(
    conn: &mut SqliteConnection,
    replica: &Replica,
    transport: &T,
    credentials: &Credentials,
    now: impl Fn() -> u64 + Sync,
    apply: &mut A,
    settles: bool,
) -> Result<SyncReport, Error> {
    check_connection(conn).await?;
    if replica.suspended(conn).await? {
        return Err(fongo_sync_sqlite::Error::Suspended.into());
    }
    let rpc = Rpc {
        transport,
        now: &now,
    };
    let caps = rpc.capabilities().await?;
    let library = credentials.library_id();
    let me = credentials.device_id();
    let pace = apply.pace();
    let mut held = pace.held;
    if !pace.pause.is_zero() && !held.is_zero() && held >= pace.hold {
        pause(pace.pause).await;
        held = Duration::ZERO;
    }
    let saved = state::load(conn, library).await?;
    let mut cursor = saved.cursor;
    let limit = caps.max_page_batches.unwrap_or(DEFAULT_PAGE).max(1);
    let mut keys = Keys::new(credentials);
    let mut report = SyncReport::default();

    let mut pull = Pull {
        read: cursor,
        held: Vec::new(),
        applied: false,
        appended: saved.appended,
        sending: saved.sending,
        landed: None,
    };
    let mut steps = Steps {
        replica,
        library,
        pace,
        apply,
        committed: false,
        failed: false,
        held,
        conflicts: Vec::new(),
    };
    let pulled = read_pages(
        &rpc,
        conn,
        &mut steps,
        &mut keys,
        limit,
        &mut pull,
        &mut report,
    )
    .await;
    steps.failed |= pulled.is_err();
    let collect = !pull.applied;
    let committed = if matches!(pulled, Err(Error::ConnectionUnsound { .. })) {
        Ok(())
    } else {
        pull.commit(conn, &mut steps, library, &mut report, collect)
            .await
    };
    pulled.and(committed)?;
    cursor = pull.read;
    report.head = cursor.max(report.blocked.map_or(0, |b| b.seq));

    if settles && report.blocked.is_none() && !report.more {
        let (mut tx, began) = steps.begin(conn).await?;
        let settled = async {
            begin_watch(&mut tx).await?;
            let settled = steps.apply.caught_up(&mut tx, replica).await?;
            end_watch(&mut tx).await?;
            Ok(settled)
        }
        .await;
        let settled = match settled {
            Ok(settled) => settled,
            Err(error) => return Err(abandon(tx, error).await),
        };
        tx.commit().await?;
        steps.committed(began);
        if let Some(settled) = settled {
            steps.conflicts.push(settled.conflicts.clone());
            merge_applied(&mut report.applied, settled);
        }
    }

    if let Some(sending) = pull.sending.take() {
        let ours = Envelope::decode(&sending.envelope)
            .is_ok_and(|envelope| envelope.library == library && envelope.device == me);
        if ours {
            let count = open(&sending.envelope, credentials.library(), |_| {
                Some(credentials.device().verifying_key())
            })
            .map_or(0, |opened| opened.changes.len());
            match append(&rpc, credentials, sending.envelope).await {
                Ok(seq) => {
                    acknowledge(conn, &mut steps, library, sending.through, seq, &mut cursor)
                        .await?;
                    report.sent += count;
                    report.head = report.head.max(seq);
                }
                Err(Error::Server { code, .. }) if refuses_the_envelope(code) => {
                    clear_sending(conn, &mut steps, library).await?;
                }
                Err(error) => return Err(error),
            }
        } else {
            clear_sending(conn, &mut steps, library).await?;
        }
    }

    let max = caps.max_batch_bytes;
    let seal_at_most = steps.pace.seal.max(1);
    loop {
        let full = replica.outbox(conn, seal_at_most).await?;
        let Some(first) = full.changes.first() else {
            break;
        };
        let mut n = fitting(&full.changes, max);
        let (outbox, envelope) = loop {
            if n == 0 {
                let max = max.unwrap_or(u64::MAX);
                let internal = first.field == DELETED || first.field == ALIAS;
                if internal || max < envelope_len(SMALLEST_BATCH) as u64 {
                    return Err(Error::MaxBatchTooSmall { max });
                }
                return Err(Error::ChangeTooLarge {
                    collection: first.collection.clone(),
                    field: first.field.clone(),
                    bytes: envelope_len(encode_batch(std::slice::from_ref(first)).len()),
                    max,
                });
            }
            let outbox = if n < full.changes.len() {
                replica.outbox(conn, n as u32).await?
            } else {
                full.clone()
            };
            let nonce = random_nonce().map_err(|_| Error::Randomness)?;
            let envelope = seal(
                &outbox.changes,
                credentials.library(),
                credentials.epoch(),
                credentials.device(),
                nonce,
            );
            if max.is_none_or(|max| envelope.len() as u64 <= max) {
                break (outbox, envelope);
            }
            n -= 1;
        };
        let sending = Sending {
            through: outbox.through,
            envelope,
        };
        steps.turn(conn).await?;
        let began = Instant::now();
        state::record_sending(conn, library, &sending).await?;
        steps.committed(began);
        let seq = append(&rpc, credentials, sending.envelope).await?;
        acknowledge(conn, &mut steps, library, sending.through, seq, &mut cursor).await?;
        report.sent += outbox.changes.len();
        report.head = report.head.max(seq);
    }
    let mut conflicts = Vec::new();
    for mut applied in std::mem::take(&mut steps.conflicts) {
        for conflict in &mut applied {
            if let Ok((gid, key)) = replica
                .group_of(conn, &conflict.collection, conflict.gid)
                .await
            {
                conflict.gid = gid;
                conflict.key = key;
            }
        }
        merge_conflicts(&mut conflicts, applied);
    }
    report.applied.conflicts = conflicts;
    report.held = steps.held;
    Ok(report)
}

/// Forgets the recorded envelope, in a write of its own.
async fn clear_sending<A: Apply>(
    conn: &mut SqliteConnection,
    steps: &mut Steps<'_, A>,
    library: fongo_sync_protocol::LibraryId,
) -> Result<(), Error> {
    steps.turn(conn).await?;
    let began = Instant::now();
    state::clear_sending(conn, library).await?;
    steps.committed(began);
    Ok(())
}

/// The replica received changes are applied to, how, and the turns the sync takes at writing.
struct Steps<'a, A> {
    replica: &'a Replica,
    library: fongo_sync_protocol::LibraryId,
    apply: &'a mut A,
    pace: Pace,
    /// A transaction of this sync committed since the last turn was given.
    committed: bool,
    /// Something in this sync failed: no turn is given after that.
    failed: bool,
    /// How long this sync held the write lock since it last paused.
    held: Duration,
    /// The conflicts each apply reported, in order.
    conflicts: Vec<Vec<Conflict>>,
}

impl<A: Apply> Steps<'_, A> {
    /// Before a write: once a transaction of this sync has committed, pauses when the pace calls
    /// for it, and then calls [`Apply::between`].
    async fn turn(&mut self, conn: &mut SqliteConnection) -> Result<(), Error> {
        if !std::mem::take(&mut self.committed) || self.failed {
            return Ok(());
        }
        if !self.pace.pause.is_zero() && self.held >= self.pace.hold {
            pause(self.pace.pause).await;
            self.held = Duration::ZERO;
        }
        let between = async {
            self.apply.between(conn).await?;
            check_connection(conn).await
        };
        let result = between.await;
        self.failed = result.is_err();
        result
    }

    /// Takes a turn and begins a transaction whose first statement writes, so it waits for the
    /// write lock at its start, in the busy handler, and never finds at a later write that another
    /// connection wrote since it first read, which SQLite answers with an error instead of a wait.
    /// sqlx's `begin_with("BEGIN IMMEDIATE")` would do the same, but a future dropped inside it
    /// can leave the transaction open with nothing to end it.
    async fn begin<'c>(
        &mut self,
        conn: &'c mut SqliteConnection,
    ) -> Result<(Transaction<'c, Sqlite>, Instant), Error> {
        self.turn(conn).await?;
        let began = Instant::now();
        let mut tx = conn.begin().await?;
        sqlx::query("UPDATE fongo_client_state SET cursor = cursor WHERE library = ?")
            .bind(&self.library.0[..])
            .execute(&mut *tx)
            .await?;
        Ok((tx, began))
    }

    /// A write that began at `began` has committed.
    fn committed(&mut self, began: Instant) {
        self.committed = true;
        self.held += began.elapsed();
    }

    /// Collects local writes in steps of [`Pace::collect`] rows, each a transaction of its own,
    /// while at least that many wait, so the apply that follows collects fewer; but no more rows
    /// than waited when the first step began, so that writes made meanwhile cannot keep the sync
    /// from its apply.
    async fn collect(
        &mut self,
        conn: &mut SqliteConnection,
        report: &mut SyncReport,
    ) -> Result<(), Error> {
        let rows = self.pace.collect.max(1);
        let replica = self.replica;
        let mut left = replica.uncollected_rows(conn, usize::MAX).await?;
        while left >= rows && replica.uncollected_rows(conn, rows).await? >= rows {
            let (mut tx, began) = self.begin(conn).await?;
            let collected = async {
                begin_watch(&mut tx).await?;
                let collected = self.apply.collect(&mut tx, replica, rows).await?;
                end_watch(&mut tx).await?;
                Ok(collected)
            }
            .await;
            let collected = match collected {
                Ok(collected) => collected,
                Err(error) => return Err(abandon(tx, error).await),
            };
            tx.commit().await?;
            self.committed(began);
            left = left.saturating_sub(collected.rows.max(1));
            if !collected.touched.is_empty() {
                let applied = Applied {
                    touched: collected.touched,
                    waiting: report.applied.waiting,
                    held: report.applied.held.clone(),
                    ..Applied::default()
                };
                merge_applied(&mut report.applied, applied);
            }
            if !collected.more {
                break;
            }
        }
        Ok(())
    }
}

/// Marks the client's transaction before the application's step, with a savepoint and a row
/// written inside it, so [`end_watch`] can tell whether the step ended the transaction or rolled
/// back to the savepoint.
async fn begin_watch(conn: &mut SqliteConnection) -> Result<(), Error> {
    for sql in [
        "SAVEPOINT fongo_client_watch",
        "CREATE TEMP TABLE IF NOT EXISTS fongo_client_watch (mark INTEGER NOT NULL)",
        "INSERT INTO temp.fongo_client_watch VALUES (1)",
    ] {
        sqlx::query(sql).execute(&mut *conn).await?;
    }
    Ok(())
}

/// Fails when the row [`begin_watch`] wrote is gone, or its savepoint: the transaction ended, or
/// was rolled back to the savepoint.
async fn end_watch(conn: &mut SqliteConnection) -> Result<(), Error> {
    let marks: Result<i64, _> = sqlx::query_scalar("SELECT count(*) FROM temp.fongo_client_watch")
        .fetch_one(&mut *conn)
        .await;
    if marks.ok() != Some(1) {
        return Err(Error::TransactionEnded);
    }
    for sql in [
        "DELETE FROM temp.fongo_client_watch",
        "RELEASE fongo_client_watch",
    ] {
        sqlx::query(sql)
            .execute(&mut *conn)
            .await
            .map_err(|_| Error::TransactionEnded)?;
    }
    Ok(())
}

/// Returns `error` once `tx`, which failed, is dropped with a transaction for its rollback to end.
/// A statement that ended SQLite's transaction would leave the rollback sqlx makes on drop with
/// none, and sqlx would then go on counting the transaction open, so that every later one on the
/// connection was a savepoint that never reached the file. `BEGIN` opens one when SQLite has none,
/// and fails, changing nothing, when it has.
async fn abandon(mut tx: Transaction<'_, Sqlite>, error: Error) -> Error {
    let _ = sqlx::query("BEGIN").execute(&mut *tx).await;
    drop(tx);
    error
}

/// Whether SQLite has a transaction open on `conn`: SQLite refuses a change of safety level inside
/// one. The statement is prepared afresh each time, and any failure counts as inside. Being a
/// statement, it runs after any rollback a dropped transaction queued.
async fn in_transaction(conn: &mut SqliteConnection) -> bool {
    sqlx::query("PRAGMA temp.synchronous = OFF")
        .persistent(false)
        .execute(&mut *conn)
        .await
        .is_err()
}

/// Whether `conn` can carry a sync: SQLite has no transaction open on it and sqlx counts none.
/// Fails with [`Error::ConnectionUnsound`] otherwise. [`sync`] and [`sync_with`] call it first,
/// and again before returning an error. It writes nothing to the database; its probe asks for the
/// temporary schema's `synchronous` to be OFF, which is already so unless the application changed
/// it.
pub async fn check_connection(conn: &mut SqliteConnection) -> Result<(), Error> {
    // The probe first: sqlx's count is right only once a rollback it queued has run.
    if in_transaction(conn).await || conn.is_in_transaction() {
        return Err(Error::ConnectionUnsound { source: None });
    }
    Ok(())
}

/// Where reading the log stands: envelopes through `read` are dealt with, and `held` has the
/// changes among them not yet applied. `appended` lists where this database's own appends landed
/// past `read`.
struct Pull {
    read: u64,
    held: Vec<Change>,
    applied: bool,
    appended: BTreeSet<u64>,
    /// The envelope recorded before an append whose answer never came, until the pull finds it.
    sending: Option<Sending>,
    /// The outbox position the recorded envelope covers, once found at `read`.
    landed: Option<i64>,
}

impl Pull {
    /// Applies what is held and stores the cursor at `read`, in one transaction. With `collect`
    /// it applies even when nothing is held, so local writes are collected. The bases of local
    /// edits sent in envelopes at or before `read` are then released.
    async fn commit<A: Apply>(
        &mut self,
        conn: &mut SqliteConnection,
        steps: &mut Steps<'_, A>,
        library: fongo_sync_protocol::LibraryId,
        report: &mut SyncReport,
        collect: bool,
    ) -> Result<(), Error> {
        let replica = steps.replica;
        if collect || !self.held.is_empty() {
            steps.collect(conn, report).await?;
        }
        let (mut tx, began) = steps.begin(conn).await?;
        let written = async {
            let applied = if collect || !self.held.is_empty() {
                begin_watch(&mut tx).await?;
                let applied = steps.apply.apply(&mut tx, replica, &self.held).await?;
                end_watch(&mut tx).await?;
                Some(applied)
            } else {
                None
            };
            state::advance(&mut tx, library, self.read).await?;
            if let Some(through) = self.landed {
                replica.acknowledge(&mut tx, through).await?;
                state::clear_sending(&mut tx, library).await?;
                state::record_appended(&mut tx, library, self.read, through).await?;
            }
            if let Some(through) = state::read_past(&mut tx, library, self.read).await? {
                replica.release_bases(&mut tx, through).await?;
            }
            Ok(applied)
        }
        .await;
        let applied = match written {
            Ok(applied) => applied,
            Err(error) => return Err(abandon(tx, error).await),
        };
        tx.commit().await?;
        steps.committed(began);
        self.landed = None;
        if let Some(applied) = applied {
            self.applied = true;
            report.received += self.held.len();
            steps.conflicts.push(applied.conflicts.clone());
            merge_applied(&mut report.applied, applied);
        }
        self.held.clear();
        Ok(())
    }
}

/// Reads pages after `pull.read` until the log ends, an unlisted signer blocks, or the page limit.
/// Stores the cursor after each page unless changes are held to be applied together.
async fn read_pages<T: Transport, N: Fn() -> u64 + Sync, A: Apply>(
    rpc: &Rpc<'_, T, N>,
    conn: &mut SqliteConnection,
    steps: &mut Steps<'_, A>,
    keys: &mut Keys<'_>,
    limit: u32,
    pull: &mut Pull,
    report: &mut SyncReport,
) -> Result<(), Error> {
    let library = keys.credentials.library_id();
    for _ in 0..MAX_PAGES {
        let query = ReadBatches {
            after: pull.read,
            limit: Some(limit),
        };
        let body = rpc
            .call(
                Method::Get,
                query.target(library),
                Body::Empty,
                Some(keys.credentials.device()),
            )
            .await?;
        let page = BatchPage::decode(&body)
            .map_err(|_| Error::MalformedResponse("a batch page does not decode"))?;
        check_page(&page, pull.read, limit)?;
        for batch in &page.batches {
            let landed = pull
                .sending
                .take_if(|sending| sending.envelope == batch.envelope);
            let opened = keys.open(rpc, batch).await?;
            if let Opened::Changes(changes) = &opened
                && !pull.held.is_empty()
                && pull.held.len() + changes.len() > steps.pace.apply
            {
                pull.commit(conn, steps, library, report, false).await?;
            }
            let before = pull.held.len();
            match opened {
                Opened::Changes(changes) => pull.held.extend(changes),
                Opened::Refused(refused) => {
                    report.refused_total += 1;
                    if report.refused.len() < REFUSALS_LISTED {
                        report.refused.push(refused);
                    }
                }
                Opened::Unlisted(device) => {
                    report.blocked = Some(Blocked {
                        seq: batch.seq,
                        device,
                    });
                    return Ok(());
                }
            }
            pull.read = batch.seq;
            // The server stored the recorded envelope although its answer was lost: it is
            // appended here, and must not be appended again.
            if let Some(sending) = landed {
                report.sent += pull.held.len() - before;
                pull.landed = Some(sending.through);
                pull.appended.insert(batch.seq);
            }
            // A change after this device's own envelope may have been made knowing its edits,
            // so what came before is compared with their bases, and then the bases released.
            if pull.appended.remove(&batch.seq) {
                pull.commit(conn, steps, library, report, false).await?;
            }
        }
        if !page.more {
            return Ok(());
        }
        if pull.held.is_empty() || pull.held.len() >= steps.pace.apply {
            pull.commit(conn, steps, library, report, false).await?;
        }
    }
    report.more = true;
    Ok(())
}

/// Refuses a page that does not continue the log from `after` one sequence number at a time, or
/// that could keep a reader asking forever.
fn check_page(page: &BatchPage, after: u64, limit: u32) -> Result<(), Error> {
    if page.batches.len() > limit as usize {
        return Err(Error::MalformedResponse(
            "a page holds more envelopes than were asked for",
        ));
    }
    if page.batches.is_empty() && page.more {
        return Err(Error::MalformedResponse(
            "an empty page says more envelopes follow",
        ));
    }
    let mut last = after;
    for batch in &page.batches {
        if batch.seq <= last {
            return Err(Error::MalformedResponse(
                "a page repeats or reorders sequence numbers, or goes back past the cursor",
            ));
        }
        if batch.seq != last + 1 || i64::try_from(batch.seq).is_err() {
            return Err(Error::MalformedResponse("a page skips sequence numbers"));
        }
        last = batch.seq;
    }
    Ok(())
}

/// The keys envelopes may be signed with: this device's own, and the library's device list,
/// fetched when first needed and again on meeting each device it lacks.
struct Keys<'a> {
    credentials: &'a Credentials,
    recovery: DeviceId,
    devices: Option<HashMap<DeviceId, (VerifyingKey, bool)>>,
    looked_for: HashSet<DeviceId>,
}

enum Opened {
    Changes(Vec<Change>),
    Refused(Refused),
    /// Signed by a device the list lacks even fetched afresh.
    Unlisted(DeviceId),
}

enum Signer {
    Key(VerifyingKey),
    Refused(Refusal),
    Unlisted,
}

impl<'a> Keys<'a> {
    fn new(credentials: &'a Credentials) -> Keys<'a> {
        let recovery = credentials.library().recovery_signing_key().verifying_key();
        Keys {
            credentials,
            recovery: DeviceId::of(&recovery),
            devices: None,
            looked_for: HashSet::new(),
        }
    }

    /// The changes in one stored envelope, its refusal, or that its signer is unlisted.
    async fn open<T: Transport, N: Fn() -> u64 + Sync>(
        &mut self,
        rpc: &Rpc<'_, T, N>,
        batch: &StoredBatch,
    ) -> Result<Opened, Error> {
        let refuse = |device, reason| {
            Ok(Opened::Refused(Refused {
                seq: batch.seq,
                device,
                reason,
            }))
        };
        let Ok(envelope) = Envelope::decode(&batch.envelope) else {
            return refuse(None, Refusal::Malformed);
        };
        let device = envelope.device;
        if envelope.library != self.credentials.library_id() {
            return refuse(Some(device), Refusal::WrongLibrary);
        }
        let key = match self.signer(rpc, device).await? {
            Signer::Key(key) => key,
            Signer::Refused(reason) => return refuse(Some(device), reason),
            Signer::Unlisted => return Ok(Opened::Unlisted(device)),
        };
        match open(&batch.envelope, self.credentials.library(), |_| Some(key)) {
            Ok(opened) => Ok(Opened::Changes(opened.changes)),
            Err(error) => refuse(Some(device), refusal(error)),
        }
    }

    async fn signer<T: Transport, N: Fn() -> u64 + Sync>(
        &mut self,
        rpc: &Rpc<'_, T, N>,
        device: DeviceId,
    ) -> Result<Signer, Error> {
        if device == self.recovery {
            return Ok(Signer::Refused(Refusal::RecoveryKey));
        }
        if device == self.credentials.device_id() {
            return Ok(Signer::Key(self.credentials.device().verifying_key()));
        }
        let listed = |devices: &Option<HashMap<_, _>>| {
            devices.as_ref().is_some_and(|d| d.contains_key(&device))
        };
        if self.devices.is_none() {
            self.devices = Some(self.fetch(rpc).await?);
        }
        if !listed(&self.devices) && self.looked_for.insert(device) {
            self.devices = Some(self.fetch(rpc).await?);
        }
        Ok(match self.devices.as_ref().and_then(|d| d.get(&device)) {
            None => Signer::Unlisted,
            Some((_, true)) => Signer::Refused(Refusal::RecoveryKey),
            Some((key, false)) => Signer::Key(*key),
        })
    }

    async fn fetch<T: Transport, N: Fn() -> u64 + Sync>(
        &self,
        rpc: &Rpc<'_, T, N>,
    ) -> Result<HashMap<DeviceId, (VerifyingKey, bool)>, Error> {
        let body = rpc
            .call(
                Method::Get,
                devices_path(self.credentials.library_id()),
                Body::Empty,
                Some(self.credentials.device()),
            )
            .await?;
        let devices: Devices = json(&body, "the device list is not the documented JSON")?;
        Ok(devices
            .devices
            .into_iter()
            .map(|d| (DeviceId::of(&d.public_key), (d.public_key, d.recovery)))
            .collect())
    }
}

/// Whether the server refused an append for what the envelope is, which sending the same bytes
/// again cannot change, rather than for the request or the moment.
fn refuses_the_envelope(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::TooLarge
            | ErrorCode::MalformedEnvelope
            | ErrorCode::EnvelopeWrongLibrary
            | ErrorCode::EnvelopeWrongDevice
            | ErrorCode::EnvelopeBadSignature
    )
}

fn refusal(error: EnvelopeError) -> Refusal {
    match error {
        EnvelopeError::Decode(_) | EnvelopeError::Batch(_) => Refusal::Malformed,
        EnvelopeError::WrongLibrary => Refusal::WrongLibrary,
        EnvelopeError::UnknownDevice => Refusal::UnknownDevice,
        EnvelopeError::WrongDevice | EnvelopeError::BadSignature => Refusal::BadSignature,
        EnvelopeError::Decrypt => Refusal::Undecryptable,
    }
}

async fn append<T: Transport, N: Fn() -> u64 + Sync>(
    rpc: &Rpc<'_, T, N>,
    credentials: &Credentials,
    envelope: Vec<u8>,
) -> Result<u64, Error> {
    let body = rpc
        .call(
            Method::Post,
            batches_path(credentials.library_id()),
            Body::Binary(envelope),
            Some(credentials.device()),
        )
        .await?;
    let appended: Appended = json(&body, "an append's answer is not the documented JSON")?;
    if appended.seq == 0 || i64::try_from(appended.seq).is_err() {
        return Err(Error::MalformedResponse(
            "an append's sequence number is out of range",
        ));
    }
    Ok(appended.seq)
}

/// Drops what the server confirmed from the outbox and records where the envelope landed. When it
/// landed right after the cursor, nothing came between, so the cursor moves past it too. The bases
/// of its edits are released at once when the cursor is past it, which is also the case when the
/// server answers with an earlier copy of the same envelope; otherwise by the pull that reads
/// past it.
async fn acknowledge<A: Apply>(
    conn: &mut SqliteConnection,
    steps: &mut Steps<'_, A>,
    library: fongo_sync_protocol::LibraryId,
    through: i64,
    seq: u64,
    cursor: &mut u64,
) -> Result<(), Error> {
    let follows = Some(seq) == cursor.checked_add(1);
    let replica = steps.replica;
    let (mut tx, began) = steps.begin(conn).await?;
    let written = async {
        replica.acknowledge(&mut tx, through).await?;
        state::clear_sending(&mut tx, library).await?;
        state::record_appended(&mut tx, library, seq, through).await?;
        if follows {
            state::advance(&mut tx, library, seq).await?;
        }
        let read = if follows { seq } else { *cursor };
        if let Some(through) = state::read_past(&mut tx, library, read).await? {
            replica.release_bases(&mut tx, through).await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = written {
        return Err(abandon(tx, error).await);
    }
    tx.commit().await?;
    steps.committed(began);
    if follows {
        *cursor = seq;
    }
    Ok(())
}

/// How many of `changes`, from the front, seal into an envelope of at most `max` bytes.
fn fitting(changes: &[Change], max: Option<u64>) -> usize {
    let Some(max) = max else {
        return changes.len();
    };
    let mut plaintext = 2;
    let mut n = 0;
    for change in changes {
        let size = encode_batch(std::slice::from_ref(change)).len() - 3;
        if envelope_len(plaintext + size + varint_len(n + 1)) as u64 > max {
            break;
        }
        plaintext += size;
        n += 1;
    }
    n
}

/// The length of an envelope around a batch of `plaintext` bytes, laid out as the protocol
/// documents: header, ciphertext with its 16-byte tag, signature.
fn envelope_len(plaintext: usize) -> usize {
    let ciphertext = plaintext + 16;
    1 + 16 + 16 + 4 + 24 + varint_len(ciphertext) + ciphertext + 64
}

fn varint_len(mut n: usize) -> usize {
    let mut len = 1;
    while n >= 0x80 {
        n >>= 7;
        len += 1;
    }
    len
}

/// Adds what a later apply did to what earlier ones did, as [`SyncReport::applied`] is built.
///
/// - `accepted` is summed.
/// - `conflicts` are appended, after dropping each earlier one that `from` reports again: the same
///   local edit, that is the same collection, entity, field, base and local value. A later apply
///   reports a local edit again only for a remote change later than the one already reported,
///   and one apply of everything would have reported only that. Each conflict in `from` replaces
///   at most one earlier one. A different local edit of the same field, such as one written
///   between two applies, is a different conflict, and both are kept.
/// - `waiting` and `held` are taken from `from`: each apply reports every entity still held, so
///   the later report replaces the earlier.
/// - `touched` lists keys in the order they were first touched, each key's entries together.
///   Within a key it holds one entry per entity, that entity's last, in the order those entries
///   came, so the entities that left a key come before the one it was left holding.
pub fn merge_applied(into: &mut Applied, from: Applied) {
    into.accepted += from.accepted;
    into.waiting = from.waiting;
    into.held = from.held;
    merge_conflicts(&mut into.conflicts, from.conflicts);
    let mut keys: Vec<(String, String)> = Vec::new();
    let mut at: HashMap<(String, String), Vec<Touched>> = HashMap::new();
    for touched in std::mem::take(&mut into.touched)
        .into_iter()
        .chain(from.touched)
    {
        let key = (touched.collection.clone(), touched.key.clone());
        let entries = at.entry(key.clone()).or_insert_with(|| {
            keys.push(key);
            Vec::new()
        });
        entries.retain(|e| e.gid != touched.gid);
        entries.push(touched);
    }
    for key in keys {
        into.touched.extend(at.remove(&key).unwrap_or_default());
    }
}

/// Appends `from` to `into` as [`merge_applied`] merges conflicts.
fn merge_conflicts(into: &mut Vec<Conflict>, from: Vec<Conflict>) {
    type Edits<'a> = Vec<(&'a Option<Value>, &'a Value)>;
    let mut again: HashMap<(String, Gid, String), Edits> = HashMap::new();
    for c in &from {
        again
            .entry((c.collection.clone(), c.gid, c.field.clone()))
            .or_default()
            .push((&c.base, &c.local));
    }
    into.retain(|c| {
        let Some(edits) = again.get_mut(&(c.collection.clone(), c.gid, c.field.clone())) else {
            return true;
        };
        match edits
            .iter()
            .position(|&(base, local)| *base == c.base && *local == c.local)
        {
            Some(at) => {
                edits.swap_remove(at);
                false
            }
            None => true,
        }
    });
    into.extend(from);
}

#[cfg(test)]
mod tests {
    use fongo_sync_protocol::{LibraryId, LibraryKeys, SigningKey};
    use fongo_sync_sqlite::core::Hlc;

    use super::*;

    fn change(text: &str) -> Change {
        Change {
            collection: "notes".into(),
            gid: Gid([1; 16]),
            field: "body".into(),
            value: Value::Text(text.into()),
            clock: Hlc::zero(Gid([2; 16])),
        }
    }

    #[test]
    fn the_size_estimate_is_the_sealed_size() {
        let keys = LibraryKeys::new(LibraryId([1; 16]), [2; 32]);
        let device = SigningKey::from_bytes(&[3; 32]);
        for count in [0usize, 1, 2, 130] {
            for len in [0usize, 1, 100, 200] {
                let changes: Vec<Change> = (0..count).map(|_| change(&"x".repeat(len))).collect();
                let sealed = seal(&changes, &keys, 0, &device, [0; 24]).len();
                assert_eq!(envelope_len(encode_batch(&changes).len()), sealed);
                assert_eq!(fitting(&changes, Some(sealed as u64)), count);
                if count > 0 {
                    assert!(fitting(&changes, Some(sealed as u64 - 1)) < count);
                }
            }
        }
    }

    #[test]
    fn the_smallest_batch_is_one_empty_change() {
        let empty = Change {
            collection: String::new(),
            gid: Gid([1; 16]),
            field: String::new(),
            value: Value::Null,
            clock: Hlc::zero(Gid([2; 16])),
        };
        assert_eq!(encode_batch(&[empty]).len(), SMALLEST_BATCH);
    }

    fn touched(entries: &[(&str, u8, bool)]) -> Applied {
        Applied {
            touched: entries
                .iter()
                .map(|&(key, gid, deleted)| Touched {
                    collection: "books".into(),
                    key: key.into(),
                    gid: Gid([gid; 16]),
                    deleted,
                })
                .collect(),
            ..Applied::default()
        }
    }

    #[test]
    fn merged_reports_keep_each_entity_that_left_a_key_before_the_one_it_holds() {
        let cases: [(&[_], &[_], &[_]); 3] = [
            (
                &[("[1]", 1, true), ("[1]", 2, false)],
                &[("[1]", 2, false), ("[2]", 3, false)],
                &[("[1]", 1, true), ("[1]", 2, false), ("[2]", 3, false)],
            ),
            (
                &[("[1]", 1, true), ("[1]", 2, false)],
                &[("[1]", 2, true), ("[1]", 1, false)],
                &[("[1]", 2, true), ("[1]", 1, false)],
            ),
            (
                &[("[2]", 3, false), ("[1]", 1, false)],
                &[("[1]", 1, true)],
                &[("[2]", 3, false), ("[1]", 1, true)],
            ),
        ];
        for (first, second, want) in cases {
            let mut report = Applied::default();
            merge_applied(&mut report, touched(first));
            merge_applied(&mut report, touched(second));
            assert_eq!(
                report.touched,
                touched(want).touched,
                "{first:?} then {second:?}"
            );
        }
    }

    fn conflict(gid: u8, local: &str, remote: &str) -> Conflict {
        Conflict {
            collection: "notes".into(),
            gid: Gid([gid; 16]),
            key: None,
            field: "body".into(),
            base: Some(Value::Text("base".into())),
            local: Value::Text(local.into()),
            remote: Value::Text(remote.into()),
            remote_won: true,
        }
    }

    #[test]
    fn a_conflict_reported_again_replaces_only_the_same_local_edit() {
        let cases: [(&[_], &[_], &[_]); 6] = [
            // The same edit, met by a later remote change.
            (&[(1, "m1", "t1")], &[(1, "m1", "t2")], &[(1, "m1", "t2")]),
            // Another local edit of the field, written between the two applies.
            (
                &[(1, "m1", "t1")],
                &[(1, "m2", "t2")],
                &[(1, "m1", "t1"), (1, "m2", "t2")],
            ),
            // Another entity's conflict stays.
            (
                &[(1, "m1", "t1"), (2, "m1", "t1")],
                &[(2, "m1", "t2")],
                &[(1, "m1", "t1"), (2, "m1", "t2")],
            ),
            // Two members of one group, reported under its id with different edits.
            (
                &[(1, "m1", "t1"), (1, "m2", "t1")],
                &[(1, "m1", "t2")],
                &[(1, "m2", "t1"), (1, "m1", "t2")],
            ),
            // Two members with the same edit: each report replaces one.
            (
                &[(1, "m1", "t1"), (1, "m1", "t1")],
                &[(1, "m1", "t2")],
                &[(1, "m1", "t1"), (1, "m1", "t2")],
            ),
            (
                &[(1, "m1", "t1"), (1, "m1", "t1")],
                &[(1, "m1", "t2"), (1, "m1", "t2")],
                &[(1, "m1", "t2"), (1, "m1", "t2")],
            ),
        ];
        let applied = |list: &[(u8, &str, &str)]| Applied {
            conflicts: list
                .iter()
                .map(|&(gid, local, remote)| conflict(gid, local, remote))
                .collect(),
            ..Applied::default()
        };
        for (first, second, want) in cases {
            let mut report = Applied::default();
            merge_applied(&mut report, applied(first));
            merge_applied(&mut report, applied(second));
            assert_eq!(
                report.conflicts,
                applied(want).conflicts,
                "{first:?} then {second:?}"
            );
        }
    }

    #[test]
    fn a_page_must_continue_the_log_one_by_one() {
        let page = |seqs: &[u64], more| BatchPage {
            batches: seqs
                .iter()
                .map(|&seq| StoredBatch {
                    seq,
                    envelope: Vec::new(),
                })
                .collect(),
            more,
        };
        assert!(check_page(&page(&[4, 5, 6], true), 3, 3).is_ok());
        assert!(check_page(&page(&[], false), 3, 3).is_ok());
        for (bad, after) in [
            (page(&[4, 5, 6, 7], false), 3),
            (page(&[], true), 3),
            (page(&[5, 4], false), 3),
            (page(&[4, 6], false), 3),
            (page(&[5], false), 3),
            (page(&[3], false), 3),
            (page(&[1], false), 3),
            (page(&[4, 4], false), 3),
            (page(&[1 << 63], false), (1 << 63) - 1),
        ] {
            assert!(check_page(&bad, after, 3).is_err(), "{bad:?}");
        }
    }
}

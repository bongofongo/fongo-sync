//! The public face of the kernel over SQLite. Every operation locks the connection, runs in one
//! savepoint, and asks `fongo_sync_core` every question whose answer is a decision: this module
//! and its two siblings only keep facts, rows and images, and carry out what the core decides.
//!
//! What the adapter keeps, besides the facts in [`crate::store`]: for each local row the entity
//! it stands for and its image, the values the kernel last wrote to or read from it
//! (`fongo_ids`); the rows the application wrote since the last collect (`fongo_dirty`, filled by
//! triggers); and the entities whose rows wait (`fongo_held`).

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_core::eval::{Evaluator, Origin};
use fongo_sync_core::model::{Row, Schema};
use fongo_sync_core::plan::{self, FieldEdit};
use fongo_sync_core::source::admit;
use fongo_sync_core::{Change, Clock, Gid, Hlc, Value, wins};
use sqlx::{Connection, SqliteConnection};

use crate::adopt;
use crate::collect::{Edits, Minted, Place, last_adopted, marked_at, marked_past, marked_through};
use crate::db::{Db, as_gid, as_int, as_text, loaded, quoted, stored};
use crate::error::{Error, Result};
use crate::image;
use crate::registry::{Collection, Key, Kind, Registry};
use crate::store::{Facts, LAYOUT, SUSPENDED, State, TABLES, as_clock, capture, state};
use crate::tables::{self, NOW, every_row, read_row};

/// What [`Replica::resume`] makes of rows changed while the triggers were suspended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resume {
    /// They are the application's edits, sent like any other.
    Send,
    /// They are this replica's own baseline: not sent, and kept in the row until this replica or
    /// another next writes the field.
    Baseline,
}

/// About how many rows written since the last collect one bounded step collects. On a fast laptop
/// a step of 1,000 rows that depend on no later ones took 65 to 230 ms in a library of tens of
/// thousands of changes, and up to 340 ms in rollback-journal mode and 0.6 s in WAL mode in one of
/// a million, by how wide the rows are; a step takes more rows where they depend on later ones,
/// share a millisecond, or follow rows that cannot be collected yet.
pub const COLLECT_ROWS: usize = 1_000;

/// What one bounded step of collecting did; see [`Replica::collect_step`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Collected {
    /// Changes the step produced.
    pub produced: usize,
    /// Marked rows the step collected.
    pub rows: usize,
    /// Marked rows taken by tries the step gave up and took again, larger, because rows they
    /// took depended on rows past them. Fewer than the marked rows the step took, those it
    /// collected ([`Collected::rows`]) and those among them that wait, so the tries given up cost
    /// no more than the try kept; see [`Replica::collect_step`] for what else a step costs.
    pub abandoned: usize,
    /// Rows written later than those the step took are still to be collected: call
    /// [`Replica::collect_step`] again. False once every row left marked is one the step found
    /// cannot be collected yet, so a caller looping while this holds stops.
    pub more: bool,
    /// The rows the step wrote or removed in bringing rows up to date, as [`Applied::touched`]
    /// lists them: a merge of entities its rows were found to share, a row held back before
    /// that it could write at last, and the like. A row it collected is among them when bringing
    /// rows up to date wrote or removed it, as one a merge gives up; one only collected is not.
    pub touched: Vec<Touched>,
}

/// Local changes waiting to be sent, oldest first.
#[derive(Debug, Clone, Default)]
pub struct Outbox {
    pub changes: Vec<Change>,
    /// Pass to [`Replica::acknowledge`] once `changes` are safely delivered.
    pub through: i64,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Applied {
    /// Incoming changes that replaced what this replica held.
    pub accepted: usize,
    /// Entities held back because a row they need has not arrived or a constraint refused them.
    /// They are retried on every later apply.
    pub waiting: usize,
    /// The rows this apply wrote or removed, by collection in registry order, then by key. A key
    /// whose entity was removed and whose local id went to another lists both, in that order.
    pub touched: Vec<Touched>,
    /// Fields flagged to report conflicts that were edited here and elsewhere concurrently. Every
    /// collect remembers the base of each local edit to such a field, and every apply compares
    /// the changes it receives with every base remembered until [`Replica::release_bases`]
    /// forgets it, whether or not the edit has been sent.
    pub conflicts: Vec<Conflict>,
    /// The entities counted in `waiting`, by collection and id.
    pub held: Vec<Held>,
}

/// An entity whose row could not be written, or a row a merge gave up that another table will
/// not let go of. When the kernel cannot resolve what holds it back, such as a partial unique
/// index another row already satisfies, the application can read the entity's merged values with
/// [`Replica::merged`], change its own rows, and call [`Replica::apply`] with no changes to try
/// again. [`Replica::made`] and [`Replica::written_here`] let one replica alone make that change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Held {
    pub collection: String,
    pub gid: Gid,
    /// The key of the entity's local row, when it has one.
    pub key: Option<String>,
}

/// A local edit not yet sent and a remote edit to the same field, neither made after seeing the
/// other. The field's strategy has already chosen between them; the application may combine the
/// two, for instance by a three-way merge of text, and write the result as a new edit.
#[derive(Debug, Clone, PartialEq)]
pub struct Conflict {
    pub collection: String,
    pub gid: Gid,
    /// The key of the entity's local row, when it has one.
    pub key: Option<String>,
    pub field: String,
    /// The value both edits were made against, unless the field had none.
    pub base: Option<Value>,
    pub local: Value,
    pub remote: Value,
    /// Whether the field now holds the remote edit rather than the local one.
    pub remote_won: bool,
}

/// Which replica made an entity, as [`Replica::made`] answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Made {
    /// This replica.
    Here,
    /// Another replica, or one this replica cannot name yet.
    Elsewhere,
    /// No replica: the making that counts is a row found when its collection was first
    /// installed. It is known by a clock with wall time 0, so a making whose clock reads exactly
    /// 0 is taken for one; a making stamped before the epoch may be, since its replica ticks from
    /// 0 and reads 0 only if its clock has never moved past it.
    Adopted,
}

/// What a replica holds, in a form that does not depend on how it is stored, so tests can compare
/// it with what it should hold. Every list is sorted.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Inspection {
    /// This replica's id: the node of every clock it issues.
    pub node: Option<Gid>,
    /// The winning entry of each field of each entity, `$deleted` included, as (collection, gid,
    /// field, value, clock).
    pub registers: Vec<(String, Gid, String, Value, Hlc)>,
    /// Each entity known to be one with others, as (collection, gid, canonical gid of its group).
    pub aliases: Vec<(String, Gid, Gid)>,
    /// The entity each row of a minted collection holds, as (collection, key, gid).
    pub ids: Vec<(String, String, Gid)>,
    /// Entities whose rows wait, as (collection, gid).
    pub held: Vec<(String, Gid)>,
    /// Rows a merge gave up that another table still holds, as (collection, key).
    pub leftover: Vec<(String, String)>,
    /// Rows written, or removed, since the last collect, and rows a first install in steps found
    /// and has not adopted yet, as (collection, key).
    pub uncollected: Vec<(String, String)>,
    /// Fields whose value the application kept as this replica's own on resuming, as
    /// (collection, key, field).
    pub pinned: Vec<(String, String, String)>,
}

/// A row an apply wrote or removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Touched {
    pub collection: String,
    /// The row's key as the JSON array the change-capture triggers record: `[7]` for a minted
    /// row, the key fields in order for a natural one.
    pub key: String,
    pub gid: Gid,
    pub deleted: bool,
}

/// The most rounds of writing one operation makes; see [`Replica::step`].
pub(crate) const RETRY_ROUNDS: usize = 8;

/// One database's side of a synced library.
#[derive(Debug, Clone)]
pub struct Replica {
    pub(crate) registry: Registry,
    pub(crate) schema: Schema,
}

/// Whether SQLite can store a change: no text or blob in it is longer than its limit. One that
/// is not is ignored, as `source::admit` ignores a change it cannot keep.
fn storable(change: &Change, limit: usize) -> bool {
    let long = match &change.value {
        Value::Text(t) => t.len(),
        Value::Blob(b) => b.len(),
        _ => 0,
    };
    long.max(change.field.len()).max(change.collection.len()) < limit
}

pub(crate) fn same(a: &Value, b: &Value) -> bool {
    a.total_cmp(b) == Ordering::Equal
}

pub(crate) fn meta_get(db: &Db, key: &str) -> Result<Option<Value>> {
    let rows = db.query(
        "SELECT value FROM fongo_meta WHERE key = ?1",
        &[Value::Text(key.into())],
    )?;
    Ok(rows.into_iter().next().and_then(|mut r| r.pop()))
}

pub(crate) fn meta_set(db: &Db, key: &str, value: Value) -> Result<()> {
    db.exec(
        "INSERT INTO fongo_meta (key, value) VALUES (?1, ?2)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        &[Value::Text(key.into()), value],
    )?;
    Ok(())
}

fn node_of(db: &Db) -> Result<Gid> {
    match meta_get(db, "node")? {
        Some(node) => as_gid(&node),
        None => Err(Error::Corrupt("the replica is not installed".into())),
    }
}

fn now_ms(db: &Db) -> Result<u64> {
    let rows = db.query(&format!("SELECT {NOW}"), &[])?;
    Ok(rows
        .first()
        .and_then(|r| match r[0] {
            Value::Int(ms) => u64::try_from(ms).ok(),
            _ => None,
        })
        .unwrap_or(0))
}

/// Drops every object of a type the kernel made: its triggers, or its tables.
fn drop_kernel(db: &Db, kind: &str) -> Result<()> {
    let names = db.query(
        "SELECT name FROM sqlite_master WHERE type = ?1 AND name LIKE 'fongo!_%' ESCAPE '!'",
        &[Value::Text(kind.into())],
    )?;
    for row in names {
        let name = quoted(as_text(&row[0])?);
        db.exec(&format!("DROP {kind} IF EXISTS {name}"), &[])?;
    }
    Ok(())
}

/// Locks the connection and runs `work` on its handle. It pings first: the worker answers only
/// once it has run every earlier command and holds the handle, which it keeps until our unlock
/// arrives, so `lock_handle` cannot find the lock free and leave that unlock behind (see the
/// crate documentation).
async fn with_db<T>(conn: &mut SqliteConnection, work: impl FnOnce(&Db) -> Result<T>) -> Result<T> {
    conn.ping().await?;
    let mut handle = conn.lock_handle().await?;
    // SAFETY: the handle is locked for as long as `db` lives, and nothing else uses it meanwhile.
    let db = unsafe { Db::new(handle.as_raw_handle()) };
    let out = work(&db);
    drop(db);
    out
}

/// Whether the kernel's tables are in the database, at the layout this build uses: `false` when
/// there are none; [`Error::NewerLayout`], [`Error::OlderLayout`] or [`Error::UnknownLayout`] when
/// they are of another layout. A read that writes nothing.
pub async fn installed(conn: &mut SqliteConnection) -> Result<bool> {
    with_db(conn, |db| {
        db.atomically(|db| match state(db)? {
            State::Absent => Ok(false),
            other => other.current().map(|_| true),
        })
    })
    .await
}

/// A local row: what the kernel last knew of it.
pub(crate) struct Mapped {
    pub gid: Gid,
    pub image: Row,
    pub doomed: bool,
}

pub(crate) fn mapping(db: &Db, collection: &str, key: &str) -> Result<Option<Mapped>> {
    let rows = db.query(
        "SELECT gid, image, doomed FROM fongo_ids WHERE collection = ?1 AND key = ?2",
        &[Value::Text(collection.into()), Value::Text(key.into())],
    )?;
    let Some(r) = rows.into_iter().next() else {
        return Ok(None);
    };
    Ok(Some(Mapped {
        gid: as_gid(&r[0])?,
        image: match &r[1] {
            Value::Blob(bytes) => image::decode(bytes)?,
            _ => return Err(Error::Corrupt("an image is not a blob".into())),
        },
        doomed: r[2] == Value::Int(1),
    }))
}

/// The keys of the rows that stand for `gid`, with whether a merge gave each up.
pub(crate) fn rows_of(db: &Db, collection: &str, gid: Gid) -> Result<Vec<(String, bool)>> {
    let mut rows = db
        .query(
            "SELECT key, doomed FROM fongo_ids INDEXED BY fongo_ids_gid
              WHERE collection = ?1 AND gid = ?2",
            &[Value::Text(collection.into()), Value::Ref(gid)],
        )?
        .iter()
        .map(|r| Ok((as_text(&r[0])?.to_string(), r[1] == Value::Int(1))))
        .collect::<Result<Vec<_>>>()?;
    rows.sort();
    Ok(rows)
}

/// Changes recorded, and the entities whose rows they may change.
type Recorded = (Vec<Change>, BTreeSet<(String, Gid)>);

/// Each kept entry of each field of each entity, as an inspection reads them.
type Kept = BTreeMap<(String, Gid), BTreeMap<String, Vec<(Value, Hlc)>>>;

/// State of one operation.
pub(crate) struct Ctx<'d> {
    pub db: &'d Db,
    pub clock: Clock,
    pub node: Gid,
    pub wall: u64,
    pub produced: usize,
    pub accepted: usize,
    /// Rows written or removed, by collection, key and entity: when first reported, and whether
    /// last removed.
    pub touched: BTreeMap<(usize, String, Gid), (usize, bool)>,
    /// What local edits to fields that report conflicts replaced.
    pub bases: Vec<Base>,
    /// Marked rows collected.
    pub collected: usize,
    /// Marked rows taken by tries of a bounded step that were given up.
    pub abandoned: usize,
    /// While a bounded step brings rows up to date, what it reached.
    pub watch: Option<Watch>,
    /// A step adopting found rows from their records: what it writes to a row the application
    /// has written since, it writes only to fields the application left as the image holds them.
    pub adopting: bool,
}

/// What a bounded try must not leave a later write to: the entities whose rows a merge may
/// change (only `merge_unless` names these), and every row settle looked at, some of which may
/// stand for no entity yet.
#[derive(Default)]
pub(crate) struct Watch {
    pub entities: BTreeSet<(String, Gid)>,
    pub keys: BTreeSet<(String, String)>,
}

/// What a try of a bounded step changed in its [`Ctx`], to put back when the try is undone.
struct Saved {
    clock: Clock,
    produced: usize,
    collected: usize,
    accepted: usize,
    touched: BTreeMap<(usize, String, Gid), (usize, bool)>,
    bases: usize,
}

pub(crate) struct Base {
    pub collection: String,
    pub gid: Gid,
    pub key: String,
    pub base: Option<(Value, Hlc)>,
    pub change: Change,
}

impl<'d> Ctx<'d> {
    fn begin(db: &'d Db) -> Result<Ctx<'d>> {
        let node = node_of(db)?;
        let clock = match meta_get(db, "clock")? {
            Some(clock) => Clock::resume(as_clock(&clock)?),
            None => Clock::new(node),
        };
        meta_set(db, "applying", Value::Int(1))?;
        Ok(Ctx {
            db,
            clock,
            node,
            wall: now_ms(db)?,
            produced: 0,
            accepted: 0,
            touched: BTreeMap::new(),
            bases: Vec::new(),
            collected: 0,
            abandoned: 0,
            watch: None,
            adopting: false,
        })
    }

    fn save(&self) -> Saved {
        Saved {
            clock: self.clock.clone(),
            produced: self.produced,
            collected: self.collected,
            accepted: self.accepted,
            touched: self.touched.clone(),
            bases: self.bases.len(),
        }
    }

    fn restore(&mut self, saved: Saved) {
        self.clock = saved.clock;
        self.produced = saved.produced;
        self.collected = saved.collected;
        self.accepted = saved.accepted;
        self.touched = saved.touched;
        self.bases.truncate(saved.bases);
        self.watch = None;
    }

    /// Notes a row a bounded step looked at.
    pub(crate) fn looked_at(&mut self, collection: &str, key: &str) {
        if let Some(watch) = &mut self.watch {
            watch.keys.insert((collection.to_string(), key.to_string()));
        }
    }

    fn finish(&self) -> Result<()> {
        meta_set(self.db, "clock", Value::Text(self.clock.last().encode()))?;
        meta_set(self.db, "applying", Value::Int(0))
    }

    /// The clock of a write found in a row that predates the kernel: older than any a replica
    /// issues, so it fills a gap and never overwrites.
    pub(crate) fn adoption_clock(&mut self) -> Result<Hlc> {
        let seq = match meta_get(self.db, "adopted_seq")? {
            Some(Value::Int(seq)) => u32::try_from(seq).unwrap_or(u32::MAX),
            _ => 0,
        }
        .saturating_add(1);
        meta_set(self.db, "adopted_seq", Value::Int(i64::from(seq)))?;
        Ok(Hlc {
            wall_ms: 0,
            counter: seq,
            node: self.node,
        })
    }

    pub(crate) fn touch(&mut self, idx: usize, key: &str, gid: Gid, deleted: bool) {
        let first = self.touched.len();
        self.touched
            .entry((idx, key.to_string(), gid))
            .or_insert((first, deleted))
            .1 = deleted;
    }
}

impl Replica {
    pub fn new(registry: Registry) -> Replica {
        let schema = registry.model_schema();
        Replica { registry, schema }
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    pub(crate) fn evaluator(&self) -> Evaluator<'_> {
        Evaluator::new(&self.schema).expect("a registry's schema passes its check")
    }

    pub(crate) fn collection(&self, idx: usize) -> &Collection {
        &self.registry.collections()[idx]
    }

    /// The (collection, field) pairs that refer to rows of collection `idx`.
    pub(crate) fn referring(&self, idx: usize) -> Vec<(usize, String)> {
        let name = &self.collection(idx).name;
        let mut out = Vec::new();
        for (i, c) in self.registry.collections().iter().enumerate() {
            for f in &c.fields {
                if matches!(&f.kind, Kind::Ref { collection, .. } if collection == name) {
                    out.push((i, f.name.clone()));
                }
            }
        }
        out
    }

    /// The collection a reference field names, if it is one.
    pub(crate) fn target_of(&self, c: &Collection, field: &str) -> Option<usize> {
        c.fields
            .iter()
            .find(|f| f.name == field)
            .and_then(|f| match &f.kind {
                Kind::Ref { collection, .. } => self.registry.index(collection),
                Kind::Plain => None,
            })
    }

    /// Appends a change this replica made to the outbox.
    pub(crate) fn send(&self, cx: &mut Ctx, change: &Change) -> Result<()> {
        let (value, kind) = stored(&change.value);
        cx.db.exec(
            "INSERT INTO fongo_outbox (collection, gid, field, value, is_ref, clock)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            &[
                Value::Text(change.collection.clone()),
                Value::Ref(change.gid),
                Value::Text(change.field.clone()),
                value,
                kind,
                Value::Text(change.clock.encode()),
            ],
        )?;
        cx.produced += 1;
        Ok(())
    }

    /// Merges a batch into the facts: this replica's own, which it also sends, or one from
    /// elsewhere. Returns the entities whose rows may change, asked of the facts before the merge.
    pub(crate) fn merge(
        &self,
        cx: &mut Ctx,
        changes: &[Change],
        local: bool,
    ) -> Result<BTreeSet<(String, Gid)>> {
        Ok(self
            .merge_unless(cx, changes, local, None)?
            .unwrap_or_default())
    }

    /// [`Replica::merge`], unless an entity the changes affect has a row marked past `place`:
    /// then nothing is merged, and the latest such row's place is returned.
    pub(crate) fn merge_unless(
        &self,
        cx: &mut Ctx,
        changes: &[Change],
        local: bool,
        place: Option<&Place>,
    ) -> Result<std::result::Result<BTreeSet<(String, Gid)>, Place>> {
        let limit = cx.db.length_limit();
        let fits = |c: &&Change| storable(c, limit);
        let kept: Vec<Change>;
        let changes = if changes.iter().all(|c| fits(&c)) {
            changes
        } else {
            kept = changes.iter().filter(fits).cloned().collect();
            &kept
        };
        if changes.is_empty() {
            return Ok(Ok(BTreeSet::new()));
        }
        let ev = self.evaluator();
        let facts = Facts { db: cx.db };
        let affected = ev.affected(&facts, changes);
        cx.db.check()?;
        if let Some(place) = place {
            let named = Watch {
                entities: match &affected {
                    Ok(set) => set.clone(),
                    Err(_) => ev.everything(&facts),
                },
                keys: BTreeSet::new(),
            };
            if let Some(past) = self.marked_beyond(cx.db, &named, place)? {
                return Ok(Err(past));
            }
        }
        for change in changes {
            if let Some(fact) = admit(&self.schema, change)
                && facts.keep(&fact)?
                && !local
            {
                cx.accepted += 1;
            }
        }
        if local {
            for change in changes {
                self.send(cx, change)?;
            }
        }
        let set = match affected {
            Ok(set) => set,
            Err(_) => ev.everything(&facts),
        };
        cx.db.check()?;
        Ok(Ok(set))
    }

    /// Collects local edits, merges `incoming`, and brings every row the core names to its
    /// target. With `take`, collects only about that many of the rows marked, the earliest
    /// written, and returns whether later ones are still marked; see [`Replica::bounded`].
    fn step(
        &self,
        cx: &mut Ctx,
        incoming: &[Change],
        mut set: BTreeSet<(String, Gid)>,
        take: Option<usize>,
    ) -> Result<bool> {
        cx.collected += self.adopt_as_recorded(cx)?;
        if adopt::recorded(cx.db)? {
            // Found rows are adopted before anything written since: a step adopts the next
            // batches; a whole collect or an apply adopts every one first.
            if let Some(want) = take {
                debug_assert!(incoming.is_empty());
                if !set.is_empty() {
                    self.settle(cx, set)?;
                }
                cx.collected += self.adopt(cx, Some(want.max(1)))?;
                return Ok(adopt::recorded(cx.db)? || marked_past_start(cx.db)?);
            }
            cx.collected += self.adopt(cx, None)?;
        }
        if let Some(want) = take {
            debug_assert!(incoming.is_empty());
            return self.bounded(cx, set, want.max(1));
        }
        if let Edits::Recorded(more, rows) = self.collect_edits(cx, None, &mut Minted::new())? {
            set.extend(more);
            cx.collected += rows;
        }
        let limit = cx.db.length_limit();
        for change in incoming {
            if self.registry.index(&change.collection).is_none()
                && change.clock.encodable()
                && storable(change, limit)
            {
                let (value, kind) = stored(&change.value);
                cx.db.exec(
                    "INSERT OR IGNORE INTO fongo_foreign (collection, gid, field, value, is_ref, clock)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    &[
                        Value::Text(change.collection.clone()),
                        Value::Ref(change.gid),
                        Value::Text(change.field.clone()),
                        value,
                        kind,
                        Value::Text(change.clock.encode()),
                    ],
                )?;
            }
            if admit(&self.schema, change).is_some() {
                cx.clock.observe(&change.clock);
            }
        }
        set.extend(self.merge(cx, incoming, false)?);
        self.settle(cx, set)?;
        Ok(false)
    }

    /// Brings every row of `set`'s entities to its target.
    pub(crate) fn settle(&self, cx: &mut Ctx, set: BTreeSet<(String, Gid)>) -> Result<()> {
        // A write a table refuses for a constraint the plan does not know of, such as a unique
        // column outside any identity, may succeed once others are written: what waits is tried
        // again while a round writes anything, up to a bound. What still waits then is tried by
        // the next operation.
        let mut wrote = self.reconcile(cx, set)?;
        for _ in 1..RETRY_ROUNDS {
            if wrote == 0
                || cx
                    .db
                    .query("SELECT 1 FROM fongo_held LIMIT 1", &[])?
                    .is_empty()
            {
                break;
            }
            wrote = self.reconcile(cx, BTreeSet::new())?;
        }
        Ok(())
    }

    /// A step that collects a prefix of the marked rows, in the order they were last written,
    /// of about `want` rows, as a collect made just after the prefix's last write would.
    /// Collecting the rest later must record what collecting everything at once would have, so
    /// the step leaves the rows marked past its prefix, and all that collecting them reads, as
    /// they were. That holds when no row marked past the prefix is one collecting the prefix
    /// reads and could find changed (a row it names, or one naming a row it gives a new entity),
    /// one standing for an entity the prefix's changes affect, or one standing for a member of a
    /// group the step evaluated, or that it looked at, in bringing rows up to date. A prefix that
    /// breaks this is undone and grown until it holds, at worst to every marked row; one always
    /// takes every row found at a first install. Returns whether rows are still marked past it.
    fn bounded(&self, cx: &mut Ctx, set: BTreeSet<(String, Gid)>, want: usize) -> Result<bool> {
        let db = cx.db;
        // Rows found when their collection was first installed are collected together: a write
        // to one before it is collected would make it the application's edit.
        let mut through = self.closed(db, last_adopted(db)?, want)?;
        let wall = cx.wall;
        let start = cx.collected;
        let mut minted = Minted::new();
        loop {
            let Some(place) = through.clone() else {
                cx.wall = wall;
                if let Edits::Recorded(more, rows) = self.collect_edits(cx, None, &mut minted)? {
                    cx.collected += rows;
                    let mut set = set;
                    set.extend(more);
                    self.settle(cx, set)?;
                }
                return Ok(false);
            };
            let saved = cx.save();
            // As a collect made just after the prefix's last write: rows written later that it
            // leaves are stamped after anything it records, as they would be after that collect.
            cx.wall = wall.min(u64::try_from(place.0).unwrap_or(0));
            db.exec("SAVEPOINT fongo_step", &[])?;
            let need = match self.collect_edits(cx, Some(&place), &mut minted)? {
                Edits::Beyond(past) => Some(past),
                Edits::Recorded(more, rows) => {
                    cx.collected += rows;
                    let mut set = set.clone();
                    set.extend(more);
                    cx.watch = Some(Watch::default());
                    self.settle(cx, set)?;
                    let watch = cx.watch.take().unwrap_or_default();
                    self.marked_beyond(db, &watch, &place)?
                }
            };
            let Some(need) = need else {
                db.exec("RELEASE fongo_step", &[])?;
                cx.wall = wall;
                let taken = cx.collected - start;
                let past = marked_past(db, &place)?;
                // What the prefix left marked waits, as it would after a collect. The step goes
                // on, as a collect after this one would, until it has taken what was asked and at
                // least as many rows as wait ahead of them, so that reading those again costs no
                // more than the rows it takes.
                let waiting = usize::try_from(marked_through(db, &Some(place.clone()))?)
                    .unwrap_or(usize::MAX);
                let enough = want.max(waiting);
                if taken >= enough || !past {
                    return Ok(past);
                }
                through = self.closed(db, Some(place), waiting.saturating_add(enough - taken))?;
                continue;
            };
            db.exec("ROLLBACK TO fongo_step", &[])?;
            db.exec("RELEASE fongo_step", &[])?;
            cx.restore(saved);
            // At least twice as many rows each time, and every row once that is more than half
            // of them, so the rows of the tries given up come to fewer than the step takes.
            let rows = marked_through(db, &Some(place.clone()))?;
            cx.abandoned += usize::try_from(rows).unwrap_or(0);
            through = self.closed(
                db,
                Some(need.max(place)),
                usize::try_from(rows.saturating_mul(2)).unwrap_or(usize::MAX),
            )?;
            if through.is_some()
                && marked_through(db, &through)?.saturating_mul(2) > marked_through(db, &None)?
            {
                through = None;
            }
        }
    }

    /// The latest place past `place` of a marked row that `watch` names, or that stands for an
    /// entity it names.
    fn marked_beyond(&self, db: &Db, watch: &Watch, place: &Place) -> Result<Option<Place>> {
        let mut beyond: Option<Place> = None;
        let mut see = |found: Place| {
            if found > *place && beyond.as_ref().is_none_or(|b| *b < found) {
                beyond = Some(found);
            }
        };
        for (collection, gid) in &watch.entities {
            for r in db.query(
                "SELECT d.at, d.key FROM fongo_ids AS i INDEXED BY fongo_ids_gid
                   JOIN fongo_dirty AS d ON d.collection = i.collection AND d.key = i.key
                  WHERE i.collection = ?1 AND i.gid = ?2",
                &[Value::Text(collection.clone()), Value::Ref(*gid)],
            )? {
                see((
                    as_int(&r[0])?,
                    collection.clone(),
                    as_text(&r[1])?.to_string(),
                ));
            }
        }
        for (collection, key) in &watch.keys {
            if let Some((at, _)) = marked_at(db, collection, key)? {
                see((at, collection.clone(), key.clone()));
            }
        }
        Ok(beyond)
    }

    /// Creates the sync tables and the triggers that notice local writes. Safe to run at every
    /// open. Rows that existed before a collection was first installed are queued as the oldest
    /// possible writes, so they fill gaps in a library but never overwrite it. Refuses a
    /// registry whose tables lack a column it names, or whose natural key columns accept NULL,
    /// and kernel tables of a layout it cannot upgrade, which [`Replica::uninstall`] removes;
    /// layout 3 is upgraded in place. Refuses with [`Error::Suspended`] while capture is
    /// suspended: only [`Replica::resume`] puts the triggers back then, having compared the
    /// tables with what the kernel last saw of them.
    pub async fn install(&self, conn: &mut SqliteConnection) -> Result<()> {
        with_db(conn, |db| {
            db.atomically_writing(|db| self.install_in(db, None, None).map(|_| ()))
        })
        .await
    }

    /// [`Replica::install`] and a first step of [`Replica::collect_step`], in one transaction.
    /// What it adopts of the registry for collections installed before (new fields, sets now
    /// written together), and every row that brings up to date, is done whole, as `install`
    /// does it; records an install under another registry left waiting are first adopted whole,
    /// under the registry they were recorded with, as by that install. The rows found in the tables of a collection installed for the first time are
    /// recorded, with their values as they stand, and later steps adopt them from the records;
    /// what the application writes to them meanwhile is collected once every record is adopted,
    /// as an edit made after a whole install. Recording reads every found row in this one
    /// transaction. Where rows written before this install are still to be collected, or an
    /// earlier install's records still wait, the found rows are instead adopted whole by the
    /// next step. A collect or an apply made while records wait adopts the rest first, whole:
    /// a caller that wants the lock held briefly takes steps until [`Collected::more`] is false
    /// before it applies anything. The adoptions are the changes a whole install would send,
    /// with the same gids and clocks, but in the outbox in the order of their batches rather than
    /// of their collections and keys. The design is set out with the kernel's tables, in
    /// `store.rs`.
    pub async fn install_step(
        &self,
        conn: &mut SqliteConnection,
        rows: usize,
    ) -> Result<Collected> {
        with_db(conn, |db| {
            db.atomically_writing(|db| self.install_in(db, None, Some(rows)))
        })
        .await
    }

    /// With `resuming`, marks the rows written while suspended before the step that brings rows
    /// to their targets, which would otherwise overwrite those writes unrecorded. With `take`,
    /// the step is bounded.
    fn install_in(
        &self,
        db: &Db,
        resuming: Option<Resume>,
        take: Option<usize>,
    ) -> Result<Collected> {
        // Only a resume compares the tables whose capture was off with the images; an install
        // would put the triggers back over rows written meanwhile, unrecorded.
        let (laid, lost) = capture(db)?;
        let lost = self.lost(&lost);
        let resuming = match laid {
            State::Current { suspended: true } => Some((resuming.ok_or(Error::Suspended)?, None)),
            State::Current { suspended: false } | State::Older(_) if !lost.is_empty() => {
                Some((resuming.ok_or(Error::Suspended)?, Some(lost)))
            }
            State::Current { suspended: false } | State::Absent | State::Older(_) => None,
            other => {
                other.current()?;
                None
            }
        };
        for sql in TABLES {
            db.exec(sql, &[])?;
        }
        meta_set(db, "layout", Value::Int(LAYOUT))?;
        db.exec(
            "INSERT OR IGNORE INTO fongo_meta (key, value) VALUES ('node', randomblob(16))",
            &[],
        )?;
        tables::columns(db, &self.registry)?;
        drop_kernel(db, "trigger")?;
        for c in self.registry.collections() {
            for sql in tables::triggers(c) {
                db.exec(&sql, &[])?;
            }
        }
        let mut cx = Ctx::begin(db)?;
        let mut set = BTreeSet::new();
        let mut adopted = Vec::new();
        cx.collected += self.adopt_as_recorded(&mut cx)?;
        // An install in steps records the rows it finds, unless rows written before it are still
        // to be collected: a whole install collects those with what it finds.
        // Nor while an earlier install's records wait: its batches are ordered among themselves.
        let record = take.is_some()
            && db
                .query("SELECT 1 FROM fongo_dirty LIMIT 1", &[])?
                .is_empty()
            && !adopt::recorded(db)?;
        let mut to_record = Vec::new();
        for (idx, c) in self.registry.collections().iter().enumerate() {
            let names: Vec<&str> = c.fields.iter().map(|f| f.name.as_str()).collect();
            let fields = Value::Text(names.join(","));
            let together: Vec<String> = c.together.iter().map(|set| set.join("+")).collect();
            let together = Value::Text(together.join(","));
            let known = format!("fields:{}", c.name);
            let sets = format!("together:{}", c.name);
            let adoption = format!("adopted:{}", c.name);
            let was_known = meta_get(db, &known)?;
            let was_together = meta_get(db, &sets)?;
            meta_set(db, &known, fields)?;
            meta_set(db, &sets, together.clone())?;
            if meta_get(db, &adoption)?.is_none() && record {
                to_record.push(idx);
                meta_set(db, &adoption, Value::Int(1))?;
                continue;
            }
            if meta_get(db, &adoption)?.is_none() {
                db.exec(
                    &format!(
                        "INSERT OR IGNORE INTO fongo_dirty (collection, key, at)
                         SELECT ?1, {}, 0 FROM {}",
                        tables::key_json(c, ""),
                        quoted(&c.table)
                    ),
                    &[Value::Text(c.name.clone())],
                )?;
                meta_set(db, &adoption, Value::Int(1))?;
                continue;
            }
            let mut added = Vec::new();
            for f in &c.fields {
                let new = match &was_known {
                    Some(Value::Text(held)) => !held.split(',').any(|h| h == f.name),
                    // A database installed before fields were recorded knew those it holds
                    // values for; any other is new to it.
                    _ => db
                        .query(
                            "SELECT 1 FROM fongo_shadow WHERE collection = ?1 AND field = ?2 LIMIT 1",
                            &[Value::Text(c.name.clone()), Value::Text(f.name.clone())],
                        )?
                        .is_empty(),
                };
                if new {
                    added.push(f.name.clone());
                }
            }
            if !added.is_empty() {
                let (changes, more) = self.adopt_fields(&mut cx, idx, &added)?;
                adopted.extend(changes);
                set.extend(more);
            }
            let regrouped = match was_together {
                Some(was) => was != together,
                None => !c.together.is_empty(),
            };
            if regrouped {
                let (changes, more) = self.complete_sets(&mut cx, idx)?;
                adopted.extend(changes);
                set.extend(more);
            }
        }
        set.extend(self.merge(&mut cx, &adopted, true)?);
        let mut foreign = Vec::new();
        for c in self.registry.collections() {
            let at = [Value::Text(c.name.clone())];
            for r in db.query(
                "SELECT gid, field, value, is_ref, clock FROM fongo_foreign WHERE collection = ?1",
                &at,
            )? {
                foreign.push(Change {
                    collection: c.name.clone(),
                    gid: as_gid(&r[0])?,
                    field: as_text(&r[1])?.to_string(),
                    value: loaded(&r[2], &r[3])?,
                    clock: as_clock(&r[4])?,
                });
            }
            db.exec("DELETE FROM fongo_foreign WHERE collection = ?1", &at)?;
        }
        for change in &foreign {
            if admit(&self.schema, change).is_some() {
                cx.clock.observe(&change.clock);
            }
        }
        set.extend(self.merge(&mut cx, &foreign, false)?);
        if let Some((how, only)) = resuming {
            // What was written while suspended is told apart from images, so every found row
            // gets one first.
            cx.collected += self.adopt(&mut cx, None)?;
            self.resume_in(db, how, only.as_ref())?;
            db.exec(
                "DELETE FROM fongo_meta WHERE key = ?1",
                &[Value::Text(SUSPENDED.into())],
            )?;
        }
        self.record_found(db, &to_record)?;
        let more = self.step(&mut cx, &[], set, take)?;
        cx.finish()?;
        Ok(Collected {
            produced: cx.produced,
            rows: cx.collected,
            abandoned: cx.abandoned,
            more,
            touched: self.touched_rows(cx.touched),
        })
    }

    /// Records the values rows hold for fields new to an adopted collection, as the oldest
    /// possible writes, so they fill gaps but never overwrite what any replica wrote. Entities
    /// that hold values for those fields from elsewhere are brought up to date too.
    fn adopt_fields(&self, cx: &mut Ctx, idx: usize, added: &[String]) -> Result<Recorded> {
        let c = self.collection(idx);
        let db = cx.db;
        let ev = self.evaluator();
        let facts = Facts { db };
        let mut changes = Vec::new();
        let mut set = BTreeSet::new();
        let mapped = db.query(
            "SELECT key, gid FROM fongo_ids WHERE collection = ?1",
            &[Value::Text(c.name.clone())],
        )?;
        for r in mapped {
            let key = as_text(&r[0])?.to_string();
            let gid = as_gid(&r[1])?;
            let (Some(row), Some(mut m)) = (read_row(db, c, &key)?, mapping(db, &c.name, &key)?)
            else {
                continue;
            };
            let mut found = Vec::new();
            // A field in a set is recorded with its set, never alone.
            for f in added.iter().filter(|f| c.together_of(f).is_none()) {
                let value = row.get(f).cloned().unwrap_or(Value::Null);
                m.image.insert(f.clone(), value.clone());
                if value != Value::Null {
                    found.push(FieldEdit::Set(f.clone(), self.shared(db, c, f, &value)?));
                }
            }
            self.write_image(db, &c.name, &key, &m.image)?;
            if found.is_empty() {
                continue;
            }
            let Ok(target) = ev.target(&facts, &c.name, gid) else {
                continue;
            };
            let clock = cx.adoption_clock()?;
            changes.extend(plan::record(
                &self.schema,
                &c.name,
                &found,
                Some(&target),
                None,
                clock,
            ));
            set.insert((c.name.clone(), gid));
        }
        for f in added {
            for r in db.query(
                "SELECT DISTINCT gid FROM fongo_shadow WHERE collection = ?1 AND field = ?2",
                &[Value::Text(c.name.clone()), Value::Text(f.clone())],
            )? {
                set.insert((c.name.clone(), as_gid(&r[0])?));
            }
        }
        db.check()?;
        Ok((changes, set))
    }

    /// After the sets of fields that change together are redrawn, writes made before hold only
    /// some fields of a set, so the set never shows whole. Each row that holds a value for such a
    /// set records the whole set afresh, and the newest of those wins.
    fn complete_sets(&self, cx: &mut Ctx, idx: usize) -> Result<Recorded> {
        let c = self.collection(idx);
        let db = cx.db;
        let ev = self.evaluator();
        let facts = Facts { db };
        let mut changes = Vec::new();
        let mut set = BTreeSet::new();
        let mapped = db.query(
            "SELECT key, gid FROM fongo_ids WHERE collection = ?1",
            &[Value::Text(c.name.clone())],
        )?;
        for r in mapped {
            let key = as_text(&r[0])?.to_string();
            let gid = as_gid(&r[1])?;
            let (Some(row), Some(mut m)) = (read_row(db, c, &key)?, mapping(db, &c.name, &key)?)
            else {
                continue;
            };
            let Ok(target) = ev.target(&facts, &c.name, gid) else {
                continue;
            };
            let members = if target.live.is_empty() {
                target.members.clone()
            } else {
                target.live.clone()
            };
            let mut found = Vec::new();
            for fields in &c.together {
                let mut clocks = Vec::new();
                for f in fields {
                    clocks.push(
                        facts
                            .register(&self.schema, &c.name, &members, f)?
                            .map(|(_, k)| k),
                    );
                }
                let whole = clocks.iter().all(|k| *k == clocks[0]);
                let empty = fields
                    .iter()
                    .all(|f| row.get(f).is_none_or(|v| *v == Value::Null));
                if whole || empty {
                    continue;
                }
                for f in fields {
                    let value = row.get(f).cloned().unwrap_or(Value::Null);
                    m.image.insert(f.clone(), value.clone());
                    found.push(FieldEdit::Set(f.clone(), value));
                }
            }
            if found.is_empty() {
                continue;
            }
            self.write_image(db, &c.name, &key, &m.image)?;
            let clock = cx.clock.tick(cx.wall);
            changes.extend(plan::record(
                &self.schema,
                &c.name,
                &found,
                Some(&target),
                None,
                clock,
            ));
            set.insert((c.name.clone(), gid));
        }
        db.check()?;
        Ok((changes, set))
    }

    pub(crate) fn write_image(
        &self,
        db: &Db,
        collection: &str,
        key: &str,
        image: &Row,
    ) -> Result<()> {
        db.exec(
            "UPDATE fongo_ids SET image = ?3 WHERE collection = ?1 AND key = ?2",
            &[
                Value::Text(collection.into()),
                Value::Text(key.into()),
                Value::Blob(image::encode(image)),
            ],
        )?;
        Ok(())
    }

    /// A field's local value as changes carry it: a reference as the gid of the row it names,
    /// NULL if that row stands for nothing.
    pub(crate) fn shared(
        &self,
        db: &Db,
        c: &Collection,
        field: &str,
        value: &Value,
    ) -> Result<Value> {
        let (Some(target), Value::Int(local)) = (self.target_of(c, field), value) else {
            return Ok(value.clone());
        };
        let name = &self.collection(target).name;
        Ok(mapping(db, name, &tables::minted_key(*local))?
            .map_or(Value::Null, |m| Value::Ref(m.gid)))
    }

    /// Removes the triggers that notice local writes, so an application can change its schema or
    /// rewrite its rows without each write being collected. [`Replica::resume`] puts them back
    /// and says what the rows written meanwhile mean.
    ///
    /// Rows a first install in steps found and has not adopted yet are adopted first, whole,
    /// under the registry they were recorded with, while the tables still match it: a schema
    /// changed while suspended never meets them. Suspending in the middle of such an install
    /// holds the write lock for the rest of its adoption, as long as a whole install of those
    /// rows.
    ///
    /// That capture is suspended is recorded with the triggers' removal, in the same
    /// transaction, and stays recorded until [`Replica::resume`]: meanwhile `install`,
    /// `install_step`, `collect`, `collect_step`, `apply` and `uncollected_rows` refuse with
    /// [`Error::Suspended`], so an application that ends while suspended resumes when it opens
    /// again. Suspending again changes nothing, and with no kernel tables there is nothing to
    /// suspend.
    pub async fn suspend(&self, conn: &mut SqliteConnection) -> Result<()> {
        with_db(conn, |db| {
            db.atomically_writing(|db| {
                match state(db)? {
                    State::Absent => return Ok(()),
                    other if other.current()? => return Ok(()),
                    _ => {}
                }
                if adopt::recorded(db)? {
                    let mut cx = Ctx::begin(db)?;
                    cx.collected += self.adopt_as_recorded(&mut cx)?;
                    cx.collected += self.adopt(&mut cx, None)?;
                    cx.finish()?;
                }
                drop_kernel(db, "trigger")?;
                meta_set(db, SUSPENDED, Value::Int(1))
            })
        })
        .await
    }

    /// Whether capture is suspended, as [`Replica::resume`] defines it: a suspend is recorded,
    /// or a collection that was installed lacks a trigger. Fails as every other call does where
    /// the kernel's tables are missing or of another layout.
    pub async fn suspended(&self, conn: &mut SqliteConnection) -> Result<bool> {
        with_db(conn, |db| {
            db.atomically(|db| {
                let (laid, lost) = capture(db)?;
                Ok(laid.current()? || !self.lost(&lost).is_empty())
            })
        })
        .await
    }

    /// The collections of the registry among `names`.
    fn lost(&self, names: &[String]) -> BTreeSet<usize> {
        names
            .iter()
            .filter_map(|n| self.registry.index(n))
            .collect()
    }

    /// Fails unless the kernel's tables are of this build's layout with capture running: not
    /// suspended, and every collection ever adopted with its triggers in place; or there are
    /// none, which it returns for the caller to decide.
    fn ready(&self, db: &Db) -> Result<State> {
        let (laid, lost) = capture(db)?;
        if laid == State::Absent {
            return Ok(laid);
        }
        if laid.current()? || !self.lost(&lost).is_empty() {
            return Err(Error::Suspended);
        }
        Ok(laid)
    }

    /// Puts the triggers back after [`Replica::suspend`], installing again so that collections
    /// and fields the registry gained meanwhile are adopted, and compares every table with the
    /// rows' images. A row added meanwhile is a new entity and a row removed is deleted,
    /// whichever `how`; a row whose local id was reused meanwhile is the old entity changed. A
    /// row changed meanwhile is, by `how`, an edit to send, or the replica's own baseline.
    ///
    /// Unlike [`Replica::install_step`], it collects every marked row before it returns, in the
    /// transaction that puts the triggers back: a row written back to its image before a later
    /// collect would otherwise lose both writes. It is as long as the rows changed meanwhile are
    /// many; collecting with [`Replica::collect_step`] before suspending leaves it only those.
    ///
    /// Capture is suspended when a suspend is recorded, or when a collection of the registry
    /// that was installed lacks any of its triggers: a build that recorded no suspension left
    /// them dropped, or the application rebuilt the collection's table, which drops its
    /// triggers with it. Without a record, only the tables of those collections are compared.
    /// When capture is intact, `resume` is [`Replica::install`]: rows written since the last
    /// collect are edits like any other, whichever `how`.
    pub async fn resume(&self, conn: &mut SqliteConnection, how: Resume) -> Result<()> {
        with_db(conn, |db| {
            db.atomically_writing(|db| self.install_in(db, Some(how), None).map(|_| ()))
        })
        .await
    }

    /// Compares the tables of the collections in `only`, or of every collection, with the images.
    fn resume_in(&self, db: &Db, how: Resume, only: Option<&BTreeSet<usize>>) -> Result<()> {
        let ev = self.evaluator();
        let facts = Facts { db };
        let mut session = ev.session(&facts);
        let mark = |collection: &str, key: &str, removed: bool| {
            db.exec(
                &format!(
                    "INSERT INTO fongo_dirty (collection, key, at, gone) VALUES (?1, ?2, {NOW}, ?3)
                     ON CONFLICT (collection, key) DO UPDATE SET at = excluded.at,
                        gone = max(gone, excluded.gone)"
                ),
                &[
                    Value::Text(collection.into()),
                    Value::Text(key.into()),
                    Value::Int(if removed { tables::REMOVED } else { 0 }),
                ],
            )
        };
        for (idx, c) in self.registry.collections().iter().enumerate() {
            if only.is_some_and(|only| !only.contains(&idx)) {
                continue;
            }
            let rows: BTreeMap<String, Row> = every_row(db, c)?.into_iter().collect();
            let mapped = db.query(
                "SELECT key, gid, image, doomed FROM fongo_ids WHERE collection = ?1",
                &[Value::Text(c.name.clone())],
            )?;
            let mut known = BTreeSet::new();
            for r in mapped {
                let key = as_text(&r[0])?.to_string();
                known.insert(key.clone());
                let Some(row) = rows.get(&key) else {
                    mark(&c.name, &key, true)?;
                    continue;
                };
                if r[3] == Value::Int(1) {
                    continue;
                }
                let mut image = match &r[2] {
                    Value::Blob(bytes) => image::decode(bytes)?,
                    _ => return Err(Error::Corrupt("an image is not a blob".into())),
                };
                let changed: Vec<&str> = c
                    .fields
                    .iter()
                    .map(|f| f.name.as_str())
                    .filter(|f| !same(&row[*f], image.get(*f).unwrap_or(&Value::Null)))
                    .collect();
                if changed.is_empty() {
                    continue;
                }
                match how {
                    Resume::Send => {
                        mark(&c.name, &key, false)?;
                    }
                    Resume::Baseline => {
                        let target = session.target(&c.name, as_gid(&r[1])?).ok();
                        for f in changed {
                            let against = target
                                .as_ref()
                                .and_then(|t| t.row.as_ref())
                                .and_then(|row| row.get(f))
                                .cloned()
                                .unwrap_or(Value::Null);
                            let (against, kind) = stored(&against);
                            db.exec(
                                "INSERT OR REPLACE INTO fongo_pins (collection, key, field, against, is_ref)
                                 VALUES (?1, ?2, ?3, ?4, ?5)",
                                &[
                                    Value::Text(c.name.clone()),
                                    Value::Text(key.clone()),
                                    Value::Text(f.to_string()),
                                    against,
                                    kind,
                                ],
                            )?;
                            image.insert(f.to_string(), row[f].clone());
                        }
                        self.write_image(db, &c.name, &key, &image)?;
                    }
                }
            }
            for key in rows.keys().filter(|k| !known.contains(*k)) {
                mark(&c.name, key, false)?;
            }
        }
        db.check()
    }

    /// Removes the triggers and every table the kernel keeps, leaving the application's own.
    /// Installing again afterwards is a fresh join: every row becomes a new entity, so syncing
    /// with replicas that knew the old ones gives two copies of each row that has no identity.
    /// Ends a suspension with the rest. Removes tables of an earlier layout too, whatever it
    /// was, but refuses those of a later one ([`Error::NewerLayout`]), which may hold what this
    /// build does not know to remove.
    pub async fn uninstall(&self, conn: &mut SqliteConnection) -> Result<()> {
        with_db(conn, |db| {
            db.atomically_writing(|db| {
                if let State::Newer(found) = state(db)? {
                    return Err(Error::NewerLayout {
                        found,
                        known: LAYOUT,
                    });
                }
                drop_kernel(db, "trigger")?;
                drop_kernel(db, "table")
            })
        })
        .await
    }

    /// Turns the local writes made since the last call into changes in the outbox, and brings
    /// the rows they affect up to date. Returns how many changes were produced.
    pub async fn collect(&self, conn: &mut SqliteConnection) -> Result<usize> {
        with_db(conn, |db| {
            db.atomically_writing(|db| {
                self.ready(db)?.current()?;
                let mut cx = Ctx::begin(db)?;
                self.step(&mut cx, &[], BTreeSet::new(), None)?;
                cx.finish()?;
                Ok(cx.produced)
            })
        })
        .await
    }

    /// [`Replica::collect`] in bounded steps: one transaction collects about `rows` of the rows
    /// written since the last collect, the earliest written, and says whether more are left. A
    /// caller that loops until none are, leaving the database free between calls, holds SQLite's
    /// write lock for one step at a time; see [`COLLECT_ROWS`] for how long one takes.
    ///
    /// A step ends only between two times: the next row marked after it was marked with a later
    /// time. Within one millisecond the order of writing is not known, so rows marked with one
    /// time are taken together: one statement that writes 50,000 rows is one step of 50,000 rows,
    /// as long as one collect of them, whatever `rows` asks.
    ///
    /// What the steps send, hold and show is what some placing of ordinary collects among the
    /// same writes gives: a step leaves the rows past it, and all that collecting them reads, as
    /// they were, and an alias it finds is stamped with the time of the last row it took, or with
    /// the wall clock where that has been set back below it. It is
    /// not, in general, what collects made at those times give: a row written again after a step
    /// is sent as last written, not as it stood. In every case the tests try, once the last step
    /// is done it is also what one
    /// collect of the same rows gives, the same changes in the same order, entities, conflict
    /// bases, rows and images, but for aliases: entities found to be one across two steps are
    /// joined by other pairs of aliases, stamped by their steps, and a reference to one of them
    /// may name another member of the group they join.
    ///
    /// A step takes more than `rows` where rows it takes depend on later ones: a row naming
    /// one made since, made again, or removed, and written again later; rows naming a row the
    /// step makes again; what a removed owner owned, written later; rows found to be one; and a
    /// row made again later at an id named by a row the step writes or removes. Most of these are
    /// found by reading the rows before anything is collected, so a chain of rows each needing a
    /// later one, such as rows each naming the next, is taken as one step as long as the chain.
    /// The rest are found only once the step's changes are merged; the step then gives up what it
    /// did and tries again with at least twice the rows, or with every row once that would be
    /// more than half of them, so the rows of the tries it gives up come to fewer than those it
    /// takes ([`Collected::abandoned`]). At worst a step takes every row. Rows found in a
    /// collection's tables when it is first installed are collected by that install.
    ///
    /// A row that cannot be collected yet, such as one naming a row that has no entity, stays
    /// marked, as it does after a collect, and every later step reads it again. So a step goes on
    /// past such rows until it has collected `rows` rows and at least as many as wait ahead of
    /// them; a step that finds only rows that wait says none are left. The rows marked are
    /// indexed in the order they are collected, so a step reads only the rows it takes. In the
    /// supported shape (see the crate's documentation), in SQLite's work, steps of 100 or 1,000
    /// rows over 100,000 rows written one at a time cost 1.12 times one collect of them, and over
    /// 5,000 rows behind 999 that wait at most 1.7 times, and each step costs at most about twice
    /// what collecting its rows at once does. Two cases cost more. Every try of a step reads the
    /// rows that wait ahead of it again, so rows that wait among many others cost each step more,
    /// by how they are spread: 20,000 rows with 5,000 waiting among them cost from 2.3 to 20 times
    /// one collect in steps of 10 and 1.8 times in steps of 1,000, and 5,000 followed by 20,000
    /// waiting 15 times in steps of 1,000.
    /// And with foreign keys off, a row of a natural collection naming a row that is gone makes
    /// the step read every marked row of its collection, which can cost 12 times one collect.
    ///
    /// Each step is a transaction of its own, committed whole or not at all: after a crash or a
    /// failure between steps, the rows not yet collected are still marked, and the next step or
    /// collect takes them; nothing collected is collected again. Rows written between steps join
    /// those left, as rows written between two collects do. Called with no transaction open, a
    /// step takes the write lock with its first statement, so while another connection writes it
    /// waits in the busy handler; called inside a transaction, it is a savepoint of it.
    pub async fn collect_step(
        &self,
        conn: &mut SqliteConnection,
        rows: usize,
    ) -> Result<Collected> {
        with_db(conn, |db| {
            db.atomically_writing(|db| {
                self.ready(db)?.current()?;
                let mut cx = Ctx::begin(db)?;
                let more = self.step(&mut cx, &[], BTreeSet::new(), Some(rows))?;
                cx.finish()?;
                Ok(Collected {
                    produced: cx.produced,
                    rows: cx.collected,
                    abandoned: cx.abandoned,
                    more,
                    touched: self.touched_rows(cx.touched),
                })
            })
        })
        .await
    }

    /// How many rows are still to be collected, counting at most `at_most`: those marked as
    /// written since the last collect, rows that cannot be collected yet among them, and rows a
    /// first install in steps found and has not adopted yet. A read that writes nothing. 0 with
    /// no kernel tables; refused while capture is suspended, when what was written meanwhile is
    /// not known.
    pub async fn uncollected_rows(
        &self,
        conn: &mut SqliteConnection,
        at_most: usize,
    ) -> Result<usize> {
        with_db(conn, |db| {
            db.atomically(|db| {
                if self.ready(db)? == State::Absent {
                    return Ok(0);
                }
                let limit = i64::try_from(at_most).unwrap_or(i64::MAX);
                let rows = db.query(
                    "SELECT count(*) FROM (SELECT 1 FROM fongo_dirty LIMIT ?1)",
                    &[Value::Int(limit)],
                )?;
                let marked = rows
                    .first()
                    .and_then(|r| as_int(&r[0]).ok())
                    .map_or(0, |n| usize::try_from(n).unwrap_or(0));
                let left = i64::try_from(at_most.saturating_sub(marked)).unwrap_or(i64::MAX);
                Ok(marked + adopt::recorded_rows(db, left)?)
            })
        })
        .await
    }

    pub async fn outbox(&self, conn: &mut SqliteConnection, limit: u32) -> Result<Outbox> {
        with_db(conn, |db| {
            db.atomically(|db| {
                state(db)?.current()?;
                let rows = db.query(
                    "SELECT seq, collection, gid, field, value, is_ref, clock
                     FROM fongo_outbox ORDER BY seq LIMIT ?1",
                    &[Value::Int(i64::from(limit))],
                )?;
                let mut outbox = Outbox::default();
                for r in rows {
                    outbox.through = as_int(&r[0])?;
                    outbox.changes.push(Change {
                        collection: as_text(&r[1])?.to_string(),
                        gid: as_gid(&r[2])?,
                        field: as_text(&r[3])?.to_string(),
                        value: loaded(&r[4], &r[5])?,
                        clock: as_clock(&r[6])?,
                    });
                }
                Ok(outbox)
            })
        })
        .await
    }

    pub async fn acknowledge(&self, conn: &mut SqliteConnection, through: i64) -> Result<()> {
        with_db(conn, |db| {
            db.atomically_writing(|db| {
                state(db)?.current()?;
                db.exec(
                    "DELETE FROM fongo_outbox WHERE seq <= ?1",
                    &[Value::Int(through)],
                )?;
                Ok(())
            })
        })
        .await
    }

    /// Merges changes from other replicas and brings the application's tables in line. Local
    /// writes are collected first, so each has a clock to be compared by.
    pub async fn apply(&self, conn: &mut SqliteConnection, changes: &[Change]) -> Result<Applied> {
        with_db(conn, |db| {
            db.atomically_writing(|db| {
                self.ready(db)?.current()?;
                self.apply_in(db, changes)
            })
        })
        .await
    }

    fn apply_in(&self, db: &Db, changes: &[Change]) -> Result<Applied> {
        let mut cx = Ctx::begin(db)?;
        self.step(&mut cx, changes, BTreeSet::new(), None)?;
        let conflicts = self.conflicts(&mut cx, changes)?;
        cx.finish()?;
        let ev = self.evaluator();
        let facts = Facts { db };
        let mut session = ev.session(&facts);
        let mut held = Vec::new();
        for r in db.query(
            "SELECT collection, gid FROM fongo_held ORDER BY collection, gid",
            &[],
        )? {
            let collection = as_text(&r[0])?.to_string();
            let gid = as_gid(&r[1])?;
            let canonical = session
                .target(&collection, gid)
                .map_or(gid, |t| t.canonical);
            let mut key = rows_of(db, &collection, gid)?
                .into_iter()
                .next()
                .map(|(k, _)| k);
            if key.is_none() && canonical != gid {
                key = rows_of(db, &collection, canonical)?
                    .into_iter()
                    .next()
                    .map(|(k, _)| k);
            }
            let entry = Held {
                collection,
                gid: canonical,
                key,
            };
            if !held.contains(&entry) {
                held.push(entry);
            }
        }
        db.check()?;
        Ok(Applied {
            accepted: cx.accepted,
            waiting: held.len(),
            touched: self.touched_rows(cx.touched),
            conflicts,
            held,
        })
    }

    /// Rows written or removed, by collection in registry order, then by key.
    fn touched_rows(&self, touched: BTreeMap<(usize, String, Gid), (usize, bool)>) -> Vec<Touched> {
        let collections = self.registry.collections();
        let mut rows: Vec<_> = touched.into_iter().collect();
        rows.sort_by(|((a, k, _), (s, _)), ((b, l, _), (t, _))| (a, k, s).cmp(&(b, l, t)));
        rows.into_iter()
            .map(|((idx, key, gid), (_, deleted))| Touched {
                collection: collections[idx].name.clone(),
                key,
                gid,
                deleted,
            })
            .collect()
    }

    /// Remembers what this collect's edits to fields that report conflicts replaced, until
    /// released. A later edit before then keeps the first base, since the other side has seen
    /// neither.
    pub(crate) fn remember_bases(&self, cx: &mut Ctx) -> Result<()> {
        if cx.bases.is_empty() {
            return Ok(());
        }
        let seq = cx
            .db
            .query("SELECT max(seq) FROM fongo_outbox", &[])?
            .first()
            .map(|r| r[0].clone())
            .unwrap_or(Value::Int(0));
        for b in std::mem::take(&mut cx.bases) {
            let (base, base_kind, base_clock) = match &b.base {
                Some((value, clock)) => {
                    let (value, kind) = stored(value);
                    (value, kind, Value::Text(clock.encode()))
                }
                None => (Value::Null, Value::Int(0), Value::Null),
            };
            let (local, local_kind) = stored(&b.change.value);
            cx.db.exec(
                "INSERT INTO fongo_bases (collection, gid, field, key, base, base_is_ref, base_clock,
                                          local, local_is_ref, local_clock, seq)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                 ON CONFLICT (collection, gid, field) DO UPDATE SET
                    key = excluded.key, local = excluded.local, local_is_ref = excluded.local_is_ref,
                    local_clock = excluded.local_clock, seq = excluded.seq",
                &[
                    Value::Text(b.collection.clone()),
                    Value::Ref(b.gid),
                    Value::Text(b.change.field.clone()),
                    Value::Text(b.key.clone()),
                    base,
                    base_kind,
                    base_clock,
                    local,
                    local_kind,
                    Value::Text(b.change.clock.encode()),
                    seq.clone(),
                ],
            )?;
        }
        Ok(())
    }

    /// Pairs each remembered local edit with the latest incoming change to the same field of the
    /// same group that is later than the value the edit replaced, and was made elsewhere.
    fn conflicts(&self, cx: &mut Ctx, changes: &[Change]) -> Result<Vec<Conflict>> {
        let db = cx.db;
        let ev = self.evaluator();
        let facts = Facts { db };
        let mut session = ev.session(&facts);
        let mut latest: BTreeMap<(String, Gid, String), &Change> = BTreeMap::new();
        for change in changes {
            if change.clock.node == cx.node {
                continue;
            }
            let Some(c) = self
                .registry
                .index(&change.collection)
                .map(|i| self.collection(i))
            else {
                continue;
            };
            if !c
                .fields
                .iter()
                .any(|f| f.name == change.field && f.reports_conflicts)
            {
                continue;
            }
            let members = match session.target(&c.name, change.gid) {
                Ok(t) if !t.members.is_empty() => t.members,
                _ => vec![change.gid],
            };
            for member in members {
                let rows = db.query(
                    "SELECT base_clock, local_clock, reported FROM fongo_bases
                     WHERE collection = ?1 AND gid = ?2 AND field = ?3",
                    &[
                        Value::Text(c.name.clone()),
                        Value::Ref(member),
                        Value::Text(change.field.clone()),
                    ],
                )?;
                let Some(r) = rows.first() else {
                    continue;
                };
                let after = |held: &Value| -> Result<bool> {
                    Ok(match held {
                        Value::Null => true,
                        clock => change.clock > as_clock(clock)?,
                    })
                };
                if !after(&r[0])? || as_clock(&r[1])? == change.clock || !after(&r[2])? {
                    continue;
                }
                let slot = (c.name.clone(), member, change.field.clone());
                if latest
                    .get(&slot)
                    .is_none_or(|held| held.clock < change.clock)
                {
                    latest.insert(slot, change);
                }
            }
        }
        let mut conflicts = Vec::new();
        for ((collection, member, field), remote) in latest {
            let at = [
                Value::Text(collection.clone()),
                Value::Ref(member),
                Value::Text(field.clone()),
            ];
            db.exec(
                "UPDATE fongo_bases SET reported = ?4 WHERE collection = ?1 AND gid = ?2 AND field = ?3",
                &[at[0].clone(), at[1].clone(), at[2].clone(), Value::Text(remote.clock.encode())],
            )?;
            let rows = db.query(
                "SELECT base, base_is_ref, local, local_is_ref, local_clock, base_clock
                 FROM fongo_bases WHERE collection = ?1 AND gid = ?2 AND field = ?3",
                &at,
            )?;
            let Some(r) = rows.first() else {
                continue;
            };
            let canonical = session
                .target(&collection, remote.gid)
                .map_or(remote.gid, |t| t.canonical);
            let key = rows_of(db, &collection, canonical)?
                .into_iter()
                .find(|(_, doomed)| !doomed)
                .map(|(k, _)| k);
            let local = loaded(&r[2], &r[3])?;
            let strategy = self
                .schema
                .collection(&collection)
                .and_then(|c| c.fields.iter().find(|f| f.name == field))
                .map_or(fongo_sync_core::Strategy::Lww, |f| f.strategy);
            conflicts.push(Conflict {
                collection,
                gid: canonical,
                key,
                base: match &r[5] {
                    Value::Null => None,
                    _ => Some(loaded(&r[0], &r[1])?),
                },
                remote_won: wins(
                    strategy,
                    Some((&local, &as_clock(&r[4])?)),
                    &remote.value,
                    &remote.clock,
                ),
                field,
                local,
                remote: remote.value.clone(),
            });
        }
        db.check()?;
        Ok(conflicts)
    }

    /// Forgets the bases of local edits sent through outbox sequence `through`. An application
    /// calls it once it has read the shared log past the changes it appended, after which any
    /// change it meets was made knowing those edits, so is not a conflict with them.
    pub async fn release_bases(&self, conn: &mut SqliteConnection, through: i64) -> Result<()> {
        with_db(conn, |db| {
            db.atomically_writing(|db| {
                state(db)?.current()?;
                db.exec(
                    "DELETE FROM fongo_bases WHERE seq <= ?1",
                    &[Value::Int(through)],
                )?;
                Ok(())
            })
        })
        .await
    }

    /// An entity's merged field values, following its alias group, with `$deleted` among them.
    /// Empty when nothing is known of it. Each field is picked on its own, so for a field in a
    /// set that changes together whose latest write has only partly arrived, it can show a value
    /// the row does not: the row, like [`Replica::written_here`], takes the set's latest write.
    pub async fn merged(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        gid: Gid,
    ) -> Result<BTreeMap<String, Value>> {
        with_db(conn, |db| {
            db.atomically(|db| {
                state(db)?.current()?;
                let Some(c) = self.schema.collection(collection) else {
                    return Ok(BTreeMap::new());
                };
                let ev = self.evaluator();
                let facts = Facts { db };
                let Ok(target) = ev.target(&facts, collection, gid) else {
                    return Ok(BTreeMap::new());
                };
                let members = if target.live.is_empty() {
                    target.members.clone()
                } else {
                    target.live.clone()
                };
                let mut out = BTreeMap::new();
                let mut fields: Vec<&str> = c.fields.iter().map(|f| f.name.as_str()).collect();
                fields.push(fongo_sync_core::DELETED);
                for f in fields {
                    if let Some((value, _)) =
                        facts.register(&self.schema, collection, &members, f)?
                    {
                        out.insert(f.to_string(), value);
                    }
                }
                db.check()?;
                Ok(out)
            })
        })
        .await
    }

    /// Which replica made an entity, read from the facts alone, so replicas that hold the same
    /// changes agree and at most one answers [`Made::Here`]. `None` when no member of its group
    /// is live, nothing is known of it, or its group is past what the kernel evaluates.
    ///
    /// Each entity was made by the replica that last inserted a row for it, by clock. A minted
    /// entity gets a row inserted once; a natural key's entity gets one each time a replica
    /// inserts a row at the key, independently or after its removal, so a natural row found
    /// when its collection was first installed and inserted since elsewhere was made by that
    /// replica. A group of entities merged by identity or alias was made by the maker of its
    /// earliest-made live member, whichever row each replica keeps for it. Where that making is
    /// a row found when its collection was first installed, the group was made by no replica:
    /// it is [`Made::Adopted`] on every replica, and [`Replica::written_here`] is the question to
    /// ask of it. Values adopted for a field the registry gains later leave the answer as it was.
    ///
    /// While changes are in flight a replica answers from what it holds: a member whose making
    /// has not arrived makes the answer [`Made::Elsewhere`], and a replica that made a member
    /// answers [`Made::Here`] until the making of an earlier member reaches it, so two replicas
    /// can both answer it until each holds the other's.
    ///
    /// Nothing guarantees that any replica answers [`Made::Here`]. The maker may be gone, may no
    /// longer register the collection, or may have uninstalled and installed again, which gives
    /// it a new id and adopts its rows afresh, so a group with an identity becomes
    /// [`Made::Adopted`]. An application that waits for the maker to act needs its own way on
    /// for a maker that never does.
    ///
    /// Read in one savepoint, so from one state of the database. The savepoint holds a read lock
    /// while it lasts: in rollback-journal mode another connection's write waits for it, or fails
    /// at once with no busy timeout; in WAL mode writers are not blocked.
    pub async fn made(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        gid: Gid,
    ) -> Result<Option<Made>> {
        with_db(conn, |db| {
            db.atomically(|db| {
                state(db)?.current()?;
                let Some(origin) = self.origin_in(db, collection, gid)? else {
                    return Ok(None);
                };
                let node = node_of(db)?;
                Ok(Some(match origin.made {
                    Some(clock) if clock.wall_ms == 0 => Made::Adopted,
                    Some(clock) if clock.node == node => Made::Here,
                    _ => Made::Elsewhere,
                }))
            })
        })
        .await
    }

    /// Whether this replica made the latest of the writes behind the values `fields` of an
    /// entity's group show. Behind each field is its winning entry among the live members by the
    /// field's strategy, or for a field in a set that changes together, the latest write to the
    /// set; the latest of those, by clock, is the one that counts. Asked of one field, it is
    /// whether this replica wrote that field's value. Read from the facts alone, so of replicas
    /// that hold the same changes at most one answers `true`.
    ///
    /// A field no live member has written takes no part. A value adopted when a collection was
    /// first installed takes part like any other, written by the replica that adopted it. A
    /// reference counts the write that set it, whatever the entity it names has become, merged
    /// or given up. `None` when no named field has a write, no member is live, nothing is known
    /// of the entity, or its group is past what the kernel evaluates; [`Error::Registry`] when
    /// the collection declares no field of one of the names.
    ///
    /// A natural key's key fields keep only their earliest few writes
    /// ([`fongo_sync_core::model::KEY_ENTRIES`]), so for a key field the write that counts is the
    /// latest of those that holds the key, which can be an earlier maker than [`Replica::made`]
    /// names.
    ///
    /// While changes are in flight a replica answers from what it holds: two replicas that
    /// wrote named fields concurrently each answer `true` until each holds the other's write.
    ///
    /// The replica that answers `true` wrote from a row it held, but that row need not stand at
    /// the state the group's fields now show: a write it cannot make, such as one a unique index
    /// refuses, is held back, and its row stays at an older state. An application settling a
    /// clash acts on its own row of the entity that gives way, the one [`Replica::key_for_gid`]
    /// names, wherever it stands, and not only on rows it finds standing in the clash. Where a
    /// merge gives its row up, it keeps the group's row; where it removed its row while a member
    /// it had not seen lives on, its writes to the removed one no longer count; an entity whose
    /// owner goes is no longer live, so nothing answers. Beyond that, nothing guarantees the
    /// replica that answers `true` still has a row, or is still there to act.
    ///
    /// Read in one savepoint, so from one state of the database. The savepoint holds a read lock
    /// while it lasts: in rollback-journal mode another connection's write waits for it, or fails
    /// at once with no busy timeout; in WAL mode writers are not blocked.
    pub async fn written_here(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        gid: Gid,
        fields: &[&str],
    ) -> Result<Option<bool>> {
        with_db(conn, |db| {
            db.atomically(|db| {
                state(db)?.current()?;
                if let Some(c) = self.registry.index(collection).map(|i| self.collection(i))
                    && let Some(f) = fields
                        .iter()
                        .find(|f| !c.fields.iter().any(|d| d.name == **f))
                {
                    return Err(Error::Registry(format!(
                        "{collection} declares no field {f}"
                    )));
                }
                let Some(origin) = self.origin_in(db, collection, gid)? else {
                    return Ok(None);
                };
                let node = node_of(db)?;
                let latest = fields.iter().filter_map(|f| origin.fields.get(*f)).max();
                Ok(latest.map(|clock| clock.node == node))
            })
        })
        .await
    }

    fn origin_in(&self, db: &Db, collection: &str, gid: Gid) -> Result<Option<Origin>> {
        if self.registry.index(collection).is_none() {
            return Ok(None);
        }
        let ev = self.evaluator();
        let facts = Facts { db };
        let origin = ev.session(&facts).origin(collection, gid).ok().flatten();
        db.check()?;
        Ok(origin)
    }

    /// The id of the entity a local row holds, by the row's key as the change-capture triggers
    /// write it. The id is the group's own when the entity has been merged with others.
    pub async fn gid_for_key(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        key: &str,
    ) -> Result<Option<Gid>> {
        with_db(conn, |db| {
            db.atomically(|db| self.gid_for_key_in(db, collection, key))
        })
        .await
    }

    fn gid_for_key_in(&self, db: &Db, collection: &str, key: &str) -> Result<Option<Gid>> {
        state(db)?.current()?;
        if self.registry.index(collection).is_none() {
            return Ok(None);
        }
        let Some(m) = mapping(db, collection, key)? else {
            return Ok(None);
        };
        let ev = self.evaluator();
        let facts = Facts { db };
        let gid = ev
            .target(&facts, collection, m.gid)
            .map_or(m.gid, |t| t.canonical);
        db.check()?;
        Ok(Some(gid))
    }

    /// The key of the local row that holds an entity, or any entity merged with it.
    pub async fn key_for_gid(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        gid: Gid,
    ) -> Result<Option<String>> {
        Ok(self.group_of(conn, collection, gid).await?.1)
    }

    /// The id an entity's group is known by, the entity's own when it is merged with none, as
    /// [`Conflict::gid`] gives it; and the key of the local row that holds the entity, or any
    /// entity merged with it, as [`Replica::key_for_gid`] finds it.
    pub async fn group_of(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        gid: Gid,
    ) -> Result<(Gid, Option<String>)> {
        with_db(conn, |db| {
            db.atomically(|db| {
                state(db)?.current()?;
                if self.registry.index(collection).is_none() {
                    return Ok((gid, None));
                }
                let ev = self.evaluator();
                let facts = Facts { db };
                let mut canonical = gid;
                let mut candidates = vec![gid];
                if let Ok(t) = ev.target(&facts, collection, gid) {
                    canonical = t.canonical;
                    candidates.insert(0, t.canonical);
                    candidates.extend(t.members);
                }
                db.check()?;
                for g in candidates {
                    if let Some((key, _)) =
                        rows_of(db, collection, g)?.into_iter().find(|(_, d)| !d)
                    {
                        return Ok((canonical, Some(key)));
                    }
                }
                Ok((canonical, None))
            })
        })
        .await
    }

    /// The global id of a row in a minted collection, once it has been collected or applied.
    pub async fn gid_of(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        local_id: i64,
    ) -> Result<Option<Gid>> {
        let key = tables::minted_key(local_id);
        with_db(conn, |db| {
            db.atomically(|db| self.gid_for_key_in(db, collection, &key))
        })
        .await
    }

    /// Everything this replica holds that decides what its rows show; see [`Inspection`].
    pub async fn inspect(&self, conn: &mut SqliteConnection) -> Result<Inspection> {
        with_db(conn, |db| db.atomically(|db| self.inspect_in(db))).await
    }

    fn inspect_in(&self, db: &Db) -> Result<Inspection> {
        state(db)?.current()?;
        let mut out = Inspection {
            node: meta_get(db, "node")?.map(|n| as_gid(&n)).transpose()?,
            ..Inspection::default()
        };
        let schema = &self.schema;
        let mut home: BTreeMap<Gid, (Hlc, String)> = BTreeMap::new();
        for r in db.query("SELECT gid, collection, first FROM fongo_claims", &[])? {
            let collection = as_text(&r[1])?.to_string();
            if schema.collection(&collection).is_none() {
                continue;
            }
            let claim = (as_clock(&r[2])?, collection);
            let gid = as_gid(&r[0])?;
            if home.get(&gid).is_none_or(|held| claim < *held) {
                home.insert(gid, claim);
            }
        }
        let mut entries: Kept = BTreeMap::new();
        for r in db.query(
            "SELECT collection, gid, field, value, is_ref, clock FROM fongo_shadow",
            &[],
        )? {
            let collection = as_text(&r[0])?.to_string();
            let gid = as_gid(&r[1])?;
            if home.get(&gid).is_none_or(|(_, c)| *c != collection) {
                continue;
            }
            entries
                .entry((collection, gid))
                .or_default()
                .entry(as_text(&r[2])?.to_string())
                .or_default()
                .push((loaded(&r[3], &r[4])?, as_clock(&r[5])?));
        }
        for ((collection, gid), fields) in &entries {
            let key: Vec<String> = match schema.collection(collection).map(|c| &c.identity) {
                Some(fongo_sync_core::model::Identity::Natural(key)) => key.clone(),
                _ => Vec::new(),
            };
            let picked = derived_key(collection, *gid, &key, fields);
            for (field, kept) in fields {
                let entry = match key.iter().position(|k| k == field) {
                    Some(i) => {
                        let value = picked
                            .as_ref()
                            .map(|p| p[i].clone())
                            .unwrap_or_else(|| kept[0].0.clone());
                        let clock = kept
                            .iter()
                            .filter(|(v, _)| same(v, &value))
                            .map(|(_, k)| *k)
                            .max()
                            .unwrap_or(kept[0].1);
                        (value, clock)
                    }
                    None => kept[0].clone(),
                };
                out.registers
                    .push((collection.clone(), *gid, field.clone(), entry.0, entry.1));
            }
        }
        let ev = self.evaluator();
        let facts = Facts { db };
        let mut session = ev.session(&facts);
        let mut grouped = BTreeSet::new();
        for (gid, (_, collection)) in &home {
            if grouped.contains(&(collection.clone(), *gid)) {
                continue;
            }
            let Ok(t) = session.target(collection, *gid) else {
                continue;
            };
            for m in &t.members {
                grouped.insert((collection.clone(), *m));
            }
            if t.members.len() > 1 {
                for m in &t.members {
                    out.aliases.push((collection.clone(), *m, t.canonical));
                }
            }
        }
        for r in db.query(
            "SELECT collection, key, gid, local, doomed FROM fongo_ids ORDER BY collection, key",
            &[],
        )? {
            let collection = as_text(&r[0])?.to_string();
            let key = as_text(&r[1])?.to_string();
            let gid = as_gid(&r[2])?;
            if r[4] == Value::Int(1) {
                out.leftover.push((collection.clone(), key.clone()));
            }
            if matches!(r[3], Value::Int(_)) {
                out.ids.push((collection, key, gid));
            }
        }
        for r in db.query("SELECT collection, gid FROM fongo_held", &[])? {
            let collection = as_text(&r[0])?.to_string();
            let gid = as_gid(&r[1])?;
            let canonical = session
                .target(&collection, gid)
                .map_or(gid, |t| t.canonical);
            out.held.push((collection, canonical));
        }
        let found = if adopt::laid(db)? {
            " UNION SELECT collection, key FROM fongo_found WHERE batch >= 0"
        } else {
            ""
        };
        for r in db.query(
            &format!("SELECT collection, key FROM fongo_dirty{found}"),
            &[],
        )? {
            out.uncollected
                .push((as_text(&r[0])?.to_string(), as_text(&r[1])?.to_string()));
        }
        for r in db.query("SELECT collection, key, field FROM fongo_pins", &[])? {
            out.pinned.push((
                as_text(&r[0])?.to_string(),
                as_text(&r[1])?.to_string(),
                as_text(&r[2])?.to_string(),
            ));
        }
        db.check()?;
        out.registers
            .sort_by(|a, b| (&a.0, a.1, &a.2).cmp(&(&b.0, b.1, &b.2)));
        out.aliases.sort();
        out.ids.sort();
        out.held.sort();
        out.held.dedup();
        out.leftover.sort();
        out.uncollected.sort();
        out.pinned.sort();
        Ok(out)
    }
}

/// The key, one kept value per key field, that derives a natural entity's gid.
fn derived_key(
    collection: &str,
    gid: Gid,
    key: &[String],
    fields: &BTreeMap<String, Vec<(Value, Hlc)>>,
) -> Option<Vec<Value>> {
    if key.is_empty() {
        return None;
    }
    let lists: Vec<Vec<Value>> = key
        .iter()
        .map(|k| {
            fields
                .get(k)
                .map(|e| e.iter().map(|(v, _)| v.clone()).collect())
                .unwrap_or_default()
        })
        .collect();
    let mut at = vec![0usize; lists.len()];
    if lists.iter().any(Vec::is_empty) {
        return None;
    }
    loop {
        let pick: Vec<Value> = lists.iter().zip(&at).map(|(l, i)| l[*i].clone()).collect();
        if Gid::derive(collection, &pick) == gid {
            return Some(pick);
        }
        let mut d = lists.len();
        loop {
            if d == 0 {
                return None;
            }
            d -= 1;
            at[d] += 1;
            if at[d] < lists[d].len() {
                break;
            }
            at[d] = 0;
        }
    }
}

pub(crate) fn key_fields(c: &Collection) -> &[String] {
    match &c.key {
        Key::Natural { fields } => fields,
        Key::Minted { .. } => &[],
    }
}

/// Whether any row is marked.
fn marked_past_start(db: &Db) -> Result<bool> {
    Ok(!db
        .query("SELECT 1 FROM fongo_dirty LIMIT 1", &[])?
        .is_empty())
}

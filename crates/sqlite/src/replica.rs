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

use fongo_sync_core::eval::Evaluator;
use fongo_sync_core::model::{Row, Schema};
use fongo_sync_core::plan::{self, FieldEdit};
use fongo_sync_core::source::admit;
use fongo_sync_core::{Change, Clock, Gid, Hlc, Value, wins};
use sqlx::SqliteConnection;

use crate::db::{Db, as_gid, as_int, as_text, loaded, quoted, stored};
use crate::error::{Error, Result};
use crate::image;
use crate::registry::{Collection, Key, Kind, Registry};
use crate::store::{Facts, LAYOUT, TABLES, as_clock};
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
    /// Fields flagged to report conflicts that were edited here and elsewhere concurrently. Only
    /// a local edit this apply collects is compared, one not yet sent, so a sync that wants these
    /// applies what it receives before it collects and sends.
    pub conflicts: Vec<Conflict>,
    /// The entities counted in `waiting`, by collection and id.
    pub held: Vec<Held>,
}

/// An entity whose row could not be written, or a row a merge gave up that another table will
/// not let go of. When the kernel cannot resolve what holds it back, such as a partial unique
/// index another row already satisfies, the application can read the entity's merged values with
/// [`Replica::merged`], change its own rows, and call [`Replica::apply`] with no changes to try
/// again.
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

/// What a replica holds, in a form that does not depend on how it is stored, so tests can compare
/// it with what it should hold. Every list is sorted.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Inspection {
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
    /// Rows written, or removed, since the last collect, as (collection, key).
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

/// Locks the connection and runs `work` on its handle.
async fn with_db<T>(conn: &mut SqliteConnection, work: impl FnOnce(&Db) -> Result<T>) -> Result<T> {
    let mut handle = conn.lock_handle().await?;
    // SAFETY: the handle is locked for as long as `db` lives, and nothing else uses it meanwhile.
    let db = unsafe { Db::new(handle.as_raw_handle()) };
    let out = work(&db);
    drop(db);
    out
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
        let node = match meta_get(db, "node")? {
            Some(node) => as_gid(&node)?,
            None => return Err(Error::Corrupt("the replica is not installed".into())),
        };
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
        })
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
            return Ok(BTreeSet::new());
        }
        let ev = self.evaluator();
        let facts = Facts { db: cx.db };
        let affected = ev.affected(&facts, changes);
        cx.db.check()?;
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
        Ok(set)
    }

    /// Collects local edits, merges `incoming`, and brings every row the core names to its
    /// target.
    fn step(
        &self,
        cx: &mut Ctx,
        incoming: &[Change],
        mut set: BTreeSet<(String, Gid)>,
    ) -> Result<()> {
        set.extend(self.collect_edits(cx)?);
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

    /// Creates the sync tables and the triggers that notice local writes. Safe to run at every
    /// open. Rows that existed before a collection was first installed are queued as the oldest
    /// possible writes, so they fill gaps in a library but never overwrite it. Refuses a
    /// registry whose tables lack a column it names, or whose natural key columns accept NULL,
    /// and kernel tables of an earlier layout, which [`Replica::uninstall`] removes.
    pub async fn install(&self, conn: &mut SqliteConnection) -> Result<()> {
        with_db(conn, |db| db.atomically(|db| self.install_in(db))).await
    }

    fn install_in(&self, db: &Db) -> Result<()> {
        let installed = !db
            .query(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'fongo_meta'",
                &[],
            )?
            .is_empty();
        if installed {
            match meta_get(db, "layout")? {
                Some(Value::Int(LAYOUT)) => {}
                other => {
                    return Err(Error::Corrupt(format!(
                        "the kernel's tables are of layout {other:?}, not {LAYOUT}; uninstall them first"
                    )));
                }
            }
        }
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
        self.step(&mut cx, &[], set)?;
        cx.finish()
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
    pub async fn suspend(&self, conn: &mut SqliteConnection) -> Result<()> {
        with_db(conn, |db| db.atomically(|db| drop_kernel(db, "trigger"))).await
    }

    /// Puts the triggers back after [`Replica::suspend`], installing again so that collections
    /// and fields the registry gained meanwhile are adopted, and compares every table with the
    /// rows' images. A row added meanwhile is a new entity and a row removed is deleted,
    /// whichever `how`; a row whose local id was reused meanwhile is the old entity changed. A
    /// row changed meanwhile is, by `how`, an edit to send, or the replica's own baseline.
    pub async fn resume(&self, conn: &mut SqliteConnection, how: Resume) -> Result<()> {
        self.install(conn).await?;
        // Collected at once: a row written back to its image before a later collect would
        // otherwise lose both writes.
        with_db(conn, |db| {
            db.atomically(|db| {
                self.resume_in(db, how)?;
                let mut cx = Ctx::begin(db)?;
                self.step(&mut cx, &[], BTreeSet::new())?;
                cx.finish()
            })
        })
        .await
    }

    fn resume_in(&self, db: &Db, how: Resume) -> Result<()> {
        let ev = self.evaluator();
        let facts = Facts { db };
        let mut session = ev.session(&facts);
        let mark = |collection: &str, key: &str, gone: bool| {
            db.exec(
                &format!(
                    "INSERT INTO fongo_dirty (collection, key, at, gone) VALUES (?1, ?2, {NOW}, ?3)
                     ON CONFLICT (collection, key) DO UPDATE SET at = excluded.at,
                        gone = max(gone, excluded.gone)"
                ),
                &[
                    Value::Text(collection.into()),
                    Value::Text(key.into()),
                    Value::Int(i64::from(gone)),
                ],
            )
        };
        for c in self.registry.collections() {
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
    pub async fn uninstall(&self, conn: &mut SqliteConnection) -> Result<()> {
        with_db(conn, |db| {
            db.atomically(|db| {
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
            db.atomically(|db| {
                let mut cx = Ctx::begin(db)?;
                self.step(&mut cx, &[], BTreeSet::new())?;
                cx.finish()?;
                Ok(cx.produced)
            })
        })
        .await
    }

    pub async fn outbox(&self, conn: &mut SqliteConnection, limit: u32) -> Result<Outbox> {
        with_db(conn, |db| {
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
        .await
    }

    pub async fn acknowledge(&self, conn: &mut SqliteConnection, through: i64) -> Result<()> {
        with_db(conn, |db| {
            db.exec(
                "DELETE FROM fongo_outbox WHERE seq <= ?1",
                &[Value::Int(through)],
            )?;
            Ok(())
        })
        .await
    }

    /// Merges changes from other replicas and brings the application's tables in line. Local
    /// writes are collected first, so each has a clock to be compared by.
    pub async fn apply(&self, conn: &mut SqliteConnection, changes: &[Change]) -> Result<Applied> {
        with_db(conn, |db| db.atomically(|db| self.apply_in(db, changes))).await
    }

    fn apply_in(&self, db: &Db, changes: &[Change]) -> Result<Applied> {
        let mut cx = Ctx::begin(db)?;
        self.step(&mut cx, changes, BTreeSet::new())?;
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
        let collections = self.registry.collections();
        let mut rows: Vec<_> = cx.touched.into_iter().collect();
        rows.sort_by(|((a, k, _), (s, _)), ((b, l, _), (t, _))| (a, k, s).cmp(&(b, l, t)));
        Ok(Applied {
            accepted: cx.accepted,
            waiting: held.len(),
            touched: rows
                .into_iter()
                .map(|((idx, key, gid), (_, deleted))| Touched {
                    collection: collections[idx].name.clone(),
                    key,
                    gid,
                    deleted,
                })
                .collect(),
            conflicts,
            held,
        })
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
            db.exec(
                "DELETE FROM fongo_bases WHERE seq <= ?1",
                &[Value::Int(through)],
            )?;
            Ok(())
        })
        .await
    }

    /// An entity's merged field values, following its alias group, with `$deleted` among them.
    /// Empty when nothing is known of it.
    pub async fn merged(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        gid: Gid,
    ) -> Result<BTreeMap<String, Value>> {
        with_db(conn, |db| {
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
                if let Some((value, _)) = facts.register(&self.schema, collection, &members, f)? {
                    out.insert(f.to_string(), value);
                }
            }
            db.check()?;
            Ok(out)
        })
        .await
    }

    /// The id of the entity a local row holds, by the row's key as the change-capture triggers
    /// write it. The id is the group's own when the entity has been merged with others.
    pub async fn gid_for_key(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        key: &str,
    ) -> Result<Option<Gid>> {
        with_db(conn, |db| self.gid_for_key_in(db, collection, key)).await
    }

    fn gid_for_key_in(&self, db: &Db, collection: &str, key: &str) -> Result<Option<Gid>> {
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
        with_db(conn, |db| {
            if self.registry.index(collection).is_none() {
                return Ok(None);
            }
            let ev = self.evaluator();
            let facts = Facts { db };
            let mut candidates = vec![gid];
            if let Ok(t) = ev.target(&facts, collection, gid) {
                candidates.insert(0, t.canonical);
                candidates.extend(t.members);
            }
            db.check()?;
            for g in candidates {
                if let Some((key, _)) = rows_of(db, collection, g)?.into_iter().find(|(_, d)| !d) {
                    return Ok(Some(key));
                }
            }
            Ok(None)
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
        with_db(conn, |db| self.gid_for_key_in(db, collection, &key)).await
    }

    /// Everything this replica holds that decides what its rows show; see [`Inspection`].
    pub async fn inspect(&self, conn: &mut SqliteConnection) -> Result<Inspection> {
        with_db(conn, |db| self.inspect_in(db)).await
    }

    fn inspect_in(&self, db: &Db) -> Result<Inspection> {
        let mut out = Inspection::default();
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
        for r in db.query("SELECT collection, key FROM fongo_dirty", &[])? {
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

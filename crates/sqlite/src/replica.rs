use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_core::{ALIAS, Change, Clock, DELETED, Gid, Hlc, Value, wins};
use sqlx::{Connection, SqliteConnection};

use crate::db::{exec, query};
use crate::error::{Error, Result};
use crate::registry::{Collection, Field, Key, Kind, Registry};

const NOW: &str = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";
const QUIET: &str =
    "WHEN COALESCE((SELECT value FROM fongo_meta WHERE key = 'applying'), '0') <> '1'";

const TABLES: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS fongo_meta (key TEXT PRIMARY KEY, value)",
    "CREATE TABLE IF NOT EXISTS fongo_dirty (
        collection TEXT NOT NULL,
        key TEXT NOT NULL,
        at INTEGER NOT NULL,
        gone INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (collection, key))",
    "CREATE TABLE IF NOT EXISTS fongo_ids (
        collection TEXT NOT NULL,
        key TEXT NOT NULL,
        gid BLOB NOT NULL UNIQUE,
        PRIMARY KEY (collection, key))",
    "CREATE TABLE IF NOT EXISTS fongo_shadow (
        gid BLOB NOT NULL,
        field TEXT NOT NULL,
        collection TEXT NOT NULL,
        value,
        is_ref INTEGER NOT NULL DEFAULT 0,
        clock TEXT NOT NULL,
        PRIMARY KEY (gid, field))",
    "CREATE TABLE IF NOT EXISTS fongo_outbox (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        collection TEXT NOT NULL,
        gid BLOB NOT NULL,
        field TEXT NOT NULL,
        value,
        is_ref INTEGER NOT NULL DEFAULT 0,
        clock TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS fongo_shadow_refs ON fongo_shadow (value) WHERE is_ref = 1",
    "CREATE TABLE IF NOT EXISTS fongo_pending (gid BLOB PRIMARY KEY, collection TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS fongo_alias (gid BLOB PRIMARY KEY, canon BLOB NOT NULL)",
    "CREATE INDEX IF NOT EXISTS fongo_alias_canon ON fongo_alias (canon)",
];

/// Local changes waiting to be sent, oldest first.
#[derive(Debug, Clone, Default)]
pub struct Outbox {
    pub changes: Vec<Change>,
    /// Pass to [`Replica::acknowledge`] once `changes` are safely delivered.
    pub through: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Applied {
    /// Incoming changes that replaced what this replica held.
    pub accepted: usize,
    /// Entities held back because a row they need has not arrived or a constraint refused them.
    /// They are retried on every later apply.
    pub waiting: usize,
    /// The rows this apply wrote or removed, by collection in registry order, then by key.
    pub touched: Vec<Touched>,
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

enum Outcome {
    Settled { deleted: bool },
    Waiting,
}

enum Local {
    Value(Value),
    /// The referenced entity is deleted, so no local row will ever exist for it.
    Gone,
    /// The referenced entity has not arrived yet.
    Unknown,
}

struct Entry {
    idx: usize,
    key: String,
    at: u64,
    gone: bool,
}

type Shadow = BTreeMap<String, (Value, Hlc)>;

/// Every entity known to be the same as one, and their merged state: for each field, the entry
/// its strategy prefers among all of them.
struct Group {
    canon: Gid,
    merged: Shadow,
}

/// State carried through one round of bringing rows in line with the shadow.
struct Pass {
    clock: Clock,
    /// Entities a merge of identities touched, to be brought in line before the round ends.
    more: BTreeSet<(usize, Gid)>,
    rows: Rows,
}

type Rows = BTreeMap<(usize, String), (Gid, bool)>;

impl Pass {
    fn new(clock: Clock) -> Pass {
        Pass {
            clock,
            more: BTreeSet::new(),
            rows: Rows::new(),
        }
    }

    fn touch(&mut self, idx: usize, key: String, gid: Gid, deleted: bool) {
        self.rows.insert((idx, key), (gid, deleted));
    }
}

/// One database's side of a synced library.
#[derive(Debug, Clone)]
pub struct Replica {
    registry: Registry,
}

fn quoted(name: &str) -> String {
    format!("\"{name}\"")
}

fn corrupt<T>(what: &str) -> Result<T> {
    Err(Error::Corrupt(what.into()))
}

fn stored(value: &Value) -> (Value, Value) {
    match value {
        Value::Ref(gid) => (Value::Blob(gid.0.to_vec()), Value::Int(1)),
        other => (other.clone(), Value::Int(0)),
    }
}

fn loaded(value: &Value, is_ref: &Value) -> Result<Value> {
    if *is_ref != Value::Int(1) {
        return Ok(value.clone());
    }
    match value {
        Value::Blob(bytes) => match Gid::from_slice(bytes) {
            Some(gid) => Ok(Value::Ref(gid)),
            None => corrupt("a reference is not 16 bytes"),
        },
        _ => corrupt("a reference is not a blob"),
    }
}

fn as_gid(value: &Value) -> Result<Gid> {
    match value {
        Value::Blob(bytes) => {
            Gid::from_slice(bytes).map_or_else(|| corrupt("a gid is not 16 bytes"), Ok)
        }
        _ => corrupt("a gid is not a blob"),
    }
}

fn as_text(value: &Value) -> Result<&str> {
    match value {
        Value::Text(text) => Ok(text),
        _ => corrupt("expected text"),
    }
}

fn as_clock(value: &Value) -> Result<Hlc> {
    Hlc::decode(as_text(value)?).map_or_else(|| corrupt("a clock does not parse"), Ok)
}

fn is_constraint(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(db) if !matches!(db.kind(), sqlx::error::ErrorKind::Other))
}

fn where_key_json(c: &Collection) -> String {
    c.key_columns()
        .iter()
        .enumerate()
        .map(|(i, column)| format!("{} = json_extract(?1, '$[{i}]')", quoted(column)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn triggers(c: &Collection) -> Vec<String> {
    let name = &c.name;
    let table = quoted(&c.table);
    let key = |row: &str| {
        let parts: Vec<String> = c
            .key_columns()
            .iter()
            .map(|column| format!("{row}.{}", quoted(column)))
            .collect();
        format!("json_array({})", parts.join(", "))
    };
    let (new, old) = (key("NEW"), key("OLD"));
    let touch = format!(
        "INSERT INTO fongo_dirty (collection, key, at) VALUES ('{name}', {new}, {NOW})
         ON CONFLICT (collection, key) DO UPDATE SET at = excluded.at;"
    );
    vec![
        format!("CREATE TRIGGER fongo_{name}_ai AFTER INSERT ON {table} {QUIET} BEGIN {touch} END"),
        format!(
            "CREATE TRIGGER fongo_{name}_au AFTER UPDATE ON {table} {QUIET} BEGIN {touch}
             INSERT INTO fongo_dirty (collection, key, at, gone)
             SELECT '{name}', {old}, {NOW}, 1 WHERE {old} <> {new}
             ON CONFLICT (collection, key) DO UPDATE SET at = excluded.at, gone = 1; END"
        ),
        format!(
            "CREATE TRIGGER fongo_{name}_ad AFTER DELETE ON {table} {QUIET} BEGIN
             INSERT INTO fongo_dirty (collection, key, at, gone) VALUES ('{name}', {old}, {NOW}, 1)
             ON CONFLICT (collection, key) DO UPDATE SET at = excluded.at, gone = 1; END"
        ),
    ]
}

async fn meta_set(conn: &mut SqliteConnection, key: &str, value: Value) -> Result<()> {
    exec(
        conn,
        "INSERT INTO fongo_meta (key, value) VALUES (?, ?)
         ON CONFLICT (key) DO UPDATE SET value = excluded.value",
        &[Value::Text(key.into()), value],
    )
    .await?;
    Ok(())
}

async fn meta_get(conn: &mut SqliteConnection, key: &str) -> Result<Option<Value>> {
    let rows = query(
        conn,
        "SELECT value FROM fongo_meta WHERE key = ?",
        &[Value::Text(key.into())],
    )
    .await?;
    Ok(rows.into_iter().next().and_then(|mut row| row.pop()))
}

async fn load_clock(conn: &mut SqliteConnection) -> Result<Clock> {
    if let Some(clock) = meta_get(conn, "clock").await? {
        return Ok(Clock::resume(as_clock(&clock)?));
    }
    match meta_get(conn, "node").await? {
        Some(node) => Ok(Clock::new(as_gid(&node)?)),
        None => corrupt("the replica is not installed"),
    }
}

async fn save_clock(conn: &mut SqliteConnection, clock: &Clock) -> Result<()> {
    meta_set(conn, "clock", Value::Text(clock.last().encode())).await
}

async fn set_applying(conn: &mut SqliteConnection, on: bool) -> Result<()> {
    meta_set(
        conn,
        "applying",
        Value::Text(if on { "1" } else { "0" }.into()),
    )
    .await
}

async fn shadow_of(conn: &mut SqliteConnection, gid: Gid) -> Result<Shadow> {
    let rows = query(
        conn,
        "SELECT field, value, is_ref, clock FROM fongo_shadow WHERE gid = ?",
        &[Value::Ref(gid)],
    )
    .await?;
    let mut fields = BTreeMap::new();
    for row in rows {
        fields.insert(
            as_text(&row[0])?.to_string(),
            (loaded(&row[1], &row[2])?, as_clock(&row[3])?),
        );
    }
    Ok(fields)
}

async fn shadow_put(conn: &mut SqliteConnection, change: &Change) -> Result<()> {
    let (value, is_ref) = stored(&change.value);
    exec(
        conn,
        "INSERT INTO fongo_shadow (gid, field, collection, value, is_ref, clock)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT (gid, field) DO UPDATE SET
            collection = excluded.collection, value = excluded.value,
            is_ref = excluded.is_ref, clock = excluded.clock",
        &[
            Value::Ref(change.gid),
            Value::Text(change.field.clone()),
            Value::Text(change.collection.clone()),
            value,
            is_ref,
            Value::Text(change.clock.encode()),
        ],
    )
    .await?;
    Ok(())
}

fn is_deleted(shadow: &Shadow) -> bool {
    matches!(shadow.get(DELETED), Some((Value::Int(1), _)))
}

async fn emit(conn: &mut SqliteConnection, change: &Change) -> Result<()> {
    shadow_put(conn, change).await?;
    send(conn, change).await
}

async fn send(conn: &mut SqliteConnection, change: &Change) -> Result<()> {
    let (value, is_ref) = stored(&change.value);
    exec(
        conn,
        "INSERT INTO fongo_outbox (collection, gid, field, value, is_ref, clock)
         VALUES (?, ?, ?, ?, ?, ?)",
        &[
            Value::Text(change.collection.clone()),
            Value::Ref(change.gid),
            Value::Text(change.field.clone()),
            value,
            is_ref,
            Value::Text(change.clock.encode()),
        ],
    )
    .await?;
    Ok(())
}

async fn has_shadow(conn: &mut SqliteConnection, gid: Gid) -> Result<bool> {
    let rows = query(
        conn,
        "SELECT 1 FROM fongo_shadow WHERE gid = ? LIMIT 1",
        &[Value::Ref(gid)],
    )
    .await?;
    Ok(!rows.is_empty())
}

/// The smallest id among those known to be the same entity as `gid`.
async fn canon_of(conn: &mut SqliteConnection, gid: Gid) -> Result<Gid> {
    let rows = query(
        conn,
        "SELECT canon FROM fongo_alias WHERE gid = ?",
        &[Value::Ref(gid)],
    )
    .await?;
    rows.first().map_or(Ok(gid), |row| as_gid(&row[0]))
}

async fn members_of(conn: &mut SqliteConnection, canon: Gid) -> Result<Vec<Gid>> {
    let rows = query(
        conn,
        "SELECT gid FROM fongo_alias WHERE canon = ? ORDER BY gid",
        &[Value::Ref(canon)],
    )
    .await?;
    if rows.is_empty() {
        return Ok(vec![canon]);
    }
    rows.iter().map(|row| as_gid(&row[0])).collect()
}

/// Records that `a` and `b` are one entity. Returns the merged group's id when they were not
/// already one.
async fn union(conn: &mut SqliteConnection, a: Gid, b: Gid) -> Result<Option<Gid>> {
    let (a, b) = (canon_of(conn, a).await?, canon_of(conn, b).await?);
    if a == b {
        return Ok(None);
    }
    let canon = a.min(b);
    let mut all = members_of(conn, a).await?;
    all.extend(members_of(conn, b).await?);
    for gid in all {
        exec(
            conn,
            "INSERT INTO fongo_alias (gid, canon) VALUES (?, ?)
             ON CONFLICT (gid) DO UPDATE SET canon = excluded.canon",
            &[Value::Ref(gid), Value::Ref(canon)],
        )
        .await?;
    }
    Ok(Some(canon))
}

async fn same(conn: &mut SqliteConnection, a: &Value, b: &Value) -> Result<bool> {
    match (a, b) {
        (Value::Ref(x), Value::Ref(y)) if x != y => {
            Ok(canon_of(conn, *x).await? == canon_of(conn, *y).await?)
        }
        _ => Ok(a == b),
    }
}

/// Each value a key part could have been written as: a reference by any member of its group.
async fn spellings(conn: &mut SqliteConnection, value: &Value) -> Result<Vec<Value>> {
    match value {
        Value::Ref(gid) => {
            let canon = canon_of(conn, *gid).await?;
            Ok(members_of(conn, canon)
                .await?
                .into_iter()
                .map(Value::Ref)
                .collect())
        }
        other => Ok(vec![other.clone()]),
    }
}

/// Every way of picking one value from each list.
fn choices(options: &[Vec<Value>]) -> Vec<Vec<Value>> {
    let mut out = vec![Vec::new()];
    for option in options {
        out = out
            .into_iter()
            .flat_map(|prefix| {
                option.iter().map(move |value| {
                    let mut next = prefix.clone();
                    next.push(value.clone());
                    next
                })
            })
            .collect();
    }
    out
}

async fn minted_gid(
    conn: &mut SqliteConnection,
    collection: &str,
    key: &str,
) -> Result<Option<Gid>> {
    let rows = query(
        conn,
        "SELECT gid FROM fongo_ids WHERE collection = ? AND key = ?",
        &[Value::Text(collection.into()), Value::Text(key.into())],
    )
    .await?;
    rows.first().map(|row| as_gid(&row[0])).transpose()
}

/// A local value on its way out: a reference becomes the global id of the row it points at.
async fn to_shared(conn: &mut SqliteConnection, field: &Field, local: Value) -> Result<Value> {
    let Kind::Ref { collection, .. } = &field.kind else {
        return Ok(local);
    };
    if local == Value::Null {
        return Ok(Value::Null);
    }
    let rows = query(
        conn,
        "SELECT gid FROM fongo_ids WHERE collection = ?1 AND key = json_array(?2)",
        &[Value::Text(collection.clone()), local],
    )
    .await?;
    match rows.first() {
        Some(row) => Ok(Value::Ref(as_gid(&row[0])?)),
        None => Ok(Value::Null),
    }
}

/// A shared value on its way in: a reference becomes the local id of the row it names.
async fn to_local(conn: &mut SqliteConnection, field: &Field, shared: &Value) -> Result<Local> {
    match (&field.kind, shared) {
        (Kind::Plain, Value::Ref(_)) => Ok(Local::Value(Value::Null)),
        (Kind::Plain, value) => Ok(Local::Value(value.clone())),
        (Kind::Ref { .. }, Value::Ref(gid)) => {
            let canon = canon_of(conn, *gid).await?;
            let rows = query(
                conn,
                "SELECT json_extract(key, '$[0]') FROM fongo_ids WHERE gid = ?",
                &[Value::Ref(canon)],
            )
            .await?;
            if let Some(row) = rows.into_iter().next() {
                return Ok(Local::Value(row[0].clone()));
            }
            let mut latest: Option<(Value, Hlc)> = None;
            for member in members_of(conn, canon).await? {
                let rows = query(
                    conn,
                    "SELECT value, clock FROM fongo_shadow WHERE gid = ? AND field = ?",
                    &[Value::Ref(member), Value::Text(DELETED.into())],
                )
                .await?;
                if let Some(row) = rows.first() {
                    let clock = as_clock(&row[1])?;
                    if latest.as_ref().is_none_or(|(_, held)| clock > *held) {
                        latest = Some((row[0].clone(), clock));
                    }
                }
            }
            match latest {
                Some((Value::Int(1), _)) => Ok(Local::Gone),
                _ => Ok(Local::Unknown),
            }
        }
        (Kind::Ref { .. }, _) => Ok(Local::Value(Value::Null)),
    }
}

impl Replica {
    pub fn new(registry: Registry) -> Replica {
        Replica { registry }
    }

    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Creates the sync tables and the triggers that notice local writes. Safe to run at every
    /// open. Rows that existed before a collection was first installed are queued as the oldest
    /// possible writes, so they fill gaps in a library but never overwrite it.
    pub async fn install(&self, conn: &mut SqliteConnection) -> Result<()> {
        let mut tx = conn.begin().await?;
        for sql in TABLES {
            exec(&mut tx, sql, &[]).await?;
        }
        exec(
            &mut tx,
            "INSERT OR IGNORE INTO fongo_meta (key, value) VALUES ('node', randomblob(16))",
            &[],
        )
        .await?;
        let existing = query(
            &mut tx,
            "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'fongo!_%' ESCAPE '!'",
            &[],
        )
        .await?;
        for row in existing {
            exec(
                &mut tx,
                &format!("DROP TRIGGER {}", quoted(as_text(&row[0])?)),
                &[],
            )
            .await?;
        }
        for c in self.registry.collections() {
            for sql in triggers(c) {
                exec(&mut tx, &sql, &[]).await?;
            }
            let adopted = format!("adopted:{}", c.name);
            if meta_get(&mut tx, &adopted).await?.is_some() {
                continue;
            }
            let key: Vec<String> = c.key_columns().iter().map(|k| quoted(k)).collect();
            exec(
                &mut tx,
                &format!(
                    "INSERT OR IGNORE INTO fongo_dirty (collection, key, at)
                     SELECT '{}', json_array({}), 0 FROM {}",
                    c.name,
                    key.join(", "),
                    quoted(&c.table)
                ),
                &[],
            )
            .await?;
            meta_set(&mut tx, &adopted, Value::Int(1)).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Turns the local writes made since the last call into changes in the outbox. Returns how
    /// many changes were produced.
    pub async fn collect(&self, conn: &mut SqliteConnection) -> Result<usize> {
        let mut tx = conn.begin().await?;
        let (produced, _) = self.collect_in(&mut tx).await?;
        tx.commit().await?;
        Ok(produced)
    }

    pub async fn outbox(&self, conn: &mut SqliteConnection, limit: u32) -> Result<Outbox> {
        let rows = query(
            conn,
            "SELECT seq, collection, gid, field, value, is_ref, clock
             FROM fongo_outbox ORDER BY seq LIMIT ?",
            &[Value::Int(i64::from(limit))],
        )
        .await?;
        let mut outbox = Outbox::default();
        for row in rows {
            if let Value::Int(seq) = row[0] {
                outbox.through = seq;
            }
            outbox.changes.push(Change {
                collection: as_text(&row[1])?.to_string(),
                gid: as_gid(&row[2])?,
                field: as_text(&row[3])?.to_string(),
                value: loaded(&row[4], &row[5])?,
                clock: as_clock(&row[6])?,
            });
        }
        Ok(outbox)
    }

    pub async fn acknowledge(&self, conn: &mut SqliteConnection, through: i64) -> Result<()> {
        exec(
            conn,
            "DELETE FROM fongo_outbox WHERE seq <= ?",
            &[Value::Int(through)],
        )
        .await?;
        Ok(())
    }

    /// Merges changes from other replicas and brings the application's tables in line. Local
    /// writes are collected first, so each has a clock to be compared by.
    pub async fn apply(&self, conn: &mut SqliteConnection, changes: &[Change]) -> Result<Applied> {
        let mut tx = conn.begin().await?;
        let (_, rows) = self.collect_in(&mut tx).await?;
        let mut pass = Pass::new(load_clock(&mut tx).await?);
        pass.rows = rows;
        set_applying(&mut tx, true).await?;

        let mut applied = Applied::default();
        let mut touched: BTreeSet<(usize, Gid)> = BTreeSet::new();
        for change in changes {
            pass.clock.observe(&change.clock);
            let idx = self.registry.index(&change.collection);
            if change.field == ALIAS {
                // Kept whatever its clock: aliases only accumulate, so none is ever superseded.
                let Value::Ref(other) = &change.value else {
                    continue;
                };
                let joined = match idx {
                    Some(idx) => {
                        self.join(&mut tx, idx, change.gid, *other, &mut pass)
                            .await?
                    }
                    None => union(&mut tx, change.gid, *other).await?.is_some(),
                };
                applied.accepted += usize::from(joined);
                continue;
            }
            let strategy = self.registry.strategy(&change.collection, &change.field);
            let shadow = shadow_of(&mut tx, change.gid).await?;
            let held = shadow.get(&change.field).map(|(v, c)| (v, c));
            if !wins(strategy, held, &change.value, &change.clock) {
                continue;
            }
            shadow_put(&mut tx, change).await?;
            applied.accepted += 1;
            if let Some(idx) = idx {
                touched.insert((idx, change.gid));
                self.settle_natural(&mut tx, idx, change.gid, &mut pass)
                    .await?;
            }
        }
        for row in query(&mut tx, "SELECT gid, collection FROM fongo_pending", &[]).await? {
            if let Some(idx) = self.registry.index(as_text(&row[1])?) {
                touched.insert((idx, as_gid(&row[0])?));
            }
        }

        let waiting = self.settle(&mut tx, touched, &mut pass).await?;
        exec(&mut tx, "DELETE FROM fongo_pending", &[]).await?;
        applied.waiting = self.hold(&mut tx, waiting).await?;
        set_applying(&mut tx, false).await?;
        save_clock(&mut tx, &pass.clock).await?;
        tx.commit().await?;
        let collections = self.registry.collections();
        applied.touched = pass
            .rows
            .into_iter()
            .map(|((idx, key), (gid, deleted))| Touched {
                collection: collections[idx].name.clone(),
                key,
                gid,
                deleted,
            })
            .collect();
        Ok(applied)
    }

    /// The global id of a row in a minted collection, once it has been collected or applied.
    pub async fn gid_of(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        local_id: i64,
    ) -> Result<Option<Gid>> {
        minted_gid(conn, collection, &format!("[{local_id}]")).await
    }

    /// Brings each entity's row in line with its group's merged state, retrying those that wait on
    /// another for as long as any progress is made. Returns those still waiting.
    async fn settle(
        &self,
        conn: &mut SqliteConnection,
        mut round: BTreeSet<(usize, Gid)>,
        pass: &mut Pass,
    ) -> Result<BTreeSet<(usize, Gid)>> {
        round.append(&mut pass.more);
        loop {
            let mut waiting = BTreeSet::new();
            let mut progressed = false;
            for &(idx, gid) in &round {
                match self.materialise(conn, idx, gid, pass).await? {
                    Outcome::Settled { deleted } => {
                        progressed = true;
                        // Before the next insert can be handed a local id the cascade just freed.
                        if deleted {
                            self.forget_cascaded(conn, pass).await?;
                        }
                    }
                    Outcome::Waiting => {
                        waiting.insert((idx, gid));
                    }
                }
            }
            let grew = !pass.more.is_empty();
            waiting.append(&mut pass.more);
            if waiting.is_empty() || !(progressed || grew) {
                return Ok(waiting);
            }
            round = waiting;
        }
    }

    /// Queues entities whose rows could not be written, to be retried by every later apply.
    async fn hold(
        &self,
        conn: &mut SqliteConnection,
        waiting: BTreeSet<(usize, Gid)>,
    ) -> Result<usize> {
        let mut held = 0;
        for (idx, gid) in waiting {
            let gid = canon_of(conn, gid).await?;
            held += exec(
                conn,
                "INSERT OR IGNORE INTO fongo_pending (gid, collection) VALUES (?, ?)",
                &[
                    Value::Ref(gid),
                    Value::Text(self.registry.collections()[idx].name.clone()),
                ],
            )
            .await?;
        }
        Ok(usize::try_from(held).unwrap_or(usize::MAX))
    }

    /// An entity, every other known to be the same, and their merged state.
    async fn group(&self, conn: &mut SqliteConnection, c: &Collection, gid: Gid) -> Result<Group> {
        let canon = canon_of(conn, gid).await?;
        let mut merged = Shadow::new();
        for member in members_of(conn, canon).await? {
            for (field, (value, clock)) in shadow_of(conn, member).await? {
                let strategy = self.registry.strategy(&c.name, &field);
                let held = merged.get(&field).map(|(v, c)| (v, c));
                if wins(strategy, held, &value, &clock) {
                    merged.insert(field, (value, clock));
                }
            }
        }
        Ok(Group { canon, merged })
    }

    /// Makes `a` and `b` one entity, with one row. Rows that referred to the row given up are
    /// pointed at the one kept, and everything that refers to the group is brought in line again.
    async fn join(
        &self,
        conn: &mut SqliteConnection,
        idx: usize,
        a: Gid,
        b: Gid,
        pass: &mut Pass,
    ) -> Result<bool> {
        let Some(canon) = union(conn, a, b).await? else {
            return Ok(false);
        };
        pass.more.insert((idx, canon));
        let c = &self.registry.collections()[idx];
        let Key::Minted { column } = &c.key else {
            return Ok(true);
        };
        let members = members_of(conn, canon).await?;

        let mut rows: Vec<(Gid, String)> = Vec::new();
        for &member in &members {
            let found = query(
                conn,
                "SELECT key FROM fongo_ids WHERE gid = ?",
                &[Value::Ref(member)],
            )
            .await?;
            if let Some(row) = found.first() {
                rows.push((member, as_text(&row[0])?.to_string()));
            }
        }
        rows.sort_by_key(|(gid, _)| (*gid != canon, *gid));
        if let Some(((_, keep), losers)) = rows.split_first() {
            for (_, lose) in losers {
                self.repoint(conn, c, lose, keep, pass).await?;
                exec(
                    conn,
                    &format!(
                        "DELETE FROM {} WHERE {} = json_extract(?1, '$[0]')",
                        quoted(&c.table),
                        quoted(column)
                    ),
                    &[Value::Text(lose.clone())],
                )
                .await?;
                exec(
                    conn,
                    "DELETE FROM fongo_ids WHERE collection = ? AND key = ?",
                    &[Value::Text(c.name.clone()), Value::Text(lose.clone())],
                )
                .await?;
                pass.touch(idx, lose.clone(), canon, true);
            }
            if !losers.is_empty() {
                self.forget_cascaded(conn, pass).await?;
            }
            // Lookups by gid in fongo_ids expect only a group's smallest id there.
            exec(
                conn,
                "UPDATE fongo_ids SET gid = ? WHERE collection = ? AND key = ?",
                &[
                    Value::Ref(canon),
                    Value::Text(c.name.clone()),
                    Value::Text(keep.clone()),
                ],
            )
            .await?;
        }

        for (d_idx, d) in self.registry.collections().iter().enumerate() {
            let Key::Natural { fields } = &d.key else {
                continue;
            };
            for f in d.fields.iter().filter(|f| fields.contains(&f.name)) {
                if !matches!(&f.kind, Kind::Ref { collection, .. } if *collection == c.name) {
                    continue;
                }
                for &member in &members {
                    let keyed = query(
                        conn,
                        "SELECT DISTINCT gid FROM fongo_shadow
                         WHERE is_ref = 1 AND value = ? AND collection = ? AND field = ?",
                        &[
                            Value::Ref(member),
                            Value::Text(d.name.clone()),
                            Value::Text(f.name.clone()),
                        ],
                    )
                    .await?;
                    for row in keyed {
                        self.settle_natural(conn, d_idx, as_gid(&row[0])?, pass)
                            .await?;
                    }
                }
            }
        }
        for &member in &members {
            let referring = query(
                conn,
                "SELECT DISTINCT collection, gid FROM fongo_shadow WHERE is_ref = 1 AND value = ?",
                &[Value::Ref(member)],
            )
            .await?;
            for row in referring {
                if let Some(i) = self.registry.index(as_text(&row[0])?) {
                    pass.more.insert((i, as_gid(&row[1])?));
                }
            }
        }
        Ok(true)
    }

    /// Points every reference to row `lose` of `c` at row `keep`. A natural-key row whose key holds
    /// such a reference is removed instead: its entity is the same as one keyed by `keep`, and is
    /// written again from the merged state of the two.
    async fn repoint(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        lose: &str,
        keep: &str,
        pass: &mut Pass,
    ) -> Result<()> {
        for (d_idx, key, gid) in self.dependents(conn, c, lose).await? {
            pass.touch(d_idx, key, gid, true);
        }
        for d in self.registry.collections() {
            for f in &d.fields {
                if !matches!(&f.kind, Kind::Ref { collection, .. } if *collection == c.name) {
                    continue;
                }
                let (table, column) = (quoted(&d.table), quoted(&f.name));
                if matches!(&d.key, Key::Natural { fields } if fields.contains(&f.name)) {
                    exec(
                        conn,
                        &format!("DELETE FROM {table} WHERE {column} = json_extract(?1, '$[0]')"),
                        &[Value::Text(lose.into())],
                    )
                    .await?;
                } else {
                    exec(
                        conn,
                        &format!(
                            "UPDATE OR IGNORE {table} SET {column} = json_extract(?2, '$[0]')
                             WHERE {column} = json_extract(?1, '$[0]')"
                        ),
                        &[Value::Text(lose.into()), Value::Text(keep.into())],
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    /// Joins a natural-key entity with every other whose key names the same entities, which it
    /// can once a reference in the key is to a merged group.
    async fn settle_natural(
        &self,
        conn: &mut SqliteConnection,
        idx: usize,
        gid: Gid,
        pass: &mut Pass,
    ) -> Result<()> {
        let c = &self.registry.collections()[idx];
        let Key::Natural { fields } = &c.key else {
            return Ok(());
        };
        let refers = c
            .fields
            .iter()
            .any(|f| fields.contains(&f.name) && matches!(f.kind, Kind::Ref { .. }));
        if !refers {
            return Ok(());
        }
        let shadow = shadow_of(conn, gid).await?;
        let mut options = Vec::with_capacity(fields.len());
        for name in fields {
            let Some((value, _)) = shadow.get(name) else {
                return Ok(());
            };
            options.push(spellings(conn, value).await?);
        }
        for key in choices(&options) {
            let other = Gid::derive(&c.name, &key);
            if other != gid
                && has_shadow(conn, other).await?
                && let Some(canon) = union(conn, gid, other).await?
            {
                pass.more.insert((idx, canon));
            }
        }
        Ok(())
    }

    /// Looks for another row holding this entity's identity. When that row's group holds the same
    /// identity in its merged state, returns the group. A row that is merely out of date gives the
    /// identity up and is brought in line later in the round.
    async fn clash(
        &self,
        conn: &mut SqliteConnection,
        idx: usize,
        canon: Gid,
        columns: &[(&Field, Value)],
        pass: &mut Pass,
    ) -> Result<Option<Gid>> {
        let c = &self.registry.collections()[idx];
        let Key::Minted { column } = &c.key else {
            return Ok(None);
        };
        for identity in &c.identity {
            let mut values = Vec::with_capacity(identity.len());
            for name in identity {
                match columns.iter().find(|(f, _)| f.name == *name) {
                    Some((_, value)) if *value != Value::Null => values.push(value.clone()),
                    _ => break,
                }
            }
            if values.len() != identity.len() {
                continue;
            }
            let matches: Vec<String> = identity
                .iter()
                .map(|f| format!("{} = ?", quoted(f)))
                .collect();
            let rows = query(
                conn,
                &format!(
                    "SELECT json_array({}) FROM {} WHERE {}",
                    quoted(column),
                    quoted(&c.table),
                    matches.join(" AND ")
                ),
                &values,
            )
            .await?;
            for row in rows {
                let key = as_text(&row[0])?.to_string();
                let Some(other) = minted_gid(conn, &c.name, &key).await? else {
                    continue;
                };
                let other = canon_of(conn, other).await?;
                if other == canon {
                    continue;
                }
                let theirs = self.group(conn, c, other).await?;
                if !is_deleted(&theirs.merged) {
                    let mut held = Vec::with_capacity(identity.len());
                    for name in identity {
                        let local = match (theirs.merged.get(name), c.field_index(name)) {
                            (Some((shared, _)), Some(i)) => {
                                match to_local(conn, &c.fields[i], shared).await? {
                                    Local::Value(value) => value,
                                    Local::Gone | Local::Unknown => Value::Null,
                                }
                            }
                            _ => Value::Null,
                        };
                        held.push(local);
                    }
                    if held == values {
                        return Ok(Some(other));
                    }
                }
                let cleared: Vec<String> = identity
                    .iter()
                    .map(|f| format!("{} = NULL", quoted(f)))
                    .collect();
                let cleared = exec(
                    conn,
                    &format!(
                        "UPDATE OR IGNORE {} SET {} WHERE {} = json_extract(?1, '$[0]')",
                        quoted(&c.table),
                        cleared.join(", "),
                        quoted(column)
                    ),
                    &[Value::Text(key.clone())],
                )
                .await?;
                if cleared > 0 {
                    pass.touch(idx, key, other, false);
                }
                pass.more.insert((idx, other));
            }
        }
        Ok(None)
    }

    async fn read_row(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        key: &str,
    ) -> Result<Option<Vec<Value>>> {
        let columns: Vec<String> = if c.fields.is_empty() {
            vec!["NULL".into()]
        } else {
            c.fields.iter().map(|f| quoted(&f.name)).collect()
        };
        let rows = query(
            conn,
            &format!(
                "SELECT {} FROM {} WHERE {}",
                columns.join(", "),
                quoted(&c.table),
                where_key_json(c)
            ),
            &[Value::Text(key.into())],
        )
        .await?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        if c.fields.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let mut shared = Vec::with_capacity(row.len());
        for (field, value) in c.fields.iter().zip(row) {
            shared.push(to_shared(conn, field, value).await?);
        }
        Ok(Some(shared))
    }

    /// The id of a natural-key entity from the shared values of its fields, or `None` when a key
    /// part refers to a row this replica cannot name.
    fn natural_gid(c: &Collection, key_fields: &[String], values: &[Value]) -> Option<Gid> {
        let mut key = Vec::with_capacity(key_fields.len());
        for name in key_fields {
            let value = values.get(c.field_index(name)?)?;
            if *value == Value::Null {
                return None;
            }
            key.push(value.clone());
        }
        Some(Gid::derive(&c.name, &key))
    }

    async fn gone_natural_gid(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        key_fields: &[String],
        key: &str,
    ) -> Result<Option<Gid>> {
        let mut values = vec![Value::Null; c.fields.len()];
        for (i, name) in key_fields.iter().enumerate() {
            let Some(index) = c.field_index(name) else {
                return Ok(None);
            };
            let rows = query(
                conn,
                &format!("SELECT json_extract(?1, '$[{i}]')"),
                &[Value::Text(key.into())],
            )
            .await?;
            let local = rows
                .into_iter()
                .next()
                .and_then(|mut r| r.pop())
                .unwrap_or(Value::Null);
            values[index] = to_shared(conn, &c.fields[index], local).await?;
        }
        self.resolve_natural(conn, c, key_fields, &values).await
    }

    /// The id a natural-key row's entity is known by. Another replica may have named a reference
    /// in the key by another member of its group, so each such spelling of the key is tried.
    async fn resolve_natural(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        key_fields: &[String],
        values: &[Value],
    ) -> Result<Option<Gid>> {
        let Some(derived) = Self::natural_gid(c, key_fields, values) else {
            return Ok(None);
        };
        if has_shadow(conn, derived).await? {
            return Ok(Some(canon_of(conn, derived).await?));
        }
        let mut options = Vec::with_capacity(key_fields.len());
        for name in key_fields {
            let value = c
                .field_index(name)
                .and_then(|i| values.get(i))
                .unwrap_or(&Value::Null);
            options.push(spellings(conn, value).await?);
        }
        for key in choices(&options) {
            let gid = Gid::derive(&c.name, &key);
            if has_shadow(conn, gid).await? {
                return Ok(Some(canon_of(conn, gid).await?));
            }
        }
        Ok(Some(derived))
    }

    /// Returns how many changes were produced, and the rows written while putting back local
    /// writes their field's strategy refused.
    async fn collect_in(&self, conn: &mut SqliteConnection) -> Result<(usize, Rows)> {
        // A write first, so the transaction holds the write lock before it reads what to do.
        exec(
            conn,
            "UPDATE fongo_meta SET value = value WHERE key = 'node'",
            &[],
        )
        .await?;
        let rows = query(
            conn,
            "SELECT collection, key, at, gone FROM fongo_dirty ORDER BY at, collection, key",
            &[],
        )
        .await?;
        if rows.is_empty() {
            return Ok((0, Rows::new()));
        }
        let mut entries = Vec::new();
        for row in &rows {
            let Some(idx) = self.registry.index(as_text(&row[0])?) else {
                continue;
            };
            entries.push(Entry {
                idx,
                key: as_text(&row[1])?.to_string(),
                at: match row[2] {
                    Value::Int(at) => u64::try_from(at).unwrap_or(0),
                    _ => 0,
                },
                gone: row[3] == Value::Int(1),
            });
        }
        let collections = self.registry.collections();
        let mut pass = Pass::new(load_clock(conn).await?);

        // Natural ids of rows that went away are derived before any minted id below is retired.
        let mut tombstones: Vec<(usize, Gid, u64)> = Vec::new();
        for e in entries.iter().filter(|e| e.gone) {
            let c = &collections[e.idx];
            if let Key::Natural { fields } = &c.key
                && let Some(gid) = self.gone_natural_gid(conn, c, fields, &e.key).await?
            {
                tombstones.push((e.idx, gid, e.at));
            }
        }

        for e in &entries {
            let c = &collections[e.idx];
            if !matches!(c.key, Key::Minted { .. }) {
                continue;
            }
            let mut mapped = minted_gid(conn, &c.name, &e.key).await?;
            if e.gone
                && let Some(gid) = mapped.take()
            {
                tombstones.push((e.idx, gid, e.at));
                exec(
                    conn,
                    "DELETE FROM fongo_ids WHERE gid = ?",
                    &[Value::Ref(gid)],
                )
                .await?;
            }
            if mapped.is_none() && self.read_row(conn, c, &e.key).await?.is_some() {
                exec(
                    conn,
                    "INSERT INTO fongo_ids (collection, key, gid) VALUES (?, ?, randomblob(16))",
                    &[Value::Text(c.name.clone()), Value::Text(e.key.clone())],
                )
                .await?;
            }
        }

        let mut produced = 0;
        let mut present: BTreeSet<Gid> = BTreeSet::new();
        let mut overruled: BTreeSet<(usize, Gid)> = BTreeSet::new();
        for e in &entries {
            let c = &collections[e.idx];
            let Some(values) = self.read_row(conn, c, &e.key).await? else {
                continue;
            };
            let gid = match &c.key {
                Key::Minted { .. } => minted_gid(conn, &c.name, &e.key).await?,
                Key::Natural { fields } => self.resolve_natural(conn, c, fields, &values).await?,
            };
            let Some(gid) = gid else { continue };
            let Group { canon: gid, merged } = self.group(conn, c, gid).await?;
            present.insert(gid);
            let mut changes = Vec::new();
            if merged.is_empty() || is_deleted(&merged) {
                changes.push((DELETED.to_string(), Value::Int(0)));
            }
            for (field, value) in c.fields.iter().zip(values) {
                let unchanged = match merged.get(&field.name) {
                    Some((held, _)) => same(conn, held, &value).await?,
                    None => value == Value::Null,
                };
                if !unchanged {
                    changes.push((field.name.clone(), value));
                }
            }
            for (field, value) in changes {
                let stamp = pass.clock.tick(e.at);
                let strategy = self.registry.strategy(&c.name, &field);
                let held = merged.get(&field).map(|(v, c)| (v, c));
                if field != DELETED && !wins(strategy, held, &value, &stamp) {
                    overruled.insert((e.idx, gid));
                    continue;
                }
                emit(
                    conn,
                    &Change {
                        collection: c.name.clone(),
                        gid,
                        field,
                        value,
                        clock: stamp,
                    },
                )
                .await?;
                produced += 1;
            }
        }

        for (idx, gid, at) in tombstones {
            let c = &collections[idx];
            let Group { canon, merged } = self.group(conn, c, gid).await?;
            if present.contains(&canon) || merged.is_empty() || is_deleted(&merged) {
                continue;
            }
            emit(
                conn,
                &Change {
                    collection: c.name.clone(),
                    gid: canon,
                    field: DELETED.into(),
                    value: Value::Int(1),
                    clock: pass.clock.tick(at),
                },
            )
            .await?;
            produced += 1;
        }

        exec(conn, "DELETE FROM fongo_dirty", &[]).await?;
        if !overruled.is_empty() {
            // A local write its field's strategy refused: the row goes back to the merged value.
            set_applying(conn, true).await?;
            let waiting = self.settle(conn, overruled, &mut pass).await?;
            self.hold(conn, waiting).await?;
            set_applying(conn, false).await?;
        }
        save_clock(conn, &pass.clock).await?;
        Ok((produced, pass.rows))
    }

    /// Makes one entity's row match the merged state of its group.
    async fn materialise(
        &self,
        conn: &mut SqliteConnection,
        idx: usize,
        gid: Gid,
        pass: &mut Pass,
    ) -> Result<Outcome> {
        let c = &self.registry.collections()[idx];
        loop {
            let Group { canon, merged } = self.group(conn, c, gid).await?;
            if merged.is_empty() {
                return Ok(Outcome::Settled { deleted: false });
            }
            let deleted = is_deleted(&merged);
            let table = quoted(&c.table);

            let mut columns: Vec<(&Field, Value)> = Vec::new();
            let mut unresolved = false;
            let mut orphaned = false;
            for field in &c.fields {
                let Some((shared, _)) = merged.get(&field.name) else {
                    continue;
                };
                let owning = matches!(&field.kind, Kind::Ref { owned: true, .. })
                    || matches!(&c.key, Key::Natural { fields } if fields.contains(&field.name));
                match to_local(conn, field, shared).await? {
                    Local::Value(value) => columns.push((field, value)),
                    Local::Gone => {
                        orphaned |= owning;
                        columns.push((field, Value::Null));
                    }
                    Local::Unknown => {
                        unresolved = true;
                        columns.push((field, Value::Null));
                    }
                }
            }

            // A row whose owner is deleted is deleted too, whether or not this database cascades.
            let deleted = deleted || orphaned;

            if !deleted && let Some(other) = self.clash(conn, idx, canon, &columns, pass).await? {
                send(
                    conn,
                    &Change {
                        collection: c.name.clone(),
                        gid: canon,
                        field: ALIAS.into(),
                        value: Value::Ref(other),
                        clock: pass.clock.tick(0),
                    },
                )
                .await?;
                self.join(conn, idx, canon, other, pass).await?;
                continue;
            }

            let mut written_key: Option<String> = None;
            let written = match &c.key {
                Key::Minted { column } => {
                    let rows = query(
                        conn,
                        "SELECT key FROM fongo_ids WHERE gid = ?",
                        &[Value::Ref(canon)],
                    )
                    .await?;
                    let key = rows.first().map(|row| as_text(&row[0])).transpose()?;
                    let key = key.map(str::to_string);
                    if deleted {
                        if let Some(key) = key {
                            let doomed = self.dependents(conn, c, &key).await?;
                            let removed = exec(
                                conn,
                                &format!(
                                    "DELETE FROM {table} WHERE {} = json_extract(?1, '$[0]')",
                                    quoted(column)
                                ),
                                &[Value::Text(key.clone())],
                            )
                            .await?;
                            exec(
                                conn,
                                "DELETE FROM fongo_ids WHERE gid = ?",
                                &[Value::Ref(canon)],
                            )
                            .await?;
                            if removed > 0 {
                                pass.touch(idx, key, canon, true);
                            }
                            for (d_idx, d_key, d_gid) in doomed {
                                let d = &self.registry.collections()[d_idx];
                                if !self.row_exists(conn, d, &d_key).await? {
                                    pass.touch(d_idx, d_key, d_gid, true);
                                }
                            }
                        }
                        return Ok(Outcome::Settled { deleted: true });
                    }
                    match key {
                        Some(_) if columns.is_empty() => Ok(0),
                        Some(key) => {
                            written_key = Some(key.clone());
                            let sets: Vec<String> = columns
                                .iter()
                                .map(|(f, _)| format!("{} = ?", quoted(&f.name)))
                                .collect();
                            let mut params: Vec<Value> =
                                columns.iter().map(|(_, v)| v.clone()).collect();
                            params.push(Value::Text(key));
                            exec(
                                conn,
                                &format!(
                                    "UPDATE {table} SET {} WHERE {} = json_extract(?, '$[0]')",
                                    sets.join(", "),
                                    quoted(column)
                                ),
                                &params,
                            )
                            .await
                        }
                        None => {
                            let inserted = self.insert(conn, c, &columns).await;
                            if inserted.is_ok() {
                                let key =
                                    query(conn, "SELECT json_array(last_insert_rowid())", &[])
                                        .await?
                                        .first()
                                        .map(|row| as_text(&row[0]).map(str::to_string))
                                        .transpose()?;
                                if let Some(key) = &key {
                                    exec(
                                        conn,
                                        "INSERT INTO fongo_ids (collection, key, gid) VALUES (?, ?, ?)",
                                        &[
                                            Value::Text(c.name.clone()),
                                            Value::Text(key.clone()),
                                            Value::Ref(canon),
                                        ],
                                    )
                                    .await?;
                                }
                                written_key = key;
                            }
                            inserted
                        }
                    }
                }
                Key::Natural { fields } => {
                    let mut key = Vec::new();
                    for name in fields {
                        match columns.iter().find(|(f, _)| f.name == *name) {
                            Some((_, value)) if *value != Value::Null => key.push(value.clone()),
                            _ if deleted => return Ok(Outcome::Settled { deleted: false }),
                            _ => return Ok(Outcome::Waiting),
                        }
                    }
                    let matches: Vec<String> = fields
                        .iter()
                        .map(|k| format!("{} = ?", quoted(k)))
                        .collect();
                    let matches = matches.join(" AND ");
                    let marks = vec!["?"; key.len()].join(", ");
                    let json = query(conn, &format!("SELECT json_array({marks})"), &key).await?;
                    let json = json
                        .first()
                        .map(|row| as_text(&row[0]).map(str::to_string))
                        .transpose()?;
                    if deleted {
                        let removed =
                            exec(conn, &format!("DELETE FROM {table} WHERE {matches}"), &key)
                                .await?;
                        if let Some(json) = json
                            && removed > 0
                        {
                            pass.touch(idx, json, canon, true);
                        }
                        return Ok(Outcome::Settled { deleted: true });
                    }
                    written_key = json;
                    let exists = !query(
                        conn,
                        &format!("SELECT 1 FROM {table} WHERE {matches}"),
                        &key,
                    )
                    .await?
                    .is_empty();
                    if exists {
                        let rest: Vec<&(&Field, Value)> = columns
                            .iter()
                            .filter(|(f, _)| !fields.contains(&f.name))
                            .collect();
                        if rest.is_empty() {
                            Ok(0)
                        } else {
                            let sets: Vec<String> = rest
                                .iter()
                                .map(|(f, _)| format!("{} = ?", quoted(&f.name)))
                                .collect();
                            let mut params: Vec<Value> =
                                rest.iter().map(|(_, v)| v.clone()).collect();
                            params.extend(key);
                            exec(
                                conn,
                                &format!("UPDATE {table} SET {} WHERE {matches}", sets.join(", ")),
                                &params,
                            )
                            .await
                        }
                    } else {
                        self.insert(conn, c, &columns).await
                    }
                }
            };

            if let (Ok(n), Some(key)) = (&written, written_key)
                && *n > 0
            {
                pass.touch(idx, key, canon, false);
            }
            return match written {
                Ok(_) if unresolved => Ok(Outcome::Waiting),
                Ok(_) => Ok(Outcome::Settled { deleted: false }),
                Err(error) if is_constraint(&error) => Ok(Outcome::Waiting),
                Err(error) => Err(error.into()),
            };
        }
    }

    async fn insert(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        columns: &[(&Field, Value)],
    ) -> sqlx::Result<u64> {
        let table = quoted(&c.table);
        if columns.is_empty() {
            return exec(conn, &format!("INSERT INTO {table} DEFAULT VALUES"), &[]).await;
        }
        let names: Vec<String> = columns.iter().map(|(f, _)| quoted(&f.name)).collect();
        let marks = vec!["?"; columns.len()].join(", ");
        let params: Vec<Value> = columns.iter().map(|(_, v)| v.clone()).collect();
        exec(
            conn,
            &format!(
                "INSERT INTO {table} ({}) VALUES ({marks})",
                names.join(", ")
            ),
            &params,
        )
        .await
    }

    /// Applying a delete can cascade to rows the triggers were told not to watch. Their ids are
    /// dropped so a reused local id is never mistaken for the old entity.
    async fn forget_cascaded(&self, conn: &mut SqliteConnection, pass: &mut Pass) -> Result<()> {
        for (idx, c) in self.registry.collections().iter().enumerate() {
            let Key::Minted { column } = &c.key else {
                continue;
            };
            let gone = query(
                conn,
                &format!(
                    "SELECT key, gid FROM fongo_ids WHERE collection = ?1 AND NOT EXISTS (
                        SELECT 1 FROM {} WHERE {} = json_extract(fongo_ids.key, '$[0]'))",
                    quoted(&c.table),
                    quoted(column)
                ),
                &[Value::Text(c.name.clone())],
            )
            .await?;
            for row in gone {
                let gid = as_gid(&row[1])?;
                pass.touch(idx, as_text(&row[0])?.to_string(), gid, true);
                exec(
                    conn,
                    "DELETE FROM fongo_ids WHERE gid = ?",
                    &[Value::Ref(gid)],
                )
                .await?;
            }
        }
        Ok(())
    }

    /// The natural-key rows that hold minted row `key` of `c` in their key, so go with it.
    async fn dependents(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        key: &str,
    ) -> Result<Vec<(usize, String, Gid)>> {
        let mut found = Vec::new();
        for (idx, d) in self.registry.collections().iter().enumerate() {
            let Key::Natural { fields } = &d.key else {
                continue;
            };
            for f in d.fields.iter().filter(|f| fields.contains(&f.name)) {
                if !matches!(&f.kind, Kind::Ref { collection, .. } if *collection == c.name) {
                    continue;
                }
                let columns: Vec<String> = fields.iter().map(|k| quoted(k)).collect();
                let rows = query(
                    conn,
                    &format!(
                        "SELECT json_array({}) FROM {} WHERE {} = json_extract(?1, '$[0]')",
                        columns.join(", "),
                        quoted(&d.table),
                        quoted(&f.name)
                    ),
                    &[Value::Text(key.into())],
                )
                .await?;
                for row in rows {
                    let key = as_text(&row[0])?.to_string();
                    if let Some(gid) = self.gone_natural_gid(conn, d, fields, &key).await? {
                        found.push((idx, key, gid));
                    }
                }
            }
        }
        Ok(found)
    }

    async fn row_exists(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        key: &str,
    ) -> Result<bool> {
        let rows = query(
            conn,
            &format!(
                "SELECT 1 FROM {} WHERE {}",
                quoted(&c.table),
                where_key_json(c)
            ),
            &[Value::Text(key.into())],
        )
        .await?;
        Ok(!rows.is_empty())
    }
}

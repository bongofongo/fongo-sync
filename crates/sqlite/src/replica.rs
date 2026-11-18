use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_core::{ALIAS, Change, Clock, DELETED, Gid, Hlc, Value, wins};
use sqlx::{Connection, SqliteConnection};

use crate::db::{exec, query};
use crate::error::{Error, Result};
use crate::registry::{Collection, Field, Key, Kind, LocalDefault, Registry};

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
    "CREATE TABLE IF NOT EXISTS fongo_bases (
        gid BLOB NOT NULL,
        field TEXT NOT NULL,
        collection TEXT NOT NULL,
        key TEXT NOT NULL,
        base,
        base_is_ref INTEGER NOT NULL DEFAULT 0,
        base_clock TEXT,
        local,
        local_is_ref INTEGER NOT NULL DEFAULT 0,
        local_clock TEXT NOT NULL,
        seq INTEGER NOT NULL,
        reported TEXT,
        PRIMARY KEY (gid, field))",
    "CREATE INDEX IF NOT EXISTS fongo_alias_canon ON fongo_alias (canon)",
];

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

/// An entity whose row could not be written. When the kernel cannot resolve what holds it back,
/// such as a partial unique index another row already satisfies, the application can read the
/// entity's merged values with [`Replica::merged`], change its own rows, and call
/// [`Replica::apply`] with no changes to try again.
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
    Settled,
    Waiting,
    /// The row is to be removed once the rows that may refer to it have been written.
    Remove(Doomed),
}

enum Doomed {
    Minted,
    /// The local values of the natural key.
    Natural(Vec<Value>),
}

enum Local {
    Value(Value),
    /// The referenced entity is deleted or owned by one that is. Holds its local id while its row
    /// is still there to be removed, else null.
    Gone(Value),
    /// The referenced entity has not arrived yet.
    Unknown,
    /// Whether the referenced entity is deleted could not be decided.
    Undecided,
}

/// How deep a chain of owners, and how many entities in all, are followed to learn whether an
/// entity is deleted. The owner graph comes from other replicas, so neither is left unbounded.
const OWNER_DEPTH: usize = 1024;
const OWNER_LIMIT: usize = 10_000;

/// Rounds of bringing rows in line in one apply, after which what is left waits for the next.
const SETTLE_ROUNDS: usize = 1_000;

enum Fate {
    Alive,
    Dead,
    /// The owner graph was too large to follow to the end.
    Undecided,
}

struct Entry {
    idx: usize,
    key: String,
    at: u64,
    gone: bool,
}

type Shadow = BTreeMap<String, (Value, Hlc)>;

/// Every entity known to be the same as one, and their merged state. The group is alive while any
/// member is, and then each field takes the entry its strategy prefers among the live members
/// only, so a deleted member's values do not come back with it. With no live member, among all.
struct Group {
    canon: Gid,
    merged: Shadow,
    /// The members not deleted, smallest first.
    live: Vec<Gid>,
}

impl Group {
    /// The member a local write is recorded on: the group's own id while it is alive, else the
    /// smallest live member.
    fn target(&self) -> Gid {
        match self.live.first() {
            Some(first) if !self.live.contains(&self.canon) => *first,
            _ => self.canon,
        }
    }
}

/// State carried through one round of bringing rows in line with the shadow.
struct Pass {
    clock: Clock,
    /// Entities a merge of identities touched, to be brought in line before the round ends.
    more: BTreeSet<(usize, Gid)>,
    rows: Rows,
    /// Whether each entity looked at is deleted or owned by one that is. Valid until groups merge.
    dead: BTreeMap<(usize, Gid), bool>,
    /// Rows written or removed and groups merged so far.
    writes: usize,
}

/// Each row written or removed, by collection, key and entity: when it was first reported, and
/// whether it was last removed. A key can hold one entity removed and another written.
type Rows = BTreeMap<(usize, String, Gid), (usize, bool)>;

#[derive(Default)]
struct Collected {
    produced: usize,
    rows: Rows,
}

impl Pass {
    fn new(clock: Clock) -> Pass {
        Pass {
            clock,
            more: BTreeSet::new(),
            rows: Rows::new(),
            dead: BTreeMap::new(),
            writes: 0,
        }
    }

    fn touch(&mut self, idx: usize, key: String, gid: Gid, deleted: bool) {
        self.writes += 1;
        let first = self.rows.len();
        self.rows
            .entry((idx, key, gid))
            .or_insert((first, deleted))
            .1 = deleted;
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

/// Drops every object of a type the kernel made: its triggers, or its tables.
async fn drop_kernel(conn: &mut SqliteConnection, kind: &str) -> Result<()> {
    let names = query(
        conn,
        "SELECT name FROM sqlite_master WHERE type = ? AND name LIKE 'fongo!_%' ESCAPE '!'",
        &[Value::Text(kind.into())],
    )
    .await?;
    for row in names {
        let name = quoted(as_text(&row[0])?);
        exec(conn, &format!("DROP {kind} IF EXISTS {name}"), &[]).await?;
    }
    Ok(())
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

/// Remembers what a local edit to a field that reports conflicts replaced, until released. A
/// later edit before then keeps the first base, since the remote side has seen neither.
async fn remember_base(
    conn: &mut SqliteConnection,
    canon: Gid,
    key: &str,
    base: Option<&(Value, Hlc)>,
    change: &Change,
) -> Result<()> {
    let seq = query(conn, "SELECT max(seq) FROM fongo_outbox", &[]).await?;
    let seq = seq
        .first()
        .map(|row| row[0].clone())
        .unwrap_or(Value::Int(0));
    let (base, base_is_ref, base_clock) = match base {
        Some((value, clock)) => {
            let (value, is_ref) = stored(value);
            (value, is_ref, Value::Text(clock.encode()))
        }
        None => (Value::Null, Value::Int(0), Value::Null),
    };
    let (local, local_is_ref) = stored(&change.value);
    exec(
        conn,
        "INSERT INTO fongo_bases (gid, field, collection, key, base, base_is_ref, base_clock,
                                  local, local_is_ref, local_clock, seq)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (gid, field) DO UPDATE SET
            key = excluded.key, local = excluded.local, local_is_ref = excluded.local_is_ref,
            local_clock = excluded.local_clock, seq = excluded.seq",
        &[
            Value::Ref(canon),
            Value::Text(change.field.clone()),
            Value::Text(change.collection.clone()),
            Value::Text(key.into()),
            base,
            base_is_ref,
            base_clock,
            local,
            local_is_ref,
            Value::Text(change.clock.encode()),
            seq,
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

/// A condition true when a row does not already hold the values bound for `fields`, so an update
/// that would change nothing leaves the row alone and is not reported.
fn differs<'a>(fields: impl Iterator<Item = &'a Field>) -> String {
    let same: Vec<String> = fields
        .map(|f| format!("{} IS ?", quoted(&f.name)))
        .collect();
    format!("NOT ({})", same.join(" AND "))
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
        drop_kernel(&mut tx, "trigger").await?;
        for c in self.registry.collections() {
            for sql in triggers(c) {
                exec(&mut tx, &sql, &[]).await?;
            }
            let adopted = format!("adopted:{}", c.name);
            let known = format!("fields:{}", c.name);
            let names: Vec<&str> = c.fields.iter().map(|f| f.name.as_str()).collect();
            let fields = Value::Text(names.join(","));
            if meta_get(&mut tx, &adopted).await?.is_some() {
                if let Some(Value::Text(held)) = meta_get(&mut tx, &known).await? {
                    let held: BTreeSet<&str> = held.split(',').collect();
                    let added: Vec<&Field> = c
                        .fields
                        .iter()
                        .filter(|f| !held.contains(f.name.as_str()))
                        .collect();
                    if !added.is_empty() {
                        self.adopt_fields(&mut tx, c, &added).await?;
                    }
                }
                meta_set(&mut tx, &known, fields).await?;
                continue;
            }
            meta_set(&mut tx, &known, fields).await?;
            let key: Vec<String> = c.key_columns().iter().map(|k| quoted(k)).collect();
            exec(
                &mut tx,
                &format!(
                    "INSERT OR IGNORE INTO fongo_dirty (collection, key, at)
                     SELECT '{}', json_array({}), 0 FROM {} WHERE 1{}",
                    c.name,
                    key.join(", "),
                    quoted(&c.table),
                    c.visible()
                ),
                &[],
            )
            .await?;
            meta_set(&mut tx, &adopted, Value::Int(1)).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// Removes the triggers that notice local writes, so an application can change its schema or
    /// rewrite its rows without the writes being collected. [`Replica::resume`] puts them back.
    /// A row written meanwhile is collected whole the next time it is written, since a write is
    /// collected as the difference between the row and the merged state.
    pub async fn suspend(&self, conn: &mut SqliteConnection) -> Result<()> {
        let mut tx = conn.begin().await?;
        drop_kernel(&mut tx, "trigger").await?;
        tx.commit().await?;
        Ok(())
    }

    /// Puts the triggers back after [`Replica::suspend`]. It installs again, so collections and
    /// fields the registry gained meanwhile are adopted.
    pub async fn resume(&self, conn: &mut SqliteConnection) -> Result<()> {
        self.install(conn).await
    }

    /// Removes the triggers and every table the kernel keeps, leaving the application's own.
    pub async fn uninstall(&self, conn: &mut SqliteConnection) -> Result<()> {
        let mut tx = conn.begin().await?;
        drop_kernel(&mut tx, "trigger").await?;
        drop_kernel(&mut tx, "table").await?;
        tx.commit().await?;
        Ok(())
    }

    /// Records the values rows already hold for fields new to an adopted collection, as the
    /// oldest possible writes, so they fill gaps but never overwrite what any replica wrote.
    async fn adopt_fields(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        added: &[&Field],
    ) -> Result<()> {
        let Some(node) = meta_get(conn, "node").await? else {
            return corrupt("the replica is not installed");
        };
        let node = as_gid(&node)?;
        let mut seq = match meta_get(conn, "adopted_seq").await? {
            Some(Value::Int(seq)) => u32::try_from(seq).unwrap_or(u32::MAX),
            _ => 0,
        };
        let keys: Vec<String> = c.key_columns().iter().map(|k| quoted(k)).collect();
        let rows = query(
            conn,
            &format!(
                "SELECT json_array({}) FROM {} WHERE 1{}",
                keys.join(", "),
                quoted(&c.table),
                c.visible()
            ),
            &[],
        )
        .await?;
        for row in rows {
            let key = as_text(&row[0])?;
            let Some(values) = self.read_row(conn, c, key).await? else {
                continue;
            };
            let gid = match &c.key {
                Key::Minted { .. } => minted_gid(conn, &c.name, key).await?,
                Key::Natural { fields } => self.resolve_natural(conn, c, fields, &values).await?,
            };
            // A row not yet collected is collected whole, new fields and all.
            let Some(gid) = gid else {
                continue;
            };
            if matches!(c.key, Key::Natural { .. }) && !has_shadow(conn, gid).await? {
                continue;
            }
            let group = self.group(conn, c, gid).await?;
            let target = group.target();
            let mut behind = false;
            for field in added {
                let Some(value) = c.field_index(&field.name).and_then(|i| values.get(i)) else {
                    continue;
                };
                if *value == Value::Null {
                    continue;
                }
                seq = seq.saturating_add(1);
                let change = Change {
                    collection: c.name.clone(),
                    gid: target,
                    field: field.name.clone(),
                    value: value.clone(),
                    clock: Hlc {
                        wall_ms: 0,
                        counter: seq,
                        node,
                    },
                };
                let own = shadow_of(conn, target).await?;
                let held = own.get(&field.name).map(|(v, c)| (v, c));
                if wins(field.strategy, held, &change.value, &change.clock) {
                    emit(conn, &change).await?;
                }
                behind |= self
                    .group(conn, c, gid)
                    .await?
                    .merged
                    .get(&field.name)
                    .is_none_or(|(merged, _)| merged != value);
            }
            if behind {
                exec(
                    conn,
                    "INSERT OR IGNORE INTO fongo_pending (gid, collection) VALUES (?, ?)",
                    &[Value::Ref(group.canon), Value::Text(c.name.clone())],
                )
                .await?;
            }
        }
        meta_set(conn, "adopted_seq", Value::Int(i64::from(seq))).await
    }

    /// Turns the local writes made since the last call into changes in the outbox. Returns how
    /// many changes were produced.
    pub async fn collect(&self, conn: &mut SqliteConnection) -> Result<usize> {
        let mut tx = conn.begin().await?;
        let produced = self.collect_in(&mut tx).await?.produced;
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
        let collected = self.collect_in(&mut tx).await?;
        let mut pass = Pass::new(load_clock(&mut tx).await?);
        pass.rows = collected.rows;
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
                    None => {
                        pass.dead.clear();
                        union(&mut tx, change.gid, *other).await?.is_some()
                    }
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
        for row in query(
            &mut tx,
            "SELECT collection, gid FROM fongo_pending ORDER BY collection, gid",
            &[],
        )
        .await?
        {
            let collection = as_text(&row[0])?.to_string();
            let gid = as_gid(&row[1])?;
            let key = self.key_for_gid(&mut tx, &collection, gid).await?;
            applied.held.push(Held {
                collection,
                gid,
                key,
            });
        }
        applied.conflicts = self.conflicts(&mut tx, changes).await?;
        set_applying(&mut tx, false).await?;
        save_clock(&mut tx, &pass.clock).await?;
        tx.commit().await?;
        let collections = self.registry.collections();
        let mut rows: Vec<_> = pass.rows.into_iter().collect();
        rows.sort_by(|((a, k, _), (s, _)), ((b, l, _), (t, _))| (a, k, s).cmp(&(b, l, t)));
        applied.touched = rows
            .into_iter()
            .map(|((idx, key, gid), (_, deleted))| Touched {
                collection: collections[idx].name.clone(),
                key,
                gid,
                deleted,
            })
            .collect();
        Ok(applied)
    }

    /// Pairs each remembered local edit with the latest incoming change to the same field of the
    /// same group that is later than the value the edit replaced, and was made elsewhere.
    async fn conflicts(
        &self,
        conn: &mut SqliteConnection,
        changes: &[Change],
    ) -> Result<Vec<Conflict>> {
        let node = meta_get(conn, "node").await?;
        let node = node.as_ref().map(as_gid).transpose()?;
        let mut latest: BTreeMap<(Vec<u8>, String), &Change> = BTreeMap::new();
        for change in changes {
            if Some(change.clock.node) == node {
                continue;
            }
            let Some(c) = self
                .registry
                .index(&change.collection)
                .map(|i| &self.registry.collections()[i])
            else {
                continue;
            };
            if !c
                .field_index(&change.field)
                .is_some_and(|i| c.fields[i].reports_conflicts)
            {
                continue;
            }
            let canon = canon_of(conn, change.gid).await?;
            for member in members_of(conn, canon).await? {
                let rows = query(
                    conn,
                    "SELECT base_clock, local_clock, reported FROM fongo_bases
                     WHERE gid = ? AND field = ?",
                    &[Value::Ref(member), Value::Text(change.field.clone())],
                )
                .await?;
                let Some(row) = rows.first() else {
                    continue;
                };
                let after = |held: &Value| -> Result<bool> {
                    Ok(match held {
                        Value::Null => true,
                        clock => change.clock > as_clock(clock)?,
                    })
                };
                if !after(&row[0])? || as_clock(&row[1])? == change.clock || !after(&row[2])? {
                    continue;
                }
                let slot = (member.0.to_vec(), change.field.clone());
                if latest
                    .get(&slot)
                    .is_none_or(|held| held.clock < change.clock)
                {
                    latest.insert(slot, change);
                }
            }
        }
        let mut conflicts = Vec::new();
        for ((member, field), remote) in latest {
            let member = Value::Blob(member);
            exec(
                conn,
                "UPDATE fongo_bases SET reported = ? WHERE gid = ? AND field = ?",
                &[
                    Value::Text(remote.clock.encode()),
                    member.clone(),
                    Value::Text(field.clone()),
                ],
            )
            .await?;
            let rows = query(
                conn,
                "SELECT collection, key, base, base_is_ref, local, local_is_ref, local_clock,
                        base_clock
                 FROM fongo_bases WHERE gid = ? AND field = ?",
                &[member, Value::Text(field.clone())],
            )
            .await?;
            let Some(row) = rows.first() else {
                continue;
            };
            let Some(idx) = self.registry.index(as_text(&row[0])?) else {
                continue;
            };
            let c = &self.registry.collections()[idx];
            let canon = canon_of(conn, remote.gid).await?;
            let key = match &c.key {
                Key::Minted { .. } => query(
                    conn,
                    "SELECT key FROM fongo_ids WHERE gid = ?",
                    &[Value::Ref(canon)],
                )
                .await?
                .first()
                .map(|row| as_text(&row[0]).map(str::to_string))
                .transpose()?,
                Key::Natural { .. } => {
                    let key = as_text(&row[1])?.to_string();
                    self.row_exists(conn, c, &key).await?.then_some(key)
                }
            };
            let local = loaded(&row[4], &row[5])?;
            let strategy = self.registry.strategy(&c.name, &field);
            conflicts.push(Conflict {
                collection: c.name.clone(),
                gid: canon,
                key,
                base: match &row[7] {
                    Value::Null => None,
                    _ => Some(loaded(&row[2], &row[3])?),
                },
                remote_won: wins(
                    strategy,
                    Some((&local, &as_clock(&row[6])?)),
                    &remote.value,
                    &remote.clock,
                ),
                field,
                local,
                remote: remote.value.clone(),
            });
        }
        Ok(conflicts)
    }

    /// Forgets the bases of local edits sent through outbox sequence `through`. An application
    /// calls it once it has read the shared log past the changes it appended, after which any
    /// change it meets was made knowing those edits, so is not a conflict with them.
    pub async fn release_bases(&self, conn: &mut SqliteConnection, through: i64) -> Result<()> {
        exec(
            conn,
            "DELETE FROM fongo_bases WHERE seq <= ?",
            &[Value::Int(through)],
        )
        .await?;
        Ok(())
    }

    /// An entity's merged field values, following its alias group, with `$deleted` among them.
    /// Empty when nothing is known of it.
    pub async fn merged(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        gid: Gid,
    ) -> Result<BTreeMap<String, Value>> {
        let Some(idx) = self.registry.index(collection) else {
            return Ok(BTreeMap::new());
        };
        let group = self
            .group(conn, &self.registry.collections()[idx], gid)
            .await?;
        Ok(group
            .merged
            .into_iter()
            .map(|(field, (value, _))| (field, value))
            .collect())
    }

    /// The id of the entity a local row holds, by the row's key as the change-capture triggers
    /// write it. The id is the group's own when the entity has been merged with others.
    pub async fn gid_for_key(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        key: &str,
    ) -> Result<Option<Gid>> {
        let Some(idx) = self.registry.index(collection) else {
            return Ok(None);
        };
        self.row_gid(conn, &self.registry.collections()[idx], key)
            .await
    }

    /// The key of the local row that holds an entity, or any entity merged with it.
    pub async fn key_for_gid(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        gid: Gid,
    ) -> Result<Option<String>> {
        let Some(idx) = self.registry.index(collection) else {
            return Ok(None);
        };
        let c = &self.registry.collections()[idx];
        let canon = canon_of(conn, gid).await?;
        let Key::Natural { fields } = &c.key else {
            return query(
                conn,
                "SELECT key FROM fongo_ids WHERE gid = ?",
                &[Value::Ref(canon)],
            )
            .await?
            .first()
            .map(|row| as_text(&row[0]).map(str::to_string))
            .transpose();
        };
        let group = self.group(conn, c, canon).await?;
        let mut pass = Pass::new(Clock::new(canon));
        let mut key = Vec::with_capacity(fields.len());
        for name in fields {
            let (Some(i), Some((shared, _))) = (c.field_index(name), group.merged.get(name)) else {
                return Ok(None);
            };
            match self.to_local(conn, &c.fields[i], shared, &mut pass).await? {
                Local::Value(value) if value != Value::Null => key.push(value),
                _ => return Ok(None),
            }
        }
        let marks = vec!["?"; key.len()].join(", ");
        let json = query(conn, &format!("SELECT json_array({marks})"), &key).await?;
        let Some(json) = json.first().map(|row| as_text(&row[0])).transpose()? else {
            return Ok(None);
        };
        let json = json.to_string();
        Ok(self.row_exists(conn, c, &json).await?.then_some(json))
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
        for _ in 0..SETTLE_ROUNDS {
            let writes = pass.writes;
            let mut waiting = BTreeSet::new();
            let mut progressed = false;
            let mut order: Vec<(usize, Gid)> = round.iter().copied().collect();
            order.sort_by_key(|&(idx, gid)| (self.registry.rank(idx), gid));
            // Writes go parents first and removals children first, so a row moved off a parent
            // that goes in the same batch is moved before the parent's removal can cascade to it.
            let mut removals = Vec::new();
            for (idx, gid) in order {
                match self.materialise(conn, idx, gid, pass).await? {
                    Outcome::Settled => progressed = true,
                    Outcome::Waiting => {
                        waiting.insert((idx, gid));
                    }
                    Outcome::Remove(doomed) => removals.push((idx, gid, doomed)),
                }
            }
            removals
                .sort_by_key(|&(idx, gid, _)| (std::cmp::Reverse(self.registry.rank(idx)), gid));
            for (idx, gid, doomed) in removals {
                if self.remove(conn, idx, gid, doomed, pass).await? {
                    progressed = true;
                } else {
                    waiting.insert((idx, gid));
                }
            }
            // Entities are queued again only by a write, so a round that wrote nothing and
            // settled nothing would repeat itself.
            let wrote = pass.writes > writes;
            waiting.append(&mut pass.more);
            if waiting.is_empty() || !(progressed || wrote) {
                return Ok(waiting);
            }
            round = waiting;
        }
        Ok(round)
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
        let mut shadows = Vec::new();
        let mut live = Vec::new();
        for member in members_of(conn, canon).await? {
            let shadow = shadow_of(conn, member).await?;
            if matches!(shadow.get(DELETED), Some((Value::Int(0), _))) {
                live.push(member);
            }
            shadows.push((member, shadow));
        }
        let mut merged = Shadow::new();
        for (member, shadow) in shadows {
            if !live.is_empty() && !live.contains(&member) {
                continue;
            }
            for (field, (value, clock)) in shadow {
                let strategy = self.registry.strategy(&c.name, &field);
                let held = merged.get(&field).map(|(v, c)| (v, c));
                if wins(strategy, held, &value, &clock) {
                    merged.insert(field, (value, clock));
                }
            }
        }
        Ok(Group {
            canon,
            merged,
            live,
        })
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
        pass.writes += 1;
        pass.dead.clear();
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
                for (table, column) in &c.local_dependents {
                    let column = quoted(column);
                    exec(
                        conn,
                        &format!(
                            "UPDATE OR IGNORE {} SET {column} = json_extract(?2, '$[0]')
                             WHERE {column} = json_extract(?1, '$[0]')",
                            quoted(table)
                        ),
                        &[Value::Text(lose.clone()), Value::Text(keep.clone())],
                    )
                    .await?;
                }
                let gone = exec(
                    conn,
                    &format!(
                        "DELETE FROM {} WHERE {} = json_extract(?1, '$[0]')",
                        quoted(&c.table),
                        quoted(column)
                    ),
                    &[Value::Text(lose.clone())],
                )
                .await;
                // A row another table will not let go of stays, no longer the group's.
                match gone {
                    Ok(_) => {}
                    Err(error) if is_constraint(&error) => {}
                    Err(error) => return Err(error.into()),
                }
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
        self.refer_again(conn, canon, pass).await?;
        Ok(true)
    }

    /// Queues every entity that refers to a group, to be brought in line again.
    async fn refer_again(
        &self,
        conn: &mut SqliteConnection,
        canon: Gid,
        pass: &mut Pass,
    ) -> Result<()> {
        for member in members_of(conn, canon).await? {
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
        Ok(())
    }

    /// A shared value on its way in: a reference becomes the local id of the row it names.
    async fn to_local(
        &self,
        conn: &mut SqliteConnection,
        field: &Field,
        shared: &Value,
        pass: &mut Pass,
    ) -> Result<Local> {
        match (&field.kind, shared) {
            (Kind::Plain, Value::Ref(_)) => Ok(Local::Value(Value::Null)),
            (Kind::Plain, value) => Ok(Local::Value(value.clone())),
            (Kind::Ref { collection, .. }, Value::Ref(gid)) => {
                let canon = canon_of(conn, *gid).await?;
                let rows = query(
                    conn,
                    "SELECT json_extract(key, '$[0]') FROM fongo_ids WHERE gid = ?",
                    &[Value::Ref(canon)],
                )
                .await?;
                let local = rows.into_iter().next().and_then(|mut row| row.pop());
                let fate = match self.registry.index(collection) {
                    Some(target) => self.fate(conn, target, canon, pass).await?,
                    None => Fate::Alive,
                };
                Ok(match (fate, local) {
                    (Fate::Undecided, _) => Local::Undecided,
                    (Fate::Dead, local) => Local::Gone(local.unwrap_or(Value::Null)),
                    (Fate::Alive, Some(local)) => Local::Value(local),
                    (Fate::Alive, None) => Local::Unknown,
                })
            }
            (Kind::Ref { .. }, _) => Ok(Local::Value(Value::Null)),
        }
    }

    /// Whether an entity's merged state has no row: it is deleted, or something it is owned by,
    /// directly or through other owners, is. Each entity in the owner graph is looked at once,
    /// and answers are kept for the rest of the pass. A graph too deep or too wide to finish is
    /// left undecided, so the entity waits rather than being removed on a guess.
    async fn fate(
        &self,
        conn: &mut SqliteConnection,
        idx: usize,
        gid: Gid,
        pass: &mut Pass,
    ) -> Result<Fate> {
        let start = (idx, canon_of(conn, gid).await?);
        if let Some(&dead) = pass.dead.get(&start) {
            return Ok(if dead { Fate::Dead } else { Fate::Alive });
        }
        let mut seen = BTreeSet::from([start]);
        let mut frontier = vec![start];
        for _ in 0..=OWNER_DEPTH {
            if frontier.is_empty() {
                for entity in seen {
                    pass.dead.insert(entity, false);
                }
                return Ok(Fate::Alive);
            }
            let mut next = Vec::new();
            for (i, g) in frontier {
                match pass.dead.get(&(i, g)) {
                    Some(true) => {
                        pass.dead.insert(start, true);
                        return Ok(Fate::Dead);
                    }
                    Some(false) => continue,
                    None => {}
                }
                let c = &self.registry.collections()[i];
                let group = self.group(conn, c, g).await?;
                if is_deleted(&group.merged) {
                    pass.dead.insert((i, g), true);
                    pass.dead.insert(start, true);
                    return Ok(Fate::Dead);
                }
                for field in c.fields.iter().filter(|f| c.owns_through(f)) {
                    let (Kind::Ref { collection, .. }, Some((Value::Ref(target), _))) =
                        (&field.kind, group.merged.get(&field.name))
                    else {
                        continue;
                    };
                    let Some(t) = self.registry.index(collection) else {
                        continue;
                    };
                    let owner = (t, canon_of(conn, *target).await?);
                    if seen.insert(owner) {
                        if seen.len() > OWNER_LIMIT {
                            return Ok(Fate::Undecided);
                        }
                        next.push(owner);
                    }
                }
            }
            next.sort();
            frontier = next;
        }
        Ok(Fate::Undecided)
    }

    /// Removes an entity's row. Rows the removal cascaded to or nulled are brought in line again,
    /// in case their merged state says otherwise. Returns false when the row must wait.
    async fn remove(
        &self,
        conn: &mut SqliteConnection,
        idx: usize,
        gid: Gid,
        doomed: Doomed,
        pass: &mut Pass,
    ) -> Result<bool> {
        let c = &self.registry.collections()[idx];
        let canon = canon_of(conn, gid).await?;
        let table = quoted(&c.table);
        match (&c.key, doomed) {
            (Key::Minted { column }, _) => {
                let rows = query(
                    conn,
                    "SELECT key FROM fongo_ids WHERE gid = ?",
                    &[Value::Ref(canon)],
                )
                .await?;
                let Some(key) = rows.first().map(|row| as_text(&row[0])).transpose()? else {
                    return Ok(true);
                };
                let key = key.to_string();
                let doomed = self.dependents(conn, c, &key).await?;
                let removed = exec(
                    conn,
                    &format!(
                        "DELETE FROM {table} WHERE {} = json_extract(?1, '$[0]')",
                        quoted(column)
                    ),
                    &[Value::Text(key.clone())],
                )
                .await;
                let removed = match removed {
                    Ok(removed) => removed,
                    // Another table still refers to the row and does not let it go.
                    Err(error) if is_constraint(&error) => return Ok(false),
                    Err(error) => return Err(error.into()),
                };
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
                self.forget_cascaded(conn, pass).await?;
                self.refer_again(conn, canon, pass).await?;
            }
            (Key::Natural { fields }, Doomed::Natural(key)) => {
                let matches: Vec<String> = fields
                    .iter()
                    .map(|k| format!("{} = ?", quoted(k)))
                    .collect();
                let removed = match exec(
                    conn,
                    &format!(
                        "DELETE FROM {table} WHERE {}{}",
                        matches.join(" AND "),
                        c.visible()
                    ),
                    &key,
                )
                .await
                {
                    Ok(removed) => removed,
                    Err(error) if is_constraint(&error) => return Ok(false),
                    Err(error) => return Err(error.into()),
                };
                if removed > 0 {
                    let marks = vec!["?"; key.len()].join(", ");
                    let json = query(conn, &format!("SELECT json_array({marks})"), &key).await?;
                    if let Some(row) = json.first() {
                        pass.touch(idx, as_text(&row[0])?.to_string(), canon, true);
                    }
                }
            }
            (Key::Natural { .. }, Doomed::Minted) => {}
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
        for (d_idx, d) in self.registry.collections().iter().enumerate() {
            for f in &d.fields {
                if !matches!(&f.kind, Kind::Ref { collection, .. } if *collection == c.name) {
                    continue;
                }
                let (table, column) = (quoted(&d.table), quoted(&f.name));
                let referring = format!(
                    "SELECT json_array({}) FROM {table} WHERE {column} = json_extract(?1, '$[0]')",
                    d.key_columns()
                        .iter()
                        .map(|k| quoted(k))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if matches!(&d.key, Key::Natural { fields } if fields.contains(&f.name)) {
                    exec(
                        conn,
                        &format!("DELETE FROM {table} WHERE {column} = json_extract(?1, '$[0]')"),
                        &[Value::Text(lose.into())],
                    )
                    .await?;
                } else {
                    let before = query(conn, &referring, &[Value::Text(lose.into())]).await?;
                    exec(
                        conn,
                        &format!(
                            "UPDATE OR IGNORE {table} SET {column} = json_extract(?2, '$[0]')
                             WHERE {column} = json_extract(?1, '$[0]')"
                        ),
                        &[Value::Text(lose.into()), Value::Text(keep.into())],
                    )
                    .await?;
                    let after = query(conn, &referring, &[Value::Text(lose.into())]).await?;
                    for row in before.iter().filter(|row| !after.contains(row)) {
                        let key = as_text(&row[0])?;
                        if let Some(gid) = self.row_gid(conn, d, key).await? {
                            pass.touch(d_idx, key.to_string(), gid, false);
                        }
                    }
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
                pass.writes += 1;
                pass.dead.clear();
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
                    "SELECT json_array({}) FROM {} WHERE {}{}",
                    quoted(column),
                    quoted(&c.table),
                    matches.join(" AND "),
                    c.visible()
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
                                match self.to_local(conn, &c.fields[i], shared, pass).await? {
                                    Local::Value(value) => value,
                                    Local::Gone(_) | Local::Unknown | Local::Undecided => {
                                        Value::Null
                                    }
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
                if self.give_up(conn, c, identity, &key).await? {
                    pass.touch(idx, key, other, false);
                    pass.more.insert((idx, other));
                }
            }
        }
        Ok(None)
    }

    /// Takes an identity off an out-of-date row so another can hold it: null where the columns
    /// allow, else a stand-in no other row holds. References stay, since they must name a row.
    async fn give_up(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        identity: &[String],
        key: &str,
    ) -> Result<bool> {
        let Key::Minted { column } = &c.key else {
            return Ok(false);
        };
        let plain: Vec<String> = identity
            .iter()
            .filter(|name| {
                c.field_index(name)
                    .is_some_and(|i| matches!(c.fields[i].kind, Kind::Plain))
            })
            .map(|name| quoted(name))
            .collect();
        if plain.is_empty() {
            return Ok(false);
        }
        let nulls: Vec<String> = plain.iter().map(|f| format!("{f} = NULL")).collect();
        let stand_ins: Vec<String> = plain
            .iter()
            .map(|f| {
                format!(
                    "{f} = CASE typeof({f})
                        WHEN 'integer' THEN -1 - abs(random() / 2)
                        WHEN 'real' THEN -1.0 - abs(random() / 2)
                        WHEN 'blob' THEN randomblob(16)
                        ELSE 'fongo:' || hex(randomblob(16)) END"
                )
            })
            .collect();
        for sets in [nulls, stand_ins] {
            let changed = exec(
                conn,
                &format!(
                    "UPDATE OR IGNORE {} SET {} WHERE {} = json_extract(?1, '$[0]')",
                    quoted(&c.table),
                    sets.join(", "),
                    quoted(column)
                ),
                &[Value::Text(key.into())],
            )
            .await?;
            if changed > 0 {
                return Ok(true);
            }
        }
        Ok(false)
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
                "SELECT {} FROM {} WHERE {}{}",
                columns.join(", "),
                quoted(&c.table),
                where_key_json(c),
                c.visible()
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

    async fn collect_in(&self, conn: &mut SqliteConnection) -> Result<Collected> {
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
            return Ok(Collected::default());
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
        // A row outside a collection's filter is, to the kernel, not there.
        for e in entries.iter_mut().filter(|e| !e.gone) {
            if self
                .read_row(conn, &collections[e.idx], &e.key)
                .await?
                .is_none()
            {
                e.gone = true;
            }
        }
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
            let group = self.group(conn, c, gid).await?;
            present.insert(group.canon);
            let gid = group.target();
            let mut merged = group.merged;
            let mut changes = Vec::new();
            if group.live.is_empty() {
                // Brought back to life: the member revived holds only what it held itself.
                changes.push((DELETED.to_string(), Value::Int(0)));
                merged = shadow_of(conn, gid).await?;
                merged.remove(DELETED);
            }
            for (field, value) in c.fields.iter().zip(values) {
                let unchanged = match merged.get(&field.name) {
                    // A reference to a row this replica does not have was written as null.
                    Some((Value::Ref(target), _)) if value == Value::Null => {
                        let target = canon_of(conn, *target).await?;
                        query(
                            conn,
                            "SELECT 1 FROM fongo_ids WHERE gid = ?",
                            &[Value::Ref(target)],
                        )
                        .await?
                        .is_empty()
                    }
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
                let reports = c
                    .field_index(&field)
                    .is_some_and(|i| c.fields[i].reports_conflicts);
                let change = Change {
                    collection: c.name.clone(),
                    gid,
                    field,
                    value,
                    clock: stamp,
                };
                emit(conn, &change).await?;
                if reports {
                    let base = merged.get(&change.field);
                    remember_base(conn, group.canon, &e.key, base, &change).await?;
                }
                produced += 1;
            }
        }

        for (idx, gid, at) in tombstones {
            let c = &collections[idx];
            let group = self.group(conn, c, gid).await?;
            if present.contains(&group.canon) {
                continue;
            }
            for member in group.live {
                emit(
                    conn,
                    &Change {
                        collection: c.name.clone(),
                        gid: member,
                        field: DELETED.into(),
                        value: Value::Int(1),
                        clock: pass.clock.tick(at),
                    },
                )
                .await?;
                produced += 1;
            }
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
        Ok(Collected {
            produced,
            rows: pass.rows,
        })
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
            let Group { canon, merged, .. } = self.group(conn, c, gid).await?;
            if merged.is_empty() {
                return Ok(Outcome::Settled);
            }
            let deleted = is_deleted(&merged);
            let table = quoted(&c.table);

            let mut columns: Vec<(&Field, Value)> = Vec::new();
            let mut leaving: BTreeMap<&str, Value> = BTreeMap::new();
            let mut unresolved = false;
            let mut orphaned = false;
            // A field no live member holds is null, even if the row took a value from a member
            // since deleted; an insert leaves it to the table's default.
            let mut absent: BTreeSet<&str> = BTreeSet::new();
            for field in &c.fields {
                let Some((shared, _)) = merged.get(&field.name) else {
                    absent.insert(&field.name);
                    columns.push((field, Value::Null));
                    continue;
                };
                match self.to_local(conn, field, shared, pass).await? {
                    Local::Undecided => return Ok(Outcome::Waiting),
                    Local::Value(value) => columns.push((field, value)),
                    Local::Gone(local) => {
                        orphaned |= c.owns_through(field);
                        leaving.insert(&field.name, local);
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
                        return Ok(Outcome::Remove(Doomed::Minted));
                    }
                    match key {
                        Some(_) if columns.is_empty() => Ok(0),
                        Some(key) => {
                            written_key = Some(key.clone());
                            let sets: Vec<String> = columns
                                .iter()
                                .map(|(f, _)| format!("{} = ?", quoted(&f.name)))
                                .collect();
                            let values: Vec<Value> =
                                columns.iter().map(|(_, v)| v.clone()).collect();
                            let mut params = values.clone();
                            params.push(Value::Text(key));
                            params.extend(values);
                            exec(
                                conn,
                                &format!(
                                    "UPDATE {table} SET {} WHERE {} = json_extract(?, '$[0]') AND {}",
                                    sets.join(", "),
                                    quoted(column),
                                    differs(columns.iter().map(|(f, _)| *f))
                                ),
                                &params,
                            )
                            .await
                        }
                        None => {
                            let inserted = self.insert(conn, c, &columns, &absent).await;
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
                                // Rows that referred to the group while it had no row hold null.
                                self.refer_again(conn, canon, pass).await?;
                            }
                            inserted
                        }
                    }
                }
                Key::Natural { fields } => {
                    let mut key = Vec::new();
                    for name in fields {
                        let value = match columns.iter().find(|(f, _)| f.name == *name) {
                            Some((_, Value::Null)) | None => leaving.get(name.as_str()),
                            Some((_, value)) => Some(value),
                        };
                        match value {
                            Some(value) if *value != Value::Null => key.push(value.clone()),
                            _ if deleted => return Ok(Outcome::Settled),
                            _ => return Ok(Outcome::Waiting),
                        }
                    }
                    if deleted {
                        return Ok(Outcome::Remove(Doomed::Natural(key)));
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
                    written_key = json;
                    let found = query(
                        conn,
                        &format!("SELECT 1, 1{} FROM {table} WHERE {matches}", c.visible()),
                        &key,
                    )
                    .await?;
                    let exists = !found.is_empty();
                    if found.first().is_some_and(|row| row[1] != Value::Int(1)) {
                        // The key's row is outside the filter, so not this replica's to write.
                        return Ok(Outcome::Settled);
                    }
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
                            let values: Vec<Value> = rest.iter().map(|(_, v)| v.clone()).collect();
                            let mut params = values.clone();
                            params.extend(key);
                            params.extend(values);
                            exec(
                                conn,
                                &format!(
                                    "UPDATE {table} SET {} WHERE {matches} AND {}",
                                    sets.join(", "),
                                    differs(rest.iter().map(|(f, _)| *f))
                                ),
                                &params,
                            )
                            .await
                        }
                    } else {
                        self.insert(conn, c, &columns, &absent).await
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
                Ok(_) => Ok(Outcome::Settled),
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
        absent: &BTreeSet<&str>,
    ) -> sqlx::Result<u64> {
        let columns: Vec<&(&Field, Value)> = columns
            .iter()
            .filter(|(f, _)| !absent.contains(f.name.as_str()))
            .collect();
        let table = quoted(&c.table);
        let mut names: Vec<String> = columns.iter().map(|(f, _)| quoted(&f.name)).collect();
        let mut marks = vec!["?".to_string(); columns.len()];
        let mut params: Vec<Value> = columns.iter().map(|(_, v)| v.clone()).collect();
        for (column, default) in &c.local_defaults {
            names.push(quoted(column));
            match default {
                LocalDefault::Value(value) => {
                    marks.push("?".into());
                    params.push(value.clone());
                }
                LocalDefault::Sql(sql) => marks.push(format!("({sql})")),
            }
        }
        if names.is_empty() {
            return exec(conn, &format!("INSERT INTO {table} DEFAULT VALUES"), &[]).await;
        }
        let marks = marks.join(", ");
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
    /// dropped so a reused local id is never mistaken for the old entity, and they are brought in
    /// line again, since their merged state may still have a row.
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
                pass.more.insert((idx, gid));
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

    /// The id of the entity a local row holds.
    async fn row_gid(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        key: &str,
    ) -> Result<Option<Gid>> {
        match &c.key {
            Key::Minted { .. } => match minted_gid(conn, &c.name, key).await? {
                Some(gid) => Ok(Some(canon_of(conn, gid).await?)),
                None => Ok(None),
            },
            Key::Natural { fields } => self.gone_natural_gid(conn, c, fields, key).await,
        }
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

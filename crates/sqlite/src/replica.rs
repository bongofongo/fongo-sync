use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;

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
        gid BLOB NOT NULL,
        PRIMARY KEY (collection, key),
        UNIQUE (collection, gid))",
    "CREATE TABLE IF NOT EXISTS fongo_shadow (
        collection TEXT NOT NULL,
        gid BLOB NOT NULL,
        field TEXT NOT NULL,
        value,
        is_ref INTEGER NOT NULL DEFAULT 0,
        clock TEXT NOT NULL,
        PRIMARY KEY (collection, gid, field))",
    "CREATE TABLE IF NOT EXISTS fongo_outbox (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        collection TEXT NOT NULL,
        gid BLOB NOT NULL,
        field TEXT NOT NULL,
        value,
        is_ref INTEGER NOT NULL DEFAULT 0,
        clock TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS fongo_shadow_refs ON fongo_shadow (value) WHERE is_ref = 1",
    "CREATE TABLE IF NOT EXISTS fongo_pending (
        collection TEXT NOT NULL,
        gid BLOB NOT NULL,
        PRIMARY KEY (collection, gid))",
    "CREATE TABLE IF NOT EXISTS fongo_alias (
        collection TEXT NOT NULL,
        gid BLOB NOT NULL,
        canon BLOB NOT NULL,
        PRIMARY KEY (collection, gid))",
    "CREATE TABLE IF NOT EXISTS fongo_deviations (
        collection TEXT NOT NULL,
        key TEXT NOT NULL,
        field TEXT NOT NULL,
        value,
        is_ref INTEGER NOT NULL DEFAULT 0,
        pinned INTEGER NOT NULL DEFAULT 0,
        against TEXT,
        PRIMARY KEY (collection, key, field))",
    "CREATE TABLE IF NOT EXISTS fongo_bases (
        collection TEXT NOT NULL,
        gid BLOB NOT NULL,
        field TEXT NOT NULL,
        key TEXT NOT NULL,
        base,
        base_is_ref INTEGER NOT NULL DEFAULT 0,
        base_clock TEXT,
        local,
        local_is_ref INTEGER NOT NULL DEFAULT 0,
        local_clock TEXT NOT NULL,
        seq INTEGER NOT NULL,
        reported TEXT,
        PRIMARY KEY (collection, gid, field))",
    "CREATE INDEX IF NOT EXISTS fongo_alias_canon ON fongo_alias (collection, canon)",
    "CREATE TABLE IF NOT EXISTS fongo_leftover (
        collection TEXT NOT NULL,
        key TEXT NOT NULL,
        gid BLOB NOT NULL,
        PRIMARY KEY (collection, key))",
    "CREATE TABLE IF NOT EXISTS fongo_alias_pairs (
        collection TEXT NOT NULL,
        gid BLOB NOT NULL,
        other BLOB NOT NULL,
        PRIMARY KEY (collection, other, gid))",
];

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

/// A group's fate, and whether it leaned on a guess about a group still being worked out.
type Walk<'a> = Pin<Box<dyn Future<Output = Result<(Fate, bool)>> + Send + 'a>>;

/// Whether an entity has a row in its merged state. A member is live when its tombstone says so
/// and every owner it names is alive; a group is alive when any member is. The answer is the
/// greatest one consistent with that, so owners that own each other are alive unless a deleted
/// one can be reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fate {
    Alive,
    Dead,
    /// A member's tombstone, or an owner, has not arrived, so whether it lives is not known yet.
    Unknown,
    /// The owner graph was too large to follow to the end.
    Undecided,
}

impl Fate {
    /// Of several members, the most alive.
    fn any(self, other: Fate) -> Fate {
        use Fate::{Alive, Dead, Undecided, Unknown};
        match (self, other) {
            (Alive, _) | (_, Alive) => Alive,
            (Undecided, _) | (_, Undecided) => Undecided,
            (Unknown, _) | (_, Unknown) => Unknown,
            (Dead, Dead) => Dead,
        }
    }

    /// Of several owners, the least alive.
    fn all(self, other: Fate) -> Fate {
        use Fate::{Alive, Dead, Undecided, Unknown};
        match (self, other) {
            (Dead, _) | (_, Dead) => Dead,
            (Undecided, _) | (_, Undecided) => Undecided,
            (Unknown, _) | (_, Unknown) => Unknown,
            (Alive, Alive) => Alive,
        }
    }
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
    /// The live members, smallest first.
    live: Vec<Gid>,
    fate: Fate,
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
    /// The fate of each group looked at. Valid until groups merge.
    fates: BTreeMap<(usize, Gid), Fate>,
    /// The groups whose fate is being worked out, and how many have been looked at, in one walk.
    walking: BTreeSet<(usize, Gid)>,
    walked: usize,
    /// Entities whose fate may have changed, so whose referrers' may have too.
    suspect: BTreeSet<(usize, Gid)>,
    /// The suspects whose referrers have been queued, once each pass.
    told: BTreeSet<(usize, Gid)>,
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
            fates: BTreeMap::new(),
            walking: BTreeSet::new(),
            walked: 0,
            suspect: BTreeSet::new(),
            told: BTreeSet::new(),
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

/// The version of the kernel's own tables.
const LAYOUT: i64 = 2;

/// Brings tables kept by an earlier version to the present layout, in which every entity is
/// keyed by its collection as well as its id.
async fn migrate(conn: &mut SqliteConnection) -> Result<()> {
    let installed = !query(
        conn,
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'fongo_meta'",
        &[],
    )
    .await?
    .is_empty();
    if !installed || meta_get(conn, "layout").await? == Some(Value::Int(LAYOUT)) {
        return Ok(());
    }
    let moved = [
        "fongo_ids",
        "fongo_shadow",
        "fongo_pending",
        "fongo_alias",
        "fongo_bases",
    ];
    for table in moved {
        let exists = !query(
            conn,
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            &[Value::Text(table.into())],
        )
        .await?
        .is_empty();
        if exists {
            exec(
                conn,
                &format!("ALTER TABLE {table} RENAME TO {table}_old"),
                &[],
            )
            .await?;
        }
    }
    exec(conn, "DROP INDEX IF EXISTS fongo_shadow_refs", &[]).await?;
    exec(conn, "DROP INDEX IF EXISTS fongo_alias_canon", &[]).await?;
    for sql in TABLES {
        exec(conn, sql, &[]).await?;
    }
    let copies = [
        (
            "fongo_ids",
            "INSERT OR IGNORE INTO fongo_ids (collection, key, gid)
             SELECT collection, key, gid FROM fongo_ids_old",
        ),
        (
            "fongo_shadow",
            "INSERT OR IGNORE INTO fongo_shadow (collection, gid, field, value, is_ref, clock)
             SELECT collection, gid, field, value, is_ref, clock FROM fongo_shadow_old",
        ),
        (
            "fongo_pending",
            "INSERT OR IGNORE INTO fongo_pending (collection, gid)
             SELECT collection, gid FROM fongo_pending_old",
        ),
        (
            "fongo_alias",
            "INSERT OR IGNORE INTO fongo_alias (collection, gid, canon)
             SELECT COALESCE((SELECT collection FROM fongo_shadow s WHERE s.gid = o.gid LIMIT 1), ''),
                    gid, canon
               FROM fongo_alias_old o",
        ),
        (
            "fongo_bases",
            "INSERT OR IGNORE INTO fongo_bases
             SELECT collection, gid, field, key, base, base_is_ref, base_clock, local,
                    local_is_ref, local_clock, seq, reported
               FROM fongo_bases_old",
        ),
    ];
    for (table, copy) in copies {
        let old = format!("{table}_old");
        let exists = !query(
            conn,
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            &[Value::Text(old.clone())],
        )
        .await?
        .is_empty();
        if exists {
            exec(conn, copy, &[]).await?;
            exec(conn, &format!("DROP TABLE {old}"), &[]).await?;
        }
    }
    Ok(())
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

async fn shadow_of(conn: &mut SqliteConnection, collection: &str, gid: Gid) -> Result<Shadow> {
    let rows = query(
        conn,
        "SELECT field, value, is_ref, clock FROM fongo_shadow WHERE collection = ? AND gid = ?",
        &[Value::Text(collection.into()), Value::Ref(gid)],
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
        "INSERT INTO fongo_shadow (collection, gid, field, value, is_ref, clock)
         VALUES (?, ?, ?, ?, ?, ?)
         ON CONFLICT (collection, gid, field) DO UPDATE SET
            value = excluded.value, is_ref = excluded.is_ref, clock = excluded.clock",
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
         ON CONFLICT (collection, gid, field) DO UPDATE SET
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

/// Whether a natural-key entity's shadow holds its whole key, which is what joins it to others
/// with the same key. A member written before its key arrived joins once it has.
async fn has_key(conn: &mut SqliteConnection, c: &Collection, gid: Gid) -> Result<bool> {
    let Key::Natural { .. } = &c.key else {
        return has_shadow(conn, &c.name, gid).await;
    };
    let shadow = shadow_of(conn, &c.name, gid).await?;
    Ok(own_key(c, &shadow) == Some(gid))
}

/// The id a natural-key entity's own key fields derive, once all are written.
fn own_key(c: &Collection, shadow: &Shadow) -> Option<Gid> {
    let Key::Natural { fields } = &c.key else {
        return None;
    };
    let mut key = Vec::with_capacity(fields.len());
    for name in fields {
        match shadow.get(name) {
            Some((value, _)) if *value != Value::Null => key.push(value.clone()),
            _ => return None,
        }
    }
    Some(Gid::derive(&c.name, &key))
}

async fn has_shadow(conn: &mut SqliteConnection, collection: &str, gid: Gid) -> Result<bool> {
    let rows = query(
        conn,
        "SELECT 1 FROM fongo_shadow WHERE collection = ? AND gid = ? LIMIT 1",
        &[Value::Text(collection.into()), Value::Ref(gid)],
    )
    .await?;
    Ok(!rows.is_empty())
}

/// The smallest id among those known to be the same entity as `gid` in `collection`. An entity
/// is its collection and its id together, so ids that name things in different collections are
/// never confused, whatever another replica claims.
async fn canon_of(conn: &mut SqliteConnection, collection: &str, gid: Gid) -> Result<Gid> {
    let rows = query(
        conn,
        "SELECT canon FROM fongo_alias WHERE collection = ? AND gid = ?",
        &[Value::Text(collection.into()), Value::Ref(gid)],
    )
    .await?;
    rows.first().map_or(Ok(gid), |row| as_gid(&row[0]))
}

async fn members_of(conn: &mut SqliteConnection, collection: &str, canon: Gid) -> Result<Vec<Gid>> {
    let rows = query(
        conn,
        "SELECT gid FROM fongo_alias WHERE collection = ? AND canon = ? ORDER BY gid",
        &[Value::Text(collection.into()), Value::Ref(canon)],
    )
    .await?;
    if rows.is_empty() {
        return Ok(vec![canon]);
    }
    rows.iter().map(|row| as_gid(&row[0])).collect()
}

/// Records that `a` and `b` are one entity. Returns the merged group's id when they were not
/// already one.
async fn union(
    conn: &mut SqliteConnection,
    collection: &str,
    a: Gid,
    b: Gid,
) -> Result<Option<Gid>> {
    let (a, b) = (
        canon_of(conn, collection, a).await?,
        canon_of(conn, collection, b).await?,
    );
    if a == b {
        return Ok(None);
    }
    let canon = a.min(b);
    let mut all = members_of(conn, collection, a).await?;
    all.extend(members_of(conn, collection, b).await?);
    for gid in all {
        exec(
            conn,
            "INSERT INTO fongo_alias (collection, gid, canon) VALUES (?, ?, ?)
             ON CONFLICT (collection, gid) DO UPDATE SET canon = excluded.canon",
            &[
                Value::Text(collection.into()),
                Value::Ref(gid),
                Value::Ref(canon),
            ],
        )
        .await?;
    }
    Ok(Some(canon))
}

/// Whether two values of a field are the same, references to members of one group included.
async fn same(conn: &mut SqliteConnection, field: &Field, a: &Value, b: &Value) -> Result<bool> {
    match (&field.kind, a, b) {
        (Kind::Ref { collection, .. }, Value::Ref(x), Value::Ref(y)) if x != y => {
            Ok(canon_of(conn, collection, *x).await? == canon_of(conn, collection, *y).await?)
        }
        _ => Ok(a == b),
    }
}

/// Each value a key part could have been written as: a reference by any member of its group.
async fn spellings(
    conn: &mut SqliteConnection,
    field: &Field,
    value: &Value,
) -> Result<Vec<Value>> {
    match (&field.kind, value) {
        (Kind::Ref { collection, .. }, Value::Ref(gid)) => {
            let canon = canon_of(conn, collection, *gid).await?;
            Ok(members_of(conn, collection, canon)
                .await?
                .into_iter()
                .map(Value::Ref)
                .collect())
        }
        (_, other) => Ok(vec![other.clone()]),
    }
}

/// The fields of sets that change together whose newest write has not all arrived. Every write
/// records the whole set at one clock, so a set is complete when all its fields hold entries of
/// one clock, or none hold any.
fn incomplete_sets<'a>(c: &'a Collection, merged: &Shadow) -> BTreeSet<&'a str> {
    let mut held = BTreeSet::new();
    for set in &c.together {
        let clocks: Vec<Option<Hlc>> = set
            .iter()
            .map(|f| merged.get(f).map(|(_, clock)| *clock))
            .collect();
        if clocks.iter().any(|clock| *clock != clocks[0]) {
            held.extend(set.iter().map(String::as_str));
        }
    }
    held
}

async fn deviations(
    conn: &mut SqliteConnection,
    collection: &str,
    key: &str,
) -> Result<BTreeMap<String, Value>> {
    let rows = query(
        conn,
        "SELECT field, value, is_ref FROM fongo_deviations WHERE collection = ? AND key = ?",
        &[Value::Text(collection.into()), Value::Text(key.into())],
    )
    .await?;
    let mut held = BTreeMap::new();
    for row in rows {
        held.insert(as_text(&row[0])?.to_string(), loaded(&row[1], &row[2])?);
    }
    Ok(held)
}

/// Records a value left in a row's field. A pinned one stands against the merged entry it was
/// recorded against, and is neither sent nor written over until that entry changes.
async fn note(
    conn: &mut SqliteConnection,
    collection: &str,
    key: &str,
    field: &str,
    value: &Value,
    pinned: Option<Option<&(Value, Hlc)>>,
) -> Result<()> {
    let (value, is_ref) = stored(value);
    let against = match pinned {
        Some(Some((_, clock))) => Value::Text(clock.encode()),
        _ => Value::Null,
    };
    exec(
        conn,
        "INSERT INTO fongo_deviations (collection, key, field, value, is_ref, pinned, against)
         VALUES (?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (collection, key, field) DO UPDATE SET
            value = excluded.value, is_ref = excluded.is_ref, pinned = excluded.pinned,
            against = excluded.against",
        &[
            Value::Text(collection.into()),
            Value::Text(key.into()),
            Value::Text(field.into()),
            value,
            is_ref,
            Value::Int(i64::from(pinned.is_some())),
            against,
        ],
    )
    .await?;
    Ok(())
}

/// The pinned values of a row that still stand: those whose merged entry has not changed since.
async fn pins(
    conn: &mut SqliteConnection,
    collection: &str,
    key: &str,
    merged: &Shadow,
) -> Result<BTreeMap<String, Value>> {
    let rows = query(
        conn,
        "SELECT field, value, is_ref, against FROM fongo_deviations
          WHERE collection = ? AND key = ? AND pinned = 1",
        &[Value::Text(collection.into()), Value::Text(key.into())],
    )
    .await?;
    let mut standing = BTreeMap::new();
    for row in rows {
        let field = as_text(&row[0])?.to_string();
        let against = match &row[3] {
            Value::Null => None,
            clock => Some(as_clock(clock)?),
        };
        if merged.get(&field).map(|(_, clock)| *clock) == against {
            standing.insert(field, loaded(&row[1], &row[2])?);
        }
    }
    Ok(standing)
}

/// Forgets what the kernel left in a row's fields, or in one of them.
async fn forget_deviations(
    conn: &mut SqliteConnection,
    collection: &str,
    key: &str,
    field: Option<&str>,
) -> Result<()> {
    let mut params = vec![Value::Text(collection.into()), Value::Text(key.into())];
    let only = match field {
        Some(field) => {
            params.push(Value::Text(field.into()));
            " AND field = ?"
        }
        None => "",
    };
    exec(
        conn,
        &format!("DELETE FROM fongo_deviations WHERE collection = ? AND key = ?{only}"),
        &params,
    )
    .await?;
    Ok(())
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
        migrate(&mut tx).await?;
        for sql in TABLES {
            exec(&mut tx, sql, &[]).await?;
        }
        meta_set(&mut tx, "layout", Value::Int(LAYOUT)).await?;
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
            let sets = format!("together:{}", c.name);
            let together: Vec<String> = c.together.iter().map(|set| set.join("+")).collect();
            let together = Value::Text(together.join(","));
            let was_together = meta_get(&mut tx, &sets).await?;
            meta_set(&mut tx, &sets, together.clone()).await?;
            if meta_get(&mut tx, &adopted).await?.is_some() {
                let regrouped = match was_together {
                    Some(was) => was != together,
                    None => !c.together.is_empty(),
                };
                if regrouped {
                    self.complete_sets(&mut tx, c).await?;
                }
                // A database installed before fields were recorded knew those it holds values
                // for; any other is new to it.
                let held = match meta_get(&mut tx, &known).await? {
                    Some(Value::Text(held)) => Some(held),
                    _ => None,
                };
                let mut added: Vec<&Field> = Vec::new();
                for field in &c.fields {
                    let new = match &held {
                        Some(held) => !held.split(',').any(|f| f == field.name),
                        None => query(
                            &mut tx,
                            "SELECT 1 FROM fongo_shadow WHERE collection = ? AND field = ? LIMIT 1",
                            &[Value::Text(c.name.clone()), Value::Text(field.name.clone())],
                        )
                        .await?
                        .is_empty(),
                    };
                    if new {
                        added.push(field);
                    }
                }
                for field in &added {
                    // Values for it may have arrived while this replica did not know it.
                    exec(
                        &mut tx,
                        "INSERT OR IGNORE INTO fongo_pending (collection, gid)
                         SELECT DISTINCT collection, gid FROM fongo_shadow
                          WHERE collection = ? AND field = ?",
                        &[Value::Text(c.name.clone()), Value::Text(field.name.clone())],
                    )
                    .await?;
                }
                if !added.is_empty() {
                    self.adopt_fields(&mut tx, c, &added).await?;
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

    /// Removes the triggers that notice local writes, so an application can change its schema or
    /// rewrite its rows without each write being collected. [`Replica::resume`] puts them back
    /// and says what the rows written meanwhile mean.
    pub async fn suspend(&self, conn: &mut SqliteConnection) -> Result<()> {
        let mut tx = conn.begin().await?;
        drop_kernel(&mut tx, "trigger").await?;
        tx.commit().await?;
        Ok(())
    }

    /// Puts the triggers back after [`Replica::suspend`], installing again so that collections
    /// and fields the registry gained meanwhile are adopted, and compares every table with what
    /// the kernel knows. A row added meanwhile is a new entity and a row removed is deleted,
    /// whichever `how`; a row whose local id was reused meanwhile is the old entity changed. A row
    /// changed meanwhile is, by `how`, an edit to send, or the replica's own baseline.
    pub async fn resume(&self, conn: &mut SqliteConnection, how: Resume) -> Result<()> {
        self.install(conn).await?;
        let mut tx = conn.begin().await?;
        let mut pass = Pass::new(load_clock(&mut tx).await?);
        let dirty = |gone: bool| {
            format!(
                "INSERT INTO fongo_dirty (collection, key, at, gone) VALUES (?, ?, {NOW}, {})
                 ON CONFLICT (collection, key) DO UPDATE SET at = excluded.at,
                    gone = max(gone, excluded.gone)",
                i64::from(gone)
            )
        };
        for (idx, c) in self.registry.collections().iter().enumerate() {
            let keys: Vec<String> = c.key_columns().iter().map(|k| quoted(k)).collect();
            if let Key::Minted { column } = &c.key {
                exec(
                    &mut tx,
                    &format!(
                        "INSERT INTO fongo_dirty (collection, key, at, gone)
                         SELECT collection, key, {NOW}, 1 FROM fongo_ids
                          WHERE collection = ?1 AND NOT EXISTS (
                            SELECT 1 FROM {} WHERE {} = json_extract(fongo_ids.key, '$[0]'))
                         ON CONFLICT (collection, key) DO UPDATE SET gone = 1",
                        quoted(&c.table),
                        quoted(column)
                    ),
                    &[Value::Text(c.name.clone())],
                )
                .await?;
            }
            let rows = query(
                &mut tx,
                &format!(
                    "SELECT json_array({}) FROM {}",
                    keys.join(", "),
                    quoted(&c.table)
                ),
                &[],
            )
            .await?;
            for row in rows {
                let key = as_text(&row[0])?.to_string();
                let Some(values) = self.read_row(&mut tx, c, &key).await? else {
                    continue;
                };
                let gid = match &c.key {
                    Key::Minted { .. } => minted_gid(&mut tx, &c.name, &key).await?,
                    Key::Natural { fields } => {
                        self.resolve_natural(&mut tx, c, fields, &values).await?
                    }
                };
                let known = match gid {
                    Some(gid) => {
                        let canon = canon_of(&mut tx, &c.name, gid).await?;
                        has_shadow(&mut tx, &c.name, canon).await?
                    }
                    None => false,
                };
                if !known {
                    exec(
                        &mut tx,
                        &dirty(false),
                        &[Value::Text(c.name.clone()), Value::Text(key)],
                    )
                    .await?;
                    continue;
                }
                let Some(gid) = gid else { continue };
                let merged = self.group(&mut tx, c, gid, &mut pass).await?.merged;
                let left = deviations(&mut tx, &c.name, &key).await?;
                let mut changed = Vec::new();
                for (field, value) in c.fields.iter().zip(values) {
                    let held = merged
                        .get(&field.name)
                        .map_or(Value::Null, |(v, _)| v.clone());
                    let kept = match left.get(&field.name) {
                        Some(left) => same(&mut tx, field, left, &value).await?,
                        None => false,
                    };
                    if !kept && !same(&mut tx, field, &held, &value).await? {
                        changed.push((field, value));
                    }
                }
                if changed.is_empty() {
                    continue;
                }
                match how {
                    Resume::Send => {
                        exec(
                            &mut tx,
                            &dirty(false),
                            &[Value::Text(c.name.clone()), Value::Text(key)],
                        )
                        .await?;
                    }
                    Resume::Baseline => {
                        for (field, value) in changed {
                            note(
                                &mut tx,
                                &c.name,
                                &key,
                                &field.name,
                                &value,
                                Some(merged.get(&field.name)),
                            )
                            .await?;
                        }
                    }
                }
            }
            if let Key::Natural { .. } = &c.key {
                self.natural_rows_gone(&mut tx, idx, &mut pass).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// Marks as removed each live natural-key entity whose row is not in its table, unless it is
    /// waiting to be written.
    async fn natural_rows_gone(
        &self,
        conn: &mut SqliteConnection,
        idx: usize,
        pass: &mut Pass,
    ) -> Result<()> {
        let c = &self.registry.collections()[idx];
        let mut seen = BTreeSet::new();
        let gids = query(
            conn,
            "SELECT DISTINCT gid FROM fongo_shadow WHERE collection = ?",
            &[Value::Text(c.name.clone())],
        )
        .await?;
        for row in gids {
            let canon = canon_of(conn, &c.name, as_gid(&row[0])?).await?;
            if !seen.insert(canon) {
                continue;
            }
            let waiting = !query(
                conn,
                "SELECT 1 FROM fongo_pending WHERE collection = ? AND gid = ?",
                &[Value::Text(c.name.clone()), Value::Ref(canon)],
            )
            .await?
            .is_empty();
            let group = self.group(conn, c, canon, pass).await?;
            if waiting || group.fate != Fate::Alive {
                continue;
            }
            let mut columns: Vec<(&Field, Value)> = Vec::new();
            for field in &c.fields {
                if let Some((shared, _)) = group.merged.get(&field.name)
                    && let Local::Value(value) = self.to_local(conn, field, shared, pass).await?
                {
                    columns.push((field, value));
                }
            }
            let Some(key) = self
                .key_of(conn, c, canon, &columns, &BTreeMap::new())
                .await?
            else {
                continue;
            };
            if !self.row_exists(conn, c, &key).await? {
                exec(
                    conn,
                    &format!(
                        "INSERT INTO fongo_dirty (collection, key, at, gone) VALUES (?, ?, {NOW}, 1)
                         ON CONFLICT (collection, key) DO UPDATE SET gone = 1"
                    ),
                    &[Value::Text(c.name.clone()), Value::Text(key)],
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Removes the triggers and every table the kernel keeps, leaving the application's own.
    /// Installing again afterwards is a fresh join: every row becomes a new entity, so syncing
    /// with replicas that knew the old ones gives two copies of each row that has no identity.
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
        let mut pass = Pass::new(Clock::new(node));
        let mut seq = match meta_get(conn, "adopted_seq").await? {
            Some(Value::Int(seq)) => u32::try_from(seq).unwrap_or(u32::MAX),
            _ => 0,
        };
        let keys: Vec<String> = c.key_columns().iter().map(|k| quoted(k)).collect();
        let rows = query(
            conn,
            &format!(
                "SELECT json_array({}) FROM {}",
                keys.join(", "),
                quoted(&c.table)
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
            if matches!(c.key, Key::Natural { .. }) && !has_shadow(conn, &c.name, gid).await? {
                continue;
            }
            let group = self.group(conn, c, gid, &mut pass).await?;
            let target = group.target();
            let mut behind = false;
            // A field in a set is recorded with its set, never alone.
            for field in added.iter().filter(|f| c.together_of(&f.name).is_none()) {
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
                let own = shadow_of(conn, &c.name, target).await?;
                let held = own.get(&field.name).map(|(v, c)| (v, c));
                if wins(field.strategy, held, &change.value, &change.clock) {
                    emit(conn, &change).await?;
                }
                behind |= self
                    .group(conn, c, gid, &mut pass)
                    .await?
                    .merged
                    .get(&field.name)
                    .is_none_or(|(merged, _)| merged != value);
            }
            if behind {
                exec(
                    conn,
                    "INSERT OR IGNORE INTO fongo_pending (collection, gid) VALUES (?, ?)",
                    &[Value::Text(c.name.clone()), Value::Ref(group.canon)],
                )
                .await?;
            }
        }
        meta_set(conn, "adopted_seq", Value::Int(i64::from(seq))).await
    }

    /// After the sets of fields that change together are redrawn, writes made before hold only
    /// some fields of a set, so the set is never complete and never written. Each row that holds
    /// a value for such a set records the whole set afresh, and the newest of those wins.
    async fn complete_sets(&self, conn: &mut SqliteConnection, c: &Collection) -> Result<()> {
        let mut clock = load_clock(conn).await?;
        let mut pass = Pass::new(clock.clone());
        let now = query(conn, &format!("SELECT {NOW}"), &[]).await?;
        let now = match now.first().map(|row| &row[0]) {
            Some(Value::Int(now)) => u64::try_from(*now).unwrap_or(0),
            _ => 0,
        };
        let keys: Vec<String> = c.key_columns().iter().map(|k| quoted(k)).collect();
        let rows = query(
            conn,
            &format!(
                "SELECT json_array({}) FROM {}",
                keys.join(", "),
                quoted(&c.table)
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
            let Some(gid) = gid else {
                continue;
            };
            let group = self.group(conn, c, gid, &mut pass).await?;
            let held = incomplete_sets(c, &group.merged);
            let target = group.target();
            let mut wrote = false;
            for set in &c.together {
                let fields: Vec<(&str, &Value)> = set
                    .iter()
                    .filter_map(|f| {
                        let i = c.field_index(f)?;
                        Some((f.as_str(), values.get(i)?))
                    })
                    .collect();
                if !held.contains(set[0].as_str()) || fields.iter().all(|(_, v)| **v == Value::Null)
                {
                    continue;
                }
                let stamp = clock.tick(now);
                for (field, value) in fields {
                    emit(
                        conn,
                        &Change {
                            collection: c.name.clone(),
                            gid: target,
                            field: field.into(),
                            value: value.clone(),
                            clock: stamp,
                        },
                    )
                    .await?;
                }
                wrote = true;
            }
            if wrote {
                exec(
                    conn,
                    "INSERT OR IGNORE INTO fongo_pending (collection, gid) VALUES (?, ?)",
                    &[Value::Text(c.name.clone()), Value::Ref(group.canon)],
                )
                .await?;
            }
        }
        save_clock(conn, &clock).await
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
            // A change no honest replica makes is ignored, and alike everywhere: a clock beyond
            // what the text form holds would be read back as another, and a tombstone is 0 or 1.
            if !change.clock.encodable()
                || (change.field == DELETED
                    && !matches!(change.value, Value::Int(0) | Value::Int(1)))
            {
                continue;
            }
            pass.clock.observe(&change.clock);
            let idx = self.registry.index(&change.collection);
            if change.field == ALIAS {
                // Kept whatever its clock: aliases only accumulate, so none is ever superseded.
                let Value::Ref(other) = &change.value else {
                    continue;
                };
                exec(
                    &mut tx,
                    "INSERT OR IGNORE INTO fongo_alias_pairs (collection, gid, other) VALUES (?, ?, ?)",
                    &[
                        Value::Text(change.collection.clone()),
                        Value::Ref(change.gid),
                        Value::Ref(*other),
                    ],
                )
                .await?;
                // An alias to an entity not seen here waits for that entity's own changes.
                if has_shadow(&mut tx, &change.collection, *other).await? {
                    let joined = self
                        .alias(
                            &mut tx,
                            idx,
                            &change.collection,
                            change.gid,
                            *other,
                            &mut pass,
                        )
                        .await?;
                    applied.accepted += usize::from(joined);
                }
                continue;
            }
            let strategy = self.registry.strategy(&change.collection, &change.field);
            let shadow = shadow_of(&mut tx, &change.collection, change.gid).await?;
            let held = shadow.get(&change.field).map(|(v, c)| (v, c));
            if wins(strategy, held, &change.value, &change.clock) {
                shadow_put(&mut tx, change).await?;
                applied.accepted += 1;
                if let Some(idx) = idx {
                    touched.insert((idx, change.gid));
                    let c = &self.registry.collections()[idx];
                    let owning = c
                        .field_index(&change.field)
                        .is_some_and(|f| c.owns_through(&c.fields[f]));
                    if change.field == DELETED || owning {
                        pass.suspect.insert((idx, change.gid));
                    }
                }
            }
            // Whatever path a key field took into the shadow, the aliases it implies follow.
            if let Some(idx) = idx {
                self.settle_natural(&mut tx, idx, change.gid, &mut pass)
                    .await?;
            }
            let waiting = query(
                &mut tx,
                "SELECT gid FROM fongo_alias_pairs WHERE collection = ? AND other = ?",
                &[
                    Value::Text(change.collection.clone()),
                    Value::Ref(change.gid),
                ],
            )
            .await?;
            for row in waiting {
                let gid = as_gid(&row[0])?;
                self.alias(&mut tx, idx, &change.collection, gid, change.gid, &mut pass)
                    .await?;
            }
        }
        for row in query(&mut tx, "SELECT collection, gid FROM fongo_pending", &[]).await? {
            if let Some(idx) = self.registry.index(as_text(&row[0])?) {
                touched.insert((idx, as_gid(&row[1])?));
            }
        }

        let waiting = self.settle(&mut tx, touched, &mut pass).await?;
        exec(&mut tx, "DELETE FROM fongo_pending", &[]).await?;
        applied.waiting = self.hold(&mut tx, waiting).await?;
        let leftovers = self.retry_leftovers(&mut tx, &mut pass).await?;
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
        applied.waiting += leftovers.len();
        applied.held.extend(leftovers);
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
        let mut latest: BTreeMap<(String, Vec<u8>, String), &Change> = BTreeMap::new();
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
            let canon = canon_of(conn, &c.name, change.gid).await?;
            for member in members_of(conn, &c.name, canon).await? {
                let rows = query(
                    conn,
                    "SELECT base_clock, local_clock, reported FROM fongo_bases
                     WHERE collection = ? AND gid = ? AND field = ?",
                    &[
                        Value::Text(c.name.clone()),
                        Value::Ref(member),
                        Value::Text(change.field.clone()),
                    ],
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
                let slot = (c.name.clone(), member.0.to_vec(), change.field.clone());
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
            let member = Value::Blob(member);
            let collection = Value::Text(collection);
            exec(
                conn,
                "UPDATE fongo_bases SET reported = ? WHERE collection = ? AND gid = ? AND field = ?",
                &[
                    Value::Text(remote.clock.encode()),
                    collection.clone(),
                    member.clone(),
                    Value::Text(field.clone()),
                ],
            )
            .await?;
            let rows = query(
                conn,
                "SELECT collection, key, base, base_is_ref, local, local_is_ref, local_clock,
                        base_clock
                 FROM fongo_bases WHERE collection = ? AND gid = ? AND field = ?",
                &[collection, member, Value::Text(field.clone())],
            )
            .await?;
            let Some(row) = rows.first() else {
                continue;
            };
            let Some(idx) = self.registry.index(as_text(&row[0])?) else {
                continue;
            };
            let c = &self.registry.collections()[idx];
            let canon = canon_of(conn, &c.name, remote.gid).await?;
            let key = match &c.key {
                Key::Minted { .. } => query(
                    conn,
                    "SELECT key FROM fongo_ids WHERE collection = ? AND gid = ?",
                    &[Value::Text(c.name.clone()), Value::Ref(canon)],
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
        let mut pass = Pass::new(Clock::new(gid));
        let group = self
            .group(conn, &self.registry.collections()[idx], gid, &mut pass)
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
        let canon = canon_of(conn, &c.name, gid).await?;
        let Key::Natural { fields } = &c.key else {
            return query(
                conn,
                "SELECT key FROM fongo_ids WHERE collection = ? AND gid = ?",
                &[Value::Text(c.name.clone()), Value::Ref(canon)],
            )
            .await?
            .first()
            .map(|row| as_text(&row[0]).map(str::to_string))
            .transpose();
        };
        let mut pass = Pass::new(Clock::new(canon));
        let group = self.group(conn, c, canon, &mut pass).await?;
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
                self.pass_on(conn, idx, gid, pass).await?;
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
            let removing = !removals.is_empty();
            for (idx, gid, doomed) in removals {
                if self.remove(conn, idx, gid, doomed, pass).await? {
                    progressed = true;
                } else {
                    waiting.insert((idx, gid));
                }
            }
            // Once for all the removals: a removal's cascade is found by what is no longer there.
            if removing {
                self.forget_cascaded(conn, pass).await?;
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
            let name = &self.registry.collections()[idx].name;
            let gid = canon_of(conn, name, gid).await?;
            held += exec(
                conn,
                "INSERT OR IGNORE INTO fongo_pending (collection, gid) VALUES (?, ?)",
                &[Value::Text(name.clone()), Value::Ref(gid)],
            )
            .await?;
        }
        Ok(usize::try_from(held).unwrap_or(usize::MAX))
    }

    /// An entity, every other known to be the same, and their merged state.
    async fn group(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        gid: Gid,
        pass: &mut Pass,
    ) -> Result<Group> {
        let canon = canon_of(conn, &c.name, gid).await?;
        let Some(idx) = self.registry.index(&c.name) else {
            return corrupt("a collection is not in the registry");
        };
        pass.walked = 0;
        pass.walking.insert((idx, canon));
        let members = self.members_fates(conn, idx, canon, pass, 0).await;
        pass.walking.remove(&(idx, canon));
        let members = members?;
        let fate = members
            .iter()
            .fold(Fate::Dead, |fate, (_, _, member, _)| fate.any(*member));
        let live: Vec<Gid> = members
            .iter()
            .filter(|(_, _, member, _)| *member == Fate::Alive)
            .map(|(gid, _, _, _)| *gid)
            .collect();
        let mut merged = Shadow::new();
        for (member, shadow, _, _) in members {
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
            fate,
        })
    }

    /// Each member of a group, its shadow, its fate, and whether that fate assumed a group still
    /// being worked out was alive.
    async fn members_fates(
        &self,
        conn: &mut SqliteConnection,
        idx: usize,
        canon: Gid,
        pass: &mut Pass,
        depth: usize,
    ) -> Result<Vec<(Gid, Shadow, Fate, bool)>> {
        let c = &self.registry.collections()[idx];
        let mut out = Vec::new();
        for member in members_of(conn, &c.name, canon).await? {
            let shadow = shadow_of(conn, &c.name, member).await?;
            // A natural-key entity whose key does not derive its id is no entity at all.
            if matches!(c.key, Key::Natural { .. })
                && own_key(c, &shadow).is_some_and(|derived| derived != member)
            {
                continue;
            }
            let (fate, assumed) = match shadow.get(DELETED) {
                Some((Value::Int(1), _)) => (Fate::Dead, false),
                Some((Value::Int(0), _)) => {
                    let mut fate = Fate::Alive;
                    let mut assumed = false;
                    for field in c.fields.iter().filter(|f| c.owns_through(f)) {
                        let Kind::Ref { collection, .. } = &field.kind else {
                            continue;
                        };
                        let in_key = matches!(&c.key, Key::Natural { fields } if fields.contains(&field.name));
                        let owner = match shadow.get(&field.name) {
                            Some((Value::Ref(target), _)) => {
                                match self.registry.index(collection) {
                                    Some(t) => {
                                        let (owner, guess) =
                                            self.fate(conn, t, *target, pass, depth + 1).await?;
                                        assumed |= guess;
                                        owner
                                    }
                                    None => Fate::Unknown,
                                }
                            }
                            // A key that names no row is not a key yet.
                            _ if in_key => Fate::Unknown,
                            _ => Fate::Alive,
                        };
                        fate = fate.all(owner);
                        if fate == Fate::Dead {
                            break;
                        }
                    }
                    (fate, assumed)
                }
                _ => (Fate::Unknown, false),
            };
            out.push((member, shadow, fate, assumed));
        }
        Ok(out)
    }

    /// The fate of the group an entity belongs to. Each group is worked out once per walk and
    /// remembered for the pass; a walk that goes too deep or too wide is left undecided.
    fn fate<'a>(
        &'a self,
        conn: &'a mut SqliteConnection,
        idx: usize,
        gid: Gid,
        pass: &'a mut Pass,
        depth: usize,
    ) -> Walk<'a> {
        Box::pin(async move {
            let c = &self.registry.collections()[idx];
            let canon = canon_of(conn, &c.name, gid).await?;
            if let Some(fate) = pass.fates.get(&(idx, canon)) {
                return Ok((*fate, false));
            }
            if pass.walking.contains(&(idx, canon)) {
                return Ok((Fate::Alive, true));
            }
            pass.walked += 1;
            if depth > OWNER_DEPTH || pass.walked > OWNER_LIMIT {
                return Ok((Fate::Undecided, false));
            }
            pass.walking.insert((idx, canon));
            let members = self.members_fates(conn, idx, canon, pass, depth).await;
            pass.walking.remove(&(idx, canon));
            let members = members?;
            let fate = members
                .iter()
                .fold(Fate::Dead, |fate, (_, _, member, _)| fate.any(*member));
            let assumed = members.iter().any(|(_, _, _, assumed)| *assumed);
            // An answer that leaned on a guess about a group still open is not kept.
            if !assumed && fate != Fate::Undecided {
                pass.fates.insert((idx, canon), fate);
            }
            Ok((fate, assumed))
        })
    }

    /// Takes an alias into account, in a collection this registry may not have.
    async fn alias(
        &self,
        conn: &mut SqliteConnection,
        idx: Option<usize>,
        collection: &str,
        a: Gid,
        b: Gid,
        pass: &mut Pass,
    ) -> Result<bool> {
        match idx {
            Some(idx) => self.join(conn, idx, a, b, pass).await,
            None => {
                pass.fates.clear();
                Ok(union(conn, collection, a, b).await?.is_some())
            }
        }
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
        let c = &self.registry.collections()[idx];
        let Some(canon) = union(conn, &c.name, a, b).await? else {
            return Ok(false);
        };
        pass.writes += 1;
        pass.fates.clear();
        pass.more.insert((idx, canon));
        let Key::Minted { .. } = &c.key else {
            return Ok(true);
        };
        let members = members_of(conn, &c.name, canon).await?;

        let mut rows: Vec<(Gid, String)> = Vec::new();
        for &member in &members {
            let found = query(
                conn,
                "SELECT key FROM fongo_ids WHERE collection = ? AND gid = ?",
                &[Value::Text(c.name.clone()), Value::Ref(member)],
            )
            .await?;
            if let Some(row) = found.first() {
                rows.push((member, as_text(&row[0])?.to_string()));
            }
        }
        rows.sort_by_key(|(gid, _)| (*gid != canon, *gid));
        if let Some(((_, keep), losers)) = rows.split_first() {
            for (_, lose) in losers {
                let given = self.give_up_row(conn, idx, lose, keep, canon, pass).await?;
                exec(
                    conn,
                    "DELETE FROM fongo_ids WHERE collection = ? AND key = ?",
                    &[Value::Text(c.name.clone()), Value::Text(lose.clone())],
                )
                .await?;
                if !given {
                    // Another table will not let the row go: the merge of the two rows waits.
                    exec(
                        conn,
                        "INSERT OR REPLACE INTO fongo_leftover (collection, key, gid) VALUES (?, ?, ?)",
                        &[
                            Value::Text(c.name.clone()),
                            Value::Text(lose.clone()),
                            Value::Ref(canon),
                        ],
                    )
                    .await?;
                }
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
        self.refer_again(conn, &c.name, canon, pass).await?;
        Ok(true)
    }

    /// When an entity's fate may have changed, so may that of everything referring to it, which
    /// is queued in turn; each entity is looked at once a pass.
    async fn pass_on(
        &self,
        conn: &mut SqliteConnection,
        idx: usize,
        gid: Gid,
        pass: &mut Pass,
    ) -> Result<()> {
        let c = &self.registry.collections()[idx];
        let canon = canon_of(conn, &c.name, gid).await?;
        let suspect = pass.suspect.remove(&(idx, gid)) | pass.suspect.remove(&(idx, canon));
        if suspect && pass.told.insert((idx, canon)) {
            self.refer_again(conn, &c.name, canon, pass).await?;
        }
        Ok(())
    }

    /// Queues every entity that refers to a group, to be brought in line again.
    async fn refer_again(
        &self,
        conn: &mut SqliteConnection,
        collection: &str,
        canon: Gid,
        pass: &mut Pass,
    ) -> Result<()> {
        for member in members_of(conn, collection, canon).await? {
            let referring = query(
                conn,
                "SELECT DISTINCT collection, gid, field FROM fongo_shadow
                 WHERE is_ref = 1 AND value = ?",
                &[Value::Ref(member)],
            )
            .await?;
            for row in referring {
                let Some(i) = self.registry.index(as_text(&row[0])?) else {
                    continue;
                };
                let d = &self.registry.collections()[i];
                let field = as_text(&row[2])?;
                let names_it = d.field_index(field).is_some_and(|f| {
                    matches!(&d.fields[f].kind, Kind::Ref { collection: target, .. } if target == collection)
                });
                if names_it {
                    let referrer = (i, as_gid(&row[1])?);
                    pass.more.insert(referrer);
                    pass.suspect.insert(referrer);
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
                let canon = canon_of(conn, collection, *gid).await?;
                let rows = query(
                    conn,
                    "SELECT json_extract(key, '$[0]') FROM fongo_ids WHERE collection = ? AND gid = ?",
                    &[Value::Text(collection.clone()), Value::Ref(canon)],
                )
                .await?;
                let local = rows.into_iter().next().and_then(|mut row| row.pop());
                let fate = match self.registry.index(collection) {
                    Some(target) => {
                        pass.walked = 0;
                        self.fate(conn, target, canon, pass, 0).await?.0
                    }
                    None => Fate::Unknown,
                };
                Ok(match (fate, local) {
                    (Fate::Undecided, _) => Local::Undecided,
                    (Fate::Dead, local) => Local::Gone(local.unwrap_or(Value::Null)),
                    (Fate::Alive, Some(local)) => Local::Value(local),
                    (Fate::Alive | Fate::Unknown, _) => Local::Unknown,
                })
            }
            (Kind::Ref { .. }, _) => Ok(Local::Value(Value::Null)),
        }
    }

    /// Removes an entity's row. Rows that referred to it are brought in line again, in case their
    /// merged state says otherwise than the database did; rows it cascaded to are found once the
    /// round's removals are done. Returns false when the row must wait.
    async fn remove(
        &self,
        conn: &mut SqliteConnection,
        idx: usize,
        gid: Gid,
        doomed: Doomed,
        pass: &mut Pass,
    ) -> Result<bool> {
        let c = &self.registry.collections()[idx];
        let canon = canon_of(conn, &c.name, gid).await?;
        let table = quoted(&c.table);
        match (&c.key, doomed) {
            (Key::Minted { column }, _) => {
                let rows = query(
                    conn,
                    "SELECT key FROM fongo_ids WHERE collection = ? AND gid = ?",
                    &[Value::Text(c.name.clone()), Value::Ref(canon)],
                )
                .await?;
                let Some(key) = rows.first().map(|row| as_text(&row[0])).transpose()? else {
                    // No row, but what refers to it may hold its fields or be owned by it.
                    if !pass.told.contains(&(idx, canon)) {
                        pass.suspect.insert((idx, canon));
                        pass.more.insert((idx, canon));
                    }
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
                    "DELETE FROM fongo_ids WHERE collection = ? AND gid = ?",
                    &[Value::Text(c.name.clone()), Value::Ref(canon)],
                )
                .await?;
                forget_deviations(conn, &c.name, &key, None).await?;
                if removed > 0 {
                    pass.touch(idx, key, canon, true);
                }
                for (d_idx, d_key, d_gid) in doomed {
                    let d = &self.registry.collections()[d_idx];
                    if !self.row_exists(conn, d, &d_key).await? {
                        forget_deviations(conn, &d.name, &d_key, None).await?;
                        pass.touch(d_idx, d_key, d_gid, true);
                    }
                }
                self.refer_again(conn, &c.name, canon, pass).await?;
            }
            (Key::Natural { fields }, Doomed::Natural(key)) => {
                let matches: Vec<String> = fields
                    .iter()
                    .map(|k| format!("{} = ?", quoted(k)))
                    .collect();
                let removed = match exec(
                    conn,
                    &format!("DELETE FROM {table} WHERE {}", matches.join(" AND ")),
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
                        let json = as_text(&row[0])?.to_string();
                        forget_deviations(conn, &c.name, &json, None).await?;
                        pass.touch(idx, json, canon, true);
                    }
                }
            }
            (Key::Natural { .. }, Doomed::Minted) => {}
        }
        Ok(true)
    }

    /// Moves what refers to row `lose` to row `keep`, then deletes `lose`. Returns false when a
    /// table the kernel does not know will not let it go.
    async fn give_up_row(
        &self,
        conn: &mut SqliteConnection,
        idx: usize,
        lose: &str,
        keep: &str,
        canon: Gid,
        pass: &mut Pass,
    ) -> Result<bool> {
        let c = &self.registry.collections()[idx];
        let Key::Minted { column } = &c.key else {
            return Ok(true);
        };
        let column = column.clone();
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
                &[Value::Text(lose.into()), Value::Text(keep.into())],
            )
            .await?;
        }
        let gone = exec(
            conn,
            &format!(
                "DELETE FROM {} WHERE {} = json_extract(?1, '$[0]')",
                quoted(&c.table),
                quoted(&column)
            ),
            &[Value::Text(lose.into())],
        )
        .await;
        match gone {
            Ok(_) => {}
            Err(error) if is_constraint(&error) => return Ok(false),
            Err(error) => return Err(error.into()),
        }
        forget_deviations(conn, &c.name, lose, None).await?;
        exec(
            conn,
            "DELETE FROM fongo_leftover WHERE collection = ? AND key = ?",
            &[Value::Text(c.name.clone()), Value::Text(lose.into())],
        )
        .await?;
        pass.touch(idx, lose.into(), canon, true);
        Ok(true)
    }

    /// Tries again each row a merge gave up but could not delete. Returns those still left.
    async fn retry_leftovers(
        &self,
        conn: &mut SqliteConnection,
        pass: &mut Pass,
    ) -> Result<Vec<Held>> {
        let mut left = Vec::new();
        let rows = query(
            conn,
            "SELECT collection, key, gid FROM fongo_leftover ORDER BY collection, key",
            &[],
        )
        .await?;
        for row in rows {
            let (name, lose) = (as_text(&row[0])?.to_string(), as_text(&row[1])?.to_string());
            let Some(idx) = self.registry.index(&name) else {
                continue;
            };
            let canon = canon_of(conn, &name, as_gid(&row[2])?).await?;
            let keep = query(
                conn,
                "SELECT key FROM fongo_ids WHERE collection = ? AND gid = ?",
                &[Value::Text(name.clone()), Value::Ref(canon)],
            )
            .await?
            .first()
            .map(|row| as_text(&row[0]).map(str::to_string))
            .transpose()?;
            let done = match keep {
                Some(keep) => {
                    self.give_up_row(conn, idx, &lose, &keep, canon, pass)
                        .await?
                }
                None => false,
            };
            if !done {
                left.push(Held {
                    collection: name,
                    gid: canon,
                    key: Some(lose),
                });
            }
        }
        Ok(left)
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
            forget_deviations(conn, &self.registry.collections()[d_idx].name, &key, None).await?;
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
        let shadow = shadow_of(conn, &c.name, gid).await?;
        if own_key(c, &shadow) != Some(gid) {
            return Ok(());
        }
        let mut options = Vec::with_capacity(fields.len());
        for name in fields {
            let (Some((value, _)), Some(i)) = (shadow.get(name), c.field_index(name)) else {
                return Ok(());
            };
            options.push(spellings(conn, &c.fields[i], value).await?);
        }
        for key in choices(&options) {
            let other = Gid::derive(&c.name, &key);
            if other != gid
                && has_key(conn, c, other).await?
                && let Some(canon) = union(conn, &c.name, gid, other).await?
            {
                pass.writes += 1;
                pass.fates.clear();
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
                let other = canon_of(conn, &c.name, other).await?;
                if other == canon {
                    continue;
                }
                let theirs = self.group(conn, c, other, pass).await?;
                if theirs.fate == Fate::Alive {
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

    /// Records, for each field of a row the kernel just wrote or tried to, the value it left there
    /// when that is not the merged value: a row waiting on a constraint, a reference to a row not
    /// here, a stand-in for an identity, part of a set not yet written whole. Collecting compares
    /// a row with these, so none of them is taken for an edit by the application.
    async fn note_deviations(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        key: &str,
        merged: &Shadow,
    ) -> Result<()> {
        let pins = pins(conn, &c.name, key, merged).await?;
        forget_deviations(conn, &c.name, key, None).await?;
        let Some(values) = self.read_row(conn, c, key).await? else {
            return Ok(());
        };
        for (field, value) in c.fields.iter().zip(values) {
            let held = merged
                .get(&field.name)
                .map_or(Value::Null, |(v, _)| v.clone());
            if same(conn, field, &held, &value).await? {
                continue;
            }
            let pinned = match pins.get(&field.name) {
                Some(pin) => same(conn, field, pin, &value).await?,
                None => false,
            };
            note(
                conn,
                &c.name,
                key,
                &field.name,
                &value,
                pinned.then(|| merged.get(&field.name)),
            )
            .await?;
        }
        Ok(())
    }

    /// Whether a row names an owner that holds no id here.
    async fn owner_unknown(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        key: &str,
    ) -> Result<bool> {
        for field in c.fields.iter().filter(|f| c.owns_through(f)) {
            if !matches!(field.kind, Kind::Ref { .. }) {
                continue;
            }
            let rows = query(
                conn,
                &format!(
                    "SELECT {} FROM {} WHERE {}",
                    quoted(&field.name),
                    quoted(&c.table),
                    where_key_json(c)
                ),
                &[Value::Text(key.into())],
            )
            .await?;
            let Some(local) = rows.into_iter().next().and_then(|mut row| row.pop()) else {
                continue;
            };
            if local != Value::Null && to_shared(conn, field, local).await? == Value::Null {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The key of the row an entity has or would have here.
    async fn key_of(
        &self,
        conn: &mut SqliteConnection,
        c: &Collection,
        canon: Gid,
        columns: &[(&Field, Value)],
        leaving: &BTreeMap<&str, Value>,
    ) -> Result<Option<String>> {
        match &c.key {
            Key::Minted { .. } => query(
                conn,
                "SELECT key FROM fongo_ids WHERE collection = ? AND gid = ?",
                &[Value::Text(c.name.clone()), Value::Ref(canon)],
            )
            .await?
            .first()
            .map(|row| as_text(&row[0]).map(str::to_string))
            .transpose(),
            Key::Natural { fields } => {
                let mut key = Vec::with_capacity(fields.len());
                for name in fields {
                    let value = match columns.iter().find(|(f, _)| f.name == *name) {
                        Some((_, Value::Null)) | None => leaving.get(name.as_str()),
                        Some((_, value)) => Some(value),
                    };
                    match value {
                        Some(value) if *value != Value::Null => key.push(value.clone()),
                        _ => return Ok(None),
                    }
                }
                let marks = vec!["?"; key.len()].join(", ");
                let json = query(conn, &format!("SELECT json_array({marks})"), &key).await?;
                json.first()
                    .map(|row| as_text(&row[0]).map(str::to_string))
                    .transpose()
            }
        }
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
        if has_key(conn, c, derived).await? {
            return Ok(Some(canon_of(conn, &c.name, derived).await?));
        }
        let mut options = Vec::with_capacity(key_fields.len());
        for name in key_fields {
            let Some(i) = c.field_index(name) else {
                return Ok(None);
            };
            let value = values.get(i).unwrap_or(&Value::Null);
            options.push(spellings(conn, &c.fields[i], value).await?);
        }
        for key in choices(&options) {
            let gid = Gid::derive(&c.name, &key);
            if has_key(conn, c, gid).await? {
                return Ok(Some(canon_of(conn, &c.name, gid).await?));
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
        let mut pass = Pass::new(load_clock(conn).await?);

        // Natural ids of rows that went away are derived before any minted id below is retired.
        let mut tombstones: Vec<(usize, Gid, u64)> = Vec::new();
        for e in entries.iter().filter(|e| e.gone) {
            let c = &collections[e.idx];
            forget_deviations(conn, &c.name, &e.key, None).await?;
            if let Key::Natural { fields } = &c.key
                && let Some(gid) = self.gone_natural_gid(conn, c, fields, &e.key).await?
            {
                tombstones.push((e.idx, gid, e.at));
            }
        }

        let mut outside: BTreeSet<(usize, String)> = BTreeSet::new();
        for e in &entries {
            let c = &collections[e.idx];
            if !matches!(c.key, Key::Minted { .. }) {
                continue;
            }
            // A row a merge gave up is not an entity of its own, whatever is done to it.
            let leftover = !query(
                conn,
                "SELECT 1 FROM fongo_leftover WHERE collection = ? AND key = ?",
                &[Value::Text(c.name.clone()), Value::Text(e.key.clone())],
            )
            .await?
            .is_empty();
            if leftover {
                if self.read_row(conn, c, &e.key).await?.is_none() {
                    exec(
                        conn,
                        "DELETE FROM fongo_leftover WHERE collection = ? AND key = ?",
                        &[Value::Text(c.name.clone()), Value::Text(e.key.clone())],
                    )
                    .await?;
                }
                outside.insert((e.idx, e.key.clone()));
                continue;
            }
            let mut mapped = minted_gid(conn, &c.name, &e.key).await?;
            if e.gone
                && let Some(gid) = mapped.take()
            {
                tombstones.push((e.idx, gid, e.at));
                exec(
                    conn,
                    "DELETE FROM fongo_ids WHERE collection = ? AND gid = ?",
                    &[Value::Text(c.name.clone()), Value::Ref(gid)],
                )
                .await?;
            }
            if mapped.is_none() && self.read_row(conn, c, &e.key).await?.is_some() {
                // A new entity: whatever was left in a row once at this key is not its.
                forget_deviations(conn, &c.name, &e.key, None).await?;
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
        let mut unowned: Vec<&Entry> = Vec::new();
        for e in &entries {
            let c = &collections[e.idx];
            if outside.contains(&(e.idx, e.key.clone())) {
                continue;
            }
            let Some(values) = self.read_row(conn, c, &e.key).await? else {
                continue;
            };
            // A row owned by one the kernel has no id for is outside sync until its owner is in.
            if self.owner_unknown(conn, c, &e.key).await? {
                unowned.push(e);
                continue;
            }
            let gid = match &c.key {
                Key::Minted { .. } => minted_gid(conn, &c.name, &e.key).await?,
                Key::Natural { fields } => self.resolve_natural(conn, c, fields, &values).await?,
            };
            let Some(gid) = gid else { continue };
            let group = self.group(conn, c, gid, &mut pass).await?;
            present.insert(group.canon);
            // A natural-key row's writes go to the id its own key derives, which holds that key.
            let gid = match &c.key {
                Key::Natural { fields } => {
                    Self::natural_gid(c, fields, &values).unwrap_or_else(|| group.target())
                }
                Key::Minted { .. } => group.target(),
            };
            let mut merged = group.merged;
            let mut changes = Vec::new();
            // A row the application writes is alive, whatever the kernel made of its group.
            let fresh = merged.is_empty() || group.fate != Fate::Alive;
            if fresh {
                // Made, or brought back to life: the member revived holds only what it held.
                changes.push((DELETED.to_string(), Value::Int(0)));
                merged = shadow_of(conn, &c.name, gid).await?;
                merged.remove(DELETED);
            }
            if fresh {
                forget_deviations(conn, &c.name, &e.key, None).await?;
            }
            let left = deviations(conn, &c.name, &e.key).await?;
            let row: Vec<(&Field, Value)> = c.fields.iter().zip(values).collect();
            let mut changed: BTreeSet<&str> = BTreeSet::new();
            for (field, value) in &row {
                let value = value.clone();
                // What the kernel left in the field is not an edit, unless written over.
                if let Some(left) = left.get(&field.name)
                    && same(conn, field, left, &value).await?
                {
                    continue;
                }
                let unchanged = match merged.get(&field.name) {
                    Some((held, _)) => same(conn, field, held, &value).await?,
                    None => value == Value::Null,
                };
                if !unchanged {
                    changed.insert(&field.name);
                }
            }
            for set in &c.together {
                // A set is recorded whole, so a write to it is never partial.
                if set.iter().any(|f| changed.contains(f.as_str())) {
                    changed.extend(set.iter().map(String::as_str));
                }
            }
            // A natural-key member written to holds its own key and tombstone, so it can be
            // joined, and counted alive, anywhere.
            if let Key::Natural { fields } = &c.key
                && (!changes.is_empty() || !changed.is_empty())
            {
                let own = shadow_of(conn, &c.name, gid).await?;
                for name in fields {
                    if !own.contains_key(name) {
                        changed.insert(name.as_str());
                    }
                }
                if !fresh && !own.contains_key(DELETED) {
                    changes.insert(0, (DELETED.to_string(), Value::Int(0)));
                }
            }
            for (field, value) in row {
                if changed.contains(field.name.as_str()) {
                    changes.push((field.name.clone(), value));
                }
            }
            let mut stamps: BTreeMap<usize, Hlc> = BTreeMap::new();
            for (field, value) in changes {
                let stamp = match c.together_of(&field) {
                    Some(set) => *stamps.entry(set).or_insert_with(|| pass.clock.tick(e.at)),
                    None => pass.clock.tick(e.at),
                };
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
                pass.fates.clear();
                if change.field == DELETED
                    || c.field_index(&change.field)
                        .is_some_and(|f| c.owns_through(&c.fields[f]))
                {
                    // What refers to it here may live or die with it.
                    pass.suspect.insert((e.idx, gid));
                    overruled.insert((e.idx, gid));
                }
                if reports {
                    let base = merged.get(&change.field);
                    remember_base(conn, group.canon, &e.key, base, &change).await?;
                }
                produced += 1;
            }
            self.settle_natural(conn, e.idx, gid, &mut pass).await?;
            let merged = self.group(conn, c, gid, &mut pass).await?.merged;
            self.note_deviations(conn, c, &e.key, &merged).await?;
        }

        for (idx, gid, at) in tombstones {
            let c = &collections[idx];
            let group = self.group(conn, c, gid, &mut pass).await?;
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
                pass.fates.clear();
                pass.suspect.insert((idx, member));
                overruled.insert((idx, member));
                produced += 1;
            }
        }

        exec(conn, "DELETE FROM fongo_dirty", &[]).await?;
        for e in unowned {
            exec(
                conn,
                "INSERT INTO fongo_dirty (collection, key, at) VALUES (?, ?, ?)",
                &[
                    Value::Text(collections[e.idx].name.clone()),
                    Value::Text(e.key.clone()),
                    Value::Int(i64::try_from(e.at).unwrap_or(0)),
                ],
            )
            .await?;
        }
        if !overruled.is_empty() {
            // A local write its field's strategy refused goes back to the merged value, and what
            // refers to an entity made, revived or deleted here is brought in line.
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
            let Group {
                canon,
                merged,
                fate,
                ..
            } = self.group(conn, c, gid, pass).await?;
            if merged.is_empty() {
                return Ok(Outcome::Settled);
            }
            if matches!(fate, Fate::Unknown | Fate::Undecided) {
                // Not known yet to have a row: whatever row there is stays as it is.
                if let Key::Minted { .. } = &c.key
                    && let Some(key) = query(
                        conn,
                        "SELECT key FROM fongo_ids WHERE collection = ? AND gid = ?",
                        &[Value::Text(c.name.clone()), Value::Ref(canon)],
                    )
                    .await?
                    .first()
                {
                    let key = as_text(&key[0])?.to_string();
                    self.note_deviations(conn, c, &key, &merged).await?;
                }
                return Ok(Outcome::Waiting);
            }
            let deleted = fate == Fate::Dead;
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

            // Fields that change together are written only once one write holds all of them;
            // until then the row keeps what it has.
            let mut held_back: BTreeSet<String> = incomplete_sets(c, &merged)
                .into_iter()
                .map(str::to_string)
                .collect();
            if let Some(key) = self.key_of(conn, c, canon, &columns, &leaving).await? {
                held_back.extend(pins(conn, &c.name, &key, &merged).await?.into_keys());
            }
            columns.retain(|(f, _)| !held_back.contains(&f.name));

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
                        "SELECT key FROM fongo_ids WHERE collection = ? AND gid = ?",
                        &[Value::Text(c.name.clone()), Value::Ref(canon)],
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
                                self.refer_again(conn, &c.name, canon, pass).await?;
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

            if let (Ok(n), Some(key)) = (&written, &written_key)
                && *n > 0
            {
                pass.touch(idx, key.clone(), canon, false);
            }
            let row = match &c.key {
                Key::Minted { .. } => query(
                    conn,
                    "SELECT key FROM fongo_ids WHERE collection = ? AND gid = ?",
                    &[Value::Text(c.name.clone()), Value::Ref(canon)],
                )
                .await?
                .first()
                .map(|row| as_text(&row[0]).map(str::to_string))
                .transpose()?,
                Key::Natural { .. } => match written_key {
                    Some(key) if self.row_exists(conn, c, &key).await? => Some(key),
                    _ => None,
                },
            };
            if let Some(row) = row {
                self.note_deviations(conn, c, &row, &merged).await?;
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
                let key = as_text(&row[0])?.to_string();
                forget_deviations(conn, &c.name, &key, None).await?;
                pass.touch(idx, key, gid, true);
                pass.more.insert((idx, gid));
                exec(
                    conn,
                    "DELETE FROM fongo_ids WHERE collection = ? AND gid = ?",
                    &[Value::Text(c.name.clone()), Value::Ref(gid)],
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
                Some(gid) => Ok(Some(canon_of(conn, &c.name, gid).await?)),
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

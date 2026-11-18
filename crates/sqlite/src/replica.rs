use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_core::{Change, Clock, DELETED, Gid, Hlc, Value, wins};
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
    "CREATE TABLE IF NOT EXISTS fongo_pending (gid BLOB PRIMARY KEY, collection TEXT NOT NULL)",
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

async fn shadow_of(
    conn: &mut SqliteConnection,
    gid: Gid,
) -> Result<BTreeMap<String, (Value, Hlc)>> {
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

fn is_deleted(shadow: &BTreeMap<String, (Value, Hlc)>) -> bool {
    matches!(shadow.get(DELETED), Some((Value::Int(1), _)))
}

async fn emit(conn: &mut SqliteConnection, change: &Change) -> Result<()> {
    shadow_put(conn, change).await?;
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
            let rows = query(
                conn,
                "SELECT json_extract(key, '$[0]') FROM fongo_ids WHERE gid = ?",
                &[Value::Ref(*gid)],
            )
            .await?;
            if let Some(row) = rows.into_iter().next() {
                return Ok(Local::Value(row[0].clone()));
            }
            let tombstone = query(
                conn,
                "SELECT value FROM fongo_shadow WHERE gid = ? AND field = ?",
                &[Value::Ref(*gid), Value::Text(DELETED.into())],
            )
            .await?;
            match tombstone.first().map(|row| &row[0]) {
                Some(Value::Int(1)) => Ok(Local::Gone),
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
        let produced = self.collect_in(&mut tx).await?;
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
        self.collect_in(&mut tx).await?;
        let mut clock = load_clock(&mut tx).await?;
        set_applying(&mut tx, true).await?;

        let mut applied = Applied::default();
        let mut touched: BTreeSet<(usize, Gid)> = BTreeSet::new();
        for change in changes {
            clock.observe(&change.clock);
            let strategy = if change.field == DELETED {
                fongo_sync_core::Strategy::Lww
            } else {
                self.registry.strategy(&change.collection, &change.field)
            };
            let shadow = shadow_of(&mut tx, change.gid).await?;
            let held = shadow.get(&change.field).map(|(v, c)| (v, c));
            if !wins(strategy, held, &change.value, &change.clock) {
                continue;
            }
            shadow_put(&mut tx, change).await?;
            applied.accepted += 1;
            if let Some(idx) = self.registry.index(&change.collection) {
                touched.insert((idx, change.gid));
            }
        }
        for row in query(&mut tx, "SELECT gid, collection FROM fongo_pending", &[]).await? {
            if let Some(idx) = self.registry.index(as_text(&row[1])?) {
                touched.insert((idx, as_gid(&row[0])?));
            }
        }

        let mut round = touched;
        loop {
            let mut waiting = BTreeSet::new();
            for &(idx, gid) in &round {
                match self.materialise(&mut tx, idx, gid).await? {
                    // Before the next insert can be handed a local id the cascade just freed.
                    Outcome::Settled { deleted: true } => self.forget_cascaded(&mut tx).await?,
                    Outcome::Settled { deleted: false } => {}
                    Outcome::Waiting => {
                        waiting.insert((idx, gid));
                    }
                }
            }
            let stuck = waiting.len() == round.len();
            round = waiting;
            if stuck || round.is_empty() {
                break;
            }
        }
        exec(&mut tx, "DELETE FROM fongo_pending", &[]).await?;
        for (idx, gid) in &round {
            exec(
                &mut tx,
                "INSERT INTO fongo_pending (gid, collection) VALUES (?, ?)",
                &[
                    Value::Ref(*gid),
                    Value::Text(self.registry.collections()[*idx].name.clone()),
                ],
            )
            .await?;
        }
        applied.waiting = round.len();
        set_applying(&mut tx, false).await?;
        save_clock(&mut tx, &clock).await?;
        tx.commit().await?;
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
        Ok(Self::natural_gid(c, key_fields, &values))
    }

    async fn collect_in(&self, conn: &mut SqliteConnection) -> Result<usize> {
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
            return Ok(0);
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
        let mut clock = load_clock(conn).await?;

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
                Key::Natural { fields } => Self::natural_gid(c, fields, &values),
            };
            let Some(gid) = gid else { continue };
            present.insert(gid);
            let shadow = shadow_of(conn, gid).await?;
            let mut changes = Vec::new();
            if shadow.is_empty() || is_deleted(&shadow) {
                changes.push((DELETED.to_string(), Value::Int(0)));
            }
            for (field, value) in c.fields.iter().zip(values) {
                let unchanged = match shadow.get(&field.name) {
                    Some((held, _)) => *held == value,
                    None => value == Value::Null,
                };
                if !unchanged {
                    changes.push((field.name.clone(), value));
                }
            }
            for (field, value) in changes {
                let stamp = clock.tick(e.at);
                let strategy = self.registry.strategy(&c.name, &field);
                let held = shadow.get(&field).map(|(v, c)| (v, c));
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
            if present.contains(&gid) {
                continue;
            }
            let shadow = shadow_of(conn, gid).await?;
            if shadow.is_empty() || is_deleted(&shadow) {
                continue;
            }
            emit(
                conn,
                &Change {
                    collection: collections[idx].name.clone(),
                    gid,
                    field: DELETED.into(),
                    value: Value::Int(1),
                    clock: clock.tick(at),
                },
            )
            .await?;
            produced += 1;
        }

        exec(conn, "DELETE FROM fongo_dirty", &[]).await?;
        if !overruled.is_empty() {
            // A local write its field's strategy refused: the row goes back to the merged value.
            set_applying(conn, true).await?;
            for (idx, gid) in overruled {
                self.materialise(conn, idx, gid).await?;
            }
            set_applying(conn, false).await?;
        }
        save_clock(conn, &clock).await?;
        Ok(produced)
    }

    /// Makes one entity's row match its merged state.
    async fn materialise(
        &self,
        conn: &mut SqliteConnection,
        idx: usize,
        gid: Gid,
    ) -> Result<Outcome> {
        let c = &self.registry.collections()[idx];
        let shadow = shadow_of(conn, gid).await?;
        let deleted = is_deleted(&shadow);
        let table = quoted(&c.table);

        let mut columns: Vec<(&Field, Value)> = Vec::new();
        let mut unresolved = false;
        let mut orphaned = false;
        for field in &c.fields {
            let Some((shared, _)) = shadow.get(&field.name) else {
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

        let written = match &c.key {
            Key::Minted { column } => {
                let rows = query(
                    conn,
                    "SELECT key FROM fongo_ids WHERE gid = ?",
                    &[Value::Ref(gid)],
                )
                .await?;
                let key = rows.first().map(|row| row[0].clone());
                if deleted {
                    if let Some(key) = key {
                        exec(
                            conn,
                            &format!(
                                "DELETE FROM {table} WHERE {} = json_extract(?1, '$[0]')",
                                quoted(column)
                            ),
                            &[key],
                        )
                        .await?;
                        exec(
                            conn,
                            "DELETE FROM fongo_ids WHERE gid = ?",
                            &[Value::Ref(gid)],
                        )
                        .await?;
                    }
                    return Ok(Outcome::Settled { deleted: true });
                }
                match key {
                    Some(_) if columns.is_empty() => Ok(0),
                    Some(key) => {
                        let sets: Vec<String> = columns
                            .iter()
                            .map(|(f, _)| format!("{} = ?", quoted(&f.name)))
                            .collect();
                        let mut params: Vec<Value> =
                            columns.iter().map(|(_, v)| v.clone()).collect();
                        params.push(key);
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
                            exec(
                                conn,
                                "INSERT INTO fongo_ids (collection, key, gid)
                                 VALUES (?, json_array(last_insert_rowid()), ?)",
                                &[Value::Text(c.name.clone()), Value::Ref(gid)],
                            )
                            .await?;
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
                if deleted {
                    exec(conn, &format!("DELETE FROM {table} WHERE {matches}"), &key).await?;
                    return Ok(Outcome::Settled { deleted: true });
                }
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
                        let mut params: Vec<Value> = rest.iter().map(|(_, v)| v.clone()).collect();
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

        match written {
            Ok(_) if unresolved => Ok(Outcome::Waiting),
            Ok(_) => Ok(Outcome::Settled { deleted: false }),
            Err(error) if is_constraint(&error) => Ok(Outcome::Waiting),
            Err(error) => Err(error.into()),
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
    async fn forget_cascaded(&self, conn: &mut SqliteConnection) -> Result<()> {
        for c in self.registry.collections() {
            let Key::Minted { column } = &c.key else {
                continue;
            };
            exec(
                conn,
                &format!(
                    "DELETE FROM fongo_ids WHERE collection = ?1 AND NOT EXISTS (
                        SELECT 1 FROM {} WHERE {} = json_extract(fongo_ids.key, '$[0]'))",
                    quoted(&c.table),
                    quoted(column)
                ),
                &[Value::Text(c.name.clone())],
            )
            .await?;
        }
        Ok(())
    }
}

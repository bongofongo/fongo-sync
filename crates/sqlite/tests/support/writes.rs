//! What the application wrote, told apart from what the kernel wrote, by comparing its tables
//! before each kernel step with how the previous step left them, and by triggers of the test's
//! own that see a row made again at the same key or a field set back to what it held. A replica may send a field of an
//! entity only when the application wrote that field of its row, or wrote any part of a set the
//! field changes with, or made or removed the row; besides that it sends aliases, and what an
//! install adopts. Anything else it sends is a value the application did not write, such as a
//! field the kernel held back in a row and later took for an edit.
//!
//! In the other direction, every value the application wrote must be sent, and once everything
//! is exchanged, held in the merged state at the clock it was sent with or a later one
//! ([`Writes::kept`]).

use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_sqlite::core::model;
use fongo_sync_sqlite::core::{ALIAS, Change, DELETED, Gid, Value};
use fongo_sync_sqlite::{Key, Kind, Replica};
use sqlx::SqliteConnection;

use super::{key_columns, quoted, rows, text};

/// Rows by collection and key, each field by name, so rows read under two registries compare.
type Rows = BTreeMap<(String, String), BTreeMap<String, Value>>;

#[derive(Default, Clone)]
pub struct Writes {
    /// The rows as the last kernel step left them.
    after: Rows,
    /// The rows as the last kernel step found them.
    found: Rows,
    /// Every entity each row has held, by collection and key.
    gids: BTreeMap<(String, String), BTreeSet<Gid>>,
    /// The entity each row held when last looked at.
    latest: BTreeMap<(String, String), Gid>,
    /// Fields the application wrote since the last send, by collection, key and field;
    /// `$deleted` for a row made or removed.
    written: BTreeSet<(String, String, String)>,
    /// Rows the application made since the last send, at a new key or one a removed row held.
    made: BTreeSet<(String, String)>,
    /// Rows made since the last kernel step, with the entities rows at their keys held before.
    earlier: BTreeMap<(String, String), BTreeSet<Gid>>,
    /// Rows the application deleted since the last send, by collection and key, with the
    /// members of the entity's group marked live when it did.
    removed: BTreeMap<(String, String), BTreeSet<Gid>>,
    /// Changes an install produced, which are not checked.
    adopted: Vec<Change>,
    /// Rows a first install in steps found and has not adopted yet, as it found them: what the
    /// application writes to one meanwhile is an edit against that.
    recorded: Rows,
    /// The entity each found row of a minted collection is adopted as.
    found_as: BTreeMap<(String, String), Gid>,
    /// Cells the application changed, with the value it wrote, not yet seen in what was sent.
    owed: BTreeMap<(String, String, String), Value>,
    /// For a reference among them, the entity the row it names stood for when it was written.
    named: BTreeMap<(String, String, String), Gid>,
    /// Cells owed of rows an install collected and removed, merging the entity it minted for
    /// each into another's row, before anything here saw that entity. Their keys may be given to
    /// other rows before the next send, so they are looked for by value, see [`Writes::unwritten`].
    merged: BTreeMap<(String, String, String), Value>,
    /// Every write the application made, as the change that carried it: each must be in the
    /// merged state once everything is exchanged.
    pub expected: Vec<Change>,
}

async fn now(conn: &mut SqliteConnection, replica: &Replica) -> Rows {
    let mut out = Rows::new();
    for c in replica.registry().collections() {
        let keys: Vec<String> = key_columns(c).iter().map(|k| quoted(k)).collect();
        let mut columns = vec![format!("json_array({})", keys.join(", "))];
        columns.extend(c.fields.iter().map(|f| quoted(&f.name)));
        let sql = format!("SELECT {} FROM {}", columns.join(", "), quoted(&c.table));
        for row in rows(conn, &sql, &[]).await {
            out.insert(
                (c.name.clone(), text(&row[0])),
                c.fields
                    .iter()
                    .map(|f| f.name.clone())
                    .zip(row[1..].iter().cloned())
                    .collect(),
            );
        }
    }
    out
}

/// Whether a member is gone for good as far as `merge` knows: tombstoned, or not live while
/// every entity it names is known. One whose tombstone has not arrived, or that waits for an
/// owner yet to arrive, may yet show.
fn gone_for_good(merge: &model::Merge, m: &model::Entity) -> bool {
    let known = m.registers.values().all(|(v, _)| match v {
        Value::Ref(o) => merge.entities.contains_key(o),
        _ => true,
    });
    match m.registers.get(DELETED) {
        Some((Value::Int(1), _)) => true,
        Some((Value::Int(0), _)) => !m.live && known,
        _ => true,
    }
}

/// Triggers that log each row made at a key and each field set, whoever does it: a field set
/// back to the value it had, such as a reference to a row at a reused local id, leaves no trace
/// in the table.
async fn witness(conn: &mut SqliteConnection, replica: &Replica) {
    for sql in [
        "CREATE TABLE IF NOT EXISTS witness_made (collection TEXT, key TEXT)",
        "CREATE TABLE IF NOT EXISTS witness_set (collection TEXT, key TEXT, field TEXT)",
    ] {
        sqlx::query(sql).execute(&mut *conn).await.unwrap();
    }
    for c in replica.registry().collections() {
        let key = |row: &str| {
            let parts: Vec<String> = key_columns(c)
                .iter()
                .map(|k| format!("{row}.{}", quoted(k)))
                .collect();
            format!("json_array({})", parts.join(", "))
        };
        let (new, old) = (key("NEW"), key("OLD"));
        let made = format!("INSERT INTO witness_made VALUES ('{}', {new});", c.name);
        let set: String = c
            .fields
            .iter()
            .map(|f| {
                let column = quoted(&f.name);
                format!(
                    "INSERT INTO witness_set SELECT '{}', {new}, '{}'
                      WHERE OLD.{column} IS NOT NEW.{column};",
                    c.name, f.name
                )
            })
            .collect();
        let set = if set.is_empty() {
            "SELECT 1;".to_string()
        } else {
            set
        };
        for sql in [
            format!(
                "CREATE TRIGGER IF NOT EXISTS witness_{}_as AFTER UPDATE ON {} BEGIN {set} END",
                c.name,
                quoted(&c.table)
            ),
            format!(
                "CREATE TRIGGER IF NOT EXISTS witness_{}_ai AFTER INSERT ON {} BEGIN {made} END",
                c.name,
                quoted(&c.table)
            ),
            format!(
                "CREATE TRIGGER IF NOT EXISTS witness_{}_au AFTER UPDATE ON {} WHEN {old} <> {new}
                 BEGIN {made} END",
                c.name,
                quoted(&c.table)
            ),
        ] {
            sqlx::query(&sql).execute(&mut *conn).await.unwrap();
        }
    }
}

impl Writes {
    /// Before a kernel step: what changed since the last is the application's.
    pub async fn before(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        taken: &[Change],
    ) {
        witness(conn, replica).await;
        let current = now(conn, replica).await;
        self.found = current.clone();
        let registry = replica.registry();
        let remade: BTreeSet<(String, String)> =
            rows(conn, "SELECT collection, key FROM witness_made", &[])
                .await
                .iter()
                .map(|row| (text(&row[0]), text(&row[1])))
                .collect();
        let mut set: BTreeMap<(String, String), BTreeSet<String>> = BTreeMap::new();
        for row in rows(conn, "SELECT collection, key, field FROM witness_set", &[]).await {
            set.entry((text(&row[0]), text(&row[1])))
                .or_default()
                .insert(text(&row[2]));
        }
        // Rows made since the last step: the kernel has not yet given them an entity, so what
        // it maps their keys to is not what a reference to them names.
        let fresh: BTreeSet<&(String, String)> = current
            .keys()
            .filter(|at| !self.after.contains_key(*at) || remade.contains(*at))
            .collect();
        self.earlier = fresh
            .iter()
            .map(|at| {
                (
                    (*at).clone(),
                    self.gids.get(*at).cloned().unwrap_or_default(),
                )
            })
            .collect();
        for (at, values) in &current {
            let Some(c) = registry.collections().iter().find(|c| c.name == at.0) else {
                continue;
            };
            let old = self.after.get(at);
            let new_row = old.is_none() || remade.contains(at);
            if new_row {
                self.made.insert(at.clone());
            }
            for f in &c.fields {
                let value = values.get(&f.name).cloned().unwrap_or(Value::Null);
                let held = |row: &BTreeMap<String, Value>| {
                    row.get(&f.name).cloned().unwrap_or(Value::Null)
                };
                let changed = match old {
                    Some(old) if !new_row => held(old) != value,
                    _ => value != Value::Null,
                };
                if changed {
                    let cell = (at.0.clone(), at.1.clone(), f.name.clone());
                    if let (Kind::Ref { collection: t, .. }, Value::Int(local)) = (&f.kind, &value)
                        && !fresh.contains(&(t.clone(), format!("[{local}]")))
                        && let Ok(Some(g)) = replica.gid_of(conn, t, *local).await
                    {
                        self.named.insert(cell.clone(), g);
                    } else {
                        self.named.remove(&cell);
                    }
                    self.owed.insert(cell, value);
                }
            }
            let mut touched = BTreeSet::new();
            for f in &c.fields {
                if old.is_none_or(|old| {
                    old.get(&f.name).unwrap_or(&Value::Null)
                        != values.get(&f.name).unwrap_or(&Value::Null)
                }) {
                    touched.insert(f.name.clone());
                }
            }
            if remade.contains(at) {
                touched.extend(c.fields.iter().map(|f| f.name.clone()));
            }
            if let Some(fields) = set.get(at) {
                touched.extend(fields.iter().cloned());
            }
            if touched.is_empty() {
                continue;
            }
            for set in &c.together {
                if set.iter().any(|f| touched.contains(f)) {
                    touched.extend(set.iter().cloned());
                }
            }
            if let Key::Natural { fields } = &c.key {
                touched.extend(fields.iter().cloned());
            }
            touched.insert(DELETED.to_string());
            for f in touched {
                self.written.insert((at.0.clone(), at.1.clone(), f));
            }
        }
        for (collection, key) in current.keys() {
            self.remember(conn, replica, collection, key).await;
        }
        let gone: BTreeSet<&(String, String)> = self
            .after
            .keys()
            .filter(|at| !current.contains_key(*at))
            .collect();
        let mut deleted = Vec::new();
        for at in &gone {
            self.written
                .insert((at.0.clone(), at.1.clone(), DELETED.to_string()));
            // A row that went with its owner is judged by its owner: its entity may live on
            // in another member of its group, under another owner.
            let Some(c) = registry.collections().iter().find(|c| c.name == at.0) else {
                continue;
            };
            let with_owner = c.fields.iter().any(|f| {
                let owning = matches!(f.kind, Kind::Ref { owned: true, .. })
                    || matches!(&c.key, Key::Natural { fields } if fields.contains(&f.name));
                match (&f.kind, self.after[*at].get(&f.name)) {
                    (Kind::Ref { collection, .. }, Some(Value::Int(local))) if owning => {
                        gone.contains(&(collection.clone(), format!("[{local}]")))
                    }
                    _ => false,
                }
            });
            if !with_owner {
                deleted.push((*at).clone());
            }
        }
        if deleted.is_empty() {
            return;
        }
        // The group as the replica knew it when the application deleted the row: the model's
        // group of what it had taken in, as far as the replica had joined it, since the model
        // may join through a gid known only from aliases before the replica can.
        let seen = super::seen(conn, replica, taken).await;
        let merge = model::merge(&registry.model_schema(), &seen);
        let canon: BTreeMap<(String, Gid), Gid> = match replica.inspect(conn).await {
            Ok(inspection) => inspection
                .aliases
                .into_iter()
                .map(|(c, g, k)| ((c, g), k))
                .collect(),
            Err(_) => BTreeMap::new(),
        };
        let canonical = |c: &str, g: Gid| *canon.get(&(c.to_string(), g)).unwrap_or(&g);
        for at in deleted {
            let mut members: BTreeSet<Gid> = self
                .found_as
                .get(&at)
                .filter(|_| self.recorded.contains_key(&at))
                .copied()
                .into_iter()
                .collect();
            if let Some(gid) = self.latest.get(&at)
                && let Some(e) = merge.entities.get(gid)
            {
                let here = canonical(&at.0, *gid);
                for (g, m) in &merge.entities {
                    if m.collection == e.collection
                        && m.canonical == e.canonical
                        && canonical(&m.collection, *g) == here
                        && !gone_for_good(&merge, m)
                    {
                        members.insert(*g);
                    }
                }
            }
            self.removed.entry(at).or_default().extend(members);
        }
    }

    /// After a kernel step: the rows as it left them.
    pub async fn after(&mut self, conn: &mut SqliteConnection, replica: &Replica) {
        witness(conn, replica).await;
        for sql in ["DELETE FROM witness_made", "DELETE FROM witness_set"] {
            sqlx::query(sql).execute(&mut *conn).await.unwrap();
        }
        self.after = now(conn, replica).await;
        self.recorded.clear();
        let keys: Vec<(String, String)> = self.after.keys().cloned().collect();
        for (collection, key) in keys {
            self.remember(conn, replica, &collection, &key).await;
        }
    }

    /// After a bounded step of collecting that left rows to collect: a row still to collect
    /// that the step did not write keeps what the steps before it left, so what the application
    /// wrote to it since is still its write when a later step collects it.
    pub async fn after_step(&mut self, conn: &mut SqliteConnection, replica: &Replica) {
        witness(conn, replica).await;
        let current = now(conn, replica).await;
        let marked: BTreeSet<(String, String)> = replica
            .inspect(conn)
            .await
            .unwrap()
            .uncollected
            .into_iter()
            .collect();
        let mut keys: BTreeSet<(String, String)> = current.keys().cloned().collect();
        keys.extend(self.after.keys().cloned());
        keys.extend(self.found.keys().cloned());
        let mut next = Rows::new();
        let mut kept = BTreeSet::new();
        for key in keys {
            if marked.contains(&key) && self.found.get(&key) == current.get(&key) {
                if let Some(row) = self.after.get(&key).or(self.recorded.get(&key)) {
                    next.insert(key.clone(), row.clone());
                }
                kept.insert(key);
            } else if let Some(row) = current.get(&key) {
                next.insert(key, row.clone());
            }
        }
        for table in ["witness_made", "witness_set"] {
            for row in rows(
                conn,
                &format!("SELECT rowid, collection, key FROM {table}"),
                &[],
            )
            .await
            {
                if !kept.contains(&(text(&row[1]), text(&row[2]))) {
                    sqlx::query(&format!("DELETE FROM {table} WHERE rowid = ?1"))
                        .bind(match row[0] {
                            Value::Int(id) => id,
                            _ => unreachable!(),
                        })
                        .execute(&mut *conn)
                        .await
                        .unwrap();
                }
            }
        }
        // What is owed of a row still to collect is its change since it was last collected,
        // which the next kernel step's look at the tables works out again; of a found row not
        // yet adopted, the values it was found with, which its adoption sends.
        let of_kept = |c: &str, k: &str| kept.contains(&(c.to_string(), k.to_string()));
        self.owed.retain(|(c, k, _), _| !of_kept(c, k));
        self.named.retain(|(c, k, _), _| !of_kept(c, k));
        self.recorded.retain(|at, _| kept.contains(at));
        for ((c, k), row) in &self.recorded {
            for (field, value) in row.iter().filter(|(_, v)| **v != Value::Null) {
                self.owed
                    .insert((c.clone(), k.clone(), field.clone()), value.clone());
            }
        }
        self.after = next;
        for (collection, key) in current.keys() {
            self.remember(conn, replica, collection, key).await;
        }
    }

    /// After an apply, which reports the rows it wrote and removed with their entities, among
    /// them rows the application made that the apply collected and then removed.
    pub fn touched(&mut self, touched: &[fongo_sync_sqlite::Touched]) {
        for t in touched.iter().filter(|t| !t.deleted) {
            self.latest
                .insert((t.collection.clone(), t.key.clone()), t.gid);
        }
        for t in touched {
            self.gids
                .entry((t.collection.clone(), t.key.clone()))
                .or_default()
                .insert(t.gid);
        }
    }

    async fn remember(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        collection: &str,
        key: &str,
    ) {
        let at = (collection.to_string(), key.to_string());
        // Before the first install there is nothing to look up.
        if let Ok(Some(gid)) = replica.gid_for_key(conn, collection, key).await {
            self.latest.insert(at.clone(), gid);
            self.gids.entry(at.clone()).or_default().insert(gid);
        }
        // A natural row's writes go to the gid its key derives, with references read as the
        // rows they name here, which may be another member of the entity's group.
        let Some(c) = replica
            .registry()
            .collections()
            .iter()
            .find(|c| c.name == collection)
        else {
            return;
        };
        let Key::Natural { fields } = &c.key else {
            return;
        };
        let mut values = Vec::new();
        for (i, name) in fields.iter().enumerate() {
            let part = rows(
                conn,
                &format!("SELECT json_extract(?1, '$[{i}]')"),
                &[Value::Text(key.to_string())],
            )
            .await
            .remove(0)
            .remove(0);
            let kind = c.fields.iter().find(|f| f.name == *name).map(|f| &f.kind);
            let value = match (kind, part) {
                (Some(Kind::Ref { collection, .. }), Value::Int(local)) => {
                    match replica.gid_of(conn, collection, local).await {
                        Ok(Some(gid)) => Value::Ref(gid),
                        _ => return,
                    }
                }
                (_, part) => part,
            };
            values.push(value);
        }
        self.gids
            .entry(at)
            .or_default()
            .insert(Gid::derive(collection, &values));
    }

    /// Before an install, which may record what rows hold for fields it adopts.
    pub async fn installing(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        taken: &[Change],
    ) -> usize {
        self.before(conn, replica, taken).await;
        match replica.outbox(conn, u32::MAX).await {
            Ok(outbox) => outbox.changes.len(),
            // Before the first install there is no outbox, and what the tables hold was found, not written.
            Err(_) => {
                self.written.clear();
                self.made.clear();
                self.recorded = self.found.clone();
                0
            }
        }
    }

    /// The entity each found row of a minted collection is adopted as, as (collection, key,
    /// gid), read from a copy that adopted them whole: every step adopts a row as that.
    pub fn found_as(&mut self, ids: Vec<(String, String, Gid)>) {
        for (c, key, gid) in ids {
            let at = (c, key);
            if self.recorded.contains_key(&at) {
                self.gids.entry(at.clone()).or_default().insert(gid);
                self.latest.entry(at.clone()).or_insert(gid);
                self.found_as.insert(at, gid);
            }
        }
    }

    /// After an install: what it added to the outbox past `waiting` is its own.
    pub async fn installed(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        waiting: usize,
    ) {
        let outbox = replica.outbox(conn, u32::MAX).await.unwrap();
        self.adopted
            .extend(outbox.changes.into_iter().skip(waiting));
        self.after(conn, replica).await;
        self.gone_with_install();
    }

    /// What was owed of a row the install collected and then removed, merging its entity into
    /// another's row, before anything here saw the entity: owed still, but by value, since the
    /// row's key could be given to another entity before the next send. A row made since the
    /// last step may sit at a key earlier rows held: their entities are not its own.
    fn gone_with_install(&mut self) {
        let gone = |at: &(String, String)| {
            !self.after.contains_key(at)
                && self.gids.get(at).is_none_or(|held| {
                    held.iter()
                        .all(|g| self.earlier.get(at).is_some_and(|e| e.contains(g)))
                })
        };
        let cells: Vec<(String, String, String)> = self
            .owed
            .keys()
            .filter(|(c, k, _)| gone(&(c.clone(), k.clone())))
            .cloned()
            .collect();
        for cell in cells {
            if let Some(value) = self.owed.remove(&cell) {
                self.merged.insert(cell.clone(), value);
            }
            self.named.remove(&cell);
        }
    }

    /// [`Writes::installed`] after an install that collected in a bounded step and left rows to
    /// collect, as [`Writes::after_step`] sees them.
    pub async fn installed_step(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        waiting: usize,
    ) {
        let outbox = replica.outbox(conn, u32::MAX).await.unwrap();
        self.adopted
            .extend(outbox.changes.into_iter().skip(waiting));
        self.after_step(conn, replica).await;
        self.gone_with_install();
    }

    /// The changes about to be sent that the application did not write, and rows it removed
    /// whose entities the changes `seen` with `sent` still show. Forgets what it wrote.
    pub async fn unwritten(
        &mut self,
        conn: &mut SqliteConnection,
        replica: &Replica,
        seen: &[Change],
        sent: &[Change],
    ) -> Vec<String> {
        let mut wrong = Vec::new();
        let inspection = replica.inspect(conn).await.unwrap();
        let canon: BTreeMap<(String, Gid), Gid> = inspection
            .aliases
            .into_iter()
            .map(|(c, g, k)| ((c, g), k))
            .collect();
        let mut canon_in: BTreeMap<&str, BTreeMap<Gid, Gid>> = BTreeMap::new();
        for ((c, g), k) in &canon {
            canon_in.entry(c.as_str()).or_default().insert(*g, *k);
        }
        let canonical = |c: &str, g: Gid| {
            canon_in
                .get(c)
                .and_then(|m| m.get(&g))
                .copied()
                .unwrap_or(g)
        };
        // Indexed once rather than searched for each change sent, since both grow with the
        // library: the rows that have held each entity, by collection and canonical gid.
        let mut holders: BTreeMap<(&str, Gid), BTreeSet<&str>> = BTreeMap::new();
        for ((c, k), held) in &self.gids {
            for g in held {
                holders
                    .entry((c.as_str(), canonical(c, *g)))
                    .or_default()
                    .insert(k.as_str());
            }
        }
        // Every entity some row here has held, and the entities this send makes that none has.
        let seen_gids: BTreeSet<Gid> = self.gids.values().flatten().copied().collect();
        let made_unseen: BTreeSet<(&str, Gid)> = sent
            .iter()
            .filter(|ch| ch.field == DELETED && ch.value == Value::Int(0))
            .filter(|ch| !seen_gids.contains(&ch.gid))
            .map(|ch| (ch.collection.as_str(), ch.gid))
            .collect();
        // Whether `ch` carries `value`; a local id stands for whichever entity the row named.
        let carries = |ch: &Change, value: &Value| match (&ch.value, value) {
            (Value::Ref(_), Value::Int(_)) => true,
            (a, b) => a.total_cmp(b) == std::cmp::Ordering::Equal,
        };
        // A row the kernel removed, merging it into another's, before anything here saw the
        // entity it minted for the row: that entity, made by this send, carries every field the
        // application wrote to the row.
        let mut merged_rows: BTreeMap<(String, String), Vec<(String, Value)>> = BTreeMap::new();
        for ((c, key, field), value) in std::mem::take(&mut self.merged) {
            merged_rows
                .entry((c, key))
                .or_default()
                .push((field, value));
        }
        // The entity that carried each row merged away, which references to the row name.
        let mut carriers: BTreeMap<(String, String), Gid> = BTreeMap::new();
        for ((c, key), cells) in merged_rows {
            let carrier = made_unseen
                .iter()
                .filter(|(mc, _)| *mc == c)
                .find(|(_, g)| {
                    cells.iter().all(|(field, value)| {
                        sent.iter().any(|ch| {
                            ch.collection == c
                                && ch.gid == *g
                                && ch.field == *field
                                && carries(ch, value)
                        })
                    })
                });
            match carrier {
                Some((_, g)) => {
                    self.expected.extend(
                        sent.iter()
                            .filter(|ch| {
                                ch.collection == c
                                    && ch.gid == *g
                                    && cells.iter().any(|(field, _)| *field == ch.field)
                            })
                            .cloned(),
                    );
                    carriers.insert((c.clone(), key.clone()), *g);
                }
                None => wrong.push(format!(
                    "the application wrote {c} row {key} as {cells:?}, the install merged the row away, and no entity it made carries it"
                )),
            }
        }
        // The fields written to rows that are gone and were made by the application or never
        // held an entity, by collection and field.
        let mut unheld: BTreeMap<(&str, &str), Vec<&str>> = BTreeMap::new();
        for (c, k, f) in &self.written {
            let at = (c.clone(), k.clone());
            if (!self.gids.contains_key(&at) || self.made.contains(&at))
                && !self.after.contains_key(&at)
            {
                unheld
                    .entry((c.as_str(), f.as_str()))
                    .or_default()
                    .push(k.as_str());
            }
        }
        for change in sent {
            // An adoption, by an install or by a later step adopting what one recorded, takes the
            // adoption clock, which no write of the application's does.
            if change.field == ALIAS || self.adopted.contains(change) || change.clock.wall_ms == 0 {
                continue;
            }
            let target = canonical(&change.collection, change.gid);
            let mut keys: Vec<String> = holders
                .get(&(change.collection.as_str(), target))
                .into_iter()
                .flatten()
                .map(|k| k.to_string())
                .collect();
            if let Some(key) = replica
                .key_for_gid(conn, &change.collection, change.gid)
                .await
                .unwrap()
            {
                keys.push(key);
            }
            // A row the application made and a kernel step removed before anything named its
            // entity can only be matched by collection and field: an apply removed it, or the
            // collect that minted its entity merged that entity into another's row, which `keys`
            // then names. The row may sit at a key an earlier row held, so `gids` names only that
            // row's entities.
            let unnamed = unheld
                .get(&(change.collection.as_str(), change.field.as_str()))
                .into_iter()
                .flatten()
                .any(|k| {
                    let cell = (
                        change.collection.clone(),
                        k.to_string(),
                        change.field.clone(),
                    );
                    // The removed row held the value sent: for a reference, the entity it named
                    // when written, or any if that row was new too.
                    change.field == DELETED
                        || match (self.owed.get(&cell), &change.value) {
                            (Some(Value::Int(_)), Value::Ref(h)) => {
                                self.named.get(&cell).is_none_or(|g| {
                                    g == h || canon.iter().any(|((_, x), k)| x == g && k == h)
                                })
                            }
                            (Some(v), sent) => v == sent,
                            (None, _) => false,
                        }
                });
            let wrote = unnamed
                || keys.iter().any(|k| {
                    self.written.contains(&(
                        change.collection.clone(),
                        k.clone(),
                        change.field.clone(),
                    ))
                });
            if !wrote {
                wrong.push(format!(
                    "sent a write the application did not make, to rows {keys:?}: {change:?}"
                ));
            }
        }
        // A row the application deleted, itself and not with its owner, is deleted everywhere:
        // what this replica has seen and sent leaves no member of its group that was marked live
        // when it deleted the row still marked live, unless the group still has a row here or
        // the row was not an entity of its own. That the group is merely not live is not enough,
        // since an owner yet to arrive revives it.
        let leftover: BTreeSet<(String, String)> = inspection.leftover.into_iter().collect();
        let mut all = seen.to_vec();
        all.extend(sent.iter().cloned());
        let after = model::merge(&replica.registry().model_schema(), &all);
        for (at, members) in &self.removed {
            let Some(gid) = self.latest.get(at) else {
                continue;
            };
            let target = canonical(&at.0, *gid);
            let still = self.after.keys().any(|other| {
                other.0 == at.0
                    && self
                        .latest
                        .get(other)
                        .is_some_and(|g| canonical(&other.0, *g) == target)
            });
            let alive: Vec<&Gid> = members
                .iter()
                .filter(|g| {
                    after
                        .entities
                        .get(g)
                        .is_some_and(|m| !gone_for_good(&after, m))
                })
                .collect();
            if !still && !leftover.contains(at) && !alive.is_empty() {
                wrong.push(format!(
                    "the application deleted {} row {} and members {alive:?} of its entity live on",
                    at.0, at.1
                ));
            }
        }
        // Every value the application wrote is carried by a change sent to its entity.
        let uncollected: BTreeSet<(String, String)> =
            inspection.uncollected.iter().cloned().collect();
        let pinned: BTreeSet<(String, String, String)> =
            inspection.pinned.iter().cloned().collect();
        let registry = replica.registry();
        let named = std::mem::take(&mut self.named);
        for ((c, key, field), value) in std::mem::take(&mut self.owed) {
            let at = (c.clone(), key.clone());
            let cell = (c.clone(), key.clone(), field.clone());
            // A row written again and not yet collected may still hold a value an earlier collect
            // recorded: what this send carries of it is matched now, the rest when it is collected.
            let waits = uncollected.contains(&at);
            // A row gone after the step is owed nothing only when the application removed it, or
            // when no entity it stood for is live in the model.
            let gone = !self.after.contains_key(&at)
                && (self.removed.contains_key(&at)
                    || !self
                        .gids
                        .get(&at)
                        .into_iter()
                        .flatten()
                        .chain(self.latest.get(&at))
                        .any(|g| after.entities.get(g).is_some_and(|m| m.live)));
            if !waits && (pinned.contains(&cell) || gone) {
                continue;
            }
            let Some(collection) = registry.collections().iter().find(|x| x.name == c) else {
                if waits {
                    self.owed.insert(cell, value);
                }
                continue;
            };
            let kind = collection
                .fields
                .iter()
                .find(|f| f.name == field)
                .map(|f| &f.kind);
            // A reference names an entity the row at that local id has stood for: when written,
            // now, or meanwhile, since a row that waited may be collected after the id was given
            // to another. A row that has since gone with a merge, and was never seen with an
            // entity, is known by none, and then any entity carried will do.
            let mut names: BTreeSet<Gid> = BTreeSet::new();
            if let (Some(Kind::Ref { collection: t, .. }), Value::Int(local)) = (kind, &value) {
                names.extend(named.get(&cell).map(|g| canonical(t, *g)));
                let held = (t.clone(), format!("[{local}]"));
                names.extend(carriers.get(&held).map(|g| canonical(t, *g)));
                names.extend(
                    self.gids
                        .get(&held)
                        .into_iter()
                        .flatten()
                        .map(|g| canonical(t, *g)),
                );
                if let Some(g) = replica.gid_of(conn, t, *local).await.unwrap() {
                    names.insert(canonical(t, g));
                }
            }
            let mut group: BTreeSet<Gid> = self
                .gids
                .get(&at)
                .into_iter()
                .flatten()
                .map(|g| canonical(&c, *g))
                .collect();
            group.extend(self.latest.get(&at).map(|g| canonical(&c, *g)));
            // A row the application made that the step removed, merging the entity it minted
            // into another's row, was never seen with that entity; `gids` names only the
            // entities of earlier rows at its key. Its write is carried by that entity.
            let unseen = self.made.contains(&at) && !self.after.contains_key(&at);
            let carried = sent.iter().find(|ch| {
                ch.collection == c
                    && ch.field == field
                    && (group.contains(&canonical(&c, ch.gid))
                        || (unseen && made_unseen.contains(&(c.as_str(), ch.gid))))
                    && match (&ch.value, &value, kind) {
                        (Value::Ref(a), Value::Int(_), Some(Kind::Ref { collection: t, .. })) => {
                            names.is_empty() || names.contains(&canonical(t, *a))
                        }
                        (a, b, _) => a.total_cmp(b) == std::cmp::Ordering::Equal,
                    }
            });
            match carried {
                Some(change) => self.expected.push(change.clone()),
                None if waits => {
                    if let Some(g) = named.get(&cell) {
                        self.named.insert(cell.clone(), *g);
                    }
                    self.owed.insert(cell, value);
                }
                None => wrong.push(format!(
                    "the application wrote {c} row {key}.{field} = {value:?} and nothing sent it"
                )),
            }
        }
        // What waits to be collected, such as a row whose owner is outside sync, is still owed.
        self.written
            .retain(|(c, k, _)| uncollected.contains(&(c.clone(), k.clone())));
        self.removed.retain(|at, _| uncollected.contains(at));
        self.made.retain(|at| uncollected.contains(at));
        self.adopted.clear();
        wrong
    }
}

impl Writes {
    /// Whether every write the application made is in the merged state of `everything`: some
    /// member of its entity's group holds the field at the clock it was sent with, or a later
    /// one, and at that clock a value at least as great; a `Max` field holds a value at least as
    /// great, and a natural key field that value.
    pub fn kept(
        &self,
        registry: &fongo_sync_sqlite::Registry,
        everything: &[Change],
    ) -> Result<(), String> {
        let schema = registry.model_schema();
        let merged = model::merge(&schema, everything);
        let mut groups: BTreeMap<Gid, Vec<Gid>> = BTreeMap::new();
        for (g, e) in &merged.entities {
            groups.entry(e.canonical).or_default().push(*g);
        }
        for w in &self.expected {
            let Some(entity) = merged.entities.get(&w.gid) else {
                return Err(format!(
                    "the application wrote {w:?}; the entity never arrived"
                ));
            };
            let c = schema
                .collection(&w.collection)
                .expect("a registered collection");
            let strategy = c
                .fields
                .iter()
                .find(|f| f.name == w.field)
                .map_or(fongo_sync_sqlite::core::Strategy::Lww, |f| f.strategy);
            let key = matches!(&c.identity, model::Identity::Natural(k) if k.contains(&w.field));
            let same = |v: &Value| v.total_cmp(&w.value) == std::cmp::Ordering::Equal;
            let kept = groups[&entity.canonical].iter().any(|m| {
                merged.entities[m]
                    .registers
                    .get(&w.field)
                    .is_some_and(|(v, at)| {
                        if key {
                            same(v)
                        } else if strategy == fongo_sync_sqlite::core::Strategy::Max {
                            v.total_cmp(&w.value) != std::cmp::Ordering::Less
                        } else {
                            at.cmp(&w.clock).then(v.total_cmp(&w.value)) != std::cmp::Ordering::Less
                        }
                    })
            });
            if !kept {
                return Err(format!(
                    "the application wrote {w:?}; the merged state lost it"
                ));
            }
        }
        Ok(())
    }
}

//! What the application wrote, told apart from what the kernel wrote, by comparing its tables
//! before each kernel step with how the previous step left them, and by triggers of the test's
//! own that see a row deleted and made again at the same key. A replica may send a field of an
//! entity only when the application wrote that field of its row, or wrote any part of a set the
//! field changes with, or made or removed the row; besides that it sends aliases, and what an
//! install adopts. Anything else it sends is a value the application did not write, such as a
//! field the kernel held back in a row and later took for an edit.

use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_sqlite::core::model;
use fongo_sync_sqlite::core::{ALIAS, Change, DELETED, Gid, Value};
use fongo_sync_sqlite::{Key, Kind, Replica};
use sqlx::SqliteConnection;

use super::{key_columns, quoted, rows, text};

type Rows = BTreeMap<(String, String), Vec<Value>>;

#[derive(Default)]
pub struct Writes {
    /// The rows as the last kernel step left them.
    after: Rows,
    /// Every entity each row has held, by collection and key.
    gids: BTreeMap<(String, String), BTreeSet<Gid>>,
    /// Fields the application wrote since the last send, by collection, key and field;
    /// `$deleted` for a row made or removed.
    written: BTreeSet<(String, String, String)>,
    /// Rows the application removed since the last send, by collection and key.
    removed: BTreeSet<(String, String)>,
    /// Changes an install produced, which are not checked.
    adopted: Vec<Change>,
}

async fn now(conn: &mut SqliteConnection, replica: &Replica) -> Rows {
    let mut out = Rows::new();
    for c in replica.registry().collections() {
        let keys: Vec<String> = key_columns(c).iter().map(|k| quoted(k)).collect();
        let mut columns = vec![format!("json_array({})", keys.join(", "))];
        columns.extend(c.fields.iter().map(|f| quoted(&f.name)));
        let sql = format!("SELECT {} FROM {}", columns.join(", "), quoted(&c.table));
        for row in rows(conn, &sql, &[]).await {
            out.insert((c.name.clone(), text(&row[0])), row[1..].to_vec());
        }
    }
    out
}

/// Triggers that log each row made at a key, whoever makes it.
async fn witness(conn: &mut SqliteConnection, replica: &Replica) {
    sqlx::query("CREATE TABLE IF NOT EXISTS witness_made (collection TEXT, key TEXT)")
        .execute(&mut *conn)
        .await
        .unwrap();
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
        for sql in [
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
    pub async fn before(&mut self, conn: &mut SqliteConnection, replica: &Replica) {
        witness(conn, replica).await;
        let current = now(conn, replica).await;
        let registry = replica.registry();
        let remade: BTreeSet<(String, String)> =
            rows(conn, "SELECT collection, key FROM witness_made", &[])
                .await
                .iter()
                .map(|row| (text(&row[0]), text(&row[1])))
                .collect();
        for (at, values) in &current {
            let Some(c) = registry.collections().iter().find(|c| c.name == at.0) else {
                continue;
            };
            let old = self.after.get(at);
            let mut touched = BTreeSet::new();
            for (i, f) in c.fields.iter().enumerate() {
                if old.is_none_or(|old| old.get(i) != values.get(i)) {
                    touched.insert(f.name.clone());
                }
            }
            if remade.contains(at) {
                touched.extend(c.fields.iter().map(|f| f.name.clone()));
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
        for at in &gone {
            self.written
                .insert((at.0.clone(), at.1.clone(), DELETED.to_string()));
            // A row that went with its owner is judged by its owner: its entity may live on
            // in another member of its group, under another owner.
            let Some(c) = registry.collections().iter().find(|c| c.name == at.0) else {
                continue;
            };
            let with_owner = c.fields.iter().enumerate().any(|(i, f)| {
                let owning = matches!(f.kind, Kind::Ref { owned: true, .. })
                    || matches!(&c.key, Key::Natural { fields } if fields.contains(&f.name));
                match (&f.kind, self.after[*at].get(i)) {
                    (Kind::Ref { collection, .. }, Some(Value::Int(local))) if owning => {
                        gone.contains(&(collection.clone(), format!("[{local}]")))
                    }
                    _ => false,
                }
            });
            if !with_owner {
                self.removed.insert((*at).clone());
            }
        }
    }

    /// After a kernel step: the rows as it left them.
    pub async fn after(&mut self, conn: &mut SqliteConnection, replica: &Replica) {
        witness(conn, replica).await;
        sqlx::query("DELETE FROM witness_made")
            .execute(&mut *conn)
            .await
            .unwrap();
        self.after = now(conn, replica).await;
        let keys: Vec<(String, String)> = self.after.keys().cloned().collect();
        for (collection, key) in keys {
            self.remember(conn, replica, &collection, &key).await;
        }
    }

    /// After an apply, which reports the rows it wrote and removed with their entities, among
    /// them rows the application made that the apply collected and then removed.
    pub fn touched(&mut self, touched: &[fongo_sync_sqlite::Touched]) {
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
        // Before the first install there is nothing to look up.
        if let Ok(Some(gid)) = replica.gid_for_key(conn, collection, key).await {
            self.gids
                .entry((collection.to_string(), key.to_string()))
                .or_default()
                .insert(gid);
        }
    }

    /// Before an install, which may record what rows hold for fields it adopts.
    pub async fn installing(&mut self, conn: &mut SqliteConnection, replica: &Replica) -> usize {
        self.before(conn, replica).await;
        // Before the first install there is no outbox.
        replica
            .outbox(conn, u32::MAX)
            .await
            .map_or(0, |outbox| outbox.changes.len())
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
        let canonical = |c: &str, g: Gid| *canon.get(&(c.to_string(), g)).unwrap_or(&g);
        for change in sent {
            if change.field == ALIAS || self.adopted.contains(change) {
                continue;
            }
            let target = canonical(&change.collection, change.gid);
            let mut keys: Vec<String> = self
                .gids
                .iter()
                .filter(|((c, _), held)| {
                    *c == change.collection && held.iter().any(|g| canonical(c, *g) == target)
                })
                .map(|((_, k), _)| k.clone())
                .collect();
            if let Some(key) = replica
                .key_for_gid(conn, &change.collection, change.gid)
                .await
                .unwrap()
            {
                keys.push(key);
            }
            let wrote = keys.iter().any(|k| {
                self.written
                    .contains(&(change.collection.clone(), k.clone(), change.field.clone()))
            });
            if !wrote {
                wrong.push(format!(
                    "sent a write the application did not make, to rows {keys:?}: {change:?}"
                ));
            }
        }
        // A row the application deleted, itself and not with its owner, is deleted everywhere:
        // what this replica has seen and sent leaves every member of its group with a tombstone,
        // unless the group still has a row here or the row was not an entity of its own. That
        // the group is merely not live is not enough, since an owner yet to arrive revives it.
        let leftover: BTreeSet<(String, String)> = inspection.leftover.into_iter().collect();
        let mut all = seen.to_vec();
        all.extend(sent.iter().cloned());
        let merge = model::merge(&replica.registry().model_schema(), &all);
        let tombstoned = |gid: &Gid| {
            let Some(e) = merge.entities.get(gid) else {
                return true;
            };
            merge
                .entities
                .values()
                .filter(|m| m.collection == e.collection && m.canonical == e.canonical)
                .all(|m| matches!(m.registers.get(DELETED), Some((Value::Int(1), _))))
        };
        for at in &self.removed {
            let Some(held) = self.gids.get(at) else {
                continue;
            };
            let targets: BTreeSet<Gid> = held.iter().map(|g| canonical(&at.0, *g)).collect();
            let still = self.after.keys().any(|other| {
                other.0 == at.0
                    && self.gids.get(other).is_some_and(|gs| {
                        gs.iter()
                            .any(|g| targets.contains(&canonical(&other.0, *g)))
                    })
            });
            if !still && !leftover.contains(at) && !held.iter().all(tombstoned) {
                wrong.push(format!(
                    "the application deleted {} row {} and its entity was not deleted",
                    at.0, at.1
                ));
            }
        }
        // What waits to be collected, such as a row whose owner is outside sync, is still owed.
        let uncollected: BTreeSet<(String, String)> = inspection.uncollected.into_iter().collect();
        self.written
            .retain(|(c, k, _)| uncollected.contains(&(c.clone(), k.clone())));
        self.removed.retain(|at| uncollected.contains(at));
        self.adopted.clear();
        wrong
    }
}

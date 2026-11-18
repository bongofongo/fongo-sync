//! What the application wrote, told apart from what the kernel wrote, by comparing its tables
//! before each kernel step with how the previous step left them. A replica may send a field of an
//! entity only when the application wrote that field of its row, or wrote any part of a set the
//! field changes with, or made or removed the row; besides that it sends aliases, and what an
//! install adopts. Anything else it sends is a value the application did not write, such as a
//! field the kernel held back in a row and later took for an edit.

use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_sqlite::core::model;
use fongo_sync_sqlite::core::{ALIAS, Change, DELETED, Gid, Value};
use fongo_sync_sqlite::{Key, Replica};
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

impl Writes {
    /// Before a kernel step: what changed since the last is the application's.
    pub async fn before(&mut self, conn: &mut SqliteConnection, replica: &Replica) {
        let current = now(conn, replica).await;
        let registry = replica.registry();
        // A row deleted and made again with the same values differs in nothing but the change
        // capture's record of it, which is the only witness here.
        let remade: BTreeSet<(String, String)> = match replica.inspect(conn).await {
            Ok(inspection) => inspection.uncollected.into_iter().collect(),
            Err(_) => BTreeSet::new(),
        };
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
            if touched.is_empty() {
                if !remade.contains(at) {
                    continue;
                }
                touched.extend(c.fields.iter().map(|f| f.name.clone()));
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
        for at in self.after.keys() {
            if !current.contains_key(at) {
                self.written
                    .insert((at.0.clone(), at.1.clone(), DELETED.to_string()));
                self.removed.insert(at.clone());
            }
        }
    }

    /// After a kernel step: the rows as it left them.
    pub async fn after(&mut self, conn: &mut SqliteConnection, replica: &Replica) {
        self.after = now(conn, replica).await;
        for (collection, key) in self.after.keys() {
            if let Some(gid) = replica.gid_for_key(conn, collection, key).await.unwrap() {
                self.gids
                    .entry((collection.clone(), key.clone()))
                    .or_default()
                    .insert(gid);
            }
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
        // A row the application removed is gone everywhere, unless its entity still has a row
        // here or the row was not an entity of its own: what this replica sends leaves the
        // entity not live, by a tombstone or because an owner is gone.
        let leftover: BTreeSet<(String, String)> = inspection.leftover.into_iter().collect();
        let mut all = seen.to_vec();
        all.extend(sent.iter().cloned());
        let merge = model::merge(&replica.registry().model_schema(), &all);
        let live = |gid: &Gid| {
            merge.entities.get(gid).is_some_and(|e| {
                merge
                    .entities
                    .values()
                    .any(|m| m.live && m.collection == e.collection && m.canonical == e.canonical)
            })
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
            if !still && !leftover.contains(at) && held.iter().any(live) {
                wrong.push(format!(
                    "the application removed {} row {} and its entity lives on",
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

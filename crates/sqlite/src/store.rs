//! The facts `fongo_sync_core::source` describes, kept in the kernel's tables and read back
//! through the indexed questions of [`Source`].

use std::collections::BTreeSet;

use fongo_sync_core::model::Schema;
use fongo_sync_core::source::{Alias, Body, Claim, Entry, Fact, Slot, Source, keep};
use fongo_sync_core::{Gid, Hlc, Value};

use crate::db::{Db, as_gid, as_int, as_text, loaded, stored};
use crate::error::{Error, Result};

/// The kernel's own tables. Layout 4: facts as `source::admit` keeps them, a row image and the
/// entity each local row stands for, what waits, and the rows a first install found that it has
/// not adopted yet.
///
/// # Adopting found rows in steps
///
/// A collection's first install adopts the rows its table already holds, as the oldest writes
/// there are. Whole, it marks them and collects them at once. In steps (`install_step`), the
/// transaction that begins the install records each found row instead, in `fongo_found`: its
/// values as they stand then, the gid a whole install would mint for it, and the counter of the
/// adoption clock a whole install would give it, both in the order a whole install takes them.
/// Later steps adopt those records, never the table, so what the application writes after the
/// install began is an ordinary edit against the record, collected once every record is adopted,
/// as it would be after a whole install made when the stepped one began.
///
/// The records are adopted in batches. A batch holds every found row that collecting could find
/// to be one entity with another (rows with equal identities or natural keys, their references
/// read as the batches they name), and every row on a cycle of references among found rows, and
/// batches are taken in an order in which a row comes after the rows it names. So no step finds
/// a row to be one with a row a later step adopts, and every reference a step records names a row
/// already adopted or adopted with it. A step takes whole batches until it has about the rows
/// asked for.
///
/// A step that adopts reads no marked row and is checked against none: in the install it stands
/// for, every write made after the install began comes after every adoption. What it writes to
/// a row the application has written since, it writes only to fields the application has left
/// as the kernel last saw them; the image takes the rest, so the later collect finds the
/// application's values as its edits, as it would have after the whole install. Until every
/// record is adopted, a collect or an apply adopts the rest first, whole, and an install made
/// meanwhile goes on with them. A found row that cannot be adopted yet is marked as a whole
/// install would leave it. A collection that already holds entities from elsewhere, which its
/// found rows may be one with, is not recorded: what is found with it is marked and adopted
/// whole by the next step, so aliases take the clocks a whole install gives them. The registry
/// the rows were recorded under is kept with them; a
/// registry that differs, at an install, a resume or any call, first adopts every record under
/// it, whole, so a change of registry meets them as it meets rows a whole install adopted.
/// Suspending adopts every record first, whole, while the tables still match that registry, so
/// a schema changed while suspended never meets a record.
///
/// Layout 3 is upgraded in place by the next install: it gains the empty `fongo_found` table and
/// the index of marks in collecting order, and keeps everything else as it was. The move is one
/// way: a build of layout 3 refuses to install over layout 4, as over any layout it does not
/// know. Such a build calling `collect` or `apply` without installing first is not refused, and
/// would leave records waiting unadopted; an application installs at every open. This build
/// refuses every call on a later layout, and every call but an install, a resume or an uninstall
/// on layout 3.
pub(crate) const TABLES: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS fongo_meta (key TEXT PRIMARY KEY, value)",
    // The smallest clock among a gid's facts in each collection.
    "CREATE TABLE IF NOT EXISTS fongo_claims (
        gid BLOB NOT NULL,
        collection TEXT NOT NULL,
        first TEXT NOT NULL,
        PRIMARY KEY (gid, collection)) WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS fongo_claims_collection ON fongo_claims (collection, gid)",
    // Every kept entry of every field: one for most fields, a few for a natural key's.
    "CREATE TABLE IF NOT EXISTS fongo_shadow (
        collection TEXT NOT NULL,
        gid BLOB NOT NULL,
        field TEXT NOT NULL,
        value,
        is_ref INTEGER NOT NULL DEFAULT 0,
        clock TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS fongo_shadow_entity ON fongo_shadow (collection, gid, field)",
    "CREATE INDEX IF NOT EXISTS fongo_shadow_value ON fongo_shadow (collection, field, value, is_ref, gid)",
    "CREATE TABLE IF NOT EXISTS fongo_alias (
        collection TEXT NOT NULL,
        a BLOB NOT NULL,
        b BLOB NOT NULL,
        PRIMARY KEY (a, b, collection)) WITHOUT ROWID",
    "CREATE INDEX IF NOT EXISTS fongo_alias_b ON fongo_alias (b)",
    // Changes to collections this registry does not have, kept for a registry that does.
    "CREATE TABLE IF NOT EXISTS fongo_foreign (
        collection TEXT NOT NULL,
        gid BLOB NOT NULL,
        field TEXT NOT NULL,
        value,
        is_ref INTEGER NOT NULL DEFAULT 0,
        clock TEXT NOT NULL,
        PRIMARY KEY (collection, gid, field, clock))",
    // Each local row: the entity it stands for, and its image. `local` is a minted row's id.
    "CREATE TABLE IF NOT EXISTS fongo_ids (
        collection TEXT NOT NULL,
        key TEXT NOT NULL,
        gid BLOB NOT NULL,
        local INTEGER,
        image BLOB NOT NULL,
        doomed INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (collection, key))",
    "CREATE INDEX IF NOT EXISTS fongo_ids_gid ON fongo_ids (collection, gid)",
    "CREATE INDEX IF NOT EXISTS fongo_ids_local ON fongo_ids (collection, local)",
    "CREATE TABLE IF NOT EXISTS fongo_dirty (
        collection TEXT NOT NULL,
        key TEXT NOT NULL,
        at INTEGER NOT NULL,
        gone INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (collection, key))",
    // The order rows are collected in, so a bounded step reads only the rows it takes. A
    // database made without it gains it at its next install; one with it works as before.
    "CREATE INDEX IF NOT EXISTS fongo_dirty_order ON fongo_dirty (at, collection, key)",
    "CREATE TABLE IF NOT EXISTS fongo_outbox (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        collection TEXT NOT NULL,
        gid BLOB NOT NULL,
        field TEXT NOT NULL,
        value,
        is_ref INTEGER NOT NULL DEFAULT 0,
        clock TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS fongo_held (
        collection TEXT NOT NULL,
        gid BLOB NOT NULL,
        PRIMARY KEY (collection, gid)) WITHOUT ROWID",
    // A field the application kept as this replica's own on resuming, and the merged value it
    // stands against.
    "CREATE TABLE IF NOT EXISTS fongo_pins (
        collection TEXT NOT NULL,
        key TEXT NOT NULL,
        field TEXT NOT NULL,
        against,
        is_ref INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (collection, key, field))",
    // A found row a first install in steps records: its batch (-1 once adopted, kept until
    // every one is), the gid and the counter of the adoption clock a whole install would give
    // it, and its values when the install began.
    "CREATE TABLE IF NOT EXISTS fongo_found (
        collection TEXT NOT NULL,
        key TEXT NOT NULL,
        batch INTEGER NOT NULL,
        gid BLOB,
        seq INTEGER NOT NULL,
        image BLOB NOT NULL,
        PRIMARY KEY (collection, key))",
    "CREATE INDEX IF NOT EXISTS fongo_found_batch ON fongo_found (batch, collection, key)",
    "CREATE TABLE IF NOT EXISTS fongo_bases (
        collection TEXT NOT NULL,
        gid BLOB NOT NULL,
        field TEXT NOT NULL,
        key TEXT,
        base,
        base_is_ref INTEGER NOT NULL DEFAULT 0,
        base_clock TEXT,
        local,
        local_is_ref INTEGER NOT NULL DEFAULT 0,
        local_clock TEXT NOT NULL,
        seq INTEGER NOT NULL,
        reported TEXT,
        PRIMARY KEY (collection, gid, field))",
];

pub(crate) const LAYOUT: i64 = 4;

/// Earlier layouts the next install upgrades in place, adding what `TABLES` adds.
pub(crate) const UPGRADES: &[i64] = &[3];

/// The `fongo_meta` key that records that capture is suspended: set by `suspend` with dropping
/// the triggers, removed by `resume` with putting them back.
pub(crate) const SUSPENDED: &str = "suspended";

/// What the kernel's tables are, as every operation reads them before anything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    /// There are none.
    Absent,
    /// [`LAYOUT`], and whether suspending was recorded.
    Current { suspended: bool },
    /// An earlier layout in [`UPGRADES`].
    Older(i64),
    /// A layout later than [`LAYOUT`].
    Newer(i64),
    /// An earlier layout this build does not upgrade, or none recorded.
    Unknown(Option<i64>),
    /// Some of the tables of the layout recorded, or the record itself, are missing.
    Partial,
}

impl State {
    /// Whether suspending was recorded, for an operation that needs the tables at [`LAYOUT`].
    pub(crate) fn current(self) -> Result<bool> {
        match self {
            State::Current { suspended } => Ok(suspended),
            State::Absent => Err(Error::NotInstalled),
            State::Older(found) => Err(Error::OlderLayout {
                found,
                known: LAYOUT,
            }),
            State::Newer(found) => Err(Error::NewerLayout {
                found,
                known: LAYOUT,
            }),
            State::Unknown(found) => Err(Error::UnknownLayout {
                found,
                known: LAYOUT,
            }),
            State::Partial => Err(Error::Corrupt(
                "some of the kernel's tables are missing; uninstall them".into(),
            )),
        }
    }
}

/// The tables [`TABLES`] makes.
pub(crate) fn kernel_tables() -> impl Iterator<Item = &'static str> {
    TABLES.iter().filter_map(|sql| {
        sql.strip_prefix("CREATE TABLE IF NOT EXISTS ")
            .and_then(|rest| rest.split([' ', '(']).next())
    })
}

/// The table layout 4 added to layout 3.
const ADDED: &str = "fongo_found";

/// Reads [`State`] in one statement: an indexed read of `fongo_meta` and a count of the kernel's
/// tables in the schema. With `capture`, the same statement also names each collection ever
/// adopted whose three capture triggers are not all there, as [`capture`] returns them.
fn read(db: &Db, capture: bool) -> Result<(State, Vec<String>)> {
    let names = kernel_tables()
        .filter(|t| *t != ADDED)
        .map(|t| format!("'{t}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let lost = if capture {
        "UNION ALL
         SELECT 'lost', substr(key, 9) FROM fongo_meta
          WHERE key > 'adopted:' AND key < 'adopted;'
            AND (SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND name IN (
                 'fongo_' || substr(key, 9) || '_ai', 'fongo_' || substr(key, 9) || '_au',
                 'fongo_' || substr(key, 9) || '_ad')) < 3"
    } else {
        ""
    };
    let sql = format!(
        "SELECT key, value FROM fongo_meta WHERE key IN ('layout', '{SUSPENDED}')
         UNION ALL
         SELECT 'tables', count(*) FROM sqlite_master WHERE type = 'table' AND name IN ({names})
         UNION ALL
         SELECT 'added', count(*) FROM sqlite_master WHERE type = 'table' AND name = '{ADDED}'
         {lost}"
    );
    let rows = match db.query(&sql, &[]) {
        Ok(rows) => rows,
        Err(e) => {
            let found = db.query(
                &format!(
                    "SELECT sum(name = 'fongo_meta'), count(*) FROM sqlite_master
                      WHERE type = 'table' AND name IN ({names}, '{ADDED}')"
                ),
                &[],
            )?;
            return match found.first().map(|r| (&r[0], &r[1])) {
                Some((Value::Int(1), _)) => Err(e),
                Some((_, Value::Int(0))) => Ok((State::Absent, Vec::new())),
                _ => Ok((State::Partial, Vec::new())),
            };
        }
    };
    let core = i64::try_from(kernel_tables().filter(|t| *t != ADDED).count()).unwrap_or(0);
    let (mut layout, mut suspended, mut tables, mut added) = (None, false, 0, 0);
    let mut lost = Vec::new();
    for r in rows {
        match as_text(&r[0])? {
            "layout" => layout = Some(r[1].clone()),
            SUSPENDED => suspended = true,
            "tables" => tables = as_int(&r[1])?,
            "added" => added = as_int(&r[1])?,
            "lost" => lost.push(as_text(&r[1])?.to_string()),
            _ => {}
        }
    }
    let state = match layout {
        Some(Value::Int(LAYOUT)) if tables == core && added == 1 => State::Current { suspended },
        Some(Value::Int(LAYOUT)) => State::Partial,
        Some(Value::Int(found)) if found > LAYOUT => State::Newer(found),
        Some(Value::Int(found)) if UPGRADES.contains(&found) && tables == core => {
            State::Older(found)
        }
        Some(Value::Int(found)) if UPGRADES.contains(&found) => State::Partial,
        Some(Value::Int(found)) => State::Unknown(Some(found)),
        _ => State::Unknown(None),
    };
    Ok((state, lost))
}

/// [`State`], read in one statement.
pub(crate) fn state(db: &Db) -> Result<State> {
    Ok(read(db, false)?.0)
}

/// [`State`], and the collections ever adopted whose capture triggers are not all there: dropped
/// by a suspend, or with their table when an application rebuilt it.
pub(crate) fn capture(db: &Db) -> Result<(State, Vec<String>)> {
    read(db, true)
}

pub(crate) fn as_clock(value: &Value) -> Result<Hlc> {
    Hlc::decode(as_text(value)?).ok_or_else(|| Error::Corrupt("a clock does not parse".into()))
}

fn clock_text(clock: &Hlc) -> Value {
    Value::Text(clock.encode())
}

/// The facts, answering the evaluator.
pub(crate) struct Facts<'a> {
    pub db: &'a Db,
}

impl Facts<'_> {
    fn entries_of(&self, collection: &str, gid: Gid, field: &str) -> Result<Vec<(Value, Hlc)>> {
        let rows = self.db.query(
            "SELECT value, is_ref, clock FROM fongo_shadow
              WHERE collection = ?1 AND gid = ?2 AND field = ?3",
            &[
                Value::Text(collection.into()),
                Value::Ref(gid),
                Value::Text(field.into()),
            ],
        )?;
        rows.iter()
            .map(|r| Ok((loaded(&r[0], &r[1])?, as_clock(&r[2])?)))
            .collect()
    }

    /// Keeps a fact as [`fongo_sync_core::source::Store::keep`] does. Returns whether anything
    /// kept changed.
    pub(crate) fn keep(&self, fact: &Fact) -> Result<bool> {
        let collection = Value::Text(fact.collection.clone());
        let mut changed = self.db.exec(
            "INSERT INTO fongo_claims (gid, collection, first) VALUES (?1, ?2, ?3)
             ON CONFLICT (gid, collection) DO UPDATE SET first = excluded.first
              WHERE excluded.first < fongo_claims.first",
            &[
                Value::Ref(fact.gid),
                collection.clone(),
                clock_text(&fact.clock),
            ],
        )? > 0;
        match &fact.body {
            Body::Alias { target } => {
                changed |= self.db.exec(
                    "INSERT OR IGNORE INTO fongo_alias (collection, a, b) VALUES (?1, ?2, ?3)",
                    &[collection, Value::Ref(fact.gid), Value::Ref(*target)],
                )? > 0;
            }
            Body::Field { field, value, slot } => {
                let mut entries = self.entries_of(&fact.collection, fact.gid, field)?;
                let (kept, _) = keep(*slot, &mut entries, value, fact.clock);
                if kept {
                    changed = true;
                    let at = [
                        collection.clone(),
                        Value::Ref(fact.gid),
                        Value::Text(field.clone()),
                    ];
                    self.db.exec(
                        "DELETE FROM fongo_shadow WHERE collection = ?1 AND gid = ?2 AND field = ?3",
                        &at,
                    )?;
                    for (value, clock) in entries {
                        let (value, kind) = stored(&value);
                        self.db.exec(
                            "INSERT INTO fongo_shadow (collection, gid, field, value, is_ref, clock)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            &[at[0].clone(), at[1].clone(), at[2].clone(), value, kind, clock_text(&clock)],
                        )?;
                    }
                }
            }
        }
        Ok(changed)
    }

    /// Every collection with a fact about `gid`, with the clock of the first.
    pub(crate) fn claims_of(&self, gid: Gid) -> Result<Vec<Claim>> {
        let rows = self.db.query(
            "SELECT collection, first FROM fongo_claims WHERE gid = ?1",
            &[Value::Ref(gid)],
        )?;
        rows.iter()
            .map(|r| {
                Ok(Claim {
                    collection: as_text(&r[0])?.to_string(),
                    first: as_clock(&r[1])?,
                })
            })
            .collect()
    }

    /// The winning entry of a field among `members`, as a group shows it.
    pub(crate) fn register(
        &self,
        schema: &Schema,
        collection: &str,
        members: &[Gid],
        field: &str,
    ) -> Result<Option<(Value, Hlc)>> {
        let strategy = schema
            .collection(collection)
            .and_then(|c| c.fields.iter().find(|f| f.name == field))
            .map_or(fongo_sync_core::Strategy::Lww, |f| f.strategy);
        let slot = fongo_sync_core::source::slot(schema, collection, field);
        let mut best: Option<(Value, Hlc)> = None;
        for &m in members {
            let mut entries = self.entries_of(collection, m, field)?;
            if slot == Slot::Earliest {
                entries.sort_by_key(|e| std::cmp::Reverse(e.1));
                entries.truncate(1);
            }
            for (v, k) in entries {
                let strategy = if field == fongo_sync_core::DELETED {
                    fongo_sync_core::Strategy::Lww
                } else {
                    strategy
                };
                if fongo_sync_core::wins(strategy, best.as_ref().map(|(v, k)| (v, k)), &v, &k) {
                    best = Some((v, k));
                }
            }
        }
        Ok(best)
    }
}

impl Source for Facts<'_> {
    fn claims(&self, gid: Gid) -> Vec<Claim> {
        match self.claims_of(gid) {
            Ok(c) => c,
            Err(e) => {
                self.db.fail(e);
                Vec::new()
            }
        }
    }

    fn entries(&self, collection: &str, gid: Gid) -> Vec<Entry> {
        let rows = self.db.answer(
            "SELECT field, value, is_ref, clock FROM fongo_shadow
              WHERE collection = ?1 AND gid = ?2",
            &[Value::Text(collection.into()), Value::Ref(gid)],
        );
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            match (as_text(&r[0]), loaded(&r[1], &r[2]), as_clock(&r[3])) {
                (Ok(field), Ok(value), Ok(clock)) => out.push(Entry {
                    field: field.to_string(),
                    value,
                    clock,
                }),
                (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => self.db.fail(e),
            }
        }
        out
    }

    fn aliases(&self, gid: Gid) -> Vec<Alias> {
        let rows = self.db.answer(
            "SELECT collection, a, b FROM fongo_alias WHERE a = ?1
             UNION SELECT collection, a, b FROM fongo_alias WHERE b = ?1",
            &[Value::Ref(gid)],
        );
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            match (as_text(&r[0]), as_gid(&r[1]), as_gid(&r[2])) {
                (Ok(collection), Ok(from), Ok(to)) => out.push(Alias {
                    collection: collection.to_string(),
                    from,
                    to,
                }),
                (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => self.db.fail(e),
            }
        }
        out
    }

    fn holding(&self, collection: &str, fields: &[(&str, &Value)]) -> Vec<Gid> {
        let mut params = vec![Value::Text(collection.into())];
        let mut sql = String::new();
        if fields.is_empty() {
            sql.push_str("SELECT gid FROM fongo_claims WHERE collection = ?1");
        }
        // The rest are tested against each gid the first holds, so the first is the pair the
        // fewest entries hold, as far as a bounded count tells.
        let mut fields = fields.to_vec();
        if fields.len() > 1 {
            let mut counted: Vec<(i64, (&str, &Value))> = fields
                .iter()
                .map(|&(field, value)| {
                    let (value_, kind) = stored(value);
                    let n = self
                        .db
                        .answer(
                            "SELECT count(*) FROM (SELECT 1 FROM fongo_shadow INDEXED BY fongo_shadow_value
                              WHERE collection = ?1 AND field = ?2 AND value IS ?3 AND is_ref = ?4
                              LIMIT 256)",
                            &[Value::Text(collection.into()), Value::Text(field.into()), value_, kind],
                        )
                        .first()
                        .and_then(|r| match r[0] {
                            Value::Int(n) => Some(n),
                            _ => None,
                        })
                        .unwrap_or(0);
                    (n, (field, value))
                })
                .collect();
            counted.sort_by_key(|(n, _)| *n);
            fields = counted.into_iter().map(|(_, pair)| pair).collect();
        }
        for (i, (field, value)) in fields.iter().enumerate() {
            let (value, kind) = stored(value);
            let n = params.len();
            params.extend([Value::Text(field.to_string()), value, kind]);
            let test = format!(
                "s{i}.collection = ?1 AND s{i}.field = ?{} AND s{i}.value IS ?{} AND s{i}.is_ref = ?{}",
                n + 1,
                n + 2,
                n + 3
            );
            if i == 0 {
                sql.push_str(&format!(
                    "SELECT DISTINCT s0.gid FROM fongo_shadow s0 INDEXED BY fongo_shadow_value WHERE {test}"
                ));
            } else {
                sql.push_str(&format!(
                    " AND EXISTS (SELECT 1 FROM fongo_shadow s{i} INDEXED BY fongo_shadow_value
                       WHERE s{i}.gid = s0.gid AND {test})"
                ));
            }
        }
        let found: BTreeSet<Gid> = self
            .db
            .answer(&sql, &params)
            .iter()
            .filter_map(|r| as_gid(&r[0]).ok())
            .collect();
        found.into_iter().collect()
    }

    fn gids(&self) -> Vec<Gid> {
        self.db
            .answer("SELECT DISTINCT gid FROM fongo_claims ORDER BY gid", &[])
            .iter()
            .filter_map(|r| as_gid(&r[0]).ok())
            .collect()
    }
}

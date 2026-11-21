//! Local edits into changes: each row the triggers marked is compared with its image, never with
//! merged state, and `plan::edits` and `plan::record` say what that means.
//!
//! The image is read in gid terms through the row-to-gid mapping as it was before this collect,
//! the row through the mapping as it will be after it. A minted row whose local id was freed and
//! taken again stands for a new entity, so rows that name that id now name the new one and are
//! compared too; a natural row whose key, so read, changed stands for a new entity as well.
//!
//! A row a merge gave up still stands for the entity it stood for, a member of the group it
//! joined: its edits are that entity's, and a row that names it names that entity. A row that names
//! a row the kernel has not yet given an entity is not collected: it stays marked until it has one.

use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_core::model::Row;
use fongo_sync_core::plan::{self, FieldEdit, NewEntity};
use fongo_sync_core::{Change, Gid, Value};

use crate::db::{Db, as_gid, as_int, as_text};
use crate::error::Result;
use crate::image;
use crate::registry::Key;
use crate::replica::{Base, Ctx, Mapped, Replica, key_fields, mapping, same};
use crate::store::Facts;
use crate::tables::{self, local_of, minted_key, read_row};

struct Item {
    idx: usize,
    key: String,
    at: i64,
    gone: bool,
    /// Not marked by a trigger: found through a row whose local id was taken again.
    found: bool,
    row: Option<Row>,
    mapped: Option<Mapped>,
    waits: bool,
    /// The row still stands for this entity.
    keeps: Option<Gid>,
    /// The row stood for this entity and no longer does.
    old: Option<Gid>,
    /// The row stands for this entity from now on.
    new: Option<Gid>,
}

impl Item {
    fn usable(&self) -> Option<&Mapped> {
        self.mapped.as_ref().filter(|m| !m.doomed)
    }
}

/// What a local id names, as the mapping will be.
enum Named {
    Gid(Gid),
    Nothing,
    Waits,
}

impl Replica {
    /// Records every edit the application made since the last collect and merges the changes.
    /// Returns what they affect, for the caller to bring up to date.
    pub(crate) fn collect_edits(&self, cx: &mut Ctx) -> Result<BTreeSet<(String, Gid)>> {
        let db = cx.db;
        let marked = db.query(
            "SELECT collection, key, at, gone FROM fongo_dirty ORDER BY at, collection, key",
            &[],
        )?;
        if marked.is_empty() {
            return Ok(BTreeSet::new());
        }
        let mut items: Vec<Item> = Vec::new();
        let mut index: BTreeMap<(usize, String), usize> = BTreeMap::new();
        for r in marked {
            let name = as_text(&r[0])?;
            let key = as_text(&r[1])?.to_string();
            let Some(idx) = self.registry.index(name) else {
                db.exec(
                    "DELETE FROM fongo_dirty WHERE collection = ?1 AND key = ?2",
                    &[r[0].clone(), r[1].clone()],
                )?;
                continue;
            };
            let item = self.item(db, idx, key, as_int(&r[2])?, as_int(&r[3])? == 1, false)?;
            index.insert((idx, item.key.clone()), items.len());
            items.push(item);
        }

        // Minted rows: a fresh gid for each row new to its local id.
        let mut decided = 0;
        while decided < items.len() {
            let start = decided;
            decided = items.len();
            for i in start..decided {
                if !matches!(self.collection(items[i].idx).key, Key::Minted { .. }) {
                    continue;
                }
                self.decide_minted(db, &mut items[i])?;
                let item = &items[i];
                let (true, true, Some(local)) =
                    (item.old.is_some(), item.new.is_some(), local_of(&item.key))
                else {
                    continue;
                };
                let at = item.at;
                for (referrer, field) in self.referring(item.idx) {
                    let c = self.collection(referrer);
                    for key in tables::rows_naming(db, c, &field, local)? {
                        if index.contains_key(&(referrer, key.clone())) {
                            continue;
                        }
                        let found = self.item(db, referrer, key, at, false, true)?;
                        index.insert((referrer, found.key.clone()), items.len());
                        items.push(found);
                    }
                }
            }
        }
        // An entity whose target cannot be determined waits, and so does every row naming a row
        // made for it, before anything records a reference to that row's fresh gid.
        let ev = self.evaluator();
        let facts = Facts { db };
        let mut session = ev.session(&facts);
        for item in items.iter_mut() {
            if !item.waits
                && let Some(g) = item.keeps.or(item.old)
                && session.target(&self.collection(item.idx).name, g).is_err()
            {
                item.waits = true;
            }
        }
        let mut cache: BTreeMap<(usize, i64), Option<Mapped>> = BTreeMap::new();
        loop {
            let mut changed = false;
            for i in 0..items.len() {
                let c = self.collection(items[i].idx);
                if !matches!(c.key, Key::Minted { .. }) || items[i].waits {
                    continue;
                }
                let Some(row) = &items[i].row else { continue };
                let mut waits = false;
                for f in &c.fields {
                    if let (Some(t), Some(Value::Int(k))) =
                        (self.target_of(c, &f.name), row.get(&f.name))
                        && matches!(
                            self.will(db, &items, &index, &mut cache, t, *k)?,
                            Named::Waits
                        )
                    {
                        waits = true;
                    }
                }
                if waits {
                    items[i].waits = true;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }

        // Natural rows: the gid their key derives, read through the mapping as it will be.
        for i in 0..items.len() {
            let c = self.collection(items[i].idx);
            let Key::Natural { fields } = &c.key else {
                continue;
            };
            self.decide_common(&mut items[i]);
            if items[i].waits {
                continue;
            }
            let Some(row) = items[i].row.clone() else {
                continue;
            };
            let mut waits = false;
            let mut now = Row::new();
            for f in &c.fields {
                let value = row.get(&f.name).cloned().unwrap_or(Value::Null);
                let value = match (self.target_of(c, &f.name), &value) {
                    (Some(t), Value::Int(k)) => {
                        match self.will(db, &items, &index, &mut cache, t, *k)? {
                            Named::Gid(g) => Value::Ref(g),
                            Named::Nothing => Value::Null,
                            Named::Waits => {
                                waits = true;
                                Value::Null
                            }
                        }
                    }
                    _ => value,
                };
                now.insert(f.name.clone(), value);
            }
            if waits {
                items[i].waits = true;
                continue;
            }
            let values: Vec<Value> = fields
                .iter()
                .map(|f| now.get(f).cloned().unwrap_or(Value::Null))
                .collect();
            let item = &mut items[i];
            let mapped = if item.gone {
                item.usable()
            } else {
                item.mapped.as_ref()
            };
            match mapped.map(|m| (m.gid, m.image.clone())) {
                Some((gid, image)) if !item.gone => {
                    let then = self.image_gids(db, c, &image)?;
                    let kept = fields.iter().all(|f| {
                        same(
                            then.get(f).unwrap_or(&Value::Null),
                            now.get(f).unwrap_or(&Value::Null),
                        )
                    });
                    if kept {
                        item.keeps = Some(gid);
                    } else {
                        item.old = Some(gid);
                        item.new = Some(Gid::derive(&c.name, &values));
                    }
                }
                Some((gid, _)) => {
                    item.old = Some(gid);
                    item.new = Some(Gid::derive(&c.name, &values));
                }
                None => item.new = Some(Gid::derive(&c.name, &values)),
            }
        }

        // Record, in the order the application wrote.
        let mut order: Vec<usize> = (0..items.len()).collect();
        order.sort_by_key(|&i| (items[i].at, i));
        let mut changes: Vec<Change> = Vec::new();
        let mut touched = BTreeSet::new();
        let mut done = Vec::new();
        for i in order {
            if items[i].waits {
                continue;
            }
            let c = self.collection(items[i].idx);
            let name = c.name.clone();
            let after = match &items[i].row {
                Some(row) => {
                    Some(self.row_gids(db, &items, &index, &mut cache, items[i].idx, row)?)
                }
                None => None,
            };
            let item = &items[i];
            let found = match (item.keeps, item.mapped.as_ref()) {
                (Some(_), Some(m)) => {
                    let before = self.image_gids(db, c, &m.image)?;
                    plan::edits(&self.schema, &name, Some(&before), after.as_ref())
                }
                _ => {
                    let mut found = Vec::new();
                    if item.old.is_some() {
                        found.push(FieldEdit::Deleted);
                    }
                    if item.new.is_some() {
                        found.extend(plan::edits(&self.schema, &name, None, after.as_ref()));
                    }
                    found
                }
            };
            if !found.is_empty() {
                let entity = item.keeps.or(item.old);
                let old = match entity {
                    Some(g) => match session.target(&name, g) {
                        Ok(t) => Some(t),
                        Err(_) => {
                            items[i].waits = true;
                            continue;
                        }
                    },
                    None => None,
                };
                let clock = if item.at == 0 {
                    cx.adoption_clock()?
                } else {
                    cx.clock.tick(u64::try_from(item.at).unwrap_or(0))
                };
                let new = match item.new {
                    Some(gid) => Some(NewEntity {
                        gid,
                        known: facts.claims_of(gid)?.iter().any(|k| k.collection == name),
                    }),
                    None => None,
                };
                let recorded = plan::record(&self.schema, &name, &found, old.as_ref(), new, clock);
                if let (Some(_), Some(t)) = (item.keeps, &old) {
                    let members = if t.live.is_empty() {
                        t.members.clone()
                    } else {
                        t.live.clone()
                    };
                    for ch in &recorded {
                        let reports = c
                            .fields
                            .iter()
                            .any(|f| f.name == ch.field && f.reports_conflicts);
                        if reports {
                            cx.bases.push(Base {
                                collection: name.clone(),
                                gid: t.canonical,
                                key: item.key.clone(),
                                base: facts.register(&self.schema, &name, &members, &ch.field)?,
                                change: ch.clone(),
                            });
                        }
                    }
                }
                changes.extend(recorded);
                if let Some(g) = entity {
                    touched.insert((name.clone(), g));
                }
                if let Some(g) = item.new {
                    touched.insert((name.clone(), g));
                }
            }
            done.push(i);
        }
        db.check()?;
        drop(session);

        for i in done {
            let item = &items[i];
            let c = self.collection(item.idx);
            let at = [Value::Text(c.name.clone()), Value::Text(item.key.clone())];
            match (&item.row, item.keeps.or(item.new)) {
                (Some(row), Some(gid)) => {
                    if let (Some(_), Some(m)) = (item.keeps, item.mapped.as_ref()) {
                        for f in &c.fields {
                            let held = m.image.get(&f.name).unwrap_or(&Value::Null);
                            if !same(held, row.get(&f.name).unwrap_or(&Value::Null)) {
                                db.exec(
                                    "DELETE FROM fongo_pins WHERE collection = ?1 AND key = ?2 AND field = ?3",
                                    &[at[0].clone(), at[1].clone(), Value::Text(f.name.clone())],
                                )?;
                            }
                        }
                    } else {
                        db.exec(
                            "DELETE FROM fongo_pins WHERE collection = ?1 AND key = ?2",
                            &at,
                        )?;
                    }
                    let local = match c.key {
                        Key::Minted { .. } => local_of(&item.key).map_or(Value::Null, Value::Int),
                        Key::Natural { .. } => Value::Null,
                    };
                    // A row a merge gave up stays given up.
                    let doomed =
                        item.keeps.is_some() && item.mapped.as_ref().is_some_and(|m| m.doomed);
                    db.exec(
                        "INSERT OR REPLACE INTO fongo_ids (collection, key, gid, local, image, doomed)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                        &[
                            at[0].clone(),
                            at[1].clone(),
                            Value::Ref(gid),
                            local,
                            Value::Blob(image::encode(row)),
                            Value::Int(i64::from(doomed)),
                        ],
                    )?;
                }
                _ => {
                    db.exec(
                        "DELETE FROM fongo_ids WHERE collection = ?1 AND key = ?2",
                        &at,
                    )?;
                    db.exec(
                        "DELETE FROM fongo_pins WHERE collection = ?1 AND key = ?2",
                        &at,
                    )?;
                }
            }
            if !item.found {
                db.exec(
                    "DELETE FROM fongo_dirty WHERE collection = ?1 AND key = ?2",
                    &at,
                )?;
            }
        }
        for item in items.iter().filter(|i| i.waits && i.found) {
            db.exec(
                "INSERT OR IGNORE INTO fongo_dirty (collection, key, at) VALUES (?1, ?2, ?3)",
                &[
                    Value::Text(self.collection(item.idx).name.clone()),
                    Value::Text(item.key.clone()),
                    Value::Int(item.at),
                ],
            )?;
        }

        let mut set = self.merge(cx, &changes, true)?;
        set.extend(touched);
        self.remember_bases(cx)?;
        Ok(set)
    }

    fn item(
        &self,
        db: &Db,
        idx: usize,
        key: String,
        at: i64,
        gone: bool,
        found: bool,
    ) -> Result<Item> {
        let c = self.collection(idx);
        Ok(Item {
            idx,
            row: read_row(db, c, &key)?,
            mapped: mapping(db, &c.name, &key)?,
            key,
            at,
            gone,
            found,
            waits: false,
            keeps: None,
            old: None,
            new: None,
        })
    }

    /// What every row decides alone: a row a merge gave up is forgotten once the application
    /// removes or replaces it; a removed row's entity loses its row.
    fn decide_common(&self, item: &mut Item) {
        if item.row.is_none()
            && let Some(m) = item.usable()
        {
            item.old = Some(m.gid);
        }
    }

    fn decide_minted(&self, db: &Db, item: &mut Item) -> Result<()> {
        self.decide_common(item);
        if item.waits || item.row.is_none() {
            return Ok(());
        }
        match item.mapped.as_ref().map(|m| m.gid) {
            Some(gid) if !item.gone => item.keeps = Some(gid),
            _ => {
                // A given-up row whose id was taken again has no old entity, so no rows naming it
                // are looked for: with foreign keys enforced, its removal changed or refused them.
                item.old = item.usable().map(|m| m.gid);
                let fresh = db.query("SELECT randomblob(16)", &[])?;
                item.new = Some(as_gid(&fresh[0][0])?);
            }
        }
        Ok(())
    }

    /// What the local id `local` of minted collection `t` names once this collect is done.
    fn will(
        &self,
        db: &Db,
        items: &[Item],
        index: &BTreeMap<(usize, String), usize>,
        cache: &mut BTreeMap<(usize, i64), Option<Mapped>>,
        t: usize,
        local: i64,
    ) -> Result<Named> {
        let key = minted_key(local);
        if let Some(&i) = index.get(&(t, key.clone())) {
            let item = &items[i];
            if item.waits {
                return Ok(match &item.mapped {
                    Some(m) if !item.gone => Named::Gid(m.gid),
                    _ => Named::Waits,
                });
            }
            return Ok(match item.new.or(item.keeps) {
                Some(g) => Named::Gid(g),
                None => Named::Nothing,
            });
        }
        let c = self.collection(t);
        if let std::collections::btree_map::Entry::Vacant(slot) = cache.entry((t, local)) {
            slot.insert(mapping(db, &c.name, &key)?);
        }
        Ok(match &cache[&(t, local)] {
            Some(m) => Named::Gid(m.gid),
            None if read_row(db, c, &key)?.is_some() => Named::Waits,
            None => Named::Nothing,
        })
    }

    /// An image in gid terms, through the mapping as it was.
    pub(crate) fn image_gids(
        &self,
        db: &Db,
        c: &crate::registry::Collection,
        image: &Row,
    ) -> Result<Row> {
        let mut out = image.clone();
        for f in &c.fields {
            if let (Some(t), Some(Value::Int(k))) = (self.target_of(c, &f.name), image.get(&f.name))
            {
                let named = mapping(db, &self.collection(t).name, &minted_key(*k))?;
                out.insert(
                    f.name.clone(),
                    named.map_or(Value::Null, |m| Value::Ref(m.gid)),
                );
            }
        }
        Ok(out)
    }

    /// A row in gid terms, through the mapping as it will be.
    fn row_gids(
        &self,
        db: &Db,
        items: &[Item],
        index: &BTreeMap<(usize, String), usize>,
        cache: &mut BTreeMap<(usize, i64), Option<Mapped>>,
        idx: usize,
        row: &Row,
    ) -> Result<Row> {
        let c = self.collection(idx);
        let mut out = row.clone();
        for f in &c.fields {
            if let (Some(t), Some(Value::Int(k))) = (self.target_of(c, &f.name), row.get(&f.name)) {
                let value = match self.will(db, items, index, cache, t, *k)? {
                    Named::Gid(g) => Value::Ref(g),
                    _ => Value::Null,
                };
                out.insert(f.name.clone(), value);
            }
        }
        let _ = key_fields(c);
        Ok(out)
    }
}

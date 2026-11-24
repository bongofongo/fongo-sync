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
//!
//! Marked rows are collected in the order they were last written, then by collection and key. A
//! bounded step takes a prefix of that order, and only one whose collecting reads no marked row
//! past it: a row it names that collecting could give another entity, or one naming a row it
//! gives a new entity, is collected with it, or the prefix grows to reach it.

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

/// Where a marked row stands in the order rows are collected in: when it was last written, then
/// its collection and key.
pub(crate) type Place = (i64, String, String);

/// Entities minted for rows by an earlier try of the same step, so that trying again mints the
/// same ones.
pub(crate) type Minted = BTreeMap<(usize, String), Gid>;

/// What [`Replica::collect_edits`] found.
pub(crate) enum Edits {
    /// The edits are recorded and merged; what they affect, and how many marked rows were
    /// collected.
    Recorded(BTreeSet<(String, Gid)>, usize),
    /// A row of the prefix depends on a marked row past it, the latest at this place. Nothing was
    /// written.
    Beyond(Place),
    /// The prefix would collect this many rows, fewer than asked for, while more are marked.
    /// Nothing was written.
    Short(usize),
}

/// Answers to what local ids name, and the latest marked row past the prefix one of them read.
struct Names {
    cache: BTreeMap<(usize, i64), Option<Mapped>>,
    bounded: bool,
    beyond: Option<Place>,
}

impl Names {
    fn past(&mut self, place: Place) {
        if self.beyond.as_ref().is_none_or(|b| *b < place) {
            self.beyond = Some(place);
        }
    }
}

/// When the row at `key` was marked, if it is, and whether a row left the key since.
pub(crate) fn marked_at(db: &Db, collection: &str, key: &str) -> Result<Option<(i64, bool)>> {
    let rows = db.query(
        "SELECT at, gone FROM fongo_dirty WHERE collection = ?1 AND key = ?2",
        &[Value::Text(collection.into()), Value::Text(key.into())],
    )?;
    rows.first()
        .map(|r| Ok((as_int(&r[0])?, as_int(&r[1])? > 0)))
        .transpose()
}

/// The place of the marked row `skip` rows after `after` in collecting order (after the start
/// when `after` is `None`), and whether any row follows it.
pub(crate) fn place_after(
    db: &Db,
    after: Option<&Place>,
    skip: usize,
) -> Result<Option<(Place, bool)>> {
    let skip = Value::Int(i64::try_from(skip).unwrap_or(i64::MAX));
    let rows = match after {
        None => db.query(
            "SELECT at, collection, key FROM fongo_dirty
              ORDER BY at, collection, key LIMIT 2 OFFSET ?1",
            &[skip],
        )?,
        Some((at, collection, key)) => db.query(
            "SELECT at, collection, key FROM fongo_dirty WHERE (at, collection, key) > (?1, ?2, ?3)
              ORDER BY at, collection, key LIMIT 2 OFFSET ?4",
            &[
                Value::Int(*at),
                Value::Text(collection.clone()),
                Value::Text(key.clone()),
                skip,
            ],
        )?,
    };
    let Some(r) = rows.first() else {
        return Ok(None);
    };
    let place = (
        as_int(&r[0])?,
        as_text(&r[1])?.to_string(),
        as_text(&r[2])?.to_string(),
    );
    Ok(Some((place, rows.len() > 1)))
}

/// How many rows are marked up to `place`, or in all with `None`.
pub(crate) fn marked_through(db: &Db, place: &Option<Place>) -> Result<i64> {
    let rows = match place {
        None => db.query("SELECT count(*) FROM fongo_dirty", &[])?,
        Some((at, collection, key)) => db.query(
            "SELECT count(*) FROM fongo_dirty WHERE (at, collection, key) <= (?1, ?2, ?3)",
            &[
                Value::Int(*at),
                Value::Text(collection.clone()),
                Value::Text(key.clone()),
            ],
        )?,
    };
    rows.first().map_or(Ok(0), |r| as_int(&r[0]))
}

/// Whether any row is marked past `place`.
pub(crate) fn marked_past(db: &Db, place: &Place) -> Result<bool> {
    let (at, collection, key) = place;
    Ok(!db
        .query(
            "SELECT 1 FROM fongo_dirty WHERE (at, collection, key) > (?1, ?2, ?3) LIMIT 1",
            &[
                Value::Int(*at),
                Value::Text(collection.clone()),
                Value::Text(key.clone()),
            ],
        )?
        .is_empty())
}

struct Item {
    idx: usize,
    key: String,
    at: i64,
    gone: bool,
    /// The key's row was removed, not only moved to another key: a row there again is new.
    removed: bool,
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
    /// Records every edit the application made since the last collect, or with `through` those
    /// of the rows marked up to that place, and merges the changes. With `want`, a prefix that
    /// would collect fewer than that many rows, while more are marked, is reported short.
    pub(crate) fn collect_edits(
        &self,
        cx: &mut Ctx,
        through: Option<&Place>,
        want: Option<usize>,
        minted: &mut Minted,
    ) -> Result<Edits> {
        let db = cx.db;
        let marked = match through {
            None => db.query(
                "SELECT collection, key, at, gone FROM fongo_dirty ORDER BY at, collection, key",
                &[],
            )?,
            Some((at, collection, key)) => db.query(
                "SELECT collection, key, at, gone FROM fongo_dirty
                  WHERE (at, collection, key) <= (?1, ?2, ?3) ORDER BY at, collection, key",
                &[
                    Value::Int(*at),
                    Value::Text(collection.clone()),
                    Value::Text(key.clone()),
                ],
            )?,
        };
        if marked.is_empty() {
            return Ok(Edits::Recorded(BTreeSet::new(), 0));
        }
        let mut names = Names {
            cache: BTreeMap::new(),
            bounded: through.is_some(),
            beyond: None,
        };
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
            let item = self.item(db, idx, key, as_int(&r[2])?, as_int(&r[3])?, false)?;
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
                self.decide_minted(db, &mut items[i], minted)?;
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
                        if names.bounded
                            && let Some((at, _)) = marked_at(db, &c.name, &key)?
                        {
                            names.past((at, c.name.clone(), key));
                            continue;
                        }
                        let found = self.item(db, referrer, key, at, 0, true)?;
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
                            self.will(db, &items, &index, &mut names, t, *k)?,
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
                        match self.will(db, &items, &index, &mut names, t, *k)? {
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
            // A key whose row moved away and back is compared with the key's image; one whose
            // row was removed and made again is a new entity.
            match mapped.map(|m| (m.gid, m.image.clone())) {
                Some((gid, image)) if !item.removed => {
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

        if let Some(place) = names.beyond.take() {
            return Ok(Edits::Beyond(place));
        }
        if let (Some(want), Some(place)) = (want, through) {
            let rows = items.iter().filter(|i| !i.waits && !i.found).count();
            if rows < want.div_ceil(2) && marked_past(db, place)? {
                return Ok(Edits::Short(rows));
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
                    Some(self.row_gids(db, &items, &index, &mut names, items[i].idx, row)?)
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
        if let Some(place) = names.beyond.take() {
            return Ok(Edits::Beyond(place));
        }

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

        let collected = items.iter().filter(|i| !i.waits && !i.found).count();
        let mut set = match self.merge_unless(cx, &changes, true, through)? {
            Ok(set) => set,
            Err(past) => return Ok(Edits::Beyond(past)),
        };
        set.extend(touched);
        self.remember_bases(cx)?;
        Ok(Edits::Recorded(set, collected))
    }

    fn item(
        &self,
        db: &Db,
        idx: usize,
        key: String,
        at: i64,
        gone: i64,
        found: bool,
    ) -> Result<Item> {
        let c = self.collection(idx);
        Ok(Item {
            idx,
            row: read_row(db, c, &key)?,
            mapped: mapping(db, &c.name, &key)?,
            key,
            at,
            gone: gone >= tables::MOVED,
            removed: gone == tables::REMOVED,
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

    fn decide_minted(&self, db: &Db, item: &mut Item, minted: &mut Minted) -> Result<()> {
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
                let gid = match minted.get(&(item.idx, item.key.clone())) {
                    Some(gid) => *gid,
                    None => {
                        let fresh = db.query("SELECT randomblob(16)", &[])?;
                        let gid = as_gid(&fresh[0][0])?;
                        minted.insert((item.idx, item.key.clone()), gid);
                        gid
                    }
                };
                item.new = Some(gid);
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
        names: &mut Names,
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
        if !names.cache.contains_key(&(t, local)) {
            let mapped = mapping(db, &c.name, &key)?;
            // A marked row past the prefix that keeps its entity names the same one once
            // collected; any other may not.
            if names.bounded
                && let Some((at, gone)) = marked_at(db, &c.name, &key)?
                && (gone || mapped.is_none() || read_row(db, c, &key)?.is_none())
            {
                names.past((at, c.name.clone(), key.clone()));
            }
            names.cache.insert((t, local), mapped);
        }
        Ok(match &names.cache[&(t, local)] {
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
        names: &mut Names,
        idx: usize,
        row: &Row,
    ) -> Result<Row> {
        let c = self.collection(idx);
        let mut out = row.clone();
        for f in &c.fields {
            if let (Some(t), Some(Value::Int(k))) = (self.target_of(c, &f.name), row.get(&f.name)) {
                let value = match self.will(db, items, index, names, t, *k)? {
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

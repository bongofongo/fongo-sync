//! Bringing rows to their targets: the entities the core names are evaluated, identity clashes
//! aliased, each live canonical entity given one row, and the writes made in the order
//! `plan::order` gives. A row the plan reports stuck, and a write the database refuses, is held:
//! its row and image stay as they were and it is tried again by every later operation.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use fongo_sync_core::model::Row;
use fongo_sync_core::plan::{self, Planned, RowAction};
use fongo_sync_core::{ALIAS, Change, Gid, Value};

use crate::db::{Db, as_gid, as_text, loaded, quoted, refused};
use crate::error::Result;
use crate::image;
use crate::registry::{Key, LocalDefault};
use crate::replica::{Ctx, Replica, mapping, rows_of, same};
use crate::store::Facts;
use crate::tables::{self, local_of, minted_key, where_key};

/// A row in a plan: one the table holds, or one to insert for an entity.
struct Slot {
    idx: usize,
    /// The row's key while it is in the table.
    key: Option<String>,
    image: Option<Row>,
    /// The entity the row is held under if its write is refused.
    owner: Option<Gid>,
    /// The entity reported with the row.
    report: Gid,
    /// A row to be removed while its entity, if shown, keeps another.
    doomed: bool,
}

#[derive(Default)]
struct Rows {
    slots: Vec<Slot>,
    existing: HashMap<(usize, String), usize>,
    fresh: HashMap<(usize, Gid), usize>,
}

const MARK: [u8; 8] = [0xA5; 8];
/// A reference to an entity that has no row here.
const MISSING: Gid = Gid([0xA6; 16]);

fn rid(i: usize) -> Gid {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&MARK);
    bytes[8..].copy_from_slice(&(i as u64).to_be_bytes());
    Gid(bytes)
}

fn slot_of(g: Gid) -> Option<usize> {
    (g.0[..8] == MARK)
        .then(|| u64::from_be_bytes(g.0[8..].try_into().expect("eight bytes")) as usize)
}

fn root(parent: &HashMap<(String, Gid), (String, Gid)>, n: (String, Gid)) -> (String, Gid) {
    let mut at = n;
    while let Some(up) = parent.get(&at) {
        at = up.clone();
    }
    at
}

impl Rows {
    fn existing(&mut self, idx: usize, key: &str) -> usize {
        if let Some(&i) = self.existing.get(&(idx, key.to_string())) {
            return i;
        }
        let i = self.slots.len();
        self.slots.push(Slot {
            idx,
            key: Some(key.to_string()),
            image: None,
            owner: None,
            report: MISSING,
            doomed: false,
        });
        self.existing.insert((idx, key.to_string()), i);
        i
    }

    fn fresh(&mut self, idx: usize, gid: Gid) -> usize {
        if let Some(&i) = self.fresh.get(&(idx, gid)) {
            return i;
        }
        let i = self.slots.len();
        self.slots.push(Slot {
            idx,
            key: None,
            image: None,
            owner: Some(gid),
            report: gid,
            doomed: false,
        });
        self.fresh.insert((idx, gid), i);
        i
    }
}

impl Replica {
    /// Brings every row of `set`'s entities, and of the entities held before, to its target.
    /// Returns how many writes succeeded.
    pub(crate) fn reconcile(
        &self,
        cx: &mut Ctx,
        mut set: BTreeSet<(String, Gid)>,
    ) -> Result<usize> {
        let db = cx.db;
        let ev = self.evaluator();
        let facts = Facts { db };
        for r in db.query("SELECT collection, gid FROM fongo_held", &[])? {
            set.insert((as_text(&r[0])?.to_string(), as_gid(&r[1])?));
        }
        db.exec("DELETE FROM fongo_held", &[])?;
        set.retain(|(c, _)| self.registry.index(c).is_some());
        let mut held: BTreeSet<(String, Gid)> = BTreeSet::new();
        // Entities left as they are because a question about them has no answer within limits.
        let mut blocked: BTreeSet<(String, Gid)> = BTreeSet::new();

        // A session's answers stand until facts change, so a new one follows each merge.
        let mut session = ev.session(&facts);
        loop {
            let mut pairs = BTreeSet::new();
            let mut seen = BTreeSet::new();
            for (c, g) in &set {
                let Ok(t) = session.target(c, *g) else {
                    continue;
                };
                if t.row.is_none() || !seen.insert((c.clone(), t.canonical)) {
                    continue;
                }
                match plan::clashes(&mut session, &t) {
                    Ok(found) => {
                        for o in found {
                            pairs.insert((c.clone(), t.canonical.min(o), t.canonical.max(o)));
                        }
                    }
                    Err(_) => {
                        blocked.insert((c.clone(), t.canonical));
                    }
                }
            }
            db.check()?;
            if pairs.is_empty() {
                break;
            }
            // A group is the connected component of its aliases, so one edge joining each two
            // parts is enough: k entities that clash at once take k - 1 aliases, not k² / 2.
            let mut parent: HashMap<(String, Gid), (String, Gid)> = HashMap::new();
            pairs.retain(|(c, a, b)| {
                let (ra, rb) = (
                    root(&parent, (c.clone(), *a)),
                    root(&parent, (c.clone(), *b)),
                );
                if ra == rb {
                    return false;
                }
                parent.insert(rb, ra);
                true
            });
            let aliases: Vec<Change> = pairs
                .into_iter()
                .map(|(c, a, b)| Change {
                    collection: c,
                    gid: b,
                    field: ALIAS.into(),
                    value: Value::Ref(a),
                    clock: cx.clock.tick(cx.wall),
                })
                .collect();
            set.extend(self.merge(cx, &aliases, true)?);
            session = ev.session(&facts);
        }

        type Wanted = BTreeMap<(usize, Gid), (Option<Row>, BTreeMap<String, Gid>)>;
        let (wanted, doomed, keeps) = loop {
            let mut wanted: Wanted = BTreeMap::new();
            // Rows to remove: the entity held if that fails, and the entity reported.
            let mut doomed: BTreeMap<(usize, String), (Gid, Gid)> = BTreeMap::new();
            for (c, g) in &set {
                let idx = self.registry.index(c).expect("only registered collections");
                let mine = rows_of(db, c, *g)?;
                for (key, _) in &mine {
                    cx.looked_at(c, key);
                }
                let t = match session.target(c, *g) {
                    Ok(t) => t,
                    Err(_) => {
                        held.insert((c.clone(), *g));
                        continue;
                    }
                };
                if blocked.contains(&(c.clone(), t.canonical)) {
                    held.insert((c.clone(), t.canonical));
                    continue;
                }
                if t.members.is_empty() {
                    for (key, _) in mine {
                        doomed.insert((idx, key), (*g, *g));
                    }
                    continue;
                }
                let entry = wanted.entry((idx, t.canonical)).or_default();
                entry.0 = t.row.clone();
                for (key, _) in mine {
                    entry.1.insert(key, *g);
                }
                for (key, _) in rows_of(db, c, t.canonical)? {
                    cx.looked_at(c, &key);
                    entry.1.insert(key, t.canonical);
                }
            }
            let mut keeps: BTreeMap<(usize, Gid), Option<String>> = BTreeMap::new();
            for ((idx, canon), (row, keys)) in &wanted {
                if row.is_none() {
                    for (key, was) in keys {
                        doomed.insert((*idx, key.clone()), (*was, *canon));
                    }
                    continue;
                }
                let keep = keys
                    .iter()
                    .find(|(_, was)| *was == canon)
                    .or_else(|| keys.iter().next())
                    .map(|(k, _)| k.clone());
                for (key, was) in keys {
                    if Some(key) != keep.as_ref() {
                        doomed.insert((*idx, key.clone()), (*was, *canon));
                    }
                }
                keeps.insert((*idx, *canon), keep);
            }
            // A row about to be removed may still be named by rows whose targets did not change;
            // they must be rewritten to name the row that now stands for the entity.
            let mut more = Vec::new();
            for (idx, key) in doomed.keys() {
                let Some(local) = local_of(key)
                    .filter(|_| matches!(self.collection(*idx).key, Key::Minted { .. }))
                else {
                    continue;
                };
                for (referrer, field) in self.referring(*idx) {
                    let c = self.collection(referrer);
                    for k in tables::rows_naming(db, c, &field, local)? {
                        cx.looked_at(&c.name, &k);
                        if let Some(m) = mapping(db, &c.name, &k)?
                            && !set.contains(&(c.name.clone(), m.gid))
                        {
                            more.push((c.name.clone(), m.gid));
                        }
                    }
                }
            }
            if more.is_empty() {
                break (wanted, doomed, keeps);
            }
            set.extend(more);
        };
        db.check()?;

        let mut rows = Rows::default();
        let mut row_for: BTreeMap<(usize, Gid), usize> = BTreeMap::new();
        for ((idx, canon), keep) in &keeps {
            let i = match keep {
                Some(key) => {
                    db.exec(
                        "UPDATE fongo_ids SET gid = ?3, doomed = 0 WHERE collection = ?1 AND key = ?2",
                        &[
                            Value::Text(self.collection(*idx).name.clone()),
                            Value::Text(key.clone()),
                            Value::Ref(*canon),
                        ],
                    )?;
                    let i = rows.existing(*idx, key);
                    rows.slots[i].owner = Some(*canon);
                    i
                }
                None => rows.fresh(*idx, *canon),
            };
            rows.slots[i].report = *canon;
            row_for.insert((*idx, *canon), i);
        }
        for ((idx, key), (owner, report)) in &doomed {
            let i = rows.existing(*idx, key);
            let s = &mut rows.slots[i];
            s.owner = Some(*owner);
            s.report = *report;
            s.doomed = true;
        }

        let columns = tables::columns(db, &self.registry)?;
        let pinning = !db
            .query("SELECT 1 FROM fongo_pins LIMIT 1", &[])?
            .is_empty();
        let mut planned = Vec::new();
        for ((idx, canon), (row, _)) in &wanted {
            let Some(row) = row else { continue };
            let i = row_for[&(*idx, *canon)];
            let c = self.collection(*idx);
            let image = self.load_image(db, &mut rows, i)?;
            let image_rids = image
                .as_ref()
                .map(|im| self.image_rids(&mut rows, *idx, im));
            let mut target = Row::new();
            for (f, value) in row {
                let value = match (self.target_of(c, f), value) {
                    (Some(t), Value::Ref(g)) => {
                        Value::Ref(self.row_for(db, &mut rows, &mut row_for, &doomed, t, *g)?)
                    }
                    _ => value.clone(),
                };
                target.insert(f.clone(), value);
            }
            if pinning && let Some(key) = rows.slots[i].key.clone() {
                for r in db.query(
                    "SELECT field, against, is_ref FROM fongo_pins WHERE collection = ?1 AND key = ?2",
                    &[Value::Text(c.name.clone()), Value::Text(key.clone())],
                )? {
                    let field = as_text(&r[0])?.to_string();
                    let against = loaded(&r[1], &r[2])?;
                    let stands = row.get(&field).is_some_and(|now| same(now, &against));
                    match image_rids.as_ref().and_then(|im| im.get(&field)) {
                        Some(kept) if stands => {
                            target.insert(field, kept.clone());
                        }
                        _ => {
                            db.exec(
                                "DELETE FROM fongo_pins WHERE collection = ?1 AND key = ?2 AND field = ?3",
                                &[Value::Text(c.name.clone()), Value::Text(key.clone()), Value::Text(field)],
                            )?;
                        }
                    }
                }
            }
            planned.push(Planned {
                collection: c.name.clone(),
                gid: rid(i),
                action: plan::write(Some(&target), image_rids.as_ref()),
                image: image_rids,
            });
        }
        for (idx, key) in doomed.keys() {
            let i = rows.existing(*idx, key);
            let image = self.load_image(db, &mut rows, i)?;
            planned.push(Planned {
                collection: self.collection(*idx).name.clone(),
                gid: rid(i),
                image: image
                    .as_ref()
                    .map(|im| self.image_rids(&mut rows, *idx, im)),
                action: RowAction::Delete,
            });
        }

        let plan = plan::order(&self.schema, &columns, &planned);
        let mut failed: BTreeSet<usize> = BTreeSet::new();
        let mut wrote = 0;
        for (_, g) in &plan.stuck {
            let Some(i) = slot_of(*g) else { continue };
            failed.insert(i);
            self.refused(db, &rows, i, &mut held)?;
        }
        for step in &plan.steps {
            let Some(i) = slot_of(step.gid) else { continue };
            if failed.contains(&i) {
                continue;
            }
            let ok = match &step.action {
                RowAction::Nothing => true,
                RowAction::Insert(row) => self.insert(cx, &mut rows, i, row)?,
                RowAction::Update(fields) => self.update(cx, &mut rows, i, fields)?,
                RowAction::Delete => self.delete(cx, &mut rows, &row_for, i)?,
            };
            if ok {
                wrote += usize::from(step.action != RowAction::Nothing);
            } else {
                failed.insert(i);
                self.refused(db, &rows, i, &mut held)?;
            }
        }
        for (c, g) in &held {
            db.exec(
                "INSERT OR IGNORE INTO fongo_held (collection, gid) VALUES (?1, ?2)",
                &[Value::Text(c.clone()), Value::Ref(*g)],
            )?;
        }
        db.check()?;
        Ok(wrote)
    }

    /// Notes, for a bounded step, the rows a row it writes or removes named: one collected later
    /// may make one of them again and find the row naming it.
    fn named_by(&self, cx: &mut Ctx, rows: &Rows, i: usize) {
        let s = &rows.slots[i];
        let (Some(image), true) = (&s.image, cx.watch.is_some()) else {
            return;
        };
        let c = self.collection(s.idx);
        for f in &c.fields {
            if let (Some(t), Some(Value::Int(local))) =
                (self.target_of(c, &f.name), image.get(&f.name))
            {
                cx.looked_at(&self.collection(t).name, &minted_key(*local));
            }
        }
    }

    fn refused(
        &self,
        db: &Db,
        rows: &Rows,
        i: usize,
        held: &mut BTreeSet<(String, Gid)>,
    ) -> Result<()> {
        let s = &rows.slots[i];
        let name = &self.collection(s.idx).name;
        if let Some(owner) = s.owner {
            held.insert((name.clone(), owner));
        }
        if s.doomed
            && let Some(key) = &s.key
        {
            db.exec(
                "UPDATE fongo_ids SET doomed = 1 WHERE collection = ?1 AND key = ?2",
                &[Value::Text(name.clone()), Value::Text(key.clone())],
            )?;
        }
        Ok(())
    }

    fn load_image(&self, db: &Db, rows: &mut Rows, i: usize) -> Result<Option<Row>> {
        let s = &rows.slots[i];
        if s.image.is_none()
            && let Some(key) = &s.key
        {
            let image = mapping(db, &self.collection(s.idx).name, key)?.map(|m| m.image);
            rows.slots[i].image = image;
        }
        Ok(rows.slots[i].image.clone())
    }

    /// An image with each reference as the plan's name for the row it names.
    fn image_rids(&self, rows: &mut Rows, idx: usize, image: &Row) -> Row {
        let c = self.collection(idx);
        let mut out = image.clone();
        for (f, value) in image {
            if let (Some(t), Value::Int(k)) = (self.target_of(c, f), value) {
                out.insert(
                    f.clone(),
                    Value::Ref(rid(rows.existing(t, &minted_key(*k)))),
                );
            }
        }
        out
    }

    /// The plan's name for the row that stands for entity `g` of collection `t`.
    fn row_for(
        &self,
        db: &Db,
        rows: &mut Rows,
        row_for: &mut BTreeMap<(usize, Gid), usize>,
        doomed: &BTreeMap<(usize, String), (Gid, Gid)>,
        t: usize,
        g: Gid,
    ) -> Result<Gid> {
        if let Some(&i) = row_for.get(&(t, g)) {
            return Ok(rid(i));
        }
        let name = &self.collection(t).name;
        let Some((key, _)) = rows_of(db, name, g)?
            .into_iter()
            .find(|(key, was_doomed)| !was_doomed && !doomed.contains_key(&(t, key.clone())))
        else {
            return Ok(MISSING);
        };
        let i = rows.existing(t, &key);
        row_for.insert((t, g), i);
        Ok(rid(i))
    }

    /// A planned row's values as the table takes them: each reference as the local id of the
    /// row it names. `None` if one names a row that is not there, or is a NaN, which SQLite
    /// would store as NULL.
    fn to_local(&self, rows: &Rows, idx: usize, row: &Row) -> Option<Row> {
        let c = self.collection(idx);
        let mut out = row.clone();
        for (f, value) in row {
            if matches!(value, Value::Real(r) if r.is_nan()) {
                return None;
            }
            if let (Some(_), Value::Ref(g)) = (self.target_of(c, f), value) {
                let s = &rows.slots[slot_of(*g)?];
                out.insert(f.clone(), Value::Int(local_of(s.key.as_ref()?)?));
            }
        }
        Some(out)
    }

    /// What a row holds after a write, as the table stores it: a value of another kind than its
    /// column's may have been converted. The image records that, so a collect never takes the
    /// conversion for an edit.
    fn stored_row(&self, db: &Db, idx: usize, key: &str, image: Row, wrote: &Row) -> Result<Row> {
        let c = self.collection(idx);
        let Some(row) = tables::read_row(db, c, key)? else {
            return Ok(image);
        };
        let mut image = image;
        for f in wrote.keys() {
            if let Some(v) = row.get(f) {
                image.insert(f.clone(), v.clone());
            }
        }
        Ok(image)
    }

    fn insert(&self, cx: &mut Ctx, rows: &mut Rows, i: usize, row: &Row) -> Result<bool> {
        let db = cx.db;
        let idx = rows.slots[i].idx;
        let c = self.collection(idx);
        let Some(values) = self.to_local(rows, idx, row) else {
            return Ok(false);
        };
        let mut names = Vec::new();
        let mut marks = Vec::new();
        let mut params = Vec::new();
        let (key, local) = match &c.key {
            Key::Minted { column } => {
                let next = db.query(
                    &format!(
                        "SELECT max(COALESCE((SELECT max({col}) FROM {table}), 0),
                                    COALESCE((SELECT max(local) FROM fongo_ids WHERE collection = ?1), 0)) + 1",
                        col = quoted(column),
                        table = quoted(&c.table)
                    ),
                    &[Value::Text(c.name.clone())],
                )?;
                let local = match next.first().map(|r| &r[0]) {
                    Some(Value::Int(n)) => *n,
                    _ => 1,
                };
                names.push(quoted(column));
                marks.push(format!("?{}", params.len() + 1));
                params.push(Value::Int(local));
                (minted_key(local), Value::Int(local))
            }
            Key::Natural { fields } => {
                let key: Vec<Value> = fields
                    .iter()
                    .map(|f| values.get(f).cloned().unwrap_or(Value::Null))
                    .collect();
                match tables::natural_key(db, &key)? {
                    Some(key) => (key, Value::Null),
                    None => return Ok(false),
                }
            }
        };
        cx.looked_at(&c.name, &key);
        for (f, v) in &values {
            names.push(quoted(f));
            marks.push(format!("?{}", params.len() + 1));
            params.push(v.clone());
        }
        for (column, default) in &c.local_defaults {
            names.push(quoted(column));
            match default {
                LocalDefault::Value(v) => {
                    marks.push(format!("?{}", params.len() + 1));
                    params.push(v.clone());
                }
                LocalDefault::Sql(sql) => marks.push(format!("({sql})")),
            }
        }
        let sql = if names.is_empty() {
            format!("INSERT INTO {} DEFAULT VALUES", quoted(&c.table))
        } else {
            format!(
                "INSERT INTO {} ({}) VALUES ({})",
                quoted(&c.table),
                names.join(", "),
                marks.join(", ")
            )
        };
        match db.exec(&sql, &params) {
            Ok(_) => {}
            Err(e) if refused(&e) => return Ok(false),
            Err(e) => return Err(e),
        }
        let report = rows.slots[i].report;
        let values = self.stored_row(db, idx, &key, values.clone(), &values)?;
        db.exec(
            "INSERT OR REPLACE INTO fongo_ids (collection, key, gid, local, image, doomed)
             VALUES (?1, ?2, ?3, ?4, ?5, 0)",
            &[
                Value::Text(c.name.clone()),
                Value::Text(key.clone()),
                Value::Ref(report),
                local,
                Value::Blob(image::encode(&values)),
            ],
        )?;
        db.exec(
            "DELETE FROM fongo_pins WHERE collection = ?1 AND key = ?2",
            &[Value::Text(c.name.clone()), Value::Text(key.clone())],
        )?;
        let s = &mut rows.slots[i];
        s.key = Some(key.clone());
        s.image = Some(values);
        rows.existing.insert((idx, key.clone()), i);
        cx.touch(idx, &key, report, false);
        Ok(true)
    }

    fn update(&self, cx: &mut Ctx, rows: &mut Rows, i: usize, fields: &Row) -> Result<bool> {
        self.named_by(cx, rows, i);
        let db = cx.db;
        let idx = rows.slots[i].idx;
        let c = self.collection(idx);
        let (Some(values), Some(key)) =
            (self.to_local(rows, idx, fields), rows.slots[i].key.clone())
        else {
            return Ok(false);
        };
        let mut image = rows.slots[i].image.clone().unwrap_or_default();
        image.extend(values.clone());
        let mut now = key.clone();
        if let Key::Natural { fields: key_fields } = &c.key
            && key_fields.iter().any(|f| values.contains_key(f))
        {
            let parts: Vec<Value> = key_fields
                .iter()
                .map(|f| image.get(f).cloned().unwrap_or(Value::Null))
                .collect();
            match tables::natural_key(db, &parts)? {
                Some(key) => now = key,
                None => return Ok(false),
            }
        }
        let mut params = vec![tables::key_param(c, &key)?];
        let mut sets = Vec::new();
        for (f, v) in &values {
            params.push(v.clone());
            sets.push(format!("{} = ?{}", quoted(f), params.len()));
        }
        let sql = format!(
            "UPDATE {} SET {} WHERE {}",
            quoted(&c.table),
            sets.join(", "),
            where_key(c)
        );
        match db.exec(&sql, &params) {
            Ok(0) => return Ok(false),
            Ok(_) => {}
            Err(e) if refused(&e) => return Ok(false),
            Err(e) => return Err(e),
        }
        if now != key {
            for table in ["fongo_ids", "fongo_pins"] {
                db.exec(
                    &format!("UPDATE {table} SET key = ?3 WHERE collection = ?1 AND key = ?2"),
                    &[
                        Value::Text(c.name.clone()),
                        Value::Text(key.clone()),
                        Value::Text(now.clone()),
                    ],
                )?;
            }
            rows.existing.insert((idx, now.clone()), i);
        }
        let image = self.stored_row(db, idx, &now, image, &values)?;
        self.write_image(db, &c.name, &now, &image)?;
        let s = &mut rows.slots[i];
        s.key = Some(now.clone());
        s.image = Some(image);
        let report = s.report;
        if now != key {
            cx.touch(idx, &key, report, true);
        }
        cx.touch(idx, &now, report, false);
        Ok(true)
    }

    fn delete(
        &self,
        cx: &mut Ctx,
        rows: &mut Rows,
        row_for: &BTreeMap<(usize, Gid), usize>,
        i: usize,
    ) -> Result<bool> {
        self.named_by(cx, rows, i);
        let db = cx.db;
        let idx = rows.slots[i].idx;
        let c = self.collection(idx);
        let Some(key) = rows.slots[i].key.clone() else {
            return Ok(true);
        };
        let kept = row_for
            .get(&(idx, rows.slots[i].report))
            .and_then(|&k| rows.slots[k].key.as_deref())
            .and_then(local_of);
        if let (true, Some(kept), Some(gone), Key::Minted { .. }) =
            (rows.slots[i].doomed, kept, local_of(&key), &c.key)
        {
            self.repoint(db, idx, gone, kept)?;
        }
        // A row still named by another waits with it: a table's ON DELETE action would otherwise
        // remove or change, behind the kernel, a row whose own write was refused.
        if let (Some(local), Key::Minted { .. }) = (local_of(&key), &c.key) {
            for (referrer, field) in self.referring(idx) {
                if !tables::rows_naming(db, self.collection(referrer), &field, local)?.is_empty() {
                    return Ok(false);
                }
            }
        }
        let sql = format!("DELETE FROM {} WHERE {}", quoted(&c.table), where_key(c));
        match db.exec(&sql, &[tables::key_param(c, &key)?]) {
            Ok(_) => {}
            Err(e) if refused(&e) => return Ok(false),
            Err(e) => return Err(e),
        }
        let at = [Value::Text(c.name.clone()), Value::Text(key.clone())];
        db.exec(
            "DELETE FROM fongo_ids WHERE collection = ?1 AND key = ?2",
            &at,
        )?;
        db.exec(
            "DELETE FROM fongo_pins WHERE collection = ?1 AND key = ?2",
            &at,
        )?;
        let s = &mut rows.slots[i];
        s.key = None;
        let report = s.report;
        cx.touch(idx, &key, report, true);
        Ok(true)
    }

    /// Before a row a merge gave up is removed, moves to the row kept the rows that name it and
    /// that the kernel does not map (written here and not yet collected), and the rows of tables
    /// outside the registry that the application declared, where their constraints allow.
    fn repoint(&self, db: &Db, idx: usize, gone: i64, kept: i64) -> Result<()> {
        for (referrer, field) in self.referring(idx) {
            let c = self.collection(referrer);
            db.exec(
                &format!(
                    "UPDATE OR IGNORE {table} SET {col} = ?1 WHERE {col} = ?2 AND NOT EXISTS (
                        SELECT 1 FROM fongo_ids WHERE collection = ?3 AND key = {key})",
                    table = quoted(&c.table),
                    col = quoted(&field),
                    key = tables::key_json(c, &format!("{}.", quoted(&c.table)))
                ),
                &[
                    Value::Int(kept),
                    Value::Int(gone),
                    Value::Text(c.name.clone()),
                ],
            )
            .or_else(|e| if refused(&e) { Ok(0) } else { Err(e) })?;
        }
        for (table, column) in &self.collection(idx).local_dependents {
            db.exec(
                &format!(
                    "UPDATE OR IGNORE {} SET {col} = ?1 WHERE {col} = ?2",
                    quoted(table),
                    col = quoted(column)
                ),
                &[Value::Int(kept), Value::Int(gone)],
            )
            .or_else(|e| if refused(&e) { Ok(0) } else { Err(e) })?;
        }
        Ok(())
    }
}

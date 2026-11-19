//! A replica with no database: the application's tables are maps, and every decision is asked of
//! the core. It is the executable design of a storage adapter.
//!
//! The adapter keeps, besides the merged facts: for every local row the gid it represents and its
//! image (the values last written to or read from it). `sync` first collects (compares rows with
//! their images and records the differences), then merges changes from elsewhere and brings
//! every row the core names to its target, in an order the core gives, refusing writes the
//! tables' constraints refuse and retrying those later.
//!
//! Three things an adapter must do that are about local rows rather than entities, each found by
//! the property this replica is tested with:
//! - Compare a row with its image in gid terms: the image through the row-to-gid mapping as it
//!   was, the row through the mapping as it will be. When a natural key changes, the row stands
//!   for a new entity, and rows that refer to it by local key now refer to that entity.
//! - Decide that a natural row's key changed by comparing key values, never by deriving a gid
//!   from the row: what the adapter wrote need not derive the entity's gid.
//! - When two local rows come to stand for one entity and one is deleted, rewrite the rows that
//!   refer to the deleted one, though their targets did not change.

use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_core::eval::{Evaluator, Target};
use fongo_sync_core::model::{ALIAS, FieldKind, Identity, Row, Schema, view};
use fongo_sync_core::plan::{Columns, Planned, RowAction, clashes, edits, order, record, write};
use fongo_sync_core::source::Store;
use fongo_sync_core::{Change, Clock, Gid, Value};

pub type Key = i64;

/// The identity a local row has in a plan: a bijection with local keys.
fn rid(k: Key) -> Gid {
    let mut bytes = [0xA0; 16];
    bytes[8..].copy_from_slice(&k.to_be_bytes());
    Gid(bytes)
}

fn key_of(g: Gid) -> Option<Key> {
    (g.0[..8] == [0xA0; 8]).then(|| Key::from_be_bytes(g.0[8..].try_into().unwrap()))
}

const MISSING: Key = -1;

fn same(a: &Value, b: &Value) -> bool {
    a.total_cmp(b) == std::cmp::Ordering::Equal
}

fn same_row(a: &Row, b: &Row) -> bool {
    a.len() == b.len()
        && a.iter()
            .zip(b)
            .all(|((fa, va), (fb, vb))| fa == fb && same(va, vb))
}

pub struct Replica<'s> {
    pub schema: &'s Schema,
    ev: Evaluator<'s>,
    pub id: u8,
    clock: Clock,
    pub wall: u64,
    pub store: Store,
    pub merged: Vec<Change>,
    pub outbox: Vec<Change>,
    pub tables: BTreeMap<String, BTreeMap<Key, Row>>,
    images: BTreeMap<(String, Key), Row>,
    gid_of: BTreeMap<(String, Key), Gid>,
    dirty: BTreeSet<(String, Key)>,
    pub held: BTreeSet<(String, Gid)>,
    parked: BTreeSet<(String, Key, String)>,
    next_key: Key,
    minted: u32,
    pub parked_steps: usize,
    pub refused: usize,
    not_null: BTreeSet<(String, String)>,
    /// Cells the application wrote since the adapter last wrote or recorded them.
    app_cells: BTreeSet<(String, Key, String)>,
    /// What each collect must have recorded, worked out from the rows alone.
    pub expected: Vec<Expected>,
}

/// A write the application made, as the collect that found it saw it: entity, field, value and
/// the clock the collect gave the row.
#[derive(Debug, Clone)]
pub struct Expected {
    pub collection: String,
    pub gid: Gid,
    pub field: String,
    pub value: Value,
    pub clock: fongo_sync_core::Hlc,
}

impl<'s> Replica<'s> {
    /// A replica whose tables refuse NULL in the `(collection, field)` columns of `not_null`.
    pub fn new(schema: &'s Schema, id: u8, not_null: &[(&str, &str)]) -> Replica<'s> {
        Replica {
            schema,
            ev: Evaluator::new(schema).unwrap(),
            id,
            clock: Clock::new(Gid([0xC0 | id; 16])),
            wall: 0,
            store: Store::default(),
            merged: Vec::new(),
            outbox: Vec::new(),
            tables: schema
                .collections
                .iter()
                .map(|c| (c.name.clone(), BTreeMap::new()))
                .collect(),
            images: BTreeMap::new(),
            gid_of: BTreeMap::new(),
            dirty: BTreeSet::new(),
            held: BTreeSet::new(),
            parked: BTreeSet::new(),
            next_key: 1,
            minted: 0,
            parked_steps: 0,
            refused: 0,
            not_null: not_null
                .iter()
                .map(|(c, f)| (c.to_string(), f.to_string()))
                .collect(),
            app_cells: BTreeSet::new(),
            expected: Vec::new(),
        }
    }

    pub fn is_clean(&self) -> bool {
        self.dirty.is_empty()
    }

    fn refs(&self, collection: &str) -> Vec<(String, String, bool)> {
        self.schema
            .collection(collection)
            .unwrap()
            .fields
            .iter()
            .filter_map(|f| match &f.kind {
                FieldKind::Reference(t) => Some((f.name.clone(), t.clone(), false)),
                FieldKind::Owner(t) => Some((f.name.clone(), t.clone(), true)),
                FieldKind::Plain => None,
            })
            .collect()
    }

    fn uniques(&self, collection: &str) -> Vec<Vec<String>> {
        let c = self.schema.collection(collection).unwrap();
        let mut sets = c.identity_keys.clone();
        if let Identity::Natural(key) = &c.identity {
            sets.push(key.clone());
        }
        sets
    }

    // --- The application's tables and their constraints ---------------------------------------

    /// Whether `row` may stand at `key` in `collection`: no NULL where the column refuses one,
    /// every reference names an existing row, and no other row shares a unique set with it.
    fn allowed(&self, collection: &str, key: Key, row: &Row) -> bool {
        let null = |f: &String| row.get(f).is_none_or(|v| *v == Value::Null);
        if self
            .not_null
            .iter()
            .any(|(c, f)| c == collection && null(f))
        {
            return false;
        }
        for (f, t, _) in self.refs(collection) {
            match row.get(&f) {
                None | Some(Value::Null) => {}
                Some(Value::Int(k)) if self.tables[&t].contains_key(k) => {}
                Some(_) => return false,
            }
        }
        for set in self.uniques(collection) {
            let tuple: Vec<&Value> = set
                .iter()
                .map(|f| row.get(f).unwrap_or(&Value::Null))
                .collect();
            if tuple.contains(&&Value::Null) {
                continue;
            }
            let clash = self.tables[collection].iter().any(|(k, other)| {
                *k != key
                    && set
                        .iter()
                        .zip(&tuple)
                        .all(|(f, v)| other.get(f).is_some_and(|w| same(w, v)))
            });
            if clash {
                return false;
            }
        }
        true
    }

    fn referred(&self, collection: &str, key: Key) -> bool {
        self.schema.collections.iter().any(|c| {
            self.refs(&c.name).iter().any(|(f, t, _)| {
                t == collection
                    && self.tables[&c.name]
                        .values()
                        .any(|r| r.get(f).is_some_and(|v| same(v, &Value::Int(key))))
            })
        })
    }

    pub fn keys(&self, collection: &str) -> Vec<Key> {
        self.tables[collection].keys().copied().collect()
    }

    /// The application inserts a row, NULL in every column it does not give; refused if a
    /// constraint refuses it.
    pub fn insert(&mut self, collection: &str, mut row: Row) -> Option<Key> {
        for f in &self.schema.collection(collection).unwrap().fields {
            row.entry(f.name.clone()).or_insert(Value::Null);
        }
        let key = self.next_key;
        if !self.allowed(collection, key, &row) {
            return None;
        }
        self.next_key += 1;
        for f in row.keys() {
            self.app_cells
                .insert((collection.to_string(), key, f.clone()));
        }
        self.tables.get_mut(collection).unwrap().insert(key, row);
        self.dirty.insert((collection.to_string(), key));
        Some(key)
    }

    /// The application sets one field of a row; refused if a constraint refuses it.
    pub fn edit(&mut self, collection: &str, key: Key, field: &str, value: Value) -> bool {
        let Some(mut row) = self.tables[collection].get(&key).cloned() else {
            return false;
        };
        row.insert(field.to_string(), value);
        if !self.allowed(collection, key, &row) {
            return false;
        }
        self.tables.get_mut(collection).unwrap().insert(key, row);
        self.dirty.insert((collection.to_string(), key));
        self.app_cells
            .insert((collection.to_string(), key, field.to_string()));
        for (c, k, f) in self.parked.clone() {
            if c == collection && k == key && f == field {
                self.parked.remove(&(c, k, f));
            }
        }
        true
    }

    /// The application deletes a row, the rows it owns, and so on down; references to any of them
    /// become NULL.
    pub fn delete(&mut self, collection: &str, key: Key) -> bool {
        if !self.tables[collection].contains_key(&key) {
            return false;
        }
        let mut doomed: BTreeSet<(String, Key)> = [(collection.to_string(), key)].into();
        loop {
            let mut more = Vec::new();
            for c in &self.schema.collections {
                for (f, t, owner) in self.refs(&c.name) {
                    if !owner {
                        continue;
                    }
                    for (k, r) in &self.tables[&c.name] {
                        if let Some(Value::Int(to)) = r.get(&f)
                            && doomed.contains(&(t.clone(), *to))
                            && !doomed.contains(&(c.name.clone(), *k))
                        {
                            more.push((c.name.clone(), *k));
                        }
                    }
                }
            }
            if more.is_empty() {
                break;
            }
            doomed.extend(more);
        }
        for c in &self.schema.collections {
            for (f, t, _) in self.refs(&c.name) {
                let nulled = self.tables[&c.name].iter().any(|(k, r)| {
                    !doomed.contains(&(c.name.clone(), *k))
                        && matches!(r.get(&f), Some(Value::Int(to)) if doomed.contains(&(t.clone(), *to)))
                });
                if nulled && self.not_null.contains(&(c.name.clone(), f.clone())) {
                    return false;
                }
            }
        }
        for c in self
            .schema
            .collections
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>()
        {
            for (f, t, _) in self.refs(&c) {
                let table = self.tables.get_mut(&c).unwrap();
                for (k, r) in table.iter_mut() {
                    if let Some(Value::Int(to)) = r.get(&f)
                        && doomed.contains(&(t.clone(), *to))
                    {
                        r.insert(f.clone(), Value::Null);
                        self.dirty.insert((c.clone(), *k));
                        self.app_cells.insert((c.clone(), *k, f.clone()));
                    }
                }
            }
        }
        for (c, k) in doomed {
            self.tables.get_mut(&c).unwrap().remove(&k);
            self.dirty.insert((c, k));
        }
        true
    }

    // --- Translation between local keys and gids ----------------------------------------------

    fn to_gids_in(&self, collection: &str, row: &Row, map: &BTreeMap<(String, Key), Gid>) -> Row {
        let mut out = row.clone();
        for (f, t, _) in self.refs(collection) {
            if let Some(Value::Int(k)) = row.get(&f) {
                let g = map.get(&(t.clone(), *k)).copied();
                out.insert(f, g.map_or(Value::Null, Value::Ref));
            }
        }
        out
    }

    fn to_rids(&self, collection: &str, row: &Row) -> Row {
        let mut out = row.clone();
        for (f, _, _) in self.refs(collection) {
            if let Some(Value::Int(k)) = row.get(&f) {
                out.insert(f, Value::Ref(rid(*k)));
            }
        }
        out
    }

    fn to_keys(&self, collection: &str, row: &Row) -> Row {
        let mut out = row.clone();
        for (f, _, _) in self.refs(collection) {
            if let Some(Value::Ref(g)) = row.get(&f) {
                out.insert(f, Value::Int(key_of(*g).unwrap_or(MISSING)));
            }
        }
        out
    }

    fn absorb(&mut self, changes: &[Change]) {
        for c in changes {
            self.store.merge(self.schema, c);
        }
        self.merged.extend(changes.iter().cloned());
    }

    // --- Collect ------------------------------------------------------------------------------

    /// Records every edit the application made since the last collect, merges the changes, and
    /// returns what they affect, for `apply` to bring up to date.
    ///
    /// Rows are compared with their images in gid terms: the image read through the mapping as it
    /// was, the row through the mapping as it will be. A row of a natural collection whose key
    /// changed represents a new entity, so the rows that refer to it now refer to that entity:
    /// they are compared too, and may themselves change entity.
    fn collect(&mut self) -> BTreeSet<(String, Gid)> {
        let mut dirty: BTreeSet<(String, Key)> = std::mem::take(&mut self.dirty);
        let was = self.gid_of.clone();
        let mut fresh: BTreeMap<(String, Key), Gid> = BTreeMap::new();
        let natural = |c: &str| -> Option<Vec<String>> {
            match &self.schema.collection(c).unwrap().identity {
                Identity::Natural(key) => Some(key.clone()),
                Identity::Minted => None,
            }
        };
        loop {
            let mut progress = false;
            let will: BTreeMap<(String, Key), Gid> = was
                .iter()
                .chain(&fresh)
                .map(|(k, g)| (k.clone(), *g))
                .collect();
            for (c, k) in dirty.clone() {
                let at = (c.clone(), k);
                let Some(row) = self.tables[&c].get(&k) else {
                    continue;
                };
                let image = self.images.get(&at);
                let gid = match natural(&c) {
                    None if image.is_none() && !fresh.contains_key(&at) => {
                        self.minted += 1;
                        let mut bytes = [0xF0; 16];
                        bytes[1] = self.id;
                        bytes[12..].copy_from_slice(&self.minted.to_be_bytes());
                        Gid(bytes)
                    }
                    None => continue,
                    Some(key) => {
                        let waits = self.refs(&c).iter().any(|(f, t, _)| {
                            key.contains(f)
                                && matches!(row.get(f), Some(Value::Int(to))
                                    if !will.contains_key(&(t.clone(), *to)))
                        });
                        if waits {
                            continue;
                        }
                        let now = self.to_gids_in(&c, row, &will);
                        let values: Vec<Value> = key
                            .iter()
                            .map(|f| now.get(f).cloned().unwrap_or(Value::Null))
                            .collect();
                        // What the adapter wrote need not derive the gid (a reference in the key
                        // reads as NULL once its entity is gone), so only a changed key makes a
                        // new entity.
                        if let Some(image) = image {
                            let then = self.to_gids_in(&c, image, &was);
                            let same_key = key.iter().all(|f| {
                                same(
                                    then.get(f).unwrap_or(&Value::Null),
                                    now.get(f).unwrap_or(&Value::Null),
                                )
                            });
                            if same_key {
                                // An earlier pass may have read a key through a row that has
                                // since kept its entity.
                                progress |= fresh.remove(&at).is_some();
                                continue;
                            }
                        }
                        Gid::derive(&c, &values)
                    }
                };
                if fresh.get(&at) == Some(&gid) {
                    continue;
                }
                fresh.insert(at.clone(), gid);
                progress = true;
                if image.is_some() {
                    for other in &self.schema.collections {
                        for (f, t, _) in self.refs(&other.name) {
                            if t != c {
                                continue;
                            }
                            for (j, r) in &self.tables[&other.name] {
                                if r.get(&f).is_some_and(|v| same(v, &Value::Int(k))) {
                                    dirty.insert((other.name.clone(), *j));
                                }
                            }
                        }
                    }
                }
            }
            if !progress {
                break;
            }
        }
        let will: BTreeMap<(String, Key), Gid> = was
            .iter()
            .chain(&fresh)
            .map(|(k, g)| (k.clone(), *g))
            .collect();

        let mut changes = Vec::new();
        let mut touched = BTreeSet::new();
        {
            let mut session = self.ev.session(&self.store);
            for (c, k) in &dirty {
                let at = (c.clone(), *k);
                let image = self.images.get(&at).cloned();
                let row = self.tables[c].get(k).cloned();
                let before = image.as_ref().map(|i| self.to_gids_in(c, i, &was));
                let after = row.as_ref().map(|r| self.to_gids_in(c, r, &will));
                let found = edits(self.schema, c, before.as_ref(), after.as_ref());
                let old_gid = image.as_ref().and_then(|_| was.get(&at).copied());
                let new = fresh.get(&at).copied();
                let writes = self.writes(c, before.as_ref(), after.as_ref(), old_gid, new);
                if found.is_empty() && writes.is_empty() {
                    if let Some(r) = row {
                        self.images.insert(at, r);
                    }
                    continue;
                }
                let old: Option<Target> = old_gid.map(|g| session.target(c, g).unwrap());
                let clock = self.clock.tick(self.wall);
                self.expected
                    .extend(writes.into_iter().map(|(gid, field, value)| Expected {
                        collection: c.clone(),
                        gid,
                        field,
                        value,
                        clock,
                    }));
                let recorded = record(self.schema, c, &found, old.as_ref(), new, clock);
                if std::env::var_os("FONGO_SIM_TRACE").is_some() {
                    eprintln!(
                        "replica {} collects {c} row {k}: {before:?} -> {after:?}; old {old:?}; records {recorded:?}",
                        self.id
                    );
                }
                let created = found.contains(&fongo_sync_core::plan::FieldEdit::Created);
                let coll = self.schema.collection(c).unwrap();
                let written_by_app =
                    |f: &String| self.app_cells.contains(&(c.clone(), *k, f.clone()));
                for ch in &recorded {
                    if ch.field == fongo_sync_core::DELETED {
                        continue;
                    }
                    let shown = after.as_ref().and_then(|r| r.get(&ch.field));
                    assert!(
                        shown.is_some_and(|v| same(v, &ch.value)),
                        "replica {} recorded {ch:?}, which its row {after:?} does not hold",
                        self.id
                    );
                    assert!(
                        !self.parked.contains(&(c.clone(), *k, ch.field.clone())),
                        "replica {} recorded a parked value: {ch:?}",
                        self.id
                    );
                    // A value the application did not write since the adapter last wrote the
                    // cell may be sent only as part of a new entity, beside a field of its
                    // `together` set that the application did write, or as a reference whose
                    // local row now stands for another entity.
                    let sibling = coll
                        .together
                        .iter()
                        .any(|set| set.contains(&ch.field) && set.iter().any(written_by_app));
                    let follows = self.refs(c).iter().any(|(f, _, _)| *f == ch.field)
                        && image.as_ref().zip(row.as_ref()).is_some_and(|(i, r)| {
                            i.get(&ch.field)
                                .is_some_and(|a| r.get(&ch.field).is_some_and(|b| same(a, b)))
                        });
                    assert!(
                        created || sibling || follows || written_by_app(&ch.field),
                        "replica {} recorded {ch:?}, which its application did not write",
                        self.id
                    );
                }
                self.app_cells.retain(|(cc, kk, _)| !(cc == c && kk == k));
                changes.extend(recorded);
                if let Some(o) = old_gid {
                    touched.insert((c.clone(), o));
                }
                match row {
                    Some(r) => {
                        self.images.insert(at.clone(), r);
                        if let Some(n) = new {
                            self.gid_of.insert(at, n);
                            touched.insert((c.clone(), n));
                        }
                    }
                    None => {
                        self.images.remove(&at);
                        self.gid_of.remove(&at);
                    }
                }
            }
        }
        let mut affected = self.ev.affected(&self.store, &changes).unwrap();
        affected.extend(touched);
        self.absorb(&changes);
        self.outbox.extend(changes);
        affected
    }

    /// What the application wrote to one row since the last collect, worked out without the
    /// planner: a deleted row deletes its entity; a new row, or a natural row whose key changed,
    /// creates an entity with its key and every non-NULL field (deleting the old one); otherwise
    /// each changed field is written. A `together` set any of whose fields is written is written
    /// whole, as the row holds it.
    fn writes(
        &self,
        collection: &str,
        before: Option<&Row>,
        after: Option<&Row>,
        old: Option<Gid>,
        new: Option<Gid>,
    ) -> Vec<(Gid, String, Value)> {
        let coll = self.schema.collection(collection).unwrap();
        let key: Vec<String> = match &coll.identity {
            Identity::Natural(k) => k.clone(),
            Identity::Minted => Vec::new(),
        };
        let get = |r: &Row, f: &str| r.get(f).cloned().unwrap_or(Value::Null);
        let whole = |chosen: BTreeSet<String>| -> BTreeSet<String> {
            let mut all = chosen.clone();
            for set in &coll.together {
                if set.iter().any(|f| chosen.contains(f)) {
                    all.extend(set.iter().cloned());
                }
            }
            all
        };
        let deleted = |g: Gid, v: i64| (g, fongo_sync_core::DELETED.to_string(), Value::Int(v));
        let mut out = Vec::new();
        match (before, after, new) {
            (Some(_), None, _) => out.extend(old.map(|o| deleted(o, 1))),
            (_, Some(a), Some(n)) => {
                if before.is_some() {
                    out.extend(old.map(|o| deleted(o, 1)));
                }
                out.push(deleted(n, 0));
                let chosen = coll
                    .fields
                    .iter()
                    .map(|f| f.name.clone())
                    .filter(|f| key.contains(f) || get(a, f) != Value::Null)
                    .collect();
                for f in whole(chosen) {
                    out.push((n, f.clone(), get(a, &f)));
                }
            }
            (Some(b), Some(a), None) => {
                let Some(o) = old else { return out };
                let chosen = coll
                    .fields
                    .iter()
                    .map(|f| f.name.clone())
                    .filter(|f| !same(&get(b, f), &get(a, f)))
                    .collect();
                for f in whole(chosen) {
                    out.push((o, f.clone(), get(a, &f)));
                }
            }
            _ => {}
        }
        out
    }

    // --- Apply --------------------------------------------------------------------------------

    /// Collects local edits, merges `incoming`, and brings every affected row to its target.
    pub fn sync(&mut self, incoming: &[Change]) {
        let mut set = self.collect();
        for c in incoming {
            self.clock.observe(&c.clock);
        }
        set.extend(self.ev.affected(&self.store, incoming).unwrap());
        self.absorb(incoming);
        self.reconcile(set);
    }

    fn reconcile(&mut self, mut set: BTreeSet<(String, Gid)>) {
        set.extend(std::mem::take(&mut self.held));
        loop {
            let mut pairs = BTreeSet::new();
            {
                let mut session = self.ev.session(&self.store);
                let mut seen = BTreeSet::new();
                for (c, g) in &set {
                    let t = session.target(c, *g).unwrap();
                    if t.row.is_none() || !seen.insert(t.canonical) {
                        continue;
                    }
                    for other in clashes(&mut session, &t).unwrap() {
                        pairs.insert((c.clone(), t.canonical.min(other), t.canonical.max(other)));
                    }
                }
            }
            if pairs.is_empty() {
                break;
            }
            let aliases: Vec<Change> = pairs
                .into_iter()
                .map(|(c, a, b)| Change {
                    collection: c,
                    gid: b,
                    field: ALIAS.into(),
                    value: Value::Ref(a),
                    clock: self.clock.tick(self.wall),
                })
                .collect();
            set.extend(self.ev.affected(&self.store, &aliases).unwrap());
            self.absorb(&aliases);
            self.outbox.extend(aliases);
        }

        let mut rows_of: BTreeMap<(String, Gid), Vec<Key>> = BTreeMap::new();
        for ((c, k), g) in &self.gid_of {
            rows_of.entry((c.clone(), *g)).or_default().push(*k);
        }
        type Wanted = BTreeMap<(String, Gid), (Option<Row>, BTreeSet<Key>)>;
        let (wanted, doomed, keeps) = loop {
            let mut wanted: Wanted = BTreeMap::new();
            let mut doomed: BTreeMap<(String, Key), (String, Gid)> = BTreeMap::new();
            let mut session = self.ev.session(&self.store);
            for (c, g) in &set {
                let t = session.target(c, *g).unwrap();
                let mine = rows_of.get(&(c.clone(), *g)).cloned().unwrap_or_default();
                if t.members.is_empty() {
                    for k in mine {
                        doomed.insert((c.clone(), k), (c.clone(), *g));
                    }
                    continue;
                }
                let entry = wanted.entry((c.clone(), t.canonical)).or_default();
                entry.0 = t.row.clone();
                entry.1.extend(mine);
                entry
                    .1
                    .extend(rows_of.get(&(c.clone(), t.canonical)).into_iter().flatten());
            }
            let mut keeps: BTreeMap<(String, Gid), Option<Key>> = BTreeMap::new();
            for ((c, canon), (row, keys)) in &wanted {
                if row.is_none() {
                    for k in keys {
                        let was = self.gid_of[&(c.clone(), *k)];
                        doomed.insert((c.clone(), *k), (c.clone(), was));
                    }
                    continue;
                }
                let keep = keys
                    .iter()
                    .copied()
                    .find(|k| self.gid_of.get(&(c.clone(), *k)) == Some(canon))
                    .or_else(|| keys.iter().copied().next());
                for k in keys {
                    if Some(*k) != keep {
                        let was = self.gid_of[&(c.clone(), *k)];
                        doomed.insert((c.clone(), *k), (c.clone(), was));
                    }
                }
                keeps.insert((c.clone(), *canon), keep);
            }
            // A row about to be deleted may still be referred to by rows whose targets did not
            // change; they must be rewritten to refer to the row that now stands for the entity.
            let mut more = Vec::new();
            for (dc, dk) in doomed.keys() {
                for other in &self.schema.collections {
                    for (f, t, _) in self.refs(&other.name) {
                        if &t != dc {
                            continue;
                        }
                        for (j, r) in &self.tables[&other.name] {
                            if r.get(&f).is_some_and(|v| same(v, &Value::Int(*dk)))
                                && let Some(g) = self.gid_of.get(&(other.name.clone(), *j))
                                && !set.contains(&(other.name.clone(), *g))
                            {
                                more.push((other.name.clone(), *g));
                            }
                        }
                    }
                }
            }
            if more.is_empty() {
                break (wanted, doomed, keeps);
            }
            set.extend(more);
        };
        let mut row_for: BTreeMap<(String, Gid), Key> = BTreeMap::new();
        let mut owner_of: BTreeMap<(String, Key), (String, Gid)> = BTreeMap::new();
        let mut inserting = BTreeSet::new();
        for ((c, canon), keep) in &keeps {
            let keep = keep.unwrap_or_else(|| {
                let k = self.next_key;
                self.next_key += 1;
                inserting.insert((c.clone(), k));
                k
            });
            self.gid_of.insert((c.clone(), keep), *canon);
            row_for.insert((c.clone(), *canon), keep);
            owner_of.insert((c.clone(), keep), (c.clone(), *canon));
        }
        for ((c, k), g) in &self.gid_of {
            if !doomed.contains_key(&(c.clone(), *k)) {
                row_for.entry((c.clone(), *g)).or_insert(*k);
            }
        }
        let to_rids_target = |c: &str, row: &Row| -> Row {
            let mut out = row.clone();
            for (f, t, _) in self.refs(c) {
                if let Some(Value::Ref(g)) = row.get(&f) {
                    let k = row_for.get(&(t.clone(), *g)).copied().unwrap_or(MISSING);
                    out.insert(f, Value::Ref(rid(k)));
                }
            }
            out
        };
        let mut planned = Vec::new();
        for ((c, canon), (row, _)) in &wanted {
            let Some(row) = row else { continue };
            let k = row_for[&(c.clone(), *canon)];
            let image = self.images.get(&(c.clone(), k)).map(|i| self.to_rids(c, i));
            planned.push(Planned {
                collection: c.clone(),
                gid: rid(k),
                image: image.clone(),
                action: write(Some(&to_rids_target(c, row)), image.as_ref()),
            });
        }
        for ((c, k), entity) in &doomed {
            owner_of.insert((c.clone(), *k), entity.clone());
            let image = self
                .images
                .get(&(c.clone(), *k))
                .map(|i| self.to_rids(c, i));
            planned.push(Planned {
                collection: c.clone(),
                gid: rid(*k),
                image,
                action: RowAction::Delete,
            });
        }

        let columns = self
            .not_null
            .iter()
            .fold(Columns::new(), |cols, (c, f)| cols.not_null(c, f));
        let plan = order(self.schema, &columns, &planned);
        let mut failed: BTreeSet<(String, Key)> = BTreeSet::new();
        // A stuck row gets no final write; its parking step, if any, others rely on.
        for (c, g) in &plan.stuck {
            let at = (c.clone(), key_of(*g).unwrap());
            self.held.insert(owner_of[&at].clone());
            let parked = plan
                .steps
                .iter()
                .any(|s| s.parked && s.collection == *c && s.gid == *g);
            if inserting.contains(&at) && !parked {
                self.gid_of.remove(&at);
            }
        }
        for step in plan.steps {
            let k = key_of(step.gid).unwrap();
            let at = (step.collection.clone(), k);
            if failed.contains(&at) {
                continue;
            }
            let c = step.collection.as_str();
            let ok = match &step.action {
                RowAction::Nothing => true,
                RowAction::Insert(row) => {
                    let row = self.to_keys(c, row);
                    let ok = !self.tables[c].contains_key(&k) && self.allowed(c, k, &row);
                    if ok {
                        self.tables.get_mut(c).unwrap().insert(k, row.clone());
                        self.images.insert(at.clone(), row);
                    }
                    ok
                }
                RowAction::Update(fields) => {
                    let fields = self.to_keys(c, fields);
                    match self.tables[c].get(&k).cloned() {
                        Some(mut row) => {
                            row.extend(fields.clone());
                            let ok = self.allowed(c, k, &row);
                            if ok {
                                self.tables.get_mut(c).unwrap().insert(k, row.clone());
                                self.images.insert(at.clone(), row);
                            }
                            ok
                        }
                        None => false,
                    }
                }
                RowAction::Delete => {
                    let ok = self.tables[c].contains_key(&k) && !self.referred(c, k);
                    if ok {
                        self.tables.get_mut(c).unwrap().remove(&k);
                        self.images.remove(&at);
                        self.gid_of.remove(&at);
                    }
                    ok
                }
            };
            if step.parked {
                self.parked_steps += 1;
            }
            if !ok {
                self.refused += 1;
            }
            if std::env::var_os("FONGO_SIM_TRACE").is_some() {
                let table = &self.tables[&step.collection];
                eprintln!(
                    "replica {} {} {step:?}; table {table:?}",
                    self.id,
                    if ok { "wrote" } else { "refused" }
                );
            }
            if ok {
                let written: Vec<String> = match &step.action {
                    RowAction::Insert(r) | RowAction::Update(r) => r.keys().cloned().collect(),
                    _ => Vec::new(),
                };
                for f in written {
                    let cell = (c.to_string(), k, f.clone());
                    let nulled = match &step.action {
                        RowAction::Insert(r) | RowAction::Update(r) => r[&f] == Value::Null,
                        _ => false,
                    };
                    if step.parked && nulled {
                        self.parked.insert(cell);
                    } else {
                        self.parked.remove(&cell);
                    }
                }
            } else {
                failed.insert(at.clone());
                self.held.insert(owner_of[&at].clone());
                if inserting.contains(&at) && !self.tables[c].contains_key(&k) {
                    self.gid_of.remove(&at);
                }
            }
        }
    }

    // --- Checks -------------------------------------------------------------------------------

    /// The application's rows, read as entities: by gid, references as gids.
    pub fn shown(&self) -> BTreeMap<(String, Gid), Row> {
        let mut out = BTreeMap::new();
        for (c, table) in &self.tables {
            for (k, row) in table {
                let g = self
                    .gid_of
                    .get(&(c.clone(), *k))
                    .copied()
                    .unwrap_or(Gid([0xEE; 16]));
                out.insert((c.clone(), g), self.to_gids_in(c, row, &self.gid_of));
            }
        }
        out
    }

    /// Whether every entity still held is one these tables cannot hold: its row has NULL where a
    /// column refuses it, refers to an entity with no row here, or takes a unique value a row of
    /// a held entity keeps; or a row that stood for it, and must go, is still referred to by the
    /// row of an entity that is held.
    pub fn waits_for_a_reason(&self) -> Result<(), String> {
        let mut session = self.ev.session(&self.store);
        let has_row = |c: &str, g: Gid| {
            self.gid_of
                .iter()
                .any(|((cc, k), gg)| cc == c && *gg == g && self.tables[c].contains_key(k))
        };
        for (c, g) in &self.held {
            let t = session.target(c, *g).unwrap();
            let unwritable = match &t.row {
                Some(row) => {
                    let null = self
                        .not_null
                        .iter()
                        .any(|(cc, f)| cc == c && row.get(f).is_none_or(|v| *v == Value::Null));
                    let missing = self.refs(c).iter().any(
                        |(f, t, _)| matches!(row.get(f), Some(Value::Ref(h)) if !has_row(t, *h)),
                    );
                    let blocked = self.uniques(c).iter().any(|set| {
                        let tuple: Vec<&Value> = set
                            .iter()
                            .map(|f| row.get(f).unwrap_or(&Value::Null))
                            .collect();
                        !tuple.contains(&&Value::Null)
                            && self.tables[c].iter().any(|(k, other)| {
                                let other = self.to_gids_in(c, other, &self.gid_of);
                                let mine = self.gid_of.get(&(c.clone(), *k));
                                mine.is_some_and(|h| {
                                    *h != t.canonical && self.held.contains(&(c.clone(), *h))
                                }) && set
                                    .iter()
                                    .zip(&tuple)
                                    .all(|(f, v)| other.get(f).is_some_and(|w| same(w, v)))
                            })
                    });
                    null || missing || blocked
                }
                None => false,
            };
            let pinned = self.gid_of.iter().any(|((cc, k), gg)| {
                cc == c
                    && gg == g
                    && self.schema.collections.iter().any(|other| {
                        self.refs(&other.name).iter().any(|(f, t, _)| {
                            t == c
                                && self.tables[&other.name].iter().any(|(j, r)| {
                                    r.get(f).is_some_and(|v| same(v, &Value::Int(*k)))
                                        && self.gid_of.get(&(other.name.clone(), *j)).is_some_and(
                                            |h| self.held.contains(&(other.name.clone(), *h)),
                                        )
                                })
                        })
                    })
            });
            let why = unwritable || pinned;
            if !why {
                return Err(format!(
                    "replica {} holds {c} {g:?} for no reason its tables give: target {:?}",
                    self.id, t.row
                ));
            }
        }
        Ok(())
    }

    /// Every row the replica holds equals what `model::view` of the changes it merged shows,
    /// except rows of entities whose writes the tables refused, which wait.
    pub fn check(&self) -> Result<(), String> {
        let expected = view(self.schema, &self.merged);
        let mut seen = BTreeSet::new();
        for (c, table) in &self.tables {
            for (k, row) in table {
                let Some(g) = self.gid_of.get(&(c.clone(), *k)).copied() else {
                    return Err(format!("replica {}: {c} row {k} has no gid", self.id));
                };
                if !seen.insert((c.clone(), g)) && !self.held.contains(&(c.clone(), g)) {
                    return Err(format!("replica {}: two rows for {c} {g:?}", self.id));
                }
                if self.held.contains(&(c.clone(), g)) {
                    continue;
                }
                let shown = self.to_gids_in(c, row, &self.gid_of);
                match expected.row(c, g) {
                    Some(e) if same_row(e, &shown) => {}
                    other => {
                        return Err(format!(
                            "replica {}: {c} {g:?} shows {shown:?}, the model {other:?}",
                            self.id
                        ));
                    }
                }
            }
        }
        for (c, rows) in &expected.collections {
            for g in rows.keys() {
                if !seen.contains(&(c.clone(), *g)) && !self.held.contains(&(c.clone(), *g)) {
                    return Err(format!("replica {}: no row for {c} {g:?}", self.id));
                }
            }
        }
        Ok(())
    }
}

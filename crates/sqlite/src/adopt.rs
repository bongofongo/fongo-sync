//! A first install in steps: the rows a collection's table holds when its install begins are
//! recorded, then adopted from the records in batches. See [`crate::store::TABLES`].

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use fongo_sync_core::Gid;
use fongo_sync_core::Value;

use crate::collect::{Edits, Found};
use crate::db::{Db, as_gid, as_int, as_text};
use crate::error::Result;
use crate::image;
use crate::registry::Key;
use crate::replica::{Ctx, Replica, meta_get, meta_set};
use crate::tables::{every_row, minted_key, read_row};

/// Whether the records' table is there: a database of an earlier layout gains it at its next
/// install, and until then holds none.
pub(crate) fn laid(db: &Db) -> Result<bool> {
    Ok(!db
        .query(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'fongo_found'",
            &[],
        )?
        .is_empty())
}

/// Whether found rows wait to be adopted from their records.
pub(crate) fn recorded(db: &Db) -> Result<bool> {
    if !laid(db)? {
        return Ok(false);
    }
    Ok(!db
        .query("SELECT 1 FROM fongo_found WHERE batch >= 0 LIMIT 1", &[])?
        .is_empty())
}

/// How many found rows wait to be adopted, counting at most `at_most`.
pub(crate) fn recorded_rows(db: &Db, at_most: i64) -> Result<usize> {
    if !laid(db)? {
        return Ok(0);
    }
    let rows = db.query(
        "SELECT count(*) FROM (SELECT 1 FROM fongo_found WHERE batch >= 0 LIMIT ?1)",
        &[Value::Int(at_most)],
    )?;
    Ok(rows
        .first()
        .map(|r| as_int(&r[0]))
        .transpose()?
        .and_then(|n| usize::try_from(n).ok())
        .unwrap_or(0))
}

/// Disjoint sets of found rows, by their place in the order a whole install takes them.
struct Sets(Vec<usize>);

impl Sets {
    fn find(&mut self, mut i: usize) -> usize {
        while self.0[i] != i {
            self.0[i] = self.0[self.0[i]];
            i = self.0[i];
        }
        i
    }

    /// Joins the sets of `a` and `b`, keeping the earlier place as the set's own.
    fn join(&mut self, a: usize, b: usize) -> bool {
        let (a, b) = (self.find(a), self.find(b));
        if a == b {
            return false;
        }
        let (keep, other) = (a.min(b), a.max(b));
        self.0[other] = keep;
        true
    }
}

impl Replica {
    /// Records the rows the tables of collections `idxs` hold, as a whole install would adopt
    /// them: in the order it takes them, each minted row with the gid it would mint and each
    /// with the counter of the adoption clock it would give, and in batches.
    pub(crate) fn record_found(&self, db: &Db, idxs: &[usize]) -> Result<()> {
        // A collection that already holds entities from elsewhere may find its rows to be one
        // with them, which stamps aliases at the adopting step's time: its rows, and with them
        // every row found with them, are marked to be adopted whole, as a whole install does.
        for &idx in idxs {
            let c = self.collection(idx);
            if !db
                .query(
                    "SELECT 1 FROM fongo_claims WHERE collection = ?1 LIMIT 1",
                    &[Value::Text(c.name.clone())],
                )?
                .is_empty()
            {
                for &idx in idxs {
                    let c = self.collection(idx);
                    db.exec(
                        &format!(
                            "INSERT OR IGNORE INTO fongo_dirty (collection, key, at)
                             SELECT ?1, {}, 0 FROM {}",
                            crate::tables::key_json(c, ""),
                            crate::db::quoted(&c.table)
                        ),
                        &[Value::Text(c.name.clone())],
                    )?;
                }
                return Ok(());
            }
        }
        let mut order: Vec<usize> = idxs.to_vec();
        order.sort_by(|a, b| self.collection(*a).name.cmp(&self.collection(*b).name));
        let mut found: Vec<(usize, String, fongo_sync_core::model::Row)> = Vec::new();
        for idx in order {
            let mut rows = every_row(db, self.collection(idx))?;
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            found.extend(rows.into_iter().map(|(key, row)| (idx, key, row)));
        }
        if found.is_empty() {
            return Ok(());
        }
        let minted: Vec<usize> = (0..found.len())
            .filter(|&i| matches!(self.collection(found[i].0).key, Key::Minted { .. }))
            .collect();
        let mut gids: Vec<Option<Gid>> = vec![None; found.len()];
        if !minted.is_empty() {
            let fresh = db.query(
                "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < ?1)
                 SELECT randomblob(16) FROM n",
                &[Value::Int(i64::try_from(minted.len()).unwrap_or(i64::MAX))],
            )?;
            for (&i, r) in minted.iter().zip(&fresh) {
                gids[i] = Some(as_gid(&r[0])?);
            }
        }
        let base = match meta_get(db, "adopted_seq")? {
            Some(Value::Int(seq)) => u32::try_from(seq).unwrap_or(u32::MAX),
            _ => 0,
        };
        let count = u32::try_from(found.len()).unwrap_or(u32::MAX);
        meta_set(
            db,
            "adopted_seq",
            Value::Int(i64::from(base.saturating_add(count))),
        )?;
        let batches = self.batches(db, &found)?;
        meta_set(db, "found_registry", Value::Text(self.registry.encode()))?;
        for (i, (idx, key, row)) in found.iter().enumerate() {
            let seq = base.saturating_add(u32::try_from(i + 1).unwrap_or(u32::MAX));
            db.exec(
                "INSERT INTO fongo_found (collection, key, batch, gid, seq, image)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                &[
                    Value::Text(self.collection(*idx).name.clone()),
                    Value::Text(key.clone()),
                    Value::Int(i64::try_from(batches[i]).unwrap_or(i64::MAX)),
                    gids[i].map_or(Value::Null, Value::Ref),
                    Value::Int(i64::from(seq)),
                    Value::Blob(image::encode(row)),
                ],
            )?;
        }
        Ok(())
    }

    /// The batch of each found row: rows that collecting could find to be one entity, or whose
    /// references among found rows form a cycle, share one; and batches are numbered so that a
    /// row's batch comes after the batches of the found rows it names.
    fn batches(
        &self,
        db: &Db,
        found: &[(usize, String, fongo_sync_core::model::Row)],
    ) -> Result<Vec<usize>> {
        let place: BTreeMap<(usize, &str), usize> = found
            .iter()
            .enumerate()
            .map(|(i, (idx, key, _))| ((*idx, key.as_str()), i))
            .collect();
        // The found rows each row names.
        let names: Vec<Vec<usize>> = found
            .iter()
            .map(|(idx, _, row)| {
                let c = self.collection(*idx);
                c.fields
                    .iter()
                    .filter_map(|f| match (self.target_of(c, &f.name), row.get(&f.name)) {
                        (Some(t), Some(Value::Int(local))) => {
                            place.get(&(t, minted_key(*local).as_str())).copied()
                        }
                        _ => None,
                    })
                    .collect()
            })
            .collect();
        let mut sets = Sets((0..found.len()).collect());
        // Rows whose identities, or natural keys, are equal once references are read as the
        // sets they name could be found to be one.
        loop {
            let mut joined = false;
            let mut seen: BTreeMap<String, usize> = BTreeMap::new();
            for (i, (idx, _, row)) in found.iter().enumerate() {
                let c = self.collection(*idx);
                let mut keys: Vec<&Vec<String>> = c.identity.iter().collect();
                let natural;
                if let Key::Natural { fields } = &c.key {
                    natural = fields.clone();
                    keys.push(&natural);
                }
                for (n, fields) in keys.iter().enumerate() {
                    // A natural key holds NULL, and a reference to no found row reads as NULL;
                    // an identity with either matches none.
                    let natural = n == c.identity.len();
                    let null = || natural.then(|| "null".to_string());
                    let mut parts = Vec::new();
                    for f in fields.iter() {
                        let value = row.get(f).cloned().unwrap_or(Value::Null);
                        let part = match (self.target_of(c, f), &value) {
                            (_, Value::Null) => null(),
                            (Some(t), Value::Int(local)) => {
                                let key = minted_key(*local);
                                match place.get(&(t, key.as_str())) {
                                    Some(&p) => Some(format!("set {}", sets.find(p))),
                                    None if read_row(db, self.collection(t), &key)?.is_some() => {
                                        Some(format!("{value:?}"))
                                    }
                                    None => null(),
                                }
                            }
                            _ => Some(format!("{value:?}")),
                        };
                        match part {
                            Some(part) => parts.push(part),
                            None => {
                                parts.clear();
                                break;
                            }
                        }
                    }
                    if parts.is_empty() {
                        continue;
                    }
                    let id = format!("{} {n} {}", c.name, parts.join(" "));
                    match seen.get(&id) {
                        Some(&other) => joined |= sets.join(i, other),
                        None => {
                            seen.insert(id, i);
                        }
                    }
                }
            }
            if !joined {
                break;
            }
        }
        // Sets on a cycle of references are joined, so the sets' graph has none: each strongly
        // connected component of it becomes one set.
        let roots: Vec<usize> = (0..found.len()).map(|i| sets.find(i)).collect();
        let mut after: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
        for (i, named) in names.iter().enumerate() {
            for &n in named {
                if roots[n] != roots[i] {
                    after.entry(roots[i]).or_default().insert(roots[n]);
                }
            }
        }
        for component in components(&after) {
            for pair in component.windows(2) {
                sets.join(pair[0], pair[1]);
            }
        }
        let roots: Vec<usize> = (0..found.len()).map(|i| sets.find(i)).collect();
        let mut after: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
        for (i, named) in names.iter().enumerate() {
            for &n in named {
                if roots[n] != roots[i] {
                    after.entry(roots[i]).or_default().insert(roots[n]);
                }
            }
        }
        // Sets in an order where each comes after those it names, earliest place first.
        let mut waiting: BTreeMap<usize, usize> = BTreeMap::new();
        let mut before: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for (&set, named) in &after {
            waiting.insert(set, named.len());
            for &n in named {
                before.entry(n).or_default().push(set);
            }
        }
        let all: BTreeSet<usize> = roots.iter().copied().collect();
        let mut ready: BinaryHeap<Reverse<usize>> = all
            .iter()
            .filter(|s| !waiting.contains_key(s))
            .map(|&s| Reverse(s))
            .collect();
        let mut number: BTreeMap<usize, usize> = BTreeMap::new();
        while let Some(Reverse(set)) = ready.pop() {
            number.insert(set, number.len());
            for &next in before.get(&set).map(Vec::as_slice).unwrap_or(&[]) {
                let left = waiting.get_mut(&next).expect("counted");
                *left -= 1;
                if *left == 0 {
                    ready.push(Reverse(next));
                }
            }
        }
        Ok(roots.iter().map(|r| number[r]).collect())
    }

    /// Adopts every waiting record under the registry it was recorded with, where this
    /// replica's differs, as installs of the two registries one after the other would: what the
    /// new registry adds is then adopted of the rows as they stand, as of any row adopted before.
    /// Returns how many it adopted.
    pub(crate) fn adopt_as_recorded(&self, cx: &mut Ctx) -> Result<usize> {
        let db = cx.db;
        if !recorded(db)? {
            return Ok(0);
        }
        let Some(Value::Text(text)) = meta_get(db, "found_registry")? else {
            return Ok(0);
        };
        if text == self.registry.encode() {
            return Ok(0);
        }
        Replica::new(crate::registry::Registry::decode(&text, &self.registry)?).adopt(cx, None)
    }

    /// Adopts found rows from their records: the next batches, until at least `want` rows, or
    /// every one with `None`; and brings the rows they affect up to date. Returns how many it
    /// adopted.
    pub(crate) fn adopt(&self, cx: &mut Ctx, want: Option<usize>) -> Result<usize> {
        let db = cx.db;
        let mut rows = match want {
            None => db.query(
                "SELECT collection, key, batch, gid, seq, image FROM fongo_found
                  WHERE batch >= 0 ORDER BY batch, collection, key",
                &[],
            )?,
            Some(want) => db.query(
                "SELECT collection, key, batch, gid, seq, image FROM fongo_found
                  WHERE batch >= 0 ORDER BY batch, collection, key LIMIT ?1",
                &[Value::Int(i64::try_from(want.max(1)).unwrap_or(i64::MAX))],
            )?,
        };
        if want.is_some()
            && let Some(last) = rows.last().map(|r| r[2].clone())
        {
            // A batch is adopted whole.
            let taken: BTreeSet<(String, String)> = rows
                .iter()
                .map(|r| Ok((as_text(&r[0])?.to_string(), as_text(&r[1])?.to_string())))
                .collect::<Result<_>>()?;
            for r in db.query(
                "SELECT collection, key, batch, gid, seq, image FROM fongo_found
                  WHERE batch = ?1 ORDER BY collection, key",
                &[last],
            )? {
                if !taken.contains(&(as_text(&r[0])?.to_string(), as_text(&r[1])?.to_string())) {
                    rows.push(r);
                }
            }
        }
        let mut records = Vec::new();
        for r in &rows {
            let name = as_text(&r[0])?;
            let Some(idx) = self.registry.index(name) else {
                db.exec(
                    "DELETE FROM fongo_found WHERE collection = ?1 AND key = ?2",
                    &[r[0].clone(), r[1].clone()],
                )?;
                continue;
            };
            let image = match &r[5] {
                Value::Blob(bytes) => image::decode(bytes)?,
                _ => continue,
            };
            records.push(Found {
                idx,
                key: as_text(&r[1])?.to_string(),
                gid: match &r[3] {
                    Value::Null => None,
                    v => Some(as_gid(v)?),
                },
                seq: u32::try_from(as_int(&r[4])?).unwrap_or(u32::MAX),
                row: image,
            });
        }
        let adopted = records.len();
        if !records.is_empty() {
            cx.adopting = true;
            if let Edits::Recorded(set, _) = self.adopt_edits(cx, &records)? {
                self.settle(cx, set)?;
            }
            cx.adopting = false;
        }
        if !recorded(db)? {
            db.exec("DELETE FROM fongo_found", &[])?;
            db.exec("DELETE FROM fongo_meta WHERE key = 'found_registry'", &[])?;
        }
        Ok(adopted)
    }
}

/// The strongly connected components of `after` with more than one set, found once by
/// Tarjan's method without recursion: each set is visited once and each edge walked once.
fn components(after: &BTreeMap<usize, BTreeSet<usize>>) -> Vec<Vec<usize>> {
    let mut index: BTreeMap<usize, usize> = BTreeMap::new();
    let mut low: BTreeMap<usize, usize> = BTreeMap::new();
    let mut on_stack: BTreeSet<usize> = BTreeSet::new();
    let mut stack: Vec<usize> = Vec::new();
    let mut out = Vec::new();
    let empty = BTreeSet::new();
    for &start in after.keys() {
        if index.contains_key(&start) {
            continue;
        }
        // Each frame: a set and the sets it names still to walk.
        let mut frames: Vec<(usize, std::collections::btree_set::Iter<'_, usize>)> = Vec::new();
        let n = index.len();
        index.insert(start, n);
        low.insert(start, n);
        stack.push(start);
        on_stack.insert(start);
        frames.push((start, after.get(&start).unwrap_or(&empty).iter()));
        while let Some((node, next)) = frames.last_mut() {
            let node = *node;
            if let Some(&n) = next.next() {
                if !index.contains_key(&n) {
                    let i = index.len();
                    index.insert(n, i);
                    low.insert(n, i);
                    stack.push(n);
                    on_stack.insert(n);
                    frames.push((n, after.get(&n).unwrap_or(&empty).iter()));
                } else if on_stack.contains(&n) {
                    let l = low[&node].min(index[&n]);
                    low.insert(node, l);
                }
                continue;
            }
            frames.pop();
            if let Some((parent, _)) = frames.last() {
                let l = low[parent].min(low[&node]);
                low.insert(*parent, l);
            }
            if low[&node] == index[&node] {
                let mut component = Vec::new();
                while let Some(top) = stack.pop() {
                    on_stack.remove(&top);
                    component.push(top);
                    if top == node {
                        break;
                    }
                }
                if component.len() > 1 {
                    out.push(component);
                }
            }
        }
    }
    out
}

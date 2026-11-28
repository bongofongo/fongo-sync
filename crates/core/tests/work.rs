//! The work the evaluator and the planner do, against a bound in terms of what they are given,
//! what they answer and what the model says they must read, over random schemas, changes and
//! row writes, and over shapes made to be expensive.
//!
//! Work is what the source is asked (one per question and one per item returned) plus what the
//! `work-counters` feature counts: rows fetched into a region, groups moved by a join, keys filed
//! again, keys looked up, members read to decide liveness and rows, gids taken off `affected`'s
//! queue and member lists compared, and in `order` rows, requirements and parkings looked at.
//!
//! - `affected` over a batch of B changes, then `target` of every pair it reports: work is at
//!   most 512 + 8 · E · log2(E + 2) · max(D, 1)². D is the most natural collections a chain of
//!   natural keys passes through (its cost grows about as the square of that depth). E is B,
//!   plus the gids the changes name, the pairs reported and the gids referring to a moved
//!   entity, plus what the source answers when asked for those referrers, plus what deciding
//!   each distinct group these fall in, before and after, reads (`reach`).
//!
//!   Deciding a group reads, by the model's groups and the facts the source keeps:
//!   - the gids of the group and of every group reached from it through natural keys (read for
//!     congruence) and owners (read for liveness, of every member of every group reached), and
//!     through the group's own references (read once each for its row, as a canonical gid of a
//!     live group);
//!   - for each of those gids, its entries where it is kept and its aliases, each alias once;
//!   - for each alias, the entries of the gid at its other end where that gid is kept in the
//!     alias's collection, fetched to see whether the two join;
//!   - for each distinct natural key among those gids, read through the groups, every key that
//!     reads so: one lookup each, and the entries of the gid that key derives where it is kept
//!     in the key's collection, fetched to see whether it holds that key.
//!
//!   Each term is a fact the source hands back or a question it is asked, each of constant
//!   size: a gid's claims are one per collection of the schema. Each is owed: a source answers
//!   for a gid's entries and aliases whole, and for every gid holding a value however it holds
//!   it, so a peer can make any of them as long as it likes, and the evaluator must read them to
//!   tell which matter. What it need not read it does not: the facts of a gid that holds a key
//!   it does not derive, the aliases of one that cannot join, the group of a gid kept elsewhere
//!   or holding a reference it does not keep; the tests named `read_nothing`, `read_no` and
//!   `decide_no` keep that work flat as those facts grow.
//!
//!   An entity is moved when its collection, its canonical gid or whether its group has a live
//!   member differs before and after the batch, and a gid refers to it when either model shows
//!   it holding a reference to the entity in a reference, owner or natural key field. These
//!   gids are in E because `affected`, as documented, queues every gid holding such a reference
//!   whenever the entity's collection, canonical gid or liveness differs, and decides each one
//!   in both sessions with the same questions as a gid the batch names. Each therefore costs
//!   what a named gid costs, one plus what deciding its group reads, though its row may not
//!   change. Leaving them out bounds by what the batch changes alone, which many gids referring
//!   to one entity exceed, as
//!   `affected_and_targets_count_the_gids_referring_to_a_moved_entity` shows.
//! - `order` over n rows with e requirements among them: at most 512 + 8 · (n + e) ·
//!   log2(n + e + 2). A requirement is a reference to a row being inserted or deleted, or a pair
//!   of rows one of which takes the unique values the other holds.
//!
//! Not near-linear in the data, though within these bounds: a natural key whose references fall
//! in groups of sizes g1, g2, … is looked up g1 · g2 · … times. Named shape `aliased_labels` (n
//! items merged, each with a tag of its own label, the tags merged) reads n² lookups, until
//! `Limits::lookups` makes it undetermined beyond 512 items; `merged_teams_and_users` (√n teams
//! and √n users merged, keyed by both) reads n.
//!
//! `FONGO_WORK_CASES` sets how many random cases run (default 200), drawn from a fixed seed
//! unless `PROPTEST_RNG_SEED` names another.

#![cfg(feature = "work-counters")]

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::{Counting, Rng, random_changes, random_schema};
use fongo_sync_core::eval::Evaluator;
use fongo_sync_core::model::{self, ALIAS, Collection, FieldKind, Identity, Row, Schema};
use fongo_sync_core::plan::{Columns, Planned, RowAction, order};
use fongo_sync_core::source::{Body, Source, Store, admit};
use fongo_sync_core::{Change, DELETED, Gid, Hlc, Strategy as Merge, Value, work};
use proptest::prelude::*;

/// Work any call may do whatever its size, and the slope of what grows with it.
const FIXED: f64 = 512.0;
const SLOPE: f64 = 8.0;

fn wide(n: u32) -> Gid {
    let mut bytes = [0u8; 16];
    bytes[12..].copy_from_slice(&n.to_be_bytes());
    Gid(bytes)
}

fn change(collection: &str, gid: Gid, field: &str, value: Value, wall_ms: u64, n: u32) -> Change {
    Change {
        collection: collection.into(),
        gid,
        field: field.into(),
        value,
        clock: Hlc {
            wall_ms,
            counter: n,
            node: Gid([9; 16]),
        },
    }
}

fn store(schema: &Schema, changes: &[Change]) -> Store {
    let mut store = Store::default();
    for c in changes {
        store.merge(schema, c);
    }
    store
}

fn log2(x: f64) -> f64 {
    (x + 2.0).log2()
}

/// Runs `affected` over `batch` and `target` of everything it names, checks the work and returns
/// it.
fn affected_within_bound(
    schema: &Schema,
    base: &[Change],
    batch: &[Change],
) -> Result<usize, String> {
    let ev = Evaluator::new(schema).unwrap();
    let before = store(schema, base);
    let mut after = before.clone();
    for c in batch {
        after.merge(schema, c);
    }
    work::take();
    let counting = Counting::new(&before);
    let Ok(found) = ev.affected(&counting, batch) else {
        return Err("affected is undetermined".into());
    };
    let mut done = counting.work.get() + work::take();
    let counting = Counting::new(&after);
    let mut session = ev.session(&counting);
    for (c, g) in &found {
        if session.target(c, *g).is_err() {
            return Err(format!("the target of {c} {g:?} is undetermined"));
        }
    }
    done += counting.work.get() + work::take();

    // The groups of what the batch names, of what `affected` reports and of what refers to an
    // entity the batch moves, before and after.
    let mut all = base.to_vec();
    all.extend_from_slice(batch);
    let models = [model::merge(schema, base), model::merge(schema, &all)];
    let groups_of = [members(&models[0]), members(&models[1])];
    let mut named: BTreeSet<(String, Gid)> = found.clone();
    for c in batch {
        if let Some(fact) = admit(schema, c) {
            named.insert((fact.collection.clone(), fact.gid));
            if let Body::Alias { target } = fact.body {
                named.insert((fact.collection, target));
            }
        }
    }
    named.extend(referrers(schema, &models));
    let mut extent = batch.len() + named.len() + holders(schema, &models, &after);
    let (mut was, mut will) = (ev.session(&before), ev.session(&after));
    let mut groups = BTreeSet::new();
    for (c, g) in &named {
        for (when, s) in [(0, &mut was), (1, &mut will)] {
            if let Ok(t) = s.target(c, *g)
                && groups.insert((when, c.clone(), t.canonical))
            {
                let kept = [&before, &after][when];
                extent += reach(schema, &models[when], &groups_of[when], kept, t.canonical);
            }
        }
    }
    work::take();
    let depth = chain(schema) as f64;
    let grows = extent as f64 * log2(extent as f64) * depth.max(1.0).powi(2);
    let bound = FIXED + SLOPE * grows;
    if std::env::var_os("FONGO_WORK_RATIOS").is_some() {
        eprintln!(
            "ratio {:.3} extent {extent} of bound {:.3}",
            done as f64 / grows,
            done as f64 / bound
        );
    }
    if done as f64 > bound {
        return Err(format!(
            "affected and targets: work {done} for extent {extent}, keys chained {} deep (bound {bound:.0})",
            chain(schema)
        ));
    }
    Ok(done)
}

/// The members of each group, by canonical gid.
type Members = BTreeMap<Gid, Vec<Gid>>;

/// The entities whose collection, canonical gid or whether a member of its group is live
/// differs between the models.
fn moved(models: &[model::Merge; 2]) -> BTreeSet<Gid> {
    let live: Vec<BTreeSet<Gid>> = models
        .iter()
        .map(|m| {
            m.entities
                .values()
                .filter(|e| e.live)
                .map(|e| e.canonical)
                .collect()
        })
        .collect();
    let read = |when: usize, x: &Gid| {
        let e = models[when].entities.get(x)?;
        Some((
            &e.collection,
            e.canonical,
            live[when].contains(&e.canonical),
        ))
    };
    models
        .iter()
        .flat_map(|m| m.entities.keys())
        .filter(|x| read(0, x) != read(1, x))
        .copied()
        .collect()
}

/// The fields that hold a reference: reference, owner and natural key fields.
fn referring(schema: &Schema) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for c in &schema.collections {
        let key: &[String] = match &c.identity {
            Identity::Natural(key) => key,
            Identity::Minted => &[],
        };
        for f in &c.fields {
            if matches!(f.kind, FieldKind::Reference(_) | FieldKind::Owner(_))
                || key.contains(&f.name)
            {
                out.push((c.name.clone(), f.name.clone()));
            }
        }
    }
    out
}

/// What the source answers when asked for the gids holding a reference to a moved entity: each
/// gid it names, and the entries `after` holds for it where it is kept, read to see whether it
/// keeps the reference.
fn holders(schema: &Schema, models: &[model::Merge; 2], after: &Store) -> usize {
    let mut n = 0;
    for x in moved(models) {
        for (c, f) in referring(schema) {
            for y in after.holding(&c, &[(&f, &Value::Ref(x))]) {
                let home = models[1].entities.get(&y).map(|e| e.collection.as_str());
                n += 1 + home.map_or(0, |h| after.entries(h, y).len());
            }
        }
    }
    n
}

/// Every gid that either model shows holding a reference to a moved entity in a reference, owner
/// or natural key field, with its collection.
fn referrers(schema: &Schema, models: &[model::Merge; 2]) -> BTreeSet<(String, Gid)> {
    let moved = moved(models);
    let mut found = BTreeSet::new();
    for m in models {
        for (y, e) in &m.entities {
            let Some(c) = schema.collection(&e.collection) else {
                continue;
            };
            let key: &[String] = match &c.identity {
                Identity::Natural(key) => key,
                Identity::Minted => &[],
            };
            let refers = c.fields.iter().any(|f| {
                let referring = matches!(f.kind, FieldKind::Reference(_) | FieldKind::Owner(_))
                    || key.contains(&f.name);
                let held = e.registers.get(&f.name).map(|(v, _)| v);
                referring && matches!(held, Some(Value::Ref(x)) if moved.contains(x))
            });
            if refers {
                found.insert((e.collection.clone(), *y));
            }
        }
    }
    found
}

fn members(merged: &model::Merge) -> Members {
    let mut members = Members::new();
    for (gid, e) in &merged.entities {
        members.entry(e.canonical).or_default().push(*gid);
    }
    members
}

/// A natural key read through the groups, with its collection: a canonical gid or a value at
/// each position.
type Sign = (String, Vec<Result<Gid, String>>);

/// What deciding the group of `g` reads at least, by the model's groups and the facts `kept`
/// holds. The groups: `g`'s, every group a natural key or an owner reaches from it, read for
/// congruence and liveness, and the groups `g`'s own references name, read for its row. For
/// each gid of those groups, the entries `kept` holds for it where it is kept, and the aliases
/// with it at either end, each once; for each alias, the entries of a gid at its other end kept
/// in its collection, which is fetched to see whether it joins. For each distinct natural key
/// among them, read through the groups, every key reading so: one lookup each, and the entries
/// of the gid that key derives where it is kept in the key's collection.
fn reach(schema: &Schema, merged: &model::Merge, members: &Members, kept: &Store, g: Gid) -> usize {
    let canonical = |x: Gid| merged.entities.get(&x).map_or(x, |e| e.canonical);
    let home = |x: Gid| merged.entities.get(&x).map(|e| e.collection.as_str());
    let of = |x: Gid| members.get(&x).cloned().unwrap_or_else(|| vec![x]);
    let facts = |x: Gid| home(x).map_or(0, |c| kept.entries(c, x).len());
    let start = canonical(g);
    let mut seen = BTreeSet::from([start]);
    let mut todo = vec![start];
    let mut signs: BTreeMap<Sign, Vec<Value>> = BTreeMap::new();
    while let Some(group) = todo.pop() {
        for m in of(group) {
            let Some(e) = merged.entities.get(&m) else {
                continue;
            };
            let Some(c) = schema.collection(&e.collection) else {
                continue;
            };
            let mut next = Vec::new();
            for f in &c.fields {
                let follows = match f.kind {
                    FieldKind::Owner(_) => true,
                    FieldKind::Reference(_) => group == start,
                    FieldKind::Plain => false,
                };
                if let (true, Some((Value::Ref(y), _))) = (follows, e.registers.get(&f.name)) {
                    next.push(canonical(*y));
                }
            }
            if let (Identity::Natural(key), true) = (&c.identity, e.keyed) {
                let values: Vec<Value> = key
                    .iter()
                    .map(|f| e.registers.get(f).map_or(Value::Null, |(v, _)| v.clone()))
                    .collect();
                let parts: Vec<Result<Gid, String>> = values
                    .iter()
                    .map(|v| match v {
                        Value::Ref(y) => Ok(canonical(*y)),
                        v => Err(format!("{v:?}")),
                    })
                    .collect();
                next.extend(parts.iter().flatten());
                signs.entry((e.collection.clone(), parts)).or_insert(values);
            }
            for y in next {
                if seen.insert(y) {
                    todo.push(y);
                }
            }
        }
    }
    let counted: BTreeSet<Gid> = seen.iter().flat_map(|&x| of(x)).collect();
    let entries: usize = counted.iter().map(|&m| facts(m)).sum();
    let aliases: BTreeSet<(String, Gid, Gid)> = counted
        .iter()
        .flat_map(|&m| kept.aliases(m))
        .map(|a| (a.collection, a.from, a.to))
        .collect();
    let neighbours: BTreeSet<Gid> = aliases
        .iter()
        .flat_map(|(c, from, to)| [(c, *from), (c, *to)])
        .filter(|(c, x)| !counted.contains(x) && home(*x) == Some(c.as_str()))
        .map(|(_, x)| x)
        .collect();
    let neighbours: usize = neighbours.iter().map(|&x| 1 + facts(x)).sum();
    let mut lookups = 0usize;
    for ((c, parts), values) in &signs {
        let lists: Vec<Vec<Value>> = parts
            .iter()
            .zip(values)
            .map(|(p, v)| match p {
                Ok(y) => of(*y).into_iter().map(Value::Ref).collect(),
                Err(_) => vec![v.clone()],
            })
            .collect();
        let n = lists.iter().fold(1usize, |n, l| n.saturating_mul(l.len()));
        lookups = lookups.saturating_add(n);
        if n > 1 << 18 {
            continue;
        }
        let mut at = vec![0; lists.len()];
        'keys: loop {
            let key: Vec<Value> = lists.iter().zip(&at).map(|(l, &i)| l[i].clone()).collect();
            let z = Gid::derive(c, &key);
            if !counted.contains(&z) && home(z) == Some(c.as_str()) {
                lookups += 1 + facts(z);
            }
            for d in (0..lists.len()).rev() {
                at[d] += 1;
                if at[d] < lists[d].len() {
                    continue 'keys;
                }
                at[d] = 0;
            }
            break;
        }
    }
    counted.len() + entries + aliases.len() + neighbours + lookups
}

/// The most natural collections a chain of natural keys passes through, each keyed on a
/// reference to the next.
fn chain(schema: &Schema) -> usize {
    fn from(schema: &Schema, c: &Collection) -> usize {
        let Identity::Natural(key) = &c.identity else {
            return 0;
        };
        1 + key
            .iter()
            .filter_map(|f| match &c.fields.iter().find(|d| &d.name == f)?.kind {
                FieldKind::Reference(t) | FieldKind::Owner(t) => schema.collection(t),
                FieldKind::Plain => None,
            })
            .map(|t| from(schema, t))
            .max()
            .unwrap_or(0)
    }
    schema
        .collections
        .iter()
        .map(|c| from(schema, c))
        .max()
        .unwrap_or(0)
}

/// Runs `order` over `planned` and checks the work.
fn order_within_bound(
    schema: &Schema,
    columns: &Columns,
    planned: &[Planned],
) -> Result<(), String> {
    work::take();
    order(schema, columns, planned);
    let done = work::take();
    let n = planned.len();
    let e = requirements(schema, planned);
    let grows = (n + e) as f64 * log2((n + e) as f64);
    let bound = FIXED + SLOPE * grows;
    if std::env::var_os("FONGO_WORK_RATIOS").is_some() {
        eprintln!(
            "order ratio {:.3} rows {n} requirements {e}",
            done as f64 / grows
        );
    }
    if done as f64 > bound {
        return Err(format!(
            "order: work {done} for {n} rows and {e} requirements (bound {bound:.0})"
        ));
    }
    Ok(())
}

/// How many requirements `order` can find among `planned`, at most: one for each reference in a
/// row's write or image, and one for each pair of rows one of which takes the values of a unique
/// set the other holds.
fn requirements(schema: &Schema, planned: &[Planned]) -> usize {
    let mut refs = 0;
    let mut holders: BTreeMap<(String, usize, Vec<String>), (usize, usize)> = BTreeMap::new();
    for p in planned {
        let Some(c) = schema.collection(&p.collection) else {
            continue;
        };
        let written = match &p.action {
            RowAction::Insert(r) | RowAction::Update(r) => Some(r),
            _ => None,
        };
        for row in [p.image.as_ref(), written].into_iter().flatten() {
            refs += row
                .iter()
                .filter(|(_, v)| matches!(v, Value::Ref(_)))
                .count();
        }
        let mut sets = c.identity_keys.clone();
        if let Identity::Natural(key) = &c.identity {
            sets.push(key.clone());
        }
        for (u, set) in sets.iter().enumerate() {
            let tuple = |row: &Row| -> Vec<String> {
                set.iter()
                    .map(|f| format!("{:?}", row.get(f).unwrap_or(&Value::Null)))
                    .collect()
            };
            if let Some(image) = &p.image {
                holders
                    .entry((p.collection.clone(), u, tuple(image)))
                    .or_default()
                    .0 += 1;
            }
            if let Some(w) = written {
                let mut row = p.image.clone().unwrap_or_default();
                row.extend(w.clone());
                holders
                    .entry((p.collection.clone(), u, tuple(&row)))
                    .or_default()
                    .1 += 1;
            }
        }
    }
    refs + holders.values().map(|(h, t)| h * t).sum::<usize>()
}

// --- Shapes made to be expensive -------------------------------------------------------------

fn items_and_tags() -> Schema {
    Schema::new(vec![
        Collection::minted("sites").plain("name", Merge::Lww),
        Collection::minted("items")
            .plain("text", Merge::Lww)
            .owner("site", "sites"),
        Collection::natural("tags", &["item", "label"])
            .owner("item", "items")
            .plain("label", Merge::Lww)
            .plain("v", Merge::Lww),
    ])
}

fn tag(item: Gid, label: i64, wall_ms: u64, n: u32) -> [Change; 2] {
    let key = [Value::Ref(item), Value::Int(label)];
    let gid = Gid::derive("tags", &key);
    [
        change("tags", gid, "item", key[0].clone(), wall_ms, n),
        change("tags", gid, "label", key[1].clone(), wall_ms, n),
    ]
}

/// A base, a batch and the schema they are over, `n` setting the size.
fn shape(kind: u8, n: u32) -> (Schema, Vec<Change>, Vec<Change>) {
    let schema = items_and_tags();
    let site = change("sites", wide(1), "name", Value::Int(0), 1, 0);
    let owned = |i: u32| change("items", wide(100 + i), "site", Value::Ref(wide(1)), 1, i);
    let merged = |i: u32| {
        change(
            "items",
            wide(100 + i),
            ALIAS,
            Value::Ref(wide(99 + i)),
            1,
            i,
        )
    };
    match kind % SHAPES {
        // Many new natural children of one parent.
        0 => {
            let base = vec![site, owned(0)];
            let batch = (0..n)
                .flat_map(|i| tag(wide(100), i.into(), 2, i))
                .collect();
            (schema, base, batch)
        }
        // A chain of aliases arriving in one batch.
        1 => {
            let batch = (0..n)
                .flat_map(|i| {
                    [
                        change("items", wide(10_000 + i), "text", Value::Int(0), 2, i),
                        change(
                            "items",
                            wide(10_000 + i),
                            ALIAS,
                            Value::Ref(wide(10_001 + i)),
                            2,
                            i,
                        ),
                    ]
                })
                .collect();
            (schema, vec![site], batch)
        }
        // A large base and a batch of one new item and three tags.
        2 => {
            let mut base = vec![site];
            for i in 0..n {
                base.push(owned(i));
                for l in 0..3 {
                    base.extend(tag(wide(100 + i), l, 1, i));
                }
            }
            let new = wide(99);
            let mut batch = vec![change("items", new, "site", Value::Ref(wide(1)), 2, 0)];
            for l in 0..3 {
                batch.extend(tag(new, l, 2, l as u32));
            }
            (schema, base, batch)
        }
        // An owner of many items renamed.
        3 => {
            let mut base = vec![site];
            base.extend((0..n).map(owned));
            let batch = vec![change("sites", wide(1), "name", Value::Int(1), 2, 0)];
            (schema, base, batch)
        }
        // A merged group whose members each have a natural child, every member edited.
        4 => {
            let mut base = vec![site];
            for i in 0..n {
                base.push(owned(i));
                if i > 0 {
                    base.push(change(
                        "items",
                        wide(100 + i),
                        ALIAS,
                        Value::Ref(wide(99 + i)),
                        1,
                        i,
                    ));
                }
                base.extend(tag(wide(100 + i), 0, 1, i));
            }
            let batch = (0..n)
                .map(|i| change("items", wide(100 + i), "text", Value::Int(i.into()), 2, i))
                .collect();
            (schema, base, batch)
        }
        // A ladder of aliases that natural keys reveal one step at a time, completed by a batch.
        5 => {
            let schema = Schema::new(vec![
                Collection::minted("roots").plain("name", Merge::Lww),
                Collection::natural("kids", &["up", "name"])
                    .owner("up", "roots")
                    .plain("name", Merge::Lww),
            ]);
            let kid = |root: Gid| {
                let key = [Value::Ref(root), Value::Int(0)];
                (Gid::derive("kids", &key), key)
            };
            let (r, rp) = (|i: u32| wide(10 + i), |i: u32| wide(1_000_000 + i));
            let mut base = Vec::new();
            for i in 0..=n {
                base.push(change("roots", r(i), "name", Value::Int(0), 1, i));
                base.push(change("roots", rp(i), "name", Value::Int(0), 1, i));
                if i > 0 {
                    base.push(change("roots", rp(i), ALIAS, Value::Ref(r(i)), 1, i));
                }
                for root in [r(i), rp(i)] {
                    let (g, key) = kid(root);
                    base.push(change("kids", g, "up", key[0].clone(), 1, i));
                    base.push(change("kids", g, "name", key[1].clone(), 1, i));
                }
            }
            for i in 0..n {
                base.push(change(
                    "kids",
                    kid(r(i)).0,
                    ALIAS,
                    Value::Ref(kid(rp(i + 1)).0),
                    1,
                    i,
                ));
            }
            let batch = vec![change("roots", rp(0), ALIAS, Value::Ref(r(0)), 2, 0)];
            (schema, base, batch)
        }
        // `aliased_labels`: items merged into one group, each with a tag of its own label, the
        // tags merged too; one field of one tag edited.
        7 => {
            let mut base = vec![site];
            let mut tags = Vec::new();
            for i in 0..n {
                base.push(owned(i));
                if i > 0 {
                    base.push(merged(i));
                }
                let t = tag(wide(100 + i), i.into(), 1, i);
                let g = t[0].gid;
                base.extend(t);
                if let Some(&last) = tags.last() {
                    base.push(change("tags", g, ALIAS, Value::Ref(last), 1, i));
                }
                tags.push(g);
            }
            let batch = vec![change("tags", tags[0], "v", Value::Int(5), 2, 0)];
            (schema, base, batch)
        }
        // One field edited of the one tag of an item merged with many others.
        8 => {
            let mut base = vec![site];
            for i in 0..n {
                base.push(owned(i));
                if i > 0 {
                    base.push(merged(i));
                }
            }
            let t = tag(wide(100), 0, 1, 0);
            let g = t[0].gid;
            base.extend(t);
            (
                schema,
                base,
                vec![change("tags", g, "v", Value::Int(5), 2, 0)],
            )
        }
        // One field edited of one item of a large merged group.
        9 => {
            let mut base = vec![site];
            for i in 0..n {
                base.push(owned(i));
                if i > 0 {
                    base.push(merged(i));
                }
            }
            let batch = vec![change(
                "items",
                wide(100 + n / 2),
                "text",
                Value::Int(1),
                2,
                0,
            )];
            (schema, base, batch)
        }
        // Tags of different items, so with different keys, merged into one chain; one edited.
        10 => {
            let mut base = vec![site];
            let mut last = None;
            for i in 0..n {
                base.push(owned(i));
                let t = tag(wide(100 + i), 0, 1, i);
                let g = t[0].gid;
                base.extend(t);
                if let Some(prev) = last {
                    base.push(change("tags", g, ALIAS, Value::Ref(prev), 1, i));
                }
                last = Some(g);
            }
            let batch = vec![change("tags", last.unwrap(), "v", Value::Int(5), 2, 0)];
            (schema, base, batch)
        }
        // Many memberships of one team, keyed by team and user in either order; one user joins.
        11 => {
            let (schema, member) = memberships(n.is_multiple_of(2));
            let team = wide(1);
            let mut base = vec![change("teams", team, "name", Value::Int(0), 1, 0)];
            for i in 0..n {
                let user = wide(1_000 + i);
                base.push(change("users", user, "name", Value::Int(0), 1, i));
                base.extend(member(team, user, 1, i));
            }
            let user = wide(999);
            let mut batch = vec![change("users", user, "name", Value::Int(0), 2, 0)];
            batch.extend(member(team, user, 2, 1));
            (schema, base, batch)
        }
        // `merged_teams_and_users`: √n teams merged and √n users merged, keyed by both in either
        // order, one membership whose role is edited.
        12 => {
            let (schema, member) = memberships(n % 2 == 1);
            let k = (n as f64).sqrt() as u32 + 1;
            let mut base = Vec::new();
            for i in 0..k {
                for (c, g, prev) in [
                    ("teams", wide(1 + i), wide(i)),
                    ("users", wide(1_000 + i), wide(999 + i)),
                ] {
                    base.push(change(c, g, "name", Value::Int(0), 1, i));
                    if i > 0 {
                        base.push(change(c, g, ALIAS, Value::Ref(prev), 1, i));
                    }
                }
            }
            let one = member(wide(1), wide(1_000), 1, 0);
            let g = one[0].gid;
            base.extend(one);
            (
                schema,
                base,
                vec![change("members", g, "role", Value::Int(1), 2, 0)],
            )
        }
        // Many tags of an item not yet seen, then its deletion alone: the item joins its
        // collection, so `affected` decides every tag again, though none of their rows changes.
        13 => {
            let base = (0..n)
                .flat_map(|l| tag(wide(100), l.into(), 1, l))
                .collect();
            let batch = vec![change("items", wide(100), DELETED, Value::Int(1), 2, 0)];
            (schema, base, batch)
        }
        // Two copies of paths through natural keys four collections deep, joined at the root.
        _ => {
            let depth = 4;
            let mut collections = vec![Collection::minted("c0").plain("name", Merge::Lww)];
            for d in 1..=depth {
                collections.push(
                    Collection::natural(&format!("c{d}"), &["up", "name"])
                        .owner("up", &format!("c{}", d - 1))
                        .plain("name", Merge::Lww),
                );
            }
            let schema = Schema::new(collections);
            let mut base = Vec::new();
            for root in [wide(1), wide(2)] {
                base.push(change("c0", root, "name", Value::Int(0), 1, 0));
                for w in 0..n {
                    let mut up = root;
                    for d in 1..=depth {
                        let key = [Value::Ref(up), Value::Int(w.into())];
                        let c = format!("c{d}");
                        let gid = Gid::derive(&c, &key);
                        base.push(change(&c, gid, "up", key[0].clone(), 1, w));
                        base.push(change(&c, gid, "name", key[1].clone(), 1, w));
                        up = gid;
                    }
                }
            }
            let batch = vec![change("c0", wide(2), ALIAS, Value::Ref(wide(1)), 2, 0)];
            (schema, base, batch)
        }
    }
}

/// How many shapes [`shape`] makes.
const SHAPES: u8 = 14;

/// Teams, users and memberships keyed by both, team first or user first; and the changes that
/// create a membership.
#[allow(clippy::type_complexity)]
fn memberships(team_first: bool) -> (Schema, impl Fn(Gid, Gid, u64, u32) -> [Change; 2]) {
    let key: &[&str] = if team_first {
        &["team", "user"]
    } else {
        &["user", "team"]
    };
    let schema = Schema::new(vec![
        Collection::minted("teams").plain("name", Merge::Lww),
        Collection::minted("users").plain("name", Merge::Lww),
        Collection::natural("members", key)
            .owner("team", "teams")
            .owner("user", "users")
            .plain("role", Merge::Lww),
    ]);
    let member = move |team: Gid, user: Gid, wall_ms: u64, n: u32| {
        let values = if team_first {
            [Value::Ref(team), Value::Ref(user)]
        } else {
            [Value::Ref(user), Value::Ref(team)]
        };
        let gid = Gid::derive("members", &values);
        [
            change("members", gid, "team", Value::Ref(team), wall_ms, n),
            change("members", gid, "user", Value::Ref(user), wall_ms, n),
        ]
    };
    (schema, member)
}

fn unique_name() -> Schema {
    Schema::new(vec![
        Collection::minted("u")
            .plain("name", Merge::Lww)
            .identity_key(&["name"]),
    ])
}

fn named(v: u32) -> Row {
    [("name".to_string(), Value::Int(v.into()))].into()
}

/// A permutation of `0..n`, fixed by `seed`, or none.
fn shuffled(n: u32, seed: Option<u64>) -> Vec<u32> {
    let mut ids: Vec<u32> = (0..n).collect();
    if let Some(seed) = seed {
        let mut rng = Rng(seed | 1);
        for i in (1..ids.len()).rev() {
            ids.swap(i, rng.below(i + 1));
        }
    }
    ids
}

fn inserted(c: &str, g: Gid, row: Row) -> Planned {
    Planned {
        collection: c.into(),
        gid: g,
        image: None,
        action: RowAction::Insert(row),
    }
}

/// How many shapes [`order_shape`] makes.
const ORDER_SHAPES: u8 = 6;

/// Rows to order, made to be expensive, `n` setting the size, `seed` (if any) shuffling gids.
fn order_shape(
    kind: u8,
    n: u32,
    not_null: bool,
    seed: Option<u64>,
) -> (Schema, Columns, Vec<Planned>) {
    match kind % ORDER_SHAPES {
        // A list of inserts linked both ways, every link accepting NULL.
        3 => {
            let schema = Schema::new(vec![
                Collection::minted("a")
                    .reference("prev", "a")
                    .reference("next", "a"),
            ]);
            let ids = shuffled(n, seed);
            let at = |i: u32| Value::Ref(wide(ids[i as usize]));
            let planned = (0..n)
                .map(|i| {
                    let mut row = Row::new();
                    row.insert("prev".into(), if i > 0 { at(i - 1) } else { Value::Null });
                    row.insert(
                        "next".into(),
                        if i + 1 < n { at(i + 1) } else { Value::Null },
                    );
                    inserted("a", wide(ids[i as usize]), row)
                })
                .collect();
            (schema, Columns::new(), planned)
        }
        // A chain of rows swapping one unique value with the next and another with the one
        // before, the first also referring to one of a pair of inserts that cannot be written.
        4 => {
            let schema = Schema::new(vec![
                Collection::minted("c")
                    .plain("f", Merge::Lww)
                    .plain("g", Merge::Lww)
                    .reference("s", "s")
                    .identity_key(&["f"])
                    .identity_key(&["g"]),
                Collection::minted("s").reference("r", "s"),
            ]);
            let ids = shuffled(n.max(2), seed);
            let n = ids.len();
            let (f0, g0): (Vec<i64>, Vec<i64>) = ((0..n as i64).collect(), (0..n as i64).collect());
            let (mut f, mut g) = (f0.clone(), g0.clone());
            for i in 0..n - 1 {
                if i % 2 == 0 {
                    f.swap(i, i + 1);
                } else {
                    g.swap(i, i + 1);
                }
            }
            let (s1, s2) = (wide(5_000_001), wide(5_000_002));
            let mut planned = vec![
                inserted("s", s1, [("r".to_string(), Value::Ref(s2))].into()),
                inserted("s", s2, [("r".to_string(), Value::Ref(s1))].into()),
            ];
            for i in 0..n {
                let image: Row = [
                    ("f".to_string(), Value::Int(f0[i])),
                    ("g".to_string(), Value::Int(g0[i])),
                    ("s".to_string(), Value::Null),
                ]
                .into();
                let mut write = Row::new();
                if f[i] != f0[i] {
                    write.insert("f".into(), Value::Int(f[i]));
                }
                if g[i] != g0[i] {
                    write.insert("g".into(), Value::Int(g[i]));
                }
                if i == 0 {
                    write.insert("s".into(), Value::Ref(s1));
                }
                planned.push(Planned {
                    collection: "c".into(),
                    gid: wide(ids[i]),
                    image: Some(image),
                    action: RowAction::Update(write),
                });
            }
            (schema, Columns::new().not_null("s", "r"), planned)
        }
        // A short list of inserts linked both ways, its far end referred to by many inserts.
        5 => {
            let schema = Schema::new(vec![
                Collection::minted("a")
                    .reference("prev", "a")
                    .reference("next", "a"),
            ]);
            let len = 2 * (n as f64).sqrt() as u32 + 2;
            let mut planned: Vec<Planned> = (0..len)
                .map(|i| {
                    let mut row = Row::new();
                    row.insert(
                        "prev".into(),
                        if i > 0 {
                            Value::Ref(wide(i - 1))
                        } else {
                            Value::Null
                        },
                    );
                    let next = if i + 1 < len {
                        Value::Ref(wide(i + 1))
                    } else {
                        Value::Null
                    };
                    row.insert("next".into(), next);
                    inserted("a", wide(i), row)
                })
                .collect();
            for j in 0..n {
                let row = [("prev".to_string(), Value::Ref(wide(len - 1)))].into();
                planned.push(inserted("a", wide(1_000_000 + j), row));
            }
            (schema, Columns::new(), planned)
        }
        k => order_shape_basic(k, n, not_null),
    }
}

fn order_shape_basic(kind: u8, n: u32, not_null: bool) -> (Schema, Columns, Vec<Planned>) {
    match kind % 3 {
        // Independent swaps of a unique value, which can or cannot be parked.
        0 => {
            let columns = if not_null {
                Columns::new().not_null("u", "name")
            } else {
                Columns::new()
            };
            let planned = (0..n)
                .flat_map(|i| {
                    [(2 * i, 2 * i + 1), (2 * i + 1, 2 * i)].map(|(a, b)| Planned {
                        collection: "u".into(),
                        gid: wide(a),
                        image: Some(named(a)),
                        action: RowAction::Update(named(b)),
                    })
                })
                .collect();
            (unique_name(), columns, planned)
        }
        // Pairs deleted together, each referring to the other, only one side of which may be
        // set to NULL, the other side ordered first.
        1 => {
            let schema = Schema::new(vec![
                Collection::minted("a").reference("r", "b"),
                Collection::minted("b").reference("r", "a"),
            ]);
            let mut planned = Vec::new();
            for i in 0..n {
                let (x, y) = (wide(i), wide(1_000_000 + (n - 1 - i)));
                for (c, g, to) in [("a", x, y), ("b", y, x)] {
                    planned.push(Planned {
                        collection: c.into(),
                        gid: g,
                        image: Some([("r".to_string(), Value::Ref(to))].into()),
                        action: RowAction::Delete,
                    });
                }
            }
            (schema, Columns::new().not_null("a", "r"), planned)
        }
        // A ring of inserted rows, each also referring to one of a pair that cannot be written.
        _ => {
            let schema = Schema::new(vec![
                Collection::minted("s").reference("r", "s"),
                Collection::minted("a")
                    .reference("next", "a")
                    .reference("on", "s"),
            ]);
            let insert = |c: &str, g: Gid, row: Row| Planned {
                collection: c.into(),
                gid: g,
                image: None,
                action: RowAction::Insert(row),
            };
            let mut planned = vec![
                insert(
                    "s",
                    wide(1),
                    [("r".to_string(), Value::Ref(wide(2)))].into(),
                ),
                insert(
                    "s",
                    wide(2),
                    [("r".to_string(), Value::Ref(wide(1)))].into(),
                ),
            ];
            for i in 0..n {
                let row = [
                    ("next".to_string(), Value::Ref(wide(100 + (i + 1) % n))),
                    ("on".to_string(), Value::Ref(wide(1))),
                ];
                planned.push(insert("a", wide(100 + i), row.into()));
            }
            (schema, Columns::new().not_null("s", "r"), planned)
        }
    }
}

/// Random row writes over a random schema: inserts, updates and deletes whose references and
/// unique values are drawn from a few rows, consistent or not, with random columns refusing NULL.
fn random_plan(rng: &mut Rng, n: usize) -> (Schema, Columns, Vec<Planned>) {
    let many = 8.max(n as u32 / 2);
    let schema = random_schema(rng);
    let mut columns = Columns::new();
    for c in &schema.collections {
        for f in &c.fields {
            if rng.one_in(4) {
                columns = columns.not_null(&c.name, &f.name);
            }
        }
    }
    let gids: Vec<Gid> = (0..many).map(|i| wide(i + 1)).collect();
    let row = |rng: &mut Rng, c: &Collection| -> Row {
        c.fields
            .iter()
            .map(|f| {
                let v = match f.kind {
                    FieldKind::Plain => Value::Int(rng.below(3) as i64),
                    _ if rng.one_in(4) => Value::Null,
                    _ => Value::Ref(*rng.pick(&gids)),
                };
                (f.name.clone(), v)
            })
            .collect()
    };
    let mut seen = BTreeSet::new();
    let mut planned = Vec::new();
    for _ in 0..n {
        let c = rng.pick(&schema.collections).clone();
        let gid = *rng.pick(&gids);
        if !seen.insert((c.name.clone(), gid)) {
            continue;
        }
        let image = (!rng.one_in(3)).then(|| row(rng, &c));
        let action = match (rng.below(3), &image) {
            (0, Some(_)) => RowAction::Delete,
            (_, None) => RowAction::Insert(row(rng, &c)),
            _ => RowAction::Update(row(rng, &c)),
        };
        planned.push(Planned {
            collection: c.name.clone(),
            gid,
            image,
            action,
        });
    }
    (schema, columns, planned)
}

fn cases() -> u32 {
    std::env::var("FONGO_WORK_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200)
}

proptest! {
    #![proptest_config(common::config(cases()))]

    #[test]
    fn affected_and_targets_work_within_their_bound_on_random_changes(seed in any::<u64>()) {
        let mut rng = Rng(seed | 1);
        let schema = random_schema(&mut rng);
        let n = 1 + rng.below(120);
        let changes = random_changes(&mut rng, &schema, n);
        let cut = rng.below(changes.len() + 1);
        if let Err(e) = affected_within_bound(&schema, &changes[..cut], &changes[cut..]) {
            prop_assert!(false, "{e}");
        }
    }

    #[test]
    fn affected_and_targets_work_within_their_bound_on_expensive_shapes(
        kind in any::<u8>(),
        n in 1u32..600,
    ) {
        let (schema, base, batch) = shape(kind, n);
        // `aliased_labels` asks n² lookups, past `Limits::lookups` beyond 512 items.
        let gives_up = kind % SHAPES == 7 && n > 512;
        match affected_within_bound(&schema, &base, &batch) {
            Err(e) if gives_up => prop_assert!(e.contains("undetermined"), "shape 7: {e}"),
            Ok(_) if gives_up => prop_assert!(false, "shape 7 at {n} items is decided"),
            Err(e) => prop_assert!(false, "shape {}: {e}", kind % SHAPES),
            Ok(_) => {}
        }
    }

    #[test]
    fn order_works_within_its_bound(
        kind in any::<u8>(),
        n in 1u32..1_500,
        not_null in any::<bool>(),
        seed in any::<u64>(),
    ) {
        let (schema, columns, planned) = if kind % (ORDER_SHAPES + 1) == ORDER_SHAPES {
            random_plan(&mut Rng(seed | 1), (seed % 3_000) as usize)
        } else {
            order_shape(kind, n, not_null, seed.is_multiple_of(2).then_some(seed))
        };
        if let Err(e) = order_within_bound(&schema, &columns, &planned) {
            prop_assert!(false, "shape {}: {e}", kind % (ORDER_SHAPES + 1));
        }
    }
}

/// The gids referring to a moved entity are decided again however little the batch changes, so
/// the bound counts them: here a batch of one change makes `n` tags be decided again. At the
/// largest `n`, work quadratic in the gids referring to the entity is several times the bound,
/// though at 300 it is within it.
#[test]
fn affected_and_targets_count_the_gids_referring_to_a_moved_entity() {
    for n in [1, 20, 300, 3_000] {
        let (schema, base, batch) = shape(13, n);
        affected_within_bound(&schema, &base, &batch).unwrap();
    }
}

/// An alias that a gid kept in one collection writes in another joins nothing, so the model's
/// group of the gid it names holds that gid alone. Deciding it should then read nothing of the
/// group the aliasing gid has in its own collection, however large: here one field of the named
/// gid is edited, and the aliasing gid is merged with `n` others in its own collection.
#[test]
fn affected_and_targets_read_no_group_through_an_alias_its_writer_is_not_kept_in() {
    flat(|n| {
        let (x, y) = (wide(1), wide(2));
        let mut base = vec![
            change("a", x, "p", Value::Int(0), 1, 0),
            change("d", y, "p", Value::Int(0), 1, 0),
            change("a", y, ALIAS, Value::Ref(x), 2, 0),
        ];
        for i in 0..n {
            let z = wide(100 + i);
            let prev = if i == 0 { y } else { wide(99 + i) };
            base.push(change("d", z, "p", Value::Int(0), 1, i));
            base.push(change("d", z, ALIAS, Value::Ref(prev), 1, i));
        }
        (
            two_collections(),
            base,
            vec![change("a", x, "p", Value::Int(1), 3, 0)],
        )
    });
}

fn two_collections() -> Schema {
    Schema::new(vec![
        Collection::minted("a").plain("p", Merge::Lww),
        Collection::minted("d").plain("p", Merge::Lww),
    ])
}

/// Checks the work of `shape(n)` against the bound for every `n`, and that it does not grow.
fn flat(shape: impl Fn(u32) -> (Schema, Vec<Change>, Vec<Change>)) {
    let works: Vec<usize> = [1, 100, 1_000, 5_000]
        .into_iter()
        .map(|n| {
            let (schema, base, batch) = shape(n);
            affected_within_bound(&schema, &base, &batch).unwrap()
        })
        .collect();
    assert!(works.iter().all(|&w| w > 0 && w == works[0]), "{works:?}");
}

/// A gid kept in `d`, aliased there by `n` others or aliasing `n` others itself, writes an alias
/// to `x` in `a`, or `n` gids of `a` write aliases to it; then one field of `x` is edited. None
/// of these aliases joins `x` with anything, and deciding `x` reads none of them.
#[test]
fn affected_and_targets_read_nothing_of_a_hub_kept_elsewhere() {
    let (x, y) = (wide(1), wide(2));
    let edit = || vec![change("a", x, "p", Value::Int(1), 3, 0)];
    let hub = |n: u32, written_by_hub: bool| {
        let mut base = vec![
            change("a", x, "p", Value::Int(0), 1, 0),
            change("d", y, "p", Value::Int(0), 1, 0),
            change("a", y, ALIAS, Value::Ref(x), 2, 0),
        ];
        for i in 0..n {
            let z = wide(100 + i);
            base.push(change("d", z, "p", Value::Int(0), 1, i));
            base.push(if written_by_hub {
                change("d", y, ALIAS, Value::Ref(z), 1, i)
            } else {
                change("d", z, ALIAS, Value::Ref(y), 1, i)
            });
        }
        (two_collections(), base, edit())
    };
    flat(|n| hub(n, false));
    flat(|n| hub(n, true));
    flat(|n| {
        let mut base = vec![
            change("a", x, "p", Value::Int(0), 1, 0),
            change("d", y, "p", Value::Int(0), 0, 0),
            change("a", x, ALIAS, Value::Ref(y), 2, 0),
        ];
        for i in 0..n {
            let z = wide(100 + i);
            base.push(change("a", z, "p", Value::Int(0), 1, i));
            base.push(change("a", z, ALIAS, Value::Ref(y), 1, i));
        }
        (two_collections(), base, edit())
    });
}

/// A source answers for a gid's aliases in every collection, though only those of the collection
/// it is kept in can join it, so the bound counts them: here `n` gids kept in `d` write aliases to
/// `x` in `a`, or `x` writes `n` aliases in `d`, and one field of `x` is edited.
#[test]
fn affected_and_targets_count_the_aliases_a_decided_gid_holds() {
    let x = wide(1);
    for n in [1, 100, 1_000, 5_000] {
        for written_by_x in [false, true] {
            let mut base = vec![change("a", x, "p", Value::Int(0), 1, 0)];
            for i in 0..n {
                let y = wide(100 + i);
                base.push(change("d", y, "p", Value::Int(0), 1, i));
                base.push(if written_by_x {
                    change("d", x, ALIAS, Value::Ref(y), 2, i)
                } else {
                    change("a", y, ALIAS, Value::Ref(x), 2, i)
                });
            }
            let batch = vec![change("a", x, "p", Value::Int(1), 3, 0)];
            affected_within_bound(&two_collections(), &base, &batch).unwrap();
        }
    }
}

/// One field edited of one item of a merged group. At the largest `n`, work quadratic in the
/// group's size is several times the bound; at the expensive shapes' sizes it is near it.
#[test]
fn affected_and_targets_decide_a_large_merged_group_in_work_linear_in_it() {
    for n in [3_000, 300, 1] {
        let (schema, base, batch) = shape(9, n);
        affected_within_bound(&schema, &base, &batch).unwrap();
    }
}

/// `a`, `d`, and `t` keyed by a reference to `a` and a number, with a plain field `v`; and the
/// gid of the `t` keyed by `x` and 0 with the changes that make it.
fn keyed_on_a() -> (Schema, Gid, Vec<Change>) {
    let schema = Schema::new(vec![
        Collection::minted("a").plain("p", Merge::Lww),
        Collection::minted("d").plain("p", Merge::Lww),
        Collection::natural("t", &["up", "k"])
            .reference("up", "a")
            .plain("k", Merge::Lww)
            .plain("v", Merge::Lww),
    ]);
    let x = wide(1);
    let key = [Value::Ref(x), Value::Int(0)];
    let t1 = Gid::derive("t", &key);
    let base = vec![
        change("a", x, "p", Value::Int(0), 1, 0),
        change("t", t1, "up", key[0].clone(), 1, 0),
        change("t", t1, "k", key[1].clone(), 1, 1),
    ];
    (schema, t1, base)
}

/// Writes `up` = `x` and `k` = 0 to `z` in `t`, the key `t1` holds, at a clock after `t1`'s.
fn same_key(z: Gid, i: u32) -> [Change; 2] {
    [
        change("t", z, "up", Value::Ref(wide(1)), 2, i),
        change("t", z, "k", Value::Int(0), 2, i),
    ]
}

/// Gids that hold the values of a natural key without being the gid that key derives join
/// nothing, whether they are kept in the key's collection or elsewhere and however many aliases
/// they hold, so deciding the entity that key derives reads none of them: one gid holding the
/// key with `n` aliases in `d` or in `t`, `n` gids kept in `t` holding it, or `n` gids kept in
/// `d` holding it, and one field of the entity edited.
#[test]
fn affected_and_targets_read_nothing_of_gids_holding_a_key_they_do_not_derive() {
    let edit = |t1: Gid| vec![change("t", t1, "v", Value::Int(1), 3, 0)];
    for aliased_in in ["d", "t"] {
        flat(|n| {
            let (schema, t1, mut base) = keyed_on_a();
            let z = wide(3);
            base.extend(same_key(z, 0));
            for i in 0..n {
                let y = wide(100 + i);
                base.push(change(aliased_in, y, "p", Value::Int(0), 1, i));
                base.push(change(aliased_in, y, ALIAS, Value::Ref(z), 2, i));
            }
            (schema, base, edit(t1))
        });
    }
    for kept_in in ["t", "d"] {
        flat(|n| {
            let (schema, t1, mut base) = keyed_on_a();
            for i in 0..n {
                let y = wide(100 + i);
                base.push(change(kept_in, y, "p", Value::Int(0), 1, i));
                base.extend(same_key(y, i));
            }
            (schema, base, edit(t1))
        });
    }
}

/// Gids that hold a reference to an entity in a field of a collection they are not kept in,
/// or in a key field whose key is not theirs, read nothing of it, so when the entity is deleted
/// none of them is decided again: one such gid merged with `n` others where it is kept.
#[test]
fn affected_and_targets_decide_no_gid_holding_a_reference_it_does_not_keep() {
    let keyed = |up: Gid, i: u32| {
        let key = [Value::Ref(up), Value::Int(0)];
        let g = Gid::derive("t", &key);
        (
            g,
            [
                change("t", g, "up", key[0].clone(), 1, i),
                change("t", g, "k", key[1].clone(), 1, i),
            ],
        )
    };
    for kept_in_d in [true, false] {
        flat(|n| {
            let (schema, _, mut base) = keyed_on_a();
            let y = if kept_in_d {
                let y = wide(3);
                base.push(change("d", y, "p", Value::Int(0), 1, 0));
                y
            } else {
                let (y, made) = keyed(wide(2), 0);
                base.extend(made);
                y
            };
            base.push(change("t", y, "up", Value::Ref(wide(1)), 2, 0));
            let home = if kept_in_d { "d" } else { "t" };
            for i in 0..n {
                let z = if kept_in_d {
                    let z = wide(100 + i);
                    base.push(change("d", z, "p", Value::Int(0), 1, i));
                    z
                } else {
                    let (z, made) = keyed(wide(100_000 + i), i);
                    base.extend(made);
                    z
                };
                base.push(change(home, z, ALIAS, Value::Ref(y), 1, i));
            }
            (
                schema,
                base,
                vec![change("a", wide(1), DELETED, Value::Int(1), 3, 0)],
            )
        });
    }
}

/// Facts the source answers for though the model shows nothing of them, each read once:
/// `n` gids kept in `d` holding a reference in `t` to an entity that is deleted, and `n`
/// undeclared fields of an entity one of whose fields is edited.
#[test]
fn affected_and_targets_count_the_facts_the_source_answers_with() {
    for n in [1, 100, 1_000, 5_000] {
        let (schema, _, mut base) = keyed_on_a();
        for i in 0..n {
            let y = wide(100 + i);
            base.push(change("d", y, "p", Value::Int(0), 1, i));
            base.push(change("t", y, "up", Value::Ref(wide(1)), 2, i));
        }
        let batch = vec![change("a", wide(1), DELETED, Value::Int(1), 3, 0)];
        affected_within_bound(&schema, &base, &batch).unwrap();

        let (schema, t1, mut base) = keyed_on_a();
        for i in 0..n {
            base.push(change("t", t1, &format!("u{i}"), Value::Int(0), 2, i));
        }
        let batch = vec![change("t", t1, "v", Value::Int(1), 3, 0)];
        affected_within_bound(&schema, &base, &batch).unwrap();
    }
}

/// A row reads each reference and owner it holds as the canonical gid of the group it names,
/// and whether that group is live, so the bound counts the group: an item referring to, or
/// owned by, a site merged with `n` others, and one field of the item edited.
#[test]
fn affected_and_targets_count_the_groups_a_row_refers_to() {
    for owner in [false, true] {
        for n in [1, 100, 1_000, 5_000] {
            let items = Collection::minted("items").plain("text", Merge::Lww);
            let items = if owner {
                items.owner("r", "sites")
            } else {
                items.reference("r", "sites")
            };
            let schema = Schema::new(vec![
                Collection::minted("sites").plain("name", Merge::Lww),
                items,
            ]);
            let mut base = vec![change("sites", wide(1), "name", Value::Int(0), 1, 0)];
            for i in 0..n {
                let prev = if i == 0 { wide(1) } else { wide(99 + i) };
                base.push(change("sites", wide(100 + i), "name", Value::Int(0), 1, i));
                base.push(change(
                    "sites",
                    wide(100 + i),
                    ALIAS,
                    Value::Ref(prev),
                    1,
                    i,
                ));
            }
            base.push(change("items", wide(9), "r", Value::Ref(wide(1)), 1, 0));
            let batch = vec![change("items", wide(9), "text", Value::Int(1), 2, 0)];
            affected_within_bound(&schema, &base, &batch).unwrap();
        }
    }
}

/// A gid written many times in every collection holds one claim in each, so where it is kept is
/// one question of constant size: `n` writes to `x` in each of `a`, `d` and `t`, and one field
/// of the `t` keyed by `x` edited.
#[test]
fn affected_and_targets_ask_where_a_gid_is_kept_in_one_small_question() {
    flat(|n| {
        let (schema, t1, mut base) = keyed_on_a();
        for i in 0..n {
            for c in ["a", "d", "t"] {
                base.push(change(c, wide(1), "p", Value::Int(i.into()), 2, i));
            }
        }
        (
            schema,
            base,
            vec![change("t", t1, "v", Value::Int(1), 3, 0)],
        )
    });
}

/// A gid that cannot join is not asked for its aliases, however many it holds: a gid kept in
/// `t` that does not hold a key deriving it, aliasing the entity edited, or the gid a key of the
/// entity derives through a merged group, claimed in `t` without that key; `n` aliases in `d`
/// name it.
#[test]
fn affected_and_targets_read_no_aliases_of_a_gid_that_cannot_join() {
    for derived in [false, true] {
        flat(|n| {
            let (schema, t1, mut base) = keyed_on_a();
            let y = if derived {
                base.push(change("a", wide(2), "p", Value::Int(0), 1, 1));
                base.push(change("a", wide(2), ALIAS, Value::Ref(wide(1)), 2, 0));
                Gid::derive("t", &[Value::Ref(wide(2)), Value::Int(0)])
            } else {
                wide(3)
            };
            base.push(change("t", y, "v", Value::Int(7), 1, 2));
            if !derived {
                base.push(change("t", y, ALIAS, Value::Ref(t1), 2, 0));
            }
            for i in 0..n {
                let z = wide(100 + i);
                base.push(change("d", z, "p", Value::Int(0), 1, i));
                base.push(change("d", z, ALIAS, Value::Ref(y), 2, i));
            }
            (
                schema,
                base,
                vec![change("t", t1, "v", Value::Int(1), 3, 0)],
            )
        });
    }
}

/// A chain of aliases arriving in one batch moves every gid of the chain, whose referrers are
/// each asked about once. At the largest `n`, asking about the referrers of every member of a
/// moved group for each of them is several times the bound.
#[test]
fn affected_and_targets_ask_for_the_referrers_of_each_moved_gid_once() {
    for n in [2_000, 300, 1] {
        let (schema, base, batch) = shape(1, n);
        affected_within_bound(&schema, &base, &batch).unwrap();
    }
}

/// The gid a key of the edited entity derives through a merged group is kept in `d`, and holds
/// `n` entries in the key's collection. It cannot hold the key there, so none of them is read.
#[test]
fn affected_and_targets_read_no_entries_of_a_derived_gid_kept_elsewhere() {
    flat(|n| {
        let schema = Schema::new(vec![
            Collection::minted("r").plain("p", Merge::Lww),
            Collection::minted("d").plain("p", Merge::Lww),
            Collection::natural("k", &["up", "p0"])
                .reference("up", "r")
                .plain("p0", Merge::Lww)
                .plain("v", Merge::Lww),
        ]);
        let (r1, r2) = (wide(1), wide(2));
        let key = [Value::Ref(r1), Value::Int(1)];
        let e = Gid::derive("k", &key);
        let z = Gid::derive("k", &[Value::Ref(r2), Value::Int(1)]);
        let mut base = vec![
            change("r", r1, "p", Value::Int(0), 1, 0),
            change("r", r2, "p", Value::Int(0), 1, 1),
            change("r", r2, ALIAS, Value::Ref(r1), 2, 0),
            change("k", e, "up", key[0].clone(), 1, 0),
            change("k", e, "p0", key[1].clone(), 1, 1),
            change("d", z, "p", Value::Int(0), 0, 0),
        ];
        for i in 0..n {
            base.push(change("k", z, &format!("f{i}"), Value::Int(1), 1, i));
        }
        (schema, base, vec![change("k", e, "v", Value::Int(1), 3, 0)])
    });
}

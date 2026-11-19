//! The work the evaluator and the planner do is bounded by what they are given and what they
//! answer, whatever the input: random schemas and changes, and shapes made to be expensive.
//!
//! Work is what the source is asked (one per question and one per item returned) plus what the
//! `work-counters` feature counts: rows fetched into a region, groups moved by a join, keys filed
//! again, questions asked, members read to decide liveness and rows, gids taken off `affected`'s
//! queue and member lists compared, and in `order` rows taken, searched for cycles and tried for
//! parking, and planning rounds.
//!
//! - `affected` over a batch of B changes, then `target` of every pair it reports: work is at
//!   most 512 + 8 · E · log2(E + 2) · max(D, 1)², where E is B plus the gids the changes name and
//!   the pairs reported, plus the members of each distinct group these fall in, before and after,
//!   and D is the most natural collections a chain of natural keys passes through (its cost grows
//!   about as the square of that depth).
//! - `order` over n rows: at most 128 + 4 · (n + 1) · F · log2(n + 2), F being one plus the most
//!   reference and unique-set fields a collection has.
//!
//! `FONGO_WORK_CASES` sets how many random cases run (default 200).

#![cfg(feature = "work-counters")]

mod common;

use std::collections::BTreeSet;

use common::{Counting, Rng, random_changes, random_schema};
use fongo_sync_core::eval::Evaluator;
use fongo_sync_core::model::{ALIAS, Collection, FieldKind, Identity, Row, Schema};
use fongo_sync_core::plan::{Columns, Planned, RowAction, order};
use fongo_sync_core::source::{Body, Store, admit};
use fongo_sync_core::{Change, Gid, Hlc, Strategy as Merge, Value, work};
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

/// Runs `affected` over `batch` and `target` of everything it names, and checks the work.
fn affected_within_bound(schema: &Schema, base: &[Change], batch: &[Change]) -> Result<(), String> {
    let ev = Evaluator::new(schema).unwrap();
    let before = store(schema, base);
    let mut after = before.clone();
    for c in batch {
        after.merge(schema, c);
    }
    work::take();
    let counting = Counting::new(&before);
    let Ok(found) = ev.affected(&counting, batch) else {
        return Ok(());
    };
    let mut done = counting.work.get() + work::take();
    let counting = Counting::new(&after);
    let mut session = ev.session(&counting);
    for (c, g) in &found {
        if session.target(c, *g).is_err() {
            return Ok(());
        }
    }
    done += counting.work.get() + work::take();

    // The groups of what the batch names and of what `affected` reports, before and after.
    let mut named: BTreeSet<(String, Gid)> = found.clone();
    for c in batch {
        if let Some(fact) = admit(schema, c) {
            named.insert((fact.collection.clone(), fact.gid));
            if let Body::Alias { target } = fact.body {
                named.insert((fact.collection, target));
            }
        }
    }
    let mut extent = batch.len() + named.len();
    let (mut was, mut will) = (ev.session(&before), ev.session(&after));
    let mut groups = BTreeSet::new();
    for (c, g) in &named {
        for (when, s) in [(0, &mut was), (1, &mut will)] {
            if let Ok(t) = s.target(c, *g)
                && groups.insert((when, c.clone(), t.canonical))
            {
                extent += t.members.len();
            }
        }
    }
    work::take();
    let depth = chain(schema) as f64;
    let grows = extent as f64 * log2(extent as f64) * depth.max(1.0).powi(2);
    let bound = FIXED + SLOPE * grows;
    if std::env::var_os("FONGO_WORK_RATIOS").is_some() {
        eprintln!("ratio {:.3} extent {extent}", done as f64 / grows);
    }
    if done as f64 > bound {
        return Err(format!(
            "affected and targets: work {done} for extent {extent}, keys chained {} deep (bound {bound:.0})",
            chain(schema)
        ));
    }
    Ok(())
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
    let fields = schema
        .collections
        .iter()
        .map(|c| {
            let refs = c
                .fields
                .iter()
                .filter(|f| f.kind != FieldKind::Plain)
                .count();
            let natural = usize::from(matches!(c.identity, Identity::Natural(_)));
            1 + refs + c.identity_keys.len() + natural
        })
        .max()
        .unwrap_or(1);
    let n = planned.len() as f64;
    let grows = (n + 1.0) * fields as f64 * log2(n);
    let bound = FIXED / 4.0 + SLOPE / 2.0 * grows;
    if std::env::var_os("FONGO_WORK_RATIOS").is_some() {
        eprintln!(
            "order ratio {:.3} rows {}",
            done as f64 / grows,
            planned.len()
        );
    }
    if done as f64 > bound {
        return Err(format!(
            "order: work {done} for {} rows (bound {bound:.0})",
            planned.len()
        ));
    }
    Ok(())
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
            .plain("label", Merge::Lww),
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
    match kind % 7 {
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

/// Rows to order, made to be expensive, `n` setting the size.
fn order_shape(kind: u8, n: u32, not_null: bool) -> (Schema, Columns, Vec<Planned>) {
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
    let schema = random_schema(rng);
    let mut columns = Columns::new();
    for c in &schema.collections {
        for f in &c.fields {
            if rng.one_in(4) {
                columns = columns.not_null(&c.name, &f.name);
            }
        }
    }
    let gids: Vec<Gid> = (0..8).map(|i| wide(i as u32 + 1)).collect();
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
    #![proptest_config(ProptestConfig::with_cases(cases()))]

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
        if let Err(e) = affected_within_bound(&schema, &base, &batch) {
            prop_assert!(false, "shape {}: {e}", kind % 7);
        }
    }

    #[test]
    fn order_works_within_its_bound(
        kind in any::<u8>(),
        n in 1u32..1_500,
        not_null in any::<bool>(),
        seed in any::<u64>(),
    ) {
        let (schema, columns, planned) = if kind % 4 == 3 {
            random_plan(&mut Rng(seed | 1), n as usize % 60)
        } else {
            order_shape(kind, n, not_null)
        };
        if let Err(e) = order_within_bound(&schema, &columns, &planned) {
            prop_assert!(false, "shape {}: {e}", kind % 4);
        }
    }
}

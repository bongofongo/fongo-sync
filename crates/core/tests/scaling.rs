//! How work grows with the size of the data. Each check counts the work one operation does at
//! two sizes, four times apart, and bounds the ratio: linear work grows about four times,
//! quadratic sixteen. Work is counted, not timed, so the checks do not depend on the machine:
//! for the evaluator, the questions it asks of the source and the items they return, and with the
//! `work-counters` feature what `work` counts as well; for `plan::order`, what `work` counts.

use std::cell::Cell;

use fongo_sync_core::eval::Evaluator;
use fongo_sync_core::model::{ALIAS, Collection, Schema};
use fongo_sync_core::source::{Alias, Claim, Entry, Source, Store};
use fongo_sync_core::{Change, Gid, Hlc, Strategy as Merge, Value};

/// A source that counts what is asked of it: one for each question and one for each item an
/// answer holds.
struct Counting<'a> {
    inner: &'a Store,
    work: Cell<usize>,
}

impl<'a> Counting<'a> {
    fn new(inner: &'a Store) -> Counting<'a> {
        Counting {
            inner,
            work: Cell::new(0),
        }
    }

    fn count<T>(&self, answer: Vec<T>) -> Vec<T> {
        self.work.set(self.work.get() + 1 + answer.len());
        answer
    }
}

impl Source for Counting<'_> {
    fn claims(&self, gid: Gid) -> Vec<Claim> {
        self.count(self.inner.claims(gid))
    }

    fn entries(&self, collection: &str, gid: Gid) -> Vec<Entry> {
        self.count(self.inner.entries(collection, gid))
    }

    fn aliases(&self, gid: Gid) -> Vec<Alias> {
        self.count(self.inner.aliases(gid))
    }

    fn holding(&self, collection: &str, fields: &[(&str, &Value)]) -> Vec<Gid> {
        self.count(self.inner.holding(collection, fields))
    }

    fn gids(&self) -> Vec<Gid> {
        self.count(self.inner.gids())
    }
}

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

fn schema() -> Schema {
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

fn store(schema: &Schema, changes: &[Change]) -> Store {
    let mut store = Store::default();
    for c in changes {
        store.merge(schema, c);
    }
    store
}

/// The work `affected` does over `batch`: what it asks of the source, and with the
/// `work-counters` feature the gids it queues.
fn affected_work(schema: &Schema, before: &Store, batch: &[Change]) -> usize {
    let counting = Counting::new(before);
    queued();
    Evaluator::new(schema)
        .unwrap()
        .affected(&counting, batch)
        .unwrap();
    counting.work.get() + queued()
}

#[cfg(feature = "work-counters")]
fn queued() -> usize {
    fongo_sync_core::work::take()
}

#[cfg(not(feature = "work-counters"))]
fn queued() -> usize {
    0
}

fn target_work(schema: &Schema, source: &Store, collection: &str, gid: Gid) -> usize {
    let counting = Counting::new(source);
    Evaluator::new(schema)
        .unwrap()
        .target(&counting, collection, gid)
        .unwrap();
    counting.work.get()
}

fn grows(what: &str, small: usize, large: usize, bound: f64) {
    let ratio = large as f64 / small.max(1) as f64;
    assert!(
        ratio < bound,
        "{what}: work {small} at one size, {large} at four times it (x{ratio:.1})"
    );
}

/// One site owning `n` items.
fn owner_of(schema: &Schema, n: u32) -> Vec<Change> {
    let mut changes = vec![change("sites", wide(1), "name", Value::Int(0), 1, 0)];
    for i in 0..n {
        changes.push(change(
            "items",
            wide(100 + i),
            "site",
            Value::Ref(wide(1)),
            1,
            i,
        ));
        changes.push(change(
            "items",
            wide(100 + i),
            "text",
            Value::Int(i.into()),
            1,
            i,
        ));
    }
    let _ = schema;
    changes
}

/// `n` items aliased into one group, each with one tag of the same label, so the tags are one
/// natural group too.
fn merged_group(n: u32) -> Vec<Change> {
    let mut changes = vec![change("sites", wide(1), "name", Value::Int(0), 1, 0)];
    for i in 0..n {
        let item = wide(100 + i);
        changes.push(change("items", item, "site", Value::Ref(wide(1)), 1, i));
        if i > 0 {
            changes.push(change("items", item, ALIAS, Value::Ref(wide(99 + i)), 1, i));
        }
        let key = [Value::Ref(item), Value::Int(0)];
        let tag = Gid::derive("tags", &key);
        changes.push(change("tags", tag, "item", key[0].clone(), 1, i));
        changes.push(change("tags", tag, "label", key[1].clone(), 1, i));
    }
    changes
}

#[test]
fn affected_grows_with_the_batch() {
    let schema = schema();
    let before = store(&schema, &owner_of(&schema, 4_000));
    let edits = |n: u32| -> Vec<Change> {
        (0..n)
            .map(|i| change("items", wide(100 + i), "text", Value::Int(-1), 2, i))
            .collect()
    };
    let (small, large) = (
        affected_work(&schema, &before, &edits(250)),
        affected_work(&schema, &before, &edits(1_000)),
    );
    grows("affected by batch size", small, large, 6.0);
}

#[test]
fn renaming_an_owner_costs_the_same_however_many_rows_it_owns() {
    let schema = schema();
    let rename = [change("sites", wide(1), "name", Value::Int(1), 2, 0)];
    let (fewer, more) = (
        store(&schema, &owner_of(&schema, 2_000)),
        store(&schema, &owner_of(&schema, 8_000)),
    );
    let ev = Evaluator::new(&schema).unwrap();
    assert_eq!(ev.affected(&more, &rename).unwrap().len(), 1);
    let (small, large) = (
        affected_work(&schema, &fewer, &rename),
        affected_work(&schema, &more, &rename),
    );
    grows("renaming an owner", small, large, 1.5);
}

#[test]
fn a_merged_group_with_natural_children_grows_linearly() {
    let schema = schema();
    let (smaller, larger) = (
        store(&schema, &merged_group(150)),
        store(&schema, &merged_group(600)),
    );
    let tag = Gid::derive("tags", &[Value::Ref(wide(120)), Value::Int(0)]);
    let ev = Evaluator::new(&schema).unwrap();
    assert_eq!(ev.target(&larger, "tags", tag).unwrap().members.len(), 600);
    grows(
        "a natural child of a merged group",
        target_work(&schema, &smaller, "tags", tag),
        target_work(&schema, &larger, "tags", tag),
        6.0,
    );

    let edit_all = |n: u32| -> Vec<Change> {
        (0..n)
            .map(|i| change("items", wide(100 + i), "text", Value::Int(i.into()), 2, i))
            .collect()
    };
    grows(
        "editing every member of a merged group",
        affected_work(&schema, &smaller, &edit_all(150)),
        affected_work(&schema, &larger, &edit_all(600)),
        6.0,
    );

    let join = |n: u32| -> Vec<Change> {
        let item = wide(100 + n);
        let key = [Value::Ref(item), Value::Int(0)];
        let tag = Gid::derive("tags", &key);
        vec![
            change("items", item, "site", Value::Ref(wide(1)), 3, 0),
            change("items", item, ALIAS, Value::Ref(wide(100)), 3, 1),
            change("tags", tag, "item", key[0].clone(), 3, 2),
            change("tags", tag, "label", key[1].clone(), 3, 3),
        ]
    };
    grows(
        "a new member joining a merged group with natural children",
        affected_work(&schema, &smaller, &join(150)),
        affected_work(&schema, &larger, &join(600)),
        6.0,
    );
}

#[test]
fn many_new_natural_children_of_one_parent_grow_linearly() {
    let schema = schema();
    let before = store(&schema, &owner_of(&schema, 1));
    let tags = |k: u32| -> Vec<Change> {
        (0..k)
            .flat_map(|i| {
                let key = [Value::Ref(wide(100)), Value::Int(i.into())];
                let tag = Gid::derive("tags", &key);
                [
                    change("tags", tag, "item", key[0].clone(), 2, i),
                    change("tags", tag, "label", key[1].clone(), 2, i),
                ]
            })
            .collect()
    };
    let ev = Evaluator::new(&schema).unwrap();
    assert_eq!(ev.affected(&before, &tags(400)).unwrap().len(), 400);
    grows(
        "new natural children of one parent",
        affected_work(&schema, &before, &tags(400)),
        affected_work(&schema, &before, &tags(1_600)),
        6.0,
    );
}

#[test]
fn a_chain_of_aliases_in_one_batch_grows_linearly() {
    let schema = schema();
    let before = store(&schema, &owner_of(&schema, 1));
    let chain = |k: u32| -> Vec<Change> {
        (0..k)
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
            .collect()
    };
    grows(
        "a chain of aliases in one batch",
        affected_work(&schema, &before, &chain(400)),
        affected_work(&schema, &before, &chain(1_600)),
        6.0,
    );
}

/// Natural keys through `depth` collections, each keyed on its parent in the one before: legal,
/// since the chain does not come round. Two copies of `width` such paths under two roots are
/// joined by aliasing the roots.
fn chain_schema(depth: usize) -> Schema {
    let mut collections = vec![Collection::minted("c0").plain("name", Merge::Lww)];
    for d in 1..=depth {
        collections.push(
            Collection::natural(&format!("c{d}"), &["up", "name"])
                .owner("up", &format!("c{}", d - 1))
                .plain("name", Merge::Lww),
        );
    }
    let schema = Schema::new(collections);
    schema.check().unwrap();
    schema
}

fn two_copies(depth: usize, width: u32) -> (Vec<Change>, Gid) {
    let mut changes = Vec::new();
    let mut deepest = wide(0);
    for root in [wide(1), wide(2)] {
        changes.push(change("c0", root, "name", Value::Int(0), 1, 0));
        for w in 0..width {
            let mut up = root;
            for d in 1..=depth {
                let key = [Value::Ref(up), Value::Int(w.into())];
                let c = format!("c{d}");
                let gid = Gid::derive(&c, &key);
                changes.push(change(&c, gid, "up", key[0].clone(), 1, w));
                changes.push(change(&c, gid, "name", key[1].clone(), 1, w));
                up = gid;
            }
            deepest = up;
        }
    }
    (changes, deepest)
}

#[test]
fn joining_two_copies_of_a_legal_chain_of_natural_keys_grows_linearly_in_width() {
    let join = [change("c0", wide(2), ALIAS, Value::Ref(wide(1)), 2, 0)];
    for depth in [3, 6] {
        let schema = chain_schema(depth);
        let (narrow, _) = two_copies(depth, 50);
        let (wide_copies, deepest) = two_copies(depth, 200);
        let (narrow, wide_copies) = (store(&schema, &narrow), store(&schema, &wide_copies));
        let ev = Evaluator::new(&schema).unwrap();
        let after = {
            let mut s = wide_copies.clone();
            s.merge(&schema, &join[0]);
            s
        };
        let deep = format!("c{depth}");
        assert_eq!(ev.target(&after, &deep, deepest).unwrap().members.len(), 2);
        grows(
            &format!("joining two copies {depth} deep"),
            affected_work(&schema, &narrow, &join),
            affected_work(&schema, &wide_copies, &join),
            6.0,
        );
    }
    // Each entity's group is found from the keys above it, so the work grows with the square
    // of the depth: the number of collections the chain passes through, which the schema fixes.
    let (shallow, deep) = (chain_schema(3), chain_schema(12));
    let work = |schema: &Schema, depth: usize| {
        let (copies, _) = two_copies(depth, 50);
        affected_work(schema, &store(schema, &copies), &join)
    };
    grows(
        "joining deeper copies",
        work(&shallow, 3),
        work(&deep, 12),
        20.0,
    );
}

#[cfg(feature = "work-counters")]
#[test]
fn ordering_many_independent_swaps_grows_linearly() {
    use fongo_sync_core::model::Row;
    use fongo_sync_core::plan::{Columns, Planned, RowAction, order};
    use fongo_sync_core::work;
    let schema = Schema::new(vec![
        Collection::minted("u")
            .plain("name", Merge::Lww)
            .identity_key(&["name"]),
    ]);
    let swaps = |k: u32| -> Vec<Planned> {
        let row = |v: u32| -> Row { [("name".to_string(), Value::Int(v.into()))].into() };
        (0..k)
            .flat_map(|i| {
                [
                    Planned {
                        collection: "u".into(),
                        gid: wide(2 * i),
                        image: Some(row(2 * i)),
                        action: RowAction::Update(row(2 * i + 1)),
                    },
                    Planned {
                        collection: "u".into(),
                        gid: wide(2 * i + 1),
                        image: Some(row(2 * i + 1)),
                        action: RowAction::Update(row(2 * i)),
                    },
                ]
            })
            .collect()
    };
    let work_for = |k: u32| {
        work::take();
        let plan = order(&schema, &Columns::new(), &swaps(k));
        assert_eq!(plan.steps.len(), 3 * k as usize);
        work::take()
    };
    grows(
        "ordering independent swaps",
        work_for(250),
        work_for(1_000),
        6.0,
    );
}

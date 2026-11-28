mod common;

use std::collections::BTreeMap;

use common::{arb_case, arb_random_case};
use fongo_sync_core::eval::Evaluator;
use fongo_sync_core::model::{ALIAS, Collection, Row, Schema, View, view};
use fongo_sync_core::source::Store;
use fongo_sync_core::{Change, Gid, Hlc, Strategy as Merge, Value};
use proptest::prelude::*;

/// Changes, and a delivery of them: every change at least once, in a random order, some again,
/// cut into batches of random sizes.
fn arb_delivery(
    case: impl Strategy<Value = (Schema, Vec<Change>)>,
) -> impl Strategy<Value = (Schema, Vec<Change>, Vec<Vec<Change>>)> {
    case.prop_flat_map(|(schema, changes)| {
        let n = changes.len();
        let order: Vec<usize> = (0..n).collect();
        (
            Just(schema),
            Just(changes),
            Just(order).prop_shuffle(),
            proptest::collection::vec(
                (any::<prop::sample::Index>(), any::<prop::sample::Index>()),
                0..10,
            ),
            proptest::collection::vec(1usize..8, 1..60),
        )
            .prop_map(|(schema, changes, mut order, repeats, sizes)| {
                let n = changes.len();
                if n > 0 {
                    for (at, which) in repeats {
                        order.insert(at.index(order.len() + 1), which.index(n));
                    }
                }
                let mut batches = Vec::new();
                let mut rest = &order[..];
                let mut sizes = sizes.into_iter().cycle();
                while !rest.is_empty() {
                    let take = sizes.next().unwrap().min(rest.len());
                    batches.push(rest[..take].iter().map(|&i| changes[i].clone()).collect());
                    rest = &rest[take..];
                }
                (schema, changes, batches)
            })
    })
}

fn as_view(rows: &BTreeMap<(String, Gid), Row>) -> View {
    let mut v = View::default();
    for ((c, g), row) in rows {
        v.insert(c, *g, row.clone());
    }
    v
}

fn from_scratch(ev: &Evaluator, store: &Store) -> View {
    let mut session = ev.session(store);
    let mut v = View::default();
    for (c, g) in ev.everything(store) {
        let t = session.target(&c, g).unwrap();
        if let Some(row) = t.row
            && t.canonical == g
        {
            v.insert(&c, g, row);
        }
    }
    v
}

proptest! {
    #![proptest_config(common::config(3000))]

    /// The incremental law: re-evaluating only what `affected` names after each batch keeps the
    /// materialised rows equal to evaluating everything, and to `model::view`.
    #[test]
    fn re_evaluating_only_the_affected_entities_keeps_every_row_right(
        (schema, changes, batches) in arb_delivery(arb_case(0..50))
    ) {
        incremental(&schema, &changes, &batches)?;
    }

    #[test]
    fn the_incremental_law_holds_over_random_schemas(
        (schema, changes, batches) in arb_delivery(arb_random_case(70))
    ) {
        incremental(&schema, &changes, &batches)?;
    }
}

fn incremental(
    schema: &Schema,
    changes: &[Change],
    batches: &[Vec<Change>],
) -> Result<(), TestCaseError> {
    let ev = Evaluator::new(schema).unwrap();
    let mut store = Store::default();
    let mut rows: BTreeMap<(String, Gid), Row> = BTreeMap::new();
    let mut merged: Vec<Change> = Vec::new();
    for batch in batches {
        let affected = ev.affected(&store, batch).unwrap();
        for c in batch {
            store.merge(schema, c);
        }
        merged.extend(batch.iter().cloned());
        let mut session = ev.session(&store);
        for (c, g) in affected {
            let t = session.target(&c, g).unwrap();
            match t.row {
                Some(row) if t.canonical == g => {
                    rows.insert((c, g), row);
                }
                _ => {
                    rows.remove(&(c, g));
                }
            }
        }
        let expected = view(schema, &merged);
        prop_assert_eq!(&as_view(&rows), &expected);
    }
    prop_assert_eq!(from_scratch(&ev, &store), view(schema, changes));
    prop_assert_eq!(as_view(&rows), view(schema, changes));
    Ok(())
}

fn gid(n: u32) -> Gid {
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

fn two_collections() -> Schema {
    Schema::new(vec![
        Collection::minted("a").plain("p", Merge::Lww),
        Collection::minted("d").plain("p", Merge::Lww),
    ])
}

/// The gids `view` shows in `collection`.
fn shown(schema: &Schema, changes: &[Change], collection: &str) -> Vec<Gid> {
    view(schema, changes)
        .collections
        .get(collection)
        .map(|rows| rows.keys().copied().collect())
        .unwrap_or_default()
}

/// `y`, kept in `d` and merged there with `n` others, writes an alias to `x` in `a`, which joins
/// nothing until an earlier change of `y` in `a` arrives and keeps it in `a`; `x` is edited
/// before that change arrives, after it, or with it. After each batch the rows are the model's.
#[test]
fn an_alias_joins_once_an_earlier_claim_keeps_its_writer_in_the_alias_collection() {
    let schema = two_collections();
    let (x, y) = (gid(1), gid(2));
    for n in [0, 3] {
        let mut base = vec![
            change("a", x, "p", Value::Int(0), 1, 0),
            change("d", y, "p", Value::Int(0), 2, 0),
            change("a", y, ALIAS, Value::Ref(x), 3, 0),
        ];
        for i in 0..n {
            let prev = if i == 0 { y } else { gid(99 + i) };
            base.push(change("d", gid(100 + i), "p", Value::Int(0), 2, i));
            base.push(change("d", gid(100 + i), ALIAS, Value::Ref(prev), 2, i));
        }
        let edit = change("a", x, "p", Value::Int(1), 4, 0);
        let earlier = change("a", y, "p", Value::Int(2), 1, 0);
        let mut all = base.clone();
        all.extend([edit.clone(), earlier.clone()]);
        assert_eq!(shown(&schema, &base, "a"), vec![x]);
        assert_eq!(shown(&schema, &base, "d"), vec![y]);
        assert_eq!(shown(&schema, &all, "a"), vec![x]);
        assert!(shown(&schema, &all, "d").iter().all(|&g| g != y));
        let orders = [
            vec![base.clone(), vec![edit.clone()], vec![earlier.clone()]],
            vec![base.clone(), vec![earlier.clone()], vec![edit.clone()]],
            vec![base.clone(), vec![edit.clone(), earlier.clone()]],
            vec![base.clone(), vec![earlier.clone(), edit.clone()]],
        ];
        for batches in orders {
            incremental(&schema, &all, &batches).unwrap();
        }
    }
}

/// `y`, kept in `d`, writes aliases in `a` to `x` and to `z`, both kept in `a`: neither alias
/// joins, so `x` and `z` stay apart, and editing any of the three keeps the model's rows.
#[test]
fn a_gid_kept_elsewhere_joins_no_two_gids_through_its_aliases() {
    let schema = two_collections();
    let (x, y, z) = (gid(1), gid(2), gid(3));
    let base = vec![
        change("d", y, "p", Value::Int(0), 1, 0),
        change("a", x, "p", Value::Int(0), 2, 0),
        change("a", z, "p", Value::Int(0), 2, 1),
        change("a", y, ALIAS, Value::Ref(x), 3, 0),
        change("a", y, ALIAS, Value::Ref(z), 3, 1),
        change("a", z, ALIAS, Value::Ref(y), 3, 2),
    ];
    assert_eq!(shown(&schema, &base, "a"), vec![x, z]);
    for edited in [
        change("a", x, "p", Value::Int(1), 4, 0),
        change("a", z, "p", Value::Int(1), 4, 0),
        change("d", y, "p", Value::Int(1), 4, 0),
        change("a", y, "p", Value::Int(1), 4, 0),
    ] {
        let mut all = base.clone();
        all.push(edited.clone());
        incremental(&schema, &all, &[base.clone(), vec![edited]]).unwrap();
    }
}

/// Natural entities keyed on references to gids kept in another collection than the field
/// names: the keys are read through those gids' groups wherever they are kept, so merging the
/// referred gids in `d` joins the entities, and the rows follow the model.
#[test]
fn a_natural_key_reads_the_group_of_a_gid_kept_elsewhere() {
    let schema = Schema::new(vec![
        Collection::minted("a").plain("p", Merge::Lww),
        Collection::minted("d").plain("p", Merge::Lww),
        Collection::natural("n", &["r"])
            .reference("r", "a")
            .plain("v", Merge::Lww),
    ]);
    let (y1, y2) = (gid(1), gid(2));
    let keyed = |y: Gid, wall_ms: u64| {
        let g = Gid::derive("n", &[Value::Ref(y)]);
        [
            change("n", g, "r", Value::Ref(y), wall_ms, 0),
            change("n", g, "v", Value::Int(wall_ms as i64), wall_ms, 1),
        ]
    };
    let mut base = vec![
        change("d", y1, "p", Value::Int(0), 1, 0),
        change("d", y2, "p", Value::Int(0), 1, 1),
    ];
    base.extend(keyed(y1, 2));
    base.extend(keyed(y2, 3));
    let merge = change("d", y2, ALIAS, Value::Ref(y1), 4, 0);
    let mut all = base.clone();
    all.push(merge.clone());
    assert_eq!(shown(&schema, &base, "n").len(), 2);
    assert_eq!(shown(&schema, &all, "n").len(), 1);
    incremental(&schema, &all, &[base.clone(), vec![merge.clone()]]).unwrap();
    incremental(&schema, &all, &[vec![merge], base]).unwrap();
}

/// `z` first holds the key `t1` holds without deriving from it, so it joins nothing; then the key
/// that derives `z` arrives, naming a gid that is merged with the one `t1`'s key names, and the
/// two entities join. In every order of the key, the merge and an edit of `t1`, and all in one
/// batch, the rows after each batch are the model's.
#[test]
fn a_gid_holding_a_key_it_does_not_derive_joins_once_its_own_key_arrives() {
    let schema = Schema::new(vec![
        Collection::minted("a").plain("p", Merge::Lww),
        Collection::natural("t", &["up", "k"])
            .reference("up", "a")
            .plain("k", Merge::Lww)
            .plain("v", Merge::Lww),
    ]);
    let (x, x2) = (gid(1), gid(2));
    let t1 = Gid::derive("t", &[Value::Ref(x), Value::Int(0)]);
    let z = Gid::derive("t", &[Value::Ref(x2), Value::Int(0)]);
    let base = vec![
        change("a", x, "p", Value::Int(0), 1, 0),
        change("a", x2, "p", Value::Int(0), 1, 1),
        change("t", t1, "up", Value::Ref(x), 1, 0),
        change("t", t1, "k", Value::Int(0), 1, 0),
        change("t", z, "up", Value::Ref(x), 1, 1),
        change("t", z, "k", Value::Int(0), 1, 1),
        change("t", z, "v", Value::Int(7), 1, 1),
    ];
    let key = vec![change("t", z, "up", Value::Ref(x2), 2, 0)];
    let merge = vec![change("a", x2, ALIAS, Value::Ref(x), 3, 0)];
    let edit = vec![change("t", t1, "v", Value::Int(1), 4, 0)];
    let mut all = base.clone();
    all.extend(key.iter().chain(&merge).chain(&edit).cloned());
    assert_eq!(shown(&schema, &base, "t"), vec![t1]);
    assert_eq!(shown(&schema, &all, "t").len(), 1);
    let parts = [key, merge, edit];
    for order in [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ] {
        let mut batches = vec![base.clone()];
        batches.extend(order.iter().map(|&i| parts[i].clone()));
        incremental(&schema, &all, &batches).unwrap();
    }
    let mut together = base.clone();
    together.extend(parts.iter().flatten().cloned());
    incremental(
        &schema,
        &all,
        &[base.clone(), together[base.len()..].to_vec()],
    )
    .unwrap();
}

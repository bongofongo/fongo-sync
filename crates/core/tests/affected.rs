mod common;

use std::collections::BTreeMap;

use common::{arb_case, arb_random_case};
use fongo_sync_core::eval::Evaluator;
use fongo_sync_core::model::{Row, Schema, View, view};
use fongo_sync_core::source::Store;
use fongo_sync_core::{Change, Gid};
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
    #![proptest_config(ProptestConfig::with_cases(3000))]

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

//! How work grows with the size of the data. Each check times one operation at two sizes, four
//! times apart, and bounds the ratio: linear work grows about four times, quadratic sixteen.
//! Times are the fastest of several runs, and the checks run one after another in one test.

use std::time::{Duration, Instant};

use fongo_sync_core::eval::Evaluator;
use fongo_sync_core::model::{ALIAS, Collection, Schema};
use fongo_sync_core::source::Store;
use fongo_sync_core::{Change, Gid, Hlc, Strategy as Merge, Value};

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

fn fastest(mut run: impl FnMut()) -> Duration {
    (0..3)
        .map(|_| {
            let started = Instant::now();
            run();
            started.elapsed()
        })
        .min()
        .unwrap()
}

/// One site owning `n` items.
fn owner_of(schema: &Schema, n: u32) -> Store {
    let mut store = Store::default();
    store.merge(
        schema,
        &change("sites", wide(1), "name", Value::Int(0), 1, 0),
    );
    for i in 0..n {
        let item = wide(100 + i);
        store.merge(
            schema,
            &change("items", item, "site", Value::Ref(wide(1)), 1, i),
        );
        store.merge(
            schema,
            &change("items", item, "text", Value::Int(i.into()), 1, i),
        );
    }
    store
}

/// `n` items aliased into one group, each with one tag of the same label, so the tags are one
/// natural group too.
fn merged_group(schema: &Schema, n: u32) -> Store {
    let mut store = Store::default();
    store.merge(
        schema,
        &change("sites", wide(1), "name", Value::Int(0), 1, 0),
    );
    for i in 0..n {
        let item = wide(100 + i);
        store.merge(
            schema,
            &change("items", item, "site", Value::Ref(wide(1)), 1, i),
        );
        if i > 0 {
            store.merge(
                schema,
                &change("items", item, ALIAS, Value::Ref(wide(99 + i)), 1, i),
            );
        }
        let key = [Value::Ref(item), Value::Int(0)];
        let tag = Gid::derive("tags", &key);
        store.merge(schema, &change("tags", tag, "item", key[0].clone(), 1, i));
        store.merge(schema, &change("tags", tag, "label", key[1].clone(), 1, i));
    }
    store
}

fn grows_linearly(what: &str, small: Duration, large: Duration) {
    let ratio = large.as_secs_f64() / small.as_secs_f64().max(1e-6);
    assert!(
        ratio < 9.0,
        "{what}: {small:?} at one size, {large:?} at four times it (x{ratio:.1})"
    );
}

#[test]
fn work_grows_with_what_changed_not_with_what_is_stored() {
    let schema = schema();
    let ev = Evaluator::new(&schema).unwrap();

    // A batch's affected set costs in proportion to the batch.
    let store = owner_of(&schema, 4_000);
    let edits = |n: u32| -> Vec<Change> {
        (0..n)
            .map(|i| change("items", wide(100 + i), "text", Value::Int(-1), 2, i))
            .collect()
    };
    let (few, many) = (edits(250), edits(1_000));
    assert_eq!(ev.affected(&store, &many).unwrap().len(), 1_000);
    let small = fastest(|| drop(ev.affected(&store, &few).unwrap()));
    let large = fastest(|| drop(ev.affected(&store, &many).unwrap()));
    grows_linearly("affected by batch size", small, large);

    // Renaming an owner affects the owner alone, however many rows it owns.
    let rename = [change("sites", wide(1), "name", Value::Int(1), 2, 0)];
    let (fewer, more) = (owner_of(&schema, 2_000), owner_of(&schema, 8_000));
    assert_eq!(ev.affected(&more, &rename).unwrap().len(), 1);
    let small = fastest(|| drop(ev.affected(&fewer, &rename).unwrap()));
    let large = fastest(|| drop(ev.affected(&more, &rename).unwrap()));
    let ratio = large.as_secs_f64() / small.as_secs_f64().max(1e-6);
    assert!(ratio < 3.0, "renaming an owner: {small:?} vs {large:?}");

    // A large merged group whose members each have a natural child.
    let (smaller, larger) = (merged_group(&schema, 150), merged_group(&schema, 600));
    let tag = Gid::derive("tags", &[Value::Ref(wide(120)), Value::Int(0)]);
    assert_eq!(ev.target(&larger, "tags", tag).unwrap().members.len(), 600);
    let small = fastest(|| drop(ev.target(&smaller, "tags", tag).unwrap()));
    let large = fastest(|| drop(ev.target(&larger, "tags", tag).unwrap()));
    grows_linearly("a natural child of a merged group", small, large);

    let edit_all = |n: u32| -> Vec<Change> {
        (0..n)
            .map(|i| change("items", wide(100 + i), "text", Value::Int(i.into()), 2, i))
            .collect()
    };
    let (few, many) = (edit_all(150), edit_all(600));
    let small = fastest(|| drop(ev.affected(&smaller, &few).unwrap()));
    let large = fastest(|| drop(ev.affected(&larger, &many).unwrap()));
    grows_linearly("editing every member of a merged group", small, large);

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
    assert!(ev.affected(&larger, &join(600)).unwrap().len() >= 2);
    let small = fastest(|| drop(ev.affected(&smaller, &join(150)).unwrap()));
    let large = fastest(|| drop(ev.affected(&larger, &join(600)).unwrap()));
    grows_linearly(
        "a new member joining a merged group with natural children",
        small,
        large,
    );
}

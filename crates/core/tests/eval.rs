mod common;

use common::{arb_case, arb_random_case, gid, mentioned};
use fongo_sync_core::eval::{Evaluator, Limits, Undetermined};
use fongo_sync_core::model::{ALIAS, Collection, Schema, View, view};
use fongo_sync_core::source::{Source, Store};
use fongo_sync_core::{Change, DELETED, Gid, Hlc, Strategy as Merge, Value};
use proptest::prelude::*;

fn store(schema: &Schema, changes: &[Change]) -> Store {
    let mut store = Store::default();
    for c in changes {
        store.merge(schema, c);
    }
    store
}

/// Asks about every mentioned gid in every collection, in one session, and checks each answer
/// against `model::view` and against a fresh session.
fn agrees(schema: &Schema, changes: &[Change]) -> Result<(), TestCaseError> {
    let expected = view(schema, changes);
    let store = store(schema, changes);
    let ev = Evaluator::new(schema).unwrap();
    let mut session = ev.session(&store);
    let mut shown = View::default();
    for c in &schema.collections {
        for g in mentioned(schema, changes) {
            let t = session.target(&c.name, g).expect("within limits");
            prop_assert_eq!(&t, &ev.target(&store, &c.name, g).unwrap());
            if t.members.is_empty() {
                prop_assert_eq!(expected.row(&c.name, g), None);
                continue;
            }
            prop_assert!(t.members.contains(&g));
            prop_assert_eq!(t.canonical, t.members[0]);
            prop_assert!(t.live.iter().all(|m| t.members.contains(m)));
            match &t.row {
                Some(row) => {
                    prop_assert_eq!(expected.row(&c.name, t.canonical), Some(row));
                    shown.insert(&c.name, t.canonical, row.clone());
                }
                None => prop_assert_eq!(expected.row(&c.name, t.canonical), None),
            }
        }
    }
    prop_assert_eq!(shown, expected);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3000))]

    #[test]
    fn the_evaluator_shows_what_the_model_shows((schema, changes) in arb_case(0..60)) {
        agrees(&schema, &changes)?;
    }

    #[test]
    fn the_evaluator_shows_what_the_model_shows_over_random_schemas(
        (schema, changes) in arb_random_case(80)
    ) {
        agrees(&schema, &changes)?;
    }

    #[test]
    fn an_answer_under_tight_limits_does_not_depend_on_what_was_asked_before(
        (schema, changes) in arb_case(0..40),
        region in 1usize..12,
        key_combinations in 0usize..3,
        order in any::<u64>(),
    ) {
        let store = store(&schema, &changes);
        let ev = Evaluator::new(&schema).unwrap().with_limits(Limits { region, key_combinations });
        let mut questions: Vec<(String, Gid)> = Vec::new();
        for c in &schema.collections {
            for g in mentioned(&schema, &changes) {
                questions.push((c.name.clone(), g));
            }
        }
        let n = questions.len();
        for i in 0..n {
            let j = (order.wrapping_mul(i as u64 + 1) % n as u64) as usize;
            questions.swap(i, j);
        }
        let mut shared = ev.session(&store);
        let full = Evaluator::new(&schema).unwrap();
        for (c, g) in &questions {
            let alone = ev.target(&store, c, *g);
            prop_assert_eq!(&shared.target(c, *g), &alone);
            if let Ok(t) = alone {
                prop_assert_eq!(t, full.target(&store, c, *g).unwrap());
            }
        }
    }
}

fn at(wall_ms: u64) -> Hlc {
    Hlc {
        wall_ms,
        counter: 0,
        node: gid(0),
    }
}

fn change(collection: &str, gid: Gid, field: &str, value: Value, wall_ms: u64) -> Change {
    Change {
        collection: collection.into(),
        gid,
        field: field.into(),
        value,
        clock: at(wall_ms),
    }
}

fn wide(n: u32) -> Gid {
    let mut bytes = [0u8; 16];
    bytes[12..].copy_from_slice(&n.to_be_bytes());
    Gid(bytes)
}

fn things() -> Schema {
    Schema::new(vec![
        Collection::minted("things").plain("name", Merge::Lww),
        Collection::minted("bits")
            .plain("name", Merge::Lww)
            .owner("thing", "things"),
    ])
}

#[test]
fn an_alias_chain_longer_than_the_region_limit_is_undetermined_from_every_end() {
    let schema = things();
    let mut changes = Vec::new();
    for i in 0..50u32 {
        changes.push(change("things", wide(i), "name", Value::Int(i.into()), 1));
        if i > 0 {
            changes.push(change("things", wide(i), ALIAS, Value::Ref(wide(i - 1)), 2));
        }
    }
    changes.push(change("bits", wide(1000), "thing", Value::Ref(wide(49)), 3));
    let store = store(&schema, &changes);
    let tight = Evaluator::new(&schema).unwrap().with_limits(Limits {
        region: 49,
        key_combinations: 1,
    });
    let mut session = tight.session(&store);
    for i in [0, 25, 49] {
        assert_eq!(session.target("things", wide(i)), Err(Undetermined));
    }
    assert_eq!(session.target("bits", wide(1000)), Err(Undetermined));

    let roomy = Evaluator::new(&schema).unwrap().with_limits(Limits {
        region: 50,
        key_combinations: 1,
    });
    let t = roomy.target(&store, "things", wide(25)).unwrap();
    assert_eq!(t.members.len(), 50);
    assert_eq!(t.canonical, wide(0));
    assert_eq!(
        roomy
            .target(&store, "bits", wide(1000))
            .unwrap()
            .row
            .unwrap()["thing"],
        Value::Ref(wide(0))
    );
}

#[test]
fn a_natural_entity_with_too_many_candidate_keys_is_undetermined_and_so_is_what_it_owns() {
    let schema = Schema::new(vec![
        Collection::natural("tags", &["a", "b"])
            .plain("a", Merge::Lww)
            .plain("b", Merge::Lww),
        Collection::minted("notes").owner("tag", "tags"),
    ]);
    let key = [Value::Int(1), Value::Int(2)];
    let tag = Gid::derive("tags", &key);
    let mut changes = vec![
        change("tags", tag, "a", key[0].clone(), 1),
        change("tags", tag, "b", key[1].clone(), 1),
        change("notes", gid(7), "tag", Value::Ref(tag), 2),
    ];
    for i in 10..15 {
        changes.push(change("tags", tag, "a", Value::Int(i), 3));
        changes.push(change("tags", tag, "b", Value::Int(i), 3));
    }
    let store = store(&schema, &changes);
    let limited = |n| {
        Evaluator::new(&schema).unwrap().with_limits(Limits {
            region: 100,
            key_combinations: n,
        })
    };
    // Each key field keeps its KEY_ENTRIES earliest writes: four values each, sixteen keys.
    assert_eq!(limited(15).target(&store, "tags", tag), Err(Undetermined));
    assert_eq!(
        limited(15).target(&store, "notes", gid(7)),
        Err(Undetermined)
    );
    assert!(
        limited(16)
            .target(&store, "notes", gid(7))
            .unwrap()
            .row
            .is_some()
    );
    agrees(&schema, &changes).unwrap();
}

#[test]
fn a_huge_group_and_an_owner_with_many_children_are_evaluated_without_quadratic_work() {
    let schema = things();
    let n = 20_000u32;
    let mut changes = Vec::new();
    for i in 0..n {
        changes.push(change("things", wide(i), "name", Value::Int(i.into()), 1));
        changes.push(change(
            "things",
            wide(i),
            ALIAS,
            Value::Ref(wide((i * 7919) % n)),
            2,
        ));
        changes.push(change("things", wide(i), ALIAS, Value::Ref(wide(i / 2)), 2));
        changes.push(change("bits", wide(n + i), "thing", Value::Ref(wide(3)), 3));
    }
    changes.push(change("things", wide(5), DELETED, Value::Int(1), 4));
    let store = store(&schema, &changes);
    let ev = Evaluator::new(&schema).unwrap();
    let started = std::time::Instant::now();
    let mut session = ev.session(&store);
    let t = session.target("things", wide(n - 1)).unwrap();
    assert_eq!(t.members.len(), n as usize);
    assert_eq!(t.live.len(), n as usize - 1);
    for i in 0..n {
        assert!(session.target("bits", wide(n + i)).unwrap().row.is_some());
    }
    assert!(
        started.elapsed().as_secs() < 20,
        "took {:?}",
        started.elapsed()
    );
}

#[test]
fn a_deep_owner_chain_is_followed_to_the_top() {
    let depth = 8;
    let mut collections = vec![Collection::minted("c0").plain("name", Merge::Lww)];
    for d in 1..depth {
        collections.push(
            Collection::minted(&format!("c{d}"))
                .plain("name", Merge::Lww)
                .owner("up", &format!("c{}", d - 1)),
        );
    }
    let schema = Schema::new(collections);
    let mut changes = vec![change("c0", wide(0), "name", Value::Int(0), 1)];
    for d in 1..depth {
        changes.push(change(
            &format!("c{d}"),
            wide(d),
            "up",
            Value::Ref(wide(d - 1)),
            2,
        ));
    }
    agrees(&schema, &changes).unwrap();
    let ev = Evaluator::new(&schema).unwrap();
    let last = format!("c{}", depth - 1);
    assert!(
        ev.target(&store(&schema, &changes), &last, wide(depth - 1))
            .unwrap()
            .row
            .is_some()
    );
    changes.push(change("c0", wide(0), DELETED, Value::Int(1), 3));
    agrees(&schema, &changes).unwrap();
    assert!(
        ev.target(&store(&schema, &changes), &last, wide(depth - 1))
            .unwrap()
            .row
            .is_none()
    );
}

fn tags() -> Schema {
    Schema::new(vec![
        Collection::natural("tags", &["label"])
            .plain("label", Merge::Lww)
            .plain("note", Merge::Lww),
        Collection::minted("notes").owner("tag", "tags"),
    ])
}

fn rewrites(tag: Gid, from: u64, n: i64) -> Vec<Change> {
    (0..n)
        .map(|i| Change {
            clock: Hlc {
                wall_ms: from,
                counter: i as u32,
                node: gid(66),
            },
            ..change("tags", tag, "label", Value::Int(i), 0)
        })
        .collect()
}

#[test]
fn hostile_rewrites_of_a_natural_key_neither_hide_the_entity_nor_grow_the_store() {
    let schema = tags();
    let key = [Value::Text("x".into())];
    let tag = Gid::derive("tags", &key);
    let mut changes = vec![
        change("tags", tag, "label", key[0].clone(), 1),
        change("notes", gid(7), "tag", Value::Ref(tag), 2),
    ];
    changes.extend(rewrites(tag, 5, 1_100));
    assert!(view(&schema, &changes).row("notes", gid(7)).is_some());
    agrees(&schema, &changes).unwrap();
    let mut reversed = changes.clone();
    reversed.reverse();
    let (forward, backward) = (store(&schema, &changes), store(&schema, &reversed));
    let kept = |s: &Store| -> Vec<(Value, Hlc)> {
        s.entries("tags", tag)
            .into_iter()
            .filter(|e| e.field == "label")
            .map(|e| (e.value, e.clock))
            .collect()
    };
    assert_eq!(kept(&forward).len(), fongo_sync_core::model::KEY_ENTRIES);
    assert_eq!(kept(&forward), kept(&backward));
    let ev = Evaluator::new(&schema).unwrap();
    assert!(ev.target(&forward, "notes", gid(7)).unwrap().row.is_some());
    let before = store(&schema, &changes[..2]);
    assert!(ev.affected(&before, &changes[2..]).is_ok());
}

#[test]
fn rewrites_older_than_the_creation_can_hide_an_entity_in_the_model_and_the_evaluator_alike() {
    let schema = tags();
    let key = [Value::Text("x".into())];
    let tag = Gid::derive("tags", &key);
    let mut changes = rewrites(tag, 0, fongo_sync_core::model::KEY_ENTRIES as i64);
    changes.push(change("tags", tag, "label", key[0].clone(), 1));
    assert!(view(&schema, &changes).row("tags", tag).is_none());
    agrees(&schema, &changes).unwrap();
}

#[test]
fn a_together_set_written_by_two_members_at_one_clock_comes_from_the_smaller() {
    let schema = Schema::new(vec![
        Collection::minted("sites")
            .plain("x", Merge::Lww)
            .plain("y", Merge::Lww)
            .together(&["x", "y"]),
    ]);
    let changes = vec![
        change("sites", gid(1), "x", Value::Int(1), 5),
        change("sites", gid(1), "y", Value::Int(1), 5),
        change("sites", gid(2), "x", Value::Int(2), 5),
        change("sites", gid(2), "y", Value::Int(2), 5),
        change("sites", gid(2), ALIAS, Value::Ref(gid(1)), 6),
    ];
    assert_eq!(
        view(&schema, &changes).row("sites", gid(1)).unwrap()["x"],
        Value::Int(1)
    );
    agrees(&schema, &changes).unwrap();
}

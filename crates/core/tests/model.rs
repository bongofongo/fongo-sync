use std::collections::BTreeMap;

use fongo_sync_core::model::FieldKind;
use fongo_sync_core::model::{ALIAS, Collection, Row, Schema, View, unmerged_identities, view};
use fongo_sync_core::{Change, DELETED, Gid, Hlc, State, Strategy as Merge, Value};
use proptest::prelude::*;

fn schema() -> Schema {
    let schema = Schema::new(vec![
        Collection::minted("works")
            .plain("title", Merge::Lww)
            .plain("rating", Merge::Max)
            .plain("isbn", Merge::Lww)
            .plain("lat", Merge::Lww)
            .plain("lon", Merge::Lww)
            .identity_key(&["isbn"])
            .together(&["lat", "lon"]),
        Collection::natural("editions", &["work", "format"])
            .owner("work", "works")
            .plain("format", Merge::Lww)
            .plain("price", Merge::Lww),
        Collection::natural("printings", &["edition", "number"])
            .owner("edition", "editions")
            .plain("number", Merge::Lww),
        Collection::minted("notes")
            .plain("body", Merge::Lww)
            .owner("work", "works")
            .reference("about", "works"),
        Collection::minted("folders")
            .plain("name", Merge::Lww)
            .owner("parent", "folders"),
    ]);
    schema.check().unwrap();
    schema
}

fn gid(n: u8) -> Gid {
    Gid([n; 16])
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

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

fn row(pairs: &[(&str, Value)]) -> Row {
    pairs
        .iter()
        .map(|(f, v)| (f.to_string(), v.clone()))
        .collect()
}

fn work(title: Value, rating: Value, isbn: Value, lat: Value, lon: Value) -> Row {
    row(&[
        ("title", title),
        ("rating", rating),
        ("isbn", isbn),
        ("lat", lat),
        ("lon", lon),
    ])
}

fn titled(title: &str) -> Row {
    work(
        text(title),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
    )
}

fn only(collection: &str, rows: &[(Gid, Row)]) -> View {
    let mut view = View::default();
    for (gid, row) in rows {
        view.insert(collection, *gid, row.clone());
    }
    view
}

fn edition_gid(work: Gid, format: &str) -> Gid {
    Gid::derive("editions", &[Value::Ref(work), text(format)])
}

fn edition(gid: Gid, work: Gid, format: &str, wall_ms: u64) -> Vec<Change> {
    vec![
        change("editions", gid, "work", Value::Ref(work), wall_ms),
        change("editions", gid, "format", text(format), wall_ms + 1),
    ]
}

// --- Properties -----------------------------------------------------------------------------

const FIELDS: [(&str, &[&str]); 6] = [
    ("works", &["title", "rating", "isbn", "lat", "lon"]),
    ("editions", &["work", "format", "price"]),
    ("printings", &["edition", "number"]),
    ("notes", &["body", "work", "about"]),
    ("folders", &["name", "parent"]),
    ("unknown", &["title"]),
];

const REFERABLE: [u8; 10] = [1, 2, 3, 4, 11, 12, 21, 41, 42, 43];

fn arb_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        1 => Just(Value::Null),
        2 => (0i64..3).prop_map(Value::Int),
        1 => "[ab]".prop_map(Value::Text),
        4 => prop::sample::select(&REFERABLE[..]).prop_map(|n| Value::Ref(gid(n))),
    ]
}

fn arb_changes() -> impl Strategy<Value = Vec<Change>> {
    let one = (
        0usize..FIELDS.len(),
        0u8..4,
        0usize..8,
        arb_value(),
        0u64..4,
        0u8..20,
    );
    proptest::collection::vec(one, 0..80).prop_map(|raw| {
        raw.into_iter()
            .enumerate()
            .map(|(i, (c, g, f, value, wall_ms, odd))| {
                let (collection, fields) = FIELDS[c];
                let (field, value) = match f {
                    6 => (DELETED.to_string(), Value::Int(i64::from(odd % 2))),
                    7 => (ALIAS.to_string(), value),
                    _ if odd == 0 => ("colour".to_string(), value),
                    _ => (fields[f % fields.len()].to_string(), value),
                };
                let shared = odd == 1;
                Change {
                    collection: collection.into(),
                    gid: if shared {
                        gid(1)
                    } else {
                        gid(c as u8 * 10 + 1 + g)
                    },
                    field,
                    value,
                    clock: Hlc {
                        wall_ms: if odd == 19 { u64::MAX } else { wall_ms },
                        counter: i as u32,
                        node: gid(odd % 3),
                    },
                }
            })
            .collect()
    })
}

fn every_reference_is_live(schema: &Schema, view: &View) -> bool {
    schema.collections.iter().all(|c| {
        view.rows(&c.name).all(|(_, row)| {
            c.fields.iter().all(|f| {
                let row_ok = row.contains_key(&f.name);
                let target = match &f.kind {
                    FieldKind::Plain => None,
                    FieldKind::Reference(t) | FieldKind::Owner(t) => Some(t),
                };
                row_ok
                    && match (target, &row[&f.name]) {
                        (Some(t), Value::Ref(g)) => view.row(t, *g).is_some(),
                        (Some(_), Value::Null) | (None, _) => true,
                        (Some(_), _) => false,
                    }
            })
        })
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn any_delivery_order_gives_the_same_view(
        (changes, shuffled) in arb_changes().prop_flat_map(|c| (Just(c.clone()), Just(c).prop_shuffle()))
    ) {
        let schema = schema();
        prop_assert_eq!(view(&schema, &changes), view(&schema, &shuffled));
    }

    #[test]
    fn delivering_changes_again_changes_nothing(
        (changes, again) in arb_changes().prop_flat_map(|c| {
            let n = c.len();
            (Just(c), proptest::collection::vec(any::<prop::sample::Index>(), 0..20)).prop_map(move |(c, picks)| {
                let again: Vec<Change> = if n == 0 { Vec::new() } else { picks.iter().map(|i| c[i.index(n)].clone()).collect() };
                (c, again)
            })
        })
    ) {
        let schema = schema();
        let mut twice = changes.clone();
        twice.extend(again);
        prop_assert_eq!(view(&schema, &changes), view(&schema, &twice));
    }

    #[test]
    fn replicas_that_saw_prefixes_and_then_merged_agree_with_one_that_saw_everything(
        (changes, first, second, cut_a, cut_b) in arb_changes().prop_flat_map(|c| {
            let n = c.len();
            (Just(c.clone()), Just(c.clone()).prop_shuffle(), Just(c).prop_shuffle(), 0..=n, 0..=n)
        })
    ) {
        let schema = schema();
        let a = &first[..cut_a];
        let b = &second[..cut_b];
        for seen in [a, b] {
            let closed = view(&schema, seen);
            prop_assert!(every_reference_is_live(&schema, &closed));
        }
        let mut union = a.to_vec();
        union.extend_from_slice(b);
        union.extend_from_slice(&first[cut_a..]);
        let whole = view(&schema, &changes);
        prop_assert!(every_reference_is_live(&schema, &whole));
        prop_assert_eq!(view(&schema, &union), whole);
    }

    #[test]
    fn aliasing_every_reported_identity_ends_with_none_reported(mut changes in arb_changes()) {
        let schema = schema();
        let mut wall_ms = 1 << 20;
        for _ in 0..changes.len() + 1 {
            let pending = unmerged_identities(&schema, &view(&schema, &changes));
            if pending.is_empty() {
                return Ok(());
            }
            for (collection, a, b) in pending {
                wall_ms += 1;
                changes.push(change(&collection, b, ALIAS, Value::Ref(a), wall_ms));
            }
        }
        prop_assert!(false, "identities were still unmerged after every round");
    }

    #[test]
    fn without_groups_or_owners_the_view_holds_what_the_core_state_holds(
        raw in proptest::collection::vec((1u8..4, 0usize..3, arb_value(), 0u64..4), 0..40)
    ) {
        let schema = Schema::new(vec![Collection::minted("things")
            .plain("title", Merge::Lww)
            .plain("high", Merge::Max)]);
        let changes: Vec<Change> = raw
            .into_iter()
            .enumerate()
            .map(|(i, (g, f, value, wall_ms))| {
                let (field, value) = match f {
                    0 => ("title", value),
                    1 => ("high", value),
                    _ => (DELETED, Value::Int(i as i64 % 2)),
                };
                Change {
                    collection: "things".into(),
                    gid: gid(g),
                    field: field.into(),
                    value,
                    clock: Hlc { wall_ms, counter: i as u32, node: gid(0) },
                }
            })
            .collect();
        let mut state = State::default();
        for c in &changes {
            state.apply(c, if c.field == "high" { Merge::Max } else { Merge::Lww });
        }
        let mut expected = View::default();
        for g in (1u8..4).map(gid) {
            let known = changes.iter().any(|c| c.gid == g);
            if !known || state.is_deleted("things", g) {
                continue;
            }
            let get = |f| state.get("things", g, f).map_or(Value::Null, |(v, _)| v.clone());
            expected.insert("things", g, row(&[("title", get("title")), ("high", get("high"))]));
        }
        prop_assert_eq!(view(&schema, &changes), expected);
    }
}

// --- 1. Validity ----------------------------------------------------------------------------

#[test]
fn a_change_to_an_unknown_collection_is_ignored() {
    let v = view(
        &schema(),
        &[change("shelves", gid(1), "title", text("x"), 1)],
    );
    assert_eq!(v, View::default());
}

#[test]
fn a_gid_belongs_to_the_collection_that_used_it_first_whatever_arrives_first() {
    let changes = [
        change("works", gid(1), "title", text("Middlemarch"), 5),
        change("folders", gid(1), "name", text("shelf"), 3),
    ];
    let expected = only(
        "folders",
        &[(
            gid(1),
            row(&[("name", text("shelf")), ("parent", Value::Null)]),
        )],
    );
    assert_eq!(view(&schema(), &changes), expected);
    let reversed: Vec<Change> = changes.iter().rev().cloned().collect();
    assert_eq!(view(&schema(), &reversed), expected);
}

#[test]
fn a_change_whose_clock_cannot_be_encoded_is_ignored_and_claims_nothing() {
    let mut far = change("folders", gid(1), "name", text("far"), 0);
    far.clock.wall_ms = 0x1_0000_0000_0000;
    let v = view(
        &schema(),
        &[
            far,
            change("works", gid(1), "title", text("Middlemarch"), 9),
        ],
    );
    assert_eq!(v, only("works", &[(gid(1), titled("Middlemarch"))]));
}

#[test]
fn a_malformed_alias_is_ignored_and_claims_nothing() {
    let v = view(
        &schema(),
        &[
            change("folders", gid(1), ALIAS, Value::Int(2), 1),
            change("folders", gid(1), ALIAS, Value::Ref(gid(1)), 2),
            change("works", gid(1), "title", text("Middlemarch"), 3),
        ],
    );
    assert_eq!(v, only("works", &[(gid(1), titled("Middlemarch"))]));
}

#[test]
fn an_undeclared_field_is_kept_claims_its_gid_and_is_not_shown() {
    let v = view(
        &schema(),
        &[
            change("works", gid(1), "colour", text("red"), 1),
            change("notes", gid(1), "body", text("a note"), 2),
        ],
    );
    let blank = work(
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
    );
    assert_eq!(v, only("works", &[(gid(1), blank)]));
}

// --- 2. Registers ---------------------------------------------------------------------------

#[test]
fn the_later_write_wins_and_the_greater_value_wins_with_ties_to_the_later_write() {
    let v = view(
        &schema(),
        &[
            change("works", gid(1), "title", text("first"), 1),
            change("works", gid(1), "title", text("second"), 2),
            change("works", gid(1), "rating", Value::Int(9), 1),
            change("works", gid(1), "rating", Value::Int(4), 2),
        ],
    );
    assert_eq!(v.row("works", gid(1)).unwrap()["title"], text("second"));
    assert_eq!(v.row("works", gid(1)).unwrap()["rating"], Value::Int(9));
}

// --- 3. Field sets --------------------------------------------------------------------------

#[test]
fn a_together_set_takes_only_the_fields_written_at_its_latest_clock() {
    let v = view(
        &schema(),
        &[
            change("works", gid(1), "lat", Value::Int(1), 1),
            change("works", gid(1), "lon", Value::Int(2), 1),
            change("works", gid(1), "lat", Value::Int(7), 2),
        ],
    );
    let r = v.row("works", gid(1)).unwrap();
    assert_eq!((&r["lat"], &r["lon"]), (&Value::Int(7), &Value::Null));
}

#[test]
fn a_together_set_comes_whole_from_the_member_written_last() {
    let v = view(
        &schema(),
        &[
            change("works", gid(1), "lat", Value::Int(1), 1),
            change("works", gid(1), "lon", Value::Int(2), 1),
            change("works", gid(2), "lon", Value::Int(5), 2),
            change("works", gid(2), ALIAS, Value::Ref(gid(1)), 3),
        ],
    );
    let r = v.row("works", gid(1)).unwrap();
    assert_eq!((&r["lat"], &r["lon"]), (&Value::Null, &Value::Int(5)));
}

// --- 4. Alias groups ------------------------------------------------------------------------

#[test]
fn an_alias_learned_before_either_entitys_fields() {
    let alias = change("works", gid(5), ALIAS, Value::Ref(gid(3)), 9);
    let fields = [
        change("works", gid(3), "title", text("Middlemarch"), 1),
        change("works", gid(5), "rating", Value::Int(4), 2),
    ];
    let early = view(&schema(), std::slice::from_ref(&alias));
    let blank = work(
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
    );
    assert_eq!(early, only("works", &[(gid(5), blank)]));

    let mut all = vec![alias];
    all.extend(fields);
    let expected = only(
        "works",
        &[(
            gid(3),
            work(
                text("Middlemarch"),
                Value::Int(4),
                Value::Null,
                Value::Null,
                Value::Null,
            ),
        )],
    );
    assert_eq!(view(&schema(), &all), expected);
}

#[test]
fn a_chain_of_aliases_is_one_group_named_by_its_smallest_gid() {
    let v = view(
        &schema(),
        &[
            change("works", gid(4), "title", text("d"), 1),
            change("works", gid(2), "title", text("b"), 2),
            change("works", gid(9), "title", text("i"), 3),
            change("works", gid(4), ALIAS, Value::Ref(gid(9)), 4),
            change("works", gid(9), ALIAS, Value::Ref(gid(2)), 5),
        ],
    );
    assert_eq!(v, only("works", &[(gid(2), titled("i"))]));
}

#[test]
fn an_alias_to_a_gid_of_another_collection_joins_nothing() {
    let v = view(
        &schema(),
        &[
            change("works", gid(1), "title", text("Middlemarch"), 1),
            change("folders", gid(2), "name", text("shelf"), 2),
            change("works", gid(1), ALIAS, Value::Ref(gid(2)), 3),
            change("folders", gid(2), ALIAS, Value::Ref(gid(1)), 4),
        ],
    );
    assert_eq!(v.row("works", gid(1)), Some(&titled("Middlemarch")));
    assert_eq!(
        v.row("folders", gid(2)),
        Some(&row(&[("name", text("shelf")), ("parent", Value::Null)]))
    );
}

#[test]
fn natural_keys_collapse_when_their_parents_merge_and_again_one_level_down() {
    let (w1, w2) = (gid(1), gid(2));
    let (e1, e2) = (edition_gid(w1, "pb"), edition_gid(w2, "pb"));
    let p1 = Gid::derive("printings", &[Value::Ref(e1), Value::Int(1)]);
    let p2 = Gid::derive("printings", &[Value::Ref(e2), Value::Int(1)]);
    let mut changes = vec![
        change("works", w1, "title", text("Middlemarch"), 1),
        change("works", w2, "title", text("Middlemarch"), 2),
    ];
    changes.extend(edition(e1, w1, "pb", 10));
    changes.extend(edition(e2, w2, "pb", 20));
    changes.push(change("editions", e2, "price", Value::Int(7), 25));
    for (p, e, wall_ms) in [(p1, e1, 30), (p2, e2, 40)] {
        changes.push(change("printings", p, "edition", Value::Ref(e), wall_ms));
        changes.push(change("printings", p, "number", Value::Int(1), wall_ms + 1));
    }

    let apart = view(&schema(), &changes);
    assert_eq!(apart.rows("editions").count(), 2);
    assert_eq!(apart.rows("printings").count(), 2);

    changes.push(change("works", w2, ALIAS, Value::Ref(w1), 50));
    let merged = view(&schema(), &changes);
    let e = e1.min(e2);
    assert_eq!(
        merged.rows("editions").collect::<Vec<_>>(),
        vec![(
            &e,
            &row(&[
                ("work", Value::Ref(w1)),
                ("format", text("pb")),
                ("price", Value::Int(7)),
            ])
        )]
    );
    assert_eq!(
        merged.rows("printings").collect::<Vec<_>>(),
        vec![(
            &p1.min(p2),
            &row(&[("edition", Value::Ref(e)), ("number", Value::Int(1))])
        )]
    );
}

#[test]
fn a_natural_entity_with_an_unwritten_key_field_matches_nothing() {
    let (e1, e2) = (gid(1), gid(2));
    let v = view(
        &schema(),
        &[
            change("works", gid(9), "title", text("Middlemarch"), 1),
            change("editions", e1, "work", Value::Ref(gid(9)), 2),
            change("editions", e2, "work", Value::Ref(gid(9)), 3),
        ],
    );
    assert_eq!(v.rows("editions").count(), 2);
}

// --- 5. Identity ----------------------------------------------------------------------------

#[test]
fn live_groups_sharing_an_identity_key_are_reported_until_aliased() {
    let mut changes = vec![
        change("works", gid(1), "isbn", text("9780141439549"), 1),
        change("works", gid(2), "isbn", text("9780141439549"), 2),
        change("works", gid(3), "isbn", text("other"), 3),
        change("works", gid(4), "title", text("no isbn"), 4),
        change("works", gid(5), "title", text("no isbn"), 5),
    ];
    let schema = schema();
    assert_eq!(
        unmerged_identities(&schema, &view(&schema, &changes)),
        vec![("works".to_string(), gid(1), gid(2))]
    );
    changes.push(change("works", gid(2), ALIAS, Value::Ref(gid(1)), 6));
    assert!(unmerged_identities(&schema, &view(&schema, &changes)).is_empty());
}

#[test]
fn a_deleted_group_does_not_need_merging() {
    let schema = schema();
    let changes = [
        change("works", gid(1), "isbn", text("x"), 1),
        change("works", gid(2), "isbn", text("x"), 2),
        change("works", gid(2), DELETED, Value::Int(1), 3),
    ];
    assert!(unmerged_identities(&schema, &view(&schema, &changes)).is_empty());
}

// --- 6. Liveness ----------------------------------------------------------------------------

#[test]
fn a_deleted_member_lends_nothing_to_its_live_group() {
    let v = view(
        &schema(),
        &[
            change("works", gid(1), "title", text("kept"), 1),
            change("works", gid(2), "title", text("deleted, later"), 2),
            change("works", gid(2), "rating", Value::Int(5), 2),
            change("works", gid(2), DELETED, Value::Int(1), 3),
            change("works", gid(2), ALIAS, Value::Ref(gid(1)), 4),
        ],
    );
    assert_eq!(v, only("works", &[(gid(1), titled("kept"))]));
}

#[test]
fn a_group_whose_members_are_all_deleted_is_gone_and_a_restore_brings_it_back() {
    let mut changes = vec![
        change("works", gid(1), "title", text("a"), 1),
        change("works", gid(2), "title", text("b"), 2),
        change("works", gid(1), ALIAS, Value::Ref(gid(2)), 3),
        change("works", gid(1), DELETED, Value::Int(1), 4),
        change("works", gid(2), DELETED, Value::Int(1), 5),
    ];
    assert_eq!(view(&schema(), &changes), View::default());
    changes.push(change("works", gid(2), DELETED, Value::Int(0), 6));
    assert_eq!(
        view(&schema(), &changes),
        only("works", &[(gid(1), titled("b"))])
    );
}

#[test]
fn an_orphan_neither_wins_a_field_nor_keeps_its_group_alive() {
    let (n1, n2) = (gid(1), gid(2));
    let note = |body: &str, owner: Gid| {
        row(&[
            ("body", text(body)),
            ("work", Value::Ref(owner)),
            ("about", Value::Null),
        ])
    };
    let mut changes = vec![
        change("works", gid(7), "title", text("kept"), 1),
        change("works", gid(8), "title", text("doomed"), 2),
        change("notes", n1, "body", text("old"), 3),
        change("notes", n1, "work", Value::Ref(gid(7)), 3),
        change("notes", n2, "body", text("newer, orphaned"), 4),
        change("notes", n2, "work", Value::Ref(gid(8)), 4),
        change("notes", n2, ALIAS, Value::Ref(n1), 5),
        change("works", gid(8), DELETED, Value::Int(1), 6),
    ];
    assert_eq!(
        view(&schema(), &changes).rows("notes").collect::<Vec<_>>(),
        vec![(&n1, &note("old", gid(7)))]
    );
    changes.push(change("works", gid(7), DELETED, Value::Int(1), 7));
    assert_eq!(view(&schema(), &changes).rows("notes").count(), 0);
}

#[test]
fn an_owner_that_was_never_seen_hides_its_rows_until_it_arrives() {
    let mut changes = vec![change("notes", gid(1), "work", Value::Ref(gid(7)), 2)];
    assert_eq!(view(&schema(), &changes), View::default());
    changes.push(change("works", gid(7), "title", text("late"), 1));
    assert!(view(&schema(), &changes).row("notes", gid(1)).is_some());
}

#[test]
fn an_owner_kept_alive_by_another_member_of_its_group_keeps_its_rows() {
    let v = view(
        &schema(),
        &[
            change("works", gid(1), "title", text("a"), 1),
            change("works", gid(2), "title", text("b"), 2),
            change("works", gid(2), ALIAS, Value::Ref(gid(1)), 3),
            change("works", gid(2), DELETED, Value::Int(1), 4),
            change("notes", gid(5), "work", Value::Ref(gid(2)), 5),
        ],
    );
    assert_eq!(v.row("notes", gid(5)).unwrap()["work"], Value::Ref(gid(1)));
}

#[test]
fn an_ownership_cycle_with_no_live_root_is_not_live() {
    let v = view(
        &schema(),
        &[
            change("folders", gid(1), "parent", Value::Ref(gid(2)), 1),
            change("folders", gid(2), "parent", Value::Ref(gid(1)), 2),
            change("folders", gid(3), "parent", Value::Null, 3),
            change("folders", gid(4), "parent", Value::Ref(gid(3)), 4),
        ],
    );
    let gids: Vec<Gid> = v.rows("folders").map(|(g, _)| *g).collect();
    assert_eq!(gids, vec![gid(3), gid(4)]);
}

#[test]
fn deleting_a_folder_hides_its_whole_subtree() {
    let v = view(
        &schema(),
        &[
            change("folders", gid(1), "name", text("root"), 1),
            change("folders", gid(2), "parent", Value::Ref(gid(1)), 2),
            change("folders", gid(3), "parent", Value::Ref(gid(2)), 3),
            change("folders", gid(1), DELETED, Value::Int(1), 4),
        ],
    );
    assert_eq!(v, View::default());
}

// --- 7, 8, 9. Group state, references and the view's shape ---------------------------------

#[test]
fn a_reference_reads_as_the_canonical_gid_while_live_and_null_otherwise() {
    let note = |about: Value| {
        row(&[
            ("body", Value::Null),
            ("work", Value::Null),
            ("about", about),
        ])
    };
    let mut changes = vec![
        change("works", gid(3), "title", text("a"), 1),
        change("works", gid(2), "title", text("b"), 2),
        change("works", gid(3), ALIAS, Value::Ref(gid(2)), 3),
        change("notes", gid(10), "about", Value::Ref(gid(3)), 4),
        change("notes", gid(11), "about", Value::Ref(gid(99)), 5),
        change("notes", gid(12), "about", text("not a reference"), 6),
    ];
    let v = view(&schema(), &changes);
    assert_eq!(v.row("notes", gid(10)), Some(&note(Value::Ref(gid(2)))));
    assert_eq!(v.row("notes", gid(11)), Some(&note(Value::Null)));
    assert_eq!(v.row("notes", gid(12)), Some(&note(Value::Null)));

    changes.push(change("works", gid(2), DELETED, Value::Int(1), 7));
    changes.push(change("works", gid(3), DELETED, Value::Int(1), 8));
    let v = view(&schema(), &changes);
    assert_eq!(v.row("notes", gid(10)), Some(&note(Value::Null)));
}

#[test]
fn a_group_takes_each_field_by_its_strategy_across_live_members() {
    let v = view(
        &schema(),
        &[
            change("works", gid(1), "title", text("older"), 1),
            change("works", gid(1), "rating", Value::Int(9), 2),
            change("works", gid(2), "title", text("newer"), 3),
            change("works", gid(2), "rating", Value::Int(3), 4),
            change("works", gid(1), ALIAS, Value::Ref(gid(2)), 5),
        ],
    );
    let expected = work(
        text("newer"),
        Value::Int(9),
        Value::Null,
        Value::Null,
        Value::Null,
    );
    assert_eq!(v, only("works", &[(gid(1), expected)]));
}

#[test]
fn views_compare_reals_by_their_total_order() {
    let nan = |n| {
        let mut v = View::default();
        let mut r = BTreeMap::new();
        r.insert("x".to_string(), Value::Real(n));
        v.insert("c", gid(1), r);
        v
    };
    assert_eq!(nan(f64::NAN), nan(f64::NAN));
    assert_ne!(nan(0.0), nan(-0.0));
}

#[test]
fn the_schema_check_refuses_what_the_model_cannot_answer() {
    let bad = [
        Schema::new(vec![
            Collection::minted("a").owner("b", "b"),
            Collection::minted("b").owner("a", "a"),
        ]),
        Schema::new(vec![
            Collection::minted("a")
                .plain("x", Merge::Max)
                .together(&["x"]),
        ]),
        Schema::new(vec![
            Collection::natural("a", &["k"])
                .plain("k", Merge::Lww)
                .identity_key(&["k"]),
        ]),
        Schema::new(vec![Collection::minted("a").plain(DELETED, Merge::Lww)]),
        Schema::new(vec![Collection::minted("a").reference("x", "nowhere")]),
    ];
    for schema in bad {
        assert!(schema.check().is_err(), "{schema:?}");
    }
    assert!(
        Schema::new(vec![Collection::minted("a").owner("parent", "a")])
            .check()
            .is_ok()
    );
}

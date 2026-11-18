use std::collections::BTreeMap;

use fongo_sync_core::model::FieldKind;
use fongo_sync_core::model::{ALIAS, Collection, Row, Schema, View, unmerged_identities, view};
use fongo_sync_core::{Change, DELETED, Gid, Hlc, State, Strategy as Merge, Value};
use proptest::prelude::*;

fn schema() -> Schema {
    let schema = Schema::new(vec![
        Collection::minted("sites")
            .plain("name", Merge::Lww)
            .plain("score", Merge::Max)
            .plain("code", Merge::Lww)
            .plain("x", Merge::Lww)
            .plain("y", Merge::Lww)
            .identity_key(&["code"])
            .together(&["x", "y"]),
        Collection::natural("rooms", &["site", "label"])
            .owner("site", "sites")
            .plain("label", Merge::Lww)
            .plain("size", Merge::Lww),
        Collection::natural("slots", &["room", "number"])
            .owner("room", "rooms")
            .plain("number", Merge::Lww),
        Collection::minted("items")
            .plain("text", Merge::Lww)
            .owner("site", "sites")
            .reference("link", "sites"),
        Collection::minted("parts")
            .plain("text", Merge::Lww)
            .owner("item", "items"),
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

fn site(name: Value, score: Value, code: Value, x: Value, y: Value) -> Row {
    row(&[
        ("name", name),
        ("score", score),
        ("code", code),
        ("x", x),
        ("y", y),
    ])
}

fn named(name: &str) -> Row {
    site(
        text(name),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
    )
}

fn blank_site() -> Row {
    site(
        Value::Null,
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

fn room_gid(site: Gid, label: &str) -> Gid {
    Gid::derive("rooms", &[Value::Ref(site), text(label)])
}

fn room(gid: Gid, site: Gid, label: &str, wall_ms: u64) -> Vec<Change> {
    vec![
        change("rooms", gid, "site", Value::Ref(site), wall_ms),
        change("rooms", gid, "label", text(label), wall_ms + 1),
    ]
}

fn room_row(site: Gid, label: &str, size: Value) -> Row {
    row(&[
        ("site", Value::Ref(site)),
        ("label", text(label)),
        ("size", size),
    ])
}

// --- Properties -----------------------------------------------------------------------------

const FIELDS: [(&str, &[&str]); 6] = [
    ("sites", &["name", "score", "code", "x", "y"]),
    ("rooms", &["site", "label", "size"]),
    ("slots", &["room", "number"]),
    ("items", &["text", "site", "link"]),
    ("parts", &["text", "item"]),
    ("unknown", &["name"]),
];

const REFERABLE: [u8; 10] = [1, 2, 3, 4, 31, 32, 33, 41, 42, 43];

const LABELS: [&str; 2] = ["a", "b"];

fn arb_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        1 => Just(Value::Null),
        2 => (0i64..3).prop_map(Value::Int),
        1 => "[ab]".prop_map(Value::Text),
        4 => prop::sample::select(&REFERABLE[..]).prop_map(|n| Value::Ref(gid(n))),
    ]
}

/// The key and gid of a natural entity: rooms are keyed by one of four sites and a label, slots
/// by one of those rooms and a number.
fn natural(collection: &str, g: u8, pick: u8) -> (Vec<Value>, Gid) {
    let room_key = vec![Value::Ref(gid(1 + g)), text(LABELS[usize::from(pick % 2)])];
    let key = match collection {
        "rooms" => room_key,
        _ => vec![
            Value::Ref(Gid::derive("rooms", &room_key)),
            Value::Int(i64::from(pick / 2)),
        ],
    };
    let gid = Gid::derive(collection, &key);
    (key, gid)
}

/// Changes to a few gids per collection. Most changes to a natural entity arrive with the writes
/// of its key (and its parent's), so natural entities are common; some key writes are hostile.
fn arb_changes() -> impl Strategy<Value = Vec<Change>> {
    let one = (
        0usize..FIELDS.len(),
        0u8..4,
        0usize..8,
        arb_value(),
        0u64..4,
        0u8..20,
        0u8..4,
    );
    proptest::collection::vec(one, 0..60).prop_map(|raw| {
        let mut changes = Vec::new();
        for (c, g, f, value, wall_ms, odd, pick) in raw {
            let (collection, fields) = FIELDS[c];
            let mut stamp = |collection: &str, gid: Gid, field: &str, value: Value| {
                changes.push(Change {
                    collection: collection.into(),
                    gid,
                    field: field.into(),
                    value,
                    clock: Hlc {
                        wall_ms: if odd == 19 { u64::MAX } else { wall_ms },
                        counter: changes.len() as u32,
                        node: Gid([odd % 3; 16]),
                    },
                });
            };
            let (key, gid) = match collection {
                "rooms" | "slots" => natural(collection, g, pick),
                _ if odd == 1 => (Vec::new(), gid(1)),
                _ => (Vec::new(), gid(c as u8 * 10 + 1 + g)),
            };
            if !key.is_empty() && odd < 15 {
                if collection == "slots" {
                    let (room_key, room) = natural("rooms", g, pick);
                    stamp("rooms", room, "site", room_key[0].clone());
                    stamp("rooms", room, "label", room_key[1].clone());
                }
                stamp(collection, gid, fields[0], key[0].clone());
                stamp(collection, gid, fields[1], key[1].clone());
            }
            let (field, value) = match f {
                6 => (DELETED, Value::Int(i64::from(odd % 2))),
                7 => (ALIAS, value),
                _ if odd == 0 => ("colour", value),
                _ => {
                    let k = f % fields.len();
                    let honest = odd != 2 && k < key.len();
                    (fields[k], if honest { key[k].clone() } else { value })
                }
            };
            stamp(collection, gid, field, value);
        }
        changes
    })
}

fn every_reference_is_live(schema: &Schema, view: &View) -> bool {
    schema.collections.iter().all(|c| {
        view.rows(&c.name).all(|(_, row)| {
            c.fields.iter().all(|f| {
                let target = match &f.kind {
                    FieldKind::Plain => None,
                    FieldKind::Reference(t) | FieldKind::Owner(t) => Some(t),
                };
                row.contains_key(&f.name)
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
    fn every_natural_row_shows_its_whole_key(changes in arb_changes()) {
        let schema = schema();
        let v = view(&schema, &changes);
        for (collection, key) in [("rooms", ["site", "label"]), ("slots", ["room", "number"])] {
            for (_, r) in v.rows(collection) {
                prop_assert!(key.iter().all(|f| r[*f] != Value::Null));
            }
        }
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
    let v = view(&schema(), &[change("halls", gid(1), "name", text("x"), 1)]);
    assert_eq!(v, View::default());
}

#[test]
fn a_gid_belongs_to_the_collection_that_used_it_first_whatever_arrives_first() {
    let changes = [
        change("sites", gid(1), "name", text("north"), 5),
        change("parts", gid(1), "text", text("bolt"), 3),
    ];
    let expected = only(
        "parts",
        &[(
            gid(1),
            row(&[("text", text("bolt")), ("item", Value::Null)]),
        )],
    );
    assert_eq!(view(&schema(), &changes), expected);
    let reversed: Vec<Change> = changes.iter().rev().cloned().collect();
    assert_eq!(view(&schema(), &reversed), expected);
}

#[test]
fn a_change_whose_clock_cannot_be_encoded_is_ignored_and_claims_nothing() {
    let mut far = change("parts", gid(1), "text", text("far"), 0);
    far.clock.wall_ms = 0x1_0000_0000_0000;
    let v = view(
        &schema(),
        &[far, change("sites", gid(1), "name", text("north"), 9)],
    );
    assert_eq!(v, only("sites", &[(gid(1), named("north"))]));
}

#[test]
fn a_malformed_alias_is_ignored_and_claims_nothing() {
    let v = view(
        &schema(),
        &[
            change("parts", gid(1), ALIAS, Value::Int(2), 1),
            change("parts", gid(1), ALIAS, Value::Ref(gid(1)), 2),
            change("sites", gid(1), "name", text("north"), 3),
        ],
    );
    assert_eq!(v, only("sites", &[(gid(1), named("north"))]));
}

#[test]
fn an_undeclared_field_is_kept_claims_its_gid_and_is_not_shown() {
    let v = view(
        &schema(),
        &[
            change("sites", gid(1), "colour", text("red"), 1),
            change("items", gid(1), "text", text("an item"), 2),
        ],
    );
    assert_eq!(v, only("sites", &[(gid(1), blank_site())]));
}

#[test]
fn a_rewrite_of_a_natural_key_field_is_ignored() {
    let s = gid(1);
    let r = room_gid(s, "a");
    let mut changes = vec![change("sites", s, "name", text("north"), 1)];
    changes.extend(room(r, s, "a", 2));
    changes.push(change("rooms", r, "label", text("b"), 10));
    changes.push(change("rooms", r, "site", Value::Ref(gid(2)), 11));
    changes.push(change("rooms", r, "size", Value::Int(3), 12));
    assert_eq!(
        view(&schema(), &changes).rows("rooms").collect::<Vec<_>>(),
        vec![(&r, &room_row(s, "a", Value::Int(3)))]
    );
}

#[test]
fn a_natural_entity_whose_key_does_not_derive_its_gid_is_not_shown_and_joins_no_group() {
    let s = gid(1);
    let honest = room_gid(s, "a");
    let mut changes = vec![change("sites", s, "name", text("north"), 1)];
    changes.extend(room(honest, s, "a", 2));
    changes.extend(room(gid(9), s, "a", 4));
    changes.push(change("rooms", gid(9), "size", Value::Int(5), 6));
    changes.push(change("rooms", gid(9), ALIAS, Value::Ref(honest), 7));
    changes.push(change("rooms", honest, ALIAS, Value::Ref(gid(9)), 8));
    assert_eq!(
        view(&schema(), &changes).rows("rooms").collect::<Vec<_>>(),
        vec![(&honest, &room_row(s, "a", Value::Null))]
    );
}

#[test]
fn a_natural_entity_is_shown_only_once_its_whole_key_has_arrived() {
    let s = gid(1);
    let r = room_gid(s, "a");
    let mut changes = vec![
        change("sites", s, "name", text("north"), 1),
        change("rooms", r, "label", text("a"), 3),
        change("rooms", r, "size", Value::Int(2), 4),
    ];
    assert_eq!(view(&schema(), &changes).rows("rooms").count(), 0);
    changes.push(change("rooms", r, "site", Value::Ref(s), 2));
    assert_eq!(
        view(&schema(), &changes).rows("rooms").collect::<Vec<_>>(),
        vec![(&r, &room_row(s, "a", Value::Int(2)))]
    );
}

// --- 2. Registers ---------------------------------------------------------------------------

#[test]
fn the_later_write_wins_and_the_greater_value_wins_with_ties_to_the_later_write() {
    let v = view(
        &schema(),
        &[
            change("sites", gid(1), "name", text("first"), 1),
            change("sites", gid(1), "name", text("second"), 2),
            change("sites", gid(1), "score", Value::Int(9), 1),
            change("sites", gid(1), "score", Value::Int(4), 2),
        ],
    );
    assert_eq!(v.row("sites", gid(1)).unwrap()["name"], text("second"));
    assert_eq!(v.row("sites", gid(1)).unwrap()["score"], Value::Int(9));
}

// --- 3. Field sets --------------------------------------------------------------------------

#[test]
fn a_together_set_takes_only_the_fields_written_at_its_latest_clock() {
    let v = view(
        &schema(),
        &[
            change("sites", gid(1), "x", Value::Int(1), 1),
            change("sites", gid(1), "y", Value::Int(2), 1),
            change("sites", gid(1), "x", Value::Int(7), 2),
        ],
    );
    let r = v.row("sites", gid(1)).unwrap();
    assert_eq!((&r["x"], &r["y"]), (&Value::Int(7), &Value::Null));
}

#[test]
fn a_together_set_comes_whole_from_the_member_written_last() {
    let v = view(
        &schema(),
        &[
            change("sites", gid(1), "x", Value::Int(1), 1),
            change("sites", gid(1), "y", Value::Int(2), 1),
            change("sites", gid(2), "y", Value::Int(5), 2),
            change("sites", gid(2), ALIAS, Value::Ref(gid(1)), 3),
        ],
    );
    let r = v.row("sites", gid(1)).unwrap();
    assert_eq!((&r["x"], &r["y"]), (&Value::Null, &Value::Int(5)));
}

// --- 4. Alias groups ------------------------------------------------------------------------

#[test]
fn an_alias_learned_before_either_entitys_fields() {
    let alias = change("sites", gid(5), ALIAS, Value::Ref(gid(3)), 9);
    let fields = [
        change("sites", gid(3), "name", text("north"), 1),
        change("sites", gid(5), "score", Value::Int(4), 2),
    ];
    let early = view(&schema(), std::slice::from_ref(&alias));
    assert_eq!(early, only("sites", &[(gid(5), blank_site())]));

    let mut all = vec![alias];
    all.extend(fields);
    let expected = only(
        "sites",
        &[(
            gid(3),
            site(
                text("north"),
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
            change("sites", gid(4), "name", text("d"), 1),
            change("sites", gid(2), "name", text("b"), 2),
            change("sites", gid(9), "name", text("i"), 3),
            change("sites", gid(4), ALIAS, Value::Ref(gid(9)), 4),
            change("sites", gid(9), ALIAS, Value::Ref(gid(2)), 5),
        ],
    );
    assert_eq!(v, only("sites", &[(gid(2), named("i"))]));
}

#[test]
fn an_alias_to_a_gid_of_another_collection_joins_nothing() {
    let v = view(
        &schema(),
        &[
            change("sites", gid(1), "name", text("north"), 1),
            change("parts", gid(2), "text", text("bolt"), 2),
            change("sites", gid(1), ALIAS, Value::Ref(gid(2)), 3),
            change("parts", gid(2), ALIAS, Value::Ref(gid(1)), 4),
        ],
    );
    assert_eq!(v.row("sites", gid(1)), Some(&named("north")));
    assert_eq!(
        v.row("parts", gid(2)),
        Some(&row(&[("text", text("bolt")), ("item", Value::Null)]))
    );
}

#[test]
fn natural_keys_collapse_when_their_parents_merge_and_again_one_level_down() {
    let (s1, s2) = (gid(1), gid(2));
    let (r1, r2) = (room_gid(s1, "a"), room_gid(s2, "a"));
    let slot1 = Gid::derive("slots", &[Value::Ref(r1), Value::Int(1)]);
    let slot2 = Gid::derive("slots", &[Value::Ref(r2), Value::Int(1)]);
    let mut changes = vec![
        change("sites", s1, "name", text("north"), 1),
        change("sites", s2, "name", text("north"), 2),
    ];
    changes.extend(room(r1, s1, "a", 10));
    changes.extend(room(r2, s2, "a", 20));
    changes.push(change("rooms", r2, "size", Value::Int(7), 25));
    for (slot, r, wall_ms) in [(slot1, r1, 30), (slot2, r2, 40)] {
        changes.push(change("slots", slot, "room", Value::Ref(r), wall_ms));
        changes.push(change("slots", slot, "number", Value::Int(1), wall_ms + 1));
    }

    let apart = view(&schema(), &changes);
    assert_eq!(apart.rows("rooms").count(), 2);
    assert_eq!(apart.rows("slots").count(), 2);

    changes.push(change("sites", s2, ALIAS, Value::Ref(s1), 50));
    let merged = view(&schema(), &changes);
    let r = r1.min(r2);
    assert_eq!(
        merged.rows("rooms").collect::<Vec<_>>(),
        vec![(&r, &room_row(s1, "a", Value::Int(7)))]
    );
    assert_eq!(
        merged.rows("slots").collect::<Vec<_>>(),
        vec![(
            &slot1.min(slot2),
            &row(&[("room", Value::Ref(r)), ("number", Value::Int(1))])
        )]
    );
}

// --- 5. Identity ----------------------------------------------------------------------------

#[test]
fn live_groups_sharing_an_identity_key_are_reported_until_aliased() {
    let mut changes = vec![
        change("sites", gid(1), "code", text("A-1"), 1),
        change("sites", gid(2), "code", text("A-1"), 2),
        change("sites", gid(3), "code", text("B-2"), 3),
        change("sites", gid(4), "name", text("no code"), 4),
        change("sites", gid(5), "name", text("no code"), 5),
    ];
    let schema = schema();
    assert_eq!(
        unmerged_identities(&schema, &view(&schema, &changes)),
        vec![("sites".to_string(), gid(1), gid(2))]
    );
    changes.push(change("sites", gid(2), ALIAS, Value::Ref(gid(1)), 6));
    assert!(unmerged_identities(&schema, &view(&schema, &changes)).is_empty());
}

#[test]
fn a_deleted_group_does_not_need_merging() {
    let schema = schema();
    let changes = [
        change("sites", gid(1), "code", text("x"), 1),
        change("sites", gid(2), "code", text("x"), 2),
        change("sites", gid(2), DELETED, Value::Int(1), 3),
    ];
    assert!(unmerged_identities(&schema, &view(&schema, &changes)).is_empty());
}

// --- 6. Liveness ----------------------------------------------------------------------------

#[test]
fn a_deleted_member_lends_nothing_to_its_live_group() {
    let v = view(
        &schema(),
        &[
            change("sites", gid(1), "name", text("kept"), 1),
            change("sites", gid(2), "name", text("deleted, later"), 2),
            change("sites", gid(2), "score", Value::Int(5), 2),
            change("sites", gid(2), DELETED, Value::Int(1), 3),
            change("sites", gid(2), ALIAS, Value::Ref(gid(1)), 4),
        ],
    );
    assert_eq!(v, only("sites", &[(gid(1), named("kept"))]));
}

#[test]
fn a_group_whose_members_are_all_deleted_is_gone_and_a_restore_brings_it_back() {
    let mut changes = vec![
        change("sites", gid(1), "name", text("a"), 1),
        change("sites", gid(2), "name", text("b"), 2),
        change("sites", gid(1), ALIAS, Value::Ref(gid(2)), 3),
        change("sites", gid(1), DELETED, Value::Int(1), 4),
        change("sites", gid(2), DELETED, Value::Int(1), 5),
    ];
    assert_eq!(view(&schema(), &changes), View::default());
    changes.push(change("sites", gid(2), DELETED, Value::Int(0), 6));
    assert_eq!(
        view(&schema(), &changes),
        only("sites", &[(gid(1), named("b"))])
    );
}

#[test]
fn an_orphan_neither_wins_a_field_nor_keeps_its_group_alive() {
    let (i1, i2) = (gid(1), gid(2));
    let item = |body: &str, owner: Gid| {
        row(&[
            ("text", text(body)),
            ("site", Value::Ref(owner)),
            ("link", Value::Null),
        ])
    };
    let mut changes = vec![
        change("sites", gid(7), "name", text("kept"), 1),
        change("sites", gid(8), "name", text("doomed"), 2),
        change("items", i1, "text", text("old"), 3),
        change("items", i1, "site", Value::Ref(gid(7)), 3),
        change("items", i2, "text", text("newer, orphaned"), 4),
        change("items", i2, "site", Value::Ref(gid(8)), 4),
        change("items", i2, ALIAS, Value::Ref(i1), 5),
        change("sites", gid(8), DELETED, Value::Int(1), 6),
    ];
    assert_eq!(
        view(&schema(), &changes).rows("items").collect::<Vec<_>>(),
        vec![(&i1, &item("old", gid(7)))]
    );
    changes.push(change("sites", gid(7), DELETED, Value::Int(1), 7));
    assert_eq!(view(&schema(), &changes).rows("items").count(), 0);
}

#[test]
fn an_owner_that_was_never_seen_hides_its_rows_until_it_arrives() {
    let mut changes = vec![change("items", gid(1), "site", Value::Ref(gid(7)), 2)];
    assert_eq!(view(&schema(), &changes), View::default());
    changes.push(change("sites", gid(7), "name", text("late"), 1));
    assert!(view(&schema(), &changes).row("items", gid(1)).is_some());
}

#[test]
fn an_owner_kept_alive_by_another_member_of_its_group_keeps_its_rows() {
    let v = view(
        &schema(),
        &[
            change("sites", gid(1), "name", text("a"), 1),
            change("sites", gid(2), "name", text("b"), 2),
            change("sites", gid(2), ALIAS, Value::Ref(gid(1)), 3),
            change("sites", gid(2), DELETED, Value::Int(1), 4),
            change("items", gid(5), "site", Value::Ref(gid(2)), 5),
        ],
    );
    assert_eq!(v.row("items", gid(5)).unwrap()["site"], Value::Ref(gid(1)));
}

#[test]
fn deleting_an_owner_hides_everything_below_it() {
    let v = view(
        &schema(),
        &[
            change("sites", gid(1), "name", text("north"), 1),
            change("items", gid(2), "site", Value::Ref(gid(1)), 2),
            change("parts", gid(3), "item", Value::Ref(gid(2)), 3),
            change("sites", gid(1), DELETED, Value::Int(1), 4),
        ],
    );
    assert_eq!(v, View::default());
}

// --- 7, 8, 9. Group state, references and the view's shape ---------------------------------

#[test]
fn a_reference_reads_as_the_canonical_gid_while_live_and_null_otherwise() {
    let item = |link: Value| row(&[("text", Value::Null), ("site", Value::Null), ("link", link)]);
    let mut changes = vec![
        change("sites", gid(3), "name", text("a"), 1),
        change("sites", gid(2), "name", text("b"), 2),
        change("sites", gid(3), ALIAS, Value::Ref(gid(2)), 3),
        change("items", gid(10), "link", Value::Ref(gid(3)), 4),
        change("items", gid(11), "link", Value::Ref(gid(99)), 5),
        change("items", gid(12), "link", text("not a reference"), 6),
    ];
    let v = view(&schema(), &changes);
    assert_eq!(v.row("items", gid(10)), Some(&item(Value::Ref(gid(2)))));
    assert_eq!(v.row("items", gid(11)), Some(&item(Value::Null)));
    assert_eq!(v.row("items", gid(12)), Some(&item(Value::Null)));

    changes.push(change("sites", gid(2), DELETED, Value::Int(1), 7));
    changes.push(change("sites", gid(3), DELETED, Value::Int(1), 8));
    let v = view(&schema(), &changes);
    assert_eq!(v.row("items", gid(10)), Some(&item(Value::Null)));
}

#[test]
fn a_group_takes_each_field_by_its_strategy_across_live_members() {
    let v = view(
        &schema(),
        &[
            change("sites", gid(1), "name", text("older"), 1),
            change("sites", gid(1), "score", Value::Int(9), 2),
            change("sites", gid(2), "name", text("newer"), 3),
            change("sites", gid(2), "score", Value::Int(3), 4),
            change("sites", gid(1), ALIAS, Value::Ref(gid(2)), 5),
        ],
    );
    let expected = site(
        text("newer"),
        Value::Int(9),
        Value::Null,
        Value::Null,
        Value::Null,
    );
    assert_eq!(v, only("sites", &[(gid(1), expected)]));
}

#[test]
fn views_compare_reals_by_their_total_order() {
    let real = |n| {
        let mut v = View::default();
        let mut r = BTreeMap::new();
        r.insert("x".to_string(), Value::Real(n));
        v.insert("c", gid(1), r);
        v
    };
    assert_eq!(real(f64::NAN), real(f64::NAN));
    assert_ne!(real(0.0), real(-0.0));
}

// --- The schema -----------------------------------------------------------------------------

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
}

#[test]
fn a_collection_may_not_own_its_own_rows_because_concurrent_moves_could_form_a_cycle() {
    let schema = Schema::new(vec![Collection::minted("a").owner("parent", "a")]);
    assert!(schema.check().is_err());
}

#[test]
fn an_owner_may_not_be_in_a_together_set_but_a_plain_reference_may() {
    let with = |field: Collection| {
        Schema::new(vec![
            Collection::minted("b"),
            field.plain("x", Merge::Lww).together(&["o", "x"]),
        ])
    };
    assert!(
        with(Collection::minted("a").owner("o", "b"))
            .check()
            .is_err()
    );
    assert!(
        with(Collection::minted("a").reference("o", "b"))
            .check()
            .is_ok()
    );
}

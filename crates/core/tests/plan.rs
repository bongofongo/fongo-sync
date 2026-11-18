mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::{arb_case, arb_family, gid};
use fongo_sync_core::eval::{Evaluator, Target};
use fongo_sync_core::model::{
    Collection, FieldKind, Identity, Row, Schema, unmerged_identities, view,
};
use fongo_sync_core::plan::{
    FieldEdit, Planned, RowAction, Step, clashes, edits, order, record, write,
};
use fongo_sync_core::source::{Source, Store};
use fongo_sync_core::{Change, Gid, Hlc, Strategy as Merge, Value};
use proptest::prelude::*;

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

fn row(pairs: &[(&str, Value)]) -> Row {
    pairs
        .iter()
        .map(|(f, v)| (f.to_string(), v.clone()))
        .collect()
}

fn set(f: &str, v: Value) -> FieldEdit {
    FieldEdit::Set(f.into(), v)
}

fn small() -> Schema {
    Schema::new(vec![
        Collection::minted("sites")
            .plain("name", Merge::Lww)
            .plain("x", Merge::Lww)
            .plain("y", Merge::Lww)
            .together(&["x", "y"]),
        Collection::natural("rooms", &["site", "label"])
            .owner("site", "sites")
            .plain("label", Merge::Lww)
            .plain("size", Merge::Lww),
    ])
}

#[test]
fn edits_find_what_changed_and_write_a_together_set_whole() {
    let schema = small();
    let before = row(&[
        ("name", text("n")),
        ("x", Value::Int(1)),
        ("y", Value::Int(2)),
    ]);
    let after = row(&[
        ("name", text("n")),
        ("x", Value::Int(5)),
        ("y", Value::Int(2)),
    ]);
    assert_eq!(
        edits(&schema, "sites", Some(&before), Some(&after)),
        vec![set("x", Value::Int(5)), set("y", Value::Int(2))]
    );
    assert_eq!(
        edits(&schema, "sites", Some(&before), Some(&before)),
        vec![]
    );
    assert_eq!(
        edits(&schema, "sites", Some(&before), None),
        vec![FieldEdit::Deleted]
    );
    assert_eq!(
        edits(&schema, "sites", None, Some(&row(&[("name", text("n"))]))),
        vec![FieldEdit::Created, set("name", text("n"))]
    );
}

#[test]
fn a_natural_rows_key_change_is_a_new_entity() {
    let schema = small();
    let a = row(&[
        ("site", Value::Ref(gid(1))),
        ("label", text("a")),
        ("size", Value::Null),
    ]);
    let b = row(&[
        ("site", Value::Ref(gid(1))),
        ("label", text("b")),
        ("size", Value::Null),
    ]);
    assert_eq!(
        edits(&schema, "rooms", Some(&a), Some(&b)),
        vec![
            FieldEdit::Deleted,
            FieldEdit::Created,
            set("site", Value::Ref(gid(1))),
            set("label", text("b")),
        ]
    );
}

#[test]
fn writes_bring_a_row_to_its_target() {
    let t = row(&[("name", text("n")), ("x", Value::Int(1))]);
    let i = row(&[("name", text("m")), ("x", Value::Int(1))]);
    assert_eq!(write(Some(&t), Some(&t)), RowAction::Nothing);
    assert_eq!(
        write(Some(&t), Some(&i)),
        RowAction::Update(row(&[("name", text("n"))]))
    );
    assert_eq!(write(Some(&t), None), RowAction::Insert(t.clone()));
    assert_eq!(write(None, Some(&t)), RowAction::Delete);
    assert_eq!(write(None, None), RowAction::Nothing);
}

fn store(schema: &Schema, changes: &[Change]) -> Store {
    let mut store = Store::default();
    for c in changes {
        store.merge(schema, c);
    }
    store
}

fn fresh(n: u32) -> Hlc {
    Hlc {
        wall_ms: 1 << 40,
        counter: n,
        node: gid(200),
    }
}

/// A value an application could write to `field` of a live row so that the merge keeps it: any
/// value for a plain `Lww` field, a greater one for `Max`, and NULL or a live entity for a
/// reference or owner.
fn a_kept_value(
    schema: &Schema,
    live: &BTreeMap<String, Vec<Gid>>,
    coll: &Collection,
    field: &str,
    current: &Value,
    pick: usize,
) -> Option<Value> {
    let f = coll.fields.iter().find(|f| f.name == field)?;
    match &f.kind {
        FieldKind::Reference(t) | FieldKind::Owner(t) => {
            let choices = live.get(t).cloned().unwrap_or_default();
            if pick.is_multiple_of(choices.len() + 1) {
                Some(Value::Null)
            } else {
                Some(Value::Ref(choices[pick % (choices.len() + 1) - 1]))
            }
        }
        FieldKind::Plain if f.strategy == Merge::Max => {
            let bigger = Value::Text(format!("~{pick}"));
            (bigger.total_cmp(current) == std::cmp::Ordering::Greater).then_some(bigger)
        }
        FieldKind::Plain => {
            let _ = schema;
            Some(match pick % 4 {
                0 => Value::Null,
                1 => Value::Int(pick as i64),
                2 => Value::Real(0.5),
                _ => text("edited"),
            })
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1500))]

    /// Collecting an edit to a live row, merging what it records and evaluating again gives the
    /// row as edited, so nothing is written back. Likewise for deleting a row and for creating a
    /// row in a minted collection.
    #[test]
    fn recorded_edits_round_trip(
        (schema, changes) in arb_case(0..50),
        picks in proptest::collection::vec((any::<usize>(), any::<usize>(), any::<usize>()), 1..6),
    ) {
        let mut store = store(&schema, &changes);
        let ev = Evaluator::new(&schema).unwrap();
        let shown = view(&schema, &changes);
        let mut live: BTreeMap<String, Vec<Gid>> = BTreeMap::new();
        for c in &schema.collections {
            live.insert(c.name.clone(), shown.rows(&c.name).map(|(g, _)| *g).collect());
        }
        let entities: Vec<(String, Gid)> = live
            .iter()
            .flat_map(|(c, gs)| gs.iter().map(move |g| (c.clone(), *g)))
            .collect();
        for (n, &(which, field, pick)) in picks.iter().enumerate() {
            let clock = fresh(n as u32);
            if entities.is_empty() || which % 7 == 0 {
                let coll = &schema.collections[pick % schema.collections.len()];
                if !matches!(coll.identity, Identity::Minted) {
                    continue;
                }
                let new = Gid([201; 16]);
                if !store.claims(new).is_empty() {
                    continue;
                }
                let mut made = Row::new();
                for (i, f) in coll.fields.iter().enumerate() {
                    let v = a_kept_value(&schema, &live, coll, &f.name, &Value::Null, pick / (i + 1))
                        .unwrap_or(Value::Null);
                    made.insert(f.name.clone(), v);
                }
                let found = edits(&schema, &coll.name, None, Some(&made));
                for c in record(&schema, &coll.name, &found, None, Some(new), clock) {
                    store.merge(&schema, &c);
                }
                let t = ev.target(&store, &coll.name, new).unwrap();
                prop_assert_eq!(t.row.as_ref(), Some(&made));
                prop_assert_eq!(write(t.row.as_ref(), Some(&made)), RowAction::Nothing);
                return Ok(());
            }
            let (c, g) = &entities[which % entities.len()];
            let before = ev.target(&store, c, *g).unwrap();
            let Some(image) = before.row.clone() else { continue };
            let coll = schema.collection(c).unwrap();
            if which % 5 == 0 {
                let found = edits(&schema, c, Some(&image), None);
                for change in record(&schema, c, &found, Some(&before), None, clock) {
                    store.merge(&schema, &change);
                }
                prop_assert_eq!(ev.target(&store, c, *g).unwrap().row, None);
                return Ok(());
            }
            let key: Vec<String> = match &coll.identity {
                Identity::Natural(k) => k.clone(),
                Identity::Minted => Vec::new(),
            };
            let f = &coll.fields[field % coll.fields.len()];
            if key.contains(&f.name) {
                continue;
            }
            let Some(v) = a_kept_value(&schema, &live, coll, &f.name, &image[&f.name], pick) else {
                continue;
            };
            let mut edited = image.clone();
            edited.insert(f.name.clone(), v);
            let found = edits(&schema, c, Some(&image), Some(&edited));
            for change in record(&schema, c, &found, Some(&before), None, clock) {
                store.merge(&schema, &change);
            }
            let after = ev.target(&store, c, *g).unwrap();
            prop_assert_eq!(after.canonical, before.canonical);
            prop_assert_eq!(after.row.as_ref(), Some(&edited));
            prop_assert_eq!(write(after.row.as_ref(), Some(&edited)), RowAction::Nothing);
        }
    }

    /// `clashes`, asked about every live group, finds exactly the pairs `unmerged_identities`
    /// reports.
    #[test]
    fn clashes_are_the_unmerged_identities((schema, changes) in arb_case(0..60)) {
        let store = store(&schema, &changes);
        let ev = Evaluator::new(&schema).unwrap();
        let mut session = ev.session(&store);
        let shown = view(&schema, &changes);
        let expected: BTreeSet<(String, Gid, Gid)> =
            unmerged_identities(&schema, &shown).into_iter().collect();
        let mut found = BTreeSet::new();
        for c in &schema.collections {
            for (g, _) in shown.rows(&c.name) {
                let t: Target = session.target(&c.name, *g).unwrap();
                for other in clashes(&mut session, &t).unwrap() {
                    found.insert((c.name.clone(), (*g).min(other), (*g).max(other)));
                }
            }
        }
        prop_assert_eq!(found, expected);
    }
}

// --- Ordering --------------------------------------------------------------------------------

type Table = BTreeMap<(String, Gid), Row>;

fn uniques(c: &Collection) -> Vec<Vec<String>> {
    let mut sets = c.identity_keys.clone();
    if let Identity::Natural(key) = &c.identity {
        sets.push(key.clone());
    }
    sets
}

/// The first constraint `table` breaks, if any.
fn broken(schema: &Schema, table: &Table) -> Option<String> {
    for ((c, g), r) in table {
        let coll = schema.collection(c).unwrap();
        for f in &coll.fields {
            if let FieldKind::Reference(t) | FieldKind::Owner(t) = &f.kind {
                match r.get(&f.name) {
                    None | Some(Value::Null) => {}
                    Some(Value::Ref(to)) if table.contains_key(&(t.clone(), *to)) => {}
                    Some(v) => return Some(format!("{c} {g:?}.{} names missing {v:?}", f.name)),
                }
            }
        }
        for set in uniques(coll) {
            let tuple: Vec<&Value> = set
                .iter()
                .map(|f| r.get(f).unwrap_or(&Value::Null))
                .collect();
            if tuple.contains(&&Value::Null) {
                continue;
            }
            for ((c2, g2), r2) in table {
                if c2 == c
                    && g2 != g
                    && set.iter().zip(&tuple).all(|(f, v)| {
                        r2.get(f)
                            .is_some_and(|w| w.total_cmp(v) == std::cmp::Ordering::Equal)
                    })
                {
                    return Some(format!("{c} {g:?} and {g2:?} share {set:?}"));
                }
            }
        }
    }
    None
}

/// Drops and nulls until `table` breaks nothing: rows clashing on a unique set or owned by a
/// missing row go, references to missing rows become NULL.
fn repair(schema: &Schema, mut table: Table) -> Table {
    loop {
        let Some(problem) = (|| {
            for ((c, g), r) in &table {
                let coll = schema.collection(c).unwrap();
                for f in &coll.fields {
                    if let FieldKind::Reference(t) | FieldKind::Owner(t) = &f.kind
                        && let Some(Value::Ref(to)) = r.get(&f.name)
                        && !table.contains_key(&(t.clone(), *to))
                    {
                        let owned = matches!(f.kind, FieldKind::Owner(_));
                        return Some((
                            (c.clone(), *g),
                            owned.then_some(()).map_or(Some(f.name.clone()), |_| None),
                        ));
                    }
                }
                for set in uniques(coll) {
                    let tuple: Vec<&Value> = set
                        .iter()
                        .map(|f| r.get(f).unwrap_or(&Value::Null))
                        .collect();
                    if tuple.contains(&&Value::Null) {
                        continue;
                    }
                    let clash = table.iter().any(|((c2, g2), r2)| {
                        c2 == c
                            && g2 < g
                            && set.iter().zip(&tuple).all(|(f, v)| {
                                r2.get(f)
                                    .is_some_and(|w| w.total_cmp(v) == std::cmp::Ordering::Equal)
                            })
                    });
                    if clash {
                        return Some(((c.clone(), *g), None));
                    }
                }
            }
            None
        })() else {
            return table;
        };
        match problem {
            (at, Some(field)) => {
                table.get_mut(&at).unwrap().insert(field, Value::Null);
            }
            (at, None) => {
                table.remove(&at);
            }
        }
    }
}

fn arb_table(schema: &Schema) -> impl Strategy<Value = Table> + use<> {
    let schema = schema.clone();
    let cells =
        proptest::collection::vec((any::<bool>(), proptest::collection::vec(0u8..6, 6)), 4 * 5);
    cells.prop_map(move |cells| {
        let mut table = Table::new();
        for (i, (present, picks)) in cells.into_iter().enumerate() {
            let coll = &schema.collections[i / 4 % schema.collections.len()];
            if !present {
                continue;
            }
            let mut r = Row::new();
            for (f, pick) in coll.fields.iter().zip(picks.iter().cycle()) {
                let v = match &f.kind {
                    FieldKind::Reference(t) | FieldKind::Owner(t) => {
                        let t = schema
                            .collections
                            .iter()
                            .position(|c| &c.name == t)
                            .unwrap();
                        if *pick == 0 {
                            Value::Null
                        } else {
                            Value::Ref(Gid([t as u8 * 10 + pick % 4; 16]))
                        }
                    }
                    FieldKind::Plain => match pick {
                        0 => Value::Null,
                        1 | 2 => text(["a", "b"][usize::from(pick - 1)]),
                        _ => Value::Int(i64::from(pick % 2)),
                    },
                };
                r.insert(f.name.clone(), v);
            }
            let c = schema
                .collections
                .iter()
                .position(|c| c.name == coll.name)
                .unwrap();
            table.insert(
                (coll.name.clone(), Gid([c as u8 * 10 + (i % 4) as u8; 16])),
                r,
            );
        }
        repair(&schema, table)
    })
}

fn execute(schema: &Schema, table: &mut Table, steps: &[Step]) -> Result<(), String> {
    for s in steps {
        let at = (s.collection.clone(), s.gid);
        match &s.action {
            RowAction::Nothing => {}
            RowAction::Insert(r) => {
                if table.insert(at, r.clone()).is_some() {
                    return Err(format!("{s:?} inserts over a row"));
                }
            }
            RowAction::Update(fields) => {
                let Some(r) = table.get_mut(&at) else {
                    return Err(format!("{s:?} updates a missing row"));
                };
                r.extend(fields.iter().map(|(f, v)| (f.clone(), v.clone())));
            }
            RowAction::Delete => {
                if table.remove(&at).is_none() {
                    return Err(format!("{s:?} deletes a missing row"));
                }
            }
        }
        if let Some(problem) = broken(schema, table) {
            return Err(format!("after {s:?}: {problem}"));
        }
    }
    Ok(())
}

fn plan_between(schema: &Schema, before: &Table, after: &Table) -> Vec<Planned> {
    let keys: BTreeSet<&(String, Gid)> = before.keys().chain(after.keys()).collect();
    keys.into_iter()
        .map(|k| Planned {
            collection: k.0.clone(),
            gid: k.1,
            image: before.get(k).cloned(),
            action: write(after.get(k), before.get(k)),
        })
        .filter(|p| p.action != RowAction::Nothing || schema.collection(&p.collection).is_none())
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3000))]

    /// From any table that keeps its constraints to any other, the ordered plan keeps them at
    /// every step and ends at the other.
    #[test]
    fn an_ordered_plan_keeps_every_constraint_at_every_step(
        (schema, before, after, shuffle) in arb_family().prop_flat_map(|f| {
            let schema = f.schema();
            (Just(schema.clone()), arb_table(&schema), arb_table(&schema), any::<u64>())
        })
    ) {
        prop_assert_eq!(broken(&schema, &before), None);
        prop_assert_eq!(broken(&schema, &after), None);
        let mut planned = plan_between(&schema, &before, &after);
        let n = planned.len();
        for i in 0..n {
            planned.swap(i, (shuffle.wrapping_mul(i as u64 + 7) % n as u64) as usize);
        }
        let steps = order(&schema, &planned);
        let mut table = before.clone();
        if let Err(e) = execute(&schema, &mut table, &steps) {
            prop_assert!(false, "{e}\nplan: {steps:#?}");
        }
        prop_assert_eq!(table, after);
        let mut reordered = planned.clone();
        reordered.reverse();
        prop_assert_eq!(order(&schema, &reordered), steps);
    }
}

#[test]
fn a_swap_of_unique_values_is_written_in_two_steps() {
    let schema = Schema::new(vec![
        Collection::minted("sites")
            .plain("code", Merge::Lww)
            .identity_key(&["code"]),
    ]);
    let (a, b) = (gid(1), gid(2));
    let before: Table = [
        (("sites".to_string(), a), row(&[("code", text("x"))])),
        (("sites".to_string(), b), row(&[("code", text("y"))])),
    ]
    .into();
    let after: Table = [
        (("sites".to_string(), a), row(&[("code", text("y"))])),
        (("sites".to_string(), b), row(&[("code", text("x"))])),
    ]
    .into();
    let steps = order(&schema, &plan_between(&schema, &before, &after));
    assert_eq!(
        steps,
        vec![
            Step {
                collection: "sites".into(),
                gid: a,
                action: RowAction::Update(row(&[("code", Value::Null)])),
                parked: true,
            },
            Step {
                collection: "sites".into(),
                gid: b,
                action: RowAction::Update(row(&[("code", text("x"))])),
                parked: false,
            },
            Step {
                collection: "sites".into(),
                gid: a,
                action: RowAction::Update(row(&[("code", text("y"))])),
                parked: false,
            },
        ]
    );
}

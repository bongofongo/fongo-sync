mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::{arb_case, arb_family, gid};
use fongo_sync_core::eval::{Evaluator, Target};
use fongo_sync_core::model::{
    Collection, FieldKind, Identity, Row, Schema, unmerged_identities, view,
};
use fongo_sync_core::plan::{
    Columns, FieldEdit, Planned, RowAction, Step, clashes, edits, order, record, write,
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
            let revives = matches!(f.kind, FieldKind::Owner(_)) && before.live != before.members;
            if revives {
                // Moving the entity moves members that were dead only through their old owner;
                // they come back and their fields compete as any live member's do.
                prop_assert_eq!(&after.row.as_ref().unwrap()[&f.name], &edited[&f.name]);
                continue;
            }
            prop_assert_eq!(after.row.as_ref(), Some(&edited));
            prop_assert_eq!(write(after.row.as_ref(), Some(&edited)), RowAction::Nothing);
        }
    }

    /// `clashes`, asked about every live group, finds exactly the pairs `unmerged_identities`
    /// reports.
    #[test]
    fn edits_report_only_what_the_application_changed(
        (schema, changes) in arb_case(0..50),
        pick in any::<usize>(),
        parked in proptest::collection::vec(any::<bool>(), 8),
        value in prop_oneof![Just(Value::Null), Just(Value::Int(7)), Just(text("edited"))],
    ) {
        // Whatever the adapter wrote into a row (a reference that reads NULL because its target is
        // not live here, a parked NULL) is in the image, so it is never reported as an edit: only
        // the field the application set, and the plain fields of a `together` set it belongs to.
        let store = store(&schema, &changes);
        let ev = Evaluator::new(&schema).unwrap();
        let mut session = ev.session(&store);
        let shown = view(&schema, &changes);
        let rows: Vec<(String, Gid)> = schema
            .collections
            .iter()
            .flat_map(|c| shown.rows(&c.name).map(move |(g, _)| (c.name.clone(), *g)))
            .collect();
        if rows.is_empty() {
            return Ok(());
        }
        let (c, g) = &rows[pick % rows.len()];
        let coll = schema.collection(c).unwrap();
        let mut image = session.target(c, *g).unwrap().row.unwrap();
        for (f, park) in coll.fields.iter().zip(&parked) {
            if *park && !matches!(f.kind, FieldKind::Plain) {
                image.insert(f.name.clone(), Value::Null);
            }
        }
        prop_assert_eq!(edits(&schema, c, Some(&image), Some(&image)), vec![]);
        let key: Vec<String> = match &coll.identity {
            Identity::Natural(k) => k.clone(),
            Identity::Minted => Vec::new(),
        };
        let field = &coll.fields[pick / 7 % coll.fields.len()];
        if key.contains(&field.name) {
            return Ok(());
        }
        let mut row = image.clone();
        row.insert(field.name.clone(), value);
        let set: Vec<&String> = coll
            .together
            .iter()
            .find(|s| s.contains(&field.name))
            .map(|s| s.iter().collect())
            .unwrap_or_default();
        for e in edits(&schema, c, Some(&image), Some(&row)) {
            let FieldEdit::Set(f, v) = e else {
                prop_assert!(false, "{:?} is not an edit of one field", e);
                unreachable!();
            };
            prop_assert!(f == field.name || set.contains(&&f), "{} reported, {} set", f, field.name);
            prop_assert!(
                f == field.name
                    || coll.fields.iter().any(|d| d.name == f && d.kind == FieldKind::Plain)
            );
            prop_assert_eq!(&v, &row[&f]);
        }
    }

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

/// A small table store that enforces what a database would: NOT NULL, foreign keys and unique
/// sets.
struct Tables<'a> {
    schema: &'a Schema,
    columns: &'a Columns,
}

impl Tables<'_> {
    /// The first constraint `table` breaks, if any.
    fn broken(&self, table: &Table) -> Option<String> {
        for ((c, g), r) in table {
            let coll = self.schema.collection(c).unwrap();
            for f in &coll.fields {
                let v = r.get(&f.name).unwrap_or(&Value::Null);
                if *v == Value::Null && !self.columns.accepts_null(c, &f.name) {
                    return Some(format!("{c} {g:?}.{} is NULL", f.name));
                }
                if let FieldKind::Reference(t) | FieldKind::Owner(t) = &f.kind {
                    match v {
                        Value::Null => {}
                        Value::Ref(to) if table.contains_key(&(t.clone(), *to)) => {}
                        v => return Some(format!("{c} {g:?}.{} names missing {v:?}", f.name)),
                    }
                }
            }
            for set in uniques(coll) {
                if let Some(other) = self.clash(table, c, *g, r, &set) {
                    return Some(format!("{c} {g:?} and {other:?} share {set:?}"));
                }
            }
        }
        None
    }

    fn clash(&self, table: &Table, c: &str, g: Gid, r: &Row, set: &[String]) -> Option<Gid> {
        let tuple: Vec<&Value> = set
            .iter()
            .map(|f| r.get(f).unwrap_or(&Value::Null))
            .collect();
        if tuple.contains(&&Value::Null) {
            return None;
        }
        table.iter().find_map(|((c2, g2), r2)| {
            let same_tuple = set.iter().zip(&tuple).all(|(f, v)| {
                r2.get(f)
                    .is_some_and(|w| w.total_cmp(v) == std::cmp::Ordering::Equal)
            });
            (c2 == c && *g2 != g && same_tuple).then_some(*g2)
        })
    }

    /// Drops rows and nulls references until `table` breaks nothing.
    fn repair(&self, mut table: Table) -> Table {
        while let Some(problem) = self.broken(&table) {
            let at = table
                .keys()
                .find(|k| problem.starts_with(&format!("{} {:?}", k.0, k.1)))
                .cloned()
                .unwrap();
            let coll = self.schema.collection(&at.0).unwrap();
            let dangling = coll.fields.iter().find(|f| {
                matches!(f.kind, FieldKind::Reference(_))
                    && self.columns.accepts_null(&at.0, &f.name)
                    && problem.contains(&format!(".{} names missing", f.name))
            });
            match dangling {
                Some(f) => {
                    table
                        .get_mut(&at)
                        .unwrap()
                        .insert(f.name.clone(), Value::Null);
                }
                None => {
                    table.remove(&at);
                }
            }
        }
        table
    }

    fn apply(&self, table: &mut Table, step: &Step) -> Result<(), String> {
        let at = (step.collection.clone(), step.gid);
        match &step.action {
            RowAction::Nothing => {}
            RowAction::Insert(r) => {
                if table.insert(at, r.clone()).is_some() {
                    return Err(format!("{step:?} inserts over a row"));
                }
            }
            RowAction::Update(fields) => {
                let Some(r) = table.get_mut(&at) else {
                    return Err(format!("{step:?} updates a missing row"));
                };
                r.extend(fields.iter().map(|(f, v)| (f.clone(), v.clone())));
            }
            RowAction::Delete => {
                if table.remove(&at).is_none() {
                    return Err(format!("{step:?} deletes a missing row"));
                }
            }
        }
        match self.broken(table) {
            Some(problem) => Err(format!("after {step:?}: {problem}")),
            None => Ok(()),
        }
    }

    /// Whether the actions can all be done one at a time, each whole, in some order: tries any
    /// action that keeps the constraints until none is left or none can go.
    fn greedy(&self, before: &Table, planned: &[Planned]) -> bool {
        let mut table = before.clone();
        let mut left: Vec<&Planned> = planned.iter().collect();
        while !left.is_empty() {
            let Some(i) = left.iter().position(|p| {
                let mut t = table.clone();
                let step = Step {
                    collection: p.collection.clone(),
                    gid: p.gid,
                    action: p.action.clone(),
                    parked: false,
                };
                self.apply(&mut t, &step).is_ok()
            }) else {
                return false;
            };
            let p = left.remove(i);
            let step = Step {
                collection: p.collection.clone(),
                gid: p.gid,
                action: p.action.clone(),
                parked: false,
            };
            self.apply(&mut table, &step).unwrap();
        }
        true
    }
}

fn arb_table(schema: &Schema) -> impl Strategy<Value = Table> + use<> {
    let schema = schema.clone();
    let cells =
        proptest::collection::vec((any::<bool>(), proptest::collection::vec(0u8..6, 6)), 4 * 5);
    cells.prop_map(move |cells| {
        let mut table = Table::new();
        for (i, (present, picks)) in cells.into_iter().enumerate() {
            let c = i / 4 % schema.collections.len();
            let coll = &schema.collections[c];
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
            table.insert(
                (coll.name.clone(), Gid([c as u8 * 10 + (i % 4) as u8; 16])),
                r,
            );
        }
        table
    })
}

fn arb_columns(schema: &Schema) -> impl Strategy<Value = Columns> + use<> {
    let fields: Vec<(String, String)> = schema
        .collections
        .iter()
        .flat_map(|c| {
            c.fields
                .iter()
                .map(move |f| (c.name.clone(), f.name.clone()))
        })
        .collect();
    proptest::collection::vec(0u8..4, fields.len()).prop_map(move |picks| {
        let mut columns = Columns::new();
        for ((c, f), pick) in fields.iter().zip(picks) {
            if pick == 0 {
                columns = columns.not_null(c, f);
            }
        }
        columns
    })
}

fn plan_between(before: &Table, after: &Table) -> Vec<Planned> {
    let keys: BTreeSet<&(String, Gid)> = before.keys().chain(after.keys()).collect();
    keys.into_iter()
        .map(|k| Planned {
            collection: k.0.clone(),
            gid: k.1,
            image: before.get(k).cloned(),
            action: write(after.get(k), before.get(k)),
        })
        .filter(|p| p.action != RowAction::Nothing)
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3000))]

    /// From any table that keeps its constraints to any other, every step of the plan keeps
    /// them, every row the plan does not report stuck ends as it should, and a stuck row is left
    /// as it was or parked. Where the actions can be done whole in some order, nothing is parked
    /// and nothing is stuck. The plan does not depend on the order the actions arrive in.
    #[test]
    fn an_ordered_plan_keeps_every_constraint_at_every_step(
        (schema, columns, before, after, shuffle) in arb_family().prop_flat_map(|f| {
            let schema = f.schema();
            (Just(schema.clone()), arb_columns(&schema), arb_table(&schema), arb_table(&schema), any::<u64>())
        })
    ) {
        let tables = Tables { schema: &schema, columns: &columns };
        let (before, after) = (tables.repair(before), tables.repair(after));
        let mut planned = plan_between(&before, &after);
        let n = planned.len();
        for i in 0..n {
            planned.swap(i, (shuffle.wrapping_mul(i as u64 + 7) % n as u64) as usize);
        }
        let plan = order(&schema, &columns, &planned);
        let mut table = before.clone();
        for step in &plan.steps {
            if let Err(e) = tables.apply(&mut table, step) {
                prop_assert!(false, "{e}\nplan: {plan:#?}");
            }
        }
        for p in &planned {
            let at = (p.collection.clone(), p.gid);
            if plan.stuck.contains(&at) {
                let parked = plan.steps.iter().any(|s| s.parked && (s.collection.clone(), s.gid) == at);
                prop_assert!(parked || table.get(&at) == before.get(&at), "{at:?} was touched");
            } else {
                prop_assert_eq!(table.get(&at), after.get(&at), "{:?}", at);
            }
        }
        if tables.greedy(&before, &planned) {
            prop_assert!(plan.stuck.is_empty(), "stuck where no cycle is: {plan:#?}");
            prop_assert!(plan.steps.iter().all(|s| !s.parked), "parked where no cycle is: {plan:#?}");
        }
        let mut reordered = planned.clone();
        reordered.reverse();
        prop_assert_eq!(order(&schema, &columns, &reordered), plan);
    }
}

fn update(c: &str, n: u8, before: Row, after: Row) -> Planned {
    Planned {
        collection: c.into(),
        gid: gid(n),
        image: Some(before.clone()),
        action: write(Some(&after), Some(&before)),
    }
}

fn step(c: &str, n: u8, fields: &[(&str, Value)], parked: bool) -> Step {
    Step {
        collection: c.into(),
        gid: gid(n),
        action: RowAction::Update(row(fields)),
        parked,
    }
}

#[test]
fn a_swap_of_unique_values_parks_one_field_of_one_row() {
    let schema = Schema::new(vec![
        Collection::minted("sites")
            .plain("code", Merge::Lww)
            .identity_key(&["code"]),
    ]);
    let planned = [
        update(
            "sites",
            1,
            row(&[("code", text("x"))]),
            row(&[("code", text("y"))]),
        ),
        update(
            "sites",
            2,
            row(&[("code", text("y"))]),
            row(&[("code", text("x"))]),
        ),
    ];
    let plan = order(&schema, &Columns::new(), &planned);
    assert_eq!(
        plan.steps,
        vec![
            step("sites", 1, &[("code", Value::Null)], true),
            step("sites", 2, &[("code", text("x"))], false),
            step("sites", 1, &[("code", text("y"))], false),
        ]
    );
    assert!(plan.stuck.is_empty());
}

fn chapters() -> Schema {
    Schema::new(vec![
        Collection::minted("books").plain("name", Merge::Lww),
        Collection::minted("chapters")
            .owner("book", "books")
            .plain("n", Merge::Lww)
            .identity_key(&["book", "n"]),
    ])
}

fn swapped_chapters() -> [Planned; 2] {
    let book = Value::Ref(gid(9));
    let at = |n: i64| row(&[("book", book.clone()), ("n", Value::Int(n))]);
    [
        update("chapters", 1, at(1), at(2)),
        update("chapters", 2, at(2), at(1)),
    ]
}

#[test]
fn a_swap_on_a_key_that_includes_an_owner_parks_only_the_field_that_moves() {
    let plan = order(&chapters(), &Columns::new(), &swapped_chapters());
    assert_eq!(
        plan.steps[0],
        step("chapters", 1, &[("n", Value::Null)], true)
    );
    assert_eq!(plan.steps.len(), 3);
    let only_owner = Columns::new().not_null("chapters", "n");
    let plan = order(&chapters(), &only_owner, &swapped_chapters());
    assert_eq!(
        plan.steps[0],
        step("chapters", 1, &[("book", Value::Null)], true)
    );
    assert!(plan.stuck.is_empty());
}

#[test]
fn a_swap_no_field_of_which_accepts_null_is_reported_stuck_and_not_written() {
    let columns = Columns::new()
        .not_null("chapters", "n")
        .not_null("chapters", "book");
    let plan = order(&chapters(), &columns, &swapped_chapters());
    assert_eq!(plan.steps, vec![]);
    assert_eq!(
        plan.stuck,
        vec![
            ("chapters".to_string(), gid(1)),
            ("chapters".to_string(), gid(2))
        ]
    );
}

#[test]
fn a_row_on_no_cycle_is_never_parked() {
    let schema = Schema::new(vec![
        Collection::minted("sites")
            .plain("code", Merge::Lww)
            .plain("tag", Merge::Lww)
            .identity_key(&["code"])
            .identity_key(&["tag"]),
    ]);
    let site = |code: &str, tag: &str| row(&[("code", text(code)), ("tag", text(tag))]);
    // Rows 2 and 3 swap codes; row 1 takes row 2's old tag, and row 4 takes row 1's.
    let planned = [
        update("sites", 2, site("x", "q"), site("y", "r")),
        update("sites", 3, site("y", "m"), site("x", "m")),
        update("sites", 1, site("w", "p"), site("w", "q")),
        update("sites", 4, site("u", "s"), site("u", "p")),
    ];
    let plan = order(&schema, &Columns::new(), &planned);
    let parked: Vec<&Step> = plan.steps.iter().filter(|s| s.parked).collect();
    assert_eq!(
        parked,
        vec![&step("sites", 2, &[("code", Value::Null)], true)]
    );
    assert!(plan.stuck.is_empty());
}

#[test]
fn moving_a_merged_entity_moves_every_member_even_one_dead_for_now() {
    let schema = Schema::new(vec![
        Collection::minted("sites").plain("name", Merge::Lww),
        Collection::minted("items")
            .plain("text", Merge::Lww)
            .owner("site", "sites"),
    ]);
    let ev = Evaluator::new(&schema).unwrap();
    let (s, t, u) = (gid(1), gid(2), gid(3));
    let (a, b) = (gid(10), gid(11));
    let at = |wall_ms: u64, counter: u32| Hlc {
        wall_ms,
        counter,
        node: gid(9),
    };
    let ch = |c: &str, g: Gid, f: &str, v: Value, n: u32| Change {
        collection: c.into(),
        gid: g,
        field: f.into(),
        value: v,
        clock: at(1, n),
    };
    // Member b is dead here only because its owner t has not arrived.
    let mut known = vec![
        ch("sites", s, "name", text("S"), 0),
        ch("sites", u, "name", text("U"), 1),
        ch("items", a, "site", Value::Ref(s), 2),
        ch("items", a, "text", text("x"), 3),
        ch("items", b, "site", Value::Ref(t), 4),
        ch("items", b, fongo_sync_core::model::ALIAS, Value::Ref(a), 5),
    ];
    let before = ev.target(&store(&schema, &known), "items", a).unwrap();
    assert_eq!(
        (before.members.clone(), before.live.clone()),
        (vec![a, b], vec![a])
    );
    let moved = record(
        &schema,
        "items",
        &[set("site", Value::Ref(u))],
        Some(&before),
        None,
        at(5, 0),
    );
    let movers: BTreeSet<Gid> = moved.iter().map(|c| c.gid).collect();
    assert_eq!(movers, [a, b].into());
    known.extend(moved);
    known.push(ch("sites", t, "name", text("T"), 6));
    let shown: Vec<Row> = view(&schema, &known)
        .rows("items")
        .map(|(_, r)| r.clone())
        .collect();
    assert_eq!(
        shown,
        vec![row(&[("site", Value::Ref(u)), ("text", text("x"))])]
    );
    known.push(Change {
        clock: at(6, 0),
        ..ch("sites", u, fongo_sync_core::DELETED, Value::Int(1), 0)
    });
    assert_eq!(view(&schema, &known).rows("items").count(), 0);
}

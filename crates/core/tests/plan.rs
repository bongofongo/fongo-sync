mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::{arb_case, arb_family, gid};
use fongo_sync_core::eval::{Evaluator, Target};
use fongo_sync_core::model::{
    Collection, FieldKind, Identity, Row, Schema, unmerged_identities, view,
};
use fongo_sync_core::plan::{
    Columns, FieldEdit, NewEntity, Plan, Planned, RowAction, Step, clashes, edits, order, record,
    write,
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
                let new = NewEntity { gid: new, known: false };
                for c in record(&schema, &coll.name, &found, None, Some(new), clock) {
                    store.merge(&schema, &c);
                }
                let t = ev.target(&store, &coll.name, new.gid).unwrap();
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

    /// Whether row `at`'s final write waits, through rows whose final writes are still pending, on
    /// itself, judged from `table` as it stands: a row waits on a row its write would refer to
    /// that is not there yet, on a row holding a unique value its write takes, and, to be
    /// deleted, on a row still referring to it that will stop.
    fn on_a_cycle(
        &self,
        table: &Table,
        planned: &[Planned],
        pending: &BTreeSet<(String, Gid)>,
        at: &(String, Gid),
    ) -> bool {
        let finals: BTreeMap<(String, Gid), Option<Row>> = planned
            .iter()
            .map(|p| {
                let row = match &p.action {
                    RowAction::Insert(r) => Some(r.clone()),
                    RowAction::Update(f) => {
                        let mut r = p.image.clone().unwrap_or_default();
                        r.extend(f.clone());
                        Some(r)
                    }
                    RowAction::Delete => None,
                    RowAction::Nothing => p.image.clone(),
                };
                ((p.collection.clone(), p.gid), row)
            })
            .collect();
        let waits_on = |x: &(String, Gid)| -> Vec<(String, Gid)> {
            let mut out = Vec::new();
            let coll = self.schema.collection(&x.0).unwrap();
            match &finals[x] {
                Some(row) => {
                    for f in &coll.fields {
                        if let (
                            FieldKind::Reference(t) | FieldKind::Owner(t),
                            Some(Value::Ref(g)),
                        ) = (&f.kind, row.get(&f.name))
                        {
                            let y = (t.clone(), *g);
                            if !table.contains_key(&y) && pending.contains(&y) {
                                out.push(y);
                            }
                        }
                    }
                    for set in uniques(coll) {
                        if let Some(other) = self.clash(table, &x.0, x.1, row, &set) {
                            let y = (x.0.clone(), other);
                            if pending.contains(&y) {
                                out.push(y);
                            }
                        }
                    }
                }
                None => {
                    for ((c, g), r) in table {
                        let y = (c.clone(), *g);
                        let refers = self.schema.collection(c).unwrap().fields.iter().any(|f| {
                            matches!(&f.kind, FieldKind::Reference(t) | FieldKind::Owner(t) if *t == x.0)
                                && r.get(&f.name) == Some(&Value::Ref(x.1))
                        });
                        if refers && y != *x && pending.contains(&y) {
                            out.push(y);
                        }
                    }
                }
            }
            out
        };
        let mut seen = BTreeSet::new();
        let mut stack = waits_on(at);
        while let Some(y) = stack.pop() {
            if y == *at {
                return true;
            }
            if seen.insert(y.clone()) {
                stack.extend(waits_on(&y));
            }
        }
        false
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

/// Rows referring to their own collection, two identity keys (one of two fields), an owner, and a
/// natural collection under that owner that refers back into itself outside its key.
fn tangled() -> Schema {
    Schema::new(vec![
        Collection::minted("pages")
            .plain("slug", Merge::Lww)
            .plain("rank", Merge::Lww)
            .plain("lang", Merge::Lww)
            .reference("next", "pages")
            .reference("cover", "media")
            .identity_key(&["slug"])
            .identity_key(&["rank", "lang"]),
        Collection::minted("media")
            .plain("hash", Merge::Lww)
            .owner("page", "pages")
            .identity_key(&["hash"]),
        Collection::natural("notes", &["medium", "n"])
            .owner("medium", "media")
            .plain("n", Merge::Lww)
            .reference("see", "notes"),
    ])
}

/// From any table that keeps its constraints to any other: every step of the plan keeps them;
/// every row the plan does not report stuck ends as it should; a stuck row gets no step at all,
/// so it is left as it was. Where the actions can be done whole in some order, nothing is parked
/// and nothing is stuck. The plan does not depend on the order the actions arrive in.
fn plan_law(
    schema: &Schema,
    columns: &Columns,
    before: Table,
    after: Table,
    shuffle: u64,
) -> Result<(), TestCaseError> {
    let tables = Tables { schema, columns };
    let (before, after) = (tables.repair(before), tables.repair(after));
    let mut planned = plan_between(&before, &after);
    for (k, row) in &before {
        if shuffle.rotate_left(k.1.0[15] as u32).is_multiple_of(3) && after.get(k) == Some(row) {
            planned.push(Planned {
                collection: k.0.clone(),
                gid: k.1,
                image: Some(row.clone()),
                action: RowAction::Nothing,
            });
        }
    }
    let n = planned.len();
    for i in 0..n {
        planned.swap(i, (shuffle.wrapping_mul(i as u64 + 7) % n as u64) as usize);
    }
    let plan = order(schema, columns, &planned);
    let mut table = before.clone();
    let mut pending: BTreeSet<(String, Gid)> = planned
        .iter()
        .filter(|p| p.action != RowAction::Nothing)
        .map(|p| (p.collection.clone(), p.gid))
        .collect();
    for step in &plan.steps {
        let at = (step.collection.clone(), step.gid);
        prop_assert!(
            !plan.stuck.contains(&at),
            "a step for stuck {at:?}: {plan:#?}"
        );
        if step.parked {
            prop_assert!(
                tables.on_a_cycle(&table, &planned, &pending, &at),
                "{at:?} parked though on no cycle of what waits: {plan:#?}"
            );
        } else {
            pending.remove(&at);
        }
        if let Err(e) = tables.apply(&mut table, step) {
            prop_assert!(false, "{e}\nplan: {plan:#?}");
        }
    }
    for p in &planned {
        let at = (p.collection.clone(), p.gid);
        let wanted = if plan.stuck.contains(&at) {
            before.get(&at)
        } else {
            after.get(&at)
        };
        prop_assert_eq!(table.get(&at), wanted, "{:?}", at);
    }
    if tables.greedy(&before, &planned) {
        prop_assert!(plan.stuck.is_empty(), "stuck where no cycle is: {plan:#?}");
        prop_assert!(
            plan.steps.iter().all(|s| !s.parked),
            "parked where no cycle is: {plan:#?}"
        );
    }
    let mut reordered = planned.clone();
    reordered.reverse();
    prop_assert_eq!(order(schema, columns, &reordered), plan);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(3000))]

    #[test]
    fn an_ordered_plan_keeps_every_constraint_at_every_step(
        (schema, columns, before, after, shuffle) in arb_family().prop_flat_map(|f| {
            let schema = f.schema();
            (Just(schema.clone()), arb_columns(&schema), arb_table(&schema), arb_table(&schema), any::<u64>())
        })
    ) {
        plan_law(&schema, &columns, before, after, shuffle)?;
    }

    #[test]
    fn an_ordered_plan_keeps_every_constraint_over_tangled_tables(
        (columns, before, after, shuffle) in (
            arb_columns(&tangled()), arb_table(&tangled()), arb_table(&tangled()), any::<u64>()
        )
    ) {
        plan_law(&tangled(), &columns, before, after, shuffle)?;
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

#[test]
fn an_entity_created_again_under_its_natural_key_does_not_bring_back_old_values() {
    let schema = Schema::new(vec![
        Collection::natural("tags", &["label"])
            .plain("label", Merge::Lww)
            .plain("note", Merge::Lww)
            .plain("hits", Merge::Max),
    ]);
    let tag = Gid::derive("tags", &[text("x")]);
    let clock = |n| Hlc {
        wall_ms: n,
        counter: 0,
        node: gid(1),
    };
    let first = row(&[
        ("label", text("x")),
        ("note", text("old")),
        ("hits", Value::Int(5)),
    ]);
    let fresh = NewEntity {
        gid: tag,
        known: false,
    };
    let mut log = record(
        &schema,
        "tags",
        &edits(&schema, "tags", None, Some(&first)),
        None,
        Some(fresh),
        clock(1),
    );
    let shown = Evaluator::new(&schema)
        .unwrap()
        .target(&store(&schema, &log), "tags", tag)
        .unwrap();
    log.extend(record(
        &schema,
        "tags",
        &edits(&schema, "tags", Some(&first), None),
        Some(&shown),
        None,
        clock(2),
    ));
    let again = row(&[
        ("label", text("x")),
        ("note", Value::Null),
        ("hits", Value::Int(0)),
    ]);
    let known = NewEntity {
        gid: tag,
        known: true,
    };
    log.extend(record(
        &schema,
        "tags",
        &edits(&schema, "tags", None, Some(&again)),
        None,
        Some(known),
        clock(3),
    ));
    // The note is reset; a Max field cannot be, and keeps the greatest value written.
    let expected = row(&[
        ("label", text("x")),
        ("note", Value::Null),
        ("hits", Value::Int(5)),
    ]);
    assert_eq!(view(&schema, &log).row("tags", tag), Some(&expected));
}

#[test]
fn clashes_on_a_key_led_by_a_reference_look_through_the_referenced_group() {
    let schema = Schema::new(vec![
        Collection::minted("sites").plain("name", Merge::Lww),
        Collection::minted("items")
            .reference("link", "sites")
            .plain("text", Merge::Lww)
            .identity_key(&["link", "text"]),
    ]);
    let at = |n: u64| Hlc {
        wall_ms: n,
        counter: 0,
        node: gid(1),
    };
    let ch = |c: &str, g: Gid, f: &str, v: Value, n: u64| Change {
        collection: c.into(),
        gid: g,
        field: f.into(),
        value: v,
        clock: at(n),
    };
    let (s1, s2, a, b) = (gid(1), gid(2), gid(10), gid(11));
    let changes = vec![
        ch("sites", s1, "name", Value::Int(1), 1),
        ch("sites", s2, "name", Value::Int(2), 1),
        ch(
            "sites",
            s2,
            fongo_sync_core::model::ALIAS,
            Value::Ref(s1),
            2,
        ),
        ch("items", a, "link", Value::Ref(s1), 3),
        ch("items", a, "text", Value::Int(0), 3),
        ch("items", b, "link", Value::Ref(s2), 4),
        ch("items", b, "text", Value::Int(0), 4),
    ];
    assert_eq!(
        unmerged_identities(&schema, &view(&schema, &changes)).len(),
        1
    );
    let store = store(&schema, &changes);
    let ev = Evaluator::new(&schema).unwrap();
    let mut session = ev.session(&store);
    let t = session.target("items", a).unwrap();
    assert_eq!(clashes(&mut session, &t).unwrap(), [b].into());
}

#[test]
fn independent_writes_come_in_ascending_order() {
    let schema = Schema::new(vec![Collection::minted("sites").plain("name", Merge::Lww)]);
    let insert = |n: u8| Planned {
        collection: "sites".into(),
        gid: gid(n),
        image: None,
        action: RowAction::Insert(row(&[("name", Value::Int(n.into()))])),
    };
    let plan = order(&schema, &Columns::new(), &[insert(3), insert(1), insert(2)]);
    let order_of: Vec<Gid> = plan.steps.iter().map(|s| s.gid).collect();
    assert_eq!(order_of, vec![gid(1), gid(2), gid(3)]);
}

fn one_change_per_register(changes: &[Change]) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for c in changes {
        if !seen.insert((c.gid, c.field.clone())) {
            return Err(format!(
                "two changes to {:?}.{} in {changes:?}",
                c.gid, c.field
            ));
        }
    }
    Ok(())
}

#[test]
fn a_key_change_back_to_a_member_of_the_group_tombstones_only_the_others() {
    let schema = small();
    let (s, room) = (
        gid(1),
        Gid::derive("rooms", &[Value::Ref(gid(1)), text("a")]),
    );
    let old = Target {
        collection: "rooms".into(),
        canonical: gid(5).min(room),
        members: {
            let mut m = vec![gid(5), room];
            m.sort();
            m
        },
        live: vec![gid(5)],
        row: Some(row(&[
            ("site", Value::Ref(s)),
            ("label", text("b")),
            ("size", Value::Null),
        ])),
    };
    let found = vec![
        FieldEdit::Deleted,
        FieldEdit::Created,
        set("site", Value::Ref(s)),
        set("label", text("a")),
    ];
    let new = NewEntity {
        gid: room,
        known: true,
    };
    let recorded = record(&schema, "rooms", &found, Some(&old), Some(new), fresh(0));
    one_change_per_register(&recorded).unwrap();
    let tombstones: Vec<(Gid, Value)> = recorded
        .iter()
        .filter(|c| c.field == fongo_sync_core::DELETED)
        .map(|c| (c.gid, c.value.clone()))
        .collect();
    assert!(tombstones.contains(&(gid(5), Value::Int(1))));
    assert!(tombstones.contains(&(room, Value::Int(0))));
    assert_eq!(tombstones.len(), 2);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1500))]

    /// Whatever the edits and whichever entity a row stood for and now stands for, `record`
    /// writes each field of each gid at most once.
    #[test]
    fn record_writes_each_register_at_most_once(
        (schema, changes) in arb_case(0..50),
        pick in any::<usize>(),
        shape in 0u8..4,
        fields in proptest::collection::vec(any::<usize>(), 0..6),
        known in any::<bool>(),
    ) {
        let store = store(&schema, &changes);
        let ev = Evaluator::new(&schema).unwrap();
        let mut session = ev.session(&store);
        let mut targets = Vec::new();
        for c in &schema.collections {
            for g in common::mentioned(&schema, &changes) {
                let t = session.target(&c.name, g).unwrap();
                if !t.members.is_empty() {
                    targets.push(t);
                }
            }
        }
        if targets.is_empty() {
            return Ok(());
        }
        let old = &targets[pick % targets.len()];
        let coll = schema.collection(&old.collection).unwrap();
        let mut found = Vec::new();
        if shape >= 1 {
            found.push(FieldEdit::Deleted);
        }
        if shape >= 2 {
            found.push(FieldEdit::Created);
        }
        if shape != 1 {
            let mut named = BTreeSet::new();
            for f in &fields {
                let field = &coll.fields[f % coll.fields.len()].name;
                if named.insert(field.clone()) {
                    found.push(set(field, Value::Int(*f as i64 % 3)));
                }
            }
        }
        let new_gid = if shape == 3 { old.members[pick % old.members.len()] } else { Gid([202; 16]) };
        let new = (shape >= 2).then_some(NewEntity { gid: new_gid, known });
        let recorded = record(&schema, &old.collection, &found, Some(old), new, fresh(0));
        if let Err(e) = one_change_per_register(&recorded) {
            prop_assert!(false, "{e}");
        }
    }
}

#[test]
fn a_row_a_parking_leaves_off_every_cycle_is_not_parked() {
    // Four rows deleted together, each referring to the next: a to b, z to a, b to c, and c to
    // both b and z. Parking a breaks the long cycle; z then waits on nothing that waits on it,
    // and only b and c still form a cycle.
    let schema = Schema::new(vec![
        Collection::minted("n")
            .reference("r1", "n")
            .reference("r2", "n"),
    ]);
    let (a, z, b, c) = (gid(1), gid(2), gid(3), gid(4));
    let deleted = |g: Gid, refs: &[(&str, Gid)]| Planned {
        collection: "n".into(),
        gid: g,
        image: Some(
            refs.iter()
                .map(|(f, t)| (f.to_string(), Value::Ref(*t)))
                .collect(),
        ),
        action: RowAction::Delete,
    };
    let planned = [
        deleted(a, &[("r1", b)]),
        deleted(z, &[("r1", a)]),
        deleted(b, &[("r1", c)]),
        deleted(c, &[("r1", b), ("r2", z)]),
    ];
    let plan = order(&schema, &Columns::new(), &planned);
    let parked: Vec<Gid> = plan
        .steps
        .iter()
        .filter(|s| s.parked)
        .map(|s| s.gid)
        .collect();
    assert_eq!(parked, vec![a, b]);
    assert!(plan.stuck.is_empty());
}

/// Rows of one collection inserted together, each naming rows by `x`, which refuses NULL, and by
/// `y` and `z`, which accept it.
fn linked() -> (Schema, Columns) {
    let schema = Schema::new(vec![
        Collection::minted("a")
            .reference("x", "a")
            .reference("y", "a")
            .reference("z", "a"),
    ]);
    (schema, Columns::new().not_null("a", "x"))
}

fn inserted(n: u8, refs: &[(&str, u8)]) -> Planned {
    Planned {
        collection: "a".into(),
        gid: gid(n),
        image: None,
        action: RowAction::Insert(
            refs.iter()
                .map(|(f, t)| (f.to_string(), Value::Ref(gid(*t))))
                .collect(),
        ),
    }
}

fn parked_rows(plan: &Plan) -> Vec<Gid> {
    plan.steps
        .iter()
        .filter(|s| s.parked)
        .map(|s| s.gid)
        .collect()
}

#[test]
fn a_cycle_that_waits_on_another_is_broken_once_the_other_is() {
    let (schema, columns) = linked();
    // 1 and 2 wait on each other, and only 1 can be inserted early, once 5 exists; 5 waits on 3,
    // which with 4 forms a cycle that inserting 3 early breaks.
    let planned = [
        inserted(1, &[("x", 5), ("y", 2)]),
        inserted(2, &[("x", 1)]),
        inserted(3, &[("x", 3), ("y", 4)]),
        inserted(4, &[("x", 4), ("y", 3)]),
        inserted(5, &[("x", 3)]),
    ];
    let plan = order(&schema, &columns, &planned);
    assert!(plan.stuck.is_empty(), "{plan:#?}");
    assert_eq!(parked_rows(&plan), vec![gid(3), gid(1)]);
    // 1 also waits on 3, which with 4 forms a cycle that inserting 3 early breaks.
    let planned = [
        inserted(1, &[("x", 3), ("y", 2)]),
        inserted(2, &[("x", 1)]),
        inserted(3, &[("x", 3), ("y", 4)]),
        inserted(4, &[("x", 3)]),
    ];
    let plan = order(&schema, &columns, &planned);
    assert!(plan.stuck.is_empty(), "{plan:#?}");
    assert_eq!(parked_rows(&plan), vec![gid(3), gid(1)]);
}

#[test]
fn a_row_that_could_not_be_parked_is_tried_again_once_what_it_waited_on_is_written() {
    let (schema, columns) = linked();
    // One set: 2 → 3 → 1 → 4 → 6 → 2, and 4 with 5. Parking 1 splits it into 2 with 3 and 4
    // with 5. Neither 2 nor 3 can be inserted early while 6 does not exist; parking 4 lets 6 be
    // written, and then 2 can be parked.
    let planned = [
        inserted(1, &[("y", 3)]),
        inserted(2, &[("x", 6), ("y", 3)]),
        inserted(3, &[("x", 2)]),
        inserted(4, &[("x", 4), ("y", 5), ("z", 1)]),
        inserted(5, &[("x", 5), ("z", 4)]),
        inserted(6, &[("x", 4)]),
    ];
    let plan = order(&schema, &columns, &planned);
    assert!(plan.stuck.is_empty(), "{plan:#?}");
    assert_eq!(parked_rows(&plan), vec![gid(1), gid(4), gid(2)]);
}

#[test]
fn a_row_that_could_not_be_parked_is_tried_again_once_what_it_waited_on_is_parked() {
    let (schema, columns) = linked();
    // As above, but 2 waits on 4 itself, which parking 4 inserts.
    let planned = [
        inserted(1, &[("y", 3)]),
        inserted(2, &[("x", 4), ("y", 3)]),
        inserted(3, &[("x", 2)]),
        inserted(4, &[("x", 4), ("y", 5), ("z", 1)]),
        inserted(5, &[("x", 5), ("z", 4)]),
    ];
    let plan = order(&schema, &columns, &planned);
    assert!(plan.stuck.is_empty(), "{plan:#?}");
    assert_eq!(parked_rows(&plan), vec![gid(1), gid(4), gid(2)]);
}

/// From one table of `n` rows referring to each other at random to another, with `x` refusing
/// NULL or not: every step keeps the constraints, every row not stuck ends as it should, and
/// where every field accepts NULL nothing is stuck. This holds for sets of rows too large for the
/// search for the fewest parkings, which are planned by parking whatever can be.
fn large_plan_law(seed: u64, n: u8, x_not_null: bool) -> Result<(), String> {
    let mut rng = common::Rng(seed | 1);
    let schema = linked().0;
    let columns = if x_not_null {
        Columns::new().not_null("a", "x")
    } else {
        Columns::new()
    };
    let tables = Tables {
        schema: &schema,
        columns: &columns,
    };
    let random = |rng: &mut common::Rng| -> Table {
        let mut table = Table::new();
        for i in 0..n {
            if rng.one_in(4) {
                continue;
            }
            let mut r = Row::new();
            for f in ["x", "y", "z"] {
                let to = Value::Ref(gid(rng.below(n as usize) as u8));
                r.insert(f.into(), if rng.one_in(5) { Value::Null } else { to });
            }
            table.insert(("a".into(), gid(i)), r);
        }
        tables.repair(table)
    };
    let before = random(&mut rng);
    let after = random(&mut rng);
    let planned = plan_between(&before, &after);
    let plan = order(&schema, &columns, &planned);
    if !x_not_null && !plan.stuck.is_empty() {
        return Err(format!("stuck though every field accepts NULL: {:?}", plan.stuck));
    }
    let mut table = before.clone();
    for step in &plan.steps {
        if plan.stuck.contains(&(step.collection.clone(), step.gid)) {
            return Err(format!("a step for stuck {step:?}"));
        }
        tables.apply(&mut table, step)?;
    }
    for p in &planned {
        let at = (p.collection.clone(), p.gid);
        let wanted = if plan.stuck.contains(&at) {
            before.get(&at)
        } else {
            after.get(&at)
        };
        if table.get(&at) != wanted {
            return Err(format!("{at:?} ends as {:?}", table.get(&at)));
        }
    }
    Ok(())
}

#[test]
fn large_sets_of_rows_waiting_on_each_other_are_written_within_the_constraints() {
    for seed in 0..48 {
        large_plan_law(seed, 250, seed % 2 == 0).unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}

#[test]
fn a_long_doubly_linked_list_inserted_at_once_is_written() {
    let (schema, _) = linked();
    let n: u8 = 250;
    let planned: Vec<Planned> = (0..n)
        .map(|i| {
            let mut refs = Vec::new();
            if i > 0 {
                refs.push(("y", i - 1));
            }
            if i + 1 < n {
                refs.push(("z", i + 1));
            }
            inserted(i, &refs)
        })
        .collect();
    let columns = Columns::new();
    let plan = order(&schema, &columns, &planned);
    assert!(plan.stuck.is_empty());
    let tables = Tables {
        schema: &schema,
        columns: &columns,
    };
    let mut table = Table::new();
    for step in &plan.steps {
        tables.apply(&mut table, step).unwrap();
    }
    assert_eq!(table.len(), n as usize);
}

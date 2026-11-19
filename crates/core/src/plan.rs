//! Decisions about rows, over plain values: which changes an application's edits to a row make,
//! what to write to a row to bring it to its target, which groups an identity key says are one
//! entity, and in what order a set of row writes can be made against tables with unique and
//! foreign-key constraints.
//!
//! An adapter keeps, for every row, an image: the values it last wrote to or read from that row.
//! A row is only ever compared with its image, never with merged state.

use std::collections::{BTreeMap, BTreeSet};

use crate::change::{Change, DELETED};
use crate::clock::Hlc;
use crate::eval::{Session, Target, Undetermined};
use crate::model::{Collection, FieldKind, Identity, Row, Schema};
use crate::source::Source;
use crate::total::{Total, same};
use crate::value::{Gid, Value};

/// Something an application did to a row, as [`edits`] finds it.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldEdit {
    /// The row names an entity it did not name before: it is new, or in a natural collection its
    /// key changed.
    Created,
    /// The row no longer names the entity it named: it was deleted, or in a natural collection
    /// its key changed.
    Deleted,
    /// The field holds this value now.
    Set(String, Value),
}

/// What the application did to a row of `collection`, found by comparing the row with its image
/// and reading nothing else. `None` for the image means the adapter has never seen the row; `None`
/// for the row means it is gone. Values are compared by `Value::total_cmp`; a field absent from a
/// row is NULL; undeclared fields are ignored.
///
/// - A new row: `Created`, then `Set` for every key field of a natural collection, every non-NULL
///   field, and every field of a `together` set any of whose fields is set.
/// - A gone row: `Deleted`.
/// - A row of a natural collection whose key changed: `Deleted`, then as for a new row.
/// - Otherwise `Set` for every changed field, and for every field of a `together` set any of whose
///   fields changed, so that the set is written at one clock.
///
/// `Set`s come in the schema's field order.
pub fn edits(
    schema: &Schema,
    collection: &str,
    image: Option<&Row>,
    row: Option<&Row>,
) -> Vec<FieldEdit> {
    let Some(coll) = schema.collection(collection) else {
        return Vec::new();
    };
    let get = |r: &Row, f: &str| r.get(f).cloned().unwrap_or(Value::Null);
    let key: &[String] = match &coll.identity {
        Identity::Natural(key) => key,
        Identity::Minted => &[],
    };
    let create = |row: &Row| {
        let mut out = vec![FieldEdit::Created];
        let chosen = expand(coll, |f| {
            key.iter().any(|k| k == f) || get(row, f) != Value::Null
        });
        out.extend(
            chosen
                .into_iter()
                .map(|f| FieldEdit::Set(f.clone(), get(row, f))),
        );
        out
    };
    match (image, row) {
        (None, None) => Vec::new(),
        (Some(_), None) => vec![FieldEdit::Deleted],
        (None, Some(row)) => create(row),
        (Some(image), Some(row)) => {
            if key.iter().any(|f| !same(&get(image, f), &get(row, f))) {
                let mut out = vec![FieldEdit::Deleted];
                out.extend(create(row));
                return out;
            }
            expand(coll, |f| !same(&get(image, f), &get(row, f)))
                .into_iter()
                .map(|f| FieldEdit::Set(f.clone(), get(row, f)))
                .collect()
        }
    }
}

/// The declared fields `pick` chooses, with every `together` set that holds one of them whole.
fn expand(coll: &Collection, pick: impl Fn(&str) -> bool) -> Vec<&String> {
    let picked: BTreeSet<&str> = coll
        .fields
        .iter()
        .map(|f| f.name.as_str())
        .filter(|f| pick(f))
        .collect();
    coll.fields
        .iter()
        .map(|f| &f.name)
        .filter(|f| {
            picked.contains(f.as_str())
                || coll
                    .together
                    .iter()
                    .any(|set| set.contains(f) && set.iter().any(|g| picked.contains(g.as_str())))
        })
        .collect()
}

/// The changes that record `edits` to a row of `collection`, all at `clock`, which must be a fresh
/// reading of the replica's clock. Values must be as changes carry them: references as gids.
///
/// - `Deleted` writes `$deleted = 1` to every member of `old`, the target of the entity the row
///   named, except `new`.
/// - `Created` writes `$deleted = 0` to `new`, the gid of the entity the row now names (a fresh
///   gid, or for a natural collection the gid its key derives), and every `Set` to it.
/// - Otherwise each `Set` goes to the smallest live member of `old` (its canonical gid if none is
///   live), so the write is one the group shows. A `Set` of an owner field goes to every member,
///   live or not: a member that is dead only because its owner has not arrived here would
///   otherwise come back under its old owner when that owner arrives. A member that was dead
///   only through its owner comes back with the move, and its fields compete as a live
///   member's do.
///
/// Returns nothing for `Created` without `new`, or for a `Set` or `Deleted` without `old`.
pub fn record(
    schema: &Schema,
    collection: &str,
    edits: &[FieldEdit],
    old: Option<&Target>,
    new: Option<Gid>,
    clock: Hlc,
) -> Vec<Change> {
    let Some(coll) = schema.collection(collection) else {
        return Vec::new();
    };
    let change = |gid: Gid, field: &str, value: Value| Change {
        collection: collection.to_string(),
        gid,
        field: field.to_string(),
        value,
        clock,
    };
    let created = edits.contains(&FieldEdit::Created);
    let mut out = Vec::new();
    if edits.contains(&FieldEdit::Deleted)
        && let Some(old) = old
    {
        for &m in &old.members {
            if Some(m) != new {
                out.push(change(m, DELETED, Value::Int(1)));
            }
        }
    }
    if created {
        let Some(new) = new else {
            return out;
        };
        out.push(change(new, DELETED, Value::Int(0)));
        for e in edits {
            if let FieldEdit::Set(f, v) = e {
                out.push(change(new, f, v.clone()));
            }
        }
        return out;
    }
    let Some(old) = old else {
        return out;
    };
    let writer = old.live.first().copied().unwrap_or(old.canonical);
    for e in edits {
        let FieldEdit::Set(f, v) = e else {
            continue;
        };
        let owner = coll
            .fields
            .iter()
            .any(|d| &d.name == f && matches!(d.kind, FieldKind::Owner(_)));
        if owner {
            for &m in &old.members {
                out.push(change(m, f, v.clone()));
            }
        } else {
            out.push(change(writer, f, v.clone()));
        }
    }
    out
}

/// A write to one row.
#[derive(Debug, Clone, PartialEq)]
pub enum RowAction {
    Nothing,
    /// Insert a row holding these values.
    Insert(Row),
    /// Set these fields; the others keep what the image says.
    Update(Row),
    Delete,
}

/// The write that brings a row whose image is `image` (`None`: no row) to `target` (`None`: no
/// row), comparing values by `Value::total_cmp`. Both must be in the same terms, references
/// translated the same way.
pub fn write(target: Option<&Row>, image: Option<&Row>) -> RowAction {
    match (target, image) {
        (None, None) => RowAction::Nothing,
        (None, Some(_)) => RowAction::Delete,
        (Some(t), None) => RowAction::Insert(t.clone()),
        (Some(t), Some(i)) => {
            let changed: Row = t
                .iter()
                .filter(|(f, v)| i.get(*f).is_none_or(|old| !same(old, v)))
                .map(|(f, v)| (f.clone(), v.clone()))
                .collect();
            if changed.is_empty() {
                RowAction::Nothing
            } else {
                RowAction::Update(changed)
            }
        }
    }
}

/// The canonical gids of the other live groups of `target`'s collection whose rows agree with
/// `target`'s, non-NULL, on every field of one of its identity keys. An adapter that finds any
/// emits an alias joining them. Uses the same groups, liveness and rows as [`Session::target`].
///
/// Cost: one `holding` question per member of the group a reference in the key's first field
/// names (one question if that field is plain), and one target per gid found.
pub fn clashes<S: Source>(
    session: &mut Session<'_, S>,
    target: &Target,
) -> Result<BTreeSet<Gid>, Undetermined> {
    let mut found = BTreeSet::new();
    let schema = session.schema;
    let (Some(coll), Some(row)) = (schema.collection(&target.collection), &target.row) else {
        return Ok(found);
    };
    for key in &coll.identity_keys {
        let values: Vec<&Value> = key
            .iter()
            .map(|f| row.get(f).unwrap_or(&Value::Null))
            .collect();
        if values.contains(&&Value::Null) {
            continue;
        }
        let first = &key[0];
        let kind = &coll
            .fields
            .iter()
            .find(|f| &f.name == first)
            .expect("checked schema")
            .kind;
        let held: Vec<Value> = match (kind, values[0]) {
            (FieldKind::Reference(t) | FieldKind::Owner(t), Value::Ref(c)) => session
                .target(t, *c)?
                .members
                .into_iter()
                .map(Value::Ref)
                .collect(),
            (_, v) => vec![v.clone()],
        };
        let mut candidates = BTreeSet::new();
        for v in &held {
            candidates.extend(session.source.holding(&coll.name, &[(first, v)]));
        }
        for c in candidates {
            let other = session.target(&coll.name, c)?;
            let Some(other_row) = &other.row else {
                continue;
            };
            let agree = key
                .iter()
                .zip(&values)
                .all(|(f, v)| other_row.get(f).is_some_and(|o| same(o, v)));
            if agree && other.canonical != target.canonical {
                found.insert(other.canonical);
            }
        }
    }
    Ok(found)
}

/// A row write to be ordered: the row's collection and identity, its image, and its action.
/// References in the image and the action are `Value::Ref` of the identities of the rows they
/// name.
#[derive(Debug, Clone, PartialEq)]
pub struct Planned {
    pub collection: String,
    pub gid: Gid,
    pub image: Option<Row>,
    pub action: RowAction,
}

/// One write in a plan. A `parked` write is the first of two to its row: it sets to NULL the
/// fields that keep a cycle of writes from proceeding (or inserts the row with them NULL), and a
/// later step writes their final values. An adapter records every write, parked or not, in the
/// row's image as what was written.
#[derive(Debug, Clone, PartialEq)]
pub struct Step {
    pub collection: String,
    pub gid: Gid,
    pub action: RowAction,
    pub parked: bool,
}

/// What [`order`] needs to know of an adapter's tables beyond the schema: which columns refuse
/// NULL. Every column accepts NULL unless named.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Columns {
    not_null: BTreeSet<(String, String)>,
}

impl Columns {
    /// Columns that all accept NULL.
    pub fn new() -> Columns {
        Columns::default()
    }

    /// The same columns, with `collection.field` refusing NULL.
    pub fn not_null(mut self, collection: &str, field: &str) -> Columns {
        self.not_null
            .insert((collection.to_string(), field.to_string()));
        self
    }

    pub fn accepts_null(&self, collection: &str, field: &str) -> bool {
        !self
            .not_null
            .contains(&(collection.to_string(), field.to_string()))
    }
}

/// The writes [`order`] puts in order, and the rows it cannot write, ascending: those on a cycle
/// no permitted parking breaks, and those that wait on them. An adapter holds the stuck rows, leaves
/// them as they are, and tries again later.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Plan {
    pub steps: Vec<Step>,
    pub stuck: Vec<(String, Gid)>,
}

/// What a requirement waits for, and so which fields parking its source sets to NULL.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Need {
    /// The source row, being inserted, exists. Parking inserts it with every field that makes it
    /// wait set to NULL.
    Exists,
    /// The source row stops referring, through this field, to a row being deleted.
    Unrefer(String),
    /// The source row lets go of its values for the unique set with this index.
    Release(usize),
}

/// An order for `planned` that tables with these constraints accept one write at a time: no NULL
/// in a column `columns` says refuses it, every reference and owner field NULL or naming an
/// existing row, and no two rows agreeing, non-NULL, on every field of an identity key or of a
/// natural key. If the rows before (the images and the rows no action touches) and after (with
/// every action done) both satisfy them, every step does too, and only stuck rows are left
/// unwritten.
///
/// A row is written after the rows it will refer to are inserted, after any row now holding the
/// unique values it takes has let go of them, and it is deleted after the rows that refer to it
/// stop doing so. Where these requirements form a cycle (two rows swapping a unique value, rows
/// inserted referring to each other, a deleted owner and a deleted owned row that refers back),
/// one row on the cycle is parked: only the fields that keep the cycle from proceeding are set to
/// NULL, choosing, for a unique set, a field the row's own write changes where one accepts NULL.
/// The smallest (collection, gid) that can be parked is. A row on no cycle is never parked; a
/// cycle no row of which can be parked within `columns` is reported as stuck.
///
/// Cost: O((n + e) log n) for n rows and e requirements (at most the number of reference and
/// unique fields across the rows), plus O(n + e) for each cycle broken.
pub fn order(schema: &Schema, columns: &Columns, planned: &[Planned]) -> Plan {
    let n = planned.len();
    let ids: Vec<(String, Gid)> = planned
        .iter()
        .map(|p| (p.collection.clone(), p.gid))
        .collect();
    let at: BTreeMap<(String, Gid), usize> = ids
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, k)| (k, i))
        .collect();
    let refs = |coll: &str| -> Vec<(String, String)> {
        schema
            .collection(coll)
            .map(|c| {
                c.fields
                    .iter()
                    .filter_map(|f| match &f.kind {
                        FieldKind::Reference(t) | FieldKind::Owner(t) => {
                            Some((f.name.clone(), t.clone()))
                        }
                        FieldKind::Plain => None,
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    let uniques = |coll: &str| -> Vec<Vec<String>> {
        schema
            .collection(coll)
            .map(|c| {
                let mut sets = c.identity_keys.clone();
                if let Identity::Natural(key) = &c.identity {
                    sets.push(key.clone());
                }
                sets
            })
            .unwrap_or_default()
    };
    let after = |p: &Planned| -> Option<Row> {
        match &p.action {
            RowAction::Insert(row) => Some(row.clone()),
            RowAction::Update(fields) => {
                let mut row = p.image.clone().unwrap_or_default();
                row.extend(fields.iter().map(|(f, v)| (f.clone(), v.clone())));
                Some(row)
            }
            RowAction::Delete => None,
            RowAction::Nothing => p.image.clone(),
        }
    };
    let tuple = |row: &Row, set: &[String]| -> Option<Vec<Total>> {
        let values: Vec<Value> = set
            .iter()
            .map(|f| row.get(f).cloned().unwrap_or(Value::Null))
            .collect();
        (!values.contains(&Value::Null)).then(|| values.into_iter().map(Total).collect())
    };
    let finals: Vec<Option<Row>> = planned.iter().map(after).collect();

    let mut holders: BTreeMap<(String, usize, Vec<Total>), Vec<usize>> = BTreeMap::new();
    for (i, p) in planned.iter().enumerate() {
        if let Some(image) = &p.image {
            for (u, set) in uniques(&p.collection).iter().enumerate() {
                if let Some(t) = tuple(image, set) {
                    holders
                        .entry((p.collection.clone(), u, t))
                        .or_default()
                        .push(i);
                }
            }
        }
    }

    // Requirements: (source, target, what the source must do first).
    let mut edges: BTreeSet<(usize, usize, Need)> = BTreeSet::new();
    for (v, p) in planned.iter().enumerate() {
        let written: Option<&Row> = match &p.action {
            RowAction::Insert(row) | RowAction::Update(row) => Some(row),
            _ => None,
        };
        for (field, target) in refs(&p.collection) {
            if let Some(Value::Ref(g)) = written.and_then(|w| w.get(&field))
                && let Some(&u) = at.get(&(target.clone(), *g))
                && matches!(planned[u].action, RowAction::Insert(_))
                && u != v
            {
                edges.insert((u, v, Need::Exists));
            }
            if let Some(Value::Ref(g)) = p.image.as_ref().and_then(|i| i.get(&field))
                && let Some(&u) = at.get(&(target.clone(), *g))
                && planned[u].action == RowAction::Delete
                && u != v
            {
                let lets_go = finals[v]
                    .as_ref()
                    .is_none_or(|f| f.get(&field).is_none_or(|now| !same(now, &Value::Ref(*g))));
                if lets_go {
                    edges.insert((v, u, Need::Unrefer(field.clone())));
                }
            }
        }
        let (Some(finished), Some(_)) = (&finals[v], written) else {
            continue;
        };
        for (u, set) in uniques(&p.collection).iter().enumerate() {
            let Some(t) = tuple(finished, set) else {
                continue;
            };
            for &h in holders
                .get(&(p.collection.clone(), u, t))
                .into_iter()
                .flatten()
            {
                let was = planned[h].image.as_ref().and_then(|i| tuple(i, set));
                let now = finals[h].as_ref().and_then(|r| tuple(r, set));
                if h != v && was != now {
                    edges.insert((h, v, Need::Release(u)));
                }
            }
        }
    }

    // Which field of unique set `u` parking row `r` sets to NULL: one its write changes if any
    // accepts NULL, else any that does.
    let release_field = |r: usize, u: usize| -> Option<String> {
        let p = &planned[r];
        let set = &uniques(&p.collection)[u];
        let open = |f: &&String| columns.accepts_null(&p.collection, f);
        let changes = |f: &&String| {
            let was = p.image.as_ref().and_then(|i| i.get(*f));
            let now = finals[r].as_ref().and_then(|row| row.get(*f));
            match (was, now) {
                (Some(a), Some(b)) => !same(a, b),
                _ => true,
            }
        };
        set.iter()
            .filter(open)
            .find(changes)
            .or_else(|| set.iter().find(open))
            .cloned()
    };

    let mut out: Vec<BTreeSet<(usize, Need)>> = vec![BTreeSet::new(); n];
    let mut into: Vec<BTreeSet<(usize, Need)>> = vec![BTreeSet::new(); n];
    for (from, to, need) in edges {
        out[from].insert((to, need.clone()));
        into[to].insert((from, need));
    }
    let mut done = vec![false; n];
    let mut stuck = vec![false; n];
    let mut parked: Vec<Option<Row>> = vec![None; n];
    let mut ready: BTreeSet<((String, Gid), usize)> = (0..n)
        .filter(|&i| into[i].is_empty())
        .map(|i| (ids[i].clone(), i))
        .collect();
    let mut steps = Vec::new();
    let drop_edge = |from: usize,
                     to: usize,
                     need: &Need,
                     out: &mut Vec<BTreeSet<(usize, Need)>>,
                     into: &mut Vec<BTreeSet<(usize, Need)>>,
                     ready: &mut BTreeSet<((String, Gid), usize)>,
                     done: &[bool],
                     stuck: &[bool]| {
        out[from].remove(&(to, need.clone()));
        into[to].remove(&(from, need.clone()));
        if into[to].is_empty() && !done[to] && !stuck[to] {
            ready.insert((ids[to].clone(), to));
        }
    };
    loop {
        while let Some((_, i)) = ready.pop_first() {
            done[i] = true;
            let p = &planned[i];
            let action = match (&parked[i], &p.action) {
                (_, RowAction::Delete) => RowAction::Delete,
                (Some(image), _) => write(finals[i].as_ref(), Some(image)),
                (None, action) => action.clone(),
            };
            if action != RowAction::Nothing {
                steps.push(Step {
                    collection: p.collection.clone(),
                    gid: p.gid,
                    action,
                    parked: false,
                });
            }
            for (to, need) in out[i].clone() {
                drop_edge(i, to, &need, &mut out, &mut into, &mut ready, &done, &stuck);
            }
        }
        let left: Vec<usize> = (0..n).filter(|&i| !done[i] && !stuck[i]).collect();
        if left.is_empty() {
            break;
        }
        let components = cycles(&left, &out);
        if components.is_empty() {
            // What is left waits on stuck rows.
            for i in left {
                stuck[i] = true;
            }
            break;
        }
        let on_cycle: BTreeMap<usize, usize> = components
            .iter()
            .enumerate()
            .flat_map(|(c, members)| members.iter().map(move |&m| (m, c)))
            .collect();
        let mut candidates: Vec<usize> = on_cycle
            .keys()
            .copied()
            .filter(|&r| parked[r].is_none())
            .collect();
        candidates.sort_by(|a, b| ids[*a].cmp(&ids[*b]));
        let mut broke = false;
        for r in candidates {
            let p = &planned[r];
            let same_cycle = |m: usize| on_cycle.get(&m) == Some(&on_cycle[&r]);
            // Fields to set NULL, and the requirements that parking satisfies.
            let mut nulls: BTreeSet<String> = BTreeSet::new();
            let mut satisfied: Vec<(usize, Need)> = Vec::new();
            let mut possible = true;
            if let RowAction::Insert(_) = p.action {
                // Inserted early, the row must wait for nothing: every field that makes it wait
                // goes in NULL, and it then exists for every row waiting on that.
                for (field, target) in refs(&p.collection) {
                    let waits = matches!(
                        finals[r].as_ref().and_then(|row| row.get(&field)),
                        Some(Value::Ref(g)) if at.get(&(target.clone(), *g)).is_some_and(|u| {
                            into[r].contains(&(*u, Need::Exists))
                        })
                    );
                    if waits {
                        if columns.accepts_null(&p.collection, &field) {
                            nulls.insert(field);
                        } else {
                            possible = false;
                        }
                    }
                }
                for (_, need) in &into[r] {
                    if let Need::Release(u) = need {
                        match release_field(r, *u) {
                            Some(f) => {
                                nulls.insert(f);
                            }
                            None => possible = false,
                        }
                    }
                }
                satisfied.extend(
                    out[r]
                        .iter()
                        .filter(|(_, need)| *need == Need::Exists)
                        .cloned(),
                );
            } else {
                for (to, need) in &out[r] {
                    if !same_cycle(*to) {
                        continue;
                    }
                    let field = match need {
                        Need::Unrefer(f) => Some(f.clone()),
                        Need::Release(u) => release_field(r, *u),
                        Need::Exists => None,
                    };
                    if let Some(f) = field.filter(|f| columns.accepts_null(&p.collection, f)) {
                        nulls.insert(f);
                        satisfied.push((*to, need.clone()));
                    }
                }
            }
            if !possible || satisfied.is_empty() {
                continue;
            }
            let (action, image) = match (&p.action, &p.image) {
                (RowAction::Insert(row), _) => {
                    let mut first = row.clone();
                    for f in &nulls {
                        first.insert(f.clone(), Value::Null);
                    }
                    (RowAction::Insert(first.clone()), first)
                }
                (_, Some(image)) => {
                    let set: Row = nulls
                        .iter()
                        .filter(|f| image.get(*f).is_some_and(|v| *v != Value::Null))
                        .map(|f| (f.clone(), Value::Null))
                        .collect();
                    let mut now = image.clone();
                    now.extend(set.clone());
                    (RowAction::Update(set), now)
                }
                (_, None) => continue,
            };
            steps.push(Step {
                collection: p.collection.clone(),
                gid: p.gid,
                action,
                parked: true,
            });
            parked[r] = Some(image);
            for (to, need) in satisfied {
                drop_edge(r, to, &need, &mut out, &mut into, &mut ready, &done, &stuck);
            }
            broke = true;
            break;
        }
        if !broke {
            let first = components
                .iter()
                .min_by_key(|members| members.iter().map(|&m| ids[m].clone()).min())
                .expect("a cycle");
            for &m in first {
                stuck[m] = true;
            }
        }
    }
    let mut stuck: Vec<(String, Gid)> = (0..n)
        .filter(|&i| stuck[i])
        .map(|i| ids[i].clone())
        .collect();
    stuck.sort();
    Plan { steps, stuck }
}

/// The strongly connected components of more than one row among `left`, by `out`'s edges.
fn cycles(left: &[usize], out: &[BTreeSet<(usize, Need)>]) -> Vec<Vec<usize>> {
    let inside: BTreeSet<usize> = left.iter().copied().collect();
    let mut index = BTreeMap::new();
    let mut low = BTreeMap::new();
    let mut on_stack = BTreeSet::new();
    let mut stack = Vec::new();
    let mut found = Vec::new();
    let mut counter = 0;
    for &start in left {
        if index.contains_key(&start) {
            continue;
        }
        // Iterative Tarjan: (node, the targets still to visit).
        let mut work: Vec<(usize, Vec<usize>)> = Vec::new();
        let next = |v: usize| -> Vec<usize> {
            let mut t: Vec<usize> = out[v]
                .iter()
                .map(|(to, _)| *to)
                .filter(|to| inside.contains(to))
                .collect();
            t.dedup();
            t.reverse();
            t
        };
        index.insert(start, counter);
        low.insert(start, counter);
        counter += 1;
        stack.push(start);
        on_stack.insert(start);
        work.push((start, next(start)));
        while let Some((v, mut todo)) = work.pop() {
            if let Some(w) = todo.pop() {
                work.push((v, todo));
                if let Some(&seen) = index.get(&w) {
                    if on_stack.contains(&w) {
                        let l = low[&v].min(seen);
                        low.insert(v, l);
                    }
                } else {
                    index.insert(w, counter);
                    low.insert(w, counter);
                    counter += 1;
                    stack.push(w);
                    on_stack.insert(w);
                    work.push((w, next(w)));
                }
                continue;
            }
            if low[&v] == index[&v] {
                let mut component = Vec::new();
                loop {
                    let w = stack.pop().expect("on the stack");
                    on_stack.remove(&w);
                    component.push(w);
                    if w == v {
                        break;
                    }
                }
                if component.len() > 1 {
                    found.push(component);
                }
            }
            if let Some((parent, _)) = work.last() {
                let l = low[parent].min(low[&v]);
                low.insert(*parent, l);
            }
        }
    }
    found
}

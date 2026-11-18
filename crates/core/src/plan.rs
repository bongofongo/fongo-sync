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

/// One write in a plan. A `parked` write is the first of two to its row: it sets the row's
/// reference, owner and unique fields to NULL (or inserts the row with them NULL) so that other
/// writes can proceed, and a later step writes the final values. An adapter records every write,
/// parked or not, in the row's image as what was written.
#[derive(Debug, Clone, PartialEq)]
pub struct Step {
    pub collection: String,
    pub gid: Gid,
    pub action: RowAction,
    pub parked: bool,
}

/// An order for `planned` that tables with these constraints accept one write at a time: every
/// reference and owner field is NULL or names an existing row, and no two rows agree, non-NULL, on
/// every field of an identity key or of a natural key. If the rows before (the images and the
/// rows no action touches) and after (with every action done) both satisfy them, every step of
/// the plan does too.
///
/// A row is written after the rows it will refer to are inserted, and after any row now holding
/// the unique values it takes has let go of them; it is deleted after the rows that refer to it
/// stop doing so. Where these requirements form a cycle (two rows swapping a unique value, rows
/// inserted referring to each other, a deleted owner and a deleted owned row that refers back),
/// the smallest row that others wait on is parked first. Ties go to the smaller (collection, gid).
///
/// Cost: O((n + e) log n) for n rows and e requirements, e being at most the number of reference
/// and unique fields across the rows.
pub fn order(schema: &Schema, planned: &[Planned]) -> Vec<Step> {
    let id = |p: &Planned| (p.collection.clone(), p.gid);
    let at: BTreeMap<(String, Gid), usize> = planned
        .iter()
        .enumerate()
        .map(|(i, p)| (id(p), i))
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
    let tuple = |row: &Row, set: &[String]| -> Option<Vec<Value>> {
        let values: Vec<Value> = set
            .iter()
            .map(|f| row.get(f).cloned().unwrap_or(Value::Null))
            .collect();
        (!values.contains(&Value::Null)).then_some(values)
    };
    let mut holders: BTreeMap<(String, usize, Vec<Total>), Vec<usize>> = BTreeMap::new();
    for (i, p) in planned.iter().enumerate() {
        if let Some(image) = &p.image {
            for (u, set) in uniques(&p.collection).iter().enumerate() {
                if let Some(t) = tuple(image, set) {
                    let key = t.into_iter().map(Total).collect();
                    holders
                        .entry((p.collection.clone(), u, key))
                        .or_default()
                        .push(i);
                }
            }
        }
    }

    let n = planned.len();
    let mut out: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); n];
    let mut into = vec![0usize; n];
    let mut need = |from: usize, to: usize, out: &mut Vec<BTreeSet<usize>>| {
        if from != to && out[from].insert(to) {
            into[to] += 1;
        }
    };
    for (v, p) in planned.iter().enumerate() {
        let finished = after(p);
        let written: Option<&Row> = match &p.action {
            RowAction::Insert(row) | RowAction::Update(row) => Some(row),
            _ => None,
        };
        for (field, target) in refs(&p.collection) {
            if let Some(Value::Ref(g)) = written.and_then(|w| w.get(&field))
                && let Some(&u) = at.get(&(target.clone(), *g))
                && matches!(planned[u].action, RowAction::Insert(_))
            {
                need(u, v, &mut out);
            }
            if let Some(Value::Ref(g)) = p.image.as_ref().and_then(|i| i.get(&field))
                && let Some(&u) = at.get(&(target.clone(), *g))
                && planned[u].action == RowAction::Delete
            {
                let lets_go = finished
                    .as_ref()
                    .is_none_or(|f| f.get(&field).is_none_or(|now| !same(now, &Value::Ref(*g))));
                if lets_go {
                    need(v, u, &mut out);
                }
            }
        }
        let Some(finished) = &finished else {
            continue;
        };
        if written.is_none() {
            continue;
        }
        for (u, set) in uniques(&p.collection).iter().enumerate() {
            let Some(t) = tuple(finished, set) else {
                continue;
            };
            let key: Vec<Total> = t.into_iter().map(Total).collect();
            for &h in holders
                .get(&(p.collection.clone(), u, key))
                .into_iter()
                .flatten()
            {
                let moves = after(&planned[h])
                    .and_then(|r| tuple(&r, set))
                    .is_none_or(|now| {
                        planned[h]
                            .image
                            .as_ref()
                            .and_then(|i| tuple(i, set))
                            .is_none_or(|was| now.iter().zip(&was).any(|(a, b)| !same(a, b)))
                    });
                if moves {
                    need(h, v, &mut out);
                }
            }
        }
    }

    let parkable = |p: &Planned| -> BTreeSet<String> {
        let mut fields: BTreeSet<String> =
            refs(&p.collection).into_iter().map(|(f, _)| f).collect();
        for set in uniques(&p.collection) {
            fields.extend(set);
        }
        fields
    };
    let ids: Vec<(String, Gid)> = planned.iter().map(id).collect();
    let mut ready: BTreeSet<((String, Gid), usize)> = (0..n)
        .filter(|&i| into[i] == 0)
        .map(|i| (ids[i].clone(), i))
        .collect();
    let mut done = vec![false; n];
    let mut parked: Vec<Option<Row>> = vec![None; n];
    let mut steps = Vec::new();
    let release = |i: usize,
                   out: &mut Vec<BTreeSet<usize>>,
                   into: &mut Vec<usize>,
                   ready: &mut BTreeSet<((String, Gid), usize)>,
                   done: &[bool]| {
        for to in std::mem::take(&mut out[i]) {
            into[to] -= 1;
            if into[to] == 0 && !done[to] {
                ready.insert((ids[to].clone(), to));
            }
        }
    };
    let mut left = n;
    while left > 0 {
        if let Some(next) = ready.pop_first() {
            let i = next.1;
            done[i] = true;
            left -= 1;
            let p = &planned[i];
            let action = match (&parked[i], &p.action) {
                (_, RowAction::Delete) => RowAction::Delete,
                (Some(image), _) => write(after(p).as_ref(), Some(image)),
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
            release(i, &mut out, &mut into, &mut ready, &done);
            continue;
        }
        let Some(i) = (0..n)
            .filter(|&i| !done[i] && parked[i].is_none() && !out[i].is_empty())
            .min_by_key(|&i| &ids[i])
        else {
            break;
        };
        let p = &planned[i];
        let fields = parkable(p);
        let (action, image) = match (&p.action, &p.image) {
            (RowAction::Insert(row), _) => {
                let mut first = row.clone();
                for f in &fields {
                    if first.contains_key(f) {
                        first.insert(f.clone(), Value::Null);
                    }
                }
                (RowAction::Insert(first.clone()), first)
            }
            (_, Some(image)) => {
                let nulls: Row = fields
                    .iter()
                    .filter(|f| image.get(*f).is_some_and(|v| *v != Value::Null))
                    .map(|f| (f.clone(), Value::Null))
                    .collect();
                let mut now = image.clone();
                now.extend(nulls.clone());
                (RowAction::Update(nulls), now)
            }
            (_, None) => (RowAction::Nothing, Row::new()),
        };
        steps.push(Step {
            collection: p.collection.clone(),
            gid: p.gid,
            action,
            parked: true,
        });
        parked[i] = Some(image);
        release(i, &mut out, &mut into, &mut ready, &done);
    }
    steps
}

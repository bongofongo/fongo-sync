//! Decisions about rows, over plain values: which changes an application's edits to a row make,
//! what to write to a row to bring it to its target, which groups an identity key says are one
//! entity, and in what order a set of row writes can be made against tables with NOT NULL,
//! unique and foreign-key constraints.
//!
//! # What an adapter must do
//!
//! An adapter keeps the facts [`crate::source`] describes, a mapping from each local row to the
//! gid it stands for, and for every row an image: the values it last wrote to or read from that
//! row. `crates/core/tests/sim` is such an adapter over in-memory tables. It must:
//!
//! - Collect before it applies, in one transaction, so that at apply time every row equals its
//!   image.
//! - Compare a row only with its image, never with merged state, and record every value it
//!   writes into a row (a reference that reads NULL because its target is not shown, a parked
//!   value) in the image, so that [`edits`] never reports it.
//! - Compare in gid terms: the image through the row-to-gid mapping as it was before this
//!   collect, the row through the mapping as it will be after it. When a natural key changes,
//!   the row stands for a new entity, and rows that refer to it by local key now refer to that
//!   entity: mark them for collecting too.
//! - Decide that a natural row's key changed by comparing key values, never by deriving a gid
//!   from the row: what the adapter wrote need not derive the entity's gid.
//! - Refuse, when an application registers a natural collection, key columns that accept NULL
//!   (two rows with a NULL in the key are one entity, which no unique constraint enforces), and
//!   key references that are not owners (a plain reference reads NULL once its target goes).
//! - Merge its own changes as it collects them, ask [`Evaluator::affected`] before merging any
//!   batch, and add the gids of rows it created while collecting, which `affected` names only if
//!   they are canonical.
//! - Keep one local row per live canonical gid. When two rows come to stand for one entity and
//!   one is deleted, rewrite the rows that refer to the deleted one, though their targets did
//!   not change.
//! - Remove the row of an entity whose target is not shown; it cannot be edited until the
//!   entity is shown again.
//! - Alias the groups [`clashes`] reports before writing rows.
//! - Write in the order [`order`] gives, with [`Columns`] from its table definitions. Hold the
//!   rows [`Plan::stuck`] names (they get no step) and every entity whose write the database
//!   refuses, under the gid the row stands for; leave row and image as they were, and retry them
//!   on every later apply.
//! - Observe the clock of a change from elsewhere only if [`crate::source::admit`] accepts it.
//!
//! [`Evaluator::affected`]: crate::eval::Evaluator::affected

use std::collections::{BTreeMap, BTreeSet};

use crate::change::{Change, DELETED, Strategy};
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

/// The entity a row stands for once `edits` say it was created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewEntity {
    pub gid: Gid,
    /// Whether the source already has a fact about this gid in the collection: the entity is
    /// created again.
    pub known: bool,
}

/// The changes that record `edits` to a row of `collection`, all at `clock`, which must be a fresh
/// reading of the replica's clock. Values must be as changes carry them: references as gids.
///
/// - `Deleted` writes `$deleted = 1` to every member of `old`, the target of the entity the row
///   named, except the new entity's gid.
/// - `Created` writes `$deleted = 0` to the new entity (`new`: a fresh gid, or for a natural
///   collection the gid its key derives), and every `Set` to it. If that gid already has facts
///   (an entity created again under the same natural key), every declared field the edits do not
///   set is written NULL too, so that old values do not come back; a `Max` field cannot be
///   reset this way and keeps the greatest value ever written.
/// - Otherwise each `Set` goes to the smallest live member of `old` (its canonical gid if none is
///   live), so the write is one the group shows. A `Set` of an owner field goes to every member,
///   live or not: a member that is dead only because its owner has not arrived here would
///   otherwise come back under its old owner when that owner arrives. A member that was dead
///   only through its owner comes back with the move, and its fields compete as a live
///   member's do.
///
/// For `Created` without `new` only the tombstones of a `Deleted` are returned; a `Set` or
/// `Deleted` without `old` records nothing.
pub fn record(
    schema: &Schema,
    collection: &str,
    edits: &[FieldEdit],
    old: Option<&Target>,
    new: Option<NewEntity>,
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
            if Some(m) != new.map(|n| n.gid) {
                out.push(change(m, DELETED, Value::Int(1)));
            }
        }
    }
    if created {
        let Some(new) = new else {
            return out;
        };
        out.push(change(new.gid, DELETED, Value::Int(0)));
        for e in edits {
            if let FieldEdit::Set(f, v) = e {
                out.push(change(new.gid, f, v.clone()));
            }
        }
        if new.known {
            for f in &coll.fields {
                let set = edits
                    .iter()
                    .any(|e| matches!(e, FieldEdit::Set(g, _) if *g == f.name));
                if !set && f.strategy != Strategy::Max {
                    out.push(change(new.gid, &f.name, Value::Null));
                }
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

/// The writes [`order`] puts in order, and the rows it cannot write, ascending: the rows of each
/// set waiting on each other that no parkings within `Columns` let be written whole, and those
/// that wait on them. A stuck row gets no step at all, not
/// even a parking, so an adapter holds it, leaves its row and image as they are, and tries again
/// later.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Plan {
    pub steps: Vec<Step>,
    pub stuck: Vec<(String, Gid)>,
}

/// What a requirement waits for, and so which fields parking its source sets to NULL.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Need {
    /// The source row, being inserted, exists. Parking inserts it with every field that still
    /// waits set to NULL.
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
/// stop doing so. Rows that wait on each other, directly or through others, form a set that is
/// written together once every row it waits on outside it is written. Within a set, when
/// nothing can proceed, one row on a cycle of what remains is parked: only the fields that keep
/// the cycle from proceeding are set to NULL, choosing, for a unique set, a field the row's own
/// write changes where one accepts NULL. A row being inserted is parked by inserting it early,
/// with NULL in every field that still waits on a row not yet written. The smallest
/// (collection, gid) that can be parked is.
///
/// Where that search finds nothing to park before the set is written, or takes more than
/// 8 · (r + e) + 4,096 steps for a set of r rows with e requirements among them, the set is
/// planned again from its start, parking at each stall every row of it that can be parked;
/// only then may a row be parked that earlier parkings have already taken off every cycle.
///
/// A set is stuck, all of it, when it waits on a stuck row or when no parkings of its own rows
/// let every one of them be written. A stuck row gets no step, not even a parking.
///
/// Cost: O((n + e) log n) for n rows and e requirements, at most the number of reference and
/// unique fields across the rows.
pub fn order(schema: &Schema, columns: &Columns, planned: &[Planned]) -> Plan {
    let rows = Rows::new(schema, columns, planned);
    let n = planned.len();
    let mut spent = 0;
    let sets = strongly_connected(&(0..n).collect::<Vec<_>>(), &rows.out, &mut spent);
    let mut set_of = vec![0; n];
    for (c, set) in sets.iter().enumerate() {
        for &r in set {
            set_of[r] = c;
        }
    }
    let mut waiting = vec![0usize; sets.len()];
    for v in 0..n {
        for (u, _) in &rows.into[v] {
            if set_of[*u] != set_of[v] {
                waiting[set_of[v]] += 1;
            }
        }
    }
    let first: Vec<&(String, Gid)> = sets
        .iter()
        .map(|set| {
            set.iter()
                .map(|&r| &rows.ids[r])
                .min()
                .expect("a set has a row")
        })
        .collect();
    let mut ready: BTreeSet<(&(String, Gid), usize)> = (0..sets.len())
        .filter(|&c| waiting[c] == 0)
        .map(|c| (first[c], c))
        .collect();
    let mut stuck = vec![false; n];
    let mut steps = Vec::new();
    while let Some((_, c)) = ready.pop_first() {
        let set = &sets[c];
        spent += set.len();
        let blocked = set.iter().any(|&v| {
            spent += rows.into[v].len();
            rows.into[v].iter().any(|(u, _)| stuck[*u])
        });
        let written = if blocked {
            None
        } else if let [r] = set[..] {
            Some(rows.finish(r, None).into_iter().collect())
        } else {
            rows.solve(set)
        };
        match written {
            Some(written) => steps.extend(written),
            None => {
                for &v in set {
                    stuck[v] = true;
                }
            }
        }
        for &v in set {
            spent += rows.out[v].len();
            for (w, _) in &rows.out[v] {
                let d = set_of[*w];
                if d != c {
                    waiting[d] -= 1;
                    if waiting[d] == 0 {
                        ready.insert((first[d], d));
                    }
                }
            }
        }
    }
    crate::work::add(spent);
    let mut stuck: Vec<(String, Gid)> = (0..n)
        .filter(|&i| stuck[i])
        .map(|i| rows.ids[i].clone())
        .collect();
    stuck.sort();
    Plan { steps, stuck }
}

/// The rows of one set, by their place in it and in `planned`.
struct Local {
    members: Vec<usize>,
    pos: BTreeMap<usize, usize>,
}

/// The rows to be ordered, what each will be, and what each waits on.
struct Rows<'a> {
    schema: &'a Schema,
    columns: &'a Columns,
    planned: &'a [Planned],
    ids: Vec<(String, Gid)>,
    at: BTreeMap<(String, Gid), usize>,
    finals: Vec<Option<Row>>,
    /// Requirements by source and by target: the source must act before the target can.
    out: Vec<BTreeSet<(usize, Need)>>,
    into: Vec<BTreeSet<(usize, Need)>>,
}

impl<'a> Rows<'a> {
    fn new(schema: &'a Schema, columns: &'a Columns, planned: &'a [Planned]) -> Rows<'a> {
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
        let finals: Vec<Option<Row>> = planned
            .iter()
            .map(|p| match &p.action {
                RowAction::Insert(row) => Some(row.clone()),
                RowAction::Update(fields) => {
                    let mut row = p.image.clone().unwrap_or_default();
                    row.extend(fields.iter().map(|(f, v)| (f.clone(), v.clone())));
                    Some(row)
                }
                RowAction::Delete => None,
                RowAction::Nothing => p.image.clone(),
            })
            .collect();
        let mut rows = Rows {
            schema,
            columns,
            planned,
            ids,
            at,
            finals,
            out: vec![BTreeSet::new(); n],
            into: vec![BTreeSet::new(); n],
        };
        let mut holders: BTreeMap<(String, usize, Vec<Total>), Vec<usize>> = BTreeMap::new();
        for (i, p) in planned.iter().enumerate() {
            if let Some(image) = &p.image {
                for (u, set) in rows.uniques(&p.collection).iter().enumerate() {
                    if let Some(t) = tuple(image, set) {
                        holders
                            .entry((p.collection.clone(), u, t))
                            .or_default()
                            .push(i);
                    }
                }
            }
        }
        let mut edges: BTreeSet<(usize, usize, Need)> = BTreeSet::new();
        for (v, p) in planned.iter().enumerate() {
            let written: Option<&Row> = match &p.action {
                RowAction::Insert(row) | RowAction::Update(row) => Some(row),
                _ => None,
            };
            for (field, target) in rows.refs(&p.collection) {
                if let Some(Value::Ref(g)) = written.and_then(|w| w.get(&field))
                    && let Some(&u) = rows.at.get(&(target.clone(), *g))
                    && matches!(planned[u].action, RowAction::Insert(_))
                    && u != v
                {
                    edges.insert((u, v, Need::Exists));
                }
                if let Some(Value::Ref(g)) = p.image.as_ref().and_then(|i| i.get(&field))
                    && let Some(&u) = rows.at.get(&(target.clone(), *g))
                    && planned[u].action == RowAction::Delete
                    && u != v
                {
                    let lets_go = rows.finals[v].as_ref().is_none_or(|f| {
                        f.get(&field).is_none_or(|now| !same(now, &Value::Ref(*g)))
                    });
                    if lets_go {
                        edges.insert((v, u, Need::Unrefer(field.clone())));
                    }
                }
            }
            let (Some(finished), Some(_)) = (&rows.finals[v], written) else {
                continue;
            };
            for (u, set) in rows.uniques(&p.collection).iter().enumerate() {
                let Some(t) = tuple(finished, set) else {
                    continue;
                };
                for &h in holders
                    .get(&(p.collection.clone(), u, t))
                    .into_iter()
                    .flatten()
                {
                    let was = planned[h].image.as_ref().and_then(|i| tuple(i, set));
                    let now = rows.finals[h].as_ref().and_then(|r| tuple(r, set));
                    if h != v && was != now {
                        edges.insert((h, v, Need::Release(u)));
                    }
                }
            }
        }
        for (from, to, need) in edges {
            rows.out[from].insert((to, need.clone()));
            rows.into[to].insert((from, need));
        }
        rows
    }

    fn refs(&self, collection: &str) -> Vec<(String, String)> {
        self.schema
            .collection(collection)
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
    }

    fn uniques(&self, collection: &str) -> Vec<Vec<String>> {
        self.schema
            .collection(collection)
            .map(|c| {
                let mut sets = c.identity_keys.clone();
                if let Identity::Natural(key) = &c.identity {
                    sets.push(key.clone());
                }
                sets
            })
            .unwrap_or_default()
    }

    fn inserts(&self, r: usize) -> bool {
        matches!(self.planned[r].action, RowAction::Insert(_))
    }

    /// Which field of unique set `u` parking row `r` sets to NULL: one its write changes if any
    /// accepts NULL, else any that does.
    fn release_field(&self, r: usize, u: usize) -> Option<String> {
        let p = &self.planned[r];
        let set = &self.uniques(&p.collection)[u];
        let open = |f: &&String| self.columns.accepts_null(&p.collection, f);
        let changes = |f: &&String| {
            let was = p.image.as_ref().and_then(|i| i.get(*f));
            let now = self.finals[r].as_ref().and_then(|row| row.get(*f));
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
    }

    /// The fields of inserted row `r` that refer to row `u`.
    fn referring(&self, r: usize, u: usize) -> Vec<String> {
        let (target, g) = &self.ids[u];
        self.refs(&self.planned[r].collection)
            .into_iter()
            .filter(|(field, t)| {
                t == target
                    && matches!(
                        self.finals[r].as_ref().and_then(|row| row.get(field)),
                        Some(Value::Ref(x)) if x == g
                    )
            })
            .map(|(field, _)| field)
            .collect()
    }

    /// Whether inserted row `r`, waiting on `u` for `need`, cannot be inserted early because of it.
    fn hard(&self, r: usize, u: usize, need: &Need) -> bool {
        match need {
            Need::Exists => self
                .referring(r, u)
                .iter()
                .any(|f| !self.columns.accepts_null(&self.planned[r].collection, f)),
            Need::Release(set) => self.release_field(r, *set).is_none(),
            Need::Unrefer(_) => false,
        }
    }

    /// Row `r`'s final write, after a parking that left it as `parked` if there was one.
    fn finish(&self, r: usize, parked: Option<&Row>) -> Option<Step> {
        let p = &self.planned[r];
        let action = match (parked, &p.action) {
            (_, RowAction::Delete) => RowAction::Delete,
            (Some(image), _) => write(self.finals[r].as_ref(), Some(image)),
            (None, action) => action.clone(),
        };
        (action != RowAction::Nothing).then(|| Step {
            collection: p.collection.clone(),
            gid: p.gid,
            action,
            parked: false,
        })
    }

    /// Parking row `r` of a set: the step, the row it leaves, and the requirements it meets,
    /// given what `r` still waits on (`into`) and what waits on it (`out`, of which only those
    /// to rows `meets` accepts count), by place in the set. `None` if `r` cannot be parked or
    /// parking it meets nothing.
    #[allow(clippy::type_complexity)]
    fn parking(
        &self,
        r: usize,
        out: &BTreeSet<(usize, Need)>,
        into: &BTreeSet<(usize, Need)>,
        local: &Local,
        meets: impl Fn(usize) -> bool,
    ) -> Option<(Step, Row, Vec<(usize, Need)>)> {
        let p = &self.planned[r];
        let mut nulls: BTreeSet<String> = BTreeSet::new();
        let mut satisfied: Vec<(usize, Need)> = Vec::new();
        if self.inserts(r) {
            // Inserted early, the row must wait for nothing: every field that makes it wait goes
            // in NULL, and it then exists for every row waiting on that.
            for (u, need) in into {
                match need {
                    Need::Exists => {
                        for f in self.referring(r, local.members[*u]) {
                            if !self.columns.accepts_null(&p.collection, &f) {
                                return None;
                            }
                            nulls.insert(f);
                        }
                    }
                    Need::Release(set) => {
                        nulls.insert(self.release_field(r, *set)?);
                    }
                    Need::Unrefer(_) => {}
                }
            }
            satisfied.extend(
                out.iter()
                    .filter(|(_, need)| *need == Need::Exists)
                    .cloned(),
            );
        } else {
            for (to, need) in out {
                if !meets(*to) {
                    continue;
                }
                let field = match need {
                    Need::Unrefer(f) => Some(f.clone()),
                    Need::Release(u) => self.release_field(r, *u),
                    Need::Exists => None,
                };
                if let Some(f) = field.filter(|f| self.columns.accepts_null(&p.collection, f)) {
                    nulls.insert(f);
                    satisfied.push((*to, need.clone()));
                }
            }
        }
        if satisfied.is_empty() {
            return None;
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
            (_, None) => return None,
        };
        let step = Step {
            collection: p.collection.clone(),
            gid: p.gid,
            action,
            parked: true,
        };
        Some((step, image, satisfied))
    }

    /// The steps that write every row of `set`, a strongly connected set whose requirements on
    /// rows outside it are all met, or `None` if no parkings of its rows let all be written.
    fn solve(&self, set: &[usize]) -> Option<Vec<Step>> {
        let m = set.len();
        let local = Local {
            members: set.to_vec(),
            pos: set.iter().enumerate().map(|(i, &r)| (r, i)).collect(),
        };
        let mut out = vec![BTreeSet::new(); m];
        let mut into = vec![BTreeSet::new(); m];
        let mut edges = 0;
        for (i, &r) in set.iter().enumerate() {
            for (w, need) in &self.out[r] {
                if let Some(&j) = local.pos.get(w) {
                    out[i].insert((j, need.clone()));
                    into[j].insert((i, need.clone()));
                    edges += 1;
                }
            }
        }
        crate::work::add(m + edges);
        let budget = 8 * (m + edges) + 4096;
        self.fewest(&local, out.clone(), into.clone(), budget)
            .or_else(|| self.every(&local, out, into))
    }

    /// Writes a set parking one row at a time, the smallest that can be parked on a cycle of what
    /// remains. `None` if no such row is left before the set is written, or after `budget` steps.
    fn fewest(
        &self,
        local: &Local,
        mut out: Vec<BTreeSet<(usize, Need)>>,
        mut into: Vec<BTreeSet<(usize, Need)>>,
        budget: usize,
    ) -> Option<Vec<Step>> {
        let m = local.members.len();
        let id = |i: usize| &self.ids[local.members[i]];
        let mut spent = 0usize;
        let mut done = vec![false; m];
        let mut parked: Vec<Option<Row>> = vec![None; m];
        let mut ready: BTreeSet<(&(String, Gid), usize)> = (0..m)
            .filter(|&i| into[i].is_empty())
            .map(|i| (id(i), i))
            .collect();
        let mut steps = Vec::new();
        let mut finished = 0usize;
        // The cycles among what is left, found at the first stall and afterwards refreshed only
        // for the component a parking changed; the rows on them that may still be parked.
        let mut components: Vec<Vec<usize>> = Vec::new();
        let mut component_of: Vec<Option<usize>> = vec![None; m];
        let mut changed: BTreeSet<usize> = BTreeSet::new();
        let mut found_cycles = false;
        let mut live = 0usize;
        let mut candidates: BTreeSet<(&(String, Gid), usize)> = BTreeSet::new();
        // Rows tried and found unparkable. Only a refreshed component or a requirement on the row
        // being met can change that, and either puts the row back among the candidates.
        let mut failed: BTreeSet<usize> = BTreeSet::new();
        let outcome = loop {
            while let Some((_, i)) = ready.pop_first() {
                spent += 1 + out[i].len();
                done[i] = true;
                finished += 1;
                candidates.remove(&(id(i), i));
                steps.extend(self.finish(local.members[i], parked[i].as_ref()));
                for (to, need) in std::mem::take(&mut out[i]) {
                    into[to].remove(&(i, need));
                    if into[to].is_empty() && !done[to] {
                        ready.insert((id(to), to));
                    }
                    if failed.remove(&to) && component_of[to].is_some() {
                        candidates.insert((id(to), to));
                    }
                }
            }
            if finished == m {
                break Some(steps);
            }
            if spent > budget {
                break None;
            }
            let mut found: Vec<Vec<usize>> = Vec::new();
            if !found_cycles {
                found_cycles = true;
                let left: Vec<usize> = (0..m).filter(|&i| !done[i]).collect();
                found.extend(cycles(&left, &out, &mut spent));
            }
            for c in std::mem::take(&mut changed) {
                let members = std::mem::take(&mut components[c]);
                live -= 1;
                for &x in &members {
                    component_of[x] = None;
                    candidates.remove(&(id(x), x));
                }
                let left: Vec<usize> = members.into_iter().filter(|&i| !done[i]).collect();
                found.extend(cycles(&left, &out, &mut spent));
            }
            for members in found {
                let c = components.len();
                for &x in &members {
                    component_of[x] = Some(c);
                    if parked[x].is_none() {
                        candidates.insert((id(x), x));
                    }
                }
                components.push(members);
                live += 1;
            }
            if live == 0 {
                break None;
            }
            let mut broke = false;
            while let Some(&(_, r)) = candidates.first() {
                spent += 1 + out[r].len() + into[r].len();
                let mine = component_of[r];
                let parking = self.parking(local.members[r], &out[r], &into[r], local, |x| {
                    component_of[x] == mine
                });
                candidates.remove(&(id(r), r));
                let Some((step, image, satisfied)) = parking else {
                    failed.insert(r);
                    continue;
                };
                steps.push(step);
                parked[r] = Some(image);
                changed.insert(mine.expect("a candidate is on a cycle"));
                for (to, need) in satisfied {
                    out[r].remove(&(to, need.clone()));
                    into[to].remove(&(r, need));
                    if into[to].is_empty() && !done[to] {
                        ready.insert((id(to), to));
                    }
                    if failed.remove(&to) && component_of[to].is_some() && parked[to].is_none() {
                        candidates.insert((id(to), to));
                    }
                }
                broke = true;
                break;
            }
            if !broke || spent > budget {
                break None;
            }
        };
        crate::work::add(spent);
        outcome
    }

    /// Writes a set parking, whenever nothing can proceed, every row of it that can be parked.
    /// `None` if a stall comes with none left: then no parkings let the whole set be written.
    fn every(
        &self,
        local: &Local,
        mut out: Vec<BTreeSet<(usize, Need)>>,
        mut into: Vec<BTreeSet<(usize, Need)>>,
    ) -> Option<Vec<Step>> {
        let m = local.members.len();
        let id = |i: usize| &self.ids[local.members[i]];
        let global = |i: usize| local.members[i];
        let mut spent = 0usize;
        // For each row being inserted, how many of its waits it cannot be inserted early around.
        let mut hard = vec![0usize; m];
        for (i, waits) in into.iter().enumerate() {
            spent += 1 + waits.len();
            if self.inserts(global(i)) {
                hard[i] = waits
                    .iter()
                    .filter(|(u, need)| self.hard(global(i), global(*u), need))
                    .count();
            }
        }
        let mut eligible: BTreeSet<(&(String, Gid), usize)> = (0..m)
            .filter(|&i| hard[i] == 0)
            .map(|i| (id(i), i))
            .collect();
        let mut done = vec![false; m];
        let mut parked: Vec<Option<Row>> = vec![None; m];
        let mut ready: BTreeSet<(&(String, Gid), usize)> = (0..m)
            .filter(|&i| into[i].is_empty())
            .map(|i| (id(i), i))
            .collect();
        let mut steps = Vec::new();
        let mut finished = 0usize;
        let outcome = loop {
            while let Some((_, i)) = ready.pop_first() {
                spent += 1 + out[i].len();
                done[i] = true;
                finished += 1;
                steps.extend(self.finish(global(i), parked[i].as_ref()));
                for (to, need) in std::mem::take(&mut out[i]) {
                    if self.inserts(global(to)) && self.hard(global(to), global(i), &need) {
                        hard[to] -= 1;
                        if hard[to] == 0 {
                            eligible.insert((id(to), to));
                        }
                    }
                    into[to].remove(&(i, need));
                    if into[to].is_empty() && !done[to] {
                        ready.insert((id(to), to));
                    }
                }
            }
            if finished == m {
                break Some(steps);
            }
            let mut any = false;
            for (_, r) in std::mem::take(&mut eligible) {
                spent += 1 + out[r].len() + into[r].len();
                if done[r] || parked[r].is_some() || into[r].is_empty() {
                    continue;
                }
                let Some((step, image, satisfied)) =
                    self.parking(global(r), &out[r], &into[r], local, |_| true)
                else {
                    continue;
                };
                steps.push(step);
                parked[r] = Some(image);
                any = true;
                for (to, need) in satisfied {
                    if self.inserts(global(to)) && self.hard(global(to), global(r), &need) {
                        hard[to] -= 1;
                        if hard[to] == 0 {
                            eligible.insert((id(to), to));
                        }
                    }
                    out[r].remove(&(to, need.clone()));
                    into[to].remove(&(r, need));
                    if into[to].is_empty() && !done[to] {
                        ready.insert((id(to), to));
                    }
                }
            }
            if !any {
                break None;
            }
        };
        crate::work::add(spent);
        outcome
    }
}

/// The values of unique set `set` in `row`, unless one is NULL.
fn tuple(row: &Row, set: &[String]) -> Option<Vec<Total>> {
    let values: Vec<Value> = set
        .iter()
        .map(|f| row.get(f).cloned().unwrap_or(Value::Null))
        .collect();
    (!values.contains(&Value::Null)).then(|| values.into_iter().map(Total).collect())
}

/// The strongly connected components of more than one row among `left`, by `out`'s edges.
fn cycles(left: &[usize], out: &[BTreeSet<(usize, Need)>], spent: &mut usize) -> Vec<Vec<usize>> {
    let mut found = strongly_connected(left, out, spent);
    found.retain(|c| c.len() > 1);
    found
}

/// The strongly connected components among `left`, by `out`'s edges, counting in `spent` each
/// row and edge looked at.
fn strongly_connected(
    left: &[usize],
    out: &[BTreeSet<(usize, Need)>],
    spent: &mut usize,
) -> Vec<Vec<usize>> {
    *spent += left.len();
    let inside: BTreeSet<usize> = left.iter().copied().collect();
    let mut index = BTreeMap::new();
    let mut low = BTreeMap::new();
    let mut on_stack = BTreeSet::new();
    let mut stack = Vec::new();
    let mut found = Vec::new();
    let mut counter = 0;
    let mut next = |v: usize| -> Vec<usize> {
        *spent += out[v].len();
        let mut t: Vec<usize> = out[v]
            .iter()
            .map(|(to, _)| *to)
            .filter(|to| inside.contains(to))
            .collect();
        t.dedup();
        t.reverse();
        t
    };
    for &start in left {
        if index.contains_key(&start) {
            continue;
        }
        // Iterative Tarjan: (node, the targets still to visit).
        let mut work: Vec<(usize, Vec<usize>)> = Vec::new();
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
                found.push(component);
            }
            if let Some((parent, _)) = work.last() {
                let l = low[parent].min(low[&v]);
                low.insert(*parent, l);
            }
        }
    }
    found
}

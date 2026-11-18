//! The facts a replica keeps about the changes it has merged, and the questions the evaluator asks
//! of them. Storing a change is mechanical: [`admit`] says which slot it competes for and the slot
//! keeps the winner. Nothing here decides which collection a gid belongs to, whether a natural key
//! holds, or which aliases count; [`crate::eval`] decides that from these facts.

use std::collections::{BTreeMap, BTreeSet};

use crate::change::{Change, DELETED, Strategy, wins};
use crate::clock::Hlc;
use crate::model::{ALIAS, Identity, Schema};
use crate::total::{Total, same};
use crate::value::{Gid, Value};

/// A collection has received a valid change to a gid; `first` is the smallest clock among them.
#[derive(Debug, Clone, PartialEq)]
pub struct Claim {
    pub collection: String,
    pub first: Hlc,
}

/// One kept entry of a field.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub field: String,
    pub value: Value,
    pub clock: Hlc,
}

/// A valid alias change: `from` and `to` are one entity of `collection`, if both belong to it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Alias {
    pub collection: String,
    pub from: Gid,
    pub to: Gid,
}

/// How the entries of one field are kept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Slot {
    /// One entry: the winner by this strategy (`crate::wins`).
    One(Strategy),
    /// One entry per distinct value under `Value::total_cmp`, each with the largest clock that
    /// wrote it. Used for the key fields of a natural collection, whose every written value
    /// matters to whether the entity's key holds.
    PerValue,
}

/// What storing a change means.
#[derive(Debug, Clone, PartialEq)]
pub struct Fact {
    pub collection: String,
    pub gid: Gid,
    pub clock: Hlc,
    pub body: Body,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Body {
    /// Keep `value` at `clock` in the field's slot, as `slot` says.
    Field {
        field: String,
        value: Value,
        slot: Slot,
    },
    /// Keep the alias from `gid` to `target`.
    Alias { target: Gid },
}

/// The fact a change contributes, or `None` if `model::view` ignores it outright: an unknown
/// collection, a clock `Hlc::encode` cannot hold, an alias that does not name another gid, or a
/// `$deleted` that is neither `Int(0)` nor `Int(1)`.
///
/// A store keeps, for each (collection, gid) any fact names, the smallest clock among those facts
/// (a [`Claim`]); for each (collection, gid, field), entries as the fact's [`Slot`] says; and the
/// set of aliases. Undeclared fields are kept like declared ones. Merging facts this way is
/// order-independent and idempotent, given that no two changes to one field share a clock with
/// different values.
pub fn admit(schema: &Schema, change: &Change) -> Option<Fact> {
    let collection = schema.collection(&change.collection)?;
    if !change.clock.encodable()
        || (change.field == DELETED && !matches!(change.value, Value::Int(0 | 1)))
    {
        return None;
    }
    let body = if change.field == ALIAS {
        match change.value {
            Value::Ref(target) if target != change.gid => Body::Alias { target },
            _ => return None,
        }
    } else {
        Body::Field {
            field: change.field.clone(),
            value: change.value.clone(),
            slot: slot(schema, &collection.name, &change.field),
        }
    };
    Some(Fact {
        collection: change.collection.clone(),
        gid: change.gid,
        clock: change.clock,
        body,
    })
}

/// How a field of a declared collection is kept.
pub fn slot(schema: &Schema, collection: &str, field: &str) -> Slot {
    let Some(c) = schema.collection(collection) else {
        return Slot::One(Strategy::Lww);
    };
    if let Identity::Natural(key) = &c.identity
        && key.iter().any(|k| k == field)
    {
        return Slot::PerValue;
    }
    match c.fields.iter().find(|f| f.name == field) {
        Some(f) if field != DELETED => Slot::One(f.strategy),
        _ => Slot::One(Strategy::Lww),
    }
}

/// Keeps `value` at `clock` in `entries` as `slot` says. Returns the value it displaced, if any,
/// and whether anything changed.
fn keep(
    slot: Slot,
    entries: &mut Vec<(Value, Hlc)>,
    value: &Value,
    clock: Hlc,
) -> (bool, Option<Value>) {
    match slot {
        Slot::One(strategy) => match entries.first() {
            Some((v, k)) if !wins(strategy, Some((v, k)), value, &clock) => (false, None),
            _ => {
                let old = entries.pop().map(|(v, _)| v);
                entries.clear();
                entries.push((value.clone(), clock));
                (true, old)
            }
        },
        Slot::PerValue => match entries.iter_mut().find(|(v, _)| same(v, value)) {
            Some((_, k)) if *k >= clock => (false, None),
            Some((_, k)) => {
                *k = clock;
                (true, None)
            }
            None => {
                entries.push((value.clone(), clock));
                (true, None)
            }
        },
    }
}

/// Whether keeping `fact` would change anything `base` keeps.
pub(crate) fn alters<S: Source>(base: &S, fact: &Fact) -> bool {
    let claimed = base
        .claims(fact.gid)
        .into_iter()
        .any(|c| c.collection == fact.collection && c.first <= fact.clock);
    if !claimed {
        return true;
    }
    match &fact.body {
        Body::Alias { target } => !base.aliases(fact.gid).contains(&Alias {
            collection: fact.collection.clone(),
            from: fact.gid,
            to: *target,
        }),
        Body::Field { field, value, slot } => {
            let mut kept: Vec<(Value, Hlc)> = base
                .entries(&fact.collection, fact.gid)
                .into_iter()
                .filter(|e| &e.field == field)
                .map(|e| (e.value, e.clock))
                .collect();
            keep(*slot, &mut kept, value, fact.clock).0
        }
    }
}

/// The questions the evaluator asks. A database adapter answers them with indexed queries over
/// the facts [`admit`] describes; [`Store`] answers them in memory.
pub trait Source {
    /// Every collection with a fact about `gid`, each with the smallest clock among its facts.
    fn claims(&self, gid: Gid) -> Vec<Claim>;
    /// Every entry kept for `gid` in `collection`.
    fn entries(&self, collection: &str, gid: Gid) -> Vec<Entry>;
    /// Every alias kept with `gid` at either end.
    fn aliases(&self, gid: Gid) -> Vec<Alias>;
    /// The gids that, in `collection`, keep for each pair an entry of that field equal to that
    /// value under `Value::total_cmp`. Must not omit one; may include gids that do not qualify.
    fn holding(&self, collection: &str, fields: &[(&str, &Value)]) -> Vec<Gid>;
    /// Every gid with a claim.
    fn gids(&self) -> Vec<Gid>;
}

type Entries = BTreeMap<String, Vec<(Value, Hlc)>>;

/// Facts held in memory, indexed as [`Source`] needs.
#[derive(Debug, Clone, Default)]
pub struct Store {
    claims: BTreeMap<Gid, BTreeMap<String, Hlc>>,
    fields: BTreeMap<(String, Gid), Entries>,
    aliases: BTreeMap<Gid, BTreeSet<Alias>>,
    index: BTreeMap<(String, String, Total), BTreeSet<Gid>>,
}

impl Store {
    /// Stores what `change` contributes. Returns whether any kept fact changed.
    pub fn merge(&mut self, schema: &Schema, change: &Change) -> bool {
        admit(schema, change).is_some_and(|fact| self.keep(&fact))
    }

    /// Stores a fact. Returns whether any kept fact changed.
    pub fn keep(&mut self, fact: &Fact) -> bool {
        let claims = self.claims.entry(fact.gid).or_default();
        let mut changed = match claims.get(&fact.collection) {
            Some(first) if *first <= fact.clock => false,
            _ => {
                claims.insert(fact.collection.clone(), fact.clock);
                true
            }
        };
        match &fact.body {
            Body::Alias { target } => {
                let alias = Alias {
                    collection: fact.collection.clone(),
                    from: fact.gid,
                    to: *target,
                };
                self.aliases
                    .entry(*target)
                    .or_default()
                    .insert(alias.clone());
                changed |= self.aliases.entry(fact.gid).or_default().insert(alias);
            }
            Body::Field { field, value, slot } => {
                let entries = self
                    .fields
                    .entry((fact.collection.clone(), fact.gid))
                    .or_default()
                    .entry(field.clone())
                    .or_default();
                let (kept, displaced) = keep(*slot, entries, value, fact.clock);
                if kept {
                    changed = true;
                    if let Some(old) = displaced {
                        let key = (fact.collection.clone(), field.clone(), Total(old));
                        if let Some(set) = self.index.get_mut(&key) {
                            set.remove(&fact.gid);
                            if set.is_empty() {
                                self.index.remove(&key);
                            }
                        }
                    }
                    self.index
                        .entry((fact.collection.clone(), field.clone(), Total(value.clone())))
                        .or_default()
                        .insert(fact.gid);
                }
            }
        }
        changed
    }

    fn touched(&self, collection: &str) -> Vec<Gid> {
        self.claims
            .iter()
            .filter(|(_, c)| c.contains_key(collection))
            .map(|(g, _)| *g)
            .collect()
    }
}

impl Source for Store {
    fn claims(&self, gid: Gid) -> Vec<Claim> {
        self.claims
            .get(&gid)
            .into_iter()
            .flatten()
            .map(|(collection, first)| Claim {
                collection: collection.clone(),
                first: *first,
            })
            .collect()
    }

    fn entries(&self, collection: &str, gid: Gid) -> Vec<Entry> {
        let Some(fields) = self.fields.get(&(collection.to_string(), gid)) else {
            return Vec::new();
        };
        fields
            .iter()
            .flat_map(|(field, entries)| {
                entries.iter().map(|(value, clock)| Entry {
                    field: field.clone(),
                    value: value.clone(),
                    clock: *clock,
                })
            })
            .collect()
    }

    fn aliases(&self, gid: Gid) -> Vec<Alias> {
        self.aliases
            .get(&gid)
            .into_iter()
            .flatten()
            .cloned()
            .collect()
    }

    fn holding(&self, collection: &str, fields: &[(&str, &Value)]) -> Vec<Gid> {
        let mut sets: Vec<&BTreeSet<Gid>> = Vec::new();
        for (field, value) in fields {
            let key = (
                collection.to_string(),
                field.to_string(),
                Total((*value).clone()),
            );
            match self.index.get(&key) {
                Some(set) => sets.push(set),
                None => return Vec::new(),
            }
        }
        sets.sort_by_key(|s| s.len());
        let Some((first, rest)) = sets.split_first() else {
            return self.touched(collection);
        };
        first
            .iter()
            .filter(|g| rest.iter().all(|s| s.contains(g)))
            .copied()
            .collect()
    }

    fn gids(&self) -> Vec<Gid> {
        self.claims.keys().copied().collect()
    }
}

/// A source as it will be once `batch` is merged into `base`, without merging it.
pub(crate) struct Overlay<'a, S> {
    schema: &'a Schema,
    base: &'a S,
    batch: Store,
}

impl<'a, S: Source> Overlay<'a, S> {
    pub(crate) fn new(schema: &'a Schema, base: &'a S, batch: &[Change]) -> Overlay<'a, S> {
        let mut store = Store::default();
        for change in batch {
            store.merge(schema, change);
        }
        Overlay {
            schema,
            base,
            batch: store,
        }
    }
}

impl<S: Source> Source for Overlay<'_, S> {
    fn claims(&self, gid: Gid) -> Vec<Claim> {
        let mut firsts: BTreeMap<String, Hlc> = BTreeMap::new();
        for claim in self
            .base
            .claims(gid)
            .into_iter()
            .chain(self.batch.claims(gid))
        {
            let first = firsts.entry(claim.collection).or_insert(claim.first);
            *first = (*first).min(claim.first);
        }
        firsts
            .into_iter()
            .map(|(collection, first)| Claim { collection, first })
            .collect()
    }

    fn entries(&self, collection: &str, gid: Gid) -> Vec<Entry> {
        let mut fields: Entries = BTreeMap::new();
        for entry in self
            .base
            .entries(collection, gid)
            .into_iter()
            .chain(self.batch.entries(collection, gid))
        {
            let slot = slot(self.schema, collection, &entry.field);
            let kept = fields.entry(entry.field).or_default();
            keep(slot, kept, &entry.value, entry.clock);
        }
        fields
            .into_iter()
            .flat_map(|(field, entries)| {
                entries.into_iter().map(move |(value, clock)| Entry {
                    field: field.clone(),
                    value,
                    clock,
                })
            })
            .collect()
    }

    fn aliases(&self, gid: Gid) -> Vec<Alias> {
        let mut all: BTreeSet<Alias> = self.base.aliases(gid).into_iter().collect();
        all.extend(self.batch.aliases(gid));
        all.into_iter().collect()
    }

    fn holding(&self, collection: &str, fields: &[(&str, &Value)]) -> Vec<Gid> {
        let mut found: BTreeSet<Gid> = self.base.holding(collection, fields).into_iter().collect();
        for gid in self.batch.touched(collection) {
            let entries = self.entries(collection, gid);
            let holds = fields.iter().all(|(field, value)| {
                entries
                    .iter()
                    .any(|e| e.field == *field && same(&e.value, value))
            });
            if holds {
                found.insert(gid);
            }
        }
        found.into_iter().collect()
    }

    fn gids(&self) -> Vec<Gid> {
        let mut all: BTreeSet<Gid> = self.base.gids().into_iter().collect();
        all.extend(self.batch.gids());
        all.into_iter().collect()
    }
}

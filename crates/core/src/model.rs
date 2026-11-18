//! What replicas must converge to, stated as one plain function: [`view`] takes a schema and a set
//! of changes and returns the data an application sees. It favours being obviously right over
//! being fast, and is meant as the oracle that storage adapters are tested against.
//!
//! The answer depends only on the set of changes: their order and any duplicates do not matter,
//! provided no two changes to one field share a clock with different values.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use crate::change::{Change, DELETED, Strategy, wins};
use crate::clock::Hlc;
use crate::value::{Gid, Value};

/// The field whose value, a `Value::Ref` to another entity of the same collection, says that the
/// two are one entity. Every such change ever made counts, not only the latest.
pub const ALIAS: &str = "$alias";

/// How a collection's entities get their gids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Identity {
    /// Entities carry arbitrary gids.
    Minted,
    /// An entity's gid is `Gid::derive(collection, key)`, the key being these fields' values in
    /// order. Two entities whose keys are equal, once every reference in them is read as its
    /// group's canonical gid, are one entity.
    Natural(Vec<String>),
}

/// What a field's value means.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldKind {
    Plain,
    /// Names an entity of the target collection. Reads as its group's canonical gid while that
    /// group is live, and as NULL otherwise.
    Reference(String),
    /// Names the entity of the target collection this one belongs to: this one is live only
    /// while that one's group is. NULL means it belongs to nothing.
    Owner(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub name: String,
    pub kind: FieldKind,
    pub strategy: Strategy,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Collection {
    pub name: String,
    pub identity: Identity,
    pub fields: Vec<Field>,
    /// Sets of fields. Two live groups whose values agree, non-null, on every field of one set
    /// are one entity, and a replica that sees them must alias them.
    pub identity_keys: Vec<Vec<String>>,
    /// Sets of fields whose values are taken as a unit from the latest clock written to any of
    /// them; a field of the set not written at that clock reads as NULL.
    pub together: Vec<Vec<String>>,
}

impl Collection {
    pub fn minted(name: &str) -> Collection {
        Collection {
            name: name.into(),
            identity: Identity::Minted,
            fields: Vec::new(),
            identity_keys: Vec::new(),
            together: Vec::new(),
        }
    }

    /// A natural collection keyed by `key`; each key field must also be declared.
    pub fn natural(name: &str, key: &[&str]) -> Collection {
        Collection {
            identity: Identity::Natural(key.iter().map(|k| k.to_string()).collect()),
            ..Collection::minted(name)
        }
    }

    pub fn field(mut self, name: &str, kind: FieldKind, strategy: Strategy) -> Collection {
        self.fields.push(Field {
            name: name.into(),
            kind,
            strategy,
        });
        self
    }

    pub fn plain(self, name: &str, strategy: Strategy) -> Collection {
        self.field(name, FieldKind::Plain, strategy)
    }

    pub fn reference(self, name: &str, target: &str) -> Collection {
        self.field(name, FieldKind::Reference(target.into()), Strategy::Lww)
    }

    pub fn owner(self, name: &str, target: &str) -> Collection {
        self.field(name, FieldKind::Owner(target.into()), Strategy::Lww)
    }

    pub fn identity_key(mut self, fields: &[&str]) -> Collection {
        self.identity_keys
            .push(fields.iter().map(|f| f.to_string()).collect());
        self
    }

    pub fn together(mut self, fields: &[&str]) -> Collection {
        self.together
            .push(fields.iter().map(|f| f.to_string()).collect());
        self
    }

    fn declared(&self, field: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.name == field)
    }

    fn strategy(&self, field: &str) -> Strategy {
        match self.declared(field) {
            Some(f) if field != DELETED => f.strategy,
            _ => Strategy::Lww,
        }
    }

    fn in_together(&self, field: &str) -> bool {
        self.together
            .iter()
            .any(|set| set.iter().any(|f| f == field))
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Schema {
    pub collections: Vec<Collection>,
}

impl Schema {
    pub fn new(collections: Vec<Collection>) -> Schema {
        Schema { collections }
    }

    pub fn collection(&self, name: &str) -> Option<&Collection> {
        self.collections.iter().find(|c| c.name == name)
    }

    /// Whether [`view`] can be asked about this schema. It refuses: duplicate names; field names
    /// starting with `$`; references to undeclared collections; undeclared fields in a key, an
    /// identity key or a `together` set; identity keys on natural collections; a `together`
    /// field that does not merge `Lww`, or that is in two sets; and owner references that form a
    /// cycle across collections. A collection owning its own rows is allowed.
    pub fn check(&self) -> Result<(), String> {
        let mut names = BTreeSet::new();
        for c in &self.collections {
            if !names.insert(c.name.as_str()) {
                return Err(format!("collection {} is declared twice", c.name));
            }
            let mut fields = BTreeSet::new();
            for f in &c.fields {
                if f.name.starts_with('$') {
                    return Err(format!("{}.{} uses a reserved name", c.name, f.name));
                }
                if !fields.insert(f.name.as_str()) {
                    return Err(format!("{}.{} is declared twice", c.name, f.name));
                }
                if let FieldKind::Reference(t) | FieldKind::Owner(t) = &f.kind
                    && self.collection(t).is_none()
                {
                    return Err(format!("{}.{} refers to unknown {t}", c.name, f.name));
                }
            }
            let undeclared = |set: &[String]| set.iter().find(|f| c.declared(f).is_none()).cloned();
            if let Identity::Natural(key) = &c.identity {
                if key.is_empty() {
                    return Err(format!("{} has an empty key", c.name));
                }
                if let Some(f) = undeclared(key) {
                    return Err(format!("{} keys on undeclared {f}", c.name));
                }
                if !c.identity_keys.is_empty() {
                    return Err(format!("{} is natural and has identity keys", c.name));
                }
            }
            for key in &c.identity_keys {
                if key.is_empty() {
                    return Err(format!("{} has an empty identity key", c.name));
                }
                if let Some(f) = undeclared(key) {
                    return Err(format!("{} has an identity key on undeclared {f}", c.name));
                }
            }
            let mut seen = BTreeSet::new();
            for set in &c.together {
                if set.is_empty() {
                    return Err(format!("{} has an empty together set", c.name));
                }
                if let Some(f) = undeclared(set) {
                    return Err(format!("{} groups undeclared {f}", c.name));
                }
                for f in set {
                    if !seen.insert(f.as_str()) {
                        return Err(format!("{}.{f} is in two together sets", c.name));
                    }
                    if c.strategy(f) != Strategy::Lww {
                        return Err(format!("{}.{f} is in a together set but not Lww", c.name));
                    }
                }
            }
        }
        for start in &self.collections {
            let mut at = vec![start.name.as_str()];
            let mut visited = BTreeSet::new();
            while let Some(name) = at.pop() {
                let fields = self.collection(name).map_or(&[][..], |c| &c.fields);
                for f in fields {
                    if let FieldKind::Owner(t) = &f.kind
                        && t != name
                    {
                        if t == &start.name {
                            return Err(format!("ownership cycles through {}", start.name));
                        }
                        if visited.insert(t.as_str()) {
                            at.push(t);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// One entity as the application sees it: every declared field, NULL where nothing is known.
pub type Row = BTreeMap<String, Value>;

/// What an application sees: for each collection, its live entities by canonical gid. A
/// collection with no live entity is absent. Values compare by `Value::total_cmp`, so a NaN
/// equals itself.
#[derive(Debug, Clone, Default)]
pub struct View {
    pub collections: BTreeMap<String, BTreeMap<Gid, Row>>,
}

impl View {
    pub fn insert(&mut self, collection: &str, gid: Gid, row: Row) {
        self.collections
            .entry(collection.to_string())
            .or_default()
            .insert(gid, row);
    }

    pub fn row(&self, collection: &str, gid: Gid) -> Option<&Row> {
        self.collections.get(collection)?.get(&gid)
    }

    pub fn rows(&self, collection: &str) -> impl Iterator<Item = (&Gid, &Row)> {
        self.collections.get(collection).into_iter().flatten()
    }
}

impl PartialEq for View {
    fn eq(&self, other: &View) -> bool {
        let same_row = |a: &Row, b: &Row| {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|((fa, va), (fb, vb))| fa == fb && va.total_cmp(vb) == Ordering::Equal)
        };
        let same_rows = |a: &BTreeMap<Gid, Row>, b: &BTreeMap<Gid, Row>| {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|((ga, ra), (gb, rb))| ga == gb && same_row(ra, rb))
        };
        self.collections.len() == other.collections.len()
            && self
                .collections
                .iter()
                .zip(&other.collections)
                .all(|((ca, a), (cb, b))| ca == cb && same_rows(a, b))
    }
}

/// The data replicas that have seen exactly `changes` must present. `schema` must pass
/// [`Schema::check`].
///
/// 1. A change is ignored if its collection is unknown, its clock does not survive
///    `Hlc::encode`, it is an [`ALIAS`] whose value is not a reference to another gid, or its gid
///    belongs to another collection. A gid belongs to the collection of its change with the
///    smallest clock, ties going to the smaller collection name. Changes to undeclared fields
///    are kept: they claim the gid, but are not shown.
/// 2. Each (gid, field) holds the winner among its changes by the field's strategy; `$deleted`
///    and undeclared fields merge `Lww`.
/// 3. An alias whose target belongs to the same collection joins the two gids. Groups are the
///    connected components, closed under natural keys becoming equal, and a group's canonical
///    gid is its smallest member. A natural entity takes part in key matching only once every key
///    field has a value.
/// 4. A member is live if its `$deleted` is not `Int(1)` and every owner field it holds is NULL
///    or names a gid of the owner collection whose group is live. This is the least such set, so
///    an ownership cycle with no live way out is not live. A group is live if a member is.
/// 5. A live group shows, for each field, the strategy winner among its live members; for a
///    `together` set, the set of the live member holding the latest clock across the set.
///    References read as described on [`FieldKind`].
pub fn view(schema: &Schema, changes: &[Change]) -> View {
    Merged::new(schema, changes).present()
}

/// Pairs of rows in `view` that an identity key says are one entity, as (collection, smaller
/// gid, larger gid). Replicas alias such pairs, so after they converge this is empty.
pub fn unmerged_identities(schema: &Schema, view: &View) -> Vec<(String, Gid, Gid)> {
    let mut found = Vec::new();
    for c in &schema.collections {
        let rows: Vec<(&Gid, &Row)> = view.rows(&c.name).collect();
        for (i, (a, ra)) in rows.iter().enumerate() {
            for (b, rb) in &rows[i + 1..] {
                let agree = |key: &Vec<String>| {
                    key.iter().all(|f| match (ra.get(f), rb.get(f)) {
                        (Some(x), Some(y)) => {
                            *x != Value::Null && x.total_cmp(y) == Ordering::Equal
                        }
                        _ => false,
                    })
                };
                if c.identity_keys.iter().any(agree) {
                    found.push((c.name.clone(), **a, **b));
                }
            }
        }
    }
    found
}

fn encodable(clock: &Hlc) -> bool {
    Hlc::decode(&clock.encode()) == Some(*clock)
}

struct Merged<'a> {
    schema: &'a Schema,
    home: BTreeMap<Gid, &'a Collection>,
    registers: BTreeMap<Gid, BTreeMap<&'a str, (Value, Hlc)>>,
    root: BTreeMap<Gid, Gid>,
    live: BTreeSet<Gid>,
}

impl<'a> Merged<'a> {
    fn new(schema: &'a Schema, changes: &'a [Change]) -> Merged<'a> {
        let candidates: Vec<(&Change, &Collection)> = changes
            .iter()
            .filter(|c| encodable(&c.clock))
            .filter(|c| c.field != ALIAS || matches!(c.value, Value::Ref(t) if t != c.gid))
            .filter_map(|c| Some((c, schema.collection(&c.collection)?)))
            .collect();

        let mut first: BTreeMap<Gid, (Hlc, &Collection)> = BTreeMap::new();
        for &(c, coll) in &candidates {
            let claim = (c.clock, coll);
            first
                .entry(c.gid)
                .and_modify(|held| {
                    if (claim.0, &claim.1.name) < (held.0, &held.1.name) {
                        *held = claim;
                    }
                })
                .or_insert(claim);
        }
        let home: BTreeMap<Gid, &Collection> =
            first.into_iter().map(|(gid, (_, c))| (gid, c)).collect();
        let accepted: Vec<(&Change, &Collection)> = candidates
            .into_iter()
            .filter(|(c, coll)| home[&c.gid].name == coll.name)
            .collect();

        let mut registers: BTreeMap<Gid, BTreeMap<&str, (Value, Hlc)>> = BTreeMap::new();
        let mut edges = BTreeSet::new();
        for &(c, coll) in &accepted {
            if c.field == ALIAS {
                if let Value::Ref(target) = c.value
                    && home.get(&target).is_some_and(|t| t.name == coll.name)
                {
                    edges.insert((c.gid, target));
                }
                continue;
            }
            let fields = registers.entry(c.gid).or_default();
            let stored = fields.get(c.field.as_str()).map(|(v, k)| (v, k));
            if wins(coll.strategy(&c.field), stored, &c.value, &c.clock) {
                fields.insert(c.field.as_str(), (c.value.clone(), c.clock));
            }
        }

        let mut merged = Merged {
            schema,
            home,
            registers,
            root: BTreeMap::new(),
            live: BTreeSet::new(),
        };
        merged.group(&edges);
        merged.enliven();
        merged
    }

    fn find(&self, gid: Gid) -> Gid {
        let mut at = gid;
        while let Some(&up) = self.root.get(&at) {
            if up == at {
                break;
            }
            at = up;
        }
        at
    }

    fn union(&mut self, a: Gid, b: Gid) -> bool {
        let (ra, rb) = (self.find(a), self.find(b));
        if ra == rb {
            return false;
        }
        let (small, large) = if ra < rb { (ra, rb) } else { (rb, ra) };
        self.root.insert(large, small);
        true
    }

    fn group(&mut self, edges: &BTreeSet<(Gid, Gid)>) {
        self.root = self.home.keys().map(|&gid| (gid, gid)).collect();
        for &(a, b) in edges {
            self.union(a, b);
        }
        loop {
            let mut merged_any = false;
            let schema = self.schema;
            for c in &schema.collections {
                let Identity::Natural(key) = &c.identity else {
                    continue;
                };
                let mut keyed: Vec<(Vec<Value>, Gid)> = Vec::new();
                for (&gid, coll) in &self.home {
                    if coll.name != c.name {
                        continue;
                    }
                    let Some(values) = key
                        .iter()
                        .map(|f| self.register(gid, f).map(|(v, _)| self.canonical(v)))
                        .collect::<Option<Vec<Value>>>()
                    else {
                        continue;
                    };
                    keyed.push((values, gid));
                }
                for (i, (ka, a)) in keyed.iter().enumerate() {
                    for (kb, b) in &keyed[i + 1..] {
                        let equal = ka
                            .iter()
                            .zip(kb)
                            .all(|(x, y)| x.total_cmp(y) == Ordering::Equal);
                        if equal && self.union(*a, *b) {
                            merged_any = true;
                        }
                    }
                }
            }
            if !merged_any {
                break;
            }
        }
    }

    fn canonical(&self, value: &Value) -> Value {
        match value {
            Value::Ref(g) => Value::Ref(self.find(*g)),
            other => other.clone(),
        }
    }

    fn register(&self, gid: Gid, field: &str) -> Option<&(Value, Hlc)> {
        self.registers.get(&gid)?.get(field)
    }

    fn live_roots(&self) -> BTreeSet<Gid> {
        self.live.iter().map(|&m| self.find(m)).collect()
    }

    fn names_live(&self, value: &Value, target: &str, live_roots: &BTreeSet<Gid>) -> Option<Gid> {
        match value {
            Value::Ref(g) if self.home.get(g).is_some_and(|c| c.name == target) => {
                let root = self.find(*g);
                live_roots.contains(&root).then_some(root)
            }
            _ => None,
        }
    }

    fn enliven(&mut self) {
        loop {
            let live_roots = self.live_roots();
            let newly: Vec<Gid> = self
                .home
                .iter()
                .filter(|(gid, _)| !self.live.contains(gid))
                .filter(|(gid, _)| {
                    !matches!(self.register(**gid, DELETED), Some((Value::Int(1), _)))
                })
                .filter(|(gid, coll)| {
                    coll.fields
                        .iter()
                        .all(|f| match (&f.kind, self.register(**gid, &f.name)) {
                            (FieldKind::Owner(_), None | Some((Value::Null, _))) => true,
                            (FieldKind::Owner(t), Some((v, _))) => {
                                self.names_live(v, t, &live_roots).is_some()
                            }
                            _ => true,
                        })
                })
                .map(|(gid, _)| *gid)
                .collect();
            if newly.is_empty() {
                break;
            }
            self.live.extend(newly);
        }
    }

    fn present(&self) -> View {
        let live_roots = self.live_roots();
        let mut members: BTreeMap<Gid, Vec<Gid>> = BTreeMap::new();
        for &m in &self.live {
            members.entry(self.find(m)).or_default().push(m);
        }
        let mut view = View::default();
        for (root, live_members) in members {
            let coll = self.home[&root];
            let read = |f: &Field, v: &Value| match &f.kind {
                FieldKind::Plain => v.clone(),
                FieldKind::Reference(t) | FieldKind::Owner(t) => self
                    .names_live(v, t, &live_roots)
                    .map_or(Value::Null, Value::Ref),
            };
            let mut row = Row::new();
            for f in coll.fields.iter().filter(|f| !coll.in_together(&f.name)) {
                let mut best: Option<&(Value, Hlc)> = None;
                for &m in &live_members {
                    if let Some(entry) = self.register(m, &f.name)
                        && wins(f.strategy, best.map(|(v, k)| (v, k)), &entry.0, &entry.1)
                    {
                        best = Some(entry);
                    }
                }
                let value = best.map_or(Value::Null, |(v, _)| read(f, v));
                row.insert(f.name.clone(), value);
            }
            for set in &coll.together {
                let mut best: Option<(Hlc, Gid)> = None;
                for &m in &live_members {
                    let latest = set
                        .iter()
                        .filter_map(|f| self.register(m, f).map(|(_, k)| *k))
                        .max();
                    if let Some(at) = latest
                        && best.is_none_or(|(held, _)| at > held)
                    {
                        best = Some((at, m));
                    }
                }
                for name in set {
                    let f = coll.declared(name).expect("checked schema");
                    let value = match best {
                        Some((at, m)) => match self.register(m, name) {
                            Some((v, k)) if *k == at => read(f, v),
                            _ => Value::Null,
                        },
                        None => Value::Null,
                    };
                    row.insert(name.clone(), value);
                }
            }
            view.insert(&coll.name, root, row);
        }
        view
    }
}

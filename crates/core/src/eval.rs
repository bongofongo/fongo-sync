//! `model::view` computed on demand: the target of one entity, pulling from a [`Source`] only the
//! facts it needs. For every input, [`Session::target`] agrees with what `model::view` shows.
//!
//! Work is bounded by [`Limits`]. Where a bound is reached the answer is [`Undetermined`], and
//! whether it is reached depends only on the facts in the source: never on the order of
//! evaluation or on what an earlier question in the same session happened to compute.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use crate::change::{DELETED, wins};
use crate::clock::Hlc;
use crate::model::{Collection, FieldKind, Identity, Row, Schema};
use crate::source::{Alias, Slot, Source, slot};
use crate::total::{Total, same_row};
use crate::value::{Gid, Value};

/// Bounds on the work one question may take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The most gids the evaluator gathers to decide one alias group: the group, the gids its
    /// natural keys name, and natural entities whose keys may equal its keys.
    pub region: usize,
    /// The most candidate keys tried for one natural entity: the product, over its key fields, of
    /// the number of distinct values ever written to that field.
    pub key_combinations: usize,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            region: 1 << 16,
            key_combinations: 1 << 10,
        }
    }
}

/// A question whose answer needs more work than [`Limits`] allow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Undetermined;

/// What one entity's alias group should look like.
#[derive(Debug, Clone)]
pub struct Target {
    pub collection: String,
    /// The group's smallest gid; the asked gid if it does not belong to the collection.
    pub canonical: Gid,
    /// Every gid of the group, ascending, live or not. Empty if the asked gid does not belong to
    /// the collection.
    pub members: Vec<Gid>,
    /// The live members, ascending.
    pub live: Vec<Gid>,
    /// The row `model::view` shows under `canonical`; `Some` exactly when a member is live.
    pub row: Option<Row>,
}

impl PartialEq for Target {
    fn eq(&self, other: &Target) -> bool {
        self.collection == other.collection
            && self.canonical == other.canonical
            && self.members == other.members
            && self.live == other.live
            && match (&self.row, &other.row) {
                (Some(a), Some(b)) => same_row(a, b),
                (None, None) => true,
                _ => false,
            }
    }
}

impl Target {
    fn absent(collection: &str, gid: Gid) -> Target {
        Target {
            collection: collection.to_string(),
            canonical: gid,
            members: Vec::new(),
            live: Vec::new(),
            row: None,
        }
    }
}

/// Answers questions about one schema.
#[derive(Debug, Clone)]
pub struct Evaluator<'s> {
    pub(crate) schema: &'s Schema,
    limits: Limits,
}

impl<'s> Evaluator<'s> {
    /// An evaluator for `schema`, which must pass `Schema::check`.
    pub fn new(schema: &'s Schema) -> Result<Evaluator<'s>, String> {
        schema.check()?;
        Ok(Evaluator {
            schema,
            limits: Limits::default(),
        })
    }

    pub fn with_limits(self, limits: Limits) -> Evaluator<'s> {
        Evaluator { limits, ..self }
    }

    pub fn schema(&self) -> &'s Schema {
        self.schema
    }

    /// A session caches what it learns from `source`, so that many questions about the same
    /// facts share work. The source must not change while the session lives.
    pub fn session<'a, S: Source>(&'a self, source: &'a S) -> Session<'a, S> {
        Session {
            schema: self.schema,
            limits: self.limits,
            source,
            infos: BTreeMap::new(),
            groups: BTreeMap::new(),
            member_live: BTreeMap::new(),
            group_live: BTreeMap::new(),
            targets: BTreeMap::new(),
        }
    }

    /// [`Session::target`] in a session of its own.
    pub fn target<S: Source>(
        &self,
        source: &S,
        collection: &str,
        gid: Gid,
    ) -> Result<Target, Undetermined> {
        self.session(source).target(collection, gid)
    }

    /// Every (collection, gid) with a claim in `source`: asking [`Session::target`] about each
    /// is the trivially complete alternative to [`Evaluator::affected`](crate::eval::Evaluator::affected).
    pub fn everything<S: Source>(&self, source: &S) -> BTreeSet<(String, Gid)> {
        let mut all = BTreeSet::new();
        for gid in source.gids() {
            for claim in source.claims(gid) {
                if self.schema.collection(&claim.collection).is_some() {
                    all.insert((claim.collection, gid));
                }
            }
        }
        all
    }
}

#[derive(Debug)]
enum Key {
    Minted,
    Verified(Vec<Value>),
    Unverified,
}

#[derive(Debug)]
struct Info {
    home: Option<usize>,
    registers: BTreeMap<String, (Value, Hlc)>,
    key: Key,
    aliases: Vec<Alias>,
}

#[derive(Debug)]
struct Group {
    canonical: Gid,
    members: Vec<Gid>,
}

type Answer<T> = Result<T, Undetermined>;

/// Questions about one source, sharing a cache. Every cached answer is exact, so the cache changes
/// how much work a question takes but never its answer.
pub struct Session<'a, S> {
    pub(crate) schema: &'a Schema,
    limits: Limits,
    pub(crate) source: &'a S,
    infos: BTreeMap<Gid, Answer<Rc<Info>>>,
    groups: BTreeMap<Gid, Answer<Rc<Group>>>,
    member_live: BTreeMap<Gid, Answer<bool>>,
    group_live: BTreeMap<Gid, Answer<bool>>,
    targets: BTreeMap<Gid, Answer<Rc<Target>>>,
}

impl<S: Source> Session<'_, S> {
    /// The target of the group `gid` belongs to in `collection`, as `model::view` of the changes
    /// behind the source would show it.
    ///
    /// Cost, for a group of `m` members whose region (see [`Limits::region`]) holds `r` gids:
    /// O(r log r) source questions and work to find the group, times the number of rounds in
    /// which new natural entities are found to share a key (one per level of natural keys naming
    /// natural keys, in data that does not contrive otherwise); then O(m) to decide liveness,
    /// each member asking once about each of its owners' groups, which are found the same way.
    pub fn target(&mut self, collection: &str, gid: Gid) -> Answer<Target> {
        self.shared(collection, gid).map(|t| (*t).clone())
    }

    /// [`Session::target`] without copying the target out of the session's cache.
    pub(crate) fn shared(&mut self, collection: &str, gid: Gid) -> Answer<Rc<Target>> {
        let Some(home) = self
            .schema
            .collections
            .iter()
            .position(|c| c.name == collection)
        else {
            return Ok(Rc::new(Target::absent(collection, gid)));
        };
        if self.info(gid)?.home != Some(home) {
            return Ok(Rc::new(Target::absent(collection, gid)));
        }
        let group = self.group(gid)?;
        if let Some(t) = self.targets.get(&group.canonical) {
            return t.clone();
        }
        let target = self.compute_target(home, &group).map(Rc::new);
        self.targets.insert(group.canonical, target.clone());
        target
    }

    pub(crate) fn home(&mut self, gid: Gid) -> Answer<Option<&Collection>> {
        let schema = self.schema;
        Ok(self.info(gid)?.home.map(|h| &schema.collections[h]))
    }

    fn compute_target(&mut self, home: usize, group: &Group) -> Answer<Target> {
        let mut live = Vec::new();
        for &m in &group.members {
            if self.member_live(m)? {
                live.push(m);
            }
        }
        let row = if live.is_empty() {
            None
        } else {
            Some(self.present(home, &live)?)
        };
        Ok(Target {
            collection: self.schema.collections[home].name.clone(),
            canonical: group.canonical,
            members: group.members.clone(),
            live,
            row,
        })
    }

    fn present(&mut self, home: usize, live: &[Gid]) -> Answer<Row> {
        let coll = &self.schema.collections[home];
        let infos: Vec<Rc<Info>> = live.iter().map(|&m| self.info(m)).collect::<Answer<_>>()?;
        let mut row = Row::new();
        let in_together = |name: &str| coll.together.iter().any(|s| s.iter().any(|f| f == name));
        for f in coll.fields.iter().filter(|f| !in_together(&f.name)) {
            let mut best: Option<&(Value, Hlc)> = None;
            for info in &infos {
                if let Some(entry) = info.registers.get(&f.name)
                    && wins(f.strategy, best.map(|(v, k)| (v, k)), &entry.0, &entry.1)
                {
                    best = Some(entry);
                }
            }
            let value = match best {
                Some((v, _)) => self.read(&f.kind, v)?,
                None => Value::Null,
            };
            row.insert(f.name.clone(), value);
        }
        for set in &coll.together {
            let mut best: Option<(Hlc, usize)> = None;
            for (i, info) in infos.iter().enumerate() {
                let latest = set
                    .iter()
                    .filter_map(|f| info.registers.get(f).map(|(_, k)| *k))
                    .max();
                if let Some(at) = latest
                    && best.is_none_or(|(held, _)| at > held)
                {
                    best = Some((at, i));
                }
            }
            for name in set {
                let kind = &coll
                    .fields
                    .iter()
                    .find(|f| &f.name == name)
                    .expect("checked schema")
                    .kind;
                let value = match best {
                    Some((at, i)) => match infos[i].registers.get(name) {
                        Some((v, k)) if *k == at => self.read(kind, v)?,
                        _ => Value::Null,
                    },
                    None => Value::Null,
                };
                row.insert(name.clone(), value);
            }
        }
        Ok(row)
    }

    fn read(&mut self, kind: &FieldKind, value: &Value) -> Answer<Value> {
        Ok(match kind {
            FieldKind::Plain => value.clone(),
            FieldKind::Reference(t) | FieldKind::Owner(t) => {
                self.names_live(value, t)?.map_or(Value::Null, Value::Ref)
            }
        })
    }

    fn names_live(&mut self, value: &Value, collection: &str) -> Answer<Option<Gid>> {
        let Value::Ref(gid) = value else {
            return Ok(None);
        };
        let schema = self.schema;
        let home = self.info(*gid)?.home;
        if home.is_none_or(|h| schema.collections[h].name != collection) {
            return Ok(None);
        }
        let group = self.group(*gid)?;
        Ok(self.group_live(&group)?.then_some(group.canonical))
    }

    fn group_live(&mut self, group: &Group) -> Answer<bool> {
        if let Some(a) = self.group_live.get(&group.canonical) {
            return *a;
        }
        let mut answer = Ok(false);
        for &m in &group.members {
            match self.member_live(m) {
                Ok(true) => {
                    answer = Ok(true);
                    break;
                }
                Ok(false) => {}
                Err(u) => answer = Err(u),
            }
        }
        self.group_live.insert(group.canonical, answer);
        answer
    }

    fn member_live(&mut self, m: Gid) -> Answer<bool> {
        if let Some(a) = self.member_live.get(&m) {
            return *a;
        }
        let answer = self.compute_member_live(m);
        self.member_live.insert(m, answer);
        answer
    }

    fn compute_member_live(&mut self, m: Gid) -> Answer<bool> {
        let info = self.info(m)?;
        let Some(home) = info.home else {
            return Ok(false);
        };
        if matches!(info.key, Key::Unverified)
            || matches!(info.registers.get(DELETED), Some((Value::Int(1), _)))
        {
            return Ok(false);
        }
        let schema = self.schema;
        let mut answer = Ok(true);
        for f in &schema.collections[home].fields {
            let FieldKind::Owner(t) = &f.kind else {
                continue;
            };
            match info.registers.get(&f.name) {
                None | Some((Value::Null, _)) => {}
                Some((v, _)) => match self.names_live(v, t) {
                    Ok(Some(_)) => {}
                    Ok(None) => return Ok(false),
                    Err(u) => answer = Err(u),
                },
            }
        }
        answer
    }

    fn info(&mut self, gid: Gid) -> Answer<Rc<Info>> {
        if let Some(a) = self.infos.get(&gid) {
            return a.clone();
        }
        let answer = self.fetch(gid).map(Rc::new);
        self.infos.insert(gid, answer.clone());
        answer
    }

    fn fetch(&self, gid: Gid) -> Answer<Info> {
        let schema = self.schema;
        let home = self
            .source
            .claims(gid)
            .into_iter()
            .filter_map(|c| {
                let i = schema
                    .collections
                    .iter()
                    .position(|s| s.name == c.collection)?;
                Some((c.first, c.collection, i))
            })
            .min()
            .map(|(_, _, i)| i);
        let Some(home) = home else {
            return Ok(Info {
                home: None,
                registers: BTreeMap::new(),
                key: Key::Minted,
                aliases: Vec::new(),
            });
        };
        let coll = &schema.collections[home];
        let mut registers: BTreeMap<String, (Value, Hlc)> = BTreeMap::new();
        let mut options: BTreeMap<String, BTreeMap<Total, Hlc>> = BTreeMap::new();
        for e in self.source.entries(&coll.name, gid) {
            match slot(schema, &coll.name, &e.field) {
                Slot::Earliest => {
                    let latest = options
                        .entry(e.field)
                        .or_default()
                        .entry(Total(e.value))
                        .or_insert(e.clock);
                    *latest = (*latest).max(e.clock);
                }
                Slot::One(strategy) => {
                    let held = registers.get(&e.field).map(|(v, k)| (v, k));
                    if wins(strategy, held, &e.value, &e.clock) {
                        registers.insert(e.field, (e.value, e.clock));
                    }
                }
            }
        }
        let key = match &coll.identity {
            Identity::Minted => Key::Minted,
            Identity::Natural(fields) => {
                let lists: Vec<Vec<(&Value, Hlc)>> = fields
                    .iter()
                    .map(|f| {
                        options
                            .get(f)
                            .into_iter()
                            .flatten()
                            .map(|(v, k)| (&v.0, *k))
                            .collect()
                    })
                    .collect();
                let combinations = lists
                    .iter()
                    .try_fold(1usize, |n, l| n.checked_mul(l.len()))
                    .unwrap_or(usize::MAX);
                if combinations > self.limits.key_combinations {
                    return Err(Undetermined);
                }
                match deriving(&coll.name, gid, &lists, &mut Vec::new()) {
                    Some(picked) => {
                        for (f, (v, k)) in fields.iter().zip(&picked) {
                            registers.insert(f.clone(), (v.clone(), *k));
                        }
                        Key::Verified(picked.into_iter().map(|(v, _)| v).collect())
                    }
                    None => Key::Unverified,
                }
            }
        };
        Ok(Info {
            home: Some(home),
            registers,
            key,
            aliases: self.source.aliases(gid),
        })
    }

    fn group(&mut self, gid: Gid) -> Answer<Rc<Group>> {
        if let Some(a) = self.groups.get(&gid) {
            return a.clone();
        }
        match self.region(gid) {
            Ok(()) => self.groups[&gid].clone(),
            Err(u) => {
                self.groups.insert(gid, Err(u));
                Err(u)
            }
        }
    }

    /// Gathers the least set of gids that contains `start` and is closed under: alias
    /// neighbours, the gids a natural key names, and natural entities that may share a key with
    /// one in the set. Alias groups computed over that set alone are the groups over every fact,
    /// so the group of every gid in it is cached.
    fn region(&mut self, start: Gid) -> Answer<()> {
        let schema = self.schema;
        let mut region = Region::default();
        region.add(start);
        let mut asked: BTreeSet<(usize, Vec<(String, Total)>)> = BTreeSet::new();
        loop {
            while let Some(i) = region.pending.pop() {
                let x = region.gids[i];
                let info = self.info(x)?;
                let home = info.home.map(|h| schema.collections[h].name.as_str());
                for a in &info.aliases {
                    if Some(a.collection.as_str()) == home {
                        region.add(if a.from == x { a.to } else { a.from });
                    }
                }
                if let Key::Verified(key) = &info.key {
                    for v in key {
                        if let Value::Ref(y) = v {
                            region.add(*y);
                        }
                    }
                }
                if region.gids.len() > self.limits.region {
                    return Err(Undetermined);
                }
            }

            let infos: Vec<Rc<Info>> = region
                .gids
                .iter()
                .map(|&g| self.info(g))
                .collect::<Answer<_>>()?;
            let mut uf = UnionFind::new(&region.gids);
            for (i, info) in infos.iter().enumerate() {
                for a in &info.aliases {
                    if a.from != region.gids[i] {
                        continue;
                    }
                    let Some(&j) = region.pos.get(&a.to) else {
                        continue;
                    };
                    let joins = |info: &Info| {
                        info.home
                            .is_some_and(|h| schema.collections[h].name == a.collection)
                            && !matches!(info.key, Key::Unverified)
                    };
                    if joins(info) && joins(&infos[j]) {
                        uf.union(i, j);
                    }
                }
            }
            let naturals: Vec<(usize, usize, &Vec<Value>)> = infos
                .iter()
                .enumerate()
                .filter_map(|(i, info)| match (&info.key, info.home) {
                    (Key::Verified(k), Some(h)) => Some((i, h, k)),
                    _ => None,
                })
                .collect();
            loop {
                let mut joined = false;
                let mut seen: BTreeMap<(usize, Vec<Total>), usize> = BTreeMap::new();
                for &(i, h, key) in &naturals {
                    let canonical: Vec<Total> = key
                        .iter()
                        .map(|v| match v {
                            Value::Ref(y) => Total(Value::Ref(uf.least(region.pos[y]))),
                            other => Total(other.clone()),
                        })
                        .collect();
                    match seen.get(&(h, canonical.clone())) {
                        Some(&j) => joined |= uf.union(i, j),
                        None => {
                            seen.insert((h, canonical), i);
                        }
                    }
                }
                if !joined {
                    break;
                }
            }

            let classes = uf.classes();
            // Natural entities that share a collection, a reference position, the values at the
            // other positions and the class that reference falls in ask the same questions, so
            // each such shape asks once per member of the class.
            let mut shapes: BTreeSet<Shape> = BTreeSet::new();
            for &(_, h, key) in &naturals {
                let Some(p) = key.iter().position(|v| matches!(v, Value::Ref(_))) else {
                    continue;
                };
                let Value::Ref(y) = key[p] else {
                    continue;
                };
                let Identity::Natural(fields) = &schema.collections[h].identity else {
                    continue;
                };
                let fixed: Vec<(String, Total)> = fields
                    .iter()
                    .zip(key)
                    .filter(|(_, v)| !matches!(v, Value::Ref(_)))
                    .map(|(f, v)| (f.clone(), Total(v.clone())))
                    .collect();
                shapes.insert((h, p, fixed, uf.find(region.pos[&y])));
            }
            for (h, p, fixed, root) in shapes {
                let Identity::Natural(fields) = &schema.collections[h].identity else {
                    continue;
                };
                for &m in &classes[&root] {
                    let mut question = vec![(fields[p].clone(), Total(Value::Ref(region.gids[m])))];
                    question.extend(fixed.iter().cloned());
                    if !asked.insert((h, question.clone())) {
                        continue;
                    }
                    let pairs: Vec<(&str, &Value)> =
                        question.iter().map(|(f, v)| (f.as_str(), &v.0)).collect();
                    for z in self.source.holding(&schema.collections[h].name, &pairs) {
                        region.add(z);
                    }
                }
            }
            if region.pending.is_empty() {
                for members in classes.values() {
                    let mut gids: Vec<Gid> = members.iter().map(|&i| region.gids[i]).collect();
                    gids.sort();
                    let group = Rc::new(Group {
                        canonical: gids[0],
                        members: gids,
                    });
                    for &g in &group.members {
                        self.groups.insert(g, Ok(group.clone()));
                    }
                }
                return Ok(());
            }
            if region.gids.len() > self.limits.region {
                return Err(Undetermined);
            }
        }
    }
}

/// The key, one value from each list in order, whose derived gid is `gid`; the first such in
/// the lists' order, as `model::view` picks it.
fn deriving<'v>(
    collection: &str,
    gid: Gid,
    lists: &[Vec<(&'v Value, Hlc)>],
    chosen: &mut Vec<(&'v Value, Hlc)>,
) -> Option<Vec<(Value, Hlc)>> {
    let Some((first, rest)) = lists.split_first() else {
        let key: Vec<Value> = chosen.iter().map(|(v, _)| (*v).clone()).collect();
        return (Gid::derive(collection, &key) == gid)
            .then(|| chosen.iter().map(|(v, k)| ((*v).clone(), *k)).collect());
    };
    for &option in first {
        chosen.push(option);
        if let Some(found) = deriving(collection, gid, rest, chosen) {
            return Some(found);
        }
        chosen.pop();
    }
    None
}

/// A natural collection, the position of the reference its key asks by, the values at the key's
/// other positions, and the class that reference falls in.
type Shape = (usize, usize, Vec<(String, Total)>, usize);

#[derive(Default)]
struct Region {
    gids: Vec<Gid>,
    pos: BTreeMap<Gid, usize>,
    pending: Vec<usize>,
}

impl Region {
    fn add(&mut self, gid: Gid) -> bool {
        if self.pos.contains_key(&gid) {
            return false;
        }
        self.pos.insert(gid, self.gids.len());
        self.pending.push(self.gids.len());
        self.gids.push(gid);
        true
    }
}

struct UnionFind {
    parent: Vec<usize>,
    size: Vec<usize>,
    least: Vec<Gid>,
}

impl UnionFind {
    fn new(gids: &[Gid]) -> UnionFind {
        UnionFind {
            parent: (0..gids.len()).collect(),
            size: vec![1; gids.len()],
            least: gids.to_vec(),
        }
    }

    fn find(&mut self, i: usize) -> usize {
        let mut root = i;
        while self.parent[root] != root {
            root = self.parent[root];
        }
        let mut at = i;
        while self.parent[at] != root {
            let next = self.parent[at];
            self.parent[at] = root;
            at = next;
        }
        root
    }

    fn least(&mut self, i: usize) -> Gid {
        let root = self.find(i);
        self.least[root]
    }

    fn union(&mut self, a: usize, b: usize) -> bool {
        let (mut ra, mut rb) = (self.find(a), self.find(b));
        if ra == rb {
            return false;
        }
        if self.size[ra] < self.size[rb] {
            std::mem::swap(&mut ra, &mut rb);
        }
        self.parent[rb] = ra;
        self.size[ra] += self.size[rb];
        self.least[ra] = self.least[ra].min(self.least[rb]);
        true
    }

    fn classes(&mut self) -> BTreeMap<usize, Vec<usize>> {
        let mut classes: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
        for i in 0..self.parent.len() {
            let root = self.find(i);
            classes.entry(root).or_default().push(i);
        }
        classes
    }
}

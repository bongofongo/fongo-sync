//! `model::view` computed on demand: the target of one entity, pulling from a [`Source`] only the
//! facts it needs. For every input, [`Session::target`] agrees with what `model::view` shows.
//!
//! What a storage adapter must do around these questions is listed in [`crate::plan`].
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
    ///
    /// Groups are kept as they are found: each gid is fetched once, each alias joins once, and a
    /// join re-reads only the keys that name the smaller of the two groups (congruence closure),
    /// so the work is near-linear in the set and the questions it asks.
    fn region(&mut self, start: Gid) -> Answer<()> {
        let schema = self.schema;
        let mut r = Closure::default();
        r.add(start);
        let mut asked: BTreeSet<(usize, Vec<(String, Total)>)> = BTreeSet::new();
        let mut infos: BTreeMap<usize, Rc<Info>> = BTreeMap::new();
        let mut edges: Vec<(usize, usize, String)> = Vec::new();
        loop {
            while let Some(i) = r.pending.pop() {
                crate::work::add(1);
                let x = r.gids[i];
                let info = self.info(x)?;
                let home = info.home.map(|h| schema.collections[h].name.as_str());
                for a in &info.aliases {
                    if Some(a.collection.as_str()) == home {
                        let j = r.add(if a.from == x { a.to } else { a.from });
                        edges.push((i, j, a.collection.clone()));
                    }
                }
                if let (Key::Verified(key), Some(h)) = (&info.key, info.home) {
                    let refs: Vec<Option<usize>> = key
                        .iter()
                        .map(|v| match v {
                            Value::Ref(y) => Some(r.add(*y)),
                            _ => None,
                        })
                        .collect();
                    r.natural(i, h, key, &refs, schema);
                }
                infos.insert(i, info);
                if r.gids.len() > self.limits.region {
                    return Err(Undetermined);
                }
            }
            for (i, j, collection) in std::mem::take(&mut edges) {
                let joins = |info: &Info| {
                    info.home
                        .is_some_and(|h| schema.collections[h].name == collection)
                        && !matches!(info.key, Key::Unverified)
                };
                if joins(&infos[&i]) && joins(&infos[&j]) {
                    r.joins.push((i, j));
                }
            }
            r.settle();
            if r.questions.is_empty() {
                break;
            }
            for ((h, p, fixed), m) in std::mem::take(&mut r.questions) {
                crate::work::add(1);
                let Identity::Natural(fields) = &schema.collections[h].identity else {
                    continue;
                };
                let mut question = vec![(fields[p].clone(), Total(Value::Ref(r.gids[m])))];
                question.extend(fixed);
                if !asked.insert((h, question.clone())) {
                    continue;
                }
                let pairs: Vec<(&str, &Value)> =
                    question.iter().map(|(f, v)| (f.as_str(), &v.0)).collect();
                for z in self.source.holding(&schema.collections[h].name, &pairs) {
                    r.add(z);
                }
            }
            if r.gids.len() > self.limits.region {
                return Err(Undetermined);
            }
        }
        for i in 0..r.gids.len() {
            if r.parent[i] != i {
                continue;
            }
            let mut gids: Vec<Gid> = r.members[i].iter().map(|&m| r.gids[m]).collect();
            gids.sort();
            let group = Rc::new(Group {
                canonical: gids[0],
                members: gids,
            });
            for &g in &group.members {
                self.groups.insert(g, Ok(group.clone()));
            }
        }
        Ok(())
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

/// A natural collection, the position of the reference its key asks by, and the values at the
/// key's other positions: natural entities of one shape ask the same question about each member of
/// the group their reference falls in.
type Shape = (usize, usize, Vec<(String, Total)>);

/// A natural entity's collection, the region positions of the gids its key names, and its key.
type NaturalKey = (usize, Vec<Option<usize>>, Vec<Value>);

/// One position of a natural key read through the groups: the group a reference falls in, or a
/// value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Part {
    Group(usize),
    Value(Total),
}

/// The groups of a region as they grow: union-find by size over the region's gids, each root
/// holding its members, the natural entities whose keys name a member, and the shapes that ask
/// about its members.
#[derive(Default)]
struct Closure {
    gids: Vec<Gid>,
    pos: BTreeMap<Gid, usize>,
    pending: Vec<usize>,
    parent: Vec<usize>,
    members: Vec<Vec<usize>>,
    uses: Vec<Vec<usize>>,
    shapes: Vec<BTreeSet<Shape>>,
    keys: BTreeMap<usize, NaturalKey>,
    signs: BTreeMap<(usize, Vec<Part>), usize>,
    sign_of: BTreeMap<usize, (usize, Vec<Part>)>,
    joins: Vec<(usize, usize)>,
    questions: Vec<(Shape, usize)>,
}

impl Closure {
    fn add(&mut self, gid: Gid) -> usize {
        if let Some(&i) = self.pos.get(&gid) {
            return i;
        }
        let i = self.gids.len();
        self.pos.insert(gid, i);
        self.pending.push(i);
        self.gids.push(gid);
        self.parent.push(i);
        self.members.push(vec![i]);
        self.uses.push(Vec::new());
        self.shapes.push(BTreeSet::new());
        i
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

    /// Records natural entity `i` of collection `h`, whose key is `key`, its references at the
    /// region positions `refs`.
    fn natural(
        &mut self,
        i: usize,
        h: usize,
        key: &[Value],
        refs: &[Option<usize>],
        schema: &Schema,
    ) {
        for j in refs.iter().flatten() {
            let root = self.find(*j);
            self.uses[root].push(i);
        }
        self.keys.insert(i, (h, refs.to_vec(), key.to_vec()));
        self.sign(i);
        let (Some(p), Identity::Natural(fields)) = (
            refs.iter().position(Option::is_some),
            &schema.collections[h].identity,
        ) else {
            return;
        };
        let fixed: Vec<(String, Total)> = fields
            .iter()
            .zip(key)
            .filter(|(_, v)| !matches!(v, Value::Ref(_)))
            .map(|(f, v)| (f.clone(), Total(v.clone())))
            .collect();
        let shape = (h, p, fixed);
        let root = self.find(refs[p].expect("a reference"));
        if self.shapes[root].insert(shape.clone()) {
            for &m in &self.members[root] {
                self.questions.push((shape.clone(), m));
            }
        }
    }

    /// Files natural entity `i` under its key read through the current groups, and joins it with
    /// any entity already filed there.
    fn sign(&mut self, i: usize) {
        crate::work::add(1);
        let (h, refs, key) = self.keys[&i].clone();
        let parts: Vec<Part> = refs
            .iter()
            .zip(&key)
            .map(|(r, v)| match r {
                Some(j) => Part::Group(self.find(*j)),
                None => Part::Value(Total(v.clone())),
            })
            .collect();
        let sign = (h, parts);
        if let Some(old) = self.sign_of.insert(i, sign.clone())
            && self.signs.get(&old) == Some(&i)
        {
            self.signs.remove(&old);
        }
        match self.signs.get(&sign) {
            Some(&j) if j != i => self.joins.push((i, j)),
            Some(_) => {}
            None => {
                self.signs.insert(sign, i);
            }
        }
    }

    /// Joins every pair waiting to be joined, and every pair those joins make congruent.
    fn settle(&mut self) {
        while let Some((a, b)) = self.joins.pop() {
            let (mut big, mut small) = (self.find(a), self.find(b));
            if big == small {
                continue;
            }
            if self.members[big].len() < self.members[small].len() {
                std::mem::swap(&mut big, &mut small);
            }
            crate::work::add(self.members[small].len());
            for shape in &self.shapes[small] {
                if !self.shapes[big].contains(shape) {
                    for &m in &self.members[big] {
                        self.questions.push((shape.clone(), m));
                    }
                }
            }
            for shape in &self.shapes[big] {
                if !self.shapes[small].contains(shape) {
                    for &m in &self.members[small] {
                        self.questions.push((shape.clone(), m));
                    }
                }
            }
            self.parent[small] = big;
            let moved = std::mem::take(&mut self.members[small]);
            self.members[big].extend(moved);
            let shapes = std::mem::take(&mut self.shapes[small]);
            self.shapes[big].extend(shapes);
            let users = std::mem::take(&mut self.uses[small]);
            for &n in &users {
                self.sign(n);
            }
            self.uses[big].extend(users);
        }
    }
}

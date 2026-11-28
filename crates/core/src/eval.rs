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
use crate::model::{Collection, Field, FieldKind, Identity, Row, Schema};
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
    /// The most keys the evaluator looks natural entities up by to decide one alias group: for
    /// each distinct natural key in the region, read through the groups, the product of the
    /// sizes of the groups its references fall in, summed. Where every group a key names holds
    /// one gid, that is one per key, so at most the region's size.
    pub lookups: usize,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            region: 1 << 16,
            key_combinations: 1 << 10,
            lookups: 1 << 18,
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

/// Where a live group's state came from: which writes made its members, and which put each
/// field's value in its row. A clock names the replica that issued it.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Origin {
    /// The earliest of the writes that made the live members. Each live member's `$deleted`
    /// holds the `Int(0)` that [`crate::plan::record`] wrote when a row was last inserted for
    /// it. `None` while some live member's is not among the facts.
    pub made: Option<Hlc>,
    /// The clock of the write behind each field of the target's row: the strategy winner among
    /// the live members, or for a field in a `together` set the latest write to the set. A field
    /// no such write covers is absent. A natural key's key field counts only its earliest
    /// [`crate::model::KEY_ENTRIES`] writes, so its clock is the latest of those holding the key,
    /// not necessarily the latest making.
    pub fields: BTreeMap<String, Hlc>,
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
            aliases: BTreeMap::new(),
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
}

#[derive(Debug)]
struct Group {
    canonical: Gid,
    members: Vec<Gid>,
}

type Answer<T> = Result<T, Undetermined>;

/// A field, the value a row shows for it, and the clock of the write behind that value.
type Shown<'a> = (&'a Field, Option<Value>, Option<Hlc>);

/// Questions about one source, sharing a cache. Every cached answer is exact, so the cache changes
/// how much work a question takes but never its answer.
pub struct Session<'a, S> {
    pub(crate) schema: &'a Schema,
    limits: Limits,
    pub(crate) source: &'a S,
    infos: BTreeMap<Gid, Answer<Rc<Info>>>,
    aliases: BTreeMap<Gid, Rc<Vec<Alias>>>,
    groups: BTreeMap<Gid, Answer<Rc<Group>>>,
    member_live: BTreeMap<Gid, Answer<bool>>,
    group_live: BTreeMap<Gid, Answer<bool>>,
    targets: BTreeMap<Gid, Answer<Rc<Target>>>,
}

impl<'a, S: Source> Session<'a, S> {
    /// The target of the group `gid` belongs to in `collection`, as `model::view` of the changes
    /// behind the source would show it.
    ///
    /// Cost, for a group of `m` members whose region (see [`Limits::region`]) holds `r` gids: each
    /// gid in the region is fetched once, and each distinct natural key in it, read through the
    /// groups, asks the source once for each choice of one member from every group its
    /// references fall in. Where those groups hold one member each, that is O(r log r) in all.
    /// Where they are large it is their product for each distinct key, up to
    /// [`Limits::lookups`] in all: `n` items merged into one group, each with a tag keyed by the
    /// item and its own label, ask n² questions, and with the default limits are undetermined
    /// beyond 512 items. Then O(m)
    /// to decide liveness, each member asking once about each of its owners' groups, which are
    /// found the same way.
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
        crate::work::add(group.members.len());
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
        let schema = self.schema;
        let coll = &schema.collections[home];
        crate::work::add(live.len() * (coll.fields.len() + 1));
        let mut row = Row::new();
        for (f, value, _) in self.shown(home, live)? {
            let value = match value {
                Some(v) => self.read(&f.kind, &v)?,
                None => Value::Null,
            };
            row.insert(f.name.clone(), value);
        }
        Ok(row)
    }

    /// Each field of a group with live members `live`, the value its row shows before references
    /// are read, and the clock of the write behind it: the strategy winner among the live
    /// members, or for a `together` set the latest write to the set, whose fields it did not
    /// write show NULL. A field no live member has written has neither.
    fn shown(&mut self, home: usize, live: &[Gid]) -> Answer<Vec<Shown<'a>>> {
        let schema = self.schema;
        let coll = &schema.collections[home];
        let infos: Vec<Rc<Info>> = live.iter().map(|&m| self.info(m)).collect::<Answer<_>>()?;
        let in_together = |name: &str| coll.together.iter().any(|s| s.iter().any(|f| f == name));
        let mut out = Vec::with_capacity(coll.fields.len());
        for f in coll.fields.iter().filter(|f| !in_together(&f.name)) {
            let mut best: Option<&(Value, Hlc)> = None;
            for info in &infos {
                if let Some(entry) = info.registers.get(&f.name)
                    && wins(f.strategy, best.map(|(v, k)| (v, k)), &entry.0, &entry.1)
                {
                    best = Some(entry);
                }
            }
            out.push((f, best.map(|(v, _)| v.clone()), best.map(|(_, k)| *k)));
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
                let f = coll
                    .fields
                    .iter()
                    .find(|f| &f.name == name)
                    .expect("checked schema");
                let value = best.and_then(|(at, i)| match infos[i].registers.get(name) {
                    Some((v, k)) if *k == at => Some(v.clone()),
                    _ => None,
                });
                out.push((f, value, best.map(|(at, _)| at)));
            }
        }
        Ok(out)
    }

    /// Where the target of the group `gid` belongs to in `collection` comes from, or `None` when
    /// no member is live; see [`Origin`]. A function of the same facts as the target.
    pub fn origin(&mut self, collection: &str, gid: Gid) -> Answer<Option<Origin>> {
        let target = self.shared(collection, gid)?;
        let Some(home) = self
            .schema
            .collections
            .iter()
            .position(|c| c.name == collection)
            .filter(|_| !target.live.is_empty())
        else {
            return Ok(None);
        };
        let mut made: Option<Hlc> = None;
        let mut whole = true;
        for &m in &target.live {
            match self.info(m)?.registers.get(DELETED) {
                Some((Value::Int(0), k)) => made = Some(made.map_or(*k, |f| f.min(*k))),
                _ => whole = false,
            }
        }
        let mut fields = BTreeMap::new();
        for (f, _, clock) in self.shown(home, &target.live)? {
            if let Some(clock) = clock {
                fields.insert(f.name.clone(), clock);
            }
        }
        Ok(Some(Origin {
            made: made.filter(|_| whole),
            fields,
        }))
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
            crate::work::add(1);
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

    /// The collection `gid` is kept in, from its claims alone.
    fn kept_in(&self, gid: Gid) -> Option<usize> {
        let schema = self.schema;
        self.source
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
            .map(|(_, _, i)| i)
    }

    fn fetch(&self, gid: Gid) -> Answer<Info> {
        let schema = self.schema;
        let Some(home) = self.kept_in(gid) else {
            return Ok(Info {
                home: None,
                registers: BTreeMap::new(),
                key: Key::Minted,
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
        })
    }

    /// The aliases kept with `gid` at either end, which only a gid that can join reads: one kept
    /// somewhere, and if natural, holding a key that derives it.
    fn aliases(&mut self, gid: Gid, info: &Info) -> Rc<Vec<Alias>> {
        if info.home.is_none() || matches!(info.key, Key::Unverified) {
            return Rc::new(Vec::new());
        }
        self.aliases
            .entry(gid)
            .or_insert_with(|| Rc::new(self.source.aliases(gid)))
            .clone()
    }

    /// Whether `gid` is kept in `collection` with `value` in `field`: in its row, or as its key.
    pub(crate) fn keeps(
        &mut self,
        gid: Gid,
        collection: &str,
        field: &str,
        value: &Value,
    ) -> Answer<bool> {
        let schema = self.schema;
        let info = self.info(gid)?;
        Ok(info
            .home
            .is_some_and(|h| schema.collections[h].name == collection)
            && info
                .registers
                .get(field)
                .is_some_and(|(v, _)| v.total_cmp(value) == std::cmp::Ordering::Equal))
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
    /// neighbours kept in the alias's collection, the gids a natural key names, and the natural
    /// entities that may share a key with one in the set, each the gid one of the keys reading
    /// as that key through the groups derives, where it is kept in the key's collection and
    /// holds that key. An alias joins only gids both kept in
    /// its collection, so of a neighbour kept elsewhere only its claims are read. Alias groups
    /// computed over that set alone are the groups over every fact, so the group of every gid in
    /// it is cached.
    ///
    /// Groups are kept as they are found: each gid is fetched once, each alias joins once, a join
    /// re-reads only the keys that name the smaller of the two groups (congruence closure), and a
    /// natural key is looked up by all its parts at once, so the work is near-linear in the set
    /// and the questions it asks.
    fn region(&mut self, start: Gid) -> Answer<()> {
        let schema = self.schema;
        let mut r = Closure {
            limit: self.limits.lookups,
            ..Closure::default()
        };
        r.add(start);
        let mut asked: BTreeSet<Gid> = BTreeSet::new();
        let mut infos: BTreeMap<usize, Rc<Info>> = BTreeMap::new();
        let mut edges: Vec<(usize, usize, String)> = Vec::new();
        loop {
            while let Some(i) = r.pending.pop() {
                crate::work::add(1);
                let x = r.gids[i];
                let info = self.info(x)?;
                let home = info.home.map(|h| schema.collections[h].name.as_str());
                for a in self.aliases(x, &info).iter() {
                    if Some(a.collection.as_str()) != home {
                        continue;
                    }
                    let y = if a.from == x { a.to } else { a.from };
                    let kept = match self.infos.get(&y) {
                        Some(Ok(known)) => known.home,
                        _ => self.kept_in(y),
                    };
                    if kept != info.home {
                        continue;
                    }
                    let j = r.add(y);
                    edges.push((i, j, a.collection.clone()));
                }
                if let (Key::Verified(key), Some(h)) = (&info.key, info.home) {
                    let refs: Vec<Option<usize>> = key
                        .iter()
                        .map(|v| match v {
                            Value::Ref(y) => Some(r.add(*y)),
                            _ => None,
                        })
                        .collect();
                    r.natural(i, h, key, &refs);
                    if r.over() {
                        return Err(Undetermined);
                    }
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
            if r.over() {
                return Err(Undetermined);
            }
            if r.questions.is_empty() {
                break;
            }
            for (h, key) in std::mem::take(&mut r.questions) {
                crate::work::add(1);
                let Identity::Natural(fields) = &schema.collections[h].identity else {
                    continue;
                };
                // Only the gid a key derives can hold it as its key, so no other is asked about,
                // and it takes room in the region only where it holds the key.
                let name = &schema.collections[h].name;
                let z = Gid::derive(name, &key);
                if !asked.insert(z) {
                    continue;
                }
                let kept = match self.infos.get(&z) {
                    Some(Ok(known)) => known.home,
                    _ => self.kept_in(z),
                };
                if kept != Some(h) {
                    continue;
                }
                let entries = self.source.entries(name, z);
                let holds = fields.iter().zip(&key).all(|(f, v)| {
                    entries
                        .iter()
                        .any(|e| &e.field == f && e.value.total_cmp(v) == std::cmp::Ordering::Equal)
                });
                if holds {
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

/// A natural entity's collection, the region positions of the gids its key names, and its key.
type NaturalKey = (usize, Vec<Option<usize>>, Vec<Value>);

/// One position of a natural key read through the groups: the group a reference falls in, or a
/// value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Part {
    Group(usize),
    Value(Total),
}

/// A natural key read through the groups, with its collection. Natural entities with one sign
/// share a key.
type Sign = (usize, Vec<Part>);

/// The groups of a region as they grow: union-find by size over the region's gids, each root
/// holding its members, the natural entities whose keys name a member, and the signs that ask
/// about its members.
///
/// Each sign with a reference asks the source, once, for every key that reads as that sign: one
/// question per choice of a member from each of its groups. When groups join, only the keys the
/// join adds are asked, and of two signs the join makes equal only the one that asked more goes on.
#[derive(Default)]
struct Closure {
    gids: Vec<Gid>,
    pos: BTreeMap<Gid, usize>,
    pending: Vec<usize>,
    parent: Vec<usize>,
    members: Vec<Vec<usize>>,
    uses: Vec<Vec<usize>>,
    keys: BTreeMap<usize, NaturalKey>,
    signs: BTreeMap<Sign, usize>,
    sign_of: BTreeMap<usize, Sign>,
    joins: Vec<(usize, usize)>,
    asks: Vec<Option<Sign>>,
    ask_of: BTreeMap<Sign, usize>,
    asks_at: Vec<BTreeSet<usize>>,
    questions: Vec<(usize, Vec<Value>)>,
    /// The lookups the live signs stand for, and the most allowed. A join never lowers the sum:
    /// two signs it makes equal named disjoint groups at some position.
    lookups: usize,
    limit: usize,
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
        self.asks_at.push(BTreeSet::new());
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
    fn natural(&mut self, i: usize, h: usize, key: &[Value], refs: &[Option<usize>]) {
        for j in refs.iter().flatten() {
            let root = self.find(*j);
            self.uses[root].push(i);
        }
        self.keys.insert(i, (h, refs.to_vec(), key.to_vec()));
        let sign = self.sign(i);
        if refs.iter().all(Option::is_none) || self.ask_of.contains_key(&sign) {
            return;
        }
        let size = lookups(&sign.1, |g| self.members[g].len());
        self.lookups = self.lookups.saturating_add(size);
        if self.over() {
            return;
        }
        let id = self.asks.len();
        let lists: Vec<&[usize]> = sign
            .1
            .iter()
            .filter_map(|p| match p {
                Part::Group(g) => Some(self.members[*g].as_slice()),
                Part::Value(_) => None,
            })
            .collect();
        let mut found = Vec::new();
        product(&sign.1, &lists, &self.gids, &mut found);
        self.questions.extend(found.into_iter().map(|k| (h, k)));
        for p in &sign.1 {
            if let Part::Group(g) = p {
                self.asks_at[*g].insert(id);
            }
        }
        self.ask_of.insert(sign.clone(), id);
        self.asks.push(Some(sign));
    }

    /// Files natural entity `i` under its key read through the current groups, and joins it with
    /// any entity already filed there.
    fn sign(&mut self, i: usize) -> Sign {
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
                self.signs.insert(sign.clone(), i);
            }
        }
        sign
    }

    /// Whether the live signs stand for more lookups than allowed.
    fn over(&self) -> bool {
        self.lookups > self.limit
    }

    /// Joins every pair waiting to be joined, and every pair those joins make congruent, unless
    /// the lookups grow past the limit.
    fn settle(&mut self) {
        while let Some((a, b)) = self.joins.pop() {
            if self.over() {
                return;
            }
            let (mut big, mut small) = (self.find(a), self.find(b));
            if big == small {
                continue;
            }
            if self.members[big].len() < self.members[small].len() {
                std::mem::swap(&mut big, &mut small);
            }
            crate::work::add(self.members[small].len());
            let kept = self.members[big].len();
            self.parent[small] = big;
            let moved = std::mem::take(&mut self.members[small]);
            self.members[big].extend(moved);
            self.regroup_asks(small, big, kept);
            let users = std::mem::take(&mut self.uses[small]);
            for &n in &users {
                self.sign(n);
            }
            self.uses[big].extend(users);
        }
    }

    /// After `small` joined `big`, whose first `kept` members were its own, moves the signs that
    /// asked about either group onto the joined group and asks the keys the join adds.
    fn regroup_asks(&mut self, small: usize, big: usize, kept: usize) {
        let mut sets = [
            std::mem::take(&mut self.asks_at[small]),
            std::mem::take(&mut self.asks_at[big]),
        ];
        if sets[0].len() > sets[1].len() {
            sets.swap(0, 1);
        }
        let [few, mut all] = sets;
        all.extend(few);
        crate::work::add(all.len());
        let mut by_sign: BTreeMap<Sign, Vec<(usize, Sign)>> = BTreeMap::new();
        for &id in &all {
            let old = self.asks[id].clone().expect("only live asks are filed");
            if self.ask_of.get(&old) == Some(&id) {
                self.ask_of.remove(&old);
            }
            let new: Vec<Part> = old
                .1
                .iter()
                .map(|p| match p {
                    Part::Group(g) if *g == small => Part::Group(big),
                    p => p.clone(),
                })
                .collect();
            by_sign.entry((old.0, new)).or_default().push((id, old));
        }
        let members = &self.members;
        let old_list = |g: usize| -> &[usize] {
            if g == big {
                &members[big][..kept]
            } else if g == small {
                &members[big][kept..]
            } else {
                &members[g]
            }
        };
        let asked = |old: &Sign| lookups(&old.1, |g| old_list(g).len());
        let mut total = self.lookups;
        for (new, olds) in &by_sign {
            let before: usize = olds.iter().map(|(_, old)| asked(old)).sum();
            total = total
                .saturating_sub(before)
                .saturating_add(lookups(&new.1, |g| members[g].len()));
        }
        self.lookups = total;
        if self.over() {
            return;
        }
        let mut found: Vec<(usize, Vec<Value>)> = Vec::new();
        let mut dropped: Vec<(usize, Sign)> = Vec::new();
        let mut kept_asks: Vec<(usize, Sign)> = Vec::new();
        for (new, olds) in by_sign {
            let (survivor, old) = olds
                .iter()
                .max_by_key(|(id, old)| (asked(old), std::cmp::Reverse(*id)))
                .cloned()
                .expect("a sign has an ask");
            for (id, _) in olds {
                if id != survivor {
                    dropped.push((id, new.clone()));
                }
            }
            let groups: Vec<usize> = old
                .1
                .iter()
                .filter_map(|p| match p {
                    Part::Group(g) => Some(*g),
                    Part::Value(_) => None,
                })
                .collect();
            for t in 0..groups.len() {
                let added: &[usize] = if groups[t] == big {
                    &members[big][kept..]
                } else if groups[t] == small {
                    &members[big][..kept]
                } else {
                    &[]
                };
                if added.is_empty() {
                    continue;
                }
                let lists: Vec<&[usize]> = groups
                    .iter()
                    .enumerate()
                    .map(|(u, &g)| match u.cmp(&t) {
                        std::cmp::Ordering::Less => old_list(g),
                        std::cmp::Ordering::Equal => added,
                        std::cmp::Ordering::Greater if g == small || g == big => &members[big][..],
                        std::cmp::Ordering::Greater => &members[g][..],
                    })
                    .collect();
                let mut keys = Vec::new();
                product(&new.1, &lists, &self.gids, &mut keys);
                found.extend(keys.into_iter().map(|k| (new.0, k)));
            }
            kept_asks.push((survivor, new));
        }
        self.asks_at[big] = all;
        for (id, sign) in dropped {
            self.asks[id] = None;
            for p in &sign.1 {
                if let Part::Group(g) = p {
                    self.asks_at[*g].remove(&id);
                }
            }
        }
        for (id, sign) in kept_asks {
            self.ask_of.insert(sign.clone(), id);
            self.asks[id] = Some(sign);
        }
        self.questions.extend(found);
    }
}

/// How many keys read as `parts`, the group at a position holding `size` of its gids.
fn lookups(parts: &[Part], size: impl Fn(usize) -> usize) -> usize {
    parts.iter().fold(1usize, |n, p| match p {
        Part::Group(g) => n.saturating_mul(size(*g)),
        Part::Value(_) => n,
    })
}

/// Every key that reads as `parts` with a reference from each of `lists` in order, one list per
/// group in `parts`.
fn product(parts: &[Part], lists: &[&[usize]], gids: &[Gid], out: &mut Vec<Vec<Value>>) {
    if lists.iter().any(|l| l.is_empty()) {
        return;
    }
    let mut at = vec![0; lists.len()];
    loop {
        let mut k = 0;
        out.push(
            parts
                .iter()
                .map(|p| match p {
                    Part::Value(v) => v.0.clone(),
                    Part::Group(_) => {
                        let m = lists[k][at[k]];
                        k += 1;
                        Value::Ref(gids[m])
                    }
                })
                .collect(),
        );
        let mut d = lists.len();
        loop {
            if d == 0 {
                return;
            }
            d -= 1;
            at[d] += 1;
            if at[d] < lists[d].len() {
                break;
            }
            at[d] = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change::{ALIAS, Change, Strategy};
    use crate::model::merge;
    use crate::source::Store;

    /// Where a gid is kept, read from its claims alone, is where its full fetch and the model
    /// keep it: with no claims, deleted, claimed in three collections at once or one after
    /// another, and claimed only by an alias it wrote.
    #[test]
    fn a_gid_is_kept_where_its_claims_alone_say() {
        let schema = Schema::new(vec![
            Collection::minted("c").plain("p", Strategy::Lww),
            Collection::minted("a").plain("p", Strategy::Lww),
            Collection::minted("b").plain("p", Strategy::Lww),
        ]);
        let gid = |n: u8| Gid([n; 16]);
        let at = |c: &str, g: u8, field: &str, value: Value, wall_ms: u64| Change {
            collection: c.into(),
            gid: gid(g),
            field: field.into(),
            value,
            clock: Hlc {
                wall_ms,
                counter: 0,
                node: Gid([9; 16]),
            },
        };
        let changes = vec![
            at("a", 2, DELETED, Value::Int(1), 1),
            at("c", 3, "p", Value::Int(0), 5),
            at("b", 3, "p", Value::Int(0), 5),
            at("a", 3, "p", Value::Int(0), 5),
            at("c", 4, "p", Value::Int(0), 3),
            at("a", 4, "p", Value::Int(0), 2),
            at("b", 4, "p", Value::Int(0), 1),
            at("b", 5, ALIAS, Value::Ref(gid(4)), 1),
            at("c", 5, "p", Value::Int(0), 2),
        ];
        let mut store = Store::default();
        for c in &changes {
            store.merge(&schema, c);
        }
        let model = merge(&schema, &changes);
        let ev = Evaluator::new(&schema).unwrap();
        let mut session = ev.session(&store);
        for g in 1..=5 {
            let kept = session.kept_in(gid(g));
            assert_eq!(kept, session.info(gid(g)).unwrap().home, "gid {g}");
            let modelled = model.entities.get(&gid(g)).map(|e| e.collection.as_str());
            assert_eq!(
                kept.map(|i| schema.collections[i].name.as_str()),
                modelled,
                "gid {g}"
            );
        }
    }
}

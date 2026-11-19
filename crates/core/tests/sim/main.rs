//! Several in-memory replicas, random application edits, and partial, reordered, per-device and
//! repeated delivery, over tables with NOT NULL, unique and foreign-key constraints.
//!
//! After every step the replica the step touched shows exactly `model::view` of what it has
//! merged, apart from entities whose writes its tables refused. Once everything is delivered:
//! - every replica shows `model::view` of everything. Where a column refuses NULL that the model
//!   can show, entities the tables cannot hold wait, each for a reason the tables give (see
//!   `Replica::waits_for_a_reason`); elsewhere nothing waits;
//! - no identity is left unmerged;
//! - every write an application made is in the merged state: for each entity, field, value and
//!   clock a collect found by comparing rows with images (see `Replica::writes`), some member of
//!   the entity's group holds that field at that clock with that value, or at a later clock (for
//!   a `Max` field, a value at least as great; for a natural key field, that value).
//!
//! In the other direction, each collect checks that it sends no value the application did not
//! write (`Replica::collect`).

#[path = "../common/mod.rs"]
mod common;
mod replica;

use std::collections::BTreeMap;

use common::{Family, arb_family};
use fongo_sync_core::model::{FieldKind, Identity, Row, Schema, merge, unmerged_identities, view};
use fongo_sync_core::{Change, Gid, Strategy as Merge, Value};
use proptest::prelude::*;
use replica::{Key, Replica};

/// The application's schema and the columns its tables refuse NULL in. Natural keys lead with an
/// owner and refuse NULL, as a registry must require: two rows with a NULL in the key are one
/// entity.
pub struct World {
    pub schema: Schema,
    pub not_null: Vec<(&'static str, &'static str)>,
    /// Whether a column refuses NULL where the model can show one (a reference whose target is
    /// gone): only then may anything wait once everything is delivered.
    pub may_refuse: bool,
}

impl World {
    pub fn new(family: Family) -> World {
        let family = Family {
            rooms_owned: true,
            slot_key_plain: false,
            ..family
        };
        let mut not_null = vec![
            ("sites", "name"),
            ("rooms", "site"),
            ("rooms", "label"),
            ("slots", "room"),
            ("slots", "number"),
            ("parts", "item"),
        ];
        if family.item_identity {
            not_null.push(("items", "link"));
        }
        World {
            schema: family.schema(),
            not_null,
            may_refuse: family.item_identity,
        }
    }

    pub fn replicas(&self, n: usize) -> Vec<Replica<'_>> {
        (0..n)
            .map(|i| Replica::new(&self.schema, i as u8, &self.not_null))
            .collect()
    }

    fn refuses_null(&self, collection: &str, field: &str) -> bool {
        self.not_null.contains(&(collection, field))
    }

    fn identity(&self, collection: &str, field: &str) -> bool {
        let c = self.schema.collection(collection).unwrap();
        c.identity_keys.iter().any(|k| k.iter().any(|f| f == field))
    }

    fn natural_key(&self, collection: &str, field: &str) -> bool {
        matches!(
            &self.schema.collection(collection).unwrap().identity,
            Identity::Natural(key) if key.iter().any(|k| k == field)
        )
    }
}

#[derive(Debug, Clone)]
pub enum Op {
    Insert {
        at: u8,
        collection: u8,
        picks: Vec<u8>,
    },
    Edit {
        at: u8,
        collection: u8,
        row: u8,
        field: u8,
        pick: u8,
    },
    Delete {
        at: u8,
        collection: u8,
        row: u8,
    },
    Sync {
        at: u8,
        from: Option<u8>,
        picks: Vec<u16>,
    },
    /// A site with an identity code and a room in it: two replicas doing this with the same code
    /// and label make sites that merge and rooms whose keys become one.
    Twin {
        at: u8,
        code: u8,
        label: u8,
    },
    /// An item under the first site, linked to the first site or room: two replicas doing this
    /// with the same text make items one identity merges.
    TwinItem {
        at: u8,
        text: u8,
        site: u8,
    },
    /// The application moves an item that stands for a merged entity to another site.
    MoveMerged {
        at: u8,
        row: u8,
        site: u8,
    },
    /// The application swaps two sites' identity codes through a temporary value, so that a
    /// replica receiving the result must write a swap.
    Swap {
        at: u8,
        first: u8,
        second: u8,
    },
}

/// A value the application might write: small domains, so that identity codes clash and natural
/// keys coincide; never NULL where the column refuses it if anything else is possible.
fn value(world: &World, r: &Replica, collection: &str, field: &str, pick: u8) -> Value {
    let f = world
        .schema
        .collection(collection)
        .unwrap()
        .fields
        .iter()
        .find(|f| f.name == field)
        .unwrap();
    let not_null = world.refuses_null(collection, field);
    match &f.kind {
        FieldKind::Reference(t) | FieldKind::Owner(t) => {
            let keys: Vec<Key> = r.keys(t);
            if keys.is_empty() || (pick == 0 && !not_null) {
                Value::Null
            } else {
                Value::Int(keys[usize::from(pick) % keys.len()])
            }
        }
        FieldKind::Plain
            if world.natural_key(collection, field) || world.identity(collection, field) =>
        {
            Value::Text(["a", "b"][usize::from(pick % 2)].into())
        }
        FieldKind::Plain => match pick % 6 {
            0 if !not_null => Value::Null,
            0..=2 => Value::Text(["a", "b"][usize::from(pick % 2)].into()),
            3 => Value::Real(0.5),
            _ => Value::Int(i64::from(pick % 3)),
        },
    }
}

/// Delivers `incoming` to replica `i` and appends what it sends to the log.
fn deliver(rs: &mut [Replica], log: &mut Vec<(u8, Change)>, i: usize, incoming: &[Change]) {
    let r = &mut rs[i];
    let start = r.outbox.len();
    r.sync(incoming);
    let id = r.id;
    log.extend(r.outbox[start..].iter().cloned().map(|c| (id, c)));
}

/// Runs one application operation or delivery. `log` holds every change any replica emitted.
pub fn step(world: &World, rs: &mut [Replica], log: &mut Vec<(u8, Change)>, op: &Op) {
    let schema = &world.schema;
    let n = rs.len();
    let at = |a: u8| usize::from(a) % n;
    let pick_collection = |c: u8| &schema.collections[usize::from(c) % schema.collections.len()];
    match op {
        Op::Insert {
            at: a,
            collection,
            picks,
        } => {
            let c = pick_collection(*collection);
            let mut row = Row::new();
            for (f, pick) in c.fields.iter().zip(picks.iter().cycle()) {
                row.insert(
                    f.name.clone(),
                    value(world, &rs[at(*a)], &c.name, &f.name, *pick),
                );
            }
            rs[at(*a)].insert(&c.name, row);
        }
        Op::Edit {
            at: a,
            collection,
            row,
            field,
            pick,
        } => {
            let c = pick_collection(*collection);
            let r = &mut rs[at(*a)];
            let keys = r.keys(&c.name);
            if keys.is_empty() {
                return;
            }
            let k = keys[usize::from(*row) % keys.len()];
            let f = &c.fields[usize::from(*field) % c.fields.len()].name;
            let v = value(world, r, &c.name, f, *pick);
            r.edit(&c.name, k, f, v);
        }
        Op::Delete {
            at: a,
            collection,
            row,
        } => {
            let c = pick_collection(*collection);
            let r = &mut rs[at(*a)];
            let keys = r.keys(&c.name);
            if !keys.is_empty() {
                r.delete(&c.name, keys[usize::from(*row) % keys.len()]);
            }
        }
        Op::Twin { at: a, code, label } => {
            let r = &mut rs[at(*a)];
            let ab = |n: u8| Value::Text(["a", "b"][usize::from(n % 2)].into());
            let site = Row::from([
                ("name".to_string(), ab(*code)),
                ("code".to_string(), ab(*code)),
            ]);
            if let Some(site) = r.insert("sites", site) {
                let room = Row::from([
                    ("site".to_string(), Value::Int(site)),
                    ("label".to_string(), ab(*label)),
                ]);
                r.insert("rooms", room);
            }
        }
        Op::TwinItem { at: a, text, site } => {
            let r = &mut rs[at(*a)];
            let sites = r.keys("sites");
            if sites.is_empty() {
                return;
            }
            let link_target = match &schema.collection("items").unwrap().fields[2].kind {
                FieldKind::Reference(t) => t.clone(),
                _ => unreachable!("items.link is a reference"),
            };
            // Link to the site with the same code as the text, or a room of it with that label,
            // making them if missing: replicas share them once sites merge.
            let word = Value::Text(["a", "b"][usize::from(text % 2)].into());
            let find = |r: &Replica, c: &str, f: &str, also: &dyn Fn(&Row) -> bool| {
                r.keys(c)
                    .into_iter()
                    .find(|k| r.tables[c][k].get(f) == Some(&word) && also(&r.tables[c][k]))
            };
            let coded = find(r, "sites", "code", &|_| true).or_else(|| {
                r.insert(
                    "sites",
                    Row::from([("name".into(), word.clone()), ("code".into(), word.clone())]),
                )
            });
            let link = match (link_target.as_str(), coded) {
                ("sites", Some(s)) => Some(s),
                (_, Some(s)) => find(r, "rooms", "label", &|row| {
                    row.get("site") == Some(&Value::Int(s))
                })
                .or_else(|| {
                    r.insert(
                        "rooms",
                        Row::from([
                            ("site".into(), Value::Int(s)),
                            ("label".into(), word.clone()),
                        ]),
                    )
                }),
                _ => None,
            };
            let link = link.map_or(Value::Null, Value::Int);
            let item = Row::from([
                ("text".to_string(), word.clone()),
                (
                    "site".to_string(),
                    Value::Int(sites[usize::from(*site) % sites.len()]),
                ),
                ("link".to_string(), link),
            ]);
            r.insert("items", item);
        }
        Op::MoveMerged { at: a, row, site } => {
            let r = &mut rs[at(*a)];
            let (items, sites) = (r.merged("items"), r.keys("sites"));
            if items.is_empty() || sites.is_empty() {
                return;
            }
            let item = items[usize::from(*row) % items.len()];
            let to = Value::Int(sites[usize::from(*site) % sites.len()]);
            r.edit("items", item, "site", to);
        }
        Op::Swap {
            at: a,
            first,
            second,
        } => {
            let r = &mut rs[at(*a)];
            let coded: Vec<(Key, Value)> = r
                .keys("sites")
                .into_iter()
                .filter_map(|k| {
                    let code = r.tables["sites"][&k].get("code").cloned()?;
                    (code != Value::Null).then_some((k, code))
                })
                .collect();
            if coded.len() < 2 {
                return;
            }
            let (x, cx) = coded[usize::from(*first) % coded.len()].clone();
            let (y, cy) = coded[usize::from(*second) % coded.len()].clone();
            if x == y {
                return;
            }
            let spare = Value::Text(format!("~{}", r.id));
            if r.edit("sites", x, "code", spare) && r.edit("sites", y, "code", cx) {
                r.edit("sites", x, "code", cy);
            }
        }
        Op::Sync { at: a, from, picks } => {
            let candidates: Vec<&Change> = log
                .iter()
                .filter(|(sender, _)| {
                    from.is_none_or(|f| usize::from(f) % n == usize::from(*sender))
                })
                .map(|(_, c)| c)
                .collect();
            let incoming: Vec<Change> = if candidates.is_empty() {
                Vec::new()
            } else {
                picks
                    .iter()
                    .map(|p| candidates[usize::from(*p) % candidates.len()].clone())
                    .collect()
            };
            deliver(rs, log, at(*a), &incoming);
        }
    }
}

/// Every replica reads the whole log once, then what is new, until no replica has anything new
/// to say.
pub fn quiesce(rs: &mut [Replica], log: &mut Vec<(u8, Change)>) -> Result<(), String> {
    let mut read = vec![0; rs.len()];
    for _ in 0..12 {
        let before = log.len();
        for i in 0..rs.len() {
            let unread: Vec<Change> = log[read[i]..].iter().map(|(_, c)| c.clone()).collect();
            read[i] = log.len();
            deliver(rs, log, i, &unread);
        }
        if log.len() == before {
            return Ok(());
        }
    }
    Err("replicas kept emitting changes".into())
}

/// Every write a collect found is in the merged state of `everything`.
fn writes_kept(schema: &Schema, rs: &[Replica], everything: &[Change]) -> Result<(), String> {
    let merged = merge(schema, everything);
    let mut groups: BTreeMap<Gid, Vec<Gid>> = BTreeMap::new();
    for (g, e) in &merged.entities {
        groups.entry(e.canonical).or_default().push(*g);
    }
    for r in rs {
        for w in &r.expected {
            let Some(entity) = merged.entities.get(&w.gid) else {
                return Err(format!(
                    "replica {} wrote {w:?}; the entity never arrived",
                    r.id
                ));
            };
            let coll = schema.collection(&w.collection).unwrap();
            let strategy = coll
                .fields
                .iter()
                .find(|f| f.name == w.field)
                .map_or(Merge::Lww, |f| f.strategy);
            let key = matches!(&coll.identity, Identity::Natural(k) if k.contains(&w.field));
            let same = |v: &Value| v.total_cmp(&w.value) == std::cmp::Ordering::Equal;
            let holds = |m: &Gid| {
                merged.entities.get(m).is_some_and(|e| {
                    e.registers
                        .get(&w.field)
                        .is_some_and(|(v, at)| *at > w.clock || (*at == w.clock && same(v)))
                })
            };
            if let Some(m) = w.members.iter().find(|m| !holds(m)) {
                return Err(format!(
                    "replica {} moved {w:?}; member {m:?} of the group kept its old owner",
                    r.id
                ));
            }
            let kept = groups[&entity.canonical].iter().any(|m| {
                merged.entities[m]
                    .registers
                    .get(&w.field)
                    .is_some_and(|(v, at)| {
                        if key {
                            same(v)
                        } else if strategy == Merge::Max && w.field != fongo_sync_core::DELETED {
                            v.total_cmp(&w.value) != std::cmp::Ordering::Less
                        } else {
                            *at > w.clock || (*at == w.clock && same(v))
                        }
                    })
            });
            if !kept {
                return Err(format!(
                    "replica {} wrote {w:?}; the merged state lost it",
                    r.id
                ));
            }
        }
    }
    Ok(())
}

/// Delivers everything and checks the state the replicas settle in.
pub fn finish(
    world: &World,
    rs: &mut [Replica],
    log: &mut Vec<(u8, Change)>,
) -> Result<(), String> {
    quiesce(rs, log)?;
    let everything: Vec<Change> = log.iter().map(|(_, c)| c.clone()).collect();
    let expected = view(&world.schema, &everything);
    for r in rs.iter() {
        r.check()?;
        if !world.may_refuse && !r.held.is_empty() {
            return Err(format!("replica {} still holds {:?}", r.id, r.held));
        }
        r.waits_for_a_reason()?;
        if view(&world.schema, &r.merged) != expected {
            return Err(format!("replica {} has not merged everything", r.id));
        }
    }
    let pending = unmerged_identities(&world.schema, &expected);
    if !pending.is_empty() {
        return Err(format!("identities left unmerged: {pending:?}"));
    }
    writes_kept(&world.schema, rs, &everything)
}

/// What one run exercised.
#[derive(Debug, Default)]
pub struct Exercised {
    pub refused: usize,
    pub parked: usize,
    pub held_at_some_step: usize,
    pub held_at_the_end: usize,
    pub natural_groups: usize,
    pub minted_groups: usize,
    pub merged_moves: usize,
}

pub fn run(world: &World, replicas: usize, ops: &[Op]) -> Result<Exercised, String> {
    let mut rs = world.replicas(replicas);
    let mut log: Vec<(u8, Change)> = Vec::new();
    let mut seen = Exercised::default();
    for (t, op) in ops.iter().enumerate() {
        for r in rs.iter_mut() {
            r.wall = t as u64 + 1;
        }
        step(world, &mut rs, &mut log, op);
        let touched = match op {
            Op::Insert { at, .. }
            | Op::Edit { at, .. }
            | Op::Delete { at, .. }
            | Op::Sync { at, .. }
            | Op::Twin { at, .. }
            | Op::TwinItem { at, .. }
            | Op::MoveMerged { at, .. }
            | Op::Swap { at, .. } => usize::from(*at) % rs.len(),
        };
        if rs[touched].is_clean() {
            rs[touched]
                .check()
                .map_err(|e| format!("after step {t} {op:?}: {e}"))?;
        }
        seen.held_at_some_step += rs.iter().filter(|r| !r.held.is_empty()).count();
    }
    for r in rs.iter_mut() {
        r.wall = ops.len() as u64 + 1;
    }
    finish(world, &mut rs, &mut log)?;
    seen.refused = rs.iter().map(|r| r.refused).sum();
    seen.parked = rs.iter().map(|r| r.parked_steps).sum();
    seen.held_at_the_end = rs.iter().map(|r| r.held.len()).sum();
    seen.merged_moves = rs
        .iter()
        .flat_map(|r| &r.expected)
        .filter(|w| w.members.len() > 1)
        .count();
    let everything: Vec<Change> = log.iter().map(|(_, c)| c.clone()).collect();
    let merged = merge(&world.schema, &everything);
    let mut sizes: BTreeMap<(String, Gid), usize> = BTreeMap::new();
    for e in merged.entities.values() {
        *sizes
            .entry((e.collection.clone(), e.canonical))
            .or_default() += 1;
    }
    for ((c, _), n) in sizes {
        if n > 1 {
            let natural = &world.schema.collection(&c).unwrap().identity;
            if matches!(natural, Identity::Natural(_)) {
                seen.natural_groups += 1;
            } else {
                seen.minted_groups += 1;
            }
        }
    }
    Ok(seen)
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (any::<u8>(), any::<u8>(), proptest::collection::vec(any::<u8>(), 6))
            .prop_map(|(at, collection, picks)| Op::Insert { at, collection, picks }),
        4 => (any::<u8>(), any::<u8>(), any::<u8>(), any::<u8>(), any::<u8>())
            .prop_map(|(at, collection, row, field, pick)| Op::Edit { at, collection, row, field, pick }),
        1 => (any::<u8>(), any::<u8>(), any::<u8>())
            .prop_map(|(at, collection, row)| Op::Delete { at, collection, row }),
        3 => (any::<u8>(), proptest::option::of(any::<u8>()), proptest::collection::vec(any::<u16>(), 0..60))
            .prop_map(|(at, from, picks)| Op::Sync { at, from, picks }),
        2 => (any::<u8>(), any::<u8>(), any::<u8>())
            .prop_map(|(at, code, label)| Op::Twin { at, code, label }),
        4 => (any::<u8>(), any::<u8>(), any::<u8>())
            .prop_map(|(at, text, site)| Op::TwinItem { at, text, site }),
        2 => (any::<u8>(), any::<u8>(), any::<u8>())
            .prop_map(|(at, first, second)| Op::Swap { at, first, second }),
        4 => (any::<u8>(), any::<u8>(), any::<u8>())
            .prop_map(|(at, row, site)| Op::MoveMerged { at, row, site }),
    ]
}

/// Families from the whole range, half of them with identity keys on both sites and items, so
/// that items on different replicas become one entity and are then moved.
fn arb_sim_family() -> impl Strategy<Value = Family> {
    (arb_family(), any::<bool>()).prop_map(|(family, merging)| Family {
        identity: family.identity || merging,
        item_identity: family.item_identity || merging,
        ..family
    })
}

fn cases() -> u32 {
    std::env::var("FONGO_SIM_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(400)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(cases()))]

    #[test]
    fn replicas_converge_to_the_model_and_keep_every_write(
        family in arb_sim_family(),
        replicas in 2usize..4,
        ops in proptest::collection::vec(arb_op(), 0..80),
    ) {
        let world = World::new(family);
        if let Err(e) = run(&world, replicas, &ops) {
            prop_assert!(false, "{e}");
        }
    }
}

/// Prints what the random runs exercise:
/// `cargo test --release -p fongo-sync-core --test sim -- --ignored --nocapture`
#[test]
#[ignore]
fn what_the_random_runs_cover() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::default();
    let cases = (
        arb_sim_family(),
        2usize..4,
        proptest::collection::vec(arb_op(), 0..80),
    );
    let mut total = Exercised::default();
    let mut with: BTreeMap<&str, usize> = BTreeMap::new();
    let n = 1_000;
    for _ in 0..n {
        let (family, replicas, ops) = cases.new_tree(&mut runner).unwrap().current();
        let seen = run(&World::new(family), replicas, &ops).unwrap();
        for (name, count, sum) in [
            ("refused", seen.refused, &mut total.refused),
            ("parked", seen.parked, &mut total.parked),
            (
                "held at some step",
                seen.held_at_some_step,
                &mut total.held_at_some_step,
            ),
            (
                "held at the end",
                seen.held_at_the_end,
                &mut total.held_at_the_end,
            ),
            (
                "natural groups",
                seen.natural_groups,
                &mut total.natural_groups,
            ),
            (
                "minted groups",
                seen.minted_groups,
                &mut total.minted_groups,
            ),
            (
                "moves of merged groups",
                seen.merged_moves,
                &mut total.merged_moves,
            ),
        ] {
            *sum += count;
            if count > 0 {
                *with.entry(name).or_default() += 1;
            }
        }
    }
    println!("{n} cases; totals {total:?}; cases with each: {with:?}");
}

// --- Exhaustive -----------------------------------------------------------------------------

/// One letter of the exhaustive alphabet on two replicas: sites that share an identity code,
/// rooms of the first site, deletes, re-creations, moves, renames, and two kinds of delivery
/// from the other replica.
#[derive(Debug, Clone, Copy)]
enum Letter {
    SiteA(u8),
    RoomFirst(u8),
    DeleteFirstSite(u8),
    DeleteFirstRoom(u8),
    RecodeFirstSite(u8),
    MoveFirstRoom(u8),
    RenameFirstRoom(u8),
    DeliverAll(u8),
    DeliverNewest(u8),
}

const ALPHABET: [Letter; 13] = [
    Letter::SiteA(0),
    Letter::SiteA(1),
    Letter::RoomFirst(0),
    Letter::RoomFirst(1),
    Letter::DeleteFirstSite(1),
    Letter::DeleteFirstRoom(0),
    Letter::RecodeFirstSite(0),
    Letter::MoveFirstRoom(1),
    Letter::RenameFirstRoom(0),
    Letter::DeliverAll(0),
    Letter::DeliverAll(1),
    Letter::DeliverNewest(0),
    Letter::DeliverNewest(1),
];

fn text(s: &str) -> Value {
    Value::Text(s.into())
}

fn row(pairs: &[(&str, Value)]) -> Row {
    pairs
        .iter()
        .map(|(f, v)| (f.to_string(), v.clone()))
        .collect()
}

fn spell(rs: &mut [Replica], log: &mut Vec<(u8, Change)>, letter: Letter) {
    match letter {
        Letter::SiteA(r) => {
            let r = &mut rs[usize::from(r)];
            let name = Value::Int(i64::from(r.id));
            r.insert("sites", row(&[("code", text("a")), ("name", name)]));
        }
        Letter::RoomFirst(r) => {
            let r = &mut rs[usize::from(r)];
            if let Some(&site) = r.keys("sites").first() {
                let room = row(&[
                    ("site", Value::Int(site)),
                    ("label", text("a")),
                    ("size", Value::Int(1)),
                ]);
                r.insert("rooms", room);
            }
        }
        Letter::DeleteFirstSite(r) => {
            let r = &mut rs[usize::from(r)];
            if let Some(&site) = r.keys("sites").first() {
                r.delete("sites", site);
            }
        }
        Letter::DeleteFirstRoom(r) => {
            let r = &mut rs[usize::from(r)];
            if let Some(&room) = r.keys("rooms").first() {
                r.delete("rooms", room);
            }
        }
        Letter::RecodeFirstSite(r) => {
            let r = &mut rs[usize::from(r)];
            if let Some(&site) = r.keys("sites").first() {
                r.edit("sites", site, "code", text("b"));
            }
        }
        Letter::MoveFirstRoom(r) => {
            let r = &mut rs[usize::from(r)];
            if let (Some(&room), Some(&site)) = (r.keys("rooms").first(), r.keys("sites").last()) {
                r.edit("rooms", room, "site", Value::Int(site));
            }
        }
        Letter::RenameFirstRoom(r) => {
            let r = &mut rs[usize::from(r)];
            if let Some(&room) = r.keys("rooms").first() {
                r.edit("rooms", room, "label", text("b"));
            }
        }
        Letter::DeliverAll(to) | Letter::DeliverNewest(to) => {
            let from = 1 - to;
            let mut theirs: Vec<Change> = log
                .iter()
                .filter(|(s, _)| *s == from)
                .map(|(_, c)| c.clone())
                .collect();
            if matches!(letter, Letter::DeliverNewest(_)) {
                theirs = theirs.split_off(theirs.len().saturating_sub(1));
            }
            deliver(rs, log, usize::from(to), &theirs);
        }
    }
}

fn spell_out(world: &World, script: &[Letter]) -> Result<(), String> {
    let mut rs = world.replicas(2);
    let mut log = Vec::new();
    for (t, letter) in script.iter().enumerate() {
        for r in rs.iter_mut() {
            r.wall = t as u64 + 1;
        }
        spell(&mut rs, &mut log, *letter);
        for r in &rs {
            if r.is_clean() {
                r.check().map_err(|e| format!("after {letter:?}: {e}"))?;
            }
        }
    }
    for r in rs.iter_mut() {
        r.wall = script.len() as u64 + 1;
    }
    finish(world, &mut rs, &mut log)
}

/// Every script over the alphabet up to the length in `FONGO_EXHAUSTIVE_LENGTH` (default 3), on
/// a schema with an identity key, owned natural rows and a `together` set.
#[test]
fn every_short_script_converges_and_keeps_every_write() {
    let length: usize = std::env::var("FONGO_EXHAUSTIVE_LENGTH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let world = World::new(Family {
        identity: true,
        together: true,
        rooms_owned: true,
        slot_key_plain: false,
        size_max: false,
        link_rooms: true,
        featured: true,
        item_identity: false,
        parts_together: false,
    });
    fn every(world: &World, script: &mut Vec<Letter>, left: usize, count: &mut usize) {
        *count += 1;
        if let Err(e) = spell_out(world, script) {
            panic!("{script:?}: {e}");
        }
        if left == 0 {
            return;
        }
        for letter in ALPHABET {
            script.push(letter);
            every(world, script, left - 1, count);
            script.pop();
        }
    }
    let mut count = 0usize;
    every(&world, &mut Vec::new(), length, &mut count);
    println!("{count} scripts up to length {length}");
}

// --- Delivery from one device at a time ------------------------------------------------------

/// A row moves to an owner that a replica reading one device's log has not seen. The row is not
/// shown there; an edit the application made to it beforehand sends only what it changed, and
/// everyone settles on the model.
#[test]
fn a_row_whose_new_owner_has_not_arrived_is_not_shown_and_sends_only_its_own_edits() {
    for variant in 0..4 {
        let world = World::new(Family::from_bits(0));
        let mut rs = world.replicas(3);
        let mut log: Vec<(u8, Change)> = Vec::new();
        let mut wall = 0;
        let mut tick = |rs: &mut [Replica]| {
            wall += 1;
            for r in rs.iter_mut() {
                r.wall = wall;
            }
        };
        let from = |log: &[(u8, Change)], who: u8| -> Vec<Change> {
            log.iter()
                .filter(|(s, _)| *s == who)
                .map(|(_, c)| c.clone())
                .collect()
        };
        tick(&mut rs);
        let s0 = rs[0].insert("sites", row(&[("name", text("S"))])).unwrap();
        let x0 = rs[0]
            .insert(
                "items",
                row(&[
                    ("text", text("old")),
                    ("site", Value::Int(s0)),
                    ("link", Value::Null),
                ]),
            )
            .unwrap();
        deliver(&mut rs, &mut log, 0, &[]);
        tick(&mut rs);
        let theirs = from(&log, 0);
        deliver(&mut rs, &mut log, 1, &theirs);
        tick(&mut rs);
        rs[2].insert("sites", row(&[("name", text("T"))])).unwrap();
        deliver(&mut rs, &mut log, 2, &[]);
        tick(&mut rs);
        let theirs = from(&log, 2);
        deliver(&mut rs, &mut log, 0, &theirs);
        let t0 = *rs[0]
            .keys("sites")
            .iter()
            .find(|k| rs[0].tables["sites"][k]["name"] == text("T"))
            .unwrap();
        tick(&mut rs);
        assert!(rs[0].edit("items", x0, "site", Value::Int(t0)));
        assert!(rs[0].edit("items", x0, "text", text("new")));
        deliver(&mut rs, &mut log, 0, &[]);
        tick(&mut rs);
        let x1 = rs[1].keys("items")[0];
        let s1 = rs[1].keys("sites")[0];
        match variant {
            1 => assert!(rs[1].edit("items", x1, "link", Value::Int(s1))),
            2 => assert!(rs[1].delete("items", x1)),
            3 => assert!(rs[1].edit("items", x1, "text", text("theirs"))),
            _ => {}
        }
        let theirs = from(&log, 0);
        deliver(&mut rs, &mut log, 1, &theirs);
        rs[1].check().unwrap();
        assert!(rs[1].keys("items").is_empty(), "variant {variant}");
        let sent: Vec<&Change> = log
            .iter()
            .filter(|(s, c)| *s == 1 && c.collection == "items")
            .map(|(_, c)| c)
            .collect();
        let fields: Vec<&str> = sent.iter().map(|c| c.field.as_str()).collect();
        let wanted: &[&str] = match variant {
            1 => &["link"],
            2 => &["$deleted"],
            3 => &["text"],
            _ => &[],
        };
        assert_eq!(fields, wanted, "variant {variant}");
        tick(&mut rs);
        finish(&world, &mut rs, &mut log).unwrap();
    }
}

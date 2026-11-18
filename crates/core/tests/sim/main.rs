//! Several in-memory replicas, random application edits, and partial, reordered, per-device and
//! repeated delivery. After every step each replica's rows equal `model::view` of what it has
//! merged (apart from rows its tables refused, which wait); once everything is delivered they
//! are equal outright, no identity is left unmerged, and nothing waits.

#[path = "../common/mod.rs"]
mod common;
mod replica;

use std::collections::BTreeMap;

use common::{Family, arb_family};
use fongo_sync_core::model::{FieldKind, Row, Schema, unmerged_identities, view};
use fongo_sync_core::{Change, Value};
use proptest::prelude::*;
use replica::{Key, Replica};

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
}

fn value(r: &Replica, schema: &Schema, collection: &str, field: &str, pick: u8) -> Value {
    let f = schema
        .collection(collection)
        .unwrap()
        .fields
        .iter()
        .find(|f| f.name == field)
        .unwrap();
    match &f.kind {
        FieldKind::Reference(t) | FieldKind::Owner(t) => {
            let keys: Vec<Key> = r.keys(t);
            if pick == 0 || keys.is_empty() {
                Value::Null
            } else {
                Value::Int(keys[usize::from(pick) % keys.len()])
            }
        }
        FieldKind::Plain => match pick % 6 {
            0 => Value::Null,
            1 | 2 => Value::Text(["a", "b"][usize::from(pick % 2)].into()),
            3 => Value::Real(0.5),
            _ => Value::Int(i64::from(pick % 3)),
        },
    }
}

/// Runs one application operation or delivery. `log` holds every change any replica emitted.
pub fn step(schema: &Schema, replicas: &mut [Replica], log: &mut Vec<(u8, Change)>, op: &Op) {
    let n = replicas.len();
    let at = |a: u8| usize::from(a) % n;
    match op {
        Op::Insert {
            at: a,
            collection,
            picks,
        } => {
            let r = &mut replicas[at(*a)];
            let c = &schema.collections[usize::from(*collection) % schema.collections.len()];
            let mut row = Row::new();
            for (f, pick) in c.fields.iter().zip(picks.iter().cycle()) {
                row.insert(f.name.clone(), value(r, schema, &c.name, &f.name, *pick));
            }
            r.insert(&c.name, row);
        }
        Op::Edit {
            at: a,
            collection,
            row,
            field,
            pick,
        } => {
            let r = &mut replicas[at(*a)];
            let c = &schema.collections[usize::from(*collection) % schema.collections.len()];
            let keys = r.keys(&c.name);
            if keys.is_empty() {
                return;
            }
            let k = keys[usize::from(*row) % keys.len()];
            let f = &c.fields[usize::from(*field) % c.fields.len()].name;
            let v = value(r, schema, &c.name, f, *pick);
            r.edit(&c.name, k, f, v);
        }
        Op::Delete {
            at: a,
            collection,
            row,
        } => {
            let r = &mut replicas[at(*a)];
            let c = &schema.collections[usize::from(*collection) % schema.collections.len()];
            let keys = r.keys(&c.name);
            if !keys.is_empty() {
                r.delete(&c.name, keys[usize::from(*row) % keys.len()]);
            }
        }
        Op::Sync { at: a, from, picks } => {
            let i = at(*a);
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
            let r = &mut replicas[i];
            let before = r.outbox.len();
            r.sync(&incoming);
            let id = r.id;
            log.extend(r.outbox[before..].iter().cloned().map(|c| (id, c)));
        }
    }
}

/// Every replica reads the whole log once, then what is new, until no replica has anything new
/// to say.
pub fn quiesce(replicas: &mut [Replica], log: &mut Vec<(u8, Change)>) -> Result<(), String> {
    let mut read = vec![0; replicas.len()];
    for _ in 0..12 {
        let before = log.len();
        for (i, r) in replicas.iter_mut().enumerate() {
            let unread: Vec<Change> = log[read[i]..].iter().map(|(_, c)| c.clone()).collect();
            read[i] = log.len();
            let start = r.outbox.len();
            r.sync(&unread);
            let id = r.id;
            log.extend(r.outbox[start..].iter().cloned().map(|c| (id, c)));
        }
        if log.len() == before {
            return Ok(());
        }
    }
    Err("replicas kept emitting changes".into())
}

pub fn run(schema: &Schema, replicas: usize, ops: &[Op]) -> Result<(), String> {
    let mut rs: Vec<Replica> = (0..replicas)
        .map(|i| Replica::new(schema, i as u8))
        .collect();
    let mut log: Vec<(u8, Change)> = Vec::new();
    for (t, op) in ops.iter().enumerate() {
        for r in rs.iter_mut() {
            r.wall = t as u64 + 1;
        }
        step(schema, &mut rs, &mut log, op);
        let touched = match op {
            Op::Insert { at, .. }
            | Op::Edit { at, .. }
            | Op::Delete { at, .. }
            | Op::Sync { at, .. } => usize::from(*at) % rs.len(),
        };
        if rs[touched].is_clean() {
            rs[touched]
                .check()
                .map_err(|e| format!("after step {t} {op:?}: {e}"))?;
        }
    }
    for r in rs.iter_mut() {
        r.wall = ops.len() as u64 + 1;
    }
    quiesce(&mut rs, &mut log)?;
    let everything: Vec<Change> = log.iter().map(|(_, c)| c.clone()).collect();
    let expected = view(schema, &everything);
    for r in &rs {
        r.check()?;
        if !r.held.is_empty() {
            return Err(format!("replica {} still holds {:?}", r.id, r.held));
        }
        if view(schema, &r.merged) != expected {
            return Err(format!("replica {} has not merged everything", r.id));
        }
    }
    let pending = unmerged_identities(schema, &expected);
    if !pending.is_empty() {
        return Err(format!("identities left unmerged: {pending:?}"));
    }
    let first = rs[0].shown();
    for r in &rs[1..] {
        if r.shown().len() != first.len() {
            return Err(format!("replica {} shows a different number of rows", r.id));
        }
    }
    Ok(())
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
    ]
}

/// The family without plain key fields that hold references, which an application writing
/// through local keys cannot produce.
fn arb_sim_family() -> impl Strategy<Value = Family> {
    arb_family().prop_map(|f| Family {
        slot_key_plain: false,
        ..f
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
    fn replicas_converge_to_the_model(
        family in arb_sim_family(),
        replicas in 2usize..4,
        ops in proptest::collection::vec(arb_op(), 0..80),
    ) {
        let schema = family.schema();
        if let Err(e) = run(&schema, replicas, &ops) {
            prop_assert!(false, "{e}");
        }
    }
}

/// Prints what the random runs exercise. `cargo test --release --test sim -- --ignored --nocapture`
#[test]
#[ignore]
fn what_the_random_runs_cover() {
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    let mut runner = TestRunner::default();
    let strat = (
        arb_sim_family(),
        2usize..4,
        proptest::collection::vec(arb_op(), 0..80),
    );
    let mut totals: BTreeMap<&str, usize> = BTreeMap::new();
    for _ in 0..400 {
        let (family, n, ops) = strat.new_tree(&mut runner).unwrap().current();
        let schema = family.schema();
        let mut rs: Vec<Replica> = (0..n).map(|i| Replica::new(&schema, i as u8)).collect();
        let mut log = Vec::new();
        let mut held = 0usize;
        for (t, op) in ops.iter().enumerate() {
            for r in rs.iter_mut() {
                r.wall = t as u64 + 1;
            }
            step(&schema, &mut rs, &mut log, op);
            held += rs.iter().filter(|r| !r.held.is_empty()).count();
        }
        quiesce(&mut rs, &mut log).unwrap();
        let all: Vec<Change> = log.iter().map(|(_, c)| c.clone()).collect();
        let v = view(&schema, &all);
        *totals.entry("cases").or_default() += 1;
        *totals.entry("changes").or_default() += all.len();
        *totals.entry("aliases").or_default() += all.iter().filter(|c| c.field == "$alias").count();
        *totals.entry("tombstones").or_default() += all
            .iter()
            .filter(|c| c.field == "$deleted" && c.value == Value::Int(1))
            .count();
        *totals.entry("held-steps").or_default() += held;
        *totals.entry("parked").or_default() += rs.iter().map(|r| r.parked_steps).sum::<usize>();
        *totals.entry("refused").or_default() += rs.iter().map(|r| r.refused).sum::<usize>();
        for c in &schema.collections {
            *totals
                .entry(Box::leak(format!("rows-{}", c.name).into_boxed_str()))
                .or_default() += v.rows(&c.name).count();
        }
    }
    println!("{totals:?}");
}

/// One letter of the exhaustive alphabet: two replicas, a site identity code, a room per site,
/// deletes, moves, renames and two kinds of delivery.
#[derive(Debug, Clone, Copy)]
enum Letter {
    SiteA(u8),
    RoomFirst(u8),
    DeleteFirstSite(u8),
    RecodeFirstSite(u8),
    MoveFirstRoom(u8),
    RenameFirstRoom(u8),
    DeliverAll(u8),
    DeliverNewest(u8),
}

const ALPHABET: [Letter; 12] = [
    Letter::SiteA(0),
    Letter::SiteA(1),
    Letter::RoomFirst(0),
    Letter::RoomFirst(1),
    Letter::DeleteFirstSite(1),
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

fn spell(schema: &Schema, rs: &mut [Replica], log: &mut Vec<(u8, Change)>, letter: Letter) {
    let row = |pairs: &[(&str, Value)]| -> Row {
        pairs
            .iter()
            .map(|(f, v)| (f.to_string(), v.clone()))
            .collect()
    };
    match letter {
        Letter::SiteA(r) => {
            let r = &mut rs[usize::from(r)];
            r.insert(
                "sites",
                row(&[("code", text("a")), ("name", Value::Int(i64::from(r.id)))]),
            );
        }
        Letter::RoomFirst(r) => {
            let r = &mut rs[usize::from(r)];
            if let Some(&site) = r.keys("sites").first() {
                r.insert(
                    "rooms",
                    row(&[("site", Value::Int(site)), ("label", text("a"))]),
                );
            }
        }
        Letter::DeleteFirstSite(r) => {
            let r = &mut rs[usize::from(r)];
            if let Some(&site) = r.keys("sites").first() {
                r.delete("sites", site);
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
            let _ = schema;
            let r = &mut rs[usize::from(to)];
            let start = r.outbox.len();
            r.sync(&theirs);
            let id = r.id;
            log.extend(r.outbox[start..].iter().cloned().map(|c| (id, c)));
        }
    }
}

fn spell_out(schema: &Schema, script: &[Letter]) -> Result<(), String> {
    let mut rs: Vec<Replica> = (0..2).map(|i| Replica::new(schema, i as u8)).collect();
    let mut log = Vec::new();
    for (t, letter) in script.iter().enumerate() {
        for r in rs.iter_mut() {
            r.wall = t as u64 + 1;
        }
        spell(schema, &mut rs, &mut log, *letter);
        for r in &rs {
            if r.is_clean() {
                r.check().map_err(|e| format!("after {letter:?}: {e}"))?;
            }
        }
    }
    for r in rs.iter_mut() {
        r.wall = script.len() as u64 + 1;
    }
    quiesce(&mut rs, &mut log)?;
    let everything: Vec<Change> = log.iter().map(|(_, c)| c.clone()).collect();
    let expected = view(schema, &everything);
    for r in &rs {
        r.check()?;
        if !r.held.is_empty() {
            return Err(format!("replica {} still holds {:?}", r.id, r.held));
        }
        if view(schema, &r.merged) != expected {
            return Err(format!("replica {} has not merged everything", r.id));
        }
    }
    if !unmerged_identities(schema, &expected).is_empty() {
        return Err("identities left unmerged".into());
    }
    Ok(())
}

/// Every script over the alphabet up to the length in `FONGO_EXHAUSTIVE_LENGTH` (default 3), on
/// a schema with an identity key, owned natural rows and a `together` set.
#[test]
fn every_short_script_converges() {
    let length: usize = std::env::var("FONGO_EXHAUSTIVE_LENGTH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);
    let schema = Family {
        identity: true,
        together: true,
        rooms_owned: true,
        slot_key_plain: false,
        size_max: false,
        link_rooms: true,
        featured: true,
        item_identity: false,
        parts_together: false,
    }
    .schema();
    let mut script = Vec::new();
    let mut count = 0usize;
    fn every(schema: &Schema, script: &mut Vec<Letter>, left: usize, count: &mut usize) {
        *count += 1;
        if let Err(e) = spell_out(schema, script) {
            panic!("{script:?}: {e}");
        }
        if left == 0 {
            return;
        }
        for letter in ALPHABET {
            script.push(letter);
            every(schema, script, left - 1, count);
            script.pop();
        }
    }
    every(&schema, &mut script, length, &mut count);
    println!("{count} scripts up to length {length}");
}

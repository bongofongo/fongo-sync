//! Schemas from a small family, and random change sets over them: honest writes, natural keys
//! that arrive with their parents' keys, and hostile changes (key rewrites, wrong collections,
//! unencodable clocks, aliases to anything).

#![allow(dead_code)]

use std::collections::BTreeSet;

use fongo_sync_core::model::{ALIAS, Collection, FieldKind, Identity, Schema};
use fongo_sync_core::{Change, DELETED, Gid, Hlc, Strategy as Merge, Value};
use proptest::prelude::*;

/// Which variant of each choice the family makes.
#[derive(Debug, Clone, Copy)]
pub struct Family {
    pub identity: bool,
    pub together: bool,
    pub rooms_owned: bool,
    pub slot_key_plain: bool,
    pub size_max: bool,
    pub link_rooms: bool,
    pub featured: bool,
    pub item_identity: bool,
    pub parts_together: bool,
}

impl Family {
    pub fn from_bits(bits: u16) -> Family {
        let bit = |n: u16| bits & (1 << n) != 0;
        Family {
            identity: bit(0),
            together: bit(1),
            rooms_owned: bit(2),
            slot_key_plain: bit(3),
            size_max: bit(4),
            link_rooms: bit(5),
            featured: bit(6),
            item_identity: bit(7),
            parts_together: bit(8),
        }
    }

    pub fn schema(self) -> Schema {
        let mut sites = Collection::minted("sites")
            .plain("name", Merge::Lww)
            .plain("score", Merge::Max)
            .plain("code", Merge::Lww)
            .plain("x", Merge::Lww)
            .plain("y", Merge::Lww);
        if self.identity {
            sites = sites.identity_key(&["code"]);
        }
        if self.together {
            sites = sites.together(&["x", "y"]);
        }
        if self.featured {
            sites = sites.reference("featured", "items");
        }
        let rooms = Collection::natural("rooms", &["site", "label"]);
        let rooms = if self.rooms_owned {
            rooms.owner("site", "sites")
        } else {
            rooms.reference("site", "sites")
        }
        .plain("label", Merge::Lww)
        .plain(
            "size",
            if self.size_max {
                Merge::Max
            } else {
                Merge::Lww
            },
        );
        let slots = Collection::natural("slots", &["room", "number"]);
        let slots = if self.slot_key_plain {
            slots.plain("room", Merge::Lww)
        } else {
            slots.owner("room", "rooms")
        }
        .plain("number", Merge::Lww);
        let mut items = Collection::minted("items")
            .plain("text", Merge::Lww)
            .owner("site", "sites")
            .reference("link", if self.link_rooms { "rooms" } else { "sites" });
        if self.item_identity {
            items = items.identity_key(&["text", "link"]);
        }
        let mut parts = Collection::minted("parts")
            .plain("text", Merge::Lww)
            .plain("note", Merge::Lww)
            .owner("item", "items");
        if self.parts_together {
            parts = parts.together(&["text", "note"]);
        }
        let schema = Schema::new(vec![sites, rooms, slots, items, parts]);
        schema.check().unwrap();
        schema
    }
}

pub fn arb_family() -> impl Strategy<Value = Family> {
    (0u16..512).prop_map(Family::from_bits)
}

pub fn gid(n: u8) -> Gid {
    Gid([n; 16])
}

pub const LABELS: [&str; 2] = ["a", "b"];

/// The `g`th minted gid of collection `c` (by index in the schema). Gid 1 is also sites' first.
pub fn minted(c: usize, g: u8) -> Gid {
    gid(c as u8 * 10 + 1 + g)
}

/// The key and gid of a natural entity of `collection`, picked by `g` and `pick`.
pub fn natural(schema: &Schema, collection: &str, g: u8, pick: u8) -> (Vec<Value>, Gid) {
    let c = schema.collection(collection).unwrap();
    let Identity::Natural(key_fields) = &c.identity else {
        panic!("{collection} is not natural");
    };
    let key: Vec<Value> = key_fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let kind = &c.fields.iter().find(|d| &d.name == f).unwrap().kind;
            match kind {
                FieldKind::Owner(t) | FieldKind::Reference(t) => {
                    Value::Ref(entity(schema, t, g, pick))
                }
                FieldKind::Plain if i == 0 => {
                    // A plain key field that holds a reference, as some applications key by one.
                    Value::Ref(natural(schema, "rooms", g, pick).1)
                }
                FieldKind::Plain if collection == "rooms" => {
                    Value::Text(LABELS[usize::from(pick % 2)].into())
                }
                FieldKind::Plain => Value::Int(i64::from(pick / 2)),
            }
        })
        .collect();
    let gid = Gid::derive(collection, &key);
    (key, gid)
}

pub fn index(schema: &Schema, collection: &str) -> usize {
    schema
        .collections
        .iter()
        .position(|c| c.name == collection)
        .unwrap()
}

/// The gid of entity `g` of `collection`, natural or minted.
pub fn entity(schema: &Schema, collection: &str, g: u8, pick: u8) -> Gid {
    let c = schema.collection(collection).unwrap();
    match c.identity {
        Identity::Natural(_) => natural(schema, collection, g % 2, pick).1,
        Identity::Minted => minted(index(schema, collection), g % 4),
    }
}

/// Every gid the generator may name.
pub fn pool(schema: &Schema) -> Vec<Gid> {
    let mut all = BTreeSet::new();
    for c in &schema.collections {
        for g in 0..4 {
            for pick in 0..4 {
                all.insert(entity(schema, &c.name, g, pick));
            }
        }
    }
    all.insert(gid(99));
    all.into_iter().collect()
}

fn arb_value(pool: Vec<Gid>) -> impl Strategy<Value = Value> {
    prop_oneof![
        1 => Just(Value::Null),
        2 => (0i64..3).prop_map(Value::Int),
        1 => Just(Value::Real(1.0)),
        1 => "[ab]".prop_map(Value::Text),
        4 => prop::sample::select(pool).prop_map(Value::Ref),
    ]
}

/// One raw step: which collection, entity, field, value, clock, oddity and key choice.
pub type Raw = (usize, u8, usize, Value, u64, u8, u8);

pub fn arb_raw(
    schema: &Schema,
    len: std::ops::Range<usize>,
) -> impl Strategy<Value = Vec<Raw>> + use<> {
    let n = schema.collections.len();
    let one = (
        0..n + 1,
        0u8..4,
        0usize..9,
        arb_value(pool(schema)),
        0u64..4,
        0u8..20,
        0u8..4,
    );
    proptest::collection::vec(one, len)
}

/// Turns raw steps into changes. Most changes to a natural entity arrive with the writes of its
/// key (and its parents' keys); some key writes are hostile. `node` and `counter` give every
/// change a distinct clock.
pub fn changes(schema: &Schema, raw: &[Raw]) -> Vec<Change> {
    let mut out: Vec<Change> = Vec::new();
    for (c, g, f, value, wall_ms, odd, pick) in raw.iter().cloned() {
        let (collection, fields): (String, Vec<String>) = match schema.collections.get(c) {
            Some(coll) => (
                coll.name.clone(),
                coll.fields.iter().map(|f| f.name.clone()).collect(),
            ),
            None => ("unknown".into(), vec!["name".into()]),
        };
        let stamp =
            |out: &mut Vec<Change>, collection: &str, gid: Gid, field: &str, value: Value| {
                let counter = out.len() as u32;
                out.push(Change {
                    collection: collection.into(),
                    gid,
                    field: field.into(),
                    value,
                    clock: Hlc {
                        wall_ms: if odd == 19 { u64::MAX } else { wall_ms },
                        counter,
                        node: Gid([odd % 3; 16]),
                    },
                });
            };
        let natural_coll = schema
            .collection(&collection)
            .is_some_and(|c| matches!(c.identity, Identity::Natural(_)));
        let (key, target) = if natural_coll {
            natural(schema, &collection, g % 2, pick)
        } else if odd == 1 {
            (Vec::new(), gid(1))
        } else {
            (Vec::new(), minted(c, g))
        };
        if natural_coll && odd < 15 {
            write_key(
                schema,
                &collection,
                g % 2,
                pick,
                &mut |coll, gid, field, value| stamp(&mut out, coll, gid, field, value),
            );
        }
        let key_fields: Vec<String> = match schema.collection(&collection).map(|c| &c.identity) {
            Some(Identity::Natural(k)) => k.clone(),
            _ => Vec::new(),
        };
        let (field, value) = match f {
            6 if odd == 3 => (DELETED.to_string(), value),
            6 => (DELETED.to_string(), Value::Int(i64::from(odd % 2))),
            7 => (ALIAS.to_string(), value),
            8 => (
                ALIAS.to_string(),
                Value::Ref(if natural_coll {
                    natural(schema, &collection, (g + 1) % 2, pick).1
                } else {
                    minted(c, g + 1)
                }),
            ),
            _ if odd == 0 => ("colour".to_string(), value),
            _ => {
                let k = f % fields.len();
                let name = fields[k].clone();
                let at = key_fields.iter().position(|kf| *kf == name);
                match at {
                    Some(i) if odd != 2 => (name, key[i].clone()),
                    _ => (name, value),
                }
            }
        };
        stamp(&mut out, &collection, target, &field, value);
    }
    out
}

fn write_key(
    schema: &Schema,
    collection: &str,
    g: u8,
    pick: u8,
    stamp: &mut dyn FnMut(&str, Gid, &str, Value),
) {
    let c = schema.collection(collection).unwrap();
    let Identity::Natural(key_fields) = &c.identity else {
        return;
    };
    let (key, gid) = natural(schema, collection, g, pick);
    for (f, v) in key_fields.iter().zip(&key) {
        let kind = &c.fields.iter().find(|d| &d.name == f).unwrap().kind;
        let parent = match kind {
            FieldKind::Owner(t) | FieldKind::Reference(t) => Some(t.clone()),
            FieldKind::Plain if matches!(v, Value::Ref(_)) => Some("rooms".to_string()),
            FieldKind::Plain => None,
        };
        if let Some(t) = parent
            && matches!(
                schema.collection(&t).map(|p| &p.identity),
                Some(Identity::Natural(_))
            )
        {
            write_key(schema, &t, g, pick, stamp);
        }
        stamp(collection, gid, f, v.clone());
    }
}

/// A schema from the family and changes over it.
pub fn arb_case(len: std::ops::Range<usize>) -> impl Strategy<Value = (Schema, Vec<Change>)> {
    arb_family().prop_flat_map(move |family| {
        let schema = family.schema();
        arb_raw(&schema, len.clone()).prop_map(move |raw| {
            let changes = changes(&schema, &raw);
            (schema.clone(), changes)
        })
    })
}

/// Every gid a change names, as its subject or in its value, and the generator's pool when the
/// schema is from the family.
pub fn mentioned(schema: &Schema, changes: &[Change]) -> BTreeSet<Gid> {
    let mut all = BTreeSet::new();
    if schema.collection("rooms").is_some() {
        all.extend(pool(schema));
    }
    for c in changes {
        all.insert(c.gid);
        if let Value::Ref(g) = c.value {
            all.insert(g);
        }
    }
    all
}

/// A small deterministic generator, for cases built from a seed.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    pub fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }

    pub fn one_in(&mut self, n: u64) -> bool {
        self.next().is_multiple_of(n)
    }

    pub fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

/// A schema of two to four collections in a random order: natural keys that lead with a
/// reference, identity keys of one or two fields, `together` sets of plain `Lww` fields, `Max`
/// fields, references in any direction and owners towards earlier collections.
pub fn random_schema(rng: &mut Rng) -> Schema {
    use fongo_sync_core::model::Field;
    loop {
        let mut names = ["b", "a", "d", "c"];
        for i in (1..names.len()).rev() {
            names.swap(i, rng.below(i + 1));
        }
        let n = 2 + rng.below(3);
        let names = &names[..n];
        let mut collections = Vec::new();
        for (i, name) in names.iter().enumerate() {
            let mut fields = Vec::new();
            for p in 0..1 + rng.below(3) {
                let strategy = if rng.one_in(4) {
                    Merge::Max
                } else {
                    Merge::Lww
                };
                fields.push(Field {
                    name: format!("p{p}"),
                    kind: FieldKind::Plain,
                    strategy,
                });
            }
            for r in 0..rng.below(3) {
                let target = names[rng.below(n)].to_string();
                fields.push(Field {
                    name: format!("r{r}"),
                    kind: FieldKind::Reference(target),
                    strategy: Merge::Lww,
                });
            }
            if i > 0 && !rng.one_in(3) {
                let target = names[rng.below(i)].to_string();
                fields.push(Field {
                    name: "o".into(),
                    kind: FieldKind::Owner(target),
                    strategy: Merge::Lww,
                });
            }
            let mut c = Collection {
                name: name.to_string(),
                identity: Identity::Minted,
                fields,
                identity_keys: Vec::new(),
                together: Vec::new(),
            };
            let names_of = |pred: &dyn Fn(&fongo_sync_core::model::Field) -> bool| -> Vec<String> {
                c.fields
                    .iter()
                    .filter(|f| pred(f))
                    .map(|f| f.name.clone())
                    .collect()
            };
            let all = names_of(&|_| true);
            let refs = names_of(&|f| f.kind != FieldKind::Plain);
            let plain_lww = names_of(&|f| f.kind == FieldKind::Plain && f.strategy == Merge::Lww);
            if !rng.one_in(2) {
                let mut key = Vec::new();
                if !refs.is_empty() && !rng.one_in(4) {
                    key.push(rng.pick(&refs).clone());
                }
                let want = 1 + rng.below(2);
                for _ in 0..4 {
                    let f = rng.pick(&all).clone();
                    if key.len() < want && !key.contains(&f) {
                        key.push(f);
                    }
                }
                c.identity = Identity::Natural(key);
            } else {
                for _ in 0..rng.below(3) {
                    let mut key = vec![rng.pick(&all).clone()];
                    let f = rng.pick(&all).clone();
                    if rng.one_in(2) && !key.contains(&f) {
                        key.push(f);
                    }
                    c.identity_keys.push(key);
                }
            }
            let mut used: Vec<String> = Vec::new();
            for _ in 0..rng.below(3) {
                let mut set = Vec::new();
                for _ in 0..1 + rng.below(3) {
                    if plain_lww.is_empty() {
                        break;
                    }
                    let f = rng.pick(&plain_lww).clone();
                    if !used.contains(&f) {
                        used.push(f.clone());
                        set.push(f);
                    }
                }
                if !set.is_empty() {
                    c.together.push(set);
                }
            }
            collections.push(c);
        }
        let schema = Schema::new(collections);
        if schema.check().is_ok() {
            return schema;
        }
    }
}

/// Gids to write to: four plain ones and the gids natural keys over small domains derive.
/// A natural entity the pool holds: its collection, gid and key.
pub type Keyed = (String, Gid, Vec<Value>);

pub fn random_pool(rng: &mut Rng, schema: &Schema) -> (Vec<Gid>, Vec<Keyed>) {
    let mut pool: Vec<Gid> = (1..=4).map(gid).collect();
    let mut keyed = Vec::new();
    for _ in 0..3 {
        for c in &schema.collections {
            let Identity::Natural(key) = &c.identity else {
                continue;
            };
            for _ in 0..3 {
                let values: Vec<Value> = key
                    .iter()
                    .map(
                        |f| match &c.fields.iter().find(|d| &d.name == f).unwrap().kind {
                            FieldKind::Plain if !rng.one_in(3) => Value::Int(rng.below(2) as i64),
                            _ if rng.one_in(12) => Value::Null,
                            _ => Value::Ref(*rng.pick(&pool)),
                        },
                    )
                    .collect();
                let g = Gid::derive(&c.name, &values);
                if !pool.contains(&g) {
                    pool.push(g);
                    keyed.push((c.name.clone(), g, values));
                }
            }
        }
    }
    (pool, keyed)
}

/// `n` changes over a random schema: honest key writes with most changes to natural entities,
/// and hostile ones: tombstones that are not 0 or 1, aliases to themselves or to non-gids,
/// undeclared fields and collections, unencodable clocks, repeats.
pub fn random_changes(rng: &mut Rng, schema: &Schema, n: usize) -> Vec<Change> {
    let (pool, keyed) = random_pool(rng, schema);
    let mut out: Vec<Change> = Vec::new();
    let value = |rng: &mut Rng| match rng.below(10) {
        0 => Value::Null,
        1 | 2 => Value::Int(rng.below(3) as i64),
        3 => Value::Real(0.5),
        4 => Value::Text("a".into()),
        _ => Value::Ref(*rng.pick(&pool)),
    };
    let mut counter = 0u32;
    let mut clock = |rng: &mut Rng| {
        counter += 1;
        Hlc {
            wall_ms: if rng.one_in(50) {
                u64::MAX
            } else {
                rng.below(5) as u64
            },
            counter,
            node: gid(200 + rng.below(3) as u8),
        }
    };
    for _ in 0..n {
        let roll = rng.below(100);
        if roll < 3 {
            out.push(Change {
                collection: "zz".into(),
                gid: *rng.pick(&pool),
                field: "p0".into(),
                value: Value::Int(1),
                clock: clock(rng),
            });
            continue;
        }
        if roll < 6 && !out.is_empty() {
            let again = out[rng.below(out.len())].clone();
            out.push(again);
            continue;
        }
        let c = rng.pick(&schema.collections);
        let mut g = *rng.pick(&pool);
        if let Identity::Natural(key) = &c.identity {
            let mine: Vec<&Keyed> = keyed.iter().filter(|(k, _, _)| *k == c.name).collect();
            if !mine.is_empty() && !rng.one_in(4) {
                let (_, kg, values) = *rng.pick(&mine);
                g = *kg;
                if !rng.one_in(3) {
                    for (f, v) in key.iter().zip(values) {
                        out.push(Change {
                            collection: c.name.clone(),
                            gid: g,
                            field: f.clone(),
                            value: v.clone(),
                            clock: clock(rng),
                        });
                    }
                }
            }
        }
        let roll = rng.below(100);
        let (field, v) = if roll < 12 {
            let v = match rng.below(6) {
                0 => Value::Int(0),
                1 => Value::Null,
                2 => Value::Int(2),
                _ => Value::Int(1),
            };
            (DELETED.to_string(), v)
        } else if roll < 27 {
            let v = match rng.below(10) {
                0 => Value::Int(1),
                1 => Value::Ref(g),
                _ => Value::Ref(*rng.pick(&pool)),
            };
            (ALIAS.to_string(), v)
        } else if roll < 30 {
            ("zz".to_string(), value(rng))
        } else {
            let f = rng.pick(&c.fields);
            let v = if f.kind != FieldKind::Plain && !rng.one_in(3) {
                if rng.one_in(10) {
                    Value::Null
                } else {
                    Value::Ref(*rng.pick(&pool))
                }
            } else {
                value(rng)
            };
            (f.name.clone(), v)
        };
        out.push(Change {
            collection: c.name.clone(),
            gid: g,
            field,
            value: v,
            clock: clock(rng),
        });
    }
    out
}

/// A random schema and changes over it, from a seed.
pub fn arb_random_case(len: usize) -> impl Strategy<Value = (Schema, Vec<Change>)> {
    any::<u64>().prop_map(move |seed| {
        let mut rng = Rng(seed | 1);
        let schema = random_schema(&mut rng);
        let n = rng.below(len + 1);
        let changes = random_changes(&mut rng, &schema, n);
        (schema, changes)
    })
}

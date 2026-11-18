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

//! Compares a replica with the reference model, `fongo_sync_core::model`, of the changes it has
//! taken in: those applied to it and those it put in its outbox.
//!
//! Two strengths. Once nothing is waiting anywhere, [`settled`] requires the rows to be exactly
//! the model's view. After any apply or collect, [`midway`] requires what must hold even while
//! the kernel holds rows back: its registers and alias groups are the model's, its own tables
//! are consistent, and every row it is not holding back is the model's row.
//!
//! A row is held back when its entity waits in `fongo_pending`, when it is a row a merge gave up
//! (`fongo_leftover`), or when it holds local writes not yet collected (`fongo_dirty`), which
//! holds back its entity too. The model
//! has no notion of waiting, so those rows are compared only once they are settled. Fields of a
//! set that changes together are also left out midway while the set's newest write has not all
//! arrived, since the kernel keeps what the row held until then; so are groups with a live member
//! whose tombstone has not arrived (see [`unconfirmed`]). A field the application pinned
//! as its own baseline on resuming is never compared: it is outside what the model describes.

#![allow(dead_code)]

pub mod world;

use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_sqlite::core::model::{self, Merge, Row, View};
use fongo_sync_sqlite::core::{Change, DELETED, Gid, Hlc, Strategy, Value, wins};
use fongo_sync_sqlite::{Collection, Key, Kind, Registry, Replica};
use sqlx::SqliteConnection;

async fn rows(conn: &mut SqliteConnection, sql: &str, params: &[Value]) -> Vec<Vec<Value>> {
    use sqlx::{Row as _, ValueRef};
    let mut query = sqlx::query(sql);
    for p in params {
        query = match p {
            Value::Null => query.bind(None::<i64>),
            Value::Int(v) => query.bind(*v),
            Value::Real(v) => query.bind(*v),
            Value::Text(v) => query.bind(v.clone()),
            Value::Blob(v) => query.bind(v.clone()),
            Value::Ref(g) => query.bind(g.0.to_vec()),
        };
    }
    let found = query.fetch_all(&mut *conn).await.unwrap();
    found
        .iter()
        .map(|row| {
            (0..row.len())
                .map(|i| {
                    if row.try_get_raw(i).unwrap().is_null() {
                        Value::Null
                    } else if let Ok(v) = row.try_get::<i64, _>(i) {
                        Value::Int(v)
                    } else if let Ok(v) = row.try_get::<f64, _>(i) {
                        Value::Real(v)
                    } else if let Ok(v) = row.try_get::<String, _>(i) {
                        Value::Text(v)
                    } else {
                        Value::Blob(row.try_get::<Vec<u8>, _>(i).unwrap())
                    }
                })
                .collect()
        })
        .collect()
}

fn gid_of(value: &Value) -> Gid {
    match value {
        Value::Blob(bytes) => Gid::from_slice(bytes).expect("a gid is 16 bytes"),
        other => panic!("not a gid: {other:?}"),
    }
}

fn text(value: &Value) -> String {
    match value {
        Value::Text(t) => t.clone(),
        other => panic!("not text: {other:?}"),
    }
}

fn quoted(name: &str) -> String {
    format!("\"{name}\"")
}

fn key_columns(c: &Collection) -> Vec<String> {
    match &c.key {
        Key::Minted { column } => vec![column.clone()],
        Key::Natural { fields } => fields.clone(),
    }
}

/// What a replica holds, read from its tables.
#[derive(Debug, Default)]
pub struct Kernel {
    /// The shadow of each registered collection: (collection, gid) to field to entry.
    pub registers: BTreeMap<(String, Gid), BTreeMap<String, (Value, Hlc)>>,
    /// Each aliased gid's canonical gid, by collection.
    pub canon: BTreeMap<(String, Gid), Gid>,
    /// The rows the application can read, by canonical gid.
    pub view: View,
    /// The local key of each row in `view`.
    pub keys: BTreeMap<(String, Gid), String>,
    /// Entities waiting, by canonical gid.
    pub pending: BTreeSet<(String, Gid)>,
    /// Rows by key that are not entities of their own yet or any more.
    pub leftover: BTreeSet<(String, String)>,
    pub dirty: BTreeSet<(String, String)>,
    /// Entities a row not yet collected belongs to, by canonical gid.
    pub uncollected: BTreeSet<(String, Gid)>,
    /// Rows by key and field holding what the kernel left there, and whether pinned.
    pub deviations: BTreeMap<(String, String, String), bool>,
    /// Problems found while reading: rows with no id, two rows for one entity.
    pub broken: Vec<String>,
}

impl Kernel {
    pub fn canonical(&self, collection: &str, gid: Gid) -> Gid {
        *self
            .canon
            .get(&(collection.to_string(), gid))
            .unwrap_or(&gid)
    }
}

pub async fn read(conn: &mut SqliteConnection, replica: &Replica) -> Kernel {
    let registry = replica.registry();
    let known: BTreeSet<&str> = registry
        .collections()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    let mut kernel = Kernel::default();
    for row in rows(
        conn,
        "SELECT collection, gid, field, value, is_ref, clock FROM fongo_shadow",
        &[],
    )
    .await
    {
        let collection = text(&row[0]);
        if !known.contains(collection.as_str()) {
            continue;
        }
        let value = if row[4] == Value::Int(1) {
            Value::Ref(gid_of(&row[3]))
        } else {
            row[3].clone()
        };
        let clock = Hlc::decode(&text(&row[5])).expect("a clock parses");
        kernel
            .registers
            .entry((collection, gid_of(&row[1])))
            .or_default()
            .insert(text(&row[2]), (value, clock));
    }
    for row in rows(conn, "SELECT collection, gid, canon FROM fongo_alias", &[]).await {
        let collection = text(&row[0]);
        if known.contains(collection.as_str()) {
            kernel
                .canon
                .insert((collection, gid_of(&row[1])), gid_of(&row[2]));
        }
    }
    for row in rows(conn, "SELECT collection, gid FROM fongo_pending", &[]).await {
        let collection = text(&row[0]);
        let gid = kernel.canonical(&collection, gid_of(&row[1]));
        kernel.pending.insert((collection, gid));
    }
    for row in rows(conn, "SELECT collection, key FROM fongo_leftover", &[]).await {
        kernel.leftover.insert((text(&row[0]), text(&row[1])));
    }
    for row in rows(conn, "SELECT collection, key FROM fongo_dirty", &[]).await {
        let (collection, key) = (text(&row[0]), text(&row[1]));
        if let Some(gid) = replica.gid_for_key(conn, &collection, &key).await.unwrap() {
            kernel.uncollected.insert((collection.clone(), gid));
        }
        kernel.dirty.insert((collection, key));
    }
    for row in rows(
        conn,
        "SELECT collection, key, field, pinned FROM fongo_deviations",
        &[],
    )
    .await
    {
        kernel.deviations.insert(
            (text(&row[0]), text(&row[1]), text(&row[2])),
            row[3] == Value::Int(1),
        );
    }
    // Minted ids by local id, read once: a lookup per row is slow for large tables.
    let mut minted: BTreeMap<(String, i64), Gid> = BTreeMap::new();
    for row in rows(
        conn,
        "SELECT collection, json_extract(key, '$[0]'), gid FROM fongo_ids",
        &[],
    )
    .await
    {
        let collection = text(&row[0]);
        let gid = kernel.canonical(&collection, gid_of(&row[2]));
        if let Value::Int(local) = row[1] {
            minted.insert((collection, local), gid);
        }
    }
    for c in registry.collections() {
        let keys: Vec<String> = key_columns(c).iter().map(|k| quoted(k)).collect();
        let mut columns = vec![format!("json_array({})", keys.join(", "))];
        columns.extend(c.fields.iter().map(|f| quoted(&f.name)));
        let sql = format!("SELECT {} FROM {}", columns.join(", "), quoted(&c.table));
        for row in rows(conn, &sql, &[]).await {
            let key = text(&row[0]);
            let at = (c.name.clone(), key.clone());
            if kernel.leftover.contains(&at) || kernel.dirty.contains(&at) {
                continue;
            }
            let gid = match (&c.key, &row[1..]) {
                (Key::Minted { .. }, _) => {
                    let local: Option<i64> =
                        key.trim_matches(|ch| ch == '[' || ch == ']').parse().ok();
                    local.and_then(|l| minted.get(&(c.name.clone(), l)).copied())
                }
                (Key::Natural { .. }, _) => replica.gid_for_key(conn, &c.name, &key).await.unwrap(),
            };
            let Some(gid) = gid else {
                kernel
                    .broken
                    .push(format!("{} row {key} has no id", c.name));
                continue;
            };
            let mut fields = Row::new();
            for (f, value) in c.fields.iter().zip(&row[1..]) {
                let value = match (&f.kind, value) {
                    (Kind::Ref { collection, .. }, Value::Int(local)) => {
                        match minted.get(&(collection.clone(), *local)) {
                            Some(g) => Value::Ref(*g),
                            None => Value::Text(format!("no id for {collection} row {local}")),
                        }
                    }
                    (_, value) => value.clone(),
                };
                fields.insert(f.name.clone(), value);
            }
            if let Some(other) = kernel.keys.insert((c.name.clone(), gid), key.clone()) {
                kernel.broken.push(format!(
                    "{} rows {other} and {key} both hold {gid:?}",
                    c.name
                ));
            }
            kernel.view.insert(&c.name, gid, fields);
        }
    }
    kernel
}

/// The changes a replica has taken in: `taken`, which the caller keeps as it applies and sends,
/// and whatever its outbox holds now.
pub async fn seen(conn: &mut SqliteConnection, replica: &Replica, taken: &[Change]) -> Vec<Change> {
    let outbox = replica.outbox(conn, u32::MAX).await.unwrap();
    let mut all = taken.to_vec();
    all.extend(outbox.changes);
    all
}

/// Registers, alias groups and the kernel's own tables, which hold at every step.
fn bookkeeping(
    registry: &Registry,
    merge: &Merge,
    kernel: &Kernel,
    seen: &[Change],
) -> Vec<String> {
    let mut wrong = kernel.broken.clone();
    let empty = BTreeMap::new();
    for ((collection, gid), registers) in &kernel.registers {
        match merge.entities.get(gid) {
            Some(e) if e.collection == *collection => {
                // Until a key deriving its gid arrives, the model sets a natural entity's key
                // fields aside while the kernel keeps them; neither shows the entity.
                let mut registers = registers.clone();
                if !e.keyed
                    && let Some(Key::Natural { fields }) = registry
                        .collections()
                        .iter()
                        .find(|c| c.name == *collection)
                        .map(|c| &c.key)
                {
                    registers.retain(|f, _| !fields.contains(f));
                }
                if e.registers != registers {
                    wrong.push(format!(
                        "{collection} {gid:?} registers differ:\n kernel {registers:?}\n model  {:?}",
                        e.registers
                    ));
                }
            }
            Some(e) => wrong.push(format!(
                "{collection} {gid:?} is in the kernel, the model puts it in {}",
                e.collection
            )),
            None => wrong.push(format!(
                "{collection} {gid:?} has registers the model does not know: {registers:?}"
            )),
        }
    }
    for (gid, e) in &merge.entities {
        if registry
            .collections()
            .iter()
            .all(|c| c.name != e.collection)
        {
            continue;
        }
        let held = kernel
            .registers
            .get(&(e.collection.clone(), *gid))
            .unwrap_or(&empty);
        if held.is_empty() && !e.registers.is_empty() {
            wrong.push(format!(
                "{} {gid:?} has no registers in the kernel, the model has {:?}",
                e.collection, e.registers
            ));
        }
        let canon = kernel.canonical(&e.collection, *gid);
        if canon != e.canonical {
            wrong.push(format!(
                "{} {gid:?} is in the group of {canon:?}, the model says {:?}",
                e.collection, e.canonical
            ));
        }
    }
    for ((collection, gid), canon) in &kernel.canon {
        if !merge
            .entities
            .get(gid)
            .is_some_and(|e| e.collection == *collection)
        {
            wrong.push(format!(
                "{collection} {gid:?} is aliased to {canon:?} but the model does not know it"
            ));
        }
        if kernel.canonical(collection, *canon) != *canon {
            wrong.push(format!(
                "{collection} {gid:?} names {canon:?}, itself aliased"
            ));
        }
        let smallest = kernel
            .canon
            .iter()
            .filter(|((c, _), k)| c == collection && *k == canon)
            .map(|((_, g), _)| *g)
            .min();
        if smallest != Some(*canon) {
            wrong.push(format!(
                "{collection} group {canon:?} is not named by its smallest member"
            ));
        }
    }
    for change in seen {
        if let Value::Text(t) = &change.value
            && t.starts_with("fongo:")
        {
            wrong.push(format!("a stand-in was sent: {change:?}"));
        }
        let identity = registry
            .collections()
            .iter()
            .find(|c| c.name == change.collection)
            .is_some_and(|c| c.identity.iter().flatten().any(|f| *f == change.field));
        let negative = match change.value {
            Value::Int(v) => v < 0,
            Value::Real(v) => v < 0.0,
            _ => false,
        };
        if identity && negative {
            wrong.push(format!("a stand-in was sent: {change:?}"));
        }
    }
    wrong
}

async fn tables(conn: &mut SqliteConnection, replica: &Replica, kernel: &Kernel) -> Vec<String> {
    let mut wrong = Vec::new();
    for c in replica.registry().collections() {
        let found = |key: &str| {
            let columns = key_columns(c);
            let matches: Vec<String> = columns
                .iter()
                .enumerate()
                .map(|(i, k)| format!("{} = json_extract(?1, '$[{i}]')", quoted(k)))
                .collect();
            (
                format!(
                    "SELECT 1 FROM {} WHERE {}",
                    quoted(&c.table),
                    matches.join(" AND ")
                ),
                Value::Text(key.to_string()),
            )
        };
        if let Key::Minted { column } = &c.key {
            let sql = format!(
                "SELECT key, gid, EXISTS (SELECT 1 FROM {} WHERE {} = json_extract(key, '$[0]'))
                   FROM fongo_ids WHERE collection = ?",
                quoted(&c.table),
                quoted(column)
            );
            for row in rows(conn, &sql, &[Value::Text(c.name.clone())]).await {
                let (key, gid) = (text(&row[0]), gid_of(&row[1]));
                let uncollected = kernel.dirty.contains(&(c.name.clone(), key.clone()));
                if !uncollected && row[2] == Value::Int(0) {
                    wrong.push(format!("{} id {key} names no row", c.name));
                }
                if kernel.canonical(&c.name, gid) != gid {
                    wrong.push(format!("{} id {key} maps to a non-canonical gid", c.name));
                }
            }
        }
        for (collection, key, field) in kernel.deviations.keys() {
            if *collection != c.name {
                continue;
            }
            let (sql, param) = found(key);
            if rows(conn, &sql, &[param]).await.is_empty() {
                wrong.push(format!(
                    "{collection} {key} has a deviation in {field} but no row"
                ));
            }
        }
    }
    wrong
}

/// The fields of each live group's sets that change together whose newest write has not all
/// arrived, by the model's registers.
fn incomplete_sets(registry: &Registry, merge: &Merge) -> BTreeSet<(String, Gid, String)> {
    let mut out = BTreeSet::new();
    let mut live: BTreeMap<(String, Gid), Vec<Gid>> = BTreeMap::new();
    for (gid, e) in &merge.entities {
        if e.live {
            live.entry((e.collection.clone(), e.canonical))
                .or_default()
                .push(*gid);
        }
    }
    for ((collection, canon), members) in live {
        let Some(c) = registry.collections().iter().find(|c| c.name == collection) else {
            continue;
        };
        for set in &c.together {
            let clocks: Vec<Option<Hlc>> = set
                .iter()
                .map(|f| {
                    let mut best: Option<&(Value, Hlc)> = None;
                    for m in &members {
                        if let Some(entry) = merge.entities[m].registers.get(f)
                            && wins(Strategy::Lww, best.map(|(v, k)| (v, k)), &entry.0, &entry.1)
                        {
                            best = Some(entry);
                        }
                    }
                    best.map(|(_, k)| *k)
                })
                .collect();
            if clocks.iter().any(|k| *k != clocks[0]) {
                for f in set {
                    out.insert((collection.clone(), canon, f.clone()));
                }
            }
        }
    }
    out
}

/// Groups with a live member the kernel cannot yet count live: its tombstone has not arrived, or
/// an owner it names has no member that can be. The model counts a member with no tombstone live,
/// having no notion of waiting; the kernel waits for the tombstone, which every replica writes
/// when it makes an entity, before it shows the member or lends its fields to the group.
fn unconfirmed(schema: &model::Schema, merge: &Merge) -> BTreeSet<(String, Gid)> {
    let mut confirmed: BTreeSet<Gid> = BTreeSet::new();
    loop {
        let groups: BTreeSet<(&str, Gid)> = confirmed
            .iter()
            .map(|g| {
                let e = &merge.entities[g];
                (e.collection.as_str(), e.canonical)
            })
            .collect();
        let more: Vec<Gid> = merge
            .entities
            .iter()
            .filter(|(gid, e)| !confirmed.contains(gid) && e.live)
            .filter(|(_, e)| matches!(e.registers.get(DELETED), Some((Value::Int(0), _))))
            .filter(|(_, e)| {
                let Some(c) = schema.collection(&e.collection) else {
                    return false;
                };
                c.fields
                    .iter()
                    .all(|f| match (&f.kind, e.registers.get(&f.name)) {
                        (model::FieldKind::Owner(t), Some((Value::Ref(owner), _))) => merge
                            .entities
                            .get(owner)
                            .is_some_and(|o| groups.contains(&(t.as_str(), o.canonical))),
                        _ => true,
                    })
            })
            .map(|(gid, _)| *gid)
            .collect();
        if more.is_empty() {
            break;
        }
        confirmed.extend(more);
    }
    merge
        .entities
        .iter()
        .filter(|(gid, e)| e.live && !confirmed.contains(gid))
        .map(|(_, e)| (e.collection.clone(), e.canonical))
        .collect()
}

fn pinned(kernel: &Kernel) -> BTreeSet<(String, Gid, String)> {
    let mut out = BTreeSet::new();
    for ((collection, key, field), pinned) in &kernel.deviations {
        if !pinned {
            continue;
        }
        if let Some(((_, gid), _)) = kernel
            .keys
            .iter()
            .find(|((c, _), k)| c == collection && *k == key)
        {
            out.insert((collection.clone(), *gid, field.clone()));
        }
    }
    out
}

/// Compares rows, leaving out the entities and fields `skip` names.
fn rows_differ(
    expected: &View,
    actual: &View,
    skip_entity: impl Fn(&str, Gid) -> bool,
    skip_field: impl Fn(&str, Gid, &str) -> bool,
) -> Vec<String> {
    let mut wrong = Vec::new();
    let names: BTreeSet<&String> = expected
        .collections
        .keys()
        .chain(actual.collections.keys())
        .collect();
    let same = |a: &Value, b: &Value| a.total_cmp(b) == std::cmp::Ordering::Equal;
    for name in names {
        let gids: BTreeSet<&Gid> = expected
            .rows(name)
            .chain(actual.rows(name))
            .map(|(g, _)| g)
            .collect();
        for gid in gids {
            if skip_entity(name, *gid) {
                continue;
            }
            match (expected.row(name, *gid), actual.row(name, *gid)) {
                (Some(m), Some(k)) => {
                    for (field, value) in m {
                        if skip_field(name, *gid, field) {
                            continue;
                        }
                        let held = k.get(field).unwrap_or(&Value::Null);
                        if !same(value, held) {
                            wrong.push(format!(
                                "{name} {gid:?}.{field}: kernel {held:?}, model {value:?}"
                            ));
                        }
                    }
                }
                (Some(m), None) => {
                    wrong.push(format!("{name} {gid:?} has no row, the model shows {m:?}"))
                }
                (None, Some(k)) => wrong.push(format!(
                    "{name} {gid:?} has a row the model does not: {k:?}"
                )),
                (None, None) => {}
            }
        }
    }
    wrong
}

fn verdict(wrong: Vec<String>, what: &str) -> Result<(), String> {
    if wrong.is_empty() {
        Ok(())
    } else {
        Err(format!("{what}:\n{}", wrong.join("\n")))
    }
}

/// What must hold after any apply or collect.
pub async fn midway(
    conn: &mut SqliteConnection,
    replica: &Replica,
    taken: &[Change],
) -> Result<(), String> {
    let seen = seen(conn, replica, taken).await;
    let registry = replica.registry();
    let schema = registry.model_schema();
    let merge = model::merge(&schema, &seen);
    let expected = model::view(&schema, &seen);
    let kernel = read(conn, replica).await;
    let mut wrong = bookkeeping(registry, &merge, &kernel, &seen);
    wrong.extend(tables(conn, replica, &kernel).await);
    let incomplete = incomplete_sets(registry, &merge);
    let pinned = pinned(&kernel);
    let unconfirmed = unconfirmed(&schema, &merge);
    wrong.extend(rows_differ(
        &expected,
        &kernel.view,
        |c, g| {
            let at = (c.to_string(), g);
            kernel.pending.contains(&at)
                || kernel.uncollected.contains(&at)
                || unconfirmed.contains(&at)
        },
        |c, g, f| {
            let at = (c.to_string(), g, f.to_string());
            incomplete.contains(&at) || pinned.contains(&at)
        },
    ));
    if !wrong.is_empty() {
        wrong.push(context(&merge, &kernel));
    }
    verdict(wrong, "the replica departs from the model")
}

/// The groups and what waits, to read a failure by.
fn context(merge: &Merge, kernel: &Kernel) -> String {
    let mut out =
        vec!["-- model entities (gid collection canonical keyed live registers):".to_string()];
    for (gid, e) in &merge.entities {
        out.push(format!(
            "{gid:?} {} {:?} {} {} {:?}",
            e.collection, e.canonical, e.keyed, e.live, e.registers
        ));
    }
    out.push(format!("-- kernel pending: {:?}", kernel.pending));
    out.push(format!("-- kernel keys: {:?}", kernel.keys));
    out.push(format!("-- kernel deviations: {:?}", kernel.deviations));
    out.join("\n")
}

/// What must hold once nothing waits: the rows are the model's view, and no two rows share an
/// identity.
pub async fn settled(
    conn: &mut SqliteConnection,
    replica: &Replica,
    taken: &[Change],
) -> Result<(), String> {
    let seen = seen(conn, replica, taken).await;
    let registry = replica.registry();
    let schema = registry.model_schema();
    let merge = model::merge(&schema, &seen);
    let expected = model::view(&schema, &seen);
    let kernel = read(conn, replica).await;
    let mut wrong = bookkeeping(registry, &merge, &kernel, &seen);
    wrong.extend(tables(conn, replica, &kernel).await);
    for (what, left) in [
        ("waiting", format!("{:?}", kernel.pending)),
        ("left over", format!("{:?}", kernel.leftover)),
        ("not collected", format!("{:?}", kernel.dirty)),
    ] {
        if left != "{}" {
            wrong.push(format!("{what}: {left}"));
        }
    }
    // A reference to an entity that is gone reads as null, which the kernel records as left in
    // the row since its register still names the entity. Anything else left is a row that has
    // not caught up.
    for ((collection, key, field), pinned) in &kernel.deviations {
        let gone_reference = registry
            .collections()
            .iter()
            .find(|c| c.name == *collection)
            .and_then(|c| c.fields.iter().find(|f| f.name == *field))
            .is_some_and(|f| matches!(f.kind, Kind::Ref { .. }))
            && kernel
                .keys
                .iter()
                .find(|((c, _), k)| c == collection && *k == key)
                .and_then(|((c, g), _)| expected.row(c, *g))
                .is_some_and(|row| row.get(field) == Some(&Value::Null));
        if !pinned && !gone_reference {
            wrong.push(format!(
                "{collection} {key}.{field} still holds what the kernel left"
            ));
        }
    }
    let pinned = pinned(&kernel);
    wrong.extend(rows_differ(
        &expected,
        &kernel.view,
        |_, _| false,
        |c, g, f| pinned.contains(&(c.to_string(), g, f.to_string())),
    ));
    for (collection, a, b) in model::unmerged_identities(&schema, &expected) {
        wrong.push(format!("{collection} {a:?} and {b:?} share an identity"));
    }
    if !wrong.is_empty() {
        wrong.push(context(&merge, &kernel));
    }
    verdict(wrong, "the settled replica is not the model's view")
}

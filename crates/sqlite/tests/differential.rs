//! The same scripts run through the core's in-memory replica (`crates/core/tests/sim`), the
//! executable design of an adapter, and through this adapter, and the two are compared after
//! every step: the changes each step sends, the application's rows and what waits. Once
//! everything is delivered both must show `model::view` of everything.
//!
//! Both sides mint gids the same way (the databases' `randomblob` counts, as the sim does) and
//! read one step's wall clock, so gids, and the order of clocks, agree; rows are compared by the
//! entity they stand for, a row not yet collected by the insert that made it, and a natural row
//! by its key.
//!
//! Where the two may legitimately differ, only what the model fixes is compared:
//! - A step's changes are compared as a set without clocks: clocks agree only in order, since
//!   the sim's wall is the step and the database's is milliseconds.
//! - Local keys are never compared: the sim numbers rows across tables, SQLite per table.
//! - A minted row inserted since the last collect is named by the insert that made it, not by
//!   the gid mapped to its local id: SQLite may reuse the id of a row whose removal is not yet
//!   collected, and the sim never does.
//! - Gids are renamed (see `Names`) before comparing: the sim mints in collection-name and
//!   local-key order within one collect, the adapter in its own order.
//! - `$alias` changes are compared by the groups they join, not pair by pair: when three or more
//!   entities clash at once the sim aliases every pair and the adapter only enough pairs. So that
//!   the logs stay aligned, a batch's aliases are delivered together.
//! - A `$deleted 1` that the same batch overrides with a later `$deleted 0` of the same entity is
//!   left out on both sides: when one row leaves a natural key and another takes it in one
//!   collect, the sim sends the old entity's tombstone and its re-creation, the adapter only the
//!   re-creation, which carries every field at the later clock.

mod support;

#[allow(dead_code)]
#[path = "../../core/tests/sim/replica.rs"]
mod sim;

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{c_int, c_void};
use std::sync::Mutex;

use fongo_sync_sqlite::core::model::{self, ALIAS, FieldKind, Identity, Row, Schema};
use fongo_sync_sqlite::core::{Change, DELETED, Gid, Value};
use fongo_sync_sqlite::{Collection, Registry, Replica};
use libsqlite3_sys as ffi;
use proptest::prelude::*;
use proptest::test_runner::RngSeed;
use sqlx::SqliteConnection;
use support::world::World;

const SCHEMA: &[&str] = &[
    "CREATE TABLE authors (id INTEGER PRIMARY KEY, name TEXT)",
    "CREATE TABLE shelves (id INTEGER PRIMARY KEY, name TEXT UNIQUE)",
    "CREATE TABLE books (
        id INTEGER PRIMARY KEY,
        shelf_id INTEGER NOT NULL REFERENCES shelves(id) ON DELETE CASCADE,
        title TEXT,
        author_id INTEGER REFERENCES authors(id) ON DELETE SET NULL)",
    "CREATE TABLE tags (
        book_id INTEGER NOT NULL REFERENCES books(id) ON DELETE CASCADE,
        tag TEXT NOT NULL,
        note TEXT,
        PRIMARY KEY (book_id, tag))",
];

/// The columns the sim's tables refuse NULL in, as `SCHEMA` declares them.
const NOT_NULL: &[(&str, &str)] = &[("books", "shelf_id"), ("tags", "book_id"), ("tags", "tag")];

/// In name order, the order the sim mints gids in within one collect.
fn registry() -> Registry {
    Registry::new(vec![
        Collection::minted("authors", "authors", "id").field("name"),
        Collection::minted("books", "books", "id")
            .owner("shelf_id", "shelves")
            .field("title")
            .reference("author_id", "authors"),
        Collection::minted("shelves", "shelves", "id")
            .field("name")
            .identity(&["name"]),
        Collection::natural("tags", "tags", &["book_id", "tag"])
            .owner("book_id", "books")
            .field("tag")
            .field("note"),
    ])
    .unwrap()
}

// --- Gids minted as the sim mints them ---------------------------------------------------------

struct Counter {
    replica: u8,
    next: Mutex<u32>,
}

unsafe extern "C" fn counted_blob(
    ctx: *mut ffi::sqlite3_context,
    _: c_int,
    _: *mut *mut ffi::sqlite3_value,
) {
    // SAFETY: registered below with a `Counter` as its user data.
    let counter = unsafe { &*ffi::sqlite3_user_data(ctx).cast::<Counter>() };
    let mut next = counter.next.lock().unwrap();
    let mut bytes = [0xF0; 16];
    bytes[1] = counter.replica;
    bytes[12..].copy_from_slice(&next.to_be_bytes());
    *next += 1;
    // SAFETY: SQLite copies the bytes before this returns.
    unsafe {
        ffi::sqlite3_result_blob(
            ctx,
            bytes.as_ptr().cast::<c_void>(),
            16,
            ffi::SQLITE_TRANSIENT(),
        );
    }
}

unsafe extern "C" fn drop_counter(counter: *mut c_void) {
    // SAFETY: SQLite passes back the pointer given below, once.
    drop(unsafe { Box::from_raw(counter.cast::<Counter>()) });
}

/// Replaces `randomblob` so that the node id takes 0 and the n-th gid minted takes n, as the
/// sim's replica `replica` numbers them.
async fn count_blobs(conn: &mut SqliteConnection, replica: u8) {
    let mut handle = support::lock(conn).await;
    let db = handle.as_raw_handle().as_ptr();
    let counter = Box::new(Counter {
        replica,
        next: Mutex::new(0),
    });
    // SAFETY: `db` is the open connection's handle, locked for this call; SQLite owns the counter
    // from here and frees it with `drop_counter`.
    let status = unsafe {
        ffi::sqlite3_create_function_v2(
            db,
            c"randomblob".as_ptr(),
            1,
            ffi::SQLITE_UTF8,
            Box::into_raw(counter).cast::<c_void>(),
            Some(counted_blob),
            None,
            None,
            Some(drop_counter),
        )
    };
    assert_eq!(status, ffi::SQLITE_OK);
}

// --- Scripts -----------------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Op {
    Insert {
        at: u8,
        collection: u8,
        picks: [u8; 3],
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
    /// Collects, retries what waits, and sends.
    Push {
        at: u8,
    },
    /// Collects, then takes the unseen changes `seed` picks, in order or reversed, and sends;
    /// `twice` takes the same changes again.
    Deliver {
        at: u8,
        seed: u64,
        reverse: bool,
        twice: bool,
    },
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (any::<u8>(), any::<u8>(), any::<[u8; 3]>())
            .prop_map(|(at, collection, picks)| Op::Insert { at, collection, picks }),
        4 => (any::<u8>(), any::<u8>(), any::<u8>(), any::<u8>(), any::<u8>())
            .prop_map(|(at, collection, row, field, pick)| Op::Edit { at, collection, row, field, pick }),
        1 => (any::<u8>(), any::<u8>(), any::<u8>())
            .prop_map(|(at, collection, row)| Op::Delete { at, collection, row }),
        2 => any::<u8>().prop_map(|at| Op::Push { at }),
        4 => (any::<u8>(), any::<u64>(), any::<bool>(), any::<bool>())
            .prop_map(|(at, seed, reverse, twice)| Op::Deliver { at, seed, reverse, twice }),
    ]
}

fn mix(seed: u64, i: usize) -> u64 {
    let mut x = seed ^ (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
    x ^ (x >> 33)
}

/// A value the application writes, named without local keys.
#[derive(Debug, Clone)]
enum Choice {
    Null,
    Text(&'static str),
    /// The row of the collection that the snapshot names this way.
    Row(String, String),
}

// --- What a replica shows, without local keys or clocks ----------------------------------------

/// A replica's rows, with references as local keys, and what it knows.
struct Local {
    tables: BTreeMap<String, BTreeMap<i64, Row>>,
    /// The entity each minted row stands for.
    gids: BTreeMap<(String, i64), Gid>,
    held: Vec<(String, Gid)>,
    merged: Vec<Change>,
}

#[derive(Debug, PartialEq)]
struct Snapshot {
    /// By collection: (the row's name, its fields), sorted.
    rows: BTreeMap<String, Vec<(String, String)>>,
    held: BTreeSet<String>,
}

struct Seen {
    snapshot: Snapshot,
    /// The local key of each named row.
    keys: BTreeMap<(String, String), i64>,
}

fn target(schema: &Schema, collection: &str, field: &str) -> Option<String> {
    let c = schema.collection(collection).unwrap();
    match &c.fields.iter().find(|f| f.name == field).unwrap().kind {
        FieldKind::Reference(t) | FieldKind::Owner(t) => Some(t.clone()),
        FieldKind::Plain => None,
    }
}

/// Names each row by its entity's canonical gid; a minted row not yet collected by the insert
/// that made it; a natural row by its key.
fn snapshot(
    schema: &Schema,
    names: &Names,
    local: &Local,
    labels: &BTreeMap<(String, i64), u32>,
) -> Seen {
    let merged = model::merge(schema, &local.merged);
    let canon = |g: Gid| names.gid(schema, merged.entities.get(&g).map_or(g, |e| e.canonical));
    fn name(
        schema: &Schema,
        local: &Local,
        labels: &BTreeMap<(String, i64), u32>,
        canon: &dyn Fn(Gid) -> Gid,
        c: &str,
        k: i64,
    ) -> String {
        let coll = schema.collection(c).unwrap();
        match &coll.identity {
            Identity::Minted => match labels.get(&(c.to_string(), k)) {
                Some(l) => format!("new{l}"),
                None => match local.gids.get(&(c.to_string(), k)) {
                    Some(g) => format!("{:?}", canon(*g)),
                    None => format!("unnamed{k}"),
                },
            },
            Identity::Natural(key) => {
                let Some(row) = local.tables[c].get(&k) else {
                    return format!("missing{k}");
                };
                let parts: Vec<String> = key
                    .iter()
                    .map(|f| value(schema, local, labels, canon, c, f, row))
                    .collect();
                format!("({})", parts.join(", "))
            }
        }
    }
    fn value(
        schema: &Schema,
        local: &Local,
        labels: &BTreeMap<(String, i64), u32>,
        canon: &dyn Fn(Gid) -> Gid,
        c: &str,
        f: &str,
        row: &Row,
    ) -> String {
        match (target(schema, c, f), row.get(f).unwrap_or(&Value::Null)) {
            (Some(t), Value::Int(k)) if local.tables[&t].contains_key(k) => {
                name(schema, local, labels, canon, &t, *k)
            }
            (Some(t), Value::Int(k)) => format!("dangling {t} {k}"),
            (_, v) => format!("{v:?}"),
        }
    }
    let mut rows = BTreeMap::new();
    let mut keys = BTreeMap::new();
    for c in &schema.collections {
        let mut named = Vec::new();
        for (k, row) in &local.tables[&c.name] {
            let n = name(schema, local, labels, &canon, &c.name, *k);
            let fields: Vec<String> = c
                .fields
                .iter()
                .map(|f| {
                    let v = value(schema, local, labels, &canon, &c.name, &f.name, row);
                    format!("{}={v}", f.name)
                })
                .collect();
            keys.entry((c.name.clone(), n.clone())).or_insert(*k);
            named.push((n, fields.join(" ")));
        }
        named.sort();
        rows.insert(c.name.clone(), named);
    }
    let held = local
        .held
        .iter()
        .map(|(c, g)| format!("{c} {:?}", canon(*g)))
        .collect();
    Seen {
        snapshot: Snapshot { rows, held },
        keys,
    }
}

/// Names for gids that do not depend on the order one collect mints them in: a minted gid by
/// the insert that made its row, or, for one whose row a merge in the same step moved to another
/// entity, by the step, replica and collection that minted it and its rank among those; a
/// natural gid by its key, read through these names.
#[derive(Default)]
struct Names {
    minted: BTreeMap<Gid, Gid>,
    natural: BTreeMap<Gid, (String, BTreeMap<String, Value>)>,
}

impl Names {
    /// Learns the gids replica `replica` minted in what it sent at `step`, given the gid each
    /// row inserted since its last collect now stands for and the insert that made it.
    fn learn(
        &mut self,
        schema: &Schema,
        replica: usize,
        step: u64,
        sent: &[Change],
        inserted: &BTreeMap<Gid, u32>,
    ) {
        let mut fresh: BTreeMap<usize, BTreeSet<Gid>> = BTreeMap::new();
        for c in sent {
            let (index, coll) = schema
                .collections
                .iter()
                .enumerate()
                .find(|(_, k)| k.name == c.collection)
                .unwrap();
            match &coll.identity {
                Identity::Natural(key) if key.contains(&c.field) => {
                    self.natural
                        .entry(c.gid)
                        .or_insert_with(|| (c.collection.clone(), BTreeMap::new()))
                        .1
                        .insert(c.field.clone(), c.value.clone());
                }
                Identity::Minted
                    if c.field == DELETED
                        && c.value == Value::Int(0)
                        && !self.minted.contains_key(&c.gid) =>
                {
                    fresh.entry(index).or_default().insert(c.gid);
                }
                _ => {}
            }
        }
        for (index, gids) in fresh {
            let mut unmatched = Vec::new();
            for g in gids {
                match inserted.get(&g) {
                    Some(label) => {
                        let mut bytes = [0xEE; 16];
                        bytes[12..].copy_from_slice(&label.to_be_bytes());
                        self.minted.insert(g, Gid(bytes));
                    }
                    None => unmatched.push(g),
                }
            }
            for (rank, g) in unmatched.into_iter().enumerate() {
                let mut bytes = [0xEE; 16];
                bytes[1] = replica as u8;
                bytes[2..10].copy_from_slice(&step.to_be_bytes());
                bytes[10] = index as u8;
                bytes[12..].copy_from_slice(&(rank as u32).to_be_bytes());
                self.minted.insert(g, Gid(bytes));
            }
        }
    }

    fn gid(&self, schema: &Schema, g: Gid) -> Gid {
        if let Some(n) = self.minted.get(&g) {
            return *n;
        }
        if let Some((c, fields)) = self.natural.get(&g)
            && let Identity::Natural(key) = &schema.collection(c).unwrap().identity
        {
            let values: Vec<Value> = key
                .iter()
                .map(|f| self.value(schema, fields.get(f).unwrap_or(&Value::Null)))
                .collect();
            return Gid::derive(c, &values);
        }
        g
    }

    fn value(&self, schema: &Schema, v: &Value) -> Value {
        match v {
            Value::Ref(g) => Value::Ref(self.gid(schema, *g)),
            other => other.clone(),
        }
    }

    fn change(&self, schema: &Schema, c: &Change) -> String {
        format!(
            "{} {:?} {} {:?}",
            c.collection,
            self.gid(schema, c.gid),
            c.field,
            self.value(schema, &c.value)
        )
    }

    /// What `changes` send: every change but `$alias` ones, and the groups those join.
    fn canonical(&self, schema: &Schema, changes: &[Change]) -> (Vec<String>, Vec<Vec<String>>) {
        let mut out: Vec<String> = changes
            .iter()
            .filter(|c| c.field != ALIAS)
            .map(|c| self.change(schema, c))
            .collect();
        out.sort();
        let mut groups: Vec<BTreeSet<String>> = Vec::new();
        for c in changes.iter().filter(|c| c.field == ALIAS) {
            let Value::Ref(to) = c.value else {
                panic!("an alias names a gid: {c:?}")
            };
            let ends = [c.gid, to].map(|g| format!("{} {:?}", c.collection, self.gid(schema, g)));
            let mut joined: BTreeSet<String> = ends.iter().cloned().collect();
            groups.retain(|g| {
                let touches = ends.iter().any(|e| g.contains(e));
                if touches {
                    joined.extend(g.iter().cloned());
                }
                !touches
            });
            groups.push(joined);
        }
        let mut groups: Vec<Vec<String>> = groups
            .into_iter()
            .map(|g| g.into_iter().collect())
            .collect();
        groups.sort();
        (out, groups)
    }

    /// Log entries in the order `canonical` sorts changes, so that both sides' logs hold one
    /// entry at each index: one per change, and one for all of a batch's `$alias` changes.
    fn entries(&self, schema: &Schema, changes: &[Change]) -> Vec<Vec<Change>> {
        let (mut aliases, mut out): (Vec<Change>, Vec<Change>) =
            changes.iter().cloned().partition(|c| c.field == ALIAS);
        out.sort_by_cached_key(|c| self.change(schema, c));
        let mut entries: Vec<Vec<Change>> = out.into_iter().map(|c| vec![c]).collect();
        if !aliases.is_empty() {
            aliases.sort_by_cached_key(|c| self.change(schema, c));
            entries.push(aliases);
        }
        entries
    }
}

/// `changes` without a `$deleted 1` that the same batch overrides with a later `$deleted 0`.
fn netted(changes: &[Change]) -> Vec<Change> {
    changes
        .iter()
        .filter(|c| {
            !(c.field == DELETED
                && c.value == Value::Int(1)
                && changes.iter().any(|d| {
                    d.collection == c.collection
                        && d.gid == c.gid
                        && d.field == DELETED
                        && d.value == Value::Int(0)
                        && d.clock > c.clock
                }))
        })
        .cloned()
        .collect()
}

// --- Both sides --------------------------------------------------------------------------------

struct Both<'s> {
    schema: &'s Schema,
    world: World,
    sims: Vec<sim::Replica<'s>>,
    conns: Vec<SqliteConnection>,
    peers: Vec<Replica>,
    /// Every change each database applied or sent.
    taken: Vec<Vec<Change>>,
    sim_names: Names,
    sql_names: Names,
    sim_labels: Vec<BTreeMap<(String, i64), u32>>,
    sql_labels: Vec<BTreeMap<(String, i64), u32>>,
    labels: u32,
    sim_log: Vec<Vec<Change>>,
    sql_log: Vec<Vec<Change>>,
    seen: Vec<Vec<bool>>,
    last: Vec<Seen>,
    step: u64,
    /// Rows a bounded step of collecting takes, by the seed, or `None` to collect whole.
    collect_rows: Option<usize>,
}

impl<'s> Both<'s> {
    async fn new(schema: &'s Schema, seed: u64, n: usize) -> Both<'s> {
        let world = World::new(seed);
        let mut conns = Vec::new();
        let mut peers = Vec::new();
        for i in 0..n {
            let mut conn = world.connect(i as u64).await;
            count_blobs(&mut conn, i as u8).await;
            for sql in SCHEMA {
                sqlx::query(sql).execute(&mut conn).await.unwrap();
            }
            let replica = Replica::new(registry());
            replica.install(&mut conn).await.unwrap();
            conns.push(conn);
            peers.push(replica);
        }
        let empty = || Seen {
            snapshot: Snapshot {
                rows: BTreeMap::new(),
                held: BTreeSet::new(),
            },
            keys: BTreeMap::new(),
        };
        Both {
            schema,
            world,
            sims: (0..n)
                .map(|i| sim::Replica::new(schema, i as u8, NOT_NULL))
                .collect(),
            conns,
            peers,
            taken: vec![Vec::new(); n],
            sim_names: Names::default(),
            sql_names: Names::default(),
            sim_labels: vec![BTreeMap::new(); n],
            sql_labels: vec![BTreeMap::new(); n],
            labels: 0,
            sim_log: Vec::new(),
            sql_log: Vec::new(),
            seen: vec![Vec::new(); n],
            collect_rows: [None, Some(1), Some(2), Some(5)][(seed % 4) as usize],
            last: (0..n).map(|_| empty()).collect(),
            step: 0,
        }
    }

    fn tick(&mut self) {
        self.step += 1;
        for s in &mut self.sims {
            s.wall = self.step;
        }
        self.world.later(1_000_000);
    }

    async fn sql_local(&mut self, i: usize) -> Local {
        let conn = &mut self.conns[i];
        let inspection = self.peers[i].inspect(conn).await.unwrap();
        let mut tables = BTreeMap::new();
        for c in &self.schema.collections {
            let columns: Vec<String> = c.fields.iter().map(|f| format!("\"{}\"", f.name)).collect();
            let sql = format!("SELECT rowid, {} FROM \"{}\"", columns.join(", "), c.name);
            let mut table = BTreeMap::new();
            for r in support::rows(conn, &sql, &[]).await {
                let Value::Int(k) = r[0] else {
                    panic!("rowid {:?}", r[0])
                };
                let row: Row = c
                    .fields
                    .iter()
                    .zip(&r[1..])
                    .map(|(f, v)| (f.name.clone(), v.clone()))
                    .collect();
                table.insert(k, row);
            }
            tables.insert(c.name.clone(), table);
        }
        let gids = inspection
            .ids
            .iter()
            .map(|(c, key, g)| {
                let k = key.trim_matches(['[', ']']).parse().unwrap();
                ((c.clone(), k), *g)
            })
            .collect();
        Local {
            tables,
            gids,
            held: inspection.held,
            merged: self.taken[i].clone(),
        }
    }

    fn sim_local(&self, i: usize) -> Local {
        let r = &self.sims[i];
        let minted = |c: &str| {
            matches!(
                self.schema.collection(c).unwrap().identity,
                Identity::Minted
            )
        };
        Local {
            tables: r.tables.clone(),
            gids: r
                .gid_of
                .iter()
                .filter(|((c, _), _)| minted(c))
                .map(|(k, g)| (k.clone(), *g))
                .collect(),
            held: r.held.iter().cloned().collect(),
            merged: r.merged.clone(),
        }
    }

    /// Compares replica `i` on both sides and keeps what it shows for picking rows.
    async fn compare(&mut self, i: usize) -> Result<(), String> {
        let sim = snapshot(
            self.schema,
            &self.sim_names,
            &self.sim_local(i),
            &self.sim_labels[i],
        );
        let sql_local = self.sql_local(i).await;
        let sql = snapshot(
            self.schema,
            &self.sql_names,
            &sql_local,
            &self.sql_labels[i],
        );
        if sim.snapshot != sql.snapshot {
            // Which side the model agrees with, to read the failure by.
            let sim_model = self.sims[i].check();
            let sql_model =
                support::midway(&mut self.conns[i], &self.peers[i], &self.taken[i]).await;
            return Err(format!(
                "replica {i} differs\nsim: {:#?}\nsql: {:#?}\nsim against the model: {sim_model:?}\nsql against the model: {sql_model:?}",
                sim.snapshot, sql.snapshot
            ));
        }
        self.last[i] = sim;
        Ok(())
    }

    fn choose(&self, i: usize, collection: &str, field: &str, pick: u8) -> Choice {
        let not_null = NOT_NULL.contains(&(collection, field));
        match target(self.schema, collection, field) {
            Some(t) => {
                let rows = self.last[i]
                    .snapshot
                    .rows
                    .get(&t)
                    .map_or(&[][..], Vec::as_slice);
                let mut names: Vec<&String> = rows.iter().map(|(n, _)| n).collect();
                names.dedup();
                if names.is_empty() || (!not_null && pick.is_multiple_of(4)) {
                    Choice::Null
                } else {
                    Choice::Row(t, names[usize::from(pick) % names.len()].clone())
                }
            }
            None if not_null => Choice::Text(["a", "b"][usize::from(pick % 2)]),
            None => {
                [Choice::Null, Choice::Text("a"), Choice::Text("b")][usize::from(pick % 3)].clone()
            }
        }
    }

    fn sim_value(&self, i: usize, choice: &Choice) -> Value {
        match choice {
            Choice::Null => Value::Null,
            Choice::Text(t) => Value::Text(t.to_string()),
            Choice::Row(c, n) => Value::Int(self.sims_key(i, c, n)),
        }
    }

    fn sims_key(&self, i: usize, c: &str, n: &str) -> i64 {
        self.last[i].keys[&(c.to_string(), n.to_string())]
    }

    async fn sql_key(&mut self, i: usize, c: &str, n: &str) -> i64 {
        let local = self.sql_local(i).await;
        snapshot(self.schema, &self.sql_names, &local, &self.sql_labels[i]).keys
            [&(c.to_string(), n.to_string())]
    }

    async fn sql_literal(&mut self, i: usize, choice: &Choice) -> String {
        match choice {
            Choice::Null => "NULL".into(),
            Choice::Text(t) => format!("'{t}'"),
            Choice::Row(c, n) => self.sql_key(i, c, n).await.to_string(),
        }
    }

    /// Runs one step on both sides and compares the replica it touched.
    async fn perform(&mut self, op: &Op) -> Result<(), String> {
        self.tick();
        let n = self.sims.len();
        let schema = self.schema;
        let collection = |c: u8| schema.collections[usize::from(c) % 4].name.clone();
        let pick_row = |me: &Self, i: usize, c: &str, row: u8| -> Option<String> {
            let rows = me.last[i]
                .snapshot
                .rows
                .get(c)
                .map_or(&[][..], Vec::as_slice);
            let mut names: Vec<&String> = rows.iter().map(|(n, _)| n).collect();
            names.dedup();
            (!names.is_empty()).then(|| names[usize::from(row) % names.len()].clone())
        };
        let i = match op {
            Op::Insert { at, .. }
            | Op::Edit { at, .. }
            | Op::Delete { at, .. }
            | Op::Push { at }
            | Op::Deliver { at, .. } => usize::from(*at) % n,
        };
        match op {
            Op::Insert {
                collection: c,
                picks,
                ..
            } => {
                let c = collection(*c);
                let fields: Vec<String> = self
                    .schema
                    .collection(&c)
                    .unwrap()
                    .fields
                    .iter()
                    .map(|f| f.name.clone())
                    .collect();
                let choices: Vec<Choice> = fields
                    .iter()
                    .enumerate()
                    .map(|(j, f)| self.choose(i, &c, f, picks[j % 3]))
                    .collect();
                let row: Row = fields
                    .iter()
                    .zip(&choices)
                    .map(|(f, ch)| (f.clone(), self.sim_value(i, ch)))
                    .collect();
                let mut literals = Vec::new();
                for ch in &choices {
                    literals.push(self.sql_literal(i, ch).await);
                }
                self.labels += 1;
                let label = self.labels;
                if let Some(k) = self.sims[i].insert(&c, row) {
                    self.sim_labels[i].insert((c.clone(), k), label);
                }
                let sql = format!(
                    "INSERT INTO \"{c}\" ({}) VALUES ({})",
                    fields.join(", "),
                    literals.join(", ")
                );
                if let Ok(done) = sqlx::query(&sql).execute(&mut self.conns[i]).await {
                    self.sql_labels[i].insert((c, done.last_insert_rowid()), label);
                }
            }
            Op::Edit {
                collection: c,
                row,
                field,
                pick,
                ..
            } => {
                let c = collection(*c);
                let Some(name) = pick_row(self, i, &c, *row) else {
                    return Ok(());
                };
                let fields = &self.schema.collection(&c).unwrap().fields;
                let f = fields[usize::from(*field) % fields.len()].name.clone();
                let choice = self.choose(i, &c, &f, *pick);
                let value = self.sim_value(i, &choice);
                let k = self.sims_key(i, &c, &name);
                self.sims[i].edit(&c, k, &f, value);
                let literal = self.sql_literal(i, &choice).await;
                let k = self.sql_key(i, &c, &name).await;
                let sql = format!("UPDATE \"{c}\" SET \"{f}\" = {literal} WHERE rowid = {k}");
                let _ = sqlx::query(&sql).execute(&mut self.conns[i]).await;
            }
            Op::Delete {
                collection: c, row, ..
            } => {
                let c = collection(*c);
                let Some(name) = pick_row(self, i, &c, *row) else {
                    return Ok(());
                };
                let k = self.sims_key(i, &c, &name);
                self.sims[i].delete(&c, k);
                let k = self.sql_key(i, &c, &name).await;
                let sql = format!("DELETE FROM \"{c}\" WHERE rowid = {k}");
                let _ = sqlx::query(&sql).execute(&mut self.conns[i]).await;
            }
            Op::Push { .. } => return self.exchange(i, &[]).await,
            Op::Deliver {
                seed,
                reverse,
                twice,
                ..
            } => {
                self.seen[i].resize(self.sim_log.len(), false);
                let mut chosen: Vec<usize> = (0..self.sim_log.len())
                    .filter(|&j| !self.seen[i][j] && !mix(*seed, j).is_multiple_of(3))
                    .collect();
                if *reverse {
                    chosen.reverse();
                }
                for &j in &chosen {
                    self.seen[i][j] = true;
                }
                self.exchange(i, &chosen).await?;
                if *twice {
                    self.exchange(i, &chosen).await?;
                }
                return Ok(());
            }
        }
        self.compare(i).await
    }

    /// Collects on replica `i`, applies the changes at `picks` in the logs, and sends.
    async fn exchange(&mut self, i: usize, picks: &[usize]) -> Result<(), String> {
        let sim_in: Vec<Change> = picks
            .iter()
            .flat_map(|&j| self.sim_log[j].clone())
            .collect();
        let sql_in: Vec<Change> = picks
            .iter()
            .flat_map(|&j| self.sql_log[j].clone())
            .collect();

        let start = self.sims[i].outbox.len();
        self.sims[i].sync(&sim_in);
        let sim_out = netted(&self.sims[i].outbox[start..]);

        let (conn, peer) = (&mut self.conns[i], &self.peers[i]);
        match self.collect_rows {
            None => {
                peer.collect(conn).await.unwrap();
            }
            Some(rows) => loop {
                let collected = peer.collect_step(conn, rows).await.unwrap();
                let mut seen = self.taken[i].clone();
                seen.extend(peer.outbox(conn, 1_000_000).await.unwrap().changes);
                support::midway(conn, peer, &seen)
                    .await
                    .map_err(|e| format!("replica {i}, collecting in steps of {rows}: {e}"))?;
                if !collected.more {
                    break;
                }
            },
        }
        peer.apply(conn, &sql_in).await.unwrap();
        let outbox = peer.outbox(conn, 1_000_000).await.unwrap();
        peer.acknowledge(conn, outbox.through).await.unwrap();
        self.taken[i].extend(sql_in);
        self.taken[i].extend(outbox.changes.iter().cloned());
        let sql_out = netted(&outbox.changes);

        let sim_ids = self.sims[i].gid_of.clone();
        let sql_ids = self.sql_local(i).await.gids;
        let inserted = |labels: &BTreeMap<(String, i64), u32>,
                        ids: &BTreeMap<(String, i64), Gid>| {
            labels
                .iter()
                .filter_map(|(k, l)| ids.get(k).map(|g| (*g, *l)))
                .collect::<BTreeMap<Gid, u32>>()
        };
        let (sim_new, sql_new) = (
            inserted(&self.sim_labels[i], &sim_ids),
            inserted(&self.sql_labels[i], &sql_ids),
        );
        self.sim_labels[i].retain(|k, _| !sim_ids.contains_key(k));
        self.sql_labels[i].retain(|k, _| !sql_ids.contains_key(k));
        self.sim_names
            .learn(self.schema, i, self.step, &sim_out, &sim_new);
        self.sql_names
            .learn(self.schema, i, self.step, &sql_out, &sql_new);
        let a = self.sim_names.canonical(self.schema, &sim_out);
        let b = self.sql_names.canonical(self.schema, &sql_out);
        self.sim_log
            .extend(self.sim_names.entries(self.schema, &sim_out));
        self.sql_log
            .extend(self.sql_names.entries(self.schema, &sql_out));
        if a != b {
            return Err(format!(
                "replica {i} sends differently\nsim: {a:#?}\nsql: {b:#?}"
            ));
        }
        self.compare(i).await
    }

    /// Every replica takes everything it has not seen, until nothing new is sent.
    async fn quiesce(&mut self) -> Result<(), String> {
        for _ in 0..20 {
            let before = self.sim_log.len();
            for i in 0..self.sims.len() {
                self.tick();
                self.seen[i].resize(self.sim_log.len(), false);
                let unseen: Vec<usize> = (0..self.sim_log.len())
                    .filter(|&j| !self.seen[i][j])
                    .collect();
                for &j in &unseen {
                    self.seen[i][j] = true;
                }
                self.exchange(i, &unseen).await?;
            }
            let all_seen = self
                .seen
                .iter()
                .all(|s| s.len() == self.sim_log.len() && s.iter().all(|x| *x));
            if self.sim_log.len() == before && all_seen {
                return Ok(());
            }
        }
        Err("the replicas never stopped sending".into())
    }

    async fn settle(&mut self) -> Result<(), String> {
        self.quiesce().await?;
        for i in 0..self.sims.len() {
            self.sims[i].check().map_err(|e| format!("sim: {e}"))?;
            if !self.sims[i].held.is_empty() {
                return Err(format!(
                    "sim replica {i} still holds {:?}",
                    self.sims[i].held
                ));
            }
            support::settled(&mut self.conns[i], &self.peers[i], &self.taken[i])
                .await
                .map_err(|e| format!("sql replica {i}: {e}"))?;
            if !self.last[i].snapshot.held.is_empty() {
                return Err(format!("sql replica {i} still holds something"));
            }
        }
        Ok(())
    }
}

fn run(replicas: usize, seed: u64, script: &[Op]) -> Result<(), String> {
    let registry = registry();
    let schema = registry.model_schema();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let outcome = runtime.block_on(async {
        let mut both = Both::new(&schema, seed, replicas).await;
        for (step, op) in script.iter().enumerate() {
            both.perform(op)
                .await
                .map_err(|e| format!("after step {step} {op:?}: {e}"))?;
        }
        both.settle()
            .await
            .map_err(|e| format!("while delivering everything: {e}"))
    });
    outcome.map_err(|e| format!("{e}\n\n{}", reproduction(replicas, seed, script)))
}

/// A failing case as a test to paste below.
fn reproduction(replicas: usize, seed: u64, script: &[Op]) -> String {
    let steps: Vec<String> = script.iter().map(|op| format!("        {op:?},")).collect();
    format!(
        "#[test]\nfn found() {{\n    use Op::*;\n    let script = vec![\n{}\n    ];\n    run({replicas}, {seed}, &script).unwrap();\n}}",
        steps.join("\n")
    )
}

/// A few cases unless `FONGO_DIFF_CASES` asks for more.
fn cases() -> u32 {
    std::env::var("FONGO_DIFF_CASES")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(24)
}

/// One fixed run unless `PROPTEST_RNG_SEED` asks for another.
fn rng_seed() -> RngSeed {
    match std::env::var("PROPTEST_RNG_SEED") {
        Ok(_) => ProptestConfig::default().rng_seed,
        Err(_) => RngSeed::Fixed(23),
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: cases(),
        rng_seed: rng_seed(),
        failure_persistence: None,
        ..ProptestConfig::default()
    })]
    #[test]
    fn the_sim_and_the_adapter_send_show_and_hold_the_same_at_every_step(
        replicas in 2usize..4,
        seed in any::<u64>(),
        script in proptest::collection::vec(arb_op(), 0..60)
    ) {
        if let Err(error) = run(replicas, seed, &script) {
            prop_assert!(false, "{}", error);
        }
    }
}

/// Found by a long run. One collect records a shelf written at step 3 and an author written at
/// step 5. Unless both sides stamp the rows in the order they were written, the shelf's edit takes
/// a clock after step 5 on one side and loses to another replica's step-4 write on the other.
#[test]
fn rows_collected_together_are_stamped_in_the_order_they_were_written() {
    use Op::*;
    let script = vec![
        Insert {
            at: 19,
            collection: 102,
            picks: [2, 0, 0],
        },
        Push { at: 1 },
        Edit {
            at: 29,
            collection: 94,
            row: 0,
            field: 0,
            pick: 24,
        },
        Insert {
            at: 30,
            collection: 122,
            picks: [80, 0, 0],
        },
        Insert {
            at: 55,
            collection: 152,
            picks: [0, 0, 0],
        },
    ];
    run(2, 0, &script).unwrap();
}

/// Found by a long run: a tag's key changes from `b` to `a` and a new tag takes `b` before the
/// next collect. Both sides send a re-creation of `(book, b)`; the sim also its tombstone.
#[test]
fn a_natural_key_one_row_leaves_and_another_takes_in_one_collect() {
    use Op::*;
    let script = vec![
        Insert {
            at: 1,
            collection: 70,
            picks: [0, 0, 0],
        },
        Insert {
            at: 187,
            collection: 185,
            picks: [0, 0, 0],
        },
        Insert {
            at: 55,
            collection: 11,
            picks: [0, 157, 0],
        },
        Deliver {
            at: 7,
            seed: 0,
            reverse: false,
            twice: false,
        },
        Edit {
            at: 31,
            collection: 3,
            row: 0,
            field: 181,
            pick: 20,
        },
        Insert {
            at: 31,
            collection: 123,
            picks: [0, 125, 0],
        },
    ];
    run(2, 0, &script).unwrap();
}

/// Found by a long run. A tag's key goes from `b` to `a` and back to `b` before the next collect,
/// which sends nothing: the adapter once sent the entity anew.
#[test]
fn a_natural_key_that_changes_and_changes_back_before_a_collect() {
    use Op::*;
    let script = vec![
        Insert {
            at: 1,
            collection: 70,
            picks: [0, 0, 0],
        },
        Insert {
            at: 187,
            collection: 185,
            picks: [0, 0, 0],
        },
        Insert {
            at: 55,
            collection: 11,
            picks: [0, 157, 0],
        },
        Deliver {
            at: 7,
            seed: 0,
            reverse: false,
            twice: false,
        },
        Insert {
            at: 55,
            collection: 73,
            picks: [0, 0, 0],
        },
        Edit {
            at: 31,
            collection: 3,
            row: 0,
            field: 181,
            pick: 20,
        },
        Insert {
            at: 47,
            collection: 55,
            picks: [151, 29, 0],
        },
        Insert {
            at: 115,
            collection: 179,
            picks: [29, 74, 0],
        },
        Edit {
            at: 155,
            collection: 19,
            row: 198,
            field: 190,
            pick: 31,
        },
    ];
    run(2, 0, &script).unwrap();
}

/// Replica 0 changes a tag's key away and back before collecting while replica 1 writes its note:
/// the note stays, since replica 0 made no edit to the tag.
#[test]
fn a_key_changed_away_and_back_does_not_overwrite_a_concurrent_note() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let world = World::new(0);
        let mut peers = Vec::new();
        for n in 0..2 {
            let mut conn = world.connect(n).await;
            for sql in SCHEMA {
                sqlx::query(sql).execute(&mut conn).await.unwrap();
            }
            let replica = Replica::new(registry());
            replica.install(&mut conn).await.unwrap();
            peers.push((conn, replica));
        }
        async fn run(peer: &mut (SqliteConnection, Replica), sql: &str) -> Vec<Change> {
            for s in sql.split(';').filter(|s| !s.trim().is_empty()) {
                sqlx::query(s).execute(&mut peer.0).await.unwrap();
            }
            peer.1.collect(&mut peer.0).await.unwrap();
            let outbox = peer.1.outbox(&mut peer.0, 1000).await.unwrap();
            peer.1
                .acknowledge(&mut peer.0, outbox.through)
                .await
                .unwrap();
            outbox.changes
        }
        let made = run(
            &mut peers[0],
            "INSERT INTO shelves (name) VALUES ('s'); \
             INSERT INTO books (shelf_id, title) VALUES (1, 't'); \
             INSERT INTO tags (book_id, tag) VALUES (1, 'b')",
        )
        .await;
        let (conn, replica) = &mut peers[1];
        replica.apply(conn, &made).await.unwrap();
        world.later(1000);
        let noted = run(&mut peers[1], "UPDATE tags SET note = 'n'").await;
        world.later(1000);
        let moved = run(
            &mut peers[0],
            "UPDATE tags SET tag = 'a'; UPDATE tags SET tag = 'b'",
        )
        .await;
        let (conn, replica) = &mut peers[0];
        replica.apply(conn, &noted).await.unwrap();
        let (conn, replica) = &mut peers[1];
        replica.apply(conn, &moved).await.unwrap();
        for (conn, _) in &mut peers {
            let notes = support::rows(conn, "SELECT note FROM tags", &[]).await;
            assert_eq!(notes, vec![vec![Value::Text("n".into())]], "sent {moved:?}");
        }
    });
}

/// A tag removed and made again at the same key is a re-creation, and sends the NULL it left.
#[test]
fn a_natural_row_removed_and_made_again_sends_the_fields_it_left_null() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let world = World::new(0);
        let mut conn = world.connect(0).await;
        for sql in SCHEMA {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }
        let replica = Replica::new(registry());
        replica.install(&mut conn).await.unwrap();
        for sql in [
            "INSERT INTO shelves (name) VALUES ('s')",
            "INSERT INTO books (shelf_id, title) VALUES (1, 't')",
            "INSERT INTO tags (book_id, tag, note) VALUES (1, 'b', 'n')",
        ] {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }
        replica.collect(&mut conn).await.unwrap();
        let first = replica.outbox(&mut conn, 1000).await.unwrap();
        replica.acknowledge(&mut conn, first.through).await.unwrap();
        world.later(1000);
        for sql in [
            "DELETE FROM tags",
            "INSERT INTO tags (book_id, tag) VALUES (1, 'b')",
        ] {
            sqlx::query(sql).execute(&mut conn).await.unwrap();
        }
        replica.collect(&mut conn).await.unwrap();
        let sent = replica.outbox(&mut conn, 1000).await.unwrap().changes;
        let fields: Vec<(&str, &Value)> =
            sent.iter().map(|c| (c.field.as_str(), &c.value)).collect();
        assert!(fields.contains(&("$deleted", &Value::Int(0))), "{fields:?}");
        assert!(fields.contains(&("note", &Value::Null)), "{fields:?}");
    });
}

/// Found by a long run (seed 23); a limit of this harness. Two books minted in one collect whose
/// rows a merge moved in the same step are named by rank, and the two sides mint in different
/// orders, so the names cross.
#[test]
#[ignore = "harness: gids named by rank cross when both sides mint in different orders"]
fn two_books_minted_together_and_named_by_rank() {
    use Op::*;
    let script = vec![
        Insert {
            at: 0,
            collection: 0,
            picks: [0, 0, 0],
        },
        Insert {
            at: 108,
            collection: 0,
            picks: [0, 0, 0],
        },
        Insert {
            at: 94,
            collection: 190,
            picks: [0, 0, 0],
        },
        Deliver {
            at: 28,
            seed: 0,
            reverse: false,
            twice: false,
        },
        Insert {
            at: 0,
            collection: 0,
            picks: [0, 0, 0],
        },
        Deliver {
            at: 80,
            seed: 149553245618935222,
            reverse: false,
            twice: false,
        },
        Delete {
            at: 176,
            collection: 118,
            row: 0,
        },
        Insert {
            at: 20,
            collection: 0,
            picks: [0, 0, 0],
        },
        Insert {
            at: 53,
            collection: 0,
            picks: [0, 0, 0],
        },
        Insert {
            at: 0,
            collection: 0,
            picks: [0, 0, 0],
        },
        Insert {
            at: 0,
            collection: 0,
            picks: [0, 0, 0],
        },
        Insert {
            at: 0,
            collection: 0,
            picks: [0, 0, 0],
        },
        Insert {
            at: 40,
            collection: 105,
            picks: [0, 8, 0],
        },
        Insert {
            at: 79,
            collection: 61,
            picks: [0, 0, 0],
        },
        Edit {
            at: 7,
            collection: 129,
            row: 74,
            field: 0,
            pick: 0,
        },
        Push { at: 29 },
    ];
    run(3, 0, &script).unwrap();
}

/// Found by a long run (seed 7); a limit of this harness. Two shelves from one replica end in one
/// group, whose canonical gid is the lower raw gid; the sides mint them in different orders, so
/// the canonical member differs though both agree with the model.
#[test]
#[ignore = "harness: a group's canonical member follows raw gid order, which renaming does not keep"]
fn a_group_of_two_shelves_from_one_replica() {
    use Op::*;
    let script = vec![
        Insert {
            at: 0,
            collection: 212,
            picks: [21, 0, 0],
        },
        Insert {
            at: 0,
            collection: 144,
            picks: [1, 0, 0],
        },
        Insert {
            at: 0,
            collection: 4,
            picks: [198, 0, 0],
        },
        Insert {
            at: 0,
            collection: 71,
            picks: [0, 0, 0],
        },
        Insert {
            at: 0,
            collection: 52,
            picks: [108, 0, 0],
        },
        Insert {
            at: 0,
            collection: 92,
            picks: [22, 0, 0],
        },
        Insert {
            at: 62,
            collection: 1,
            picks: [0, 0, 0],
        },
        Insert {
            at: 0,
            collection: 53,
            picks: [0, 0, 0],
        },
        Insert {
            at: 0,
            collection: 20,
            picks: [94, 0, 0],
        },
        Insert {
            at: 43,
            collection: 14,
            picks: [0, 0, 0],
        },
        Insert {
            at: 0,
            collection: 116,
            picks: [43, 0, 0],
        },
        Insert {
            at: 109,
            collection: 42,
            picks: [119, 0, 0],
        },
        Edit {
            at: 0,
            collection: 0,
            row: 61,
            field: 0,
            pick: 0,
        },
        Edit {
            at: 47,
            collection: 142,
            row: 44,
            field: 0,
            pick: 175,
        },
        Push { at: 6 },
        Push { at: 151 },
        Deliver {
            at: 20,
            seed: 12509316771714229147,
            reverse: false,
            twice: false,
        },
        Edit {
            at: 126,
            collection: 146,
            row: 0,
            field: 0,
            pick: 136,
        },
    ];
    run(2, 0, &script).unwrap();
}

/// Replica 1 removes a tag; later replica 0 removes it and makes it again at the same key. The
/// later write is a re-creation and wins: the tag survives on both.
#[test]
fn a_natural_row_removed_and_made_again_outlives_an_earlier_removal_elsewhere() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let world = World::new(0);
        let mut peers = Vec::new();
        for n in 0..2 {
            let mut conn = world.connect(n).await;
            for sql in SCHEMA {
                sqlx::query(sql).execute(&mut conn).await.unwrap();
            }
            let replica = Replica::new(registry());
            replica.install(&mut conn).await.unwrap();
            peers.push((conn, replica));
        }
        async fn run(peer: &mut (SqliteConnection, Replica), sql: &str) -> Vec<Change> {
            for s in sql.split(';').filter(|s| !s.trim().is_empty()) {
                sqlx::query(s).execute(&mut peer.0).await.unwrap();
            }
            peer.1.collect(&mut peer.0).await.unwrap();
            let outbox = peer.1.outbox(&mut peer.0, 1000).await.unwrap();
            peer.1
                .acknowledge(&mut peer.0, outbox.through)
                .await
                .unwrap();
            outbox.changes
        }
        let made = run(
            &mut peers[0],
            "INSERT INTO shelves (name) VALUES ('s'); \
             INSERT INTO books (shelf_id, title) VALUES (1, 't'); \
             INSERT INTO tags (book_id, tag, note) VALUES (1, 'b', 'n')",
        )
        .await;
        let (conn, replica) = &mut peers[1];
        replica.apply(conn, &made).await.unwrap();
        world.later(1000);
        let removed = run(&mut peers[1], "DELETE FROM tags").await;
        world.later(1000);
        let again = run(
            &mut peers[0],
            "DELETE FROM tags; INSERT INTO tags (book_id, tag, note) VALUES (1, 'b', 'n')",
        )
        .await;
        let (conn, replica) = &mut peers[0];
        replica.apply(conn, &removed).await.unwrap();
        let (conn, replica) = &mut peers[1];
        replica.apply(conn, &again).await.unwrap();
        for (conn, _) in &mut peers {
            let tags = support::rows(conn, "SELECT tag, note FROM tags", &[]).await;
            let want = vec![vec![Value::Text("b".into()), Value::Text("n".into())]];
            assert_eq!(tags, want, "sent {again:?}");
        }
    });
}

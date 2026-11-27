//! The files a replica's state refers to, checked against the protocol's definition computed
//! from the changes it holds: for each collection, gid and field marked as naming files, the
//! winner among every change held to it, whatever the row's liveness or group.
//!
//! Under the replica's own registry the files its declared fields refer to must be exactly
//! those, with the files its rows held back and kept as its own besides. Everything it reports
//! must be among the references the changes it holds carry, and under any later registry the
//! definition's files must all be reported: a replica never reports as unreferred a file that a
//! field it cannot interpret refers to.

use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_sqlite::core::{Change, Gid, Hlc, Strategy, Value, wins};
use fongo_sync_sqlite::{ContentHash, FileChanges, FileRef, FilesFrom, Registry, Replica};
use sqlx::SqliteConnection;

use super::{key_columns, quoted, rows};

/// Contents, by hash: a server names a file by its hash alone, so the questions are about
/// hashes.
pub type Files = BTreeSet<ContentHash>;

/// What a replica reports: every file, and those a declared field among the changes held or a
/// row held back names.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reported {
    pub all: Files,
    pub named: Files,
}

fn bytes(value: &Value) -> Option<ContentHash> {
    match value {
        Value::Blob(b) => FileRef::decode(b).ok().map(|f| f.hash),
        _ => None,
    }
}

/// Every reference any of `changes` carries, in any field.
pub fn carried(changes: &[Change]) -> Files {
    changes.iter().filter_map(|c| bytes(&c.value)).collect()
}

/// The files `changes` refer to under `registry`, by the protocol's definition.
pub fn referred(registry: &Registry, changes: &[Change]) -> Files {
    let mut winners: BTreeMap<(&str, Gid, &str), (&Value, &Hlc)> = BTreeMap::new();
    for c in changes {
        if !c.clock.encodable() || !registry.is_file(&c.collection, &c.field) {
            continue;
        }
        let slot = (c.collection.as_str(), c.gid, c.field.as_str());
        let held = winners.get(&slot).copied();
        if wins(Strategy::Lww, held, &c.value, &c.clock) {
            winners.insert(slot, (&c.value, &c.clock));
        }
    }
    winners.values().filter_map(|(v, _)| bytes(v)).collect()
}

/// Pages through `files` `limit` at a time, requiring the values held listed once each and in
/// order, with one answer of whether it is declared for every reference to one content, then
/// what rows held back show, all declared. The answer for a content is whether either run
/// declares it.
pub async fn listed(
    conn: &mut SqliteConnection,
    replica: &Replica,
    limit: usize,
) -> Result<BTreeMap<ContentHash, bool>, String> {
    let mut out: BTreeMap<ContentHash, bool> = BTreeMap::new();
    let mut held: BTreeSet<ContentHash> = BTreeSet::new();
    let mut from: Option<FilesFrom> = None;
    let mut last: Option<Vec<u8>> = None;
    let mut values = true;
    loop {
        let page = replica
            .files(conn, from.as_ref(), limit)
            .await
            .map_err(|e| e.to_string())?;
        if page.held_back {
            values = false;
            for r in &page.files {
                if !r.declared {
                    return Err("a row held back listed as not declared".into());
                }
                held.insert(r.file.hash);
            }
        } else {
            if !values {
                return Err("values listed after rows held back".into());
            }
            if page.files.len() > limit {
                return Err(format!("a page of {limit} held {}", page.files.len()));
            }
            for r in &page.files {
                let b = r.file.encode();
                if last.as_ref().is_some_and(|l| *l >= b) {
                    return Err("files listed out of order or twice".into());
                }
                last = Some(b.clone());
                if out
                    .insert(r.file.hash, r.declared)
                    .is_some_and(|d| d != r.declared)
                {
                    return Err("two references to one content listed as declared and not".into());
                }
            }
        }
        match page.next {
            Some(next) if from.as_ref().is_some_and(|f| next <= *f) => {
                return Err("a page's cursor did not move on".into());
            }
            Some(next) => from = Some(next),
            None => break,
        }
    }
    for hash in held {
        out.insert(hash, true);
    }
    Ok(out)
}

/// The files in fields marked as naming files of the rows at `keys` of each collection.
async fn shown(
    conn: &mut SqliteConnection,
    replica: &Replica,
    keys: &BTreeSet<(String, String)>,
    only: Option<&BTreeSet<(String, String, String)>>,
) -> Files {
    let mut out = Files::new();
    for (collection, key) in keys {
        let Some(c) = replica
            .registry()
            .collections()
            .iter()
            .find(|c| c.name == *collection)
        else {
            continue;
        };
        let columns: Vec<String> = key_columns(c).iter().map(|k| quoted(k)).collect();
        for f in &c.fields {
            if !replica.registry().is_file(&c.name, &f.name)
                || only.is_some_and(|o| {
                    !o.contains(&(collection.clone(), key.clone(), f.name.clone()))
                })
            {
                continue;
            }
            let sql = format!(
                "SELECT {} FROM {} WHERE json_array({}) = ?1",
                quoted(&f.name),
                quoted(&c.table),
                columns.join(", ")
            );
            for row in rows(conn, &sql, &[Value::Text(key.clone())]).await {
                out.extend(bytes(&row[0]));
            }
        }
    }
    out
}

/// What the replica reports, checked against the definition over `seen`, the changes it holds,
/// under its registry and under each of `later`.
pub async fn check(
    conn: &mut SqliteConnection,
    replica: &Replica,
    seen: &[Change],
    later: &[Registry],
) -> Result<Reported, String> {
    let listed = listed(conn, replica, 3).await?;
    let all: Files = listed.keys().cloned().collect();
    let declared: Files = listed
        .iter()
        .filter(|(_, d)| **d)
        .map(|(b, _)| *b)
        .collect();
    let claimed: Files = replica
        .files_to_claim(conn)
        .await
        .map_err(|e| e.to_string())?
        .iter()
        .map(|f| f.hash)
        .collect();
    let inspection = replica.inspect(conn).await.map_err(|e| e.to_string())?;
    let pinned_cells: BTreeSet<(String, String, String)> =
        inspection.pinned.iter().cloned().collect();
    let pinned_rows: BTreeSet<(String, String)> = pinned_cells
        .iter()
        .map(|(c, k, _)| (c.clone(), k.clone()))
        .collect();
    let pinned = shown(conn, replica, &pinned_rows, Some(&pinned_cells)).await;
    let mut held_rows: BTreeSet<(String, String)> = inspection.leftover.iter().cloned().collect();
    for (c, g) in &inspection.held {
        if let Some(key) = replica
            .key_for_gid(conn, c, *g)
            .await
            .map_err(|e| e.to_string())?
        {
            held_rows.insert((c.clone(), key));
        }
    }
    let shown_held = shown(conn, replica, &held_rows, None).await;
    let exact = referred(replica.registry(), seen);
    let name = |f: &Files| -> Vec<String> { f.iter().map(|h| format!("{h:?}")).collect() };
    let mut shown_apart = shown_held.clone();
    shown_apart.extend(pinned.iter().copied());
    if !claimed.is_subset(&shown_apart) || !pinned.is_subset(&claimed) {
        return Err(format!(
            "files_to_claim names {:?}, rows held back show {:?} and fields kept {:?}",
            name(&claimed),
            name(&shown_held),
            name(&pinned)
        ));
    }
    let held: Files = claimed.intersection(&shown_held).copied().collect();
    if !exact.is_subset(&declared) {
        return Err(format!(
            "the changes held refer to {:?}, declared as referred are {:?}",
            name(&exact),
            name(&declared)
        ));
    }
    let mut named = exact.clone();
    named.extend(held.iter().cloned());
    let mut beside = named.clone();
    beside.extend(pinned.iter().cloned());
    if declared != beside {
        return Err(format!(
            "declared as referred are {:?}, the changes held, rows held back and fields kept refer to {:?}",
            name(&declared),
            name(&beside)
        ));
    }
    let mut possible = beside;
    possible.extend(carried(seen));
    if !all.is_subset(&possible) {
        return Err(format!(
            "reported {:?}, of which some no change held carries: {:?}",
            name(&all),
            name(&possible)
        ));
    }
    for registry in later {
        let theirs = referred(registry, seen);
        if !theirs.is_subset(&all) {
            return Err(format!(
                "under a later registry the changes held refer to {:?}, but only {:?} are reported",
                name(&theirs),
                name(&all)
            ));
        }
    }
    let mut asked: Vec<ContentHash> = carried(seen).into_iter().collect();
    asked.push(FileRef::of(b"held by nobody").hash);
    let answers = replica
        .refers(conn, &asked)
        .await
        .map_err(|e| e.to_string())?;
    for (h, refers) in asked.iter().zip(answers) {
        if refers != all.contains(h) {
            return Err(format!(
                "refers says {refers} of {h:?}, the list says otherwise"
            ));
        }
    }
    Ok(Reported { all, named })
}

/// What an operation reported doing to the files, against what was reported before and after
/// it: it adds exactly the files a declared field or row held back newly names, drops only files
/// no longer referred to, and drops every file that a declared field or row held back named
/// before and nothing names after. With `brought`, the references the operation's changes carry,
/// adding changes makes the state refer to a file only through a change that sets a reference
/// to it.
pub fn reported(
    before: &Reported,
    after: &Reported,
    did: &FileChanges,
    brought: Option<&Files>,
) -> Result<(), String> {
    let added: Files = did.added.iter().map(|f| f.hash).collect();
    let dropped: Files = did.dropped.iter().copied().collect();
    let newly: Files = after.named.difference(&before.named).cloned().collect();
    if added != newly {
        return Err(format!(
            "reported {} added, {} newly declared as referred",
            added.len(),
            newly.len()
        ));
    }
    let gone: Files = before.all.difference(&after.all).cloned().collect();
    if !dropped.is_subset(&gone) {
        return Err("reported dropped a file still referred to, or never referred".into());
    }
    let owed: Files = before
        .named
        .iter()
        .filter(|b| !after.all.contains(*b))
        .cloned()
        .collect();
    if !owed.is_subset(&dropped) {
        return Err(format!(
            "{} files no longer referred to were not reported dropped",
            owed.difference(&dropped).count()
        ));
    }
    if let Some(brought) = brought {
        let new: Files = after.all.difference(&before.all).cloned().collect();
        if !new.is_subset(brought) {
            return Err("the state came to refer to a file no change brought".into());
        }
    }
    Ok(())
}

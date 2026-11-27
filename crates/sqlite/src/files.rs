//! The files the changes a replica holds name, read from the facts by the indexes of values that
//! may be file references (see [`crate::store::TABLES`]). Nothing here decides anything a merge
//! decides: what refers to a file is read from what merging kept.

use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_core::{Change, DELETED, Gid, Strategy, Value, wins};
use fongo_sync_protocol::FileRef;
use sqlx::SqliteConnection;

use crate::db::{Db, as_gid, as_text, loaded};
use crate::error::{Error, Result};
use crate::registry::{Collection, Kind, file_of};
use crate::replica::{Ctx, Replica, rows_of, with_db};
use crate::store::{Facts, MAY_NAME_A_FILE, as_clock, state};
use crate::tables::read_row;

/// What an operation did to the files this replica refers to, each list in the order of the
/// references' encoded bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileChanges {
    /// Files that a field declared as naming files refers to after the operation, among the
    /// changes held or in a row held back, and did not before it: files to fetch. A file a field
    /// kept as this replica's own on resuming showed already may be among them.
    pub added: Vec<FileRef>,
    /// Files that the state referred to before the operation, as [`Replica::refers`] counts, and
    /// does not after it: files to consider retiring. Every such file that a field declared as
    /// naming files or a row held back named before the operation is reported; one that only
    /// data this registry cannot interpret, or a field kept as this replica's own, named may not
    /// be.
    pub dropped: Vec<FileRef>,
}

/// One file of [`Replica::files`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Referred {
    pub file: FileRef,
    /// Whether a field this registry declares as naming files, among the changes held or in a
    /// row held back or kept as this replica's own, names it; otherwise only data this registry
    /// cannot interpret may.
    pub declared: bool,
}

/// Where a page of [`Replica::files`] ended, to ask for the next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesFrom(Vec<u8>);

/// A page of [`Replica::files`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FilePage {
    pub files: Vec<Referred>,
    /// Where to ask for the next page; `None` once there are no more.
    pub next: Option<FilesFrom>,
}

/// Where a run of [`Replica::purge_step`] stopped, to go on from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PurgeFrom {
    collection: String,
    gid: Gid,
}

/// What one step of [`Replica::purge_step`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Purged {
    /// Deleted entities the step looked at.
    pub examined: usize,
    /// File fields it set to NULL, each a change in the outbox.
    pub cleared: usize,
    /// Where to go on from; `None` once every deleted entity has been looked at.
    pub next: Option<PurgeFrom>,
    pub files: FileChanges,
}

/// Whether, when an operation first met a file, something held referred to it: a field declared
/// as naming files or a row held back (`declared`), or anything at all (`any`).
#[derive(Debug, Clone, Copy)]
struct Seen {
    declared: bool,
    any: bool,
}

/// The files an operation met, each with what referred to it before the operation touched it.
#[derive(Default)]
pub(crate) struct FileWatch {
    seen: BTreeMap<Vec<u8>, Seen>,
}

fn bytes_of(value: &Value) -> Option<&[u8]> {
    match value {
        Value::Blob(bytes) if FileRef::decode(bytes).is_ok() => Some(bytes),
        _ => None,
    }
}

/// Whether any value held, of any collection and field, is `bytes`.
fn held_anywhere(db: &Db, bytes: &[u8]) -> Result<bool> {
    for table in [
        "fongo_shadow INDEXED BY fongo_shadow_files",
        "fongo_foreign INDEXED BY fongo_foreign_files",
    ] {
        let found = db.query(
            &format!("SELECT 1 FROM {table} WHERE value = ?1 AND {MAY_NAME_A_FILE} LIMIT 1"),
            &[Value::Blob(bytes.to_vec())],
        )?;
        if !found.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}

impl Replica {
    fn file_set(&self) -> BTreeSet<(&str, &str)> {
        self.registry.file_fields().into_iter().collect()
    }

    /// Whether the entry kept of a field declared as naming files is `bytes`, for some entity.
    fn declared_holds(&self, db: &Db, bytes: &[u8]) -> Result<bool> {
        for (collection, field) in self.registry.file_fields() {
            let found = db.query(
                "SELECT 1 FROM fongo_shadow INDEXED BY fongo_shadow_value
                  WHERE collection = ?1 AND field = ?2 AND value = ?3 AND is_ref = 0 LIMIT 1",
                &[
                    Value::Text(collection.into()),
                    Value::Text(field.into()),
                    Value::Blob(bytes.to_vec()),
                ],
            )?;
            if !found.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The files the rows of entities held back show in fields declared as naming files.
    fn held_back(&self, db: &Db) -> Result<BTreeSet<Vec<u8>>> {
        let mut out = BTreeSet::new();
        if self.registry.file_fields().is_empty() {
            return Ok(out);
        }
        for r in db.query("SELECT collection, gid FROM fongo_held", &[])? {
            let name = as_text(&r[0])?;
            let Some(idx) = self.registry.index(name) else {
                continue;
            };
            let c = self.collection(idx);
            if !c.fields.iter().any(|f| matches!(f.kind, Kind::File)) {
                continue;
            }
            for (key, _) in rows_of(db, name, as_gid(&r[1])?)? {
                self.row_files(db, c, &key, None, &mut out)?;
            }
        }
        Ok(out)
    }

    /// The files fields kept as this replica's own on resuming show, of those declared as naming
    /// files.
    fn pinned(&self, db: &Db) -> Result<BTreeSet<Vec<u8>>> {
        let mut out = BTreeSet::new();
        if self.registry.file_fields().is_empty()
            || db
                .query("SELECT 1 FROM fongo_pins LIMIT 1", &[])?
                .is_empty()
        {
            return Ok(out);
        }
        for r in db.query("SELECT collection, key, field FROM fongo_pins", &[])? {
            let (name, key, field) = (as_text(&r[0])?, as_text(&r[1])?, as_text(&r[2])?);
            if let Some(idx) = self.registry.index(name)
                && self.registry.is_file(name, field)
            {
                self.row_files(db, self.collection(idx), key, Some(field), &mut out)?;
            }
        }
        Ok(out)
    }

    /// The files the row at `key` shows in its fields declared as naming files, or in `only`.
    fn row_files(
        &self,
        db: &Db,
        c: &Collection,
        key: &str,
        only: Option<&str>,
        out: &mut BTreeSet<Vec<u8>>,
    ) -> Result<()> {
        let Some(row) = read_row(db, c, key)? else {
            return Ok(());
        };
        for f in &c.fields {
            if matches!(f.kind, Kind::File)
                && only.is_none_or(|o| o == f.name)
                && let Some(bytes) = row.get(&f.name).and_then(bytes_of)
            {
                out.insert(bytes.to_vec());
            }
        }
        Ok(())
    }

    /// Begins watching what an operation does to the files referred to, when any field names
    /// files.
    pub(crate) fn watch_files(&self, cx: &mut Ctx) -> Result<()> {
        if self.registry.file_fields().is_empty() {
            return Ok(());
        }
        let seen = self
            .held_back(cx.db)?
            .into_iter()
            .map(|bytes| {
                let seen = Seen {
                    declared: true,
                    any: true,
                };
                (bytes, seen)
            })
            .collect();
        cx.files = Some(FileWatch { seen });
        Ok(())
    }

    /// Refuses local changes that put anything but a file reference or NULL in a field that
    /// names files.
    pub(crate) fn only_files(&self, db: &Db, changes: &[Change]) -> Result<()> {
        let files = self.file_set();
        if files.is_empty() {
            return Ok(());
        }
        for change in changes {
            if files.contains(&(change.collection.as_str(), change.field.as_str()))
                && change.value != Value::Null
                && file_of(&change.value).is_none()
            {
                return Err(Error::NotAFile {
                    collection: change.collection.clone(),
                    field: change.field.clone(),
                    key: rows_of(db, &change.collection, change.gid)?
                        .into_iter()
                        .next()
                        .map(|(k, _)| k),
                });
            }
        }
        Ok(())
    }

    /// Notes, before its fact is kept, a file a change brings to a field naming files.
    pub(crate) fn saw_brought(&self, cx: &mut Ctx, change: &Change) -> Result<()> {
        let Some(watch) = &cx.files else {
            return Ok(());
        };
        let Some(bytes) = bytes_of(&change.value) else {
            return Ok(());
        };
        if watch.seen.contains_key(bytes)
            || !self.registry.is_file(&change.collection, &change.field)
        {
            return Ok(());
        }
        let declared = self.declared_holds(cx.db, bytes)?;
        let any = declared || held_anywhere(cx.db, bytes)?;
        if let Some(watch) = &mut cx.files {
            watch.seen.insert(bytes.to_vec(), Seen { declared, any });
        }
        Ok(())
    }

    /// Notes a file a kept fact replaced in a field naming files: it was referred to until then.
    pub(crate) fn saw_replaced(&self, cx: &mut Ctx, change: &Change, replaced: &Value) {
        if let (Some(watch), Some(bytes)) = (&mut cx.files, bytes_of(replaced))
            && self.registry.is_file(&change.collection, &change.field)
        {
            watch.seen.entry(bytes.to_vec()).or_insert(Seen {
                declared: true,
                any: true,
            });
        }
    }

    /// What the operation watched did to the files referred to.
    pub(crate) fn file_changes(&self, cx: &mut Ctx) -> Result<FileChanges> {
        let Some(watch) = cx.files.take() else {
            return Ok(FileChanges::default());
        };
        let db = cx.db;
        let held = self.held_back(db)?;
        let mut out = FileChanges::default();
        // Read only when a file would drop: a field kept as this replica's own may still show it.
        let mut pinned: Option<BTreeSet<Vec<u8>>> = None;
        for (bytes, before) in watch.seen {
            let declared = held.contains(&bytes) || self.declared_holds(db, &bytes)?;
            let mut any = declared || held_anywhere(db, &bytes)?;
            if before.any && !any {
                if pinned.is_none() {
                    pinned = Some(self.pinned(db)?);
                }
                any = pinned.as_ref().is_some_and(|p| p.contains(&bytes));
            }
            let file = FileRef::decode(&bytes)
                .map_err(|_| Error::Corrupt("a file reference does not decode".into()))?;
            if declared && !before.declared {
                out.added.push(file);
            }
            if before.any && !any {
                out.dropped.push(file);
            }
        }
        Ok(out)
    }

    /// The files the changes this replica holds may refer to, in pages of at most `limit`, in
    /// the order of their encoded references, each once; see [`Replica::refers`] for what counts.
    /// `from` is the [`FilePage::next`] of the page before, `None` for the first.
    ///
    /// A page reads at most `limit` distinct values in each of the two indexes of values that may
    /// name files, looks each file it returns up in every field declared as naming files, and
    /// reads the rows held back and the fields kept as this replica's own; it reads nothing else.
    /// A read that writes nothing; it answers while capture is suspended too.
    pub async fn files(
        &self,
        conn: &mut SqliteConnection,
        from: Option<&FilesFrom>,
        limit: usize,
    ) -> Result<FilePage> {
        with_db(conn, |db| {
            db.atomically(|db| {
                state(db)?.current()?;
                self.files_in(db, from, limit.max(1))
            })
        })
        .await
    }

    fn files_in(&self, db: &Db, from: Option<&FilesFrom>, limit: usize) -> Result<FilePage> {
        let after = from.map(|f| f.0.clone()).unwrap_or_default();
        let n = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut candidates: BTreeMap<Vec<u8>, bool> = BTreeMap::new();
        // Each index is read up to `limit` values: past the least last value of one that had
        // more, what the others hold may be missing from the page.
        let mut bound: Option<Vec<u8>> = None;
        for table in [
            "fongo_shadow INDEXED BY fongo_shadow_files",
            "fongo_foreign INDEXED BY fongo_foreign_files",
        ] {
            let rows = db.query(
                &format!(
                    "SELECT DISTINCT value FROM {table}
                      WHERE {MAY_NAME_A_FILE} AND value > ?1 ORDER BY value LIMIT ?2"
                ),
                &[Value::Blob(after.clone()), Value::Int(n)],
            )?;
            let cut = rows.len() == limit;
            let mut last = None;
            for r in rows {
                if let Value::Blob(bytes) = &r[0] {
                    candidates.entry(bytes.clone()).or_insert(false);
                    last = Some(bytes.clone());
                }
            }
            if cut && let Some(last) = last {
                bound = Some(bound.map_or(last.clone(), |b: Vec<u8>| b.min(last)));
            }
        }
        let mut shown = self.held_back(db)?;
        shown.extend(self.pinned(db)?);
        for bytes in shown.into_iter().filter(|b| *b > after) {
            candidates.insert(bytes, true);
        }
        let mut page = FilePage::default();
        let mut last: Option<Vec<u8>> = None;
        let mut stopped = false;
        for (bytes, shown) in candidates {
            if page.files.len() == limit || bound.as_ref().is_some_and(|b| bytes > *b) {
                stopped = true;
                break;
            }
            if let Ok(file) = FileRef::decode(&bytes) {
                let declared = shown || self.declared_holds(db, &bytes)?;
                page.files.push(Referred { file, declared });
            }
            last = Some(bytes);
        }
        page.next = match (stopped, bound) {
            (true, _) => last.map(FilesFrom),
            (false, Some(bound)) => Some(FilesFrom(bound)),
            (false, None) => None,
        };
        Ok(page)
    }

    /// Whether this replica's state may refer to each of `files`, in their order. It may when any
    /// value it holds is the file's encoded reference, whatever the collection and field and
    /// whether it is a field's winner, or a row it holds back, or a field it keeps as its own on
    /// resuming, shows the file in a field declared as naming files.
    ///
    /// That is a superset of what the protocol's definition of a set of changes referring to a
    /// file gives for the changes this replica holds (`fongo_sync_protocol::api`, "Deleting
    /// files"), under this registry and under any registry a later version of the application
    /// has, given that a field once marked as naming files stays so and merged by clock. For a
    /// field this registry declares as naming files, the entry kept is the field's winner among
    /// every change held to it. For a field it does not declare, of a collection it does, the
    /// entry kept is the winner by clock, the same as that field's winner wherever it names
    /// files. Changes to a collection it does not declare are held whole, every one counted.
    /// Rows written since the last collect are not read: collect first.
    ///
    /// Each file costs two indexed lookups; the call reads the rows held back and the fields
    /// kept as this replica's own once. A read that writes nothing; it answers while capture is
    /// suspended too.
    pub async fn refers(
        &self,
        conn: &mut SqliteConnection,
        files: &[FileRef],
    ) -> Result<Vec<bool>> {
        with_db(conn, |db| {
            db.atomically(|db| {
                state(db)?.current()?;
                let mut shown = self.held_back(db)?;
                shown.extend(self.pinned(db)?);
                files
                    .iter()
                    .map(|file| {
                        let bytes = file.encode();
                        Ok(shown.contains(&bytes) || held_anywhere(db, &bytes)?)
                    })
                    .collect()
            })
        })
        .await
    }

    /// The files that rows this replica holds back show in fields declared as naming files, in
    /// the order of their encoded references: those a device claims before each report that it
    /// has settled. Reads every row held back. A read that writes nothing.
    pub async fn held_files(&self, conn: &mut SqliteConnection) -> Result<Vec<FileRef>> {
        with_db(conn, |db| {
            db.atomically(|db| {
                state(db)?.current()?;
                Ok(self
                    .held_back(db)?
                    .iter()
                    .filter_map(|b| FileRef::decode(b).ok())
                    .collect())
            })
        })
        .await
    }

    /// Sets to NULL, as a local write that is sent as any other, each field naming files of
    /// entities deleted before `before` (milliseconds since the Unix epoch, by the clock of the
    /// winning deletion), so that they no longer refer to their files. A deleted row keeps
    /// referring to its file until then: deleting does not clear it, since what is sent and
    /// merged would change.
    ///
    /// An entity counts as deleted when its own deletion wins and its group shows no row; none
    /// of the group's members may have a row here, held back or given up. A row deleted only
    /// with its owner, which no change deleted itself, is not cleared. Should the entity come back,
    /// its fields read NULL unless written again after the clearing.
    ///
    /// A step looks at about `rows` deleted entities, of collections with fields naming files, in
    /// the order of their collections' names and their ids, starting from `from` (the
    /// [`Purged::next`] of the step before, `None` for the first), and is a transaction of its
    /// own. A run of steps until `next` is `None` looks at every deleted entity once. Its writes
    /// are merged as a collect merges them, and none needs a row written. Refused while capture
    /// is suspended.
    pub async fn purge_step(
        &self,
        conn: &mut SqliteConnection,
        before: u64,
        from: Option<&PurgeFrom>,
        rows: usize,
    ) -> Result<Purged> {
        with_db(conn, |db| {
            db.atomically_writing(|db| {
                self.ready(db)?.current()?;
                let mut cx = Ctx::begin(db)?;
                self.watch_files(&mut cx)?;
                let mut purged = self.purge_in(&mut cx, before, from, rows.max(1))?;
                purged.files = self.file_changes(&mut cx)?;
                cx.finish()?;
                Ok(purged)
            })
        })
        .await
    }

    fn purge_in(
        &self,
        cx: &mut Ctx,
        before: u64,
        from: Option<&PurgeFrom>,
        rows: usize,
    ) -> Result<Purged> {
        let db = cx.db;
        let mut collections: Vec<&Collection> = self
            .registry
            .collections()
            .iter()
            .filter(|c| c.fields.iter().any(|f| matches!(f.kind, Kind::File)))
            .collect();
        collections.sort_by(|a, b| a.name.cmp(&b.name));
        let ev = self.evaluator();
        let facts = Facts { db };
        let mut session = ev.session(&facts);
        let mut purged = Purged::default();
        let mut changes = Vec::new();
        for c in collections {
            let after = match from {
                Some(f) if f.collection > c.name => continue,
                Some(f) if f.collection == c.name => Value::Ref(f.gid),
                _ => Value::Blob(Vec::new()),
            };
            let left = rows - purged.examined;
            let deleted = db.query(
                "SELECT gid, clock FROM fongo_shadow INDEXED BY fongo_shadow_value
                  WHERE collection = ?1 AND field = ?2 AND value = 1 AND is_ref = 0 AND gid > ?3
                  ORDER BY gid LIMIT ?4",
                &[
                    Value::Text(c.name.clone()),
                    Value::Text(DELETED.into()),
                    after,
                    Value::Int(i64::try_from(left).unwrap_or(i64::MAX)),
                ],
            )?;
            purged.examined += deleted.len();
            let mut last = None;
            for r in &deleted {
                let gid = as_gid(&r[0])?;
                last = Some(gid);
                if as_clock(&r[1])?.wall_ms >= before {
                    continue;
                }
                let mut held = Vec::new();
                for e in db.query(
                    "SELECT field, value, is_ref, clock FROM fongo_shadow INDEXED BY fongo_shadow_entity
                      WHERE collection = ?1 AND gid = ?2",
                    &[Value::Text(c.name.clone()), Value::Ref(gid)],
                )? {
                    let field = as_text(&e[0])?;
                    let value = loaded(&e[1], &e[2])?;
                    if self.registry.is_file(&c.name, field) && file_of(&value).is_some() {
                        held.push((field.to_string(), value, as_clock(&e[3])?));
                    }
                }
                if held.is_empty() {
                    continue;
                }
                let Ok(target) = session.target(&c.name, gid) else {
                    continue;
                };
                if target.row.is_some() {
                    continue;
                }
                let mut members = target.members.clone();
                members.push(gid);
                let mut has_row = false;
                for m in members {
                    has_row |= !rows_of(db, &c.name, m)?.is_empty();
                }
                if has_row {
                    continue;
                }
                let clock = cx.clock.tick(cx.wall);
                for (field, value, at) in held {
                    if wins(Strategy::Lww, Some((&value, &at)), &Value::Null, &clock) {
                        changes.push(Change {
                            collection: c.name.clone(),
                            gid,
                            field,
                            value: Value::Null,
                            clock,
                        });
                    }
                }
            }
            db.check()?;
            if deleted.len() == left {
                purged.next = last.map(|gid| PurgeFrom {
                    collection: c.name.clone(),
                    gid,
                });
                break;
            }
        }
        drop(session);
        purged.cleared = changes.len();
        // The entities cleared show no row and have none, so no row is brought up to date.
        self.merge(cx, &changes, true)?;
        Ok(purged)
    }
}

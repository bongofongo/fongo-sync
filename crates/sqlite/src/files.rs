//! The files the changes a replica holds name, read from the facts by the indexes of values that
//! may be file references (see [`crate::store::TABLES`]). Nothing here decides anything a merge
//! decides: what refers to a file is read from what merging kept.
//!
//! A server stores a file under a name derived from its content hash alone, so every answer here
//! is about a content hash: a file is referred to while any value held names that hash, at any
//! size.

use std::collections::{BTreeMap, BTreeSet};

use fongo_sync_core::{Change, DELETED, Gid, Strategy, Value, wins};
use fongo_sync_protocol::{ContentHash, FILE_REF_FORMAT, FileRef};
use sqlx::SqliteConnection;

use crate::db::{Db, as_gid, as_text, loaded};
use crate::error::{Error, Result};
use crate::registry::{Collection, Key, Kind, file_of};
use crate::replica::{Ctx, Replica, rows_of, with_db};
use crate::store::{Facts, MAY_NAME_A_FILE, as_clock, state};
use crate::tables::read_row;

/// What an operation did to the files this replica refers to, as [`Replica::refers`] counts them,
/// by content hash.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileChanges {
    /// References to contents that a field declared as naming files, among the changes held or
    /// in a row held back, names after the operation and none did before it: files to fetch.
    /// Every reference the operation met to such a content is listed, in the order of their
    /// encoded bytes, so one content met at two sizes is listed twice. A content a field kept as
    /// this replica's own on resuming showed already may be among them.
    pub added: Vec<FileRef>,
    /// Contents that the state referred to before the operation, at any size, and does not after
    /// it: files to consider retiring. Every such content that a field declared as naming files
    /// or a row held back named before the operation is reported; one that only data this
    /// registry cannot interpret, or a field kept as this replica's own, named may not be.
    pub dropped: Vec<ContentHash>,
}

/// One reference of [`Replica::files`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Referred {
    pub file: FileRef,
    /// Whether a field this registry declares as naming files, among the changes held or in a
    /// row held back or kept as this replica's own, names this content at any size; otherwise
    /// only data this registry cannot interpret may.
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

/// What, when an operation first met a content, referred to it: a field declared as naming files
/// or a row held back (`declared`), or anything at all (`any`); and the references to it met.
#[derive(Debug, Clone)]
struct Seen {
    declared: bool,
    any: bool,
    met: BTreeSet<Vec<u8>>,
}

/// The contents an operation met, each with what referred to it before the operation touched it,
/// and the fields of the registry of the operation, which an adoption under an older registry
/// still answers to.
pub(crate) struct FileWatch {
    fields: BTreeSet<(String, String)>,
    seen: BTreeMap<ContentHash, Seen>,
}

impl FileWatch {
    fn names_files(&self, change: &Change) -> bool {
        self.fields
            .contains(&(change.collection.clone(), change.field.clone()))
    }
}

/// The content a value names, if it is an encoded [`FileRef`].
fn hash_of(value: &Value) -> Option<ContentHash> {
    file_of(value).map(|f| f.hash)
}

/// The encoded references to `hash`, of any size, are those from the first value to before the
/// second.
fn span(hash: &ContentHash) -> (Value, Value) {
    let mut from = Vec::with_capacity(33);
    from.push(FILE_REF_FORMAT);
    from.extend_from_slice(&hash.0);
    let mut to = from.clone();
    for b in to.iter_mut().rev() {
        let (next, carry) = b.overflowing_add(1);
        *b = next;
        if !carry {
            break;
        }
    }
    (Value::Blob(from), Value::Blob(to))
}

/// The values read of one content before the answer is taken to be yes: a value that begins
/// as a reference to it and is not one is kept only where something wrote it on purpose.
const AT_MOST: i64 = 16;

/// Whether `rows`, values that begin as references to one content, name it: one of them is a
/// reference, or there are too many to tell.
fn names(rows: &[Vec<Value>]) -> bool {
    rows.len() as i64 >= AT_MOST || rows.iter().any(|r| file_of(&r[0]).is_some())
}

/// Whether any value held, of any collection and field, names `hash`.
fn held_anywhere(db: &Db, hash: &ContentHash) -> Result<bool> {
    let (from, to) = span(hash);
    for table in [
        "fongo_shadow INDEXED BY fongo_shadow_files",
        "fongo_foreign INDEXED BY fongo_foreign_files",
    ] {
        let found = db.query(
            &format!(
                "SELECT DISTINCT value FROM {table}
                  WHERE value >= ?1 AND value < ?2 AND {MAY_NAME_A_FILE} LIMIT {AT_MOST}"
            ),
            &[from.clone(), to.clone()],
        )?;
        if names(&found) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether the entry kept of one of `fields` names `hash`, for some entity.
fn declared_holds<'f>(
    db: &Db,
    fields: impl IntoIterator<Item = (&'f str, &'f str)>,
    hash: &ContentHash,
) -> Result<bool> {
    let (from, to) = span(hash);
    for (collection, field) in fields {
        let found = db.query(
            &format!(
                "SELECT DISTINCT value FROM fongo_shadow INDEXED BY fongo_shadow_value
                  WHERE collection = ?1 AND field = ?2 AND value >= ?3 AND value < ?4
                    AND is_ref = 0 LIMIT {AT_MOST}"
            ),
            &[
                Value::Text(collection.into()),
                Value::Text(field.into()),
                from.clone(),
                to.clone(),
            ],
        )?;
        if names(&found) {
            return Ok(true);
        }
    }
    Ok(false)
}

const PINNED: &str = "shown >= x'01' AND shown < x'02'";

impl Replica {
    fn file_set(&self) -> BTreeSet<(String, String)> {
        self.registry
            .file_fields()
            .into_iter()
            .map(|(c, f)| (c.to_string(), f.to_string()))
            .collect()
    }

    /// Whether a field kept as this replica's own on resuming, declared as naming files, shows
    /// `hash`.
    fn pinned_holds(&self, db: &Db, hash: &ContentHash) -> Result<bool> {
        let (from, to) = span(hash);
        let mut read = 0;
        for r in db.query(
            &format!(
                "SELECT shown, collection, field FROM fongo_pins INDEXED BY fongo_pins_files
                  WHERE shown >= ?1 AND shown < ?2 AND {PINNED} LIMIT {AT_MOST}"
            ),
            &[from, to],
        )? {
            read += 1;
            if file_of(&r[0]).is_some() && self.registry.is_file(as_text(&r[1])?, as_text(&r[2])?) {
                return Ok(true);
            }
        }
        Ok(read >= AT_MOST)
    }

    /// Calls `each` with every reference the rows of entities held back show in fields declared
    /// as naming files. Reads every row held back.
    fn held_back(&self, db: &Db, mut each: impl FnMut(Vec<u8>)) -> Result<()> {
        if self.registry.file_fields().is_empty() {
            return Ok(());
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
                let Some(row) = read_row(db, c, &key)? else {
                    continue;
                };
                for f in c.fields.iter().filter(|f| matches!(f.kind, Kind::File)) {
                    if let Some(file) = row.get(&f.name).and_then(file_of) {
                        each(file.encode());
                    }
                }
            }
        }
        Ok(())
    }

    fn held_hashes(&self, db: &Db) -> Result<BTreeSet<ContentHash>> {
        let mut out = BTreeSet::new();
        self.held_back(db, |b| {
            if let Ok(f) = FileRef::decode(&b) {
                out.insert(f.hash);
            }
        })?;
        Ok(out)
    }

    /// Begins watching what an operation does to the files referred to, when any field names
    /// files. Every operation that merges begins so: a local write is checked against the
    /// fields this registry declares as naming files only while it watches.
    pub(crate) fn watch_files(&self, cx: &mut Ctx) -> Result<()> {
        let fields = self.file_set();
        if fields.is_empty() {
            return Ok(());
        }
        let seen = self
            .held_hashes(cx.db)?
            .into_iter()
            .map(|hash| {
                let seen = Seen {
                    declared: true,
                    any: true,
                    met: BTreeSet::new(),
                };
                (hash, seen)
            })
            .collect();
        cx.files = Some(FileWatch { fields, seen });
        Ok(())
    }

    /// Refuses local changes that put anything but a file reference or NULL in a field the
    /// operation's registry declares as naming files.
    pub(crate) fn only_files(&self, cx: &Ctx, changes: &[Change]) -> Result<()> {
        let Some(watch) = &cx.files else {
            return Ok(());
        };
        for change in changes {
            if watch.names_files(change)
                && change.value != Value::Null
                && file_of(&change.value).is_none()
            {
                return Err(Error::NotAFile {
                    collection: change.collection.clone(),
                    field: change.field.clone(),
                    key: rows_of(cx.db, &change.collection, change.gid)?
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
        let (Some(file), true) = (file_of(&change.value), watch.names_files(change)) else {
            return Ok(());
        };
        if !watch.seen.contains_key(&file.hash) {
            let fields = watch.fields.iter().map(|(c, f)| (c.as_str(), f.as_str()));
            let declared = declared_holds(cx.db, fields, &file.hash)?;
            let any = declared
                || held_anywhere(cx.db, &file.hash)?
                || self.pinned_holds(cx.db, &file.hash)?;
            let seen = Seen {
                declared,
                any,
                met: BTreeSet::new(),
            };
            if let Some(watch) = &mut cx.files {
                watch.seen.insert(file.hash, seen);
            }
        }
        if let Some(seen) = cx.files.as_mut().and_then(|w| w.seen.get_mut(&file.hash)) {
            seen.met.insert(file.encode());
        }
        Ok(())
    }

    /// Notes a file a kept fact replaced in a field naming files: it was referred to until then.
    pub(crate) fn saw_replaced(&self, cx: &mut Ctx, change: &Change, replaced: &Value) {
        if let (Some(watch), Some(hash)) = (&mut cx.files, hash_of(replaced))
            && watch.names_files(change)
        {
            watch.seen.entry(hash).or_insert(Seen {
                declared: true,
                any: true,
                met: BTreeSet::new(),
            });
        }
    }

    /// What the operation watched did to the files referred to.
    pub(crate) fn file_changes(&self, cx: &mut Ctx) -> Result<FileChanges> {
        let Some(watch) = cx.files.take() else {
            return Ok(FileChanges::default());
        };
        let db = cx.db;
        let held = self.held_hashes(db)?;
        let fields: Vec<(&str, &str)> = self.registry.file_fields();
        let mut out = FileChanges::default();
        for (hash, before) in watch.seen {
            let declared = held.contains(&hash) || declared_holds(db, fields.clone(), &hash)?;
            let any = declared || held_anywhere(db, &hash)? || self.pinned_holds(db, &hash)?;
            if declared && !before.declared {
                for b in &before.met {
                    if let Ok(file) = FileRef::decode(b) {
                        out.added.push(file);
                    }
                }
            }
            if before.any && !any {
                out.dropped.push(hash);
            }
        }
        out.added.sort_by_key(FileRef::encode);
        Ok(out)
    }

    /// Every reference the changes this replica holds may refer to, in pages of at most `limit`,
    /// in the order of their encoded bytes, each once; see [`Replica::refers`] for what counts.
    /// One content held at two sizes is listed twice, next to each other: whether it is referred
    /// to is a question about its hash. `from` is the [`FilePage::next`] of the page before, `None`
    /// for the first.
    ///
    /// A page reads at most `limit` values from each of the indexes of values held and fields kept
    /// as this replica's own that may name files, starting at `from`, so each page costs the same
    /// wherever it starts; looks each content it returns up in every field declared as naming
    /// files; and reads every row held back, keeping at most `limit` of what they show. The pages
    /// are not a snapshot: a page reflects the state when it is read, and what was written
    /// between pages may be listed or missed. A read that writes nothing. It answers while
    /// capture is suspended, when what the application wrote meanwhile is not known: see
    /// [`Replica::nothing_pending`].
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
        let mut candidates: BTreeSet<Vec<u8>> = BTreeSet::new();
        // Each source is read up to `limit` values: past the least last value of one that had
        // more, what the others hold may be missing from the page.
        let mut bound: Option<Vec<u8>> = None;
        let mut cut = |rows: Vec<Vec<Value>>, candidates: &mut BTreeSet<Vec<u8>>| {
            let full = rows.len() == limit;
            let mut last = None;
            for r in rows {
                if let Value::Blob(bytes) = &r[0] {
                    candidates.insert(bytes.clone());
                    last = Some(bytes.clone());
                }
            }
            if full && let Some(last) = last {
                bound = Some(bound.take().map_or(last.clone(), |b: Vec<u8>| b.min(last)));
            }
        };
        // The cursor comes first: SQLite bounds the range by the first lower bound it is given.
        for table in [
            "fongo_shadow INDEXED BY fongo_shadow_files",
            "fongo_foreign INDEXED BY fongo_foreign_files",
        ] {
            let rows = db.query(
                &format!(
                    "SELECT DISTINCT value FROM {table}
                      WHERE value > ?1 AND {MAY_NAME_A_FILE} ORDER BY value LIMIT ?2"
                ),
                &[Value::Blob(after.clone()), Value::Int(n)],
            )?;
            cut(rows, &mut candidates);
        }
        let mut pinned = Vec::new();
        for r in db.query(
            &format!(
                "SELECT shown, collection, field FROM fongo_pins INDEXED BY fongo_pins_files
                  WHERE shown > ?1 AND {PINNED} ORDER BY shown LIMIT ?2"
            ),
            &[Value::Blob(after.clone()), Value::Int(n)],
        )? {
            // Every row read bounds the page, whether or not its field names files.
            let named = self.registry.is_file(as_text(&r[1])?, as_text(&r[2])?);
            pinned.push((r, named));
        }
        let mut kept = BTreeSet::new();
        let named: BTreeSet<Vec<u8>> = pinned
            .iter()
            .filter(|(_, named)| *named)
            .filter_map(|(r, _)| match &r[0] {
                Value::Blob(b) => Some(b.clone()),
                _ => None,
            })
            .collect();
        cut(pinned.into_iter().map(|(r, _)| r).collect(), &mut kept);
        candidates.extend(kept.into_iter().filter(|b| named.contains(b)));
        // Of the rows held back, only the least `limit` past the cursor are kept.
        let mut held: BTreeSet<Vec<u8>> = BTreeSet::new();
        self.held_back(db, |b| {
            if b > after {
                held.insert(b);
                if held.len() > limit {
                    held.pop_last();
                }
            }
        })?;
        candidates.extend(held);
        let mut page = FilePage::default();
        let mut last: Option<Vec<u8>> = None;
        let mut stopped = false;
        for bytes in candidates {
            if page.files.len() == limit || bound.as_ref().is_some_and(|b| bytes > *b) {
                stopped = true;
                break;
            }
            if let Ok(file) = FileRef::decode(&bytes) {
                page.files.push(Referred {
                    file,
                    declared: false,
                });
            }
            last = Some(bytes);
        }
        let asked: BTreeSet<ContentHash> = page.files.iter().map(|r| r.file.hash).collect();
        let mut held_shown = BTreeSet::new();
        self.held_back(db, |b| {
            if let Ok(f) = FileRef::decode(&b)
                && asked.contains(&f.hash)
            {
                held_shown.insert(f.hash);
            }
        })?;
        let fields = self.registry.file_fields();
        for r in &mut page.files {
            let hash = r.file.hash;
            r.declared = held_shown.contains(&hash)
                || declared_holds(db, fields.clone(), &hash)?
                || self.pinned_holds(db, &hash)?;
        }
        page.next = match (stopped, bound) {
            (true, _) => last.map(FilesFrom),
            (false, Some(bound)) => Some(FilesFrom(bound)),
            (false, None) => None,
        };
        Ok(page)
    }

    /// Whether this replica's state may refer to each content of `hashes`, in their order. It
    /// may when any value it holds is a reference to the content, at any size, whatever the
    /// collection and field and whether it is a field's winner, or a row it holds back, or a
    /// field it keeps as its own on resuming, shows the content in a field declared as naming
    /// files.
    ///
    /// That is a superset of what the protocol's definition of a set of changes referring to a
    /// file gives for the changes this replica holds (`fongo_sync_protocol::api`, "Deleting
    /// files"), under this registry and under any registry a later version of the application
    /// has, given that a field once marked as naming files stays so in every version that writes
    /// it, merged by clock and never part of a key. For a field this registry declares as naming
    /// files, the entry kept is the field's winner among every change held to it. For a field
    /// it does not declare, of a collection it does, the entry kept is the winner by clock, the
    /// same as that field's winner wherever it names files. Changes to a collection it does not
    /// declare are held whole, every one counted.
    ///
    /// It answers about what this replica holds now: rows written since the last collect are
    /// not read, and while capture is suspended what the application wrote meanwhile is not
    /// known. A device retires a file only when [`Replica::nothing_pending`] holds.
    ///
    /// Each content costs a few indexed lookups; the call reads every row held back once. A read
    /// that writes nothing.
    pub async fn refers(
        &self,
        conn: &mut SqliteConnection,
        hashes: &[ContentHash],
    ) -> Result<Vec<bool>> {
        with_db(conn, |db| {
            db.atomically(|db| {
                state(db)?.current()?;
                let asked: BTreeSet<ContentHash> = hashes.iter().copied().collect();
                let mut shown = BTreeSet::new();
                self.held_back(db, |b| {
                    if let Ok(f) = FileRef::decode(&b)
                        && asked.contains(&f.hash)
                    {
                        shown.insert(f.hash);
                    }
                })?;
                hashes
                    .iter()
                    .map(|hash| {
                        Ok(shown.contains(hash)
                            || held_anywhere(db, hash)?
                            || self.pinned_holds(db, hash)?)
                    })
                    .collect()
            })
        })
        .await
    }

    /// The references that rows this replica holds back, and fields it keeps as its own on
    /// resuming, show in fields declared as naming files, in the order of their encoded bytes:
    /// they name files no change this replica holds may set, and a device claims them before
    /// each report that it has settled. Reads every row held back and every field kept as this
    /// replica's own that may name a file. A read that writes nothing.
    pub async fn files_to_claim(&self, conn: &mut SqliteConnection) -> Result<Vec<FileRef>> {
        with_db(conn, |db| {
            db.atomically(|db| {
                state(db)?.current()?;
                let mut out = BTreeSet::new();
                self.held_back(db, |b| {
                    out.insert(b);
                })?;
                for r in db.query(
                    &format!(
                        "SELECT shown, collection, field FROM fongo_pins INDEXED BY fongo_pins_files
                          WHERE {PINNED}"
                    ),
                    &[],
                )? {
                    if let (Value::Blob(b), true) = (
                        &r[0],
                        self.registry.is_file(as_text(&r[1])?, as_text(&r[2])?),
                    ) && FileRef::decode(b).is_ok()
                    {
                        out.insert(b.clone());
                    }
                }
                Ok(out.iter().filter_map(|b| FileRef::decode(b).ok()).collect())
            })
        })
        .await
    }

    /// Whether this replica's state is exactly the changes it has merged with nothing of its
    /// own pending: capture is not suspended, no row is written and not yet collected, no row a
    /// first install in steps found waits to be adopted, and the outbox is empty. A device may
    /// retire a file only while this holds, with `through` the last envelope it has merged, its
    /// own included; [`Replica::refers`] then answers about exactly the changes through it. A
    /// few indexed reads; a read that writes nothing.
    pub async fn nothing_pending(&self, conn: &mut SqliteConnection) -> Result<bool> {
        with_db(conn, |db| {
            db.atomically(|db| {
                if let Err(e) = self.ready(db).and_then(|s| s.current()) {
                    return match e {
                        Error::Suspended => Ok(false),
                        e => Err(e),
                    };
                }
                for sql in [
                    "SELECT 1 FROM fongo_dirty LIMIT 1",
                    "SELECT 1 FROM fongo_found WHERE batch >= 0 LIMIT 1",
                    "SELECT 1 FROM fongo_outbox LIMIT 1",
                ] {
                    if !db.query(sql, &[])?.is_empty() {
                        return Ok(false);
                    }
                }
                Ok(true)
            })
        })
        .await
    }

    /// Sets to NULL, as a local write that is sent as any other, each field naming files of
    /// minted entities deleted before `before` (milliseconds since the Unix epoch, by the clock
    /// of the winning deletion), so that they no longer refer to their files. A deleted row keeps
    /// referring to its file until then: deleting does not clear it, since what is sent and
    /// merged would change.
    ///
    /// A clearing never shows: it is written only to a member whose own deletion wins, whose
    /// group shows no row and none of whose group's members has a row here, held back or given
    /// up. Such a member never shows again, so neither does its NULL: a group shows only the
    /// fields of its live members, a member whose own deletion wins is not live while that
    /// deletion wins, and only making a row writes a member's deletion back to 0, which for a
    /// minted collection makes a new entity. A row deleted only with its owner, which no change
    /// deleted itself, is not cleared. Natural collections are not cleared at all: a row made
    /// again at the same key anywhere is the same entity again, which a clearing could empty.
    /// Their deleted rows keep their files; an application that wants them gone clears a file
    /// field itself before it deletes the row.
    ///
    /// A step looks at about `rows` deleted entities, of minted collections with fields naming
    /// files, in the order of their collections' names and their ids, starting from `from` (the
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
            .filter(|c| matches!(c.key, Key::Minted { .. }))
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

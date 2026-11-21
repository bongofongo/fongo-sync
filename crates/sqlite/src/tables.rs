//! The application's tables: reading a row by its key, the triggers that notice its writes, and
//! what its definitions say about NULL.

use std::collections::BTreeSet;

use fongo_sync_core::Value;
use fongo_sync_core::model::Row;
use fongo_sync_core::plan::Columns;

use crate::db::{Db, as_int, as_text, quoted};
use crate::error::{Error, Result};
use crate::registry::{Collection, Key, Registry};

/// The wall clock in milliseconds, as the triggers stamp writes.
/// `fongo_dirty.gone` for a key whose row moved to another key, and for one whose row was
/// removed or another row was inserted at; a removal outranks a move.
pub(crate) const MOVED: i64 = 1;
pub(crate) const REMOVED: i64 = 2;

pub(crate) const NOW: &str = "CAST(unixepoch('subsec') * 1000 AS INTEGER)";

const QUIET: &str = "WHEN COALESCE((SELECT value FROM fongo_meta WHERE key = 'applying'), 0) <> 1";

/// A row's key as the triggers write it: the JSON array of its key columns.
pub(crate) fn key_json(c: &Collection, row: &str) -> String {
    let parts: Vec<String> = c
        .key_columns()
        .iter()
        .map(|k| format!("{row}{}", quoted(k)))
        .collect();
    format!("json_array({})", parts.join(", "))
}

/// The condition that picks a row by its key, bound as `?1`.
pub(crate) fn where_key(c: &Collection) -> String {
    match &c.key {
        Key::Minted { column } => format!("{} = ?1", quoted(column)),
        Key::Natural { fields } => fields
            .iter()
            .enumerate()
            .map(|(i, f)| format!("{} = json_extract(?1, '$[{i}]')", quoted(f)))
            .collect::<Vec<_>>()
            .join(" AND "),
    }
}

/// A minted row's local id, from its key.
pub(crate) fn local_of(key: &str) -> Option<i64> {
    key.strip_prefix('[')?
        .strip_suffix(']')?
        .trim()
        .parse()
        .ok()
}

pub(crate) fn minted_key(local: i64) -> String {
    format!("[{local}]")
}

/// What binds `?1` of [`where_key`] for this key.
pub(crate) fn key_param(c: &Collection, key: &str) -> Result<Value> {
    match &c.key {
        Key::Minted { .. } => local_of(key)
            .map(Value::Int)
            .ok_or_else(|| Error::Corrupt(format!("`{key}` is not a minted key"))),
        Key::Natural { .. } => Ok(Value::Text(key.to_string())),
    }
}

/// The synced fields of the row at `key`, as it holds them.
pub(crate) fn read_row(db: &Db, c: &Collection, key: &str) -> Result<Option<Row>> {
    let columns: Vec<String> = c.fields.iter().map(|f| quoted(&f.name)).collect();
    let select = if columns.is_empty() {
        "1".to_string()
    } else {
        columns.join(", ")
    };
    let sql = format!(
        "SELECT {select} FROM {} WHERE {}",
        quoted(&c.table),
        where_key(c)
    );
    let rows = db.query(&sql, &[key_param(c, key)?])?;
    Ok(rows.into_iter().next().map(|values| {
        c.fields
            .iter()
            .zip(values)
            .map(|(f, v)| (f.name.clone(), v))
            .collect()
    }))
}

/// Every row of a table: its key and its synced fields.
pub(crate) fn every_row(db: &Db, c: &Collection) -> Result<Vec<(String, Row)>> {
    let mut columns = vec![key_json(c, "")];
    columns.extend(c.fields.iter().map(|f| quoted(&f.name)));
    let sql = format!("SELECT {} FROM {}", columns.join(", "), quoted(&c.table));
    let mut out = Vec::new();
    for values in db.query(&sql, &[])? {
        let key = as_text(&values[0])?.to_string();
        let row = c
            .fields
            .iter()
            .zip(&values[1..])
            .map(|(f, v)| (f.name.clone(), v.clone()))
            .collect();
        out.push((key, row));
    }
    Ok(out)
}

/// The keys of the rows of `c` whose `column` holds `local`.
pub(crate) fn rows_naming(
    db: &Db,
    c: &Collection,
    column: &str,
    local: i64,
) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT {} FROM {} WHERE {} = ?1",
        key_json(c, ""),
        quoted(&c.table),
        quoted(column)
    );
    db.query(&sql, &[Value::Int(local)])?
        .iter()
        .map(|r| as_text(&r[0]).map(str::to_string))
        .collect()
}

/// The key the triggers would record for a natural row holding these key values, or `None` if
/// they could not record one: SQLite's JSON holds no blob, and a non-finite real would not read
/// back as itself.
pub(crate) fn natural_key(db: &Db, values: &[Value]) -> Result<Option<String>> {
    let storable = |v: &Value| match v {
        Value::Null | Value::Int(_) | Value::Text(_) => true,
        Value::Real(r) => r.is_finite(),
        Value::Blob(_) | Value::Ref(_) => false,
    };
    if !values.iter().all(storable) {
        return Ok(None);
    }
    let marks = vec!["?"; values.len()].join(", ");
    match db.query(&format!("SELECT json_array({marks})"), values) {
        Ok(rows) => match rows.first().map(|r| &r[0]) {
            Some(Value::Text(key)) => Ok(Some(key.clone())),
            _ => Ok(None),
        },
        Err(e) if crate::db::refused(&e) => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) fn triggers(c: &Collection) -> Vec<String> {
    let name = &c.name;
    let table = quoted(&c.table);
    let (new, old) = (key_json(c, "NEW."), key_json(c, "OLD."));
    let mut watched: Vec<String> = c.key_columns().iter().map(|k| k.to_string()).collect();
    for f in &c.fields {
        if !watched.contains(&f.name) {
            watched.push(f.name.clone());
        }
    }
    let watched: Vec<String> = watched.iter().map(|w| quoted(w)).collect();
    let touch = format!(
        "INSERT INTO fongo_dirty (collection, key, at) VALUES ('{name}', {new}, {NOW})
         ON CONFLICT (collection, key) DO UPDATE SET at = excluded.at;"
    );
    vec![
        // A row inserted at a key another row left is a new row there.
        format!(
            "CREATE TRIGGER fongo_{name}_ai AFTER INSERT ON {table} {QUIET} BEGIN
             INSERT INTO fongo_dirty (collection, key, at) VALUES ('{name}', {new}, {NOW})
             ON CONFLICT (collection, key) DO UPDATE SET at = excluded.at,
                gone = CASE WHEN gone > 0 THEN 2 ELSE 0 END; END"
        ),
        format!(
            "CREATE TRIGGER fongo_{name}_au AFTER UPDATE OF {} ON {table} {QUIET} BEGIN {touch}
             INSERT INTO fongo_dirty (collection, key, at, gone)
             SELECT '{name}', {old}, {NOW}, 1 WHERE {old} <> {new}
             ON CONFLICT (collection, key) DO UPDATE SET at = excluded.at, gone = max(gone, 1); END",
            watched.join(", ")
        ),
        format!(
            "CREATE TRIGGER fongo_{name}_ad AFTER DELETE ON {table} {QUIET} BEGIN
             INSERT INTO fongo_dirty (collection, key, at, gone) VALUES ('{name}', {old}, {NOW}, 2)
             ON CONFLICT (collection, key) DO UPDATE SET at = excluded.at, gone = 2; END"
        ),
    ]
}

/// Which synced columns never hold NULL, read from the tables' definitions: those declared
/// `NOT NULL` and a rowid alias. Refuses a registry whose tables lack a column it names, or whose
/// natural keys accept NULL: two rows with NULL in the key would be one entity, which no unique
/// constraint enforces.
pub(crate) fn columns(db: &Db, registry: &Registry) -> Result<Columns> {
    let mut columns = Columns::new();
    for c in registry.collections() {
        let table = sql_text(&c.table);
        let info = db.query(
            &format!("SELECT name, \"notnull\", pk, type FROM pragma_table_info({table})"),
            &[],
        )?;
        if info.is_empty() {
            return Err(Error::Registry(format!(
                "table `{}` does not exist",
                c.table
            )));
        }
        let mut not_null = BTreeSet::new();
        let mut present = BTreeSet::new();
        let mut primary = Vec::new();
        for row in &info {
            let name = as_text(&row[0])?.to_string();
            if as_int(&row[1])? == 1 {
                not_null.insert(name.clone());
            }
            if as_int(&row[2])? > 0 {
                primary.push((name.clone(), as_text(&row[3])?.to_string()));
            }
            present.insert(name);
        }
        if let [(name, declared)] = primary.as_slice()
            && declared.eq_ignore_ascii_case("INTEGER")
            && !has_primary_key_index(db, &table)?
        {
            not_null.insert(name.clone());
        }
        let mut needed: Vec<&str> = c.key_columns();
        needed.extend(c.fields.iter().map(|f| f.name.as_str()));
        needed.extend(c.local_defaults.iter().map(|(col, _)| col.as_str()));
        if let Some(missing) = needed.iter().find(|n| !present.contains(**n)) {
            return Err(Error::Registry(format!(
                "`{}` has no column `{missing}`",
                c.table
            )));
        }
        if let Key::Natural { fields } = &c.key
            && let Some(open) = fields.iter().find(|f| !not_null.contains(*f))
        {
            return Err(Error::Registry(format!(
                "key column `{}.{open}` of a natural collection must be NOT NULL or the table's INTEGER PRIMARY KEY",
                c.table
            )));
        }
        for f in &c.fields {
            if not_null.contains(&f.name) {
                columns = columns.not_null(&c.name, &f.name);
            }
        }
    }
    Ok(columns)
}

/// Whether SQLite keeps an index for the table's primary key. Of rowid tables, only one whose key
/// is the rowid itself has none; that covers `INTEGER PRIMARY KEY DESC` (an index, not an alias)
/// and WITHOUT ROWID tables (always an index).
fn has_primary_key_index(db: &Db, table: &str) -> Result<bool> {
    let rows = db.query(
        &format!("SELECT count(*) FROM pragma_index_list({table}) WHERE origin = 'pk'"),
        &[],
    )?;
    Ok(rows.first().map(|r| as_int(&r[0])).transpose()? != Some(0))
}

/// A string literal for SQL; names are plain identifiers, as the registry requires.
fn sql_text(name: &str) -> String {
    format!("'{name}'")
}

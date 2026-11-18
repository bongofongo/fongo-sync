use std::collections::BTreeSet;

use fongo_sync_core::{DELETED, Strategy, Value};

use crate::error::{Error, Result};

/// How an entity is identified.
#[derive(Debug, Clone)]
pub enum Key {
    /// The table has an `INTEGER PRIMARY KEY` column that means nothing outside this database, so
    /// the replica mints a global id for each row.
    Minted { column: String },
    /// These fields are the identity, so every replica derives the same global id from them. They
    /// must be `NOT NULL`.
    Natural { fields: Vec<String> },
}

#[derive(Debug, Clone)]
pub enum Kind {
    Plain,
    /// Holds the local id of a row in a minted collection, translated to its global id in transit.
    /// `owned` says what a deleted target means: the row goes with it, or the field goes null.
    Ref {
        collection: String,
        owned: bool,
    },
}

#[derive(Debug, Clone)]
pub struct Field {
    pub name: String,
    pub kind: Kind,
    pub strategy: Strategy,
    /// Whether an apply reports a local edit and a remote one made without seeing each other.
    pub reports_conflicts: bool,
}

/// One synced table.
#[derive(Debug, Clone)]
pub struct Collection {
    pub name: String,
    pub table: String,
    pub key: Key,
    pub fields: Vec<Field>,
    /// Sets of fields that name the thing itself. Two entities whose values for every field of one
    /// set are equal and not null are the same entity, and become one on every replica.
    pub identity: Vec<Vec<String>>,
    /// Columns of tables outside the registry that hold this collection's local ids, as
    /// `(table, column)`.
    pub local_dependents: Vec<(String, String)>,
    /// Columns the kernel does not sync, and what to put in them when it inserts a row.
    pub local_defaults: Vec<(String, LocalDefault)>,
    /// Sets of fields that change together.
    pub together: Vec<Vec<String>>,
}

/// What the kernel writes into a column it does not sync when it inserts a row.
#[derive(Debug, Clone, PartialEq)]
pub enum LocalDefault {
    Value(Value),
    /// An SQL expression, written into the insert as given. It comes from the application,
    /// never from what a user typed.
    Sql(String),
}

impl Collection {
    pub fn minted(name: &str, table: &str, key_column: &str) -> Collection {
        Collection {
            name: name.into(),
            table: table.into(),
            key: Key::Minted {
                column: key_column.into(),
            },
            fields: Vec::new(),
            identity: Vec::new(),
            local_dependents: Vec::new(),
            local_defaults: Vec::new(),
            together: Vec::new(),
        }
    }

    pub fn natural(name: &str, table: &str, key_fields: &[&str]) -> Collection {
        Collection {
            name: name.into(),
            table: table.into(),
            key: Key::Natural {
                fields: key_fields.iter().map(|f| (*f).into()).collect(),
            },
            fields: Vec::new(),
            identity: Vec::new(),
            local_dependents: Vec::new(),
            local_defaults: Vec::new(),
            together: Vec::new(),
        }
    }

    pub fn field(self, name: &str) -> Collection {
        self.field_with(name, Strategy::Lww)
    }

    pub fn field_with(mut self, name: &str, strategy: Strategy) -> Collection {
        self.fields.push(Field {
            name: name.into(),
            kind: Kind::Plain,
            strategy,
            reports_conflicts: false,
        });
        self
    }

    /// A field whose concurrent edits [`crate::Replica::apply`] reports in
    /// [`crate::Applied::conflicts`], so the application can combine them. The later edit is
    /// still the one kept until the application writes something else.
    pub fn field_reporting_conflicts(mut self, name: &str) -> Collection {
        self.fields.push(Field {
            name: name.into(),
            kind: Kind::Plain,
            strategy: Strategy::Lww,
            reports_conflicts: true,
        });
        self
    }

    /// Declares a table the kernel does not sync whose `column` refers to this collection's rows.
    /// When two entities merge and one of their rows is given up, rows of `table` that point at
    /// it are moved to the row kept, unless that would break a constraint of `table`; those are
    /// left to its `ON DELETE` action. If that action refuses the deletion, the merge of the two
    /// rows waits, reported in [`crate::Applied::held`], and is tried again by every apply until
    /// the application lets the row go; meanwhile nothing written to that row is sent.
    pub fn local_dependent(mut self, table: &str, column: &str) -> Collection {
        self.local_dependents.push((table.into(), column.into()));
        self
    }

    /// A column this replica keeps for itself that must be filled when the kernel inserts a row,
    /// such as a `NOT NULL` column without a `DEFAULT`.
    pub fn local_default(mut self, column: &str, value: LocalDefault) -> Collection {
        self.local_defaults.push((column.into(), value));
        self
    }

    /// Declares fields that only make sense together, such as a status and the dates that go
    /// with it. A write to any of them records all of them at one clock, and the newest such
    /// write wins as a whole, so merging never yields a combination no replica wrote; until all
    /// of a write has arrived the row keeps what it held. The fields must be plain fields or
    /// references merged by clock, neither owners nor part of the key. When a field joins a set
    /// that already holds writes, each replica whose rows hold a value for the set records the
    /// whole set again on install, and the newest of those wins.
    pub fn together(mut self, fields: &[&str]) -> Collection {
        self.together
            .push(fields.iter().map(|f| (*f).into()).collect());
        self
    }

    /// Declares an identity: two live entities equal on all of `fields`, none of them null, are
    /// the same entity, and become one on every replica, with one row. Only a minted collection
    /// can have one, and the table should hold the fields `UNIQUE` together. A row that holds an
    /// identity only because it is out of date gives it up for the rest of an apply, taking null
    /// or, where null is refused, a stand-in value; if a `CHECK` on the table refuses that too,
    /// the rows wait for the application rather than swap.
    pub fn identity(mut self, fields: &[&str]) -> Collection {
        self.identity
            .push(fields.iter().map(|f| (*f).into()).collect());
        self
    }

    /// A reference that goes null when its target is deleted (`ON DELETE SET NULL`).
    pub fn reference(self, name: &str, collection: &str) -> Collection {
        self.refer(name, collection, false)
    }

    /// A reference whose row is deleted with its target (`ON DELETE CASCADE`).
    pub fn owner(self, name: &str, collection: &str) -> Collection {
        self.refer(name, collection, true)
    }

    fn refer(mut self, name: &str, collection: &str, owned: bool) -> Collection {
        self.fields.push(Field {
            name: name.into(),
            kind: Kind::Ref {
                collection: collection.into(),
                owned,
            },
            strategy: Strategy::Lww,
            reports_conflicts: false,
        });
        self
    }

    pub(crate) fn key_columns(&self) -> Vec<&str> {
        match &self.key {
            Key::Minted { column } => vec![column.as_str()],
            Key::Natural { fields } => fields.iter().map(String::as_str).collect(),
        }
    }

    /// Whether a row goes when the row this field refers to goes.
    pub(crate) fn owns_through(&self, field: &Field) -> bool {
        matches!(field.kind, Kind::Ref { owned: true, .. })
            || matches!(&self.key, Key::Natural { fields } if fields.contains(&field.name))
    }

    /// Which set of fields changing together holds `field`, if any.
    pub(crate) fn together_of(&self, field: &str) -> Option<usize> {
        self.together
            .iter()
            .position(|set| set.iter().any(|f| f == field))
    }

    pub(crate) fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }
}

/// The synced tables. A reference may name any minted collection; ownership may not go round in
/// a circle between collections, though a collection may own its own rows.
#[derive(Debug, Clone)]
pub struct Registry {
    collections: Vec<Collection>,
    /// Each collection's place when rows are written owners first.
    ranks: Vec<usize>,
}

fn is_identifier(text: &str) -> bool {
    let mut chars = text.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl Registry {
    pub fn new(collections: Vec<Collection>) -> Result<Registry> {
        let bad = |message: String| Err(Error::Registry(message));
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        let mut minted: BTreeSet<&str> = BTreeSet::new();
        for c in &collections {
            let mut names = vec![c.name.as_str(), c.table.as_str()];
            names.extend(c.key_columns());
            names.extend(c.fields.iter().map(|f| f.name.as_str()));
            for (table, column) in &c.local_dependents {
                names.push(table);
                names.push(column);
            }
            names.extend(c.local_defaults.iter().map(|(column, _)| column.as_str()));
            if let Some(name) = names.iter().find(|n| !is_identifier(n)) {
                return bad(format!(
                    "`{name}` in `{}` is not a plain identifier",
                    c.name
                ));
            }
            if !seen.insert(&c.name) {
                return bad(format!("collection `{}` is declared twice", c.name));
            }
            let mut fields: BTreeSet<&str> = BTreeSet::new();
            for f in &c.fields {
                if !fields.insert(&f.name) {
                    return bad(format!("`{}.{}` is declared twice", c.name, f.name));
                }
                if f.name == DELETED {
                    return bad(format!("`{}.{}` is reserved", c.name, f.name));
                }
            }
            if let Some((column, _)) = c.local_defaults.iter().find(|(column, _)| {
                fields.contains(column.as_str()) || c.key_columns().contains(&column.as_str())
            }) {
                return bad(format!(
                    "`{}.{column}` is synced, so cannot have a local default",
                    c.name
                ));
            }
            match &c.key {
                Key::Minted { column } => {
                    if fields.contains(column.as_str()) {
                        return bad(format!(
                            "`{}.{column}` is the minted key, not a field",
                            c.name
                        ));
                    }
                    minted.insert(&c.name);
                }
                Key::Natural { fields: key } => {
                    if key.is_empty() {
                        return bad(format!("`{}` has an empty key", c.name));
                    }
                    if let Some(missing) = key.iter().find(|k| !fields.contains(k.as_str())) {
                        return bad(format!("key field `{}.{missing}` is not declared", c.name));
                    }
                }
            }
            let mut grouped: BTreeSet<&str> = BTreeSet::new();
            for set in &c.together {
                for name in set {
                    let Some(field) = c.fields.iter().find(|f| f.name == *name) else {
                        return bad(format!("`{}.{name}` is not declared", c.name));
                    };
                    if field.strategy != Strategy::Lww
                        || c.key_columns().contains(&name.as_str())
                        || c.owns_through(field)
                    {
                        return bad(format!(
                            "`{}.{name}` cannot change together with others",
                            c.name
                        ));
                    }
                    if !grouped.insert(name) {
                        return bad(format!("`{}.{name}` is in two sets", c.name));
                    }
                }
            }
            for identity in &c.identity {
                if !matches!(c.key, Key::Minted { .. }) {
                    return bad(format!(
                        "`{}` has an identity but is not a minted collection",
                        c.name
                    ));
                }
                if identity.is_empty() {
                    return bad(format!("`{}` has an empty identity", c.name));
                }
                if let Some(missing) = identity.iter().find(|k| !fields.contains(k.as_str())) {
                    return bad(format!(
                        "identity field `{}.{missing}` is not declared",
                        c.name
                    ));
                }
            }
        }
        for c in &collections {
            for f in &c.fields {
                if let Kind::Ref { collection, .. } = &f.kind
                    && !minted.contains(collection.as_str())
                {
                    return bad(format!(
                        "`{}.{}` refers to `{collection}`, which is not a minted collection",
                        c.name, f.name
                    ));
                }
                // Two replicas could each make one row the owner of the other.
                if matches!(&f.kind, Kind::Ref { collection, owned: true } if *collection == c.name)
                {
                    return bad(format!("`{}.{}` makes a row own its kind", c.name, f.name));
                }
            }
        }
        let owners: Vec<BTreeSet<usize>> = collections
            .iter()
            .map(|c| {
                c.fields
                    .iter()
                    .filter(|f| c.owns_through(f))
                    .filter_map(|f| match &f.kind {
                        Kind::Ref { collection, .. } => {
                            collections.iter().position(|o| o.name == *collection)
                        }
                        Kind::Plain => None,
                    })
                    .collect()
            })
            .collect();
        let mut ranks = vec![usize::MAX; collections.len()];
        for next in 0..collections.len() {
            let ready = (0..collections.len()).find(|&i| {
                ranks[i] == usize::MAX && owners[i].iter().all(|&o| ranks[o] != usize::MAX)
            });
            let Some(ready) = ready else {
                let stuck = (0..collections.len()).find(|&i| ranks[i] == usize::MAX);
                return bad(format!(
                    "`{}` is owned, through others, by itself",
                    stuck.map_or("", |i| collections[i].name.as_str())
                ));
            };
            ranks[ready] = next;
        }
        Ok(Registry { collections, ranks })
    }

    pub fn collections(&self) -> &[Collection] {
        &self.collections
    }

    /// Where a collection comes when rows are written parents first.
    pub(crate) fn rank(&self, idx: usize) -> usize {
        self.ranks[idx]
    }

    pub(crate) fn index(&self, name: &str) -> Option<usize> {
        self.collections.iter().position(|c| c.name == name)
    }

    pub(crate) fn strategy(&self, collection: &str, field: &str) -> Strategy {
        if field == DELETED {
            return Strategy::Lww;
        }
        self.index(collection)
            .and_then(|i| {
                let c = &self.collections[i];
                c.field_index(field).map(|f| c.fields[f].strategy)
            })
            .unwrap_or(Strategy::Lww)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reference_names_a_minted_collection_declared_anywhere() {
        let books = Collection::minted("books", "books", "id").reference("author_id", "authors");
        let authors = Collection::minted("authors", "authors", "id").reference("best", "books");
        let registry = Registry::new(vec![books.clone(), authors.clone()]).unwrap();
        assert_eq!(registry.rank(0), 0);
        let tags = Collection::natural("tags", "tags", &["tag"]).field("tag");
        let wrong = Collection::minted("books", "books", "id").reference("tag", "tags");
        assert!(Registry::new(vec![tags, wrong]).is_err());
    }

    #[test]
    fn owners_come_first_and_may_not_go_round() {
        let notes = Collection::minted("notes", "notes", "id").owner("book_id", "books");
        let books = Collection::minted("books", "books", "id").owner("shelf_id", "shelves");
        let shelves = Collection::minted("shelves", "shelves", "id").field("name");
        let registry = Registry::new(vec![notes.clone(), books, shelves]).unwrap();
        assert!(registry.rank(2) < registry.rank(1) && registry.rank(1) < registry.rank(0));
        let circle = Collection::minted("books", "books", "id").owner("note_id", "notes");
        assert!(Registry::new(vec![notes, circle]).is_err());
        let own_kind = Collection::minted("books", "books", "id").owner("parent", "books");
        assert!(Registry::new(vec![own_kind]).is_err());
        let referring = Collection::minted("books", "books", "id").reference("sequel", "books");
        assert!(Registry::new(vec![referring]).is_ok());
    }

    #[test]
    fn names_that_could_break_out_of_sql_are_refused() {
        let c = Collection::minted("books", "books\"; DROP TABLE x; --", "id");
        assert!(Registry::new(vec![c]).is_err());
    }

    #[test]
    fn a_natural_key_names_declared_fields() {
        let c = Collection::natural("settings", "settings", &["key"]).field("value");
        assert!(Registry::new(vec![c]).is_err());
        let c = Collection::natural("settings", "settings", &["key"])
            .field("key")
            .field("value");
        assert!(Registry::new(vec![c]).is_ok());
    }

    #[test]
    fn fields_that_change_together_are_declared_merged_by_clock_and_in_one_set() {
        let books = || {
            Collection::minted("books", "books", "id")
                .field("status")
                .field("finished")
                .field_with("pages", Strategy::Max)
        };
        assert!(Registry::new(vec![books().together(&["status", "finished"])]).is_ok());
        assert!(Registry::new(vec![books().together(&["status", "started"])]).is_err());
        assert!(Registry::new(vec![books().together(&["status", "pages"])]).is_err());
        let owned = Collection::minted("notes", "notes", "id")
            .owner("book_id", "books")
            .reference("next", "notes")
            .field("body");
        let registry = |set: &[&str]| Registry::new(vec![books(), owned.clone().together(set)]);
        assert!(registry(&["body", "next"]).is_ok());
        assert!(registry(&["body", "book_id"]).is_err());
        let twice = books()
            .together(&["status", "finished"])
            .together(&["finished"]);
        assert!(Registry::new(vec![twice]).is_err());
    }

    #[test]
    fn a_local_default_is_for_a_column_that_is_not_synced() {
        let books = || Collection::minted("books", "books", "id").field("title");
        let default = || LocalDefault::Value(Value::Int(0));
        assert!(Registry::new(vec![books().local_default("seen", default())]).is_ok());
        assert!(Registry::new(vec![books().local_default("title", default())]).is_err());
        assert!(Registry::new(vec![books().local_default("id", default())]).is_err());
    }

    #[test]
    fn an_identity_names_declared_fields_of_a_minted_collection() {
        let books = || Collection::minted("books", "books", "id").field("isbn");
        assert!(Registry::new(vec![books().identity(&["isbn"])]).is_ok());
        assert!(Registry::new(vec![books().identity(&["issn"])]).is_err());
        assert!(Registry::new(vec![books().identity(&[])]).is_err());
        let settings = Collection::natural("settings", "settings", &["key"])
            .field("key")
            .field("value")
            .identity(&["value"]);
        assert!(Registry::new(vec![settings]).is_err());
    }
}

use std::collections::BTreeSet;

use fongo_sync_core::model;
use fongo_sync_core::{DELETED, Strategy, Value};

use crate::error::{Error, Result};

/// How an entity is identified.
#[derive(Debug, Clone)]
pub enum Key {
    /// The table has an `INTEGER PRIMARY KEY` column that means nothing outside this database, so
    /// the replica mints a global id for each row.
    Minted { column: String },
    /// These fields are the identity, so every replica derives the same global id from them. Each
    /// of their columns must be declared `NOT NULL` or be the table's `INTEGER PRIMARY KEY`, an
    /// alias for the rowid, which never holds NULL; [`crate::Replica::install`] checks.
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
    /// write wins as a whole, so merging never yields a combination no replica wrote; a field of
    /// it whose part of that write has not arrived yet reads NULL. The exception is two writes
    /// at one clock, which two databases writing as one node make (see
    /// [`crate::SharedNode`]): each field then takes the greater of its two values, which can
    /// mix the writes. The fields must be plain fields merged by clock, not references nor part
    /// of the key.
    ///
    /// A registry that draws a set anew (adds a field to one, merges two, or groups fields
    /// written apart) leaves the writes made before it holding only part of the new set. On
    /// install, each replica records such a set again for every entity whose set is not whole:
    /// each field with the value the merged changes hold, or for a field the registry has just
    /// declared that nothing holds yet, the value in the row, at the clock just past the latest
    /// write to the set this replica holds, carrying its own id; a set no change holds anything
    /// of takes the clock of a found row. A held-back row's values and an edit not yet collected
    /// are never sent this way: the edit is collected afterwards, as an edit. So an edit to the
    /// set made elsewhere and not yet received still wins, unless it was made in the same
    /// millisecond as that latest write, as is any part of that write still on its way, or it is
    /// older than that write and wrote a field of the new set that the write did not. Such an
    /// edit is lost for good: the record replaces it in the merge, so it does not come back when
    /// the set is later narrowed. Replicas that do this apart agree on one of their values. Sets
    /// removed, narrowed or reordered send nothing; a field that leaves a set shows its own
    /// latest write that the merge still holds, which the install writes into the rows: it
    /// writes every row of the collection again, in its one transaction. A replica with an edit
    /// to a field of the set that reports conflicts, not yet sent, is told of such a record as a
    /// [`crate::Conflict`] whose remote value is the base, and keeps its edit.
    ///
    /// Adding a field to a set changes what the set means. Where a replica still on the old
    /// registry writes the set after another has recorded it again, the new field reads NULL on
    /// every upgraded replica until that one upgrades and records the set again with the value
    /// it received, or its own if it received none; if the set is written on an upgraded replica
    /// meanwhile, that write records the NULL and the value is gone.
    pub fn together(mut self, fields: &[&str]) -> Collection {
        self.together
            .push(fields.iter().map(|f| (*f).into()).collect());
        self
    }

    /// Declares an identity: two live entities equal on all of `fields`, none of them null, are
    /// the same entity, and become one on every replica, with one row. Only a minted collection
    /// can have one, and the table should hold the fields `UNIQUE` together. A row that holds an
    /// identity only because it is out of date gives it up for the rest of an apply by taking
    /// NULL in one of its fields. Where every field refuses NULL, rows that swap identities
    /// cannot be written one at a time: they wait, reported in [`crate::Applied::held`], until
    /// the application changes its rows.
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
}

/// The synced tables. A reference may name any minted collection; ownership may not go round in
/// a circle, and a collection may not own its own rows.
#[derive(Debug, Clone)]
pub struct Registry {
    collections: Vec<Collection>,
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
                if set.is_empty() {
                    return bad(format!("`{}` has an empty set of fields", c.name));
                }
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
        let mut placed = vec![false; collections.len()];
        for _ in 0..collections.len() {
            let ready = (0..collections.len())
                .find(|&i| !placed[i] && owners[i].iter().all(|&o| placed[o]));
            let Some(ready) = ready else {
                let stuck = (0..collections.len()).find(|&i| !placed[i]);
                return bad(format!(
                    "`{}` is owned, through others, by itself",
                    stuck.map_or("", |i| collections[i].name.as_str())
                ));
            };
            placed[ready] = true;
        }
        let registry = Registry { collections };
        registry.model_schema().check().map_err(Error::Registry)?;
        Ok(registry)
    }

    /// The reference model's schema for these collections, which says what replicas using this
    /// registry must converge to. A field in a natural key that refers to another collection owns
    /// the row, since the row's identity goes with its target.
    pub fn model_schema(&self) -> model::Schema {
        let collections = self
            .collections
            .iter()
            .map(|c| model::Collection {
                name: c.name.clone(),
                identity: match &c.key {
                    Key::Minted { .. } => model::Identity::Minted,
                    Key::Natural { fields } => model::Identity::Natural(fields.clone()),
                },
                fields: c
                    .fields
                    .iter()
                    .map(|f| model::Field {
                        name: f.name.clone(),
                        kind: match &f.kind {
                            Kind::Plain => model::FieldKind::Plain,
                            Kind::Ref { collection, .. } if c.owns_through(f) => {
                                model::FieldKind::Owner(collection.clone())
                            }
                            Kind::Ref { collection, .. } => {
                                model::FieldKind::Reference(collection.clone())
                            }
                        },
                        strategy: f.strategy,
                    })
                    .collect(),
                identity_keys: c.identity.clone(),
                together: c.together.clone(),
            })
            .collect();
        model::Schema::new(collections)
    }

    pub fn collections(&self) -> &[Collection] {
        &self.collections
    }

    pub(crate) fn index(&self, name: &str) -> Option<usize> {
        self.collections.iter().position(|c| c.name == name)
    }
}

impl Registry {
    /// A text [`Registry::decode`] rebuilds this registry from, kept with the rows a first
    /// install in steps records, so they are adopted under the registry they were recorded
    /// with.
    pub(crate) fn encode(&self) -> String {
        let mut lines = Vec::new();
        for c in &self.collections {
            lines.push(match &c.key {
                Key::Minted { column } => format!("C\t{}\t{}\tM\t{column}", c.name, c.table),
                Key::Natural { fields } => {
                    format!("C\t{}\t{}\tN\t{}", c.name, c.table, fields.join(","))
                }
            });
            for f in &c.fields {
                let (kind, target, owned) = match &f.kind {
                    Kind::Plain => ("P", "", false),
                    Kind::Ref { collection, owned } => ("R", collection.as_str(), *owned),
                };
                let strategy = match f.strategy {
                    Strategy::Lww => "Lww",
                    Strategy::Max => "Max",
                };
                lines.push(format!(
                    "F\t{}\t{kind}\t{target}\t{}\t{strategy}\t{}",
                    f.name,
                    u8::from(owned),
                    u8::from(f.reports_conflicts)
                ));
            }
            for set in &c.identity {
                lines.push(format!("I\t{}", set.join(",")));
            }
            for set in &c.together {
                lines.push(format!("T\t{}", set.join(",")));
            }
            for (table, column) in &c.local_dependents {
                lines.push(format!("D\t{table}\t{column}"));
            }
        }
        lines.join("\n")
    }

    /// The registry [`Registry::encode`] wrote, with the local defaults `current` gives its
    /// collections: no SQL is read back from the database. Its names are checked as any
    /// registry's are.
    pub(crate) fn decode(text: &str, current: &Registry) -> Result<Registry> {
        let bad = || Error::Corrupt("a recorded registry does not parse".into());
        let list = |s: &str| -> Vec<String> {
            s.split(',')
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect()
        };
        let mut collections: Vec<Collection> = Vec::new();
        for line in text.lines() {
            let parts: Vec<&str> = line.split('\t').collect();
            if parts[0] == "C" {
                let key = match (parts.get(3), parts.get(4)) {
                    (Some(&"M"), Some(column)) => Key::Minted {
                        column: (*column).to_string(),
                    },
                    (Some(&"N"), Some(fields)) => Key::Natural {
                        fields: list(fields),
                    },
                    _ => return Err(bad()),
                };
                collections.push(Collection {
                    name: parts.get(1).ok_or_else(bad)?.to_string(),
                    table: parts.get(2).ok_or_else(bad)?.to_string(),
                    key,
                    fields: Vec::new(),
                    identity: Vec::new(),
                    local_dependents: Vec::new(),
                    local_defaults: Vec::new(),
                    together: Vec::new(),
                });
                continue;
            }
            let c = collections.last_mut().ok_or_else(bad)?;
            match parts.as_slice() {
                ["F", name, kind, target, owned, strategy, reports] => c.fields.push(Field {
                    name: (*name).to_string(),
                    kind: match *kind {
                        "P" => Kind::Plain,
                        "R" => Kind::Ref {
                            collection: (*target).to_string(),
                            owned: *owned == "1",
                        },
                        _ => return Err(bad()),
                    },
                    strategy: match *strategy {
                        "Lww" => Strategy::Lww,
                        "Max" => Strategy::Max,
                        _ => return Err(bad()),
                    },
                    reports_conflicts: *reports == "1",
                }),
                ["I", set] => c.identity.push(list(set)),
                ["T", set] => c.together.push(list(set)),
                ["D", table, column] => c
                    .local_dependents
                    .push(((*table).to_string(), (*column).to_string())),
                _ => return Err(bad()),
            }
        }
        for c in &mut collections {
            if let Some(now) = current.collections.iter().find(|n| n.name == c.name) {
                c.local_defaults.clone_from(&now.local_defaults);
            }
        }
        Registry::new(collections)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reference_names_a_minted_collection_declared_anywhere() {
        let books = Collection::minted("books", "books", "id").reference("author_id", "authors");
        let authors = Collection::minted("authors", "authors", "id").reference("best", "books");
        assert!(Registry::new(vec![books.clone(), authors.clone()]).is_ok());
        let tags = Collection::natural("tags", "tags", &["tag"]).field("tag");
        let wrong = Collection::minted("books", "books", "id").reference("tag", "tags");
        assert!(Registry::new(vec![tags, wrong]).is_err());
    }

    #[test]
    fn owners_come_first_and_may_not_go_round() {
        let notes = Collection::minted("notes", "notes", "id").owner("book_id", "books");
        let books = Collection::minted("books", "books", "id").owner("shelf_id", "shelves");
        let shelves = Collection::minted("shelves", "shelves", "id").field("name");
        assert!(Registry::new(vec![notes.clone(), books, shelves]).is_ok());
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
        assert!(registry(&["body", "next"]).is_err());
        assert!(registry(&["body", "book_id"]).is_err());
        let twice = books()
            .together(&["status", "finished"])
            .together(&["finished"]);
        assert!(Registry::new(vec![twice]).is_err());
        assert!(Registry::new(vec![books().together(&[])]).is_err());
    }

    #[test]
    fn the_model_schema_keeps_keys_owners_and_sets() {
        let registry = Registry::new(vec![
            Collection::minted("books", "books", "id")
                .field("isbn")
                .field_with("pages", Strategy::Max)
                .reference("sequel", "books")
                .field("status")
                .field("finished")
                .together(&["status", "finished"])
                .identity(&["isbn"]),
            Collection::natural("tags", "tags", &["book_id", "tag"])
                .reference("book_id", "books")
                .field("tag"),
            Collection::minted("notes", "notes", "id").owner("book_id", "books"),
        ])
        .unwrap();
        let schema = registry.model_schema();
        assert_eq!(schema.check(), Ok(()));
        let expected = model::Schema::new(vec![
            model::Collection::minted("books")
                .plain("isbn", Strategy::Lww)
                .plain("pages", Strategy::Max)
                .reference("sequel", "books")
                .plain("status", Strategy::Lww)
                .plain("finished", Strategy::Lww)
                .identity_key(&["isbn"])
                .together(&["status", "finished"]),
            model::Collection::natural("tags", &["book_id", "tag"])
                .owner("book_id", "books")
                .plain("tag", Strategy::Lww),
            model::Collection::minted("notes").owner("book_id", "books"),
        ]);
        assert_eq!(schema, expected);
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

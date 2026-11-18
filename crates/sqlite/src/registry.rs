use std::collections::BTreeSet;

use fongo_sync_core::{DELETED, Strategy};

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

    /// Declares an identity: entities equal on all of `fields` are merged. Only a minted
    /// collection can have one, and the table should hold the fields `UNIQUE` together.
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

    pub(crate) fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }
}

/// The synced tables, parents before the collections that refer to them.
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
            for f in &c.fields {
                if let Kind::Ref { collection, .. } = &f.kind
                    && !minted.contains(collection.as_str())
                {
                    return bad(format!(
                        "`{}.{}` refers to `{collection}`, which is not a minted collection declared before it",
                        c.name, f.name
                    ));
                }
            }
        }
        Ok(Registry { collections })
    }

    pub fn collections(&self) -> &[Collection] {
        &self.collections
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
    fn a_reference_must_point_at_an_earlier_minted_collection() {
        let books = Collection::minted("books", "books", "id").reference("author_id", "authors");
        let authors = Collection::minted("authors", "authors", "id").field("name");
        assert!(Registry::new(vec![books.clone(), authors.clone()]).is_err());
        assert!(Registry::new(vec![authors, books]).is_ok());
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

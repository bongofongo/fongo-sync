#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Sql(#[from] sqlx::Error),
    /// SQLite refused a statement; `code` is its extended result code.
    #[error("sqlite: {message} (code {code})")]
    Sqlite { code: i32, message: String },
    #[error("registry: {0}")]
    Registry(String),
    #[error("sync state is corrupt: {0}")]
    Corrupt(String),
    /// The database holds none of the kernel's tables: it was never installed, or was
    /// uninstalled. [`crate::Replica::install`] lays them down.
    #[error("the sync kernel is not installed in this database; install it first")]
    NotInstalled,
    /// Capture is suspended ([`crate::Replica::suspend`]): rows written meanwhile are known only
    /// once [`crate::Replica::resume`] compares the tables with what the kernel last saw, so
    /// nothing that would collect, merge or put the triggers back runs until then.
    #[error("change capture is suspended; resume it first")]
    Suspended,
    /// The kernel's tables are of a layout later than any this build knows, laid down by a newer
    /// build. Nothing is read or changed; only a build that knows `found` can use them.
    #[error(
        "the sync kernel's tables are of layout {found}, newer than layout {known}, the latest this build knows"
    )]
    NewerLayout { found: i64, known: i64 },
    /// The kernel's tables are of an earlier layout that [`crate::Replica::install`] upgrades in
    /// place. Nothing else runs on them until it has.
    #[error(
        "the sync kernel's tables are of layout {found}, older than layout {known}; install to upgrade them first"
    )]
    OlderLayout { found: i64, known: i64 },
    /// The kernel's tables are of a layout this build neither uses nor upgrades, or record none.
    /// [`crate::Replica::uninstall`] removes them.
    #[error(
        "the sync kernel's tables are of layout {found:?}, which this build cannot upgrade to layout {known}; uninstall them first"
    )]
    UnknownLayout { found: Option<i64>, known: i64 },
}

pub type Result<T> = std::result::Result<T, Error>;

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
    /// The application wrote a value other than an encoded
    /// [`FileRef`](fongo_sync_protocol::FileRef) or NULL to a field that names files
    /// ([`crate::Collection::file`]), and a collect, or the collect an apply or a resume begins
    /// with, met the write. Nothing was collected; the write stays to be collected, and once the
    /// row at `key` holds a reference or NULL there, or is removed, the next call takes it.
    /// Values adopted from rows already there at an install, and values from elsewhere, are
    /// never refused.
    #[error(
        "`{collection}.{field}` names files, and a row holds a value that is not a file reference"
    )]
    NotAFile {
        collection: String,
        field: String,
        key: Option<String>,
    },
    /// A trigger of the application's has the name of a capture trigger the kernel makes for a
    /// collection of the registry, `fongo_<collection>_ai`, `_au` or `_ad` in any case. An
    /// install or resume refused it before changing anything; renaming the trigger lets it run.
    #[error("the trigger `{name}` has a name reserved to the sync kernel's capture triggers")]
    Reserved { name: String },
    /// The registry declares `collection.field` as naming files where a registry this database
    /// was installed under declared it as a field that names none, or the reverse
    /// (`names_files` says which the registry asks for). A version that wrote references to a
    /// plain field sent them with no claim on their files, so a field never becomes one that
    /// names files, nor stops being one: use a new field. `install`, `install_step` and `resume`
    /// refuse before changing anything; see [`crate::Collection::file`] for what is recorded.
    #[error(
        "`{collection}.{field}` was declared otherwise by a registry this database was installed under; a field never becomes or stops being one that names files, so use a new field"
    )]
    FileKind {
        collection: String,
        field: String,
        names_files: bool,
    },
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

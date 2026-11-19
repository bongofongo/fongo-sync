#[derive(Debug, thiserror::Error)]
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
}

pub type Result<T> = std::result::Result<T, Error>;

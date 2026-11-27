use fongo_sync_protocol::RecoveryError;
use fongo_sync_protocol::api::ErrorCode;
use thiserror::Error;

use crate::transport::TransportError;

/// Why a call failed. None of them leaves the database or the server's log inconsistent: the
/// next call picks up where this one stopped.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The transport had no response to give. Retry later.
    #[error("transport: {0}")]
    Transport(#[source] TransportError),
    /// The server refused the request.
    #[error("server refused the request ({status}): {}", code.message())]
    Server { status: u16, code: ErrorCode },
    /// The server refused the request's timestamp: this device's clock is too far from the
    /// server's. The user has to fix the clock.
    #[error("this device's clock is too far from the server's")]
    ClockSkew,
    /// The server answered with something the protocol does not allow. Nothing of it was applied.
    #[error("the server's response is malformed: {0}")]
    MalformedResponse(&'static str),
    /// The server speaks a protocol version this client does not.
    #[error("the server speaks protocol version {server}; this client speaks {client}")]
    NewerProtocol { server: u32, client: u32 },
    /// One change is too large for any envelope the server accepts, so it can never be sent. The
    /// changes before it in the outbox were sent.
    #[error(
        "a change to `{collection}.{field}` seals to {bytes} bytes; the server accepts at most {max}"
    )]
    ChangeTooLarge {
        collection: String,
        field: String,
        bytes: usize,
        max: u64,
    },
    /// The server's maximum envelope size is below that of any envelope carrying a change, so
    /// nothing can be sent until it is raised.
    #[error("the server accepts envelopes of at most {max} bytes, smaller than any envelope")]
    MaxBatchTooSmall { max: u64 },
    /// The key is listed in the library, but with another wrap key or membership proof than the
    /// one these credentials hold or the request carried: registered earlier with other
    /// credentials for the same key, or changed or dropped by the server. The device can sync, but
    /// must not count on receiving a new library secret sealed to its wrap key. Nothing was
    /// changed.
    #[error("this device's key is registered with another wrap key or membership proof")]
    RegisteredOtherwise,
    /// The wrap key offered for a new device is not signed by that device's key for this
    /// library. Nothing was sent.
    #[error("the new device's wrap key is not signed by its key for this library")]
    WrapKeyNotSigned,
    #[error("recovery code: {0}")]
    RecoveryCode(#[from] RecoveryError),
    #[error("the operating system's randomness source failed")]
    Randomness,
    #[error("database: {0}")]
    Database(#[from] fongo_sync_sqlite::Error),
    /// The client's own tables in the database are of a later layout than this build knows,
    /// written by a later build. Nothing was read from the server or written.
    #[error("the sync client's tables are of layout {found}; this build knows layout {known}")]
    NewerClientTables { found: i64, known: i64 },
    /// [`crate::step_past`] was asked to step past an envelope the pull is not stopped at, or
    /// with other bytes than it stopped at. Nothing changed.
    #[error("the pull is not stopped at envelope {seq} with those bytes")]
    NotStoppedAt { seq: u64 },
    /// The application's own step, given through [`crate::Apply`], failed.
    #[error("the application's step failed: {0}")]
    Application(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// A step given through [`crate::Apply`] returned with the client's transaction ended, as a
    /// statement SQLite answers by rolling back ends it, or rolled back to the savepoint the client
    /// made before it. Nothing in that transaction was kept and the cursor did not move.
    #[error("the transaction a sync opened ended inside the application's step")]
    TransactionEnded,
    /// The connection cannot be trusted: SQLite has a transaction open on it, or sqlx counts one
    /// that SQLite does not have. Either a sync found it so before reading or writing anything, or
    /// a sync that failed left it so, and `source` is that failure. Finish a transaction the
    /// application opened; otherwise close the connection, awaiting the close, before writing
    /// anything more, and open a fresh one. A pooled connection is detached and closed rather
    /// than given back.
    #[error(
        "the connection cannot be trusted: a transaction is open on it, or sqlx counts one SQLite \
         does not have; finish a transaction you opened, or else close this connection, awaiting \
         the close, and open a fresh one (from a pool, detach and close it rather than return it)"
    )]
    ConnectionUnsound { source: Option<Box<Error>> },
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Error {
        Error::Database(error.into())
    }
}

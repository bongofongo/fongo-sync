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
    #[error("recovery code: {0}")]
    RecoveryCode(#[from] RecoveryError),
    #[error("the operating system's randomness source failed")]
    Randomness,
    #[error("database: {0}")]
    Database(#[from] fongo_sync_sqlite::Error),
    /// The application's own step, given through [`crate::Apply`], failed.
    #[error("the application's step failed: {0}")]
    Application(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// A step given through [`crate::Apply`] returned with the client's transaction ended, as a
    /// statement SQLite answers by rolling back ends it, or rolled back to the savepoint the client
    /// made before it. Nothing in that transaction was kept and the cursor did not move.
    #[error("the transaction a sync opened ended inside the application's step")]
    TransactionEnded,
}

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Error {
        Error::Database(error.into())
    }
}

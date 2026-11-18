//! The wire protocol between devices and a sync server: batch encoding, library and device keys,
//! sealed batches the server can verify but not read, request signatures, recovery codes and the
//! control-plane messages.
//!
//! Needs no threads, async runtime or randomness source, so it builds for
//! `wasm32-unknown-unknown`. The `rand` feature adds key and nonce generation from the operating
//! system and is for native targets only; it does not build for `wasm32-unknown-unknown`.
//!
//! # Limits of version 1
//!
//! - A device cannot be removed or revoked, and the library secret cannot be rotated. Removing a
//!   device's key from a server would not lock it out anyway: a holder of the secret can derive
//!   the recovery key and add itself back. Real revocation needs a new secret delivered to the
//!   remaining devices, which is future work.
//! - Every key epoch's key derives from the one library secret, so a new epoch does not exclude
//!   anyone who held the secret. The protocol sets no server-side rules for epochs: a server
//!   neither checks nor orders them.

pub mod api;
mod batch;
mod envelope;
mod hex;
mod keys;
mod recovery;
mod request;
mod wire;

pub use batch::{BATCH_FORMAT, decode_batch, encode_batch};
pub use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
pub use envelope::{ENVELOPE_FORMAT, Envelope, EnvelopeError, Opened, open, seal, verify_envelope};
pub use keys::{DeviceId, LibraryId, LibraryKeys};
#[cfg(feature = "rand")]
pub use keys::{generate_signing_key, random_nonce, random_request_nonce};
pub use recovery::RecoveryError;
pub use request::{
    HEADER_DEVICE, HEADER_NONCE, HEADER_SIGNATURE, HEADER_TIMESTAMP, RequestAuth, RequestError,
    sign_request, verify_request,
};
pub use wire::DecodeError;

/// The protocol version a server reports in [`api::Capabilities`]. It changes when any message,
/// encoding or derivation in this crate changes incompatibly.
pub const PROTOCOL_VERSION: u32 = 1;

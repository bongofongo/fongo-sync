//! The wire protocol between devices and a sync server: batch encoding, library and device keys,
//! sealed batches the server can verify but not read, request signatures, recovery codes and the
//! control-plane messages.
//!
//! Needs no threads, async runtime or randomness source, so it builds for
//! `wasm32-unknown-unknown`. The `rand` feature adds key and nonce generation from the operating
//! system.

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
pub use keys::{generate_signing_key, random_nonce};
pub use recovery::RecoveryError;
pub use request::{
    HEADER_DEVICE, HEADER_SIGNATURE, HEADER_TIMESTAMP, RequestAuth, RequestError, sign_request,
    verify_request,
};
pub use wire::DecodeError;

/// The protocol version a server reports in [`api::Capabilities`]. It changes when any message,
/// encoding or derivation in this crate changes incompatibly.
pub const PROTOCOL_VERSION: u32 = 1;

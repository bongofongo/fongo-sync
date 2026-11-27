//! The wire protocol between devices and a sync server: batch encoding, library and device keys,
//! sealed batches the server can verify but not read, request signatures, recovery codes, the
//! format of stored files and the control-plane messages.
//!
//! Needs no threads, async runtime or randomness source, so it builds for
//! `wasm32-unknown-unknown`. The `rand` feature adds key and nonce generation from the operating
//! system and is for native targets only; it does not build for `wasm32-unknown-unknown`.
//!
//! # Files
//!
//! A file travels apart from the batches. A change names it with a [`FileRef`], its SHA-256
//! [`ContentHash`] and true size, encoded as an ordinary blob value, so the batch format and
//! merging know nothing of files. The server stores it under a [`BlobId`], an HMAC of the hash
//! under a key derived from the library secret. Its bytes, padded with zeros to [`padme`] of its
//! size, are cut into chunks of [`FILE_CHUNK_LEN`] bytes and each chunk is sealed with
//! XChaCha20-Poly1305 under a key derived from the library secret and the hash, by
//! [`FileSealer`], and opened by [`FileOpener`]. Each chunk's nonce is derived from the chunk
//! itself, so one content seals to the same bytes on every device of a library, a chunk has one
//! stored form, and different bytes never share a nonce. The server learns each file's padded
//! size, which requests touch it, which envelopes and devices claim or retire it, and that two
//! uploads of it are one file; not its content, its hash or its true size.
//!
//! [`api`] holds the routes that upload a file in chunks or whole, read it back, ask which files
//! a server holds, and let devices claim and retire files so that a server can delete those
//! nothing refers to. This crate defines the format and the messages; no client or server for
//! them exists in this repository yet.
//!
//! # What a device relies on the server for
//!
//! Envelopes are sealed and signed, so a server can neither read a change nor forge or alter one:
//! a device refuses an envelope whose signature does not verify under a key the device list
//! holds. A file's chunks are sealed in the same way, and opening one checks it. A device relies on
//! the server for the rest:
//!
//! - completeness: every envelope the server answered with a sequence number is kept, and is
//!   returned to every device that reads past the one before it. A device never receives an
//!   envelope that is dropped or withheld from it.
//! - one log: every device reads the same envelope at each sequence number. Devices shown
//!   different logs can diverge for good.
//! - honest sequence numbers: each library's envelopes are numbered 1, 2, 3 and so on without
//!   gaps, the number in [`api::Appended`] is the one the envelope is read back at, and the head a
//!   server reports or checks against is its log's.
//! - the device list: which keys' envelopes and requests a library accepts, as the limits below
//!   say.
//! - deleting files only by the rules in [`api`]. A file deleted early is unavailable, not altered.
//!
//! A client can observe one kind of failure: a page whose numbers do not run on from its cursor
//! without a gap, repeat or step back. It cannot observe a log truncated or rolled back, which
//! looks like nothing new; a fork, where devices are shown different envelopes at one number; a
//! number in [`api::Appended`] that the envelope is not read back at; or an envelope withheld from
//! it. This version detects none of these. It does not rely on a server storing an identical
//! envelope only once, since a copy is applied idempotently, nor on notifications arriving, since
//! they are hints, nor on anything a server reports about a library (`last_seen`, its status) for
//! the library's content.
//!
//! # Limits of version 1
//!
//! - The library secret cannot be rotated. A device can be removed through
//!   [`api::RemoveDevice`]: the server then refuses its requests and no longer waits for it before
//!   deleting files. That is an act of the server only, and does not exclude a holder of the
//!   secret: they can derive the recovery key and add a device again, and can read anything
//!   sealed under the secret that they obtain. Excluding one needs a new secret delivered to the
//!   remaining devices, which is future work; a device may register a [`WrapKey`] for it now,
//!   which nothing in this version uses. Each wrap key is signed by its device's key, but the
//!   device list as a whole is still taken from the server, which can add or leave out a key.
//! - Every key epoch's key derives from the one library secret, so a new epoch does not exclude
//!   anyone who held the secret. The protocol sets no server-side rules for epochs: a server
//!   neither checks nor orders them.
//! - A file's key derives from the library secret and its content alone, so whoever holds the
//!   secret can read every file of the library they obtain.

pub mod api;
mod batch;
mod envelope;
mod file;
mod hex;
mod keys;
mod recovery;
mod request;
mod wire;

pub use batch::{BATCH_FORMAT, decode_batch, encode_batch};
pub use ed25519_dalek::{Signature, SigningKey, VerifyingKey};
pub use envelope::{ENVELOPE_FORMAT, Envelope, EnvelopeError, Opened, open, seal, verify_envelope};
pub use file::{
    BlobId, ContentHash, FILE_CHUNK_LEN, FILE_REF_FORMAT, FileError, FileHasher, FileLayout,
    FileOpener, FileRef, FileSealer, SEALED_CHUNK_LEN, SealedChunk, padme,
};
pub use keys::{DeviceId, FileKeyRoot, FileNameKey, LibraryId, LibraryKeys, WrapKey};
#[cfg(feature = "rand")]
pub use keys::{generate_signing_key, random_nonce, random_request_nonce};
pub use recovery::RecoveryError;
pub use request::{
    HEADER_DEVICE, HEADER_NONCE, HEADER_SIGNATURE, HEADER_TIMESTAMP, RequestAuth, RequestError,
    sign_request, sign_request_digest, verify_request, verify_request_digest,
};
pub use wire::DecodeError;

/// The protocol version a server reports in [`api::Capabilities`]. It changes when any message,
/// encoding or derivation in this crate changes incompatibly.
pub const PROTOCOL_VERSION: u32 = 1;

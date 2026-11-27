//! Control-plane messages, sent as JSON, the paths they go to, the error vocabulary and the
//! change notification. Every request except `GET` [`CAPABILITIES_PATH`] carries
//! [`RequestAuth`](crate::RequestAuth) headers.
//!
//! The blob routes ([`BLOB_PATH`] to [`BLOBS_RETIRE_PATH`]), [`SETTLED_PATH`], [`STATUS_PATH`],
//! [`ENTITLEMENT_PATH`] and [`REMOVE_DEVICE_PATH`] are for devices only: the recovery key is
//! refused with [`ErrorCode::Forbidden`]. A removed device is refused on every route with
//! [`ErrorCode::DeviceRemoved`].
//!
//! A library that is not entitled to the service is refused [`ErrorCode::NotEntitled`] when
//! appending an envelope (at either batch path), claiming, retiring or writing a blob; every `GET`, a blob query, adding a
//! device, [`STATUS_PATH`], [`ENTITLEMENT_PATH`], [`SETTLED_PATH`] and [`REMOVE_DEVICE_PATH`]
//! still work. A library over its allowance is refused [`ErrorCode::QuotaExceeded`] when beginning
//! or writing a blob, and everything else works.
//!
//! # Blob routes at their edges
//!
//! | request | when the blob | answer |
//! |---|---|---|
//! | a chunk upload, `complete`, or a `GET` | was never begun | [`ErrorCode::BlobNotFound`] |
//! | a chunk upload | is begun, but the index is not below its chunk count or the body is not that chunk's [`BlobInfo::stored_chunk_len`] | [`ErrorCode::MalformedRequest`] |
//! | a chunk upload | is complete | success if the bytes equal the stored chunk, else [`ErrorCode::BlobMismatch`]; nothing changes |
//! | `complete` | is complete | success if the digest matches, else [`ErrorCode::BlobMismatch`] |
//! | `whole` | was begun with the same one-chunk shape | stores the chunk and completes |
//! | `whole` | was begun with another shape | [`ErrorCode::BlobMismatch`] |
//! | `whole` | is complete | success if the bytes equal the stored chunk, else [`ErrorCode::BlobMismatch`] |
//! | `GET` a chunk or the blob | is not complete | [`ErrorCode::BlobIncomplete`] |
//! | begin | is complete with the same shape | `{complete: true, have: every chunk}` |
//!
//! # Deleting files
//!
//! A server deletes a complete blob only when no change in the library can refer to it any more.
//! The rules rest on one definition. A set of changes refers to a file when, for some field the
//! application marks as naming files and some collection and gid, the field's strategy winner
//! among the set's changes to that collection, gid and field is a reference to the file. Every
//! change to it whose clock is valid counts, whatever the row's liveness, group or collection: a deleted
//! or merged row still refers to its file until a later change to that field replaces it. Adding
//! changes to a set therefore makes it refer to a file only through an added change that sets a
//! reference to it. What an application shows is not monotone in this way (removing one member of
//! a merged group can show an older member's file again without any change naming it), which is
//! why it is not the definition.
//!
//! A device:
//!
//! - sends each envelope any of whose changes sets a file reference to [`BATCHES_CLAIMING_PATH`],
//!   with the id of every file it sets, so that the claim is recorded at the envelope's sequence
//!   number in the same step as the append. A server whose [`Capabilities::blobs`] is false gets
//!   it at [`BATCHES_PATH`].
//! - retires a file ([`RetireBlobs`]) only when it has merged every envelope through `through`,
//!   has nothing unsent, and the changes through `through` do not refer to the file; its rows held
//!   back count as referring.
//! - reports [`Settled`] `seq` only when it has merged every envelope through `seq` and has
//!   nothing unsent.
//! - may claim at [`BLOBS_CLAIM_PATH`] without appending, recorded at the current head: to keep a
//!   file a row held back needs, or to learn which files the server lacks.
//!
//! A server:
//!
//! - treats a retirement `(through, id)` as void once the id has a claim recorded at a sequence
//!   number at or above `through`, whether the claim arrived before or after the retirement.
//! - deletes a complete blob only when it has a retirement that is not void, every device in
//!   [`Devices::settling`] has reported [`Settled`] at or above that retirement's `through` (one
//!   that never reported counts as 0), and [`Capabilities::blob_wait_days`] have passed since the
//!   retirement arrived. A deletion and a claim of the same id are ordered: a claim after the
//!   deletion finds the blob missing.
//!
//! Why no change that is or will be in the log refers to a deleted file: an honest retirement at
//! `through` means the changes through `through` do not refer to it. Every later set of changes
//! holds those, so one that refers to the file holds a later change setting a reference to it. If
//! that change was appended, it was appended with a claim at a sequence number above `through`,
//! which voids the retirement. If it is not yet appended, it is unsent on a device, and a device
//! holding unsent changes has not settled; every device that counts has settled at or above
//! `through`, so it made the change after settling.
//!
//! That last case is the limit: a device that newly refers to a file while it is retired, and
//! sends the change after the file is deleted, finds it missing. If it holds the file it uploads
//! it again; if not, the file is unavailable. The same holds for a removed device that joins
//! again and sends changes it had not sent. A device that refers anew to a file it does not hold
//! can narrow the window by claiming it at [`BLOBS_CLAIM_PATH`] at once.

mod blobs;
mod error;
mod notify;
mod route;

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

pub use blobs::{
    BlobDigest, BlobIds, BlobInfo, BlobState, ClaimingAppend, CompleteBlob, CompleteBlobs,
    DigestError, Entitlement, HaveChunk, LibraryStatus, MissingBlobs, RemoveDevice, RetireBlobs,
    Settled,
};
pub use error::{ErrorBody, ErrorCode};
pub use notify::{NOTIFICATION_FORMAT, Notification};
pub use route::{MalformedQuery, Route};

use crate::hex::{serde_key, serde_sig};
use crate::keys::{DeviceId, LibraryId, WrapKey};
use crate::wire::{DecodeError, Reader, put_blob, put_varint};

/// `GET`: [`Capabilities`].
pub const CAPABILITIES_PATH: &str = "/v1/capabilities";
/// `POST` [`CreateLibrary`]: [`Devices`]. Authenticated by the new device's own key.
pub const LIBRARIES_PATH: &str = "/v1/libraries";
/// `GET`: [`Devices`]. `POST` [`AddDevice`]: [`Devices`]. `{library}` is the library id in hex.
pub const DEVICES_PATH: &str = "/v1/libraries/{library}/devices";
/// `POST` envelope bytes: [`Appended`]. `GET` with [`ReadBatches`] as the query: a [`BatchPage`].
pub const BATCHES_PATH: &str = "/v1/libraries/{library}/batches";
/// `POST` a [`ClaimingAppend`] (binary): [`Appended`]. An append that also claims the blobs its
/// envelope's changes set references to.
pub const BATCHES_CLAIMING_PATH: &str = "/v1/libraries/{library}/batches/claiming";
/// `GET` upgraded to a WebSocket: a stream of [`Notification`]s. Offered when
/// [`Capabilities::notifications`] is true.
pub const NOTIFICATIONS_PATH: &str = "/v1/libraries/{library}/notifications";

/// `POST` [`BlobInfo`]: [`BlobState`], beginning or resuming an upload; idempotent, and
/// [`ErrorCode::BlobMismatch`] if an earlier begin gave another size or chunk count. `GET`:
/// [`BlobInfo`] of a complete blob, else [`ErrorCode::BlobNotFound`] or
/// [`ErrorCode::BlobIncomplete`]. `{id}` is the [`BlobId`](crate::BlobId) in hex.
pub const BLOB_PATH: &str = "/v1/libraries/{library}/blobs/{id}";
/// `POST` one stored chunk (binary) of a begun blob, of the length [`BlobInfo::stored_chunk_len`]
/// gives, answered with no body. Until the blob is complete it replaces whatever was stored at that
/// index; see [`BlobState`]. `GET`: the stored chunk (binary) of a complete blob. `{n}` is the chunk
/// index in decimal.
pub const BLOB_CHUNK_PATH: &str = "/v1/libraries/{library}/blobs/{id}/chunks/{n}";
/// `POST` [`CompleteBlob`]: [`BlobInfo`], or [`ErrorCode::BlobIncomplete`] unless every chunk is
/// stored, or [`ErrorCode::BlobMismatch`] if the chunks stored are not those the digest names,
/// leaving the blob incomplete. From then on the blob is readable and never changes. Idempotent.
pub const BLOB_COMPLETE_PATH: &str = "/v1/libraries/{library}/blobs/{id}/complete";
/// `POST` the only stored chunk (binary) of a one-chunk blob: [`BlobInfo`]. Begins, stores and
/// completes in one request; it needs no [`CompleteBlob`], since the request itself holds the
/// chunk it completes with.
pub const BLOB_WHOLE_PATH: &str = "/v1/libraries/{library}/blobs/{id}/whole";
/// `POST` [`BlobIds`]: [`CompleteBlobs`].
pub const BLOBS_QUERY_PATH: &str = "/v1/libraries/{library}/blobs/query";
/// `POST` [`BlobIds`]: [`MissingBlobs`].
pub const BLOBS_CLAIM_PATH: &str = "/v1/libraries/{library}/blobs/claim";
/// `POST` [`RetireBlobs`], answered with no body.
pub const BLOBS_RETIRE_PATH: &str = "/v1/libraries/{library}/blobs/retire";
/// `POST` [`Settled`], answered with no body.
pub const SETTLED_PATH: &str = "/v1/libraries/{library}/settled";
/// `GET`: [`LibraryStatus`].
pub const STATUS_PATH: &str = "/v1/libraries/{library}/status";
/// `POST` [`Entitlement`]: [`LibraryStatus`].
pub const ENTITLEMENT_PATH: &str = "/v1/libraries/{library}/entitlement";
/// `POST` [`RemoveDevice`]: [`Devices`]. Naming the recovery key is [`ErrorCode::Forbidden`].
pub const REMOVE_DEVICE_PATH: &str = "/v1/libraries/{library}/devices/remove";

/// Content type of envelope request bodies and [`BatchPage`] responses.
pub const BINARY_CONTENT_TYPE: &str = "application/octet-stream";

const PROOF_LABEL: &[u8] = b"fongo-sync/v1/create-library\0";
const WRAP_PROOF_LABEL: &[u8] = b"fongo-sync/v1/wrap-key\0";
const PAGE_FORMAT: u8 = 1;
const PAGE_MORE: u8 = 1;

/// [`DEVICES_PATH`] for one library.
pub fn devices_path(library: LibraryId) -> String {
    DEVICES_PATH.replace("{library}", &library.to_hex())
}

/// [`BATCHES_PATH`] for one library.
pub fn batches_path(library: LibraryId) -> String {
    BATCHES_PATH.replace("{library}", &library.to_hex())
}

/// [`NOTIFICATIONS_PATH`] for one library.
pub fn notifications_path(library: LibraryId) -> String {
    NOTIFICATIONS_PATH.replace("{library}", &library.to_hex())
}

/// What a server offers. Absent quotas are unlimited or undisclosed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub protocol_version: u32,
    /// Whether [`BATCHES_CLAIMING_PATH`], the blob routes ([`BLOB_PATH`] to
    /// [`BLOBS_RETIRE_PATH`]), [`SETTLED_PATH`], [`STATUS_PATH`] and [`ENTITLEMENT_PATH`] are
    /// served.
    pub blobs: bool,
    /// Whether [`NOTIFICATIONS_PATH`] is served.
    pub notifications: bool,
    /// How far a request timestamp may be from the server's clock, either side, in seconds.
    pub skew_secs: u64,
    /// Largest envelope accepted, in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_bytes: Option<u64>,
    /// Largest total of stored envelope bytes per library.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_library_bytes: Option<u64>,
    /// Most devices per library, not counting the recovery key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_devices: Option<u32>,
    /// Most envelopes in one [`BatchPage`], whatever `limit` asks for.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_page_batches: Option<u32>,
    /// Most envelope bytes in one [`BatchPage`]; a single larger envelope still comes alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_page_bytes: Option<u64>,
    /// Largest blob accepted, in stored bytes ([`BlobInfo::size`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_blob_bytes: Option<u64>,
    /// Most ids in one [`BlobIds`] sent to [`BLOBS_QUERY_PATH`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_query_ids: Option<u32>,
    /// Days a retired blob is kept at least, counted from when its retirement arrived; see
    /// [the module documentation](self#deleting-files).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_wait_days: Option<u32>,
    /// Days without activity after which an incomplete upload may be dropped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob_incomplete_days: Option<u32>,
    /// Whether [`REMOVE_DEVICE_PATH`] is served.
    #[serde(default, skip_serializing_if = "is_false")]
    pub device_removal: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Registers a library with its first device and its recovery key. The server must refuse an id
/// already taken and a `proof` that does not verify.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawCreateLibrary")]
pub struct CreateLibrary {
    pub library: LibraryId,
    #[serde(with = "serde_key")]
    pub device_key: VerifyingKey,
    /// [`LibraryKeys::recovery_signing_key`](crate::LibraryKeys::recovery_signing_key)'s public half.
    #[serde(with = "serde_key")]
    pub recovery_key: VerifyingKey,
    /// The device key's signature over `fongo-sync/v1/create-library` and a zero byte, the
    /// library id, the device key and the recovery key.
    #[serde(with = "serde_sig")]
    pub proof: Signature,
    /// The first device's wrap key, stored and returned in its [`DeviceRecord`].
    #[serde(flatten)]
    pub wrap: Option<SignedWrapKey>,
    /// An opaque token for the server's own policy. Nothing signs it: whoever holds it may
    /// present it, as with [`Entitlement`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entitlement: Option<String>,
}

impl CreateLibrary {
    pub fn new(
        library: LibraryId,
        device: &SigningKey,
        recovery_key: VerifyingKey,
    ) -> CreateLibrary {
        let device_key = device.verifying_key();
        CreateLibrary {
            library,
            device_key,
            recovery_key,
            proof: device.sign(&proof_message(library, &device_key, &recovery_key)),
            wrap: None,
            entitlement: None,
        }
    }

    /// A request that also registers the first device's wrap key, signed by the device key.
    pub fn with_wrap_key(
        library: LibraryId,
        device: &SigningKey,
        recovery_key: VerifyingKey,
        wrap_key: WrapKey,
    ) -> CreateLibrary {
        CreateLibrary {
            wrap: Some(SignedWrapKey::new(library, device, wrap_key)),
            ..CreateLibrary::new(library, device, recovery_key)
        }
    }

    /// Whether `proof` shows the caller holds `device_key`'s private half.
    pub fn verify(&self) -> bool {
        self.device_key
            .verify_strict(
                &proof_message(self.library, &self.device_key, &self.recovery_key),
                &self.proof,
            )
            .is_ok()
    }

    /// Whether the request carries a wrap key that the device key signed for this library.
    pub fn verify_wrap(&self) -> bool {
        self.wrap
            .as_ref()
            .is_some_and(|wrap| wrap.verify(self.library, &self.device_key))
    }
}

fn proof_message(library: LibraryId, device: &VerifyingKey, recovery: &VerifyingKey) -> Vec<u8> {
    let mut out = PROOF_LABEL.to_vec();
    out.extend_from_slice(&library.0);
    out.extend_from_slice(device.as_bytes());
    out.extend_from_slice(recovery.as_bytes());
    out
}

/// A device's [`WrapKey`] with the device key's signature over it, so that any device shown it
/// can check that the device chose it and not whoever passed it on. In JSON it is the two fields
/// `wrap_key` and `wrap_proof` of the message that carries it, both or neither.
///
/// `wrap_proof` signs `fongo-sync/v1/wrap-key` and a zero byte, the library id, the device's
/// public key and the wrap key. A server refuses a wrap key whose proof does not verify, with
/// [`ErrorCode::InvalidProof`]. The proof carries no counter, so a server can present any wrap key
/// a device ever signed for the library; replacing a wrap key will need a new label or a counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SignedWrapKey {
    pub wrap_key: WrapKey,
    #[serde(with = "serde_sig")]
    pub wrap_proof: Signature,
}

impl SignedWrapKey {
    /// `wrap_key`, signed by `device` for `library`.
    pub fn new(library: LibraryId, device: &SigningKey, wrap_key: WrapKey) -> SignedWrapKey {
        SignedWrapKey {
            wrap_key,
            wrap_proof: device.sign(&wrap_message(library, &device.verifying_key(), &wrap_key)),
        }
    }

    /// Whether `device_key` signed this wrap key for `library`.
    pub fn verify(&self, library: LibraryId, device_key: &VerifyingKey) -> bool {
        device_key
            .verify_strict(
                &wrap_message(library, device_key, &self.wrap_key),
                &self.wrap_proof,
            )
            .is_ok()
    }
}

fn wrap_message(library: LibraryId, device: &VerifyingKey, wrap_key: &WrapKey) -> Vec<u8> {
    let mut out = WRAP_PROOF_LABEL.to_vec();
    out.extend_from_slice(&library.0);
    out.extend_from_slice(device.as_bytes());
    out.extend_from_slice(&wrap_key.0);
    out
}

/// Adds a device. Signed, through [`RequestAuth`](crate::RequestAuth), by a device already in the
/// library or by its recovery key.
///
/// A key already listed and not removed is accepted and changes nothing; a removed one is
/// restored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawAddDevice")]
pub struct AddDevice {
    #[serde(with = "serde_key")]
    pub device_key: VerifyingKey,
    /// The new device's wrap key, signed by the new device's key, stored and returned in its
    /// [`DeviceRecord`].
    #[serde(flatten)]
    pub wrap: Option<SignedWrapKey>,
}

impl AddDevice {
    pub fn new(device_key: VerifyingKey) -> AddDevice {
        AddDevice {
            device_key,
            wrap: None,
        }
    }

    /// Adds `device` to `library` with its wrap key. A device that adds another without its
    /// signing key takes the [`SignedWrapKey`] the new device made and sets `wrap` itself.
    pub fn with_wrap_key(library: LibraryId, device: &SigningKey, wrap_key: WrapKey) -> AddDevice {
        AddDevice {
            device_key: device.verifying_key(),
            wrap: Some(SignedWrapKey::new(library, device, wrap_key)),
        }
    }

    /// Whether the request carries a wrap key that the new device's key signed for `library`.
    pub fn verify_wrap(&self, library: LibraryId) -> bool {
        self.wrap
            .as_ref()
            .is_some_and(|wrap| wrap.verify(library, &self.device_key))
    }
}

/// One key the library knows. Its device id is [`DeviceId::of`](crate::DeviceId::of) the key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawDeviceRecord")]
pub struct DeviceRecord {
    #[serde(with = "serde_key")]
    pub public_key: VerifyingKey,
    /// The library's recovery key: it may authenticate requests but never signs envelopes.
    pub recovery: bool,
    /// The wrap key the device registered, if any.
    #[serde(flatten)]
    pub wrap: Option<SignedWrapKey>,
    /// Removed through [`REMOVE_DEVICE_PATH`]: the server refuses its requests, and it no longer
    /// holds back the deletion of retired blobs.
    #[serde(default, skip_serializing_if = "is_false")]
    pub removed: bool,
    /// Unix seconds of the device's last accepted request, possibly coarse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<u64>,
}

impl DeviceRecord {
    /// Whether the record carries a wrap key that its own key signed for `library`. The server
    /// can drop a wrap key, but cannot put in one the device did not sign.
    pub fn verify_wrap(&self, library: LibraryId) -> bool {
        self.wrap
            .as_ref()
            .is_some_and(|wrap| wrap.verify(library, &self.public_key))
    }
}

#[derive(Deserialize)]
struct RawCreateLibrary {
    library: LibraryId,
    #[serde(with = "serde_key")]
    device_key: VerifyingKey,
    #[serde(with = "serde_key")]
    recovery_key: VerifyingKey,
    #[serde(with = "serde_sig")]
    proof: Signature,
    #[serde(default)]
    wrap_key: Option<WrapKey>,
    #[serde(default)]
    wrap_proof: Option<ProofHex>,
    #[serde(default)]
    entitlement: Option<String>,
}

#[derive(Deserialize)]
struct RawAddDevice {
    #[serde(with = "serde_key")]
    device_key: VerifyingKey,
    #[serde(default)]
    wrap_key: Option<WrapKey>,
    #[serde(default)]
    wrap_proof: Option<ProofHex>,
}

#[derive(Deserialize)]
struct RawDeviceRecord {
    #[serde(with = "serde_key")]
    public_key: VerifyingKey,
    recovery: bool,
    #[serde(default)]
    wrap_key: Option<WrapKey>,
    #[serde(default)]
    wrap_proof: Option<ProofHex>,
    #[serde(default)]
    removed: Option<bool>,
    #[serde(default)]
    last_seen: Option<u64>,
}

#[derive(Deserialize)]
struct ProofHex(#[serde(with = "serde_sig")] Signature);

/// Why a message was refused that carried a wrap key without its proof, or a proof without its
/// key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("wrap_key and wrap_proof come together or not at all")]
pub struct UnpairedWrapKey;

fn pair(
    wrap_key: Option<WrapKey>,
    wrap_proof: Option<ProofHex>,
) -> Result<Option<SignedWrapKey>, UnpairedWrapKey> {
    match (wrap_key, wrap_proof) {
        (None, None) => Ok(None),
        (Some(wrap_key), Some(ProofHex(wrap_proof))) => Ok(Some(SignedWrapKey {
            wrap_key,
            wrap_proof,
        })),
        _ => Err(UnpairedWrapKey),
    }
}

impl TryFrom<RawCreateLibrary> for CreateLibrary {
    type Error = UnpairedWrapKey;

    fn try_from(raw: RawCreateLibrary) -> Result<CreateLibrary, UnpairedWrapKey> {
        Ok(CreateLibrary {
            library: raw.library,
            device_key: raw.device_key,
            recovery_key: raw.recovery_key,
            proof: raw.proof,
            wrap: pair(raw.wrap_key, raw.wrap_proof)?,
            entitlement: raw.entitlement,
        })
    }
}

impl TryFrom<RawAddDevice> for AddDevice {
    type Error = UnpairedWrapKey;

    fn try_from(raw: RawAddDevice) -> Result<AddDevice, UnpairedWrapKey> {
        Ok(AddDevice {
            device_key: raw.device_key,
            wrap: pair(raw.wrap_key, raw.wrap_proof)?,
        })
    }
}

impl TryFrom<RawDeviceRecord> for DeviceRecord {
    type Error = UnpairedWrapKey;

    fn try_from(raw: RawDeviceRecord) -> Result<DeviceRecord, UnpairedWrapKey> {
        Ok(DeviceRecord {
            public_key: raw.public_key,
            recovery: raw.recovery,
            wrap: pair(raw.wrap_key, raw.wrap_proof)?,
            removed: raw.removed.unwrap_or(false),
            last_seen: raw.last_seen,
        })
    }
}

/// Every key a library knows, in the order they were added.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Devices {
    pub devices: Vec<DeviceRecord>,
}

impl Devices {
    /// The devices a server waits for before deleting a retired blob: every key that is neither
    /// removed nor the recovery key.
    pub fn settling(&self) -> impl Iterator<Item = &DeviceRecord> {
        self.devices.iter().filter(|d| !d.removed && !d.recovery)
    }

    /// Applies a [`RemoveDevice`]: marks `device` removed. The recovery key is
    /// [`ErrorCode::Forbidden`]; an id not listed, or a device already removed, changes nothing.
    pub fn remove(&mut self, device: DeviceId) -> Result<(), ErrorCode> {
        match self
            .devices
            .iter_mut()
            .find(|d| DeviceId::of(&d.public_key) == device)
        {
            Some(d) if d.recovery => Err(ErrorCode::Forbidden),
            Some(d) => {
                d.removed = true;
                Ok(())
            }
            None => Ok(()),
        }
    }
}

/// The response to an appended envelope: the sequence number the server gave it. Sequence numbers
/// start at 1 and increase by one per library.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Appended {
    pub seq: u64,
}

/// Query of a batch read: envelopes with sequence numbers greater than `after`, oldest first, at
/// most `limit` of them (at least 1). Sent as [`ReadBatches::to_query`] and read with
/// [`ReadBatches::parse_query`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadBatches {
    pub after: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

/// One stored envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBatch {
    pub seq: u64,
    pub envelope: Vec<u8>,
}

/// A page of envelopes in sequence order. `more` says the server holds later ones.
///
/// Bytes: format, flags (bit 0 is `more`), a LEB128 count, then per batch a big-endian u64
/// sequence number and the envelope as LEB128 length then bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BatchPage {
    pub batches: Vec<StoredBatch>,
    pub more: bool,
}

impl BatchPage {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![PAGE_FORMAT, if self.more { PAGE_MORE } else { 0 }];
        put_varint(&mut out, self.batches.len() as u64);
        for batch in &self.batches {
            out.extend_from_slice(&batch.seq.to_be_bytes());
            put_blob(&mut out, &batch.envelope);
        }
        out
    }

    /// Fails on anything `encode` would not write. Allocation is bounded by a constant multiple of
    /// the input's length, because every count and length is checked against the remaining input
    /// first.
    pub fn decode(bytes: &[u8]) -> Result<BatchPage, DecodeError> {
        let mut r = Reader::new(bytes);
        let version = r.u8()?;
        if version != PAGE_FORMAT {
            return Err(DecodeError::UnsupportedVersion(version));
        }
        let flags = r.u8()?;
        if flags & !PAGE_MORE != 0 {
            return Err(DecodeError::UnsupportedFlags(flags));
        }
        let count = r.len(8 + 1)?;
        let mut batches = Vec::with_capacity(count);
        for _ in 0..count {
            let seq = r.u64()?;
            let envelope = r.blob()?.to_vec();
            batches.push(StoredBatch { seq, envelope });
        }
        r.finish()?;
        Ok(BatchPage {
            batches,
            more: flags & PAGE_MORE != 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;

    #[test]
    fn create_library_proves_the_device_key_and_survives_json() {
        let device = SigningKey::from_bytes(&[1; 32]);
        let recovery = SigningKey::from_bytes(&[2; 32]).verifying_key();
        let request = CreateLibrary::new(LibraryId([3; 16]), &device, recovery);
        assert!(request.verify());
        let text = serde_json::to_string(&request).unwrap();
        let back: CreateLibrary = serde_json::from_str(&text).unwrap();
        assert_eq!(back, request);
        assert!(back.verify());

        let mut forged = request.clone();
        forged.library = LibraryId([4; 16]);
        assert!(!forged.verify());
        let mut swapped = request.clone();
        swapped.recovery_key = device.verifying_key();
        assert!(!swapped.verify());

        let mut entitled = request.clone();
        entitled.entitlement = Some("token".into());
        assert!(entitled.verify());
        assert!(!request.verify_wrap());
    }

    #[test]
    fn a_wrap_key_is_signed_by_its_device_and_leaves_the_proof_alone() {
        let device = SigningKey::from_bytes(&[1; 32]);
        let recovery = SigningKey::from_bytes(&[2; 32]).verifying_key();
        let library = LibraryId([3; 16]);
        let request = CreateLibrary::with_wrap_key(library, &device, recovery, WrapKey([5; 32]));
        assert!(request.verify() && request.verify_wrap());
        assert_eq!(
            request.proof,
            CreateLibrary::new(library, &device, recovery).proof
        );
        let back: CreateLibrary =
            serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
        assert_eq!(back, request);

        let wrap = request.wrap.unwrap();
        let mut replaced = wrap;
        replaced.wrap_key = WrapKey([6; 32]);
        assert!(!replaced.verify(library, &device.verifying_key()));
        assert!(!wrap.verify(LibraryId([4; 16]), &device.verifying_key()));
        assert!(!wrap.verify(library, &recovery));

        let add = AddDevice::with_wrap_key(library, &device, WrapKey([5; 32]));
        assert!(add.verify_wrap(library) && !add.verify_wrap(LibraryId([4; 16])));
        assert!(!AddDevice::new(device.verifying_key()).verify_wrap(library));
        let record = DeviceRecord {
            public_key: device.verifying_key(),
            recovery: false,
            wrap: add.wrap,
            removed: false,
            last_seen: None,
        };
        assert!(record.verify_wrap(library));
        let impostor = DeviceRecord {
            public_key: recovery,
            ..record
        };
        assert!(!impostor.verify_wrap(library));
    }

    #[test]
    fn a_wrap_key_without_its_proof_is_refused() {
        let device = SigningKey::from_bytes(&[1; 32]);
        let key_hex = crate::hex::encode(device.verifying_key().as_bytes());
        let add = AddDevice::with_wrap_key(LibraryId([3; 16]), &device, WrapKey([5; 32]));
        let full = serde_json::to_value(&add).unwrap();
        let proof = full["wrap_proof"].clone();
        let wrap_key = "05".repeat(32);
        for body in [
            json!({"device_key": key_hex, "wrap_key": wrap_key}),
            json!({"device_key": key_hex, "wrap_proof": proof}),
        ] {
            assert!(serde_json::from_value::<AddDevice>(body.clone()).is_err());
            let mut record = body.clone();
            record["public_key"] = record["device_key"].take();
            record["recovery"] = json!(false);
            assert!(serde_json::from_value::<DeviceRecord>(record).is_err());
        }
        let request = CreateLibrary::with_wrap_key(
            LibraryId([3; 16]),
            &device,
            device.verifying_key(),
            WrapKey([5; 32]),
        );
        let mut half = serde_json::to_value(&request).unwrap();
        half.as_object_mut().unwrap().remove("wrap_proof");
        assert!(serde_json::from_value::<CreateLibrary>(half).is_err());
        assert_eq!(serde_json::from_value::<AddDevice>(full).unwrap(), add);
    }

    #[test]
    fn json_shapes_are_plain() {
        let caps = Capabilities {
            protocol_version: 1,
            blobs: false,
            notifications: true,
            skew_secs: 300,
            max_batch_bytes: Some(10),
            max_library_bytes: None,
            max_devices: None,
            max_page_batches: Some(256),
            max_page_bytes: None,
            max_blob_bytes: None,
            max_query_ids: None,
            blob_wait_days: None,
            blob_incomplete_days: None,
            device_removal: false,
        };
        assert_eq!(
            serde_json::to_value(&caps).unwrap(),
            json!({
                "protocol_version": 1,
                "blobs": false,
                "notifications": true,
                "skew_secs": 300,
                "max_batch_bytes": 10,
                "max_page_batches": 256,
            })
        );
        let key = SigningKey::from_bytes(&[1; 32]).verifying_key();
        let add = AddDevice::new(key);
        assert_eq!(
            serde_json::to_value(&add).unwrap(),
            json!({"device_key": crate::hex::encode(key.as_bytes())})
        );
        let read: ReadBatches = serde_json::from_value(json!({"after": 7})).unwrap();
        assert_eq!(
            read,
            ReadBatches {
                after: 7,
                limit: None
            }
        );
        assert!(serde_json::from_value::<AddDevice>(json!({"device_key": "00"})).is_err());
    }

    #[test]
    fn the_new_fields_are_plain_and_optional() {
        let device = SigningKey::from_bytes(&[1; 32]);
        let key = device.verifying_key();
        let key_hex = crate::hex::encode(key.as_bytes());
        let wrap = SignedWrapKey::new(LibraryId([3; 16]), &device, WrapKey([2; 32]));
        let record = DeviceRecord {
            public_key: key,
            recovery: false,
            wrap: Some(wrap),
            removed: true,
            last_seen: Some(1_700_000_000),
        };
        let text = serde_json::to_string(&record).unwrap();
        assert_eq!(
            text,
            format!(
                r#"{{"public_key":"{key_hex}","recovery":false,"wrap_key":"{}","wrap_proof":"{}","removed":true,"last_seen":1700000000}}"#,
                "02".repeat(32),
                crate::hex::encode(&wrap.wrap_proof.to_bytes()),
            )
        );
        assert_eq!(serde_json::from_str::<DeviceRecord>(&text).unwrap(), record);
        let old: DeviceRecord =
            serde_json::from_value(json!({"public_key": key_hex, "recovery": true})).unwrap();
        assert_eq!((old.wrap, old.removed, old.last_seen), (None, false, None));

        let caps: Capabilities = serde_json::from_value(json!({
            "protocol_version": 1,
            "blobs": true,
            "notifications": false,
            "skew_secs": 300,
            "max_blob_bytes": 1,
            "max_query_ids": 3,
            "blob_wait_days": 7,
            "blob_incomplete_days": 4,
            "device_removal": true,
            "from_the_future": [1],
        }))
        .unwrap();
        assert_eq!(
            (
                caps.max_blob_bytes,
                caps.max_query_ids,
                caps.blob_wait_days,
                caps.blob_incomplete_days,
                caps.device_removal
            ),
            (Some(1), Some(3), Some(7), Some(4), true)
        );
        assert!(
            serde_json::from_value::<AddDevice>(json!({"device_key": key_hex, "wrap_key": "ab", "wrap_proof": crate::hex::encode(&wrap.wrap_proof.to_bytes())}))
                .is_err()
        );
        let mut create = CreateLibrary::new(LibraryId([3; 16]), &device, key);
        create.entitlement = Some("token".into());
        let value = serde_json::to_value(&create).unwrap();
        assert_eq!(value["entitlement"], json!("token"));
        assert!(value.get("wrap_key").is_none() && value.get("wrap_proof").is_none());
    }

    #[test]
    fn removal_spares_the_recovery_key_and_ignores_strangers() {
        let record = |seed: u8, recovery: bool| DeviceRecord {
            public_key: SigningKey::from_bytes(&[seed; 32]).verifying_key(),
            recovery,
            wrap: None,
            removed: false,
            last_seen: None,
        };
        let mut devices = Devices {
            devices: vec![record(1, false), record(2, true), record(3, false)],
        };
        let id = |seed: u8| DeviceId::of(&SigningKey::from_bytes(&[seed; 32]).verifying_key());
        assert_eq!(devices.settling().count(), 2);
        let before = devices.clone();
        assert_eq!(devices.remove(id(2)), Err(ErrorCode::Forbidden));
        assert_eq!(devices.remove(id(9)), Ok(()));
        assert_eq!(devices, before);
        assert_eq!(devices.remove(id(3)), Ok(()));
        assert_eq!(devices.remove(id(3)), Ok(()));
        assert!(devices.devices[2].removed);
        let settling: Vec<_> = devices.settling().map(|d| d.public_key).collect();
        assert_eq!(settling, [before.devices[0].public_key]);
    }

    #[test]
    fn a_null_removed_reads_as_false() {
        let key = crate::hex::encode(SigningKey::from_bytes(&[1; 32]).verifying_key().as_bytes());
        let record: DeviceRecord =
            serde_json::from_value(json!({"public_key": key, "recovery": false, "removed": null}))
                .unwrap();
        assert!(!record.removed);
    }

    #[test]
    fn paths_name_the_library_in_hex() {
        let id = LibraryId([0xab; 16]);
        assert_eq!(
            batches_path(id),
            format!("/v1/libraries/{}/batches", "ab".repeat(16))
        );
        assert_eq!(
            devices_path(id),
            format!("/v1/libraries/{}/devices", "ab".repeat(16))
        );
        assert_eq!(
            notifications_path(id),
            format!("/v1/libraries/{}/notifications", "ab".repeat(16))
        );
    }

    proptest! {
        #[test]
        fn pages_round_trip(
            raw in proptest::collection::vec((any::<u64>(), proptest::collection::vec(any::<u8>(), 0..64)), 0..8),
            more in any::<bool>(),
        ) {
            let page = BatchPage {
                batches: raw.into_iter().map(|(seq, envelope)| StoredBatch { seq, envelope }).collect(),
                more,
            };
            let bytes = page.encode();
            prop_assert_eq!(BatchPage::decode(&bytes), Ok(page));
            for cut in 0..bytes.len() {
                prop_assert!(BatchPage::decode(&bytes[..cut]).is_err());
            }
        }

        #[test]
        fn decoding_arbitrary_bytes_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = BatchPage::decode(&bytes);
        }
    }
}

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
//! appending an envelope, claiming, retiring or writing a blob; every `GET`, a blob query, adding a
//! device, [`STATUS_PATH`], [`ENTITLEMENT_PATH`], [`SETTLED_PATH`] and [`REMOVE_DEVICE_PATH`]
//! still work. A library over its allowance is refused [`ErrorCode::QuotaExceeded`] when beginning
//! or writing a blob, and everything else works.

mod blobs;
mod error;
mod notify;
mod route;

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

pub use blobs::{
    BlobIds, BlobInfo, BlobState, CompleteBlobs, Entitlement, LibraryStatus, MissingBlobs,
    RemoveDevice, RetireBlobs, Settled,
};
pub use error::{ErrorBody, ErrorCode};
pub use notify::{NOTIFICATION_FORMAT, Notification};
pub use route::{MalformedQuery, Route};

use crate::hex::{serde_key, serde_sig};
use crate::keys::{LibraryId, WrapKey};
use crate::wire::{DecodeError, Reader, put_blob, put_varint};

/// `GET`: [`Capabilities`].
pub const CAPABILITIES_PATH: &str = "/v1/capabilities";
/// `POST` [`CreateLibrary`]: [`Devices`]. Authenticated by the new device's own key.
pub const LIBRARIES_PATH: &str = "/v1/libraries";
/// `GET`: [`Devices`]. `POST` [`AddDevice`]: [`Devices`]. `{library}` is the library id in hex.
pub const DEVICES_PATH: &str = "/v1/libraries/{library}/devices";
/// `POST` envelope bytes: [`Appended`]. `GET` with [`ReadBatches`] as the query: a [`BatchPage`].
pub const BATCHES_PATH: &str = "/v1/libraries/{library}/batches";
/// `GET` upgraded to a WebSocket: a stream of [`Notification`]s. Offered when
/// [`Capabilities::notifications`] is true.
pub const NOTIFICATIONS_PATH: &str = "/v1/libraries/{library}/notifications";

/// `POST` [`BlobInfo`]: [`BlobState`], beginning or resuming an upload; idempotent, and
/// [`ErrorCode::BlobMismatch`] if an earlier begin gave another size or chunk count. `GET`:
/// [`BlobInfo`] of a complete blob, else [`ErrorCode::BlobNotFound`] or
/// [`ErrorCode::BlobIncomplete`]. `{id}` is the [`BlobId`](crate::BlobId) in hex.
pub const BLOB_PATH: &str = "/v1/libraries/{library}/blobs/{id}";
/// `POST` one stored chunk (binary) of a begun blob, of the length [`BlobInfo::stored_chunk_len`]
/// gives, answered with no body; idempotent. `GET`: the stored chunk (binary) of a complete blob.
/// `{n}` is the chunk index in decimal.
pub const BLOB_CHUNK_PATH: &str = "/v1/libraries/{library}/blobs/{id}/chunks/{n}";
/// `POST` with no body: [`BlobInfo`], or [`ErrorCode::BlobIncomplete`] unless every chunk is
/// stored. From then on the blob is readable and never changes. Idempotent.
pub const BLOB_COMPLETE_PATH: &str = "/v1/libraries/{library}/blobs/{id}/complete";
/// `POST` the only stored chunk (binary) of a one-chunk blob: [`BlobInfo`]. Begins, stores and
/// completes in one request.
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
/// `POST` [`RemoveDevice`]: [`Devices`].
pub const REMOVE_DEVICE_PATH: &str = "/v1/libraries/{library}/devices/remove";

/// Content type of envelope request bodies and [`BatchPage`] responses.
pub const BINARY_CONTENT_TYPE: &str = "application/octet-stream";

const PROOF_LABEL: &[u8] = b"fongo-sync/v1/create-library\0";
const WRAP_KEY_PROOF_LABEL: &[u8] = b"fongo-sync/v1/create-library/wrap-key\0";
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
    /// Whether the blob routes, [`BLOB_PATH`] to [`BLOBS_RETIRE_PATH`], and [`SETTLED_PATH`],
    /// [`STATUS_PATH`] and [`ENTITLEMENT_PATH`] are served.
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
    /// Largest total of stored blob bytes per library, complete or not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_blobs_bytes: Option<u64>,
    /// Most ids in one [`BlobIds`] sent to [`BLOBS_QUERY_PATH`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_query_ids: Option<u32>,
    /// Days a retired blob is kept after every device not removed has settled past its
    /// retirement.
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
pub struct CreateLibrary {
    pub library: LibraryId,
    #[serde(with = "serde_key")]
    pub device_key: VerifyingKey,
    /// [`LibraryKeys::recovery_signing_key`](crate::LibraryKeys::recovery_signing_key)'s public half.
    #[serde(with = "serde_key")]
    pub recovery_key: VerifyingKey,
    /// The device key's signature over the library id, both keys and the wrap key if there is one.
    ///
    /// Signed bytes: `fongo-sync/v1/create-library` and a zero byte, the library id, the device
    /// key and the recovery key; with a wrap key, then `fongo-sync/v1/create-library/wrap-key`, a
    /// zero byte and the wrap key. Without one they are what a server that knows no wrap keys
    /// checks.
    #[serde(with = "serde_sig")]
    pub proof: Signature,
    /// The first device's [`WrapKey`], stored and returned in its [`DeviceRecord`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrap_key: Option<WrapKey>,
    /// An opaque token for the server's own policy. It is not covered by `proof`: whoever holds
    /// it may present it, as with [`Entitlement`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entitlement: Option<String>,
}

impl CreateLibrary {
    pub fn new(
        library: LibraryId,
        device: &SigningKey,
        recovery_key: VerifyingKey,
    ) -> CreateLibrary {
        CreateLibrary::signed(library, device, recovery_key, None)
    }

    /// A request that also registers the first device's wrap key, covered by `proof`.
    pub fn with_wrap_key(
        library: LibraryId,
        device: &SigningKey,
        recovery_key: VerifyingKey,
        wrap_key: WrapKey,
    ) -> CreateLibrary {
        CreateLibrary::signed(library, device, recovery_key, Some(wrap_key))
    }

    fn signed(
        library: LibraryId,
        device: &SigningKey,
        recovery_key: VerifyingKey,
        wrap_key: Option<WrapKey>,
    ) -> CreateLibrary {
        let device_key = device.verifying_key();
        CreateLibrary {
            library,
            device_key,
            recovery_key,
            proof: device.sign(&proof_message(
                library,
                &device_key,
                &recovery_key,
                wrap_key.as_ref(),
            )),
            wrap_key,
            entitlement: None,
        }
    }

    /// Whether `proof` shows the caller holds `device_key`'s private half and chose `wrap_key`.
    pub fn verify(&self) -> bool {
        self.device_key
            .verify_strict(
                &proof_message(
                    self.library,
                    &self.device_key,
                    &self.recovery_key,
                    self.wrap_key.as_ref(),
                ),
                &self.proof,
            )
            .is_ok()
    }
}

fn proof_message(
    library: LibraryId,
    device: &VerifyingKey,
    recovery: &VerifyingKey,
    wrap_key: Option<&WrapKey>,
) -> Vec<u8> {
    let mut out = PROOF_LABEL.to_vec();
    out.extend_from_slice(&library.0);
    out.extend_from_slice(device.as_bytes());
    out.extend_from_slice(recovery.as_bytes());
    if let Some(wrap_key) = wrap_key {
        out.extend_from_slice(WRAP_KEY_PROOF_LABEL);
        out.extend_from_slice(&wrap_key.0);
    }
    out
}

/// Adds a device. Signed, through [`RequestAuth`](crate::RequestAuth), by a device already in the
/// library or by its recovery key.
///
/// A key already listed and not removed is accepted and changes nothing; a removed one is
/// restored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddDevice {
    #[serde(with = "serde_key")]
    pub device_key: VerifyingKey,
    /// The new device's [`WrapKey`], stored and returned in its [`DeviceRecord`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrap_key: Option<WrapKey>,
}

/// One key the library knows. Its device id is [`DeviceId::of`](crate::DeviceId::of) the key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceRecord {
    #[serde(with = "serde_key")]
    pub public_key: VerifyingKey,
    /// The library's recovery key: it may authenticate requests but never signs envelopes.
    pub recovery: bool,
    /// The wrap key the device was registered with, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrap_key: Option<WrapKey>,
    /// Removed through [`REMOVE_DEVICE_PATH`]: the server refuses its requests, and it no longer
    /// holds back the deletion of retired blobs.
    #[serde(default, skip_serializing_if = "is_false")]
    pub removed: bool,
    /// Unix seconds of the device's last accepted request, possibly coarse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_seen: Option<u64>,
}

/// Every key a library knows, in the order they were added.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Devices {
    pub devices: Vec<DeviceRecord>,
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
        let mut wrapped = request;
        wrapped.wrap_key = Some(WrapKey([5; 32]));
        assert!(!wrapped.verify());
    }

    #[test]
    fn a_wrap_key_is_covered_by_the_proof() {
        let device = SigningKey::from_bytes(&[1; 32]);
        let recovery = SigningKey::from_bytes(&[2; 32]).verifying_key();
        let request =
            CreateLibrary::with_wrap_key(LibraryId([3; 16]), &device, recovery, WrapKey([5; 32]));
        assert!(request.verify());
        let text = serde_json::to_string(&request).unwrap();
        let back: CreateLibrary = serde_json::from_str(&text).unwrap();
        assert!(back.verify());
        assert_ne!(
            request.proof,
            CreateLibrary::new(LibraryId([3; 16]), &device, recovery).proof
        );

        let mut stripped = request.clone();
        stripped.wrap_key = None;
        assert!(!stripped.verify());
        let mut replaced = request;
        replaced.wrap_key = Some(WrapKey([6; 32]));
        assert!(!replaced.verify());
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
            max_blobs_bytes: None,
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
        let add = AddDevice {
            device_key: key,
            wrap_key: None,
        };
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
        let key = SigningKey::from_bytes(&[1; 32]).verifying_key();
        let key_hex = crate::hex::encode(key.as_bytes());
        let record = DeviceRecord {
            public_key: key,
            recovery: false,
            wrap_key: Some(WrapKey([2; 32])),
            removed: true,
            last_seen: Some(1_700_000_000),
        };
        assert_eq!(
            serde_json::to_value(&record).unwrap(),
            json!({
                "public_key": key_hex,
                "recovery": false,
                "wrap_key": "02".repeat(32),
                "removed": true,
                "last_seen": 1_700_000_000u64,
            })
        );
        let old: DeviceRecord =
            serde_json::from_value(json!({"public_key": key_hex, "recovery": true})).unwrap();
        assert_eq!(
            (old.wrap_key, old.removed, old.last_seen),
            (None, false, None)
        );

        let caps: Capabilities = serde_json::from_value(json!({
            "protocol_version": 1,
            "blobs": true,
            "notifications": false,
            "skew_secs": 300,
            "max_blob_bytes": 1,
            "max_blobs_bytes": 2,
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
                caps.max_blobs_bytes,
                caps.max_query_ids,
                caps.blob_wait_days,
                caps.blob_incomplete_days,
                caps.device_removal
            ),
            (Some(1), Some(2), Some(3), Some(7), Some(4), true)
        );
        let add = AddDevice {
            device_key: key,
            wrap_key: Some(WrapKey([0xab; 32])),
        };
        assert_eq!(
            serde_json::to_value(&add).unwrap(),
            json!({"device_key": key_hex, "wrap_key": "ab".repeat(32)})
        );
        assert!(
            serde_json::from_value::<AddDevice>(json!({"device_key": key_hex, "wrap_key": "ab"}))
                .is_err()
        );
        let mut create =
            CreateLibrary::new(LibraryId([3; 16]), &SigningKey::from_bytes(&[1; 32]), key);
        create.entitlement = Some("token".into());
        let value = serde_json::to_value(&create).unwrap();
        assert_eq!(value["entitlement"], json!("token"));
        assert!(value.get("wrap_key").is_none());
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

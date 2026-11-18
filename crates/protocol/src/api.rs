//! Control-plane messages, sent as JSON, and the paths they go to. Every request except
//! `GET` [`CAPABILITIES_PATH`] carries [`RequestAuth`](crate::RequestAuth) headers.

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};

use crate::hex::{serde_key, serde_sig};
use crate::keys::LibraryId;
use crate::wire::{DecodeError, Reader, put_blob, put_varint};

/// `GET`: [`Capabilities`].
pub const CAPABILITIES_PATH: &str = "/v1/capabilities";
/// `POST` [`CreateLibrary`]: [`Devices`]. Authenticated by the new device's own key.
pub const LIBRARIES_PATH: &str = "/v1/libraries";
/// `GET`: [`Devices`]. `POST` [`AddDevice`]: [`Devices`]. `{library}` is the library id in hex.
pub const DEVICES_PATH: &str = "/v1/libraries/{library}/devices";
/// `POST` envelope bytes: [`Appended`]. `GET` with [`ReadBatches`] as the query: a [`BatchPage`].
pub const BATCHES_PATH: &str = "/v1/libraries/{library}/batches";

/// Content type of envelope request bodies and [`BatchPage`] responses.
pub const BINARY_CONTENT_TYPE: &str = "application/octet-stream";

const PROOF_LABEL: &[u8] = b"fongo-sync/v1/create-library\0";
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

/// What a server offers. Absent quotas are unlimited or undisclosed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub protocol_version: u32,
    pub blobs: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_library_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_devices: Option<u32>,
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
    /// The device key's signature over the library id and both keys.
    #[serde(with = "serde_sig")]
    pub proof: Signature,
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
}

fn proof_message(library: LibraryId, device: &VerifyingKey, recovery: &VerifyingKey) -> Vec<u8> {
    let mut out = PROOF_LABEL.to_vec();
    out.extend_from_slice(&library.0);
    out.extend_from_slice(device.as_bytes());
    out.extend_from_slice(recovery.as_bytes());
    out
}

/// Adds a device. Signed, through [`RequestAuth`](crate::RequestAuth), by a device already in the
/// library or by its recovery key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddDevice {
    #[serde(with = "serde_key")]
    pub device_key: VerifyingKey,
}

/// One key the library knows. Its device id is [`DeviceId::of`](crate::DeviceId::of) the key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeviceRecord {
    #[serde(with = "serde_key")]
    pub public_key: VerifyingKey,
    /// The library's recovery key: it may authenticate requests but never signs envelopes.
    pub recovery: bool,
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

/// Query of a batch read: envelopes with sequence numbers greater than `after`, oldest first.
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

    /// Fails on anything `encode` would not write, allocating no more than the input's length.
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
        let mut swapped = request;
        swapped.recovery_key = device.verifying_key();
        assert!(!swapped.verify());
    }

    #[test]
    fn json_shapes_are_plain() {
        let caps = Capabilities {
            protocol_version: 1,
            blobs: false,
            max_batch_bytes: Some(10),
            max_library_bytes: None,
            max_devices: None,
        };
        assert_eq!(
            serde_json::to_value(&caps).unwrap(),
            json!({"protocol_version": 1, "blobs": false, "max_batch_bytes": 10})
        );
        let key = SigningKey::from_bytes(&[1; 32]).verifying_key();
        let add = AddDevice { device_key: key };
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

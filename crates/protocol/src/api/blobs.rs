use serde::{Deserialize, Serialize};

use sha2::{Digest, Sha256};

use std::collections::BTreeMap;

use thiserror::Error;

use crate::envelope::Envelope;
use crate::file::{BlobId, CHUNK_OVERHEAD, FileLayout, SEALED_CHUNK_LEN, SealedChunk, padme};
use crate::hex::serde_digest;
use crate::keys::DeviceId;
use crate::wire::{DecodeError, Reader, put_varint};

const OVERHEAD: u64 = CHUNK_OVERHEAD as u64;

/// A blob's stored shape: `size`, the total of its stored chunks' bytes, and `chunks`, how many
/// there are. The body that begins an upload at [`BLOB_PATH`](super::BLOB_PATH) and the answer
/// for a complete blob.
///
/// A server cannot see a file's true size, but it can check that some file is laid out this way
/// ([`BlobInfo::is_valid`]) and how long each chunk must be ([`BlobInfo::stored_chunk_len`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobInfo {
    pub size: u64,
    pub chunks: u64,
}

impl BlobInfo {
    /// Whether some file is stored as `chunks` chunks totalling `size` bytes: every chunk but the
    /// last is [`SEALED_CHUNK_LEN`] bytes, the last is a nonce, a tag and at least one byte between
    /// them (only an empty file has a chunk of just a nonce and a tag), and the plaintext total is
    /// its own [`padme`].
    pub fn is_valid(&self) -> bool {
        self.last_chunk_len().is_some()
    }

    /// The stored length chunk `index` must have, or `None` when there is no such chunk or the
    /// shape is not valid.
    pub fn stored_chunk_len(&self, index: u64) -> Option<u64> {
        let last = self.last_chunk_len()?;
        match index.cmp(&(self.chunks - 1)) {
            std::cmp::Ordering::Less => Some(SEALED_CHUNK_LEN as u64),
            std::cmp::Ordering::Equal => Some(last),
            std::cmp::Ordering::Greater => None,
        }
    }

    fn last_chunk_len(&self) -> Option<u64> {
        let full = self
            .chunks
            .checked_sub(1)?
            .checked_mul(SEALED_CHUNK_LEN as u64)?;
        let last = self.size.checked_sub(full)?;
        let least = if self.chunks == 1 {
            OVERHEAD
        } else {
            OVERHEAD + 1
        };
        if !(least..=SEALED_CHUNK_LEN as u64).contains(&last) {
            return None;
        }
        let plain = self.size - self.chunks * OVERHEAD;
        (padme(plain) == Some(plain)).then_some(last)
    }
}

impl From<FileLayout> for BlobInfo {
    fn from(layout: FileLayout) -> BlobInfo {
        BlobInfo {
            size: layout.stored_size(),
            chunks: layout.chunk_count(),
        }
    }
}

/// The answer to beginning an upload: whether the blob is already complete, and the chunks the
/// server holds, by increasing index.
///
/// Until a blob is complete, a server replaces a chunk when the same index is uploaded again; once
/// it is complete, nothing about it changes. A device resuming an upload skips a chunk only when
/// the server's hash of it equals [`SealedChunk::sha256`] of the chunk this device sealed for that
/// index ([`HaveChunk::holds`]), and uploads every other chunk again. It completes with the
/// [`BlobDigest`] of the chunks it sealed ([`CompleteBlob`]), which the server checks against the
/// chunks it holds. Sealing is deterministic and each chunk has one stored form, so a device that
/// completes a blob has checked every chunk against its own copy of the file.
///
/// What this does not cover: a device that completes a blob with the wrong content, by intent or
/// through a bug, leaves that content hash unreadable, since this version has no way to replace a
/// complete blob.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobState {
    pub complete: bool,
    pub have: Vec<HaveChunk>,
}

/// A chunk a server holds: its index and SHA-256 of its stored bytes, computed by the server, as
/// 64 lowercase hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HaveChunk {
    pub n: u64,
    #[serde(with = "serde_digest")]
    pub sha256: [u8; 32],
}

impl HaveChunk {
    /// What a server reports for `stored`, the bytes it holds as chunk `n`.
    pub fn of(n: u64, stored: &[u8]) -> HaveChunk {
        HaveChunk {
            n,
            sha256: Sha256::digest(stored).into(),
        }
    }

    /// Whether the server holds exactly `chunk`, so that uploading it again can be skipped.
    pub fn holds(&self, chunk: &SealedChunk) -> bool {
        self.n == chunk.index && self.sha256 == chunk.sha256()
    }
}

/// The body that completes an upload at [`BLOB_COMPLETE_PATH`](super::BLOB_COMPLETE_PATH): the
/// [`BlobDigest`] of the chunks the uploader sealed, as 64 lowercase hex digits.
///
/// The server computes the digest of the chunks it holds at that moment, atomically with marking
/// the blob complete, and answers [`ErrorCode::BlobMismatch`](super::ErrorCode::BlobMismatch),
/// leaving the blob incomplete, if the two differ. A chunk another device replaced after this
/// device's begin therefore cannot be completed in. An uploader that gets that answer begins
/// again and uploads each chunk whose hash differs from its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteBlob {
    #[serde(with = "serde_digest")]
    pub digest: [u8; 32],
}

/// The digest of a blob's stored chunks: SHA-256 over the concatenation, in index order, of each
/// stored chunk's 32-byte SHA-256. Chunks or their hashes may be added in any order; adding an
/// index again replaces it.
///
/// It keeps 32 bytes per index added, so a server bounds the chunk count (by
/// [`Capabilities::max_blob_bytes`](super::Capabilities::max_blob_bytes)) before using it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlobDigest {
    chunks: u64,
    hashes: BTreeMap<u64, [u8; 32]>,
}

/// Why a [`BlobDigest`] refused a chunk or could not finish.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum DigestError {
    #[error("chunk {index} is out of range: the blob has {count} chunks")]
    IndexOutOfRange { index: u64, count: u64 },
    #[error("chunk {index} is missing")]
    Missing { index: u64 },
}

impl BlobDigest {
    /// The digest of a blob of `chunks` chunks, with none added yet.
    pub fn new(chunks: u64) -> BlobDigest {
        BlobDigest {
            chunks,
            hashes: BTreeMap::new(),
        }
    }

    /// Adds chunk `index` by the SHA-256 of its stored bytes.
    pub fn add(&mut self, index: u64, sha256: [u8; 32]) -> Result<(), DigestError> {
        if index >= self.chunks {
            return Err(DigestError::IndexOutOfRange {
                index,
                count: self.chunks,
            });
        }
        self.hashes.insert(index, sha256);
        Ok(())
    }

    /// Adds a chunk this device sealed.
    pub fn add_chunk(&mut self, chunk: &SealedChunk) -> Result<(), DigestError> {
        self.add(chunk.index, chunk.sha256())
    }

    /// The digest, once every index from 0 to the chunk count has been added.
    pub fn finish(&self) -> Result<[u8; 32], DigestError> {
        let mut digest = Sha256::new();
        for index in 0..self.chunks {
            let hash = self
                .hashes
                .get(&index)
                .ok_or(DigestError::Missing { index })?;
            digest.update(hash);
        }
        Ok(digest.finalize().into())
    }
}

/// The body of [`BATCHES_CLAIMING_PATH`](super::BATCHES_CLAIMING_PATH): an envelope and the ids of
/// the blobs its changes set references to. The server appends the envelope as
/// [`BATCHES_PATH`](super::BATCHES_PATH) would, with the same checks and errors, and in the same
/// atomic step records each id as claimed at the envelope's sequence number; it answers
/// [`Appended`](super::Appended). An envelope the server already holds is not appended again, and
/// its ids are claimed at the library's head instead. A request that names an id needing a record
/// the library has no room for is refused with
/// [`ErrorCode::LibraryFull`](super::ErrorCode::LibraryFull) and appends nothing. The ids count
/// against [`Capabilities::max_query_ids`](super::Capabilities::max_query_ids), and not against
/// [`Capabilities::max_batch_bytes`](super::Capabilities::max_batch_bytes).
///
/// The ids tell the server which envelope, and so which device and when, refers to which blob
/// ids; not what the files are.
///
/// Bytes: a LEB128 count, that many 32-byte blob ids, then the envelope exactly as
/// [`BATCHES_PATH`](super::BATCHES_PATH) takes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimingAppend {
    pub ids: Vec<BlobId>,
    pub envelope: Vec<u8>,
}

impl ClaimingAppend {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(10 + 32 * self.ids.len() + self.envelope.len());
        put_varint(&mut out, self.ids.len() as u64);
        for id in &self.ids {
            out.extend_from_slice(&id.0);
        }
        out.extend_from_slice(&self.envelope);
        out
    }

    /// Fails on anything `encode` would not write from an envelope that [`Envelope::decode`]
    /// reads; the envelope's signature is not checked here. Allocation is bounded by a constant
    /// multiple of the input's length.
    pub fn decode(bytes: &[u8]) -> Result<ClaimingAppend, DecodeError> {
        let mut r = Reader::new(bytes);
        let count = r.len(32)?;
        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            ids.push(BlobId(r.array()?));
        }
        let envelope = r.rest();
        Envelope::decode(envelope)?;
        Ok(ClaimingAppend {
            ids,
            envelope: envelope.to_vec(),
        })
    }
}

/// The body of a query or a claim, of at most
/// [`Capabilities::max_query_ids`](super::Capabilities::max_query_ids) distinct ids. A claim sent
/// to [`BLOBS_CLAIM_PATH`](super::BLOBS_CLAIM_PATH) is recorded at the library's current head.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobIds {
    pub ids: Vec<BlobId>,
}

/// The answer to a query: those of the ids asked about that are complete on the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteBlobs {
    pub complete: Vec<BlobId>,
}

/// The answer to a claim: those of the ids claimed that are not complete on the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingBlobs {
    pub missing: Vec<BlobId>,
}

/// Marks complete blobs as referred to by no change through log position `through`, by the rules
/// in [the module documentation](super#deleting-files). Ids the server does not hold complete are
/// ignored. A server keeps the earliest retirement of an id that is not void, so sending one again
/// never restarts the wait.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetireBlobs {
    pub through: u64,
    pub ids: Vec<BlobId>,
}

/// The sender has dealt with every envelope through sequence number `seq`, by merging it or by
/// skipping it and recording that it did, and has nothing unsent. A device that skipped an
/// envelope may settle, but never retires through it. A server keeps the greatest it was told,
/// and keeps it when a removed device is restored; a `seq` above the library's head is
/// [`ErrorCode::MalformedRequest`](super::ErrorCode::MalformedRequest).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Settled {
    pub seq: u64,
}

/// Whether the library is entitled to the service, the stored bytes counted against it, and its
/// allowance if it has one. `used_bytes` and `allowance_bytes` count stored blob bytes, complete
/// or not; envelope bytes are not counted here, and are bounded by
/// [`Capabilities::max_library_bytes`](super::Capabilities::max_library_bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LibraryStatus {
    pub entitled: bool,
    pub used_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowance_bytes: Option<u64>,
}

/// An opaque token the server's policy reads; a server without one ignores it. A stale token, one
/// the policy reads but no longer acts on, leaves the status as it was and is answered with it; one
/// the server cannot verify is refused with
/// [`ErrorCode::NotEntitled`](super::ErrorCode::NotEntitled).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entitlement {
    pub entitlement: String,
}

/// Removes a device, which may be the sender. From then on the server refuses the removed
/// device's requests with [`ErrorCode::DeviceRemoved`](super::ErrorCode::DeviceRemoved), keeps
/// the envelopes it appended, and no longer waits for it before deleting retired blobs. Naming the
/// recovery key is refused with [`ErrorCode::Forbidden`](super::ErrorCode::Forbidden); naming an
/// id the library does not list, or a device already removed, changes nothing. Either way the
/// answer is the device list; [`Devices::remove`](super::Devices::remove) does this to a list.
///
/// Removal does not lock anyone out: whoever holds the library secret can derive the recovery
/// key and add a device again, and can read whatever they obtain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoveDevice {
    pub device: DeviceId,
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;
    use crate::file::{ContentHash, FileRef};

    const SEALED: u64 = SEALED_CHUNK_LEN as u64;

    #[test]
    fn layouts_are_valid_shapes() {
        for size in [0, 1, 7, 9, 4_194_304, 4_194_305, 10_000_000] {
            let layout = FileRef {
                hash: ContentHash([0; 32]),
                size,
            }
            .layout()
            .unwrap();
            let info = BlobInfo::from(layout);
            assert!(info.is_valid(), "{size}");
            for i in 0..=info.chunks {
                assert_eq!(
                    info.stored_chunk_len(i),
                    layout.stored_chunk_len(i),
                    "{size}"
                );
            }
        }
    }

    #[test]
    fn other_shapes_are_not() {
        for (size, chunks) in [
            (0, 0),
            (39, 1),
            (40 + 9, 1),
            (SEALED + 1, 1),
            (SEALED + 40, 2),
            (SEALED - 1 + 40 + 4, 2),
            (u64::MAX, u64::MAX),
            (u64::MAX, 2),
        ] {
            let info = BlobInfo { size, chunks };
            assert!(!info.is_valid(), "{size} {chunks}");
            assert_eq!(info.stored_chunk_len(0), None);
        }
        for size in [40, 40 + 10] {
            let info = BlobInfo { size, chunks: 1 };
            assert!(info.is_valid());
            assert_eq!(info.stored_chunk_len(0), Some(size));
            assert_eq!(info.stored_chunk_len(u64::MAX), None);
        }
    }

    proptest! {
        #[test]
        fn a_valid_shape_is_the_layout_of_its_plaintext(size in any::<u64>(), chunks in 0u64..5000) {
            let info = BlobInfo { size, chunks };
            let _ = info.stored_chunk_len(size);
            if info.is_valid() {
                let layout = FileRef { hash: ContentHash([0; 32]), size: size - 40 * chunks }
                    .layout()
                    .unwrap();
                prop_assert_eq!(BlobInfo::from(layout), info);
            }
        }

        #[test]
        fn every_file_size_has_a_valid_shape(size in 0u64..1 << 40) {
            let layout = FileRef { hash: ContentHash([0; 32]), size }.layout().unwrap();
            prop_assert!(BlobInfo::from(layout).is_valid());
        }
    }

    #[test]
    fn json_shapes_are_plain() {
        let id = BlobId([0xab; 32]);
        let hex = "ab".repeat(32);
        let device = DeviceId([0xcd; 16]);
        let cases = [
            (
                serde_json::to_value(BlobInfo {
                    size: 26,
                    chunks: 1,
                })
                .unwrap(),
                json!({"size": 26, "chunks": 1}),
            ),
            (
                serde_json::to_value(BlobState {
                    complete: false,
                    have: vec![HaveChunk {
                        n: 2,
                        sha256: [0xab; 32],
                    }],
                })
                .unwrap(),
                json!({"complete": false, "have": [{"n": 2, "sha256": hex}]}),
            ),
            (
                serde_json::to_value(BlobIds { ids: vec![id] }).unwrap(),
                json!({ "ids": [hex] }),
            ),
            (
                serde_json::to_value(CompleteBlobs { complete: vec![id] }).unwrap(),
                json!({ "complete": [hex] }),
            ),
            (
                serde_json::to_value(MissingBlobs { missing: vec![] }).unwrap(),
                json!({"missing": []}),
            ),
            (
                serde_json::to_value(RetireBlobs {
                    through: 9,
                    ids: vec![id],
                })
                .unwrap(),
                json!({"through": 9, "ids": [hex]}),
            ),
            (
                serde_json::to_value(Settled { seq: 4 }).unwrap(),
                json!({"seq": 4}),
            ),
            (
                serde_json::to_value(LibraryStatus {
                    entitled: true,
                    used_bytes: 5,
                    allowance_bytes: None,
                })
                .unwrap(),
                json!({"entitled": true, "used_bytes": 5}),
            ),
            (
                serde_json::to_value(Entitlement {
                    entitlement: "t".into(),
                })
                .unwrap(),
                json!({"entitlement": "t"}),
            ),
            (
                serde_json::to_value(RemoveDevice { device }).unwrap(),
                json!({"device": "cd".repeat(16)}),
            ),
        ];
        for (actual, expected) in cases {
            assert_eq!(actual, expected);
        }
        assert!(serde_json::from_value::<BlobIds>(json!({ "ids": [hex.to_uppercase()] })).is_err());
        assert!(
            serde_json::from_value::<HaveChunk>(json!({"n": 0, "sha256": hex.to_uppercase()}))
                .is_err()
        );
        assert!(serde_json::from_value::<HaveChunk>(json!({"n": 0, "sha256": "ab"})).is_err());
    }

    #[test]
    fn a_digest_is_the_hash_of_the_chunk_hashes_in_order() {
        let hashes = [[1u8; 32], [2; 32], [3; 32]];
        let mut digest = BlobDigest::new(3);
        assert_eq!(digest.finish(), Err(DigestError::Missing { index: 0 }));
        for i in [2, 0] {
            digest.add(i as u64, hashes[i]).unwrap();
        }
        assert_eq!(digest.finish(), Err(DigestError::Missing { index: 1 }));
        digest.add(1, [9; 32]).unwrap();
        let replaced = digest.finish().unwrap();
        digest.add(1, hashes[1]).unwrap();
        let whole = digest.finish().unwrap();
        assert_ne!(whole, replaced);
        assert_eq!(whole, <[u8; 32]>::from(Sha256::digest(hashes.concat())));
        assert_eq!(
            digest.add(3, [0; 32]),
            Err(DigestError::IndexOutOfRange { index: 3, count: 3 })
        );
        assert_eq!(
            BlobDigest::new(0).finish(),
            Ok(<[u8; 32]>::from(Sha256::digest(b"")))
        );

        let body = CompleteBlob { digest: whole };
        let value = serde_json::to_value(body).unwrap();
        assert_eq!(value, json!({ "digest": crate::hex::encode(&whole) }));
        assert_eq!(serde_json::from_value::<CompleteBlob>(value).unwrap(), body);
        assert!(
            serde_json::from_value::<CompleteBlob>(
                json!({ "digest": crate::hex::encode(&whole).to_uppercase() })
            )
            .is_err()
        );
    }

    fn envelope() -> Vec<u8> {
        crate::seal(
            &[],
            &crate::LibraryKeys::new(crate::LibraryId([1; 16]), [2; 32]),
            0,
            &crate::SigningKey::from_bytes(&[3; 32]),
            [4; 24],
        )
    }

    proptest! {
        #[test]
        fn claiming_appends_round_trip(ids in proptest::collection::vec(any::<[u8; 32]>(), 0..5)) {
            let body = ClaimingAppend {
                ids: ids.into_iter().map(BlobId).collect(),
                envelope: envelope(),
            };
            let bytes = body.encode();
            prop_assert_eq!(ClaimingAppend::decode(&bytes), Ok(body));
            for cut in 0..bytes.len() {
                prop_assert!(ClaimingAppend::decode(&bytes[..cut]).is_err());
            }
            let mut longer = bytes;
            longer.push(0);
            prop_assert_eq!(ClaimingAppend::decode(&longer), Err(DecodeError::TrailingBytes));
        }

        #[test]
        fn decoding_arbitrary_claiming_appends_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..400)) {
            if let Ok(body) = ClaimingAppend::decode(&bytes) {
                prop_assert_eq!(body.encode(), bytes);
            }
        }
    }

    #[test]
    fn a_claiming_append_refuses_padding_huge_counts_and_a_bad_envelope() {
        let mut padded = vec![0x81, 0x00];
        padded.extend_from_slice(&[0; 32]);
        padded.extend_from_slice(&envelope());
        assert_eq!(ClaimingAppend::decode(&padded), Err(DecodeError::BadLength));
        let mut huge = Vec::new();
        put_varint(&mut huge, u64::MAX >> 1);
        huge.extend_from_slice(&envelope());
        assert_eq!(ClaimingAppend::decode(&huge), Err(DecodeError::Truncated));
        let mut other = vec![0];
        other.extend_from_slice(&envelope());
        other[1] = 2;
        assert_eq!(
            ClaimingAppend::decode(&other),
            Err(DecodeError::UnsupportedVersion(2))
        );
    }

    #[test]
    fn a_server_reports_what_a_sealer_compares() {
        let bytes = vec![7u8; 100];
        let keys = crate::LibraryKeys::new(crate::LibraryId([1; 16]), [2; 32]);
        let mut sealer = crate::FileSealer::new(&keys, FileRef::of(&bytes)).unwrap();
        sealer.feed(&bytes).unwrap();
        let chunk = sealer.next_chunk().unwrap().unwrap();
        let have = HaveChunk::of(0, &chunk.bytes);
        assert_eq!(have.sha256, <[u8; 32]>::from(Sha256::digest(&chunk.bytes)));
        assert!(have.holds(&chunk));
        let mut other = chunk.bytes.clone();
        other[0] ^= 1;
        assert!(!HaveChunk::of(0, &other).holds(&chunk));
        assert!(!HaveChunk::of(1, &chunk.bytes).holds(&chunk));
    }
}

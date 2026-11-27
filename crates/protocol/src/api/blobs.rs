use serde::{Deserialize, Serialize};

use sha2::{Digest, Sha256};

use crate::file::{BlobId, CHUNK_OVERHEAD, FileLayout, SEALED_CHUNK_LEN, SealedChunk, padme};
use crate::hex::serde_digest;
use crate::keys::DeviceId;

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
/// index ([`HaveChunk::holds`]), and uploads every other chunk again. Sealing is deterministic and
/// each chunk has one stored form, so a device that completes a blob this way has checked every
/// chunk against its own copy of the file.
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

/// The body of a query or a claim. A query takes at most
/// [`Capabilities::max_query_ids`](super::Capabilities::max_query_ids) ids.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobIds {
    pub ids: Vec<BlobId>,
}

/// The answer to a query: those of the ids asked about that are complete on the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompleteBlobs {
    pub complete: Vec<BlobId>,
}

/// The answer to a claim: those of the ids claimed that are not complete on the server. The
/// claim has cleared any retirement of every id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingBlobs {
    pub missing: Vec<BlobId>,
}

/// Marks complete blobs as referred to by nothing in the sender's merged state as of log position
/// `through`. Ids the server does not hold are ignored.
///
/// A server deletes a retired blob only when no claim has named it since, every device not
/// removed has reported [`Settled`] at or past `through` (one that never reported counts as 0), and
/// [`Capabilities::blob_wait_days`](super::Capabilities::blob_wait_days) have passed since the
/// retirement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetireBlobs {
    pub through: u64,
    pub ids: Vec<BlobId>,
}

/// The sender has merged every envelope through sequence number `seq` and has nothing unsent. A
/// server keeps the greatest it was told.
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

/// An opaque token the server's policy reads; a server without one ignores it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entitlement {
    pub entitlement: String,
}

/// Removes a device, which may be the sender. From then on the server refuses the removed
/// device's requests with [`ErrorCode::DeviceRemoved`](super::ErrorCode::DeviceRemoved), keeps
/// the envelopes it appended, and no longer waits for it before deleting retired blobs.
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

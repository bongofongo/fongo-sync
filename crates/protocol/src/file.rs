use std::fmt;

use chacha20poly1305::aead::{AeadInPlace, KeyInit};
use chacha20poly1305::{Tag, XChaCha20Poly1305, XNonce};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use thiserror::Error;
use zeroize::Zeroize;

use crate::keys::{FileKey, FileKeyRoot, FileNameKey, LibraryKeys};
use crate::wire::{DecodeError, Reader, put_varint};

/// The file reference format this crate writes, and the only one it reads.
pub const FILE_REF_FORMAT: u8 = 1;

/// Plaintext bytes in every chunk of a file's stream but the last.
pub const FILE_CHUNK_LEN: usize = 4_194_304;

/// Stored bytes of every chunk but the last: a 24-byte nonce, [`FILE_CHUNK_LEN`] bytes of
/// ciphertext and a 16-byte tag.
pub const SEALED_CHUNK_LEN: usize = FILE_CHUNK_LEN + CHUNK_OVERHEAD;

/// Stored bytes a chunk adds to its plaintext: its nonce and its tag.
pub(crate) const CHUNK_OVERHEAD: usize = NONCE_LEN + TAG_LEN;

const NONCE_LEN: usize = 24;
const TAG_LEN: usize = 16;
const FILE_KEY_INFO: &[u8] = b"fongo-sync/v1/file-key";
const CHUNK_KEY_INFO: &[u8] = b"fongo-sync/v1/file-chunk-key";
const NONCE_KEY_INFO: &[u8] = b"fongo-sync/v1/file-nonce-key";
const AD_LABEL: &[u8] = b"fongo-sync/v1/file\0";
const AD_LEN: usize = 19 + 32 + 8 + 8 + 8 + 1;

/// SHA-256 of a file's bytes. It travels only inside encrypted batches; the server never sees it.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentHash(pub [u8; 32]);

impl ContentHash {
    pub fn of(bytes: &[u8]) -> ContentHash {
        ContentHash(Sha256::digest(bytes).into())
    }
}

impl fmt::Debug for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentHash({}..)", crate::hex::encode(&self.0[..4]))
    }
}

/// The name a server stores a file under: HMAC-SHA256 keyed with the library's
/// [`FileNameKey`] over the [`ContentHash`]. Without the library secret it says nothing about
/// the content, and one content has one name in a library.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobId(pub [u8; 32]);

impl BlobId {
    /// Lowercase hex, 64 characters.
    pub fn to_hex(&self) -> String {
        crate::hex::encode(&self.0)
    }

    /// `None` unless `text` is exactly 64 lowercase hex digits.
    pub fn from_hex(text: &str) -> Option<BlobId> {
        crate::hex::decode_lower(text).map(BlobId)
    }
}

impl fmt::Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobId({})", self.to_hex())
    }
}

impl serde::Serialize for BlobId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_hex())
    }
}

impl<'de> serde::Deserialize<'de> for BlobId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<BlobId, D::Error> {
        let text = <std::borrow::Cow<'de, str>>::deserialize(d)?;
        BlobId::from_hex(&text)
            .ok_or_else(|| serde::de::Error::custom("expected 64 lowercase hex digits"))
    }
}

impl FileNameKey {
    /// HMAC-SHA256 under this key of the content hash's 32 bytes.
    pub fn blob_id(&self, hash: &ContentHash) -> BlobId {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(self.as_bytes())
            .expect("HMAC takes a key of any length");
        mac.update(&hash.0);
        BlobId(mac.finalize().into_bytes().into())
    }
}

impl FileKeyRoot {
    /// The key of the file whose content hash is `hash`, which [`FileSealer`] expands into its
    /// chunk key and nonce key: HKDF-Expand (RFC 5869, section 2.3) with
    /// this root as the pseudorandom key, info `fongo-sync/v1/file-key` followed by the hash's
    /// 32 bytes, and 32 bytes of output. There is no Extract step: the root, itself HKDF output,
    /// is the pseudorandom key. With one block of output this is HMAC-SHA256 keyed with the root
    /// over the info followed by the byte 1.
    pub(crate) fn file_key(&self, hash: &ContentHash) -> FileKey {
        let mut out = [0u8; 32];
        Hkdf::<Sha256>::from_prk(self.as_bytes())
            .expect("32 bytes is a valid HKDF-SHA256 pseudorandom key")
            .expand_multi_info(&[FILE_KEY_INFO, &hash.0], &mut out)
            .expect("32 bytes is a valid HKDF-SHA256 output length");
        FileKey::from_bytes(out)
    }
}

/// What a change carries to name a file: its content hash and its true size. It travels as an
/// ordinary blob value.
///
/// Bytes: format ([`FILE_REF_FORMAT`]), the 32-byte hash, then the size as a LEB128 integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FileRef {
    pub hash: ContentHash,
    pub size: u64,
}

impl FileRef {
    /// The reference of bytes already in memory; [`FileHasher`] reads a file in pieces.
    pub fn of(bytes: &[u8]) -> FileRef {
        FileRef {
            hash: ContentHash::of(bytes),
            size: bytes.len() as u64,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 32 + 10);
        out.push(FILE_REF_FORMAT);
        out.extend_from_slice(&self.hash.0);
        put_varint(&mut out, self.size);
        out
    }

    /// Fails on anything `encode` would not write: another format, a size not minimally encoded,
    /// or bytes after the size.
    pub fn decode(bytes: &[u8]) -> Result<FileRef, DecodeError> {
        let mut r = Reader::new(bytes);
        let version = r.u8()?;
        if version != FILE_REF_FORMAT {
            return Err(DecodeError::UnsupportedVersion(version));
        }
        let hash = ContentHash(r.array()?);
        let size = r.varint()?;
        r.finish()?;
        Ok(FileRef { hash, size })
    }

    /// How this file is laid out as a stream of chunks.
    pub fn layout(&self) -> Result<FileLayout, FileError> {
        FileLayout::new(self.size, FILE_CHUNK_LEN as u64)
    }
}

/// Hashes a file read in pieces of any size.
#[derive(Clone, Default)]
pub struct FileHasher {
    hash: Sha256,
    size: u64,
}

impl FileHasher {
    pub fn new() -> FileHasher {
        FileHasher::default()
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.hash.update(bytes);
        self.size = self.size.saturating_add(bytes.len() as u64);
    }

    /// The reference of everything given to [`FileHasher::update`].
    pub fn finish(self) -> FileRef {
        FileRef {
            hash: ContentHash(self.hash.finalize().into()),
            size: self.size,
        }
    }

    /// Whether everything given to [`FileHasher::update`] is the file `expected` names.
    pub fn verify(self, expected: &FileRef) -> Result<(), FileError> {
        let found = self.finish();
        if found.size != expected.size {
            Err(FileError::SizeMismatch)
        } else if found.hash != expected.hash {
            Err(FileError::HashMismatch)
        } else {
            Ok(())
        }
    }
}

/// The padded length of `len` bytes, by Padmé: `len` itself when it is below 2; otherwise, with
/// `e` = ⌊log2 `len`⌋ and `s` = ⌊log2 `e`⌋ + 1, `len` rounded up to a multiple of 2^(`e` − `s`).
/// `None` when that does not fit in a `u64`, which is so for every `len` above 2^64 − 2^57.
///
/// It is never below `len`, adds less than an eighth of `len`, never decreases as `len` grows,
/// and is its own padded length. A padded length leaves O(log log `len`) bits of the length
/// visible.
pub fn padme(len: u64) -> Option<u64> {
    if len < 2 {
        return Some(len);
    }
    let e = 63 - len.leading_zeros();
    let s = 32 - e.leading_zeros();
    let mask = (1u64 << (e - s)) - 1;
    len.checked_add(mask).map(|n| n & !mask)
}

/// How a file of a given size is stored: its bytes and then zeros up to [`padme`] of its size,
/// cut into chunks of [`FILE_CHUNK_LEN`] bytes (the last may be shorter; an empty file is one
/// empty chunk), each stored with a 24-byte nonce before it and a 16-byte tag after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileLayout {
    size: u64,
    padded: u64,
    chunks: u64,
    chunk: u64,
}

impl FileLayout {
    fn new(size: u64, chunk: u64) -> Result<FileLayout, FileError> {
        let padded = padme(size).ok_or(FileError::TooLarge)?;
        let chunks = padded.div_ceil(chunk).max(1);
        let layout = FileLayout {
            size,
            padded,
            chunks,
            chunk,
        };
        layout.checked_stored_size().ok_or(FileError::TooLarge)?;
        Ok(layout)
    }

    /// The file's true size.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// [`padme`] of the size: the plaintext length of the stream.
    pub fn padded_size(&self) -> u64 {
        self.padded
    }

    /// At least 1.
    pub fn chunk_count(&self) -> u64 {
        self.chunks
    }

    /// The total of every stored chunk's length: the padded size and 40 bytes per chunk.
    pub fn stored_size(&self) -> u64 {
        self.checked_stored_size()
            .expect("checked when the layout was made")
    }

    /// The stored length of chunk `index`, or `None` when the file has no such chunk.
    pub fn stored_chunk_len(&self, index: u64) -> Option<u64> {
        (index < self.chunks).then(|| (self.plain_len(index) + CHUNK_OVERHEAD) as u64)
    }

    fn checked_stored_size(&self) -> Option<u64> {
        self.chunks
            .checked_mul(CHUNK_OVERHEAD as u64)?
            .checked_add(self.padded)
    }

    /// Plaintext bytes of chunk `index` < `chunks`, padding included.
    fn plain_len(&self, index: u64) -> usize {
        let len = if index + 1 < self.chunks {
            self.chunk
        } else {
            self.padded - (self.chunks - 1) * self.chunk
        };
        len as usize
    }

    /// Bytes of the file in chunk `index` < `chunks`, padding excluded.
    fn data_len(&self, index: u64) -> usize {
        let before = index * self.chunk;
        self.size
            .saturating_sub(before)
            .min(self.plain_len(index) as u64) as usize
    }
}

/// Why a file's stream could not be sealed or opened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum FileError {
    #[error("the file is too large for the stream format")]
    TooLarge,
    #[error("chunk {index} is out of range: the file has {count} chunks")]
    IndexOutOfRange { index: u64, count: u64 },
    #[error("chunk {index} is {actual} bytes, not {expected}")]
    WrongLength {
        index: u64,
        expected: u64,
        actual: u64,
    },
    #[error("chunk {index} does not open as that chunk of this file")]
    DoesNotOpen { index: u64 },
    #[error("chunk {index} has padding that is not zeros")]
    BadPadding { index: u64 },
    #[error("the bytes are not the file the reference names")]
    HashMismatch,
    #[error("the bytes are not as long as the reference says")]
    SizeMismatch,
    #[error("chunks are left that were not taken from the sealer")]
    Incomplete,
}

/// The keys, name and layout of one file, shared by sealing and opening.
///
/// Chunk `n` is sealed with XChaCha20-Poly1305 under the chunk key, with associated data
/// `fongo-sync/v1/file` and a zero byte, the blob id, the chunk count and the true size (u64
/// big-endian each), `n` (u64 big-endian) and 1 for the last chunk or 0 for any other. Its nonce
/// is synthetic: the first 24 bytes of HMAC-SHA256 under the nonce key of the associated data
/// followed by the chunk's padded plaintext. Both keys are HKDF-Expand of the file key as the
/// pseudorandom key, with infos `fongo-sync/v1/file-chunk-key` and `fongo-sync/v1/file-nonce-key`.
/// The stored chunk is the nonce, the ciphertext and the tag.
struct Stream {
    cipher: XChaCha20Poly1305,
    nonce_key: Hmac<Sha256>,
    id: BlobId,
    layout: FileLayout,
    size: u64,
}

impl Stream {
    fn new(keys: &LibraryKeys, file: &FileRef, chunk: u64) -> Result<Stream, FileError> {
        let layout = FileLayout::new(file.size, chunk)?;
        let id = keys.file_name_key().blob_id(&file.hash);
        let key = keys.file_key_root().file_key(&file.hash);
        let expand = Hkdf::<Sha256>::from_prk(key.as_bytes())
            .expect("32 bytes is a valid HKDF-SHA256 pseudorandom key");
        let mut chunk_key = [0u8; 32];
        let mut nonce_key = [0u8; 32];
        expand
            .expand(CHUNK_KEY_INFO, &mut chunk_key)
            .and_then(|()| expand.expand(NONCE_KEY_INFO, &mut nonce_key))
            .expect("32 bytes is a valid HKDF-SHA256 output length");
        let cipher = XChaCha20Poly1305::new(&chunk_key.into());
        let mac = <Hmac<Sha256> as Mac>::new_from_slice(&nonce_key)
            .expect("HMAC takes a key of any length");
        chunk_key.zeroize();
        nonce_key.zeroize();
        Ok(Stream {
            cipher,
            nonce_key: mac,
            id,
            layout,
            size: file.size,
        })
    }

    fn associated_data(&self, index: u64) -> [u8; AD_LEN] {
        let mut ad = [0u8; AD_LEN];
        let (label, rest) = ad.split_at_mut(AD_LABEL.len());
        label.copy_from_slice(AD_LABEL);
        rest[..32].copy_from_slice(&self.id.0);
        rest[32..40].copy_from_slice(&self.layout.chunks.to_be_bytes());
        rest[40..48].copy_from_slice(&self.size.to_be_bytes());
        rest[48..56].copy_from_slice(&index.to_be_bytes());
        rest[56] = u8::from(index + 1 == self.layout.chunks);
        ad
    }

    fn nonce_mac(&self, ad: &[u8], plain: &[u8]) -> Hmac<Sha256> {
        let mut mac = self.nonce_key.clone();
        mac.update(ad);
        mac.update(plain);
        mac
    }

    /// Seals `buf`, which holds [`NONCE_LEN`] bytes of room and then the chunk's padded
    /// plaintext, into the stored chunk.
    fn seal(&self, index: u64, buf: &mut Vec<u8>) {
        self.seal_with(&self.associated_data(index), buf);
    }

    fn seal_with(&self, ad: &[u8], buf: &mut Vec<u8>) {
        let (nonce, plain) = buf.split_at_mut(NONCE_LEN);
        let digest = self.nonce_mac(ad, plain).finalize().into_bytes();
        nonce.copy_from_slice(&digest[..NONCE_LEN]);
        let tag = self
            .cipher
            .encrypt_in_place_detached(XNonce::from_slice(nonce), ad, plain)
            .expect("a chunk is far below XChaCha20-Poly1305's length limit");
        buf.extend_from_slice(&tag);
    }
}

/// One chunk of a sealed file, as it is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedChunk {
    pub index: u64,
    pub bytes: Vec<u8>,
}

impl SealedChunk {
    /// SHA-256 of the stored bytes, to compare with what a server reports it holds for this
    /// index ([`HaveChunk`](crate::api::HaveChunk)).
    pub fn sha256(&self) -> [u8; 32] {
        Sha256::digest(&self.bytes).into()
    }
}

/// Room for a nonce, then a chunk of `plain` bytes and its tag.
fn chunk_buffer(plain: usize) -> Vec<u8> {
    let mut buf = Vec::with_capacity(plain + CHUNK_OVERHEAD);
    buf.resize(NONCE_LEN, 0);
    buf
}

/// Seals a file whose [`FileRef`] is already known, from its bytes given in pieces of any size,
/// into its chunks in order.
///
/// The key and the name both come from the content hash, so the bytes are read twice: once by a
/// [`FileHasher`] for the reference, then here. Two sealers given the same library and bytes
/// produce the same chunks, so any device can resume or finish another's upload. Each chunk's
/// nonce is derived from the chunk itself, so different bytes sealed under one reference never
/// share a nonce.
///
/// The sealer checks the bytes against the reference and gives out no chunk that holds the end of
/// the file until they match: a file that changed after it was hashed is
/// [`FileError::HashMismatch`] or [`FileError::SizeMismatch`], and never a different file under
/// the reference's name. Chunks it gave out before that hold the wrong content; the upload cannot
/// be completed from them, since its last chunk is never given out, and a device that resumes it
/// uploads again every chunk whose stored hash is not that of the chunk it sealed.
///
/// It holds one chunk at a time.
pub struct FileSealer {
    stream: Stream,
    file: FileRef,
    hasher: Sha256,
    fed: u64,
    index: u64,
    buf: Vec<u8>,
    verified: bool,
    failed: Option<FileError>,
}

impl FileSealer {
    pub fn new(keys: &LibraryKeys, file: FileRef) -> Result<FileSealer, FileError> {
        FileSealer::with_chunk(keys, file, FILE_CHUNK_LEN as u64)
    }

    fn with_chunk(keys: &LibraryKeys, file: FileRef, chunk: u64) -> Result<FileSealer, FileError> {
        let stream = Stream::new(keys, &file, chunk)?;
        let buf = chunk_buffer(stream.layout.plain_len(0));
        Ok(FileSealer {
            stream,
            file,
            hasher: Sha256::new(),
            fed: 0,
            index: 0,
            buf,
            verified: false,
            failed: None,
        })
    }

    /// The name the server stores this file under.
    pub fn blob_id(&self) -> BlobId {
        self.stream.id
    }

    pub fn layout(&self) -> FileLayout {
        self.stream.layout
    }

    /// Takes bytes of the file, from where the last call stopped, and says how many it took: all
    /// of `data`, or fewer once the chunk being built is full, when it takes no more until
    /// [`FileSealer::next_chunk`] has given that chunk out.
    pub fn feed(&mut self, data: &[u8]) -> Result<usize, FileError> {
        if let Some(error) = self.failed {
            return Err(error);
        }
        if data.len() as u64 > self.file.size - self.fed {
            return Err(self.fail(FileError::SizeMismatch));
        }
        if self.index == self.stream.layout.chunks {
            return Ok(0);
        }
        let room = self.stream.layout.plain_len(self.index) + NONCE_LEN - self.buf.len();
        let take = room.min(data.len());
        self.buf.extend_from_slice(&data[..take]);
        self.hasher.update(&data[..take]);
        self.fed += take as u64;
        Ok(take)
    }

    /// The next sealed chunk once it is whole, in order; `None` while it needs more of the file,
    /// and after the last.
    pub fn next_chunk(&mut self) -> Result<Option<SealedChunk>, FileError> {
        if let Some(error) = self.failed {
            return Err(error);
        }
        let layout = self.stream.layout;
        if self.index == layout.chunks {
            return Ok(None);
        }
        let want = layout.plain_len(self.index);
        if self.fed == self.file.size {
            if !self.verified {
                let hash = ContentHash(std::mem::take(&mut self.hasher).finalize().into());
                if hash != self.file.hash {
                    return Err(self.fail(FileError::HashMismatch));
                }
                self.verified = true;
            }
            self.buf.resize(NONCE_LEN + want, 0);
        }
        if self.buf.len() < NONCE_LEN + want {
            return Ok(None);
        }
        let index = self.index;
        self.index += 1;
        let next = if self.index < layout.chunks {
            chunk_buffer(layout.plain_len(self.index))
        } else {
            Vec::new()
        };
        let mut bytes = std::mem::replace(&mut self.buf, next);
        self.stream.seal(index, &mut bytes);
        Ok(Some(SealedChunk { index, bytes }))
    }

    /// Succeeds once every chunk has been taken. Fails if the file ended before the reference's
    /// size, or if chunks are left.
    pub fn finish(self) -> Result<(), FileError> {
        if let Some(error) = self.failed {
            Err(error)
        } else if self.fed < self.file.size {
            Err(FileError::SizeMismatch)
        } else if self.index < self.stream.layout.chunks {
            Err(FileError::Incomplete)
        } else {
            Ok(())
        }
    }

    fn fail(&mut self, error: FileError) -> FileError {
        self.failed = Some(error);
        error
    }
}

/// Opens the chunks of one file, in any order.
///
/// Each chunk is authenticated as chunk `index` of this file: one moved, cut, extended, sealed
/// for another file or another library, with its padding changed, or with any nonce but the one
/// its plaintext gives, does not open. A chunk therefore has exactly one stored form. That does not
/// prove the file: a key holder could seal other bytes under the reference. Before trusting the
/// file, check it against the reference with a [`FileHasher`]: feed it each opened chunk in
/// index order as it is written, or, for chunks fetched out of order, rehash the assembled file.
pub struct FileOpener {
    stream: Stream,
}

impl FileOpener {
    pub fn new(keys: &LibraryKeys, file: FileRef) -> Result<FileOpener, FileError> {
        FileOpener::with_chunk(keys, file, FILE_CHUNK_LEN as u64)
    }

    fn with_chunk(keys: &LibraryKeys, file: FileRef, chunk: u64) -> Result<FileOpener, FileError> {
        Ok(FileOpener {
            stream: Stream::new(keys, &file, chunk)?,
        })
    }

    /// The name the server stores this file under.
    pub fn blob_id(&self) -> BlobId {
        self.stream.id
    }

    pub fn layout(&self) -> FileLayout {
        self.stream.layout
    }

    /// The file's bytes in stored chunk `index`, without padding. A chunk of the wrong length is
    /// refused before any decryption.
    pub fn open(&self, index: u64, stored: &[u8]) -> Result<Vec<u8>, FileError> {
        let layout = self.stream.layout;
        let expected = layout
            .stored_chunk_len(index)
            .ok_or(FileError::IndexOutOfRange {
                index,
                count: layout.chunks,
            })?;
        if stored.len() as u64 != expected {
            return Err(FileError::WrongLength {
                index,
                expected,
                actual: stored.len() as u64,
            });
        }
        let (nonce, rest) = stored.split_at(NONCE_LEN);
        let (ciphertext, tag) = rest.split_at(rest.len() - TAG_LEN);
        let ad = self.stream.associated_data(index);
        let mut buf = ciphertext.to_vec();
        self.stream
            .cipher
            .decrypt_in_place_detached(
                XNonce::from_slice(nonce),
                &ad,
                &mut buf,
                Tag::from_slice(tag),
            )
            .map_err(|_| FileError::DoesNotOpen { index })?;
        self.stream
            .nonce_mac(&ad, &buf)
            .verify_truncated_left(nonce)
            .map_err(|_| FileError::DoesNotOpen { index })?;
        let data = layout.data_len(index);
        if buf[data..].iter().any(|&b| b != 0) {
            return Err(FileError::BadPadding { index });
        }
        buf.truncate(data);
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::keys::LibraryId;

    fn keys() -> LibraryKeys {
        LibraryKeys::new(LibraryId([1; 16]), [2; 32])
    }

    fn content(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
            .collect()
    }

    /// Seals `bytes` feeding it in pieces of the lengths in `cuts`, repeated.
    fn seal_with(keys: &LibraryKeys, bytes: &[u8], chunk: u64, cuts: &[usize]) -> Vec<Vec<u8>> {
        let mut sealer = FileSealer::with_chunk(keys, FileRef::of(bytes), chunk).unwrap();
        let mut out = Vec::new();
        let mut rest = bytes;
        let mut cut = cuts.iter().cycle();
        while !rest.is_empty() {
            let piece = (*cut.next().unwrap()).clamp(1, rest.len());
            let mut piece_rest = &rest[..piece];
            while !piece_rest.is_empty() {
                let took = sealer.feed(piece_rest).unwrap();
                piece_rest = &piece_rest[took..];
                while let Some(c) = sealer.next_chunk().unwrap() {
                    assert_eq!(c.index, out.len() as u64);
                    out.push(c.bytes);
                }
            }
            rest = &rest[piece..];
        }
        while let Some(c) = sealer.next_chunk().unwrap() {
            assert_eq!(c.index, out.len() as u64);
            out.push(c.bytes);
        }
        sealer.finish().unwrap();
        out
    }

    fn open_all(keys: &LibraryKeys, file: FileRef, chunk: u64, chunks: &[Vec<u8>]) -> Vec<u8> {
        let opener = FileOpener::with_chunk(keys, file, chunk).unwrap();
        let mut hasher = FileHasher::new();
        let mut out = Vec::new();
        for (i, c) in chunks.iter().enumerate() {
            let plain = opener.open(i as u64, c).unwrap();
            hasher.update(&plain);
            out.extend_from_slice(&plain);
        }
        hasher.verify(&file).unwrap();
        out
    }

    #[test]
    fn padme_matches_a_table() {
        let table: &[(u64, u64)] = &[
            (0, 0),
            (1, 1),
            (2, 2),
            (3, 3),
            (7, 7),
            (8, 8),
            (9, 10),
            (15, 16),
            (17, 18),
            (33, 36),
            (65, 72),
            (129, 144),
            (255, 256),
            (257, 272),
            (1000, 1024),
            (4_194_305, 4_325_376),
            (1 << 28, 1 << 28),
            ((1 << 28) + 1, (1 << 28) + (1 << 23)),
            (0xFE00_0000_0000_0000, 0xFE00_0000_0000_0000),
        ];
        for &(n, padded) in table {
            assert_eq!(padme(n), Some(padded), "{n}");
        }
        assert_eq!(padme(0xFE00_0000_0000_0001), None);
        assert_eq!(padme(u64::MAX), None);
    }

    #[test]
    fn padme_holds_its_properties_for_every_small_length() {
        let mut last = 0;
        for n in 0..1 << 16 {
            let p = padme(n).unwrap();
            assert!(p >= n && (p - n) * 8 < n.max(1), "{n}");
            assert!(p >= last, "{n}");
            assert_eq!(padme(p), Some(p), "{n}");
            last = p;
        }
    }

    proptest! {
        #[test]
        fn padme_holds_its_properties(n in any::<u64>(), m in any::<u64>()) {
            match padme(n) {
                Some(p) => {
                    prop_assert!(p >= n);
                    prop_assert!(u128::from(p - n) * 8 < u128::from(n.max(1)));
                    prop_assert_eq!(padme(p), Some(p));
                }
                None => prop_assert!(n > 0xFE00_0000_0000_0000),
            }
            let (lo, hi) = (n.min(m), n.max(m));
            if let (Some(a), Some(b)) = (padme(lo), padme(hi)) {
                prop_assert!(a <= b);
            }
            if padme(lo).is_none() {
                prop_assert!(padme(hi).is_none());
            }
        }
    }

    #[test]
    fn layouts_add_up() {
        let chunk = FILE_CHUNK_LEN as u64;
        for (size, chunks, last) in [
            (0, 1, 40),
            (1, 1, 41),
            (chunk, 1, chunk + 40),
            (chunk + 1, 2, (1 << 17) + 40),
            ((1 << 28) + chunk, 66, chunk + 40),
        ] {
            let layout = FileRef {
                hash: ContentHash([0; 32]),
                size,
            }
            .layout()
            .unwrap();
            assert_eq!(layout.chunk_count(), chunks, "{size}");
            assert_eq!(layout.stored_chunk_len(chunks - 1), Some(last), "{size}");
            assert_eq!(layout.stored_chunk_len(chunks), None);
            let total: u64 = (0..chunks)
                .map(|i| layout.stored_chunk_len(i).unwrap())
                .sum();
            assert_eq!(total, layout.stored_size());
            assert_eq!(layout.stored_size(), layout.padded_size() + 40 * chunks);
        }
        let huge = FileRef {
            hash: ContentHash([0; 32]),
            size: u64::MAX,
        };
        assert_eq!(huge.layout(), Err(FileError::TooLarge));
        assert_eq!(
            FileSealer::new(&keys(), huge).err(),
            Some(FileError::TooLarge)
        );
        let largest = FileRef {
            hash: ContentHash([0; 32]),
            size: 0xFE00_0000_0000_0000,
        };
        assert!(largest.layout().is_ok());
    }

    #[test]
    fn padding_can_fill_whole_chunks() {
        let chunk = 16;
        let bytes = content(528, 0);
        let layout = FileLayout::new(528, chunk).unwrap();
        assert_eq!(layout.padded_size(), 544);
        assert_eq!(layout.chunk_count(), 34);
        let chunks = seal_with(&keys(), &bytes, chunk, &[7]);
        let opener = FileOpener::with_chunk(&keys(), FileRef::of(&bytes), chunk).unwrap();
        assert_eq!(opener.open(33, &chunks[33]).unwrap(), Vec::<u8>::new());
        assert_eq!(
            open_all(&keys(), FileRef::of(&bytes), chunk, &chunks),
            bytes
        );
    }

    proptest! {
        #[test]
        fn sealed_files_open_to_their_bytes(
            len in 0usize..3000,
            chunk in prop_oneof![Just(16u64), Just(64), Just(1000)],
            cuts in proptest::collection::vec(1usize..400, 1..6),
            seed in any::<u8>(),
        ) {
            let bytes = content(len, seed);
            let file = FileRef::of(&bytes);
            let chunks = seal_with(&keys(), &bytes, chunk, &cuts);
            let layout = FileLayout::new(len as u64, chunk).unwrap();
            prop_assert_eq!(chunks.len() as u64, layout.chunk_count());
            for (i, c) in chunks.iter().enumerate() {
                prop_assert_eq!(Some(c.len() as u64), layout.stored_chunk_len(i as u64));
            }
            prop_assert_eq!(&chunks, &seal_with(&keys(), &bytes, chunk, &[usize::MAX]));
            prop_assert_eq!(open_all(&keys(), file, chunk, &chunks), bytes.clone());

            let opener = FileOpener::with_chunk(&keys(), file, chunk).unwrap();
            let mut assembled = vec![0u8; len];
            for i in (0..chunks.len()).rev() {
                let plain = opener.open(i as u64, &chunks[i]).unwrap();
                let at = i * chunk as usize;
                if !plain.is_empty() {
                    assembled[at..at + plain.len()].copy_from_slice(&plain);
                }
            }
            prop_assert_eq!(FileRef::of(&assembled), file);
        }

        #[test]
        fn a_changed_file_never_yields_its_last_chunk(
            len in 1usize..600,
            at in any::<prop::sample::Index>(),
            grow in any::<bool>(),
        ) {
            let chunk = 64;
            let bytes = content(len, 3);
            let mut changed = bytes.clone();
            if grow {
                changed.push(0);
            } else {
                changed[at.index(len)] ^= 1;
            }
            let mut sealer = FileSealer::with_chunk(&keys(), FileRef::of(&bytes), chunk).unwrap();
            let last = sealer.layout().chunk_count() - 1;
            let mut rest = &changed[..];
            let mut error = None;
            'outer: loop {
                match sealer.feed(rest) {
                    Ok(took) => rest = &rest[took..],
                    Err(e) => { error = Some(e); break; }
                }
                loop {
                    match sealer.next_chunk() {
                        Ok(Some(c)) => prop_assert!(c.index < last),
                        Ok(None) => break,
                        Err(e) => { error = Some(e); break 'outer; }
                    }
                }
                if rest.is_empty() {
                    break;
                }
            }
            let error = error.or(sealer.finish().err());
            prop_assert!(matches!(error, Some(FileError::HashMismatch | FileError::SizeMismatch)));
        }
    }

    #[test]
    fn a_short_file_or_untaken_chunks_do_not_finish() {
        let bytes = content(100, 0);
        let mut sealer = FileSealer::with_chunk(&keys(), FileRef::of(&bytes), 64).unwrap();
        assert_eq!(sealer.feed(&bytes[..99]), Ok(64));
        assert!(sealer.next_chunk().unwrap().is_some());
        assert_eq!(sealer.feed(&bytes[64..99]), Ok(35));
        assert_eq!(sealer.next_chunk(), Ok(None));
        assert_eq!(sealer.finish(), Err(FileError::SizeMismatch));

        let mut sealer = FileSealer::with_chunk(&keys(), FileRef::of(&bytes), 64).unwrap();
        assert_eq!(sealer.feed(&bytes), Ok(64));
        assert_eq!(sealer.feed(&bytes[64..]), Ok(0));
        assert!(sealer.next_chunk().unwrap().is_some());
        assert_eq!(sealer.feed(&bytes[64..]), Ok(36));
        assert_eq!(sealer.finish(), Err(FileError::Incomplete));
    }

    fn small() -> (Vec<u8>, FileRef, Vec<Vec<u8>>) {
        let bytes = content(150, 9);
        let chunks = seal_with(&keys(), &bytes, 64, &[usize::MAX]);
        (bytes.clone(), FileRef::of(&bytes), chunks)
    }

    #[test]
    fn every_bit_flip_truncation_and_extension_is_refused() {
        let (_, file, chunks) = small();
        let opener = FileOpener::with_chunk(&keys(), file, 64).unwrap();
        for (i, c) in chunks.iter().enumerate() {
            let i = i as u64;
            for bit in 0..c.len() * 8 {
                let mut bad = c.clone();
                bad[bit / 8] ^= 1 << (bit % 8);
                assert_eq!(
                    opener.open(i, &bad),
                    Err(FileError::DoesNotOpen { index: i })
                );
            }
            for cut in 0..c.len() {
                assert!(matches!(
                    opener.open(i, &c[..cut]),
                    Err(FileError::WrongLength { .. })
                ));
            }
            let mut longer = c.clone();
            longer.push(0);
            assert!(matches!(
                opener.open(i, &longer),
                Err(FileError::WrongLength { .. })
            ));
        }
    }

    #[test]
    fn a_chunk_opens_only_in_its_place_in_its_file() {
        let (bytes, file, chunks) = small();
        assert_eq!(chunks.len(), 3);
        let opener = FileOpener::with_chunk(&keys(), file, 64).unwrap();
        assert_eq!(
            opener.open(1, &chunks[0]),
            Err(FileError::DoesNotOpen { index: 1 })
        );
        assert_eq!(
            opener.open(0, &chunks[1]),
            Err(FileError::DoesNotOpen { index: 0 })
        );
        assert_eq!(
            opener.open(3, &chunks[2]),
            Err(FileError::IndexOutOfRange { index: 3, count: 3 })
        );
        assert_eq!(
            opener.open(u64::MAX, &chunks[2]),
            Err(FileError::IndexOutOfRange {
                index: u64::MAX,
                count: 3
            })
        );

        let mut other = bytes.clone();
        other[0] ^= 1;
        let other_chunks = seal_with(&keys(), &other, 64, &[usize::MAX]);
        assert_eq!(
            opener.open(1, &other_chunks[1]),
            Err(FileError::DoesNotOpen { index: 1 })
        );

        let longer = FileRef {
            hash: file.hash,
            size: file.size + 1,
        };
        let opener = FileOpener::with_chunk(&keys(), longer, 64).unwrap();
        assert_eq!(opener.layout().chunk_count(), 3);
        assert_eq!(
            opener.open(0, &chunks[0]),
            Err(FileError::DoesNotOpen { index: 0 })
        );

        let elsewhere = LibraryKeys::new(LibraryId([5; 16]), [2; 32]);
        let opener = FileOpener::with_chunk(&elsewhere, file, 64).unwrap();
        assert_eq!(
            opener.open(0, &chunks[0]),
            Err(FileError::DoesNotOpen { index: 0 })
        );
        let other_secret = LibraryKeys::new(LibraryId([1; 16]), [5; 32]);
        let opener = FileOpener::with_chunk(&other_secret, file, 64).unwrap();
        assert_eq!(
            opener.open(0, &chunks[0]),
            Err(FileError::DoesNotOpen { index: 0 })
        );
    }

    #[test]
    fn the_last_flag_and_the_padding_are_authenticated_and_checked() {
        let (bytes, file, chunks) = small();
        let stream = Stream::new(&keys(), &file, 64).unwrap();
        let opener = FileOpener::with_chunk(&keys(), file, 64).unwrap();

        let mut ad = stream.associated_data(1);
        ad[AD_LEN - 1] = 1;
        let mut flagged = chunk_buffer(64);
        flagged.extend_from_slice(&opener.open(1, &chunks[1]).unwrap());
        stream.seal_with(&ad, &mut flagged);
        assert_eq!(
            opener.open(1, &flagged),
            Err(FileError::DoesNotOpen { index: 1 })
        );

        let mut tail = chunk_buffer(32);
        tail.extend_from_slice(&bytes[128..]);
        tail.resize(NONCE_LEN + opener.layout().plain_len(2), 0);
        *tail.last_mut().unwrap() = 1;
        stream.seal(2, &mut tail);
        assert_eq!(
            opener.open(2, &tail),
            Err(FileError::BadPadding { index: 2 })
        );
    }

    #[test]
    fn a_chunk_has_one_stored_form() {
        let (_, file, chunks) = small();
        let stream = Stream::new(&keys(), &file, 64).unwrap();
        let opener = FileOpener::with_chunk(&keys(), file, 64).unwrap();
        let plain = opener.open(0, &chunks[0]).unwrap();
        let ad = stream.associated_data(0);
        for chosen in [[0u8; 24], [0xff; 24], {
            let mut n = [0u8; 24];
            n.copy_from_slice(&chunks[0][..24]);
            n[0] ^= 1;
            n
        }] {
            let mut forged = chosen.to_vec();
            let mut body = plain.clone();
            let tag = stream
                .cipher
                .encrypt_in_place_detached(XNonce::from_slice(&chosen), &ad, &mut body)
                .unwrap();
            forged.extend_from_slice(&body);
            forged.extend_from_slice(&tag);
            assert_eq!(
                opener.open(0, &forged),
                Err(FileError::DoesNotOpen { index: 0 })
            );
        }
    }

    proptest! {
        #[test]
        fn different_bytes_under_one_reference_never_share_a_nonce(
            a in proptest::collection::vec(any::<u8>(), 64),
            b in proptest::collection::vec(any::<u8>(), 64),
            index in 0u64..3,
        ) {
            let (_, file, _) = small();
            let stream = Stream::new(&keys(), &file, 64).unwrap();
            let sealed = |plain: &[u8]| {
                let mut buf = chunk_buffer(64);
                buf.extend_from_slice(plain);
                stream.seal(index, &mut buf);
                buf
            };
            let (x, y) = (sealed(&a), sealed(&b));
            prop_assert_eq!(a == b, x[..NONCE_LEN] == y[..NONCE_LEN]);
            prop_assert_eq!(x.clone(), sealed(&a));
        }

        #[test]
        fn a_file_changed_after_hashing_seals_its_changed_chunks_under_new_nonces(
            at in 0usize..150,
        ) {
            let bytes = content(150, 9);
            let mut changed = bytes.clone();
            changed[at] ^= 1;
            let honest = seal_with(&keys(), &bytes, 64, &[usize::MAX]);
            let mut sealer = FileSealer::with_chunk(&keys(), FileRef::of(&bytes), 64).unwrap();
            let mut rest = &changed[..];
            let mut out = Vec::new();
            while !rest.is_empty() {
                let took = sealer.feed(rest).unwrap();
                rest = &rest[took..];
                while let Ok(Some(c)) = sealer.next_chunk() {
                    out.push(c.bytes);
                }
            }
            prop_assert_eq!(sealer.next_chunk(), Err(FileError::HashMismatch));
            for (i, c) in out.iter().enumerate() {
                let same = (i * 64..(i + 1) * 64).contains(&at);
                prop_assert_eq!(c[..NONCE_LEN] == honest[i][..NONCE_LEN], !same);
            }
        }
    }

    #[test]
    fn a_chunk_hash_is_that_of_its_stored_bytes() {
        let (_, _, chunks) = small();
        let chunk = SealedChunk {
            index: 0,
            bytes: chunks[0].clone(),
        };
        assert_eq!(chunk.sha256(), <[u8; 32]>::from(Sha256::digest(&chunks[0])));
    }

    #[test]
    fn the_same_bytes_seal_alike_in_one_library_only() {
        let bytes = content(500, 1);
        let a = seal_with(&keys(), &bytes, 64, &[3]);
        assert_eq!(a, seal_with(&keys(), &bytes, 64, &[200, 1]));
        let elsewhere = LibraryKeys::new(LibraryId([5; 16]), [2; 32]);
        let b = seal_with(&elsewhere, &bytes, 64, &[3]);
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(&b) {
            assert_ne!(x, y);
        }
        let file = FileRef::of(&bytes);
        assert_ne!(
            keys().file_name_key().blob_id(&file.hash),
            elsewhere.file_name_key().blob_id(&file.hash)
        );
    }

    #[test]
    fn the_real_chunk_size_round_trips_across_a_boundary() {
        let bytes = content(FILE_CHUNK_LEN + 1, 4);
        let file = FileRef::of(&bytes);
        let chunks = seal_with(&keys(), &bytes, FILE_CHUNK_LEN as u64, &[1 << 20, 12_345]);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), SEALED_CHUNK_LEN);
        let opener = FileOpener::new(&keys(), file).unwrap();
        assert_eq!(
            opener.open(1, &chunks[1]).unwrap(),
            &bytes[FILE_CHUNK_LEN..]
        );
        assert_eq!(
            open_all(&keys(), file, FILE_CHUNK_LEN as u64, &chunks),
            bytes
        );
    }

    #[test]
    fn references_round_trip_and_refuse_anything_else() {
        for size in [0, 1, 127, 128, u64::MAX] {
            let file = FileRef {
                hash: ContentHash([7; 32]),
                size,
            };
            let bytes = file.encode();
            assert_eq!(FileRef::decode(&bytes), Ok(file));
            for cut in 0..bytes.len() {
                assert_eq!(FileRef::decode(&bytes[..cut]), Err(DecodeError::Truncated));
            }
            let mut longer = bytes.clone();
            longer.push(0);
            assert_eq!(FileRef::decode(&longer), Err(DecodeError::TrailingBytes));
            let mut other = bytes;
            other[0] = 2;
            assert_eq!(
                FileRef::decode(&other),
                Err(DecodeError::UnsupportedVersion(2))
            );
        }
        let mut padded = vec![1];
        padded.extend_from_slice(&[0; 32]);
        padded.extend_from_slice(&[0x81, 0x00]);
        assert_eq!(FileRef::decode(&padded), Err(DecodeError::BadLength));
    }

    proptest! {
        #[test]
        fn decoding_arbitrary_references_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..64)) {
            if let Ok(file) = FileRef::decode(&bytes) {
                prop_assert_eq!(file.encode(), bytes);
            }
        }

        #[test]
        fn opening_arbitrary_bytes_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..200),
            index in 0u64..6,
            size in 0u64..300,
        ) {
            let file = FileRef { hash: ContentHash([1; 32]), size };
            let opener = FileOpener::with_chunk(&keys(), file, 64).unwrap();
            prop_assert!(opener.open(index, &bytes).is_err());
        }

        #[test]
        fn blob_ids_read_only_their_own_text(id in any::<[u8; 32]>(), text in "[0-9a-fA-F]{0,66}") {
            let id = BlobId(id);
            prop_assert_eq!(BlobId::from_hex(&id.to_hex()), Some(id));
            if let Some(read) = BlobId::from_hex(&text) {
                prop_assert_eq!(read.to_hex(), text);
            }
        }
    }

    #[test]
    fn a_content_hash_prints_only_a_prefix() {
        let hash = ContentHash([0xab; 32]);
        assert_eq!(format!("{hash:?}"), "ContentHash(abababab..)");
    }

    #[test]
    fn blob_ids_are_strict() {
        let hex = "ab".repeat(32);
        assert!(BlobId::from_hex(&hex).is_some());
        assert_eq!(BlobId::from_hex(&hex.to_uppercase()), None);
        assert_eq!(BlobId::from_hex(&hex[1..]), None);
        assert_eq!(BlobId::from_hex(&format!("{hex}0")), None);
        assert_eq!(BlobId::from_hex(&format!("+{}", &hex[1..])), None);
        let json = serde_json::to_string(&BlobId([0xab; 32])).unwrap();
        assert_eq!(json, format!("\"{hex}\""));
        assert!(serde_json::from_str::<BlobId>(&json.to_uppercase()).is_err());
    }
}

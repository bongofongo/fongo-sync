use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use fongo_sync_core::Change;
use thiserror::Error;
use zeroize::Zeroize;

use crate::batch::{decode_batch, encode_batch};
use crate::keys::{DeviceId, LibraryId, LibraryKeys};
use crate::wire::{DecodeError, Reader, put_blob};

/// The envelope format this crate writes, and the only one it reads.
pub const ENVELOPE_FORMAT: u8 = 1;

const SIGNATURE_LABEL: &[u8] = b"fongo-sync/v1/envelope\0";
const AEAD_LABEL: &[u8] = b"fongo-sync/v1/batch\0";

/// A sealed batch as it travels and as the server stores it. The server can read every field but
/// cannot decrypt `ciphertext`.
///
/// Bytes: format, library id, device id, epoch (u32 big-endian), nonce, ciphertext (LEB128 length
/// then bytes), then an Ed25519 signature over a domain label followed by every preceding byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub library: LibraryId,
    pub device: DeviceId,
    pub epoch: u32,
    pub nonce: [u8; 24],
    pub ciphertext: Vec<u8>,
    pub signature: Signature,
}

impl Envelope {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = self.unsigned_bytes();
        out.extend_from_slice(&self.signature.to_bytes());
        out
    }

    /// Parses the structure without checking the signature. Fails on anything `encode` would not
    /// write. Allocation is bounded by a constant multiple of the input's length, because every
    /// length is checked against the remaining input first.
    pub fn decode(bytes: &[u8]) -> Result<Envelope, DecodeError> {
        let mut r = Reader::new(bytes);
        let version = r.u8()?;
        if version != ENVELOPE_FORMAT {
            return Err(DecodeError::UnsupportedVersion(version));
        }
        let library = LibraryId(r.array()?);
        let device = DeviceId(r.array()?);
        let epoch = r.u32()?;
        let nonce = r.array()?;
        let ciphertext = r.blob()?.to_vec();
        let signature = Signature::from_bytes(&r.array()?);
        r.finish()?;
        Ok(Envelope {
            library,
            device,
            epoch,
            nonce,
            ciphertext,
            signature,
        })
    }

    fn unsigned_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 16 + 16 + 4 + 24 + 10 + self.ciphertext.len() + 64);
        out.push(ENVELOPE_FORMAT);
        out.extend_from_slice(&self.library.0);
        out.extend_from_slice(&self.device.0);
        out.extend_from_slice(&self.epoch.to_be_bytes());
        out.extend_from_slice(&self.nonce);
        put_blob(&mut out, &self.ciphertext);
        out
    }

    fn verify(&self, key: &VerifyingKey) -> Result<(), EnvelopeError> {
        if DeviceId::of(key) != self.device {
            return Err(EnvelopeError::WrongDevice);
        }
        key.verify_strict(&signed_message(&self.unsigned_bytes()), &self.signature)
            .map_err(|_| EnvelopeError::BadSignature)
    }
}

/// Why an envelope was refused.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum EnvelopeError {
    #[error("malformed envelope: {0}")]
    Decode(DecodeError),
    #[error("envelope belongs to another library")]
    WrongLibrary,
    #[error("envelope names a device the library does not know")]
    UnknownDevice,
    #[error("the key given is not the key of the device the envelope names")]
    WrongDevice,
    #[error("signature does not verify")]
    BadSignature,
    #[error("ciphertext does not decrypt under this library's key for its epoch")]
    Decrypt,
    #[error("decrypted batch is malformed: {0}")]
    Batch(DecodeError),
}

/// A verified, decrypted envelope.
#[derive(Debug, Clone, PartialEq)]
pub struct Opened {
    pub device: DeviceId,
    pub epoch: u32,
    pub changes: Vec<Change>,
}

/// Encrypts `changes` under the library's key for `epoch` and signs the result as `device`.
///
/// `nonce` must never repeat for one library and epoch; 24 random bytes satisfy that.
pub fn seal(
    changes: &[Change],
    keys: &LibraryKeys,
    epoch: u32,
    device: &SigningKey,
    nonce: [u8; 24],
) -> Vec<u8> {
    let library = keys.id;
    let device_id = DeviceId::of(&device.verifying_key());
    let mut plaintext = encode_batch(changes);
    let ciphertext = cipher(keys, epoch)
        .encrypt(
            XNonce::from_slice(&nonce),
            Payload {
                msg: &plaintext,
                aad: &associated_data(library, device_id, epoch),
            },
        )
        .expect("a batch is far below XChaCha20-Poly1305's length limit");
    plaintext.zeroize();
    let mut envelope = Envelope {
        library,
        device: device_id,
        epoch,
        nonce,
        ciphertext,
        signature: Signature::from_bytes(&[0; 64]),
    };
    envelope.signature = device.sign(&signed_message(&envelope.unsigned_bytes()));
    envelope.encode()
}

/// Verifies an envelope's signature against the key `device_key` returns for its device, then
/// decrypts it. Needs the library secret; this is what devices call. It cannot tell a device key
/// from the recovery key, so a caller that must refuse envelopes the recovery key signed has to
/// keep that key out of what `device_key` returns.
pub fn open(
    bytes: &[u8],
    keys: &LibraryKeys,
    device_key: impl FnOnce(DeviceId) -> Option<VerifyingKey>,
) -> Result<Opened, EnvelopeError> {
    let envelope = Envelope::decode(bytes).map_err(EnvelopeError::Decode)?;
    if envelope.library != keys.id {
        return Err(EnvelopeError::WrongLibrary);
    }
    let key = device_key(envelope.device).ok_or(EnvelopeError::UnknownDevice)?;
    envelope.verify(&key)?;
    let mut plaintext = cipher(keys, envelope.epoch)
        .decrypt(
            XNonce::from_slice(&envelope.nonce),
            Payload {
                msg: &envelope.ciphertext,
                aad: &associated_data(envelope.library, envelope.device, envelope.epoch),
            },
        )
        .map_err(|_| EnvelopeError::Decrypt)?;
    let changes = decode_batch(&plaintext).map_err(EnvelopeError::Batch);
    plaintext.zeroize();
    Ok(Opened {
        device: envelope.device,
        epoch: envelope.epoch,
        changes: changes?,
    })
}

/// Checks an envelope's structure and that `device_key` signed it, without any library secret.
/// This is what the server calls; it must still check that the library and device are the ones
/// the request is for. It cannot tell a device key from the recovery key, so a server that must
/// refuse envelopes the recovery key signed has to check that itself.
pub fn verify_envelope(bytes: &[u8], device_key: &VerifyingKey) -> Result<Envelope, EnvelopeError> {
    let envelope = Envelope::decode(bytes).map_err(EnvelopeError::Decode)?;
    envelope.verify(device_key)?;
    Ok(envelope)
}

fn cipher(keys: &LibraryKeys, epoch: u32) -> XChaCha20Poly1305 {
    let mut key = keys.batch_key(epoch);
    let cipher = XChaCha20Poly1305::new(&key.into());
    key.zeroize();
    cipher
}

fn signed_message(unsigned: &[u8]) -> Vec<u8> {
    let mut message = Vec::with_capacity(SIGNATURE_LABEL.len() + unsigned.len());
    message.extend_from_slice(SIGNATURE_LABEL);
    message.extend_from_slice(unsigned);
    message
}

fn associated_data(library: LibraryId, device: DeviceId, epoch: u32) -> Vec<u8> {
    let mut aad = Vec::with_capacity(AEAD_LABEL.len() + 1 + 16 + 16 + 4);
    aad.extend_from_slice(AEAD_LABEL);
    aad.push(ENVELOPE_FORMAT);
    aad.extend_from_slice(&library.0);
    aad.extend_from_slice(&device.0);
    aad.extend_from_slice(&epoch.to_be_bytes());
    aad
}

#[cfg(test)]
mod tests {
    use fongo_sync_core::{Gid, Hlc, Value};
    use proptest::prelude::*;

    use super::*;
    use crate::batch::tests::arb_changes;

    fn library() -> LibraryKeys {
        LibraryKeys::new(LibraryId([1; 16]), [2; 32])
    }

    fn device() -> SigningKey {
        SigningKey::from_bytes(&[3; 32])
    }

    fn changes() -> Vec<Change> {
        vec![Change {
            collection: "things".into(),
            gid: Gid([4; 16]),
            field: "title".into(),
            value: Value::Text("hello".into()),
            clock: Hlc {
                wall_ms: 5,
                counter: 6,
                node: Gid([7; 16]),
            },
        }]
    }

    fn sealed() -> Vec<u8> {
        seal(&changes(), &library(), 0, &device(), [9; 24])
    }

    fn known(id: DeviceId) -> Option<VerifyingKey> {
        let key = device().verifying_key();
        (DeviceId::of(&key) == id).then_some(key)
    }

    #[test]
    fn a_sealed_batch_opens_to_what_was_sealed() {
        let opened = open(&sealed(), &library(), known).unwrap();
        assert_eq!(opened.changes, changes());
        assert_eq!(opened.epoch, 0);
        assert_eq!(opened.device, DeviceId::of(&device().verifying_key()));
    }

    #[test]
    fn the_server_check_accepts_a_valid_envelope_without_the_secret() {
        let envelope = verify_envelope(&sealed(), &device().verifying_key()).unwrap();
        assert_eq!(envelope.library, library().id);
        assert_eq!(envelope.epoch, 0);
        assert_eq!(envelope.encode(), sealed());
    }

    #[test]
    fn tampering_with_any_byte_is_refused() {
        let bytes = sealed();
        for i in 0..bytes.len() {
            let mut bad = bytes.clone();
            bad[i] ^= 0x01;
            assert!(open(&bad, &library(), known).is_err(), "byte {i}");
            assert!(
                verify_envelope(&bad, &device().verifying_key()).is_err(),
                "byte {i}"
            );
        }
        let mut longer = bytes.clone();
        longer.push(0);
        assert!(open(&longer, &library(), known).is_err());
        for cut in 0..bytes.len() {
            assert!(open(&bytes[..cut], &library(), known).is_err());
        }
    }

    #[test]
    fn every_proper_prefix_reports_truncation() {
        let bytes = sealed();
        for cut in 0..bytes.len() {
            assert_eq!(
                Envelope::decode(&bytes[..cut]),
                Err(DecodeError::Truncated),
                "cut {cut}"
            );
        }
    }

    #[test]
    fn another_librarys_keys_do_not_open_it() {
        let other_id = LibraryKeys::new(LibraryId([8; 16]), [2; 32]);
        assert_eq!(
            open(&sealed(), &other_id, known),
            Err(EnvelopeError::WrongLibrary)
        );
        let other_secret = LibraryKeys::new(library().id, [8; 32]);
        assert_eq!(
            open(&sealed(), &other_secret, known),
            Err(EnvelopeError::Decrypt)
        );
    }

    #[test]
    fn a_different_epoch_does_not_decrypt_even_when_re_signed() {
        let mut envelope = Envelope::decode(&sealed()).unwrap();
        envelope.epoch = 1;
        envelope.signature = device().sign(&signed_message(&envelope.unsigned_bytes()));
        assert_eq!(
            open(&envelope.encode(), &library(), known),
            Err(EnvelopeError::Decrypt)
        );
    }

    #[test]
    fn a_wrong_or_unknown_device_key_is_refused() {
        let stranger = SigningKey::from_bytes(&[5; 32]).verifying_key();
        assert_eq!(
            verify_envelope(&sealed(), &stranger),
            Err(EnvelopeError::WrongDevice)
        );
        assert_eq!(
            open(&sealed(), &library(), |_| Some(stranger)),
            Err(EnvelopeError::WrongDevice)
        );
        assert_eq!(
            open(&sealed(), &library(), |_| None),
            Err(EnvelopeError::UnknownDevice)
        );
    }

    #[test]
    fn a_device_cannot_pass_off_another_devices_envelope_as_its_own() {
        let mut envelope = Envelope::decode(&sealed()).unwrap();
        let thief = SigningKey::from_bytes(&[6; 32]);
        envelope.device = DeviceId::of(&thief.verifying_key());
        envelope.signature = thief.sign(&signed_message(&envelope.unsigned_bytes()));
        let bytes = envelope.encode();
        assert!(verify_envelope(&bytes, &thief.verifying_key()).is_ok());
        assert_eq!(
            open(&bytes, &library(), |_| Some(thief.verifying_key())),
            Err(EnvelopeError::Decrypt)
        );
    }

    proptest! {
        #![proptest_config(crate::proptest_config())]

        #[test]
        fn any_batch_seals_and_opens(changes in arb_changes(), epoch in any::<u32>(), nonce in any::<[u8; 24]>()) {
            let bytes = seal(&changes, &library(), epoch, &device(), nonce);
            let opened = open(&bytes, &library(), known).unwrap();
            prop_assert_eq!(crate::batch::encode_batch(&opened.changes), crate::batch::encode_batch(&changes));
        }

        #[test]
        fn decoding_arbitrary_bytes_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            let _ = Envelope::decode(&bytes);
            let _ = open(&bytes, &library(), known);
        }
    }
}

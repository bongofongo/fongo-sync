use std::fmt;

use ed25519_dalek::VerifyingKey;
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::keys::{LibraryKeys, WrapKey};

pub(crate) const MEMBER_KEY_INFO: &[u8] = b"fongo-sync/v1/member-key";
const MEMBER_PROOF_LABEL: &[u8] = b"fongo-sync/v1/member-proof\0";

/// A device's proof that whoever registered it held the library secret: HMAC-SHA256 under
/// [`LibraryKeys::member_proof`]'s key over the device's public key and its wrap key, if any.
///
/// It is for a device about to seal a new library secret: sealing only to devices whose proof
/// checks keeps a server from adding a key of its own and being handed the secret. It shows that
/// the registrant held the secret when it registered, and nothing later: not that it still holds
/// it, not that it is still trusted, and not that the device holds the private halves of the keys
/// it names. A server cannot check it, since it lacks the secret; it stores the proof and returns
/// it unchanged, and can drop it. Anyone who holds the secret can make one for any key.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MemberProof(pub [u8; 32]);

/// What checking a [`DeviceRecord`](crate::api::DeviceRecord)'s membership proof found.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemberCheck {
    /// The proof checks against this secret, for the record's key and wrap key.
    Valid,
    /// The record carries no proof: registered by a build that made none, by the recovery key,
    /// with the proof dropped by the server, or planted by the server, which is exactly what a key
    /// of its own looks like. It gives no assurance at all, and must never be treated more
    /// leniently than [`MemberCheck::Invalid`] when deciding who receives a secret; it is told
    /// apart only so that an application can tell a device that needs to register again from a
    /// forgery.
    Absent,
    /// The proof does not check: made under another secret, or for another key or wrap key.
    Invalid,
}

/// A [`DeviceRecord`](crate::api::DeviceRecord)'s `member_proof` as a server listed it. A record
/// is read leniently, so that one malformed proof does not make the whole device list unreadable:
/// anything but 64 hex digits is [`ListedMemberProof::Unreadable`], which checks as
/// [`MemberCheck::Invalid`] and is written back as an empty string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ListedMemberProof {
    Readable(MemberProof),
    Unreadable,
}

impl From<MemberProof> for ListedMemberProof {
    fn from(proof: MemberProof) -> ListedMemberProof {
        ListedMemberProof::Readable(proof)
    }
}

impl serde::Serialize for ListedMemberProof {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            ListedMemberProof::Readable(proof) => proof.serialize(s),
            ListedMemberProof::Unreadable => s.serialize_str(""),
        }
    }
}

impl<'de> serde::Deserialize<'de> for ListedMemberProof {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<ListedMemberProof, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum Lenient {
            Readable(MemberProof),
            Unreadable(serde::de::IgnoredAny),
        }
        Ok(match Lenient::deserialize(d)? {
            Lenient::Readable(proof) => ListedMemberProof::Readable(proof),
            Lenient::Unreadable(_) => ListedMemberProof::Unreadable,
        })
    }
}

impl MemberProof {
    /// Lowercase hex, 64 characters.
    pub fn to_hex(&self) -> String {
        crate::hex::encode(&self.0)
    }

    /// Accepts hex of either case; `None` unless it is exactly 64 hex digits.
    pub fn from_hex(text: &str) -> Option<MemberProof> {
        crate::hex::decode_array(text).map(MemberProof)
    }
}

impl fmt::Debug for MemberProof {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MemberProof({})", self.to_hex())
    }
}

impl serde::Serialize for MemberProof {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_hex())
    }
}

impl<'de> serde::Deserialize<'de> for MemberProof {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<MemberProof, D::Error> {
        let text = <std::borrow::Cow<'de, str>>::deserialize(d)?;
        MemberProof::from_hex(&text)
            .ok_or_else(|| serde::de::Error::custom("expected 64 hex digits"))
    }
}

impl LibraryKeys {
    /// The membership proof of a device with public key `device` and wrap key `wrap`.
    ///
    /// The key is HKDF-SHA256 with the library id as salt, the secret as input keying material
    /// and info `fongo-sync/v1/member-key`, as the batch key is derived. The proof is HMAC-SHA256
    /// under that key over `fongo-sync/v1/member-proof` and a zero byte, the library id's 16
    /// bytes, the device key's 32 bytes, then the byte 0 when there is no wrap key, or the byte 1
    /// followed by the wrap key's 32 bytes.
    pub fn member_proof(&self, device: &VerifyingKey, wrap: Option<&WrapKey>) -> MemberProof {
        MemberProof(self.member_mac(device, wrap).finalize().into_bytes().into())
    }

    /// Whether `proof` is [`LibraryKeys::member_proof`] of `device` and `wrap`, compared in
    /// constant time.
    pub fn verify_member_proof(
        &self,
        device: &VerifyingKey,
        wrap: Option<&WrapKey>,
        proof: &MemberProof,
    ) -> bool {
        self.member_mac(device, wrap).verify_slice(&proof.0).is_ok()
    }

    fn member_mac(&self, device: &VerifyingKey, wrap: Option<&WrapKey>) -> Hmac<Sha256> {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.member_key()[..])
            .expect("HMAC takes a key of any length");
        mac.update(MEMBER_PROOF_LABEL);
        mac.update(&self.id.0);
        mac.update(device.as_bytes());
        match wrap {
            None => mac.update(&[0]),
            Some(wrap) => {
                mac.update(&[1]);
                mac.update(&wrap.0);
            }
        }
        mac
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;

    use super::*;
    use crate::keys::LibraryId;

    fn key(seed: u8) -> VerifyingKey {
        SigningKey::from_bytes(&[seed; 32]).verifying_key()
    }

    #[test]
    fn a_proof_checks_only_for_its_library_secret_device_and_wrap_key() {
        let keys = LibraryKeys::new(LibraryId([1; 16]), [2; 32]);
        let wrap = WrapKey([5; 32]);
        let proof = keys.member_proof(&key(3), Some(&wrap));
        assert!(keys.verify_member_proof(&key(3), Some(&wrap), &proof));

        let other_secret = LibraryKeys::new(LibraryId([1; 16]), [9; 32]);
        let other_library = LibraryKeys::new(LibraryId([4; 16]), [2; 32]);
        assert!(!other_secret.verify_member_proof(&key(3), Some(&wrap), &proof));
        assert!(!other_library.verify_member_proof(&key(3), Some(&wrap), &proof));
        assert!(!keys.verify_member_proof(&key(4), Some(&wrap), &proof));
        assert!(!keys.verify_member_proof(&key(3), Some(&WrapKey([6; 32])), &proof));
        assert!(!keys.verify_member_proof(&key(3), None, &proof));

        let proofs = [
            proof,
            other_secret.member_proof(&key(3), Some(&wrap)),
            other_library.member_proof(&key(3), Some(&wrap)),
            keys.member_proof(&key(4), Some(&wrap)),
            keys.member_proof(&key(3), None),
            keys.member_proof(&key(3), Some(&WrapKey([0; 32]))),
        ];
        for (i, a) in proofs.iter().enumerate() {
            for b in &proofs[i + 1..] {
                assert_ne!(a, b);
            }
        }
        let without = keys.member_proof(&key(3), None);
        assert!(keys.verify_member_proof(&key(3), None, &without));
        assert!(!keys.verify_member_proof(&key(3), Some(&WrapKey([0; 32])), &without));

        let mut flipped = proof;
        flipped.0[31] ^= 1;
        assert!(!keys.verify_member_proof(&key(3), Some(&wrap), &flipped));
    }

    #[test]
    fn the_member_key_is_apart_from_the_other_keys() {
        let keys = LibraryKeys::new(LibraryId([1; 16]), [2; 32]);
        let member = *keys.member_key();
        assert_ne!(member, keys.batch_key(0));
        assert_ne!(&member, keys.file_name_key().as_bytes());
        assert_ne!(&member, keys.file_key_root().as_bytes());
        assert_ne!(member, keys.recovery_signing_key().to_bytes());
    }

    #[test]
    fn proofs_travel_as_hex() {
        let proof = MemberProof([0xab; 32]);
        assert_eq!(
            MemberProof::from_hex(&proof.to_hex().to_uppercase()),
            Some(proof)
        );
        assert_eq!(MemberProof::from_hex("ab"), None);
    }
}

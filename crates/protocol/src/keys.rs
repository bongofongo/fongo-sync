use std::fmt;

use ed25519_dalek::{SigningKey, VerifyingKey};
use hkdf::Hkdf;
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, ZeroizeOnDrop};

const BATCH_KEY_INFO: &[u8] = b"fongo-sync/v1/batch-key";
const RECOVERY_KEY_INFO: &[u8] = b"fongo-sync/v1/recovery-key";

/// The public name of a library. Carries no secret.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LibraryId(pub [u8; 16]);

/// A device's name: the first 16 bytes of SHA-256 of its Ed25519 public key.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeviceId(pub [u8; 16]);

impl DeviceId {
    pub fn of(key: &VerifyingKey) -> DeviceId {
        let digest = Sha256::digest(key.as_bytes());
        let mut id = [0u8; 16];
        id.copy_from_slice(&digest[..16]);
        DeviceId(id)
    }
}

macro_rules! hex_id {
    ($name:ident) => {
        impl $name {
            /// Lowercase hex, 32 characters.
            pub fn to_hex(&self) -> String {
                crate::hex::encode(&self.0)
            }

            /// Accepts hex of either case; `None` unless it is exactly 32 hex digits.
            pub fn from_hex(text: &str) -> Option<$name> {
                crate::hex::decode_array(text).map($name)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.to_hex())
            }
        }

        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.serialize_str(&self.to_hex())
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<$name, D::Error> {
                let text = <std::borrow::Cow<'de, str>>::deserialize(d)?;
                $name::from_hex(&text)
                    .ok_or_else(|| serde::de::Error::custom("expected 32 hex digits"))
            }
        }
    };
}

hex_id!(LibraryId);
hex_id!(DeviceId);

/// Everything a device needs to read and write a library's batches: its id and its 32-byte secret.
/// The secret never leaves the library's devices.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct LibraryKeys {
    #[zeroize(skip)]
    pub id: LibraryId,
    secret: [u8; 32],
}

impl LibraryKeys {
    pub fn new(id: LibraryId, secret: [u8; 32]) -> LibraryKeys {
        LibraryKeys { id, secret }
    }

    pub fn secret(&self) -> &[u8; 32] {
        &self.secret
    }

    /// A new library with a random id and secret.
    #[cfg(feature = "rand")]
    pub fn generate() -> Result<LibraryKeys, getrandom::Error> {
        let mut id = [0u8; 16];
        let mut secret = [0u8; 32];
        getrandom::fill(&mut id)?;
        getrandom::fill(&mut secret)?;
        Ok(LibraryKeys::new(LibraryId(id), secret))
    }

    /// The XChaCha20-Poly1305 key for batches sealed under `epoch`.
    pub(crate) fn batch_key(&self, epoch: u32) -> [u8; 32] {
        let mut info = Vec::with_capacity(BATCH_KEY_INFO.len() + 4);
        info.extend_from_slice(BATCH_KEY_INFO);
        info.extend_from_slice(&epoch.to_be_bytes());
        self.expand(&info)
    }

    /// A signing key every holder of the secret derives alike, so a device restored from the
    /// recovery code alone can authenticate to the server and add itself.
    pub fn recovery_signing_key(&self) -> SigningKey {
        let mut seed = self.expand(RECOVERY_KEY_INFO);
        let key = SigningKey::from_bytes(&seed);
        seed.zeroize();
        key
    }

    fn expand(&self, info: &[u8]) -> [u8; 32] {
        let mut out = [0u8; 32];
        Hkdf::<Sha256>::new(Some(&self.id.0), &self.secret)
            .expand(info, &mut out)
            .expect("32 bytes is a valid HKDF-SHA256 output length");
        out
    }
}

impl fmt::Debug for LibraryKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LibraryKeys")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// A new device signing key from the operating system's randomness.
#[cfg(feature = "rand")]
pub fn generate_signing_key() -> Result<SigningKey, getrandom::Error> {
    let mut seed = [0u8; 32];
    getrandom::fill(&mut seed)?;
    let key = SigningKey::from_bytes(&seed);
    seed.zeroize();
    Ok(key)
}

/// A random XChaCha20-Poly1305 nonce for `seal`.
#[cfg(feature = "rand")]
pub fn random_nonce() -> Result<[u8; 24], getrandom::Error> {
    let mut nonce = [0u8; 24];
    getrandom::fill(&mut nonce)?;
    Ok(nonce)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(id: u8, secret: u8) -> LibraryKeys {
        LibraryKeys::new(LibraryId([id; 16]), [secret; 32])
    }

    #[test]
    fn batch_keys_differ_by_epoch_library_and_secret() {
        let k = keys(1, 2);
        assert_eq!(k.batch_key(0), keys(1, 2).batch_key(0));
        assert_ne!(k.batch_key(0), k.batch_key(1));
        assert_ne!(k.batch_key(0), keys(3, 2).batch_key(0));
        assert_ne!(k.batch_key(0), keys(1, 3).batch_key(0));
        assert_ne!(k.batch_key(0), k.recovery_signing_key().to_bytes());
    }

    #[test]
    fn a_device_id_is_the_head_of_the_key_hash() {
        let key = SigningKey::from_bytes(&[7; 32]).verifying_key();
        let digest = Sha256::digest(key.as_bytes());
        assert_eq!(DeviceId::of(&key).0[..], digest[..16]);
    }

    #[test]
    fn ids_travel_as_hex() {
        let id = LibraryId([0xab; 16]);
        assert_eq!(LibraryId::from_hex(&id.to_hex().to_uppercase()), Some(id));
        assert_eq!(LibraryId::from_hex("ab"), None);
        assert!(!format!("{:?}", keys(1, 0x5a)).contains("5a5a"));
    }

    #[cfg(feature = "rand")]
    #[test]
    fn generated_keys_and_nonces_are_fresh() {
        let (a, b) = (
            LibraryKeys::generate().unwrap(),
            LibraryKeys::generate().unwrap(),
        );
        assert_ne!((a.id, a.secret()), (b.id, b.secret()));
        assert_ne!(
            generate_signing_key().unwrap().to_bytes(),
            generate_signing_key().unwrap().to_bytes()
        );
        assert_ne!(random_nonce().unwrap(), random_nonce().unwrap());
    }
}

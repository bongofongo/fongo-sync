use std::fmt;

use curve25519_dalek::montgomery::MontgomeryPoint;
use ed25519_dalek::{SigningKey, VerifyingKey};
use hkdf::Hkdf;
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

const BATCH_KEY_INFO: &[u8] = b"fongo-sync/v1/batch-key";
const RECOVERY_KEY_INFO: &[u8] = b"fongo-sync/v1/recovery-key";
const FILE_NAME_KEY_INFO: &[u8] = b"fongo-sync/v1/file-name-key";
const FILE_KEY_ROOT_INFO: &[u8] = b"fongo-sync/v1/file-key-root";

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

/// A device's X25519 public key for receiving a wrapped library secret. A server stores and
/// returns it; nothing in this version wraps a secret to it.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct WrapKey(pub [u8; 32]);

impl WrapKey {
    /// Lowercase hex, 64 characters.
    pub fn to_hex(&self) -> String {
        crate::hex::encode(&self.0)
    }

    /// Accepts hex of either case; `None` unless it is exactly 64 hex digits.
    pub fn from_hex(text: &str) -> Option<WrapKey> {
        crate::hex::decode_array(text).map(WrapKey)
    }
}

impl fmt::Debug for WrapKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WrapKey({})", self.to_hex())
    }
}

impl serde::Serialize for WrapKey {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_hex())
    }
}

impl<'de> serde::Deserialize<'de> for WrapKey {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<WrapKey, D::Error> {
        let text = <std::borrow::Cow<'de, str>>::deserialize(d)?;
        WrapKey::from_hex(&text).ok_or_else(|| serde::de::Error::custom("expected 64 hex digits"))
    }
}

/// The private half of a [`WrapKey`]: an X25519 secret key (RFC 7748), kept with the device's
/// other secrets.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct WrapSecret([u8; 32]);

impl WrapSecret {
    pub fn from_bytes(bytes: &[u8; 32]) -> WrapSecret {
        WrapSecret(*bytes)
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The public key: X25519 of these bytes, clamped, and the base point.
    pub fn wrap_key(&self) -> WrapKey {
        WrapKey(MontgomeryPoint::mul_base_clamped(self.0).to_bytes())
    }

    /// A new wrap secret from the operating system's randomness.
    #[cfg(feature = "rand")]
    pub fn generate() -> Result<WrapSecret, getrandom::Error> {
        let mut bytes = [0u8; 32];
        getrandom::fill(&mut bytes)?;
        let secret = WrapSecret(bytes);
        bytes.zeroize();
        Ok(secret)
    }
}

impl fmt::Debug for WrapSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WrapSecret({:?})", self.wrap_key())
    }
}

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

    /// The key that names a file to the server: HKDF-SHA256 with the library id as salt, the
    /// secret as input keying material and info `fongo-sync/v1/file-name-key`, as the batch key is
    /// derived.
    pub fn file_name_key(&self) -> FileNameKey {
        FileNameKey::from_bytes(self.expand(FILE_NAME_KEY_INFO))
    }

    /// The root every file key is expanded from: HKDF-SHA256 with the library id as salt, the
    /// secret as input keying material and info `fongo-sync/v1/file-key-root`.
    pub fn file_key_root(&self) -> FileKeyRoot {
        FileKeyRoot::from_bytes(self.expand(FILE_KEY_ROOT_INFO))
    }

    /// The key of [`LibraryKeys::member_proof`].
    pub(crate) fn member_key(&self) -> Zeroizing<[u8; 32]> {
        Zeroizing::new(self.expand(crate::member::MEMBER_KEY_INFO))
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

macro_rules! secret_key {
    ($(#[$doc:meta])* $vis:vis $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Zeroize, ZeroizeOnDrop)]
        $vis struct $name([u8; 32]);

        impl $name {
            pub(crate) fn from_bytes(bytes: [u8; 32]) -> $name {
                $name(bytes)
            }

            $vis fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(concat!(stringify!($name), "(..)"))
            }
        }
    };
}

secret_key!(
    /// [`LibraryKeys::file_name_key`]: turns a content hash into the name the server knows a file
    /// by.
    pub FileNameKey
);
secret_key!(
    /// [`LibraryKeys::file_key_root`]: what each file's key is expanded from.
    pub FileKeyRoot
);
secret_key!(
    /// One file's key: the pseudorandom key its chunk key and nonce key are expanded from.
    pub(crate) FileKey
);

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

/// A random nonce for `sign_request`.
#[cfg(feature = "rand")]
pub fn random_request_nonce() -> Result<[u8; 16], getrandom::Error> {
    let mut nonce = [0u8; 16];
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

    #[test]
    fn file_keys_are_apart_from_each_other_and_the_batch_keys() {
        let k = keys(1, 2);
        let derived = [
            *k.file_name_key().as_bytes(),
            *k.file_key_root().as_bytes(),
            k.batch_key(0),
            *keys(3, 2).file_name_key().as_bytes(),
            *keys(1, 3).file_key_root().as_bytes(),
        ];
        for (i, a) in derived.iter().enumerate() {
            for b in &derived[i + 1..] {
                assert_ne!(a, b);
            }
        }
        let file_key = k.file_key_root().file_key(&crate::ContentHash([9; 32]));
        let secrets = [
            *k.file_name_key().as_bytes(),
            *k.file_key_root().as_bytes(),
            *file_key.as_bytes(),
        ];
        for text in [
            format!("{:?}", k.file_name_key()),
            format!("{:?}", k.file_key_root()),
            format!("{file_key:?}"),
        ] {
            for secret in &secrets {
                assert!(!text.contains(&crate::hex::encode(&secret[..4])));
            }
        }
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
        assert_ne!(
            random_request_nonce().unwrap(),
            random_request_nonce().unwrap()
        );
    }
}

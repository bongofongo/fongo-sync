use std::fmt;

use fongo_sync_protocol::api::{DeviceRecord, SignedWrapKey};
use fongo_sync_protocol::{
    DeviceId, LibraryId, LibraryKeys, MemberCheck, SigningKey, WrapKey, WrapSecret,
    generate_signing_key,
};
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

use crate::error::Error;
use crate::library::DeviceOffer;

/// The format [`Credentials::to_bytes`] writes for credentials with a wrap secret.
/// [`Credentials::from_bytes`] reads it and [`CREDENTIALS_FORMAT_WITHOUT_WRAP`]. A build from
/// before wrap secrets cannot read it.
pub const CREDENTIALS_FORMAT: u8 = 2;

/// The format of credentials without a wrap secret, the only one builds before wrap secrets
/// wrote and read. [`Credentials::to_bytes`] still writes it for such credentials.
pub const CREDENTIALS_FORMAT_WITHOUT_WRAP: u8 = 1;

const LEN_WITHOUT_WRAP: usize = 1 + 16 + 32 + 32 + 4;
const LEN: usize = LEN_WITHOUT_WRAP + 32;

/// What a device needs to take part in a library: the library's keys, this device's signing key,
/// the key epoch it seals under, and the secret of the wrap key it registers, if it has one.
/// Whoever holds these can read and write the library.
///
/// A device without a wrap secret, such as one whose credentials an earlier build wrote, syncs as
/// any other, but registered no wrap key and no membership proof, and has no way to register them
/// later under its key.
///
/// `Debug` shows only the public parts.
#[derive(Clone)]
pub struct Credentials {
    library: LibraryKeys,
    device: SigningKey,
    epoch: u32,
    wrap: Option<WrapSecret>,
}

/// Why stored credentials could not be read.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum CredentialsError {
    #[error("stored credentials of format {format} are {len} bytes, not {expected}")]
    Length {
        format: u8,
        len: usize,
        expected: usize,
    },
    #[error("stored credentials are empty")]
    Empty,
    #[error("credentials format {0} is not supported")]
    UnsupportedFormat(u8),
}

impl Credentials {
    /// Credentials without a wrap secret; [`Credentials::with_wrap_secret`] adds one.
    pub fn new(library: LibraryKeys, device: SigningKey, epoch: u32) -> Credentials {
        Credentials {
            library,
            device,
            epoch,
            wrap: None,
        }
    }

    /// Credentials for a new library: a random library id and secret, device key and wrap
    /// secret, epoch 0. Store them before [`crate::create_library_with`] registers them. Holding a
    /// wrap secret, they are stored in a format a build from before wrap secrets cannot read.
    pub fn for_new_library() -> Result<Credentials, Error> {
        let library = LibraryKeys::generate().map_err(|_| Error::Randomness)?;
        Credentials::for_new_device(library)
    }

    /// Credentials for a new device of the library whose recovery code this is: its keys, a
    /// random device key and wrap secret, epoch 0. Store them before [`crate::join_library_with`]
    /// registers them. Holding a wrap secret, they are stored in a format a build from before wrap
    /// secrets cannot read.
    pub fn for_recovery_code(recovery_code: &str) -> Result<Credentials, Error> {
        Credentials::for_new_device(LibraryKeys::from_recovery_code(recovery_code)?)
    }

    /// Credentials for a new device of a library whose keys the application handed over: a random
    /// device key and wrap secret, epoch 0. Store them, then give a device already in the library
    /// [`Credentials::device_offer`] to add with [`crate::add_device`]. Holding a wrap secret,
    /// they are stored in a format a build from before wrap secrets cannot read.
    pub fn for_new_device(library: LibraryKeys) -> Result<Credentials, Error> {
        let device = generate_signing_key().map_err(|_| Error::Randomness)?;
        let wrap = WrapSecret::generate().map_err(|_| Error::Randomness)?;
        Ok(Credentials::new(library, device, 0).with_wrap_secret(wrap))
    }

    /// The same credentials with a wrap secret.
    pub fn with_wrap_secret(mut self, wrap: WrapSecret) -> Credentials {
        self.wrap = Some(wrap);
        self
    }

    /// The same credentials sealing under another key epoch.
    pub fn with_epoch(mut self, epoch: u32) -> Credentials {
        self.epoch = epoch;
        self
    }

    pub fn library(&self) -> &LibraryKeys {
        &self.library
    }

    pub fn library_id(&self) -> LibraryId {
        self.library.id
    }

    pub fn device(&self) -> &SigningKey {
        &self.device
    }

    pub fn device_id(&self) -> DeviceId {
        DeviceId::of(&self.device.verifying_key())
    }

    pub fn epoch(&self) -> u32 {
        self.epoch
    }

    /// The secret of the wrap key this device registers, if the credentials hold one.
    pub fn wrap_secret(&self) -> Option<&WrapSecret> {
        self.wrap.as_ref()
    }

    /// What another device of the library needs to add this one with [`crate::add_device`]: this
    /// device's public key and its wrap key, signed by its key for the library. `None` without a
    /// wrap secret.
    pub fn device_offer(&self) -> Option<DeviceOffer> {
        let wrap = self.wrap_key()?;
        Some(DeviceOffer {
            device_key: self.device.verifying_key(),
            wrap: SignedWrapKey::new(self.library.id, &self.device, wrap),
        })
    }

    /// The public half of the wrap secret, which creating or joining registers.
    pub fn wrap_key(&self) -> Option<WrapKey> {
        self.wrap.as_ref().map(WrapSecret::wrap_key)
    }

    /// Checks another device's record of this library against the secret these credentials hold:
    /// whether its membership proof shows that whoever registered it held the secret. See
    /// [`DeviceRecord::check_member_proof`].
    pub fn check_member(&self, record: &DeviceRecord) -> MemberCheck {
        record.check_member_proof(&self.library)
    }

    /// The library's recovery code, to show the user once. Whoever holds it holds the library.
    pub fn recovery_code(&self) -> String {
        self.library.recovery_code()
    }

    /// With a wrap secret, 117 bytes of format [`CREDENTIALS_FORMAT`]: the format, the library
    /// id, the library secret, the device key's seed, the epoch (big-endian) and the wrap secret.
    /// Without one, the first 85 of those bytes, of format [`CREDENTIALS_FORMAT_WITHOUT_WRAP`], as
    /// builds before wrap secrets wrote them. A build from before wrap secrets cannot read the
    /// longer form. Secret; store it as such.
    pub fn to_bytes(&self) -> Zeroizing<Vec<u8>> {
        let mut out = Zeroizing::new(Vec::with_capacity(LEN));
        out.push(match self.wrap {
            Some(_) => CREDENTIALS_FORMAT,
            None => CREDENTIALS_FORMAT_WITHOUT_WRAP,
        });
        out.extend_from_slice(&self.library.id.0);
        out.extend_from_slice(self.library.secret());
        let mut seed = self.device.to_bytes();
        out.extend_from_slice(&seed);
        seed.zeroize();
        out.extend_from_slice(&self.epoch.to_be_bytes());
        if let Some(wrap) = &self.wrap {
            out.extend_from_slice(wrap.as_bytes());
        }
        out
    }

    /// Reads what [`Credentials::to_bytes`] wrote, of either format.
    pub fn from_bytes(bytes: &[u8]) -> Result<Credentials, CredentialsError> {
        let format = *bytes.first().ok_or(CredentialsError::Empty)?;
        let expected = match format {
            CREDENTIALS_FORMAT_WITHOUT_WRAP => LEN_WITHOUT_WRAP,
            CREDENTIALS_FORMAT => LEN,
            _ => return Err(CredentialsError::UnsupportedFormat(format)),
        };
        if bytes.len() != expected {
            return Err(CredentialsError::Length {
                format,
                len: bytes.len(),
                expected,
            });
        }
        let mut id = [0u8; 16];
        id.copy_from_slice(&bytes[1..17]);
        let mut secret = Zeroizing::new([0u8; 32]);
        secret.copy_from_slice(&bytes[17..49]);
        let mut seed = Zeroizing::new([0u8; 32]);
        seed.copy_from_slice(&bytes[49..81]);
        let mut epoch = [0u8; 4];
        epoch.copy_from_slice(&bytes[81..85]);
        let wrap = (format == CREDENTIALS_FORMAT).then(|| {
            let mut wrap = Zeroizing::new([0u8; 32]);
            wrap.copy_from_slice(&bytes[85..]);
            WrapSecret::from_bytes(&wrap)
        });
        Ok(Credentials {
            library: LibraryKeys::new(LibraryId(id), *secret),
            device: SigningKey::from_bytes(&seed),
            epoch: u32::from_be_bytes(epoch),
            wrap,
        })
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("library", &self.library.id)
            .field("device", &self.device_id())
            .field("epoch", &self.epoch)
            .field("wrap_key", &self.wrap_key())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credentials() -> Credentials {
        Credentials::new(
            LibraryKeys::new(LibraryId([1; 16]), [0x5a; 32]),
            SigningKey::from_bytes(&[0x6b; 32]),
            7,
        )
    }

    fn wrapped() -> Credentials {
        credentials().with_wrap_secret(WrapSecret::from_bytes(&[0x7c; 32]))
    }

    #[test]
    fn credentials_round_trip_through_bytes() {
        let bytes = wrapped().to_bytes();
        assert_eq!(bytes.len(), 117);
        assert_eq!(bytes[0], 2);
        assert_eq!(bytes[85..], [0x7c; 32]);
        let back = Credentials::from_bytes(&bytes).unwrap();
        assert_eq!(back.library_id(), LibraryId([1; 16]));
        assert_eq!(back.library().secret(), &[0x5a; 32]);
        assert_eq!(back.device().to_bytes(), [0x6b; 32]);
        assert_eq!(back.epoch(), 7);
        assert_eq!(back.wrap_secret().unwrap().as_bytes(), &[0x7c; 32]);
        assert_eq!(back.to_bytes()[..], bytes[..]);
    }

    #[test]
    fn credentials_an_earlier_build_wrote_still_load_and_are_written_alike() {
        let mut earlier = vec![1];
        earlier.extend_from_slice(&[1; 16]);
        earlier.extend_from_slice(&[0x5a; 32]);
        earlier.extend_from_slice(&[0x6b; 32]);
        earlier.extend_from_slice(&7u32.to_be_bytes());
        let back = Credentials::from_bytes(&earlier).unwrap();
        assert_eq!(back.library().secret(), &[0x5a; 32]);
        assert_eq!(back.device().to_bytes(), [0x6b; 32]);
        assert_eq!(back.epoch(), 7);
        assert!(back.wrap_secret().is_none() && back.wrap_key().is_none());
        assert_eq!(back.to_bytes()[..], earlier[..]);
        assert_eq!(credentials().to_bytes()[..], earlier[..]);
    }

    #[test]
    fn other_lengths_and_formats_are_refused() {
        let bytes = wrapped().to_bytes();
        assert_eq!(
            Credentials::from_bytes(&bytes[..85]).err(),
            Some(CredentialsError::Length {
                format: 2,
                len: 85,
                expected: 117
            })
        );
        let old = credentials().to_bytes();
        let mut longer = old.to_vec();
        longer.extend_from_slice(&[0x7c; 32]);
        assert_eq!(
            Credentials::from_bytes(&longer).err(),
            Some(CredentialsError::Length {
                format: 1,
                len: 117,
                expected: 85
            })
        );
        let mut later = bytes.to_vec();
        later[0] = 3;
        assert_eq!(
            Credentials::from_bytes(&later).err(),
            Some(CredentialsError::UnsupportedFormat(3))
        );
        assert_eq!(
            Credentials::from_bytes(&[]).err(),
            Some(CredentialsError::Empty)
        );
    }

    #[test]
    fn new_credentials_are_fresh_and_hold_a_wrap_secret() {
        let a = Credentials::for_new_library().unwrap();
        let b = Credentials::for_new_library().unwrap();
        assert_ne!(a.library_id(), b.library_id());
        assert_ne!(a.device_id(), b.device_id());
        assert_ne!(a.wrap_key(), b.wrap_key());
        assert_eq!(a.epoch(), 0);
        let joining = Credentials::for_recovery_code(&a.recovery_code()).unwrap();
        assert_eq!(joining.library_id(), a.library_id());
        assert_eq!(joining.library().secret(), a.library().secret());
        assert_ne!(joining.device_id(), a.device_id());
        assert!(joining.wrap_key().is_some() && joining.wrap_key() != a.wrap_key());
    }

    #[test]
    fn debug_shows_no_secret() {
        let text = format!("{:?}", wrapped());
        assert!(text.contains("LibraryId(0101"), "{text}");
        assert!(text.contains("epoch: 7"), "{text}");
        assert!(text.contains("WrapKey("), "{text}");
        for secret in ["5a5a", "6b6b", "7c7c", "90, 90", "107, 107", "124, 124"] {
            assert!(!text.contains(secret), "{text}");
        }
    }
}

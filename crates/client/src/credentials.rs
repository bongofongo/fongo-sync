use std::fmt;

use fongo_sync_protocol::{DeviceId, LibraryId, LibraryKeys, SigningKey};
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

/// The format [`Credentials::to_bytes`] writes, and the only one [`Credentials::from_bytes`] reads.
pub const CREDENTIALS_FORMAT: u8 = 1;

const LEN: usize = 1 + 16 + 32 + 32 + 4;

/// What a device needs to take part in a library: the library's keys, this device's signing key
/// and the key epoch it seals under. Whoever holds these can read and write the library.
///
/// `Debug` shows only the public parts.
#[derive(Clone)]
pub struct Credentials {
    library: LibraryKeys,
    device: SigningKey,
    epoch: u32,
}

/// Why stored credentials could not be read.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CredentialsError {
    #[error("stored credentials are {0} bytes, not {LEN}")]
    Length(usize),
    #[error("credentials format {0} is not supported")]
    UnsupportedFormat(u8),
}

impl Credentials {
    pub fn new(library: LibraryKeys, device: SigningKey, epoch: u32) -> Credentials {
        Credentials {
            library,
            device,
            epoch,
        }
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

    /// The library's recovery code, to show the user once. Whoever holds it holds the library.
    pub fn recovery_code(&self) -> String {
        self.library.recovery_code()
    }

    /// 85 bytes: the format, the library id, the library secret, the device key's seed and the
    /// epoch (big-endian). Secret; store it as such.
    pub fn to_bytes(&self) -> Zeroizing<[u8; LEN]> {
        let mut out = Zeroizing::new([0u8; LEN]);
        out[0] = CREDENTIALS_FORMAT;
        out[1..17].copy_from_slice(&self.library.id.0);
        out[17..49].copy_from_slice(self.library.secret());
        let mut seed = self.device.to_bytes();
        out[49..81].copy_from_slice(&seed);
        seed.zeroize();
        out[81..].copy_from_slice(&self.epoch.to_be_bytes());
        out
    }

    /// Reads what [`Credentials::to_bytes`] wrote.
    pub fn from_bytes(bytes: &[u8]) -> Result<Credentials, CredentialsError> {
        if bytes.len() != LEN {
            return Err(CredentialsError::Length(bytes.len()));
        }
        if bytes[0] != CREDENTIALS_FORMAT {
            return Err(CredentialsError::UnsupportedFormat(bytes[0]));
        }
        let mut id = [0u8; 16];
        id.copy_from_slice(&bytes[1..17]);
        let mut secret = Zeroizing::new([0u8; 32]);
        secret.copy_from_slice(&bytes[17..49]);
        let mut seed = Zeroizing::new([0u8; 32]);
        seed.copy_from_slice(&bytes[49..81]);
        let mut epoch = [0u8; 4];
        epoch.copy_from_slice(&bytes[81..]);
        Ok(Credentials {
            library: LibraryKeys::new(LibraryId(id), *secret),
            device: SigningKey::from_bytes(&seed),
            epoch: u32::from_be_bytes(epoch),
        })
    }
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("library", &self.library.id)
            .field("device", &self.device_id())
            .field("epoch", &self.epoch)
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

    #[test]
    fn credentials_round_trip_through_bytes() {
        let bytes = credentials().to_bytes();
        assert_eq!(bytes.len(), 85);
        let back = Credentials::from_bytes(&bytes[..]).unwrap();
        assert_eq!(back.library_id(), LibraryId([1; 16]));
        assert_eq!(back.library().secret(), &[0x5a; 32]);
        assert_eq!(back.device().to_bytes(), [0x6b; 32]);
        assert_eq!(back.epoch(), 7);
        assert_eq!(back.to_bytes()[..], bytes[..]);
    }

    #[test]
    fn other_lengths_and_formats_are_refused() {
        let bytes = credentials().to_bytes();
        assert_eq!(
            Credentials::from_bytes(&bytes[1..]).err(),
            Some(CredentialsError::Length(84))
        );
        let mut later = *bytes;
        later[0] = 2;
        assert_eq!(
            Credentials::from_bytes(&later).err(),
            Some(CredentialsError::UnsupportedFormat(2))
        );
    }

    #[test]
    fn debug_shows_no_secret() {
        let text = format!("{:?}", credentials());
        assert!(text.contains("LibraryId(0101"), "{text}");
        assert!(text.contains("epoch: 7"), "{text}");
        assert!(!text.contains("5a5a") && !text.contains("6b6b"), "{text}");
        assert!(
            !text.contains("90, 90") && !text.contains("107, 107"),
            "{text}"
        );
    }
}

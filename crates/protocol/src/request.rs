use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::hex;
use crate::keys::DeviceId;

/// Header carrying the signer's device id as 32 lowercase hex digits.
pub const HEADER_DEVICE: &str = "x-fongo-device";
/// Header carrying the signing time as decimal unix seconds.
pub const HEADER_TIMESTAMP: &str = "x-fongo-timestamp";
/// Header carrying the request nonce as 32 lowercase hex digits.
pub const HEADER_NONCE: &str = "x-fongo-nonce";
/// Header carrying the Ed25519 signature as 128 lowercase hex digits.
pub const HEADER_SIGNATURE: &str = "x-fongo-signature";

const LABEL: &[u8] = b"fongo-sync/v1/request\0";

/// A request's proof of origin, sent as the four headers above.
///
/// `nonce` is 16 random bytes chosen afresh for every request. A server refuses a second request
/// with the same device id and nonce while the first is inside the skew window; that is its whole
/// defence against replay, so identical requests sent in the same second stay distinct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestAuth {
    pub device: DeviceId,
    pub timestamp: u64,
    pub nonce: [u8; 16],
    pub signature: Signature,
}

impl RequestAuth {
    /// The header values to send, in the order device, timestamp, nonce, signature.
    pub fn headers(&self) -> [(&'static str, String); 4] {
        [
            (HEADER_DEVICE, self.device.to_hex()),
            (HEADER_TIMESTAMP, self.timestamp.to_string()),
            (HEADER_NONCE, hex::encode(&self.nonce)),
            (HEADER_SIGNATURE, hex::encode(&self.signature.to_bytes())),
        ]
    }

    /// Parses the four header values as received.
    pub fn from_headers(
        device: &str,
        timestamp: &str,
        nonce: &str,
        signature: &str,
    ) -> Result<RequestAuth, RequestError> {
        let device = DeviceId::from_hex(device).ok_or(RequestError::Malformed(HEADER_DEVICE))?;
        let timestamp = timestamp
            .bytes()
            .all(|b| b.is_ascii_digit())
            .then(|| timestamp.parse().ok())
            .flatten()
            .ok_or(RequestError::Malformed(HEADER_TIMESTAMP))?;
        let nonce = hex::decode_array::<16>(nonce).ok_or(RequestError::Malformed(HEADER_NONCE))?;
        let signature = hex::decode_array::<64>(signature)
            .map(|bytes| Signature::from_bytes(&bytes))
            .ok_or(RequestError::Malformed(HEADER_SIGNATURE))?;
        Ok(RequestAuth {
            device,
            timestamp,
            nonce,
            signature,
        })
    }
}

/// Why a request's authentication was refused.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RequestError {
    #[error("header {0} is missing or malformed")]
    Malformed(&'static str),
    #[error("the key given is not the key of the device the request names")]
    WrongDevice,
    #[error("timestamp is outside the allowed clock skew")]
    Stale,
    #[error("signature does not verify")]
    BadSignature,
}

/// Signs a request. `method` is as sent (e.g. `POST`); `path` is the request target exactly as
/// sent, including any `?query`; `nonce` must be fresh random bytes for each request.
pub fn sign_request(
    key: &SigningKey,
    method: &str,
    path: &str,
    body: &[u8],
    timestamp: u64,
    nonce: [u8; 16],
) -> RequestAuth {
    let device = DeviceId::of(&key.verifying_key());
    RequestAuth {
        device,
        timestamp,
        nonce,
        signature: key.sign(&message(device, method, path, body, timestamp, &nonce)),
    }
}

/// Accepts a request signed by `key` within `skew` seconds of `now`, either side. It does not
/// detect replay: the server must also refuse a device id and nonce it has seen inside the window.
pub fn verify_request(
    auth: &RequestAuth,
    key: &VerifyingKey,
    method: &str,
    path: &str,
    body: &[u8],
    now: u64,
    skew: u64,
) -> Result<(), RequestError> {
    if DeviceId::of(key) != auth.device {
        return Err(RequestError::WrongDevice);
    }
    if auth.timestamp.abs_diff(now) > skew {
        return Err(RequestError::Stale);
    }
    key.verify_strict(
        &message(auth.device, method, path, body, auth.timestamp, &auth.nonce),
        &auth.signature,
    )
    .map_err(|_| RequestError::BadSignature)
}

fn message(
    device: DeviceId,
    method: &str,
    path: &str,
    body: &[u8],
    timestamp: u64,
    nonce: &[u8; 16],
) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(LABEL.len() + 16 + 8 + method.len() + 8 + path.len() + 32 + 8 + 16);
    out.extend_from_slice(LABEL);
    out.extend_from_slice(&device.0);
    for part in [method.as_bytes(), path.as_bytes()] {
        out.extend_from_slice(&(part.len() as u64).to_be_bytes());
        out.extend_from_slice(part);
    }
    out.extend_from_slice(&Sha256::digest(body));
    out.extend_from_slice(&timestamp.to_be_bytes());
    out.extend_from_slice(nonce);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> SigningKey {
        SigningKey::from_bytes(&[1; 32])
    }

    const PATH: &str = "/v1/libraries/00/batches?after=3";

    fn signed() -> RequestAuth {
        sign_request(&key(), "GET", PATH, b"", 1000, [9; 16])
    }

    fn check(
        auth: &RequestAuth,
        method: &str,
        path: &str,
        body: &[u8],
        now: u64,
    ) -> Result<(), RequestError> {
        verify_request(auth, &key().verifying_key(), method, path, body, now, 300)
    }

    #[test]
    fn a_signed_request_verifies_within_the_skew() {
        assert_eq!(check(&signed(), "GET", PATH, b"", 1000), Ok(()));
        assert_eq!(check(&signed(), "GET", PATH, b"", 1300), Ok(()));
        assert_eq!(check(&signed(), "GET", PATH, b"", 700), Ok(()));
        assert_eq!(
            check(&signed(), "GET", PATH, b"", 1301),
            Err(RequestError::Stale)
        );
        assert_eq!(
            check(&signed(), "GET", PATH, b"", 699),
            Err(RequestError::Stale)
        );
    }

    #[test]
    fn changing_any_signed_part_is_refused() {
        let bad = Err(RequestError::BadSignature);
        assert_eq!(check(&signed(), "POST", PATH, b"", 1000), bad);
        assert_eq!(
            check(
                &signed(),
                "GET",
                "/v1/libraries/00/batches?after=4",
                b"",
                1000
            ),
            bad
        );
        assert_eq!(check(&signed(), "GET", PATH, b"x", 1000), bad);
        let mut later = signed();
        later.timestamp += 1;
        assert_eq!(check(&later, "GET", PATH, b"", 1000), bad);
        let mut renonced = signed();
        renonced.nonce[0] ^= 1;
        assert_eq!(check(&renonced, "GET", PATH, b"", 1000), bad);
        let shifted = sign_request(&key(), "GE", &format!("T{PATH}"), b"", 1000, [9; 16]);
        assert_eq!(check(&shifted, "GET", PATH, b"", 1000), bad);
    }

    #[test]
    fn identical_requests_with_different_nonces_are_distinct() {
        let other = sign_request(&key(), "GET", PATH, b"", 1000, [8; 16]);
        assert_eq!(check(&other, "GET", PATH, b"", 1000), Ok(()));
        assert_ne!(other.signature, signed().signature);
    }

    #[test]
    fn another_devices_key_is_refused() {
        let other = SigningKey::from_bytes(&[2; 32]).verifying_key();
        assert_eq!(
            verify_request(&signed(), &other, "GET", PATH, b"", 1000, 300),
            Err(RequestError::WrongDevice)
        );
    }

    #[test]
    fn headers_round_trip_and_malformed_ones_are_named() {
        let auth = signed();
        let [(_, device), (_, timestamp), (_, nonce), (_, signature)] = auth.headers();
        assert_eq!(
            RequestAuth::from_headers(&device, &timestamp, &nonce, &signature),
            Ok(auth)
        );
        assert_eq!(
            RequestAuth::from_headers(&device, "+5", &nonce, &signature),
            Err(RequestError::Malformed(HEADER_TIMESTAMP))
        );
        assert_eq!(
            RequestAuth::from_headers("zz", &timestamp, &nonce, &signature),
            Err(RequestError::Malformed(HEADER_DEVICE))
        );
        assert_eq!(
            RequestAuth::from_headers(&device, &timestamp, "00", &signature),
            Err(RequestError::Malformed(HEADER_NONCE))
        );
        assert_eq!(
            RequestAuth::from_headers(&device, &timestamp, &nonce, ""),
            Err(RequestError::Malformed(HEADER_SIGNATURE))
        );
    }
}

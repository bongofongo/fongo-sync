const DIGITS: &[u8; 16] = b"0123456789abcdef";

pub(crate) fn encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[usize::from(b >> 4)] as char);
        out.push(DIGITS[usize::from(b & 0xf)] as char);
    }
    out
}

pub(crate) fn decode_array<const N: usize>(text: &str) -> Option<[u8; N]> {
    let text = text.as_bytes();
    if text.len() != N * 2 {
        return None;
    }
    let mut out = [0u8; N];
    for (byte, pair) in out.iter_mut().zip(text.chunks_exact(2)) {
        *byte = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(out)
}

fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Serde for byte-array keys and signatures as lowercase hex strings.
pub(crate) mod serde_key {
    use ed25519_dalek::VerifyingKey;
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub(crate) fn serialize<S: Serializer>(key: &VerifyingKey, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&super::encode(key.as_bytes()))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<VerifyingKey, D::Error> {
        let text = <std::borrow::Cow<'de, str>>::deserialize(d)?;
        let bytes = super::decode_array::<32>(&text)
            .ok_or_else(|| D::Error::custom("expected 64 hex digits"))?;
        VerifyingKey::from_bytes(&bytes).map_err(|_| D::Error::custom("not an Ed25519 public key"))
    }
}

pub(crate) mod serde_sig {
    use ed25519_dalek::Signature;
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub(crate) fn serialize<S: Serializer>(sig: &Signature, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&super::encode(&sig.to_bytes()))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Signature, D::Error> {
        let text = <std::borrow::Cow<'de, str>>::deserialize(d)?;
        super::decode_array::<64>(&text)
            .map(|bytes| Signature::from_bytes(&bytes))
            .ok_or_else(|| D::Error::custom("expected 128 hex digits"))
    }
}

/// Serde for a 32-byte digest as exactly 64 lowercase hex digits.
pub(crate) mod serde_digest {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub(crate) fn serialize<S: Serializer>(digest: &[u8; 32], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&super::encode(digest))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<[u8; 32], D::Error> {
        let text = <std::borrow::Cow<'de, str>>::deserialize(d)?;
        super::decode_lower(&text)
            .ok_or_else(|| D::Error::custom("expected 64 lowercase hex digits"))
    }
}

/// Like [`decode_array`], but only lowercase digits.
pub(crate) fn decode_lower<const N: usize>(text: &str) -> Option<[u8; N]> {
    if !text.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    decode_array(text)
}

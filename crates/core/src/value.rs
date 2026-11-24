use std::cmp::Ordering;
use std::fmt;

use sha2::{Digest, Sha256};

/// The identity of an entity across every replica.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Gid(pub [u8; 16]);

impl Gid {
    /// The id of an entity whose identity is its key: every replica derives the same one.
    pub fn derive(collection: &str, key: &[Value]) -> Gid {
        let mut hasher = Sha256::new();
        hasher.update((collection.len() as u64).to_be_bytes());
        hasher.update(collection.as_bytes());
        let mut buf = Vec::new();
        for value in key {
            buf.clear();
            value.encode(&mut buf);
            hasher.update(&buf);
        }
        let digest = hasher.finalize();
        let mut id = [0u8; 16];
        id.copy_from_slice(&digest[..16]);
        Gid(id)
    }

    pub fn from_slice(bytes: &[u8]) -> Option<Gid> {
        <[u8; 16]>::try_from(bytes).ok().map(Gid)
    }

    pub fn to_hex(&self) -> String {
        let mut hex = String::with_capacity(32);
        self.push_hex(&mut hex);
        hex
    }

    /// Appends [`Gid::to_hex`] to `out`.
    pub(crate) fn push_hex(&self, out: &mut String) {
        for &byte in &self.0 {
            push_hex_digits(out, u64::from(byte), 2);
        }
    }

    pub fn from_hex(hex: &str) -> Option<Gid> {
        if hex.len() != 32 || !hex.is_ascii() {
            return None;
        }
        let mut id = [0u8; 16];
        for (i, byte) in id.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
        }
        Some(Gid(id))
    }
}

/// Appends the low `digits` hex digits of `value`, most significant first, in lower case.
pub(crate) fn push_hex_digits(out: &mut String, value: u64, digits: u32) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for i in (0..digits).rev() {
        out.push(HEX[((value >> (4 * i)) & 0xf) as usize] as char);
    }
}

impl fmt::Debug for Gid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Gid({})", self.to_hex())
    }
}

/// A field's value. `Ref` names another entity, so a replica can translate it to its own local id.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Int(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Ref(Gid),
}

impl Value {
    fn rank(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Int(_) | Value::Real(_) => 1,
            Value::Text(_) => 2,
            Value::Blob(_) => 3,
            Value::Ref(_) => 4,
        }
    }

    /// A total order every replica agrees on, so `Strategy::Max` converges.
    pub fn total_cmp(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Real(a), Value::Real(b)) => a.total_cmp(b),
            (Value::Int(a), Value::Real(b)) => (*a as f64).total_cmp(b).then(Ordering::Less),
            (Value::Real(a), Value::Int(b)) => a.total_cmp(&(*b as f64)).then(Ordering::Greater),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
            (Value::Ref(a), Value::Ref(b)) => a.cmp(b),
            _ => self.rank().cmp(&other.rank()),
        }
    }

    fn encode(&self, out: &mut Vec<u8>) {
        let (tag, bytes): (u8, Vec<u8>) = match self {
            Value::Null => (0, Vec::new()),
            Value::Int(v) => (1, v.to_be_bytes().to_vec()),
            Value::Real(v) => (2, v.to_bits().to_be_bytes().to_vec()),
            Value::Text(v) => (3, v.as_bytes().to_vec()),
            Value::Blob(v) => (4, v.clone()),
            Value::Ref(v) => (5, v.0.to_vec()),
        };
        out.push(tag);
        out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        out.extend_from_slice(&bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_derived_id_depends_on_the_collection_and_every_key_part() {
        let a = Gid::derive("tags", &[Value::Text("ab".into()), Value::Text("c".into())]);
        let b = Gid::derive("tags", &[Value::Text("a".into()), Value::Text("bc".into())]);
        let c = Gid::derive("tag", &[Value::Text("ab".into()), Value::Text("c".into())]);
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_eq!(
            a,
            Gid::derive("tags", &[Value::Text("ab".into()), Value::Text("c".into())])
        );
    }

    #[test]
    fn hex_round_trips() {
        let id = Gid::derive("x", &[Value::Int(7)]);
        assert_eq!(Gid::from_hex(&id.to_hex()), Some(id));
        assert_eq!(Gid::from_hex("zz"), None);
    }

    #[test]
    fn an_integer_and_the_equal_real_are_ordered_not_equal() {
        assert_eq!(Value::Int(1).total_cmp(&Value::Real(1.0)), Ordering::Less);
        assert_eq!(
            Value::Real(1.0).total_cmp(&Value::Int(1)),
            Ordering::Greater
        );
        assert_eq!(Value::Null.total_cmp(&Value::Int(-5)), Ordering::Less);
    }
}

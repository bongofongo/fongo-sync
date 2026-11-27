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

    /// The one total order on values, which `Strategy::Max` merges by and `Strategy::Lww` breaks
    /// a tie of clocks by: `Null`, then numbers, then `Text` by bytes, `Blob` by bytes, `Ref` by
    /// bytes. Numbers compare exactly as numbers, whatever their sizes: an `Int` sits just below
    /// a `Real` of equal value, `-0.0` below `Int(0)` below `0.0`, and a NaN by its sign, a
    /// negative one below every number and a positive one above, NaNs among themselves as
    /// `f64::total_cmp` orders them. Two values are `Equal` only when identical, reals by bits,
    /// so `Int(1)` and `Real(1.0)` are different values.
    pub fn total_cmp(&self, other: &Value) -> Ordering {
        match (self, other) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Real(a), Value::Real(b)) => a.total_cmp(b),
            (Value::Int(a), Value::Real(b)) => int_real(*a, *b),
            (Value::Real(a), Value::Int(b)) => int_real(*b, *a).reverse(),
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

/// An integer against a real, exactly, as [`Value::total_cmp`] orders them.
fn int_real(a: i64, r: f64) -> Ordering {
    const TWO_63: f64 = 9_223_372_036_854_775_808.0;
    if r.is_nan() {
        return if r.is_sign_negative() {
            Ordering::Greater
        } else {
            Ordering::Less
        };
    }
    if r >= TWO_63 {
        return Ordering::Less;
    }
    if r < -TWO_63 {
        return Ordering::Greater;
    }
    // Within [-2^63, 2^63) the integer part of `r` is an `i64`, so the cast is exact.
    let t = r.trunc();
    match a.cmp(&(t as i64)) {
        Ordering::Equal if r > t => Ordering::Less,
        Ordering::Equal if r < t => Ordering::Greater,
        Ordering::Equal if r == 0.0 && r.is_sign_negative() => Ordering::Greater,
        Ordering::Equal => Ordering::Less,
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const TWO_53: i64 = 1 << 53;

    /// `r` as `sign * m * 2^e`, `m` an integer: exactly the number it is.
    fn parts(r: f64) -> (bool, i128, i32) {
        let bits = r.to_bits();
        let negative = bits >> 63 == 1;
        let exp = ((bits >> 52) & 0x7ff) as i32;
        let frac = (bits & ((1 << 52) - 1)) as i128;
        if exp == 0 {
            (negative, frac, -1074)
        } else {
            (negative, frac | (1 << 52), exp - 1075)
        }
    }

    /// `a` against a finite `r` as numbers, by integer arithmetic alone.
    fn numerically(a: i64, r: f64) -> Ordering {
        let (negative, m, e) = parts(r);
        let signed = |x: i128| if negative { -x } else { x };
        let a = i128::from(a);
        if m == 0 {
            return a.cmp(&0);
        }
        if e >= 0 {
            if e > 64 {
                return if negative {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            return a.cmp(&signed(m << e));
        }
        let s = -e;
        if s > 60 {
            // |r| < 2^53 / 2^61 < 1, and r is not 0.
            return match a.cmp(&0) {
                Ordering::Equal if negative => Ordering::Greater,
                Ordering::Equal => Ordering::Less,
                other => other,
            };
        }
        (a << s).cmp(&signed(m))
    }

    /// The order `Value::total_cmp` gives an `Int` and a `Real`, from the numbers they are.
    fn oracle(a: i64, r: f64) -> Ordering {
        if r.is_nan() {
            return if r.is_sign_negative() {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        if r.is_infinite() {
            return if r < 0.0 {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }
        match numerically(a, r) {
            Ordering::Equal if r == 0.0 && r.is_sign_negative() => Ordering::Greater,
            Ordering::Equal => Ordering::Less,
            other => other,
        }
    }

    fn identical(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Real(x), Value::Real(y)) => x.to_bits() == y.to_bits(),
            _ => a == b,
        }
    }

    fn curated() -> Vec<Value> {
        let reals = [
            f64::NAN,
            -f64::NAN,
            f64::from_bits(0x7ff0_0000_0000_0001),
            f64::from_bits(0xfff8_0000_0000_0001),
            f64::NEG_INFINITY,
            -9_223_372_036_854_775_808.0,
            -(TWO_53 as f64),
            -1.0,
            -0.5,
            -5e-324,
            -0.0,
            0.0,
            5e-324,
            0.5,
            1.0,
            TWO_53 as f64,
            (TWO_53 + 2) as f64,
            (TWO_53 + 4) as f64,
            9_223_372_036_854_775_808.0,
            f64::INFINITY,
        ];
        let ints = [
            i64::MIN,
            -TWO_53 - 1,
            -TWO_53,
            -1,
            0,
            1,
            TWO_53,
            TWO_53 + 1,
            TWO_53 + 5,
            i64::MAX,
        ];
        let mut all = vec![Value::Null];
        all.extend(ints.map(Value::Int));
        all.extend(reals.map(Value::Real));
        all.extend(["", "a", "b", "é"].map(|t| Value::Text(t.into())));
        all.extend([vec![], vec![0], vec![1], vec![0, 0]].map(Value::Blob));
        all.extend([Gid([0; 16]), Gid([1; 16])].map(Value::Ref));
        all
    }

    /// Checks the order's laws on three values: it is total and antisymmetric, `Equal` exactly
    /// when the values are identical, and transitive.
    fn lawful(a: &Value, b: &Value, c: &Value) -> Result<(), String> {
        let ab = a.total_cmp(b);
        if ab != b.total_cmp(a).reverse() {
            return Err(format!("{a:?} {b:?} not antisymmetric"));
        }
        if (ab == Ordering::Equal) != identical(a, b) {
            return Err(format!("{a:?} {b:?}: {ab:?}"));
        }
        let bc = b.total_cmp(c);
        if ab != Ordering::Greater && bc != Ordering::Greater {
            let ac = a.total_cmp(c);
            let want = if ab == Ordering::Less || bc == Ordering::Less {
                Ordering::Less
            } else {
                Ordering::Equal
            };
            if ac != want {
                return Err(format!("{a:?} {b:?} {c:?}: {ab:?} {bc:?} {ac:?}"));
            }
        }
        Ok(())
    }

    #[test]
    fn the_order_is_total_antisymmetric_and_transitive_over_awkward_values() {
        let all = curated();
        for a in &all {
            for b in &all {
                for c in &all {
                    lawful(a, b, c).unwrap();
                }
            }
        }
    }

    #[test]
    fn an_integer_and_a_real_compare_as_the_numbers_they_are() {
        let less = |a: Value, b: Value| {
            assert_eq!(a.total_cmp(&b), Ordering::Less, "{a:?} < {b:?}");
            assert_eq!(b.total_cmp(&a), Ordering::Greater, "{b:?} > {a:?}");
        };
        less(Value::Real(TWO_53 as f64), Value::Int(TWO_53 + 1));
        less(Value::Real((TWO_53 + 4) as f64), Value::Int(TWO_53 + 5));
        less(
            Value::Int(i64::MAX),
            Value::Real(9_223_372_036_854_775_808.0),
        );
        less(
            Value::Int(i64::MIN),
            Value::Real(-9_223_372_036_854_775_808.0),
        );
        less(Value::Real(-0.0), Value::Int(0));
        less(Value::Int(0), Value::Real(0.0));
        less(Value::Real(-f64::NAN), Value::Int(i64::MIN));
        less(Value::Int(i64::MAX), Value::Real(f64::NAN));
        less(Value::Int(0), Value::Real(5e-324));
        less(Value::Real(-5e-324), Value::Int(0));
        less(Value::Int(1), Value::Real(1.0));
        less(Value::Int(TWO_53), Value::Real(TWO_53 as f64));
        assert_ne!(Value::Int(1).total_cmp(&Value::Real(1.0)), Ordering::Equal);
    }

    #[test]
    fn max_keeps_the_greater_number_in_either_order() {
        use crate::change::{Change, State, Strategy};
        use crate::clock::Hlc;
        let pairs = [
            (Value::Real(TWO_53 as f64), Value::Int(TWO_53 + 1)),
            (Value::Real((TWO_53 + 4) as f64), Value::Int(TWO_53 + 5)),
            (
                Value::Int(i64::MAX),
                Value::Real(9_223_372_036_854_775_808.0),
            ),
            (Value::Real(-0.0), Value::Int(0)),
            (Value::Int(0), Value::Real(0.0)),
            (Value::Int(i64::MAX), Value::Real(f64::NAN)),
            (Value::Real(-f64::NAN), Value::Int(i64::MIN)),
        ];
        for (smaller, greater) in pairs {
            for (first, second) in [(&smaller, &greater), (&greater, &smaller)] {
                let mut state = State::default();
                for (i, v) in [first, second].into_iter().enumerate() {
                    let change = Change {
                        collection: "c".into(),
                        gid: Gid([0; 16]),
                        field: "f".into(),
                        value: v.clone(),
                        clock: Hlc {
                            wall_ms: i as u64,
                            counter: 0,
                            node: Gid([0; 16]),
                        },
                    };
                    state.apply(&change, Strategy::Max);
                }
                let (kept, _) = state.get("c", Gid([0; 16]), "f").unwrap();
                assert!(identical(kept, &greater), "{kept:?}, not {greater:?}");
            }
        }
    }

    /// Reals from any bits, and some near integers.
    fn arb_real() -> impl Strategy<Value = f64> {
        prop_oneof![
            any::<u64>().prop_map(f64::from_bits),
            (any::<i64>(), -3i64..=3).prop_map(|(i, d)| i.saturating_add(d) as f64),
            (-2i64..=2, 50u32..64, any::<bool>()).prop_map(|(d, p, n)| {
                let x = ((1i128 << p) + i128::from(d)) as f64;
                if n { -x } else { x }
            }),
        ]
    }

    /// Integers from anywhere, and some near the reals `arb_real` makes.
    fn arb_int() -> impl Strategy<Value = i64> {
        prop_oneof![
            any::<i64>(),
            (-2i64..=2, 50u32..63, any::<bool>()).prop_map(|(d, p, n)| {
                let x = (1i64 << p) + d;
                if n { -x } else { x }
            }),
            Just(i64::MIN),
            Just(i64::MAX),
            -2i64..=2,
        ]
    }

    fn arb_number() -> impl Strategy<Value = Value> {
        prop_oneof![
            arb_int().prop_map(Value::Int),
            arb_real().prop_map(Value::Real)
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 20_000, ..crate::proptest_config() })]

        #[test]
        fn an_integer_and_a_real_compare_as_the_exact_oracle_says(a in arb_int(), r in arb_real()) {
            prop_assert_eq!(Value::Int(a).total_cmp(&Value::Real(r)), oracle(a, r), "{} {:?}", a, r);
            prop_assert_eq!(Value::Real(r).total_cmp(&Value::Int(a)), oracle(a, r).reverse());
        }

        #[test]
        fn the_order_on_numbers_is_lawful(a in arb_number(), b in arb_number(), c in arb_number()) {
            for (x, y, z) in [(&a, &b, &c), (&a, &c, &b), (&b, &a, &c), (&b, &c, &a), (&c, &a, &b), (&c, &b, &a)] {
                if let Err(e) = lawful(x, y, z) {
                    prop_assert!(false, "{}", e);
                }
            }
        }
    }

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

use fongo_sync_core::{Change, Gid, Hlc, Value};

use crate::wire::{DecodeError, Reader, put_blob, put_varint};

/// The batch format this crate writes, and the only one it reads.
pub const BATCH_FORMAT: u8 = 1;

const MIN_CHANGE_LEN: usize = 1 + 16 + 1 + 1 + 8 + 4 + 16;

/// The canonical bytes of a batch: bit-identical batches encode to identical bytes. Reals are
/// encoded by bit pattern, so `0.0` and `-0.0`, or NaNs with different payloads, differ.
///
/// Bytes: format, flags (zero; reserved for compression), a LEB128 count, then per change: the
/// collection, the gid, the field, the value, and the clock's wall time (u64), counter (u32) and
/// node. Text and blobs are a LEB128 length then bytes; integers are big-endian. A value is a tag
/// (0 null, 1 int, 2 real, 3 text, 4 blob, 5 ref) then its 8 bytes, text, blob or 16-byte gid.
///
/// A later version may write a later format, set flags or add tags. A reader meeting one cannot
/// tell what the batch holds, and should not take it as damaged and skip it.
pub fn encode_batch(changes: &[Change]) -> Vec<u8> {
    let mut out = vec![BATCH_FORMAT, 0];
    put_varint(&mut out, changes.len() as u64);
    for change in changes {
        put_blob(&mut out, change.collection.as_bytes());
        out.extend_from_slice(&change.gid.0);
        put_blob(&mut out, change.field.as_bytes());
        put_value(&mut out, &change.value);
        out.extend_from_slice(&change.clock.wall_ms.to_be_bytes());
        out.extend_from_slice(&change.clock.counter.to_be_bytes());
        out.extend_from_slice(&change.clock.node.0);
    }
    out
}

/// Decodes what `encode_batch` wrote. Fails on any other input and never panics. Allocation is
/// bounded by a constant multiple of the input's length, because every count and length is checked
/// against the remaining input first.
pub fn decode_batch(bytes: &[u8]) -> Result<Vec<Change>, DecodeError> {
    let mut r = Reader::new(bytes);
    let version = r.u8()?;
    if version != BATCH_FORMAT {
        return Err(DecodeError::UnsupportedVersion(version));
    }
    let flags = r.u8()?;
    if flags != 0 {
        return Err(DecodeError::UnsupportedFlags(flags));
    }
    let count = r.len(MIN_CHANGE_LEN)?;
    let mut changes = Vec::with_capacity(count);
    for _ in 0..count {
        let collection = r.text()?.to_owned();
        let gid = Gid(r.array()?);
        let field = r.text()?.to_owned();
        let value = read_value(&mut r)?;
        let clock = Hlc {
            wall_ms: r.u64()?,
            counter: r.u32()?,
            node: Gid(r.array()?),
        };
        changes.push(Change {
            collection,
            gid,
            field,
            value,
            clock,
        });
    }
    r.finish()?;
    Ok(changes)
}

fn put_value(out: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Null => out.push(0),
        Value::Int(v) => {
            out.push(1);
            out.extend_from_slice(&v.to_be_bytes());
        }
        Value::Real(v) => {
            out.push(2);
            out.extend_from_slice(&v.to_bits().to_be_bytes());
        }
        Value::Text(v) => {
            out.push(3);
            put_blob(out, v.as_bytes());
        }
        Value::Blob(v) => {
            out.push(4);
            put_blob(out, v);
        }
        Value::Ref(v) => {
            out.push(5);
            out.extend_from_slice(&v.0);
        }
    }
}

fn read_value(r: &mut Reader<'_>) -> Result<Value, DecodeError> {
    Ok(match r.u8()? {
        0 => Value::Null,
        1 => Value::Int(i64::from_be_bytes(r.array()?)),
        2 => Value::Real(f64::from_bits(r.u64()?)),
        3 => Value::Text(r.text()?.to_owned()),
        4 => Value::Blob(r.blob()?.to_vec()),
        5 => Value::Ref(Gid(r.array()?)),
        tag => return Err(DecodeError::UnknownValueTag(tag)),
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use proptest::prelude::*;

    use super::*;

    fn arb_gid() -> impl Strategy<Value = Gid> {
        any::<[u8; 16]>().prop_map(Gid)
    }

    fn arb_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Null),
            any::<i64>().prop_map(Value::Int),
            any::<u64>().prop_map(|bits| Value::Real(f64::from_bits(bits))),
            ".{0,40}".prop_map(Value::Text),
            proptest::collection::vec(any::<u8>(), 0..300).prop_map(Value::Blob),
            arb_gid().prop_map(Value::Ref),
        ]
    }

    pub(crate) fn arb_changes() -> impl Strategy<Value = Vec<Change>> {
        let one = (
            ".{0,12}",
            arb_gid(),
            ".{0,12}",
            arb_value(),
            any::<u64>(),
            any::<u32>(),
            arb_gid(),
        )
            .prop_map(
                |(collection, gid, field, value, wall_ms, counter, node)| Change {
                    collection,
                    gid,
                    field,
                    value,
                    clock: Hlc {
                        wall_ms,
                        counter,
                        node,
                    },
                },
            );
        proptest::collection::vec(one, 0..20)
    }

    fn same(a: &[Change], b: &[Change]) -> bool {
        a.len() == b.len()
            && a.iter().zip(b).all(|(x, y)| {
                let value_eq = match (&x.value, &y.value) {
                    (Value::Real(p), Value::Real(q)) => p.to_bits() == q.to_bits(),
                    (p, q) => p == q,
                };
                value_eq
                    && x.collection == y.collection
                    && x.gid == y.gid
                    && x.field == y.field
                    && x.clock == y.clock
            })
    }

    proptest! {
        #[test]
        fn any_batch_round_trips(changes in arb_changes()) {
            let bytes = encode_batch(&changes);
            let back = decode_batch(&bytes).unwrap();
            prop_assert!(same(&changes, &back));
            prop_assert_eq!(encode_batch(&back), bytes);
        }

        #[test]
        fn decoding_arbitrary_bytes_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
            let _ = decode_batch(&bytes);
        }

        #[test]
        fn decoding_a_mutated_batch_never_panics(
            changes in arb_changes(),
            at in any::<prop::sample::Index>(),
            byte in any::<u8>(),
        ) {
            let mut bytes = encode_batch(&changes);
            let i = at.index(bytes.len());
            bytes[i] = byte;
            let _ = decode_batch(&bytes);
        }

        #[test]
        fn every_truncation_is_refused(changes in arb_changes(), at in any::<prop::sample::Index>()) {
            let bytes = encode_batch(&changes);
            let cut = at.index(bytes.len());
            prop_assert!(decode_batch(&bytes[..cut]).is_err());
        }
    }

    #[test]
    fn trailing_bytes_unknown_versions_and_flags_are_refused() {
        let mut bytes = encode_batch(&[]);
        assert_eq!(decode_batch(&bytes), Ok(vec![]));
        bytes.push(0);
        assert_eq!(decode_batch(&bytes), Err(DecodeError::TrailingBytes));
        assert_eq!(
            decode_batch(&[2, 0, 0]),
            Err(DecodeError::UnsupportedVersion(2))
        );
        assert_eq!(
            decode_batch(&[1, 1, 0]),
            Err(DecodeError::UnsupportedFlags(1))
        );
        assert_eq!(decode_batch(&[]), Err(DecodeError::Truncated));
    }

    #[test]
    fn a_huge_claimed_count_allocates_nothing() {
        let mut bytes = vec![BATCH_FORMAT, 0];
        put_varint(&mut bytes, u64::MAX >> 1);
        assert_eq!(decode_batch(&bytes), Err(DecodeError::Truncated));
    }

    proptest! {
        #[test]
        fn every_proper_prefix_reports_truncation(changes in arb_changes()) {
            let bytes = encode_batch(&changes);
            for cut in 0..bytes.len() {
                prop_assert_eq!(decode_batch(&bytes[..cut]), Err(DecodeError::Truncated), "cut {}", cut);
            }
        }
    }
}

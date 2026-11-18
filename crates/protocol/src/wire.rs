use thiserror::Error;

/// Why bytes from the network could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DecodeError {
    #[error("input ends early")]
    Truncated,
    #[error("input continues after the end")]
    TrailingBytes,
    #[error("unsupported format version {0}")]
    UnsupportedVersion(u8),
    #[error("unsupported flags {0:#04x}")]
    UnsupportedFlags(u8),
    #[error("unknown value tag {0}")]
    UnknownValueTag(u8),
    #[error("text is not UTF-8")]
    InvalidUtf8,
    #[error("a length is not minimally encoded or exceeds the input")]
    BadLength,
}

pub(crate) struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Reader<'a> {
        Reader { rest: bytes }
    }

    pub(crate) fn finish(self) -> Result<(), DecodeError> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(DecodeError::TrailingBytes)
        }
    }

    pub(crate) fn bytes(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if n > self.rest.len() {
            return Err(DecodeError::Truncated);
        }
        let (head, tail) = self.rest.split_at(n);
        self.rest = tail;
        Ok(head)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        let mut out = [0u8; N];
        out.copy_from_slice(self.bytes(N)?);
        Ok(out)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.array::<1>()?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    /// An unsigned LEB128 integer of at most ten bytes, rejecting non-minimal forms.
    pub(crate) fn varint(&mut self) -> Result<u64, DecodeError> {
        let mut value = 0u64;
        for i in 0..10 {
            let byte = self.u8()?;
            let low = u64::from(byte & 0x7f);
            if i == 9 && low > 1 {
                return Err(DecodeError::BadLength);
            }
            value |= low << (7 * i);
            if byte & 0x80 == 0 {
                if i > 0 && byte == 0 {
                    return Err(DecodeError::BadLength);
                }
                return Ok(value);
            }
        }
        Err(DecodeError::BadLength)
    }

    /// A length that must fit in what is left of the input, each unit taking `unit` bytes or more.
    pub(crate) fn len(&mut self, unit: usize) -> Result<usize, DecodeError> {
        let n = self.varint()?;
        let n = usize::try_from(n).map_err(|_| DecodeError::BadLength)?;
        if n.checked_mul(unit.max(1))
            .is_none_or(|need| need > self.rest.len())
        {
            return Err(DecodeError::BadLength);
        }
        Ok(n)
    }

    pub(crate) fn blob(&mut self) -> Result<&'a [u8], DecodeError> {
        let n = self.len(1)?;
        self.bytes(n)
    }

    pub(crate) fn text(&mut self) -> Result<&'a str, DecodeError> {
        std::str::from_utf8(self.blob()?).map_err(|_| DecodeError::InvalidUtf8)
    }
}

pub(crate) fn put_varint(out: &mut Vec<u8>, mut n: u64) {
    while n >= 0x80 {
        out.push((n as u8) | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
}

pub(crate) fn put_blob(out: &mut Vec<u8>, bytes: &[u8]) {
    put_varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_round_trip_and_reject_padding_and_overflow() {
        for n in [0, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut out = Vec::new();
            put_varint(&mut out, n);
            let mut r = Reader::new(&out);
            assert_eq!(r.varint(), Ok(n));
            assert_eq!(r.finish(), Ok(()));
        }
        assert_eq!(
            Reader::new(&[0x80, 0x00]).varint(),
            Err(DecodeError::BadLength)
        );
        let mut over = vec![0xff; 9];
        over.push(0x02);
        assert_eq!(Reader::new(&over).varint(), Err(DecodeError::BadLength));
        assert_eq!(Reader::new(&[0x80]).varint(), Err(DecodeError::Truncated));
    }

    #[test]
    fn a_length_longer_than_the_input_is_refused_before_allocating() {
        let mut out = Vec::new();
        put_varint(&mut out, u64::MAX);
        assert_eq!(Reader::new(&out).len(1), Err(DecodeError::BadLength));
        assert_eq!(Reader::new(&[3, 0, 0]).len(1), Err(DecodeError::BadLength));
        assert_eq!(Reader::new(&[2, 0, 0]).len(1), Ok(2));
    }
}

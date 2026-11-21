//! A row's image, as stored: each field's name and value, exactly as the row held them.

use fongo_sync_core::Value;
use fongo_sync_core::model::Row;

use crate::error::{Error, Result};

pub(crate) fn encode(row: &Row) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(row.len() as u32).to_be_bytes());
    for (field, value) in row {
        put(&mut out, field.as_bytes());
        let (tag, bytes): (u8, Vec<u8>) = match value {
            Value::Null => (0, Vec::new()),
            Value::Int(v) => (1, v.to_be_bytes().to_vec()),
            Value::Real(v) => (2, v.to_bits().to_be_bytes().to_vec()),
            Value::Text(v) => (3, v.as_bytes().to_vec()),
            Value::Blob(v) => (4, v.clone()),
            Value::Ref(v) => (5, v.0.to_vec()),
        };
        out.push(tag);
        put(&mut out, &bytes);
    }
    out
}

fn put(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(Error::Corrupt("an image is cut short".into()));
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }

    fn len(&mut self) -> Result<usize> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes(bytes.try_into().expect("four bytes")) as usize)
    }

    fn chunk(&mut self) -> Result<&'a [u8]> {
        let n = self.len()?;
        self.take(n)
    }
}

pub(crate) fn decode(bytes: &[u8]) -> Result<Row> {
    let mut r = Reader(bytes);
    let n = r.len()?;
    let mut row = Row::new();
    let bad = || Error::Corrupt("an image holds a bad value".into());
    for _ in 0..n {
        let field = String::from_utf8(r.chunk()?.to_vec()).map_err(|_| bad())?;
        let tag = r.take(1)?[0];
        let bytes = r.chunk()?;
        let eight = || <[u8; 8]>::try_from(bytes).map_err(|_| bad());
        let value = match tag {
            0 => Value::Null,
            1 => Value::Int(i64::from_be_bytes(eight()?)),
            2 => Value::Real(f64::from_bits(u64::from_be_bytes(eight()?))),
            3 => Value::Text(String::from_utf8(bytes.to_vec()).map_err(|_| bad())?),
            4 => Value::Blob(bytes.to_vec()),
            5 => Value::Ref(fongo_sync_core::Gid::from_slice(bytes).ok_or_else(bad)?),
            _ => return Err(bad()),
        };
        row.insert(field, value);
    }
    Ok(row)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_image_round_trips() {
        let row: Row = [
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Int(-3)),
            ("c".to_string(), Value::Real(f64::NAN)),
            ("d".to_string(), Value::Text("é".into())),
            ("e".to_string(), Value::Blob(vec![])),
            ("f".to_string(), Value::Ref(fongo_sync_core::Gid([7; 16]))),
        ]
        .into();
        let back = decode(&encode(&row)).unwrap();
        assert_eq!(back.len(), row.len());
        for (k, v) in &row {
            assert_eq!(v.total_cmp(&back[k]), std::cmp::Ordering::Equal);
        }
        assert!(decode(&encode(&row)[..5]).is_err());
    }
}

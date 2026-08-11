//! Versioned canonical framing for durable migration comparisons.

use sha2::{Digest, Sha256};

use super::model::{DbValue, ValueFormat};

pub const CANONICAL_ENCODING_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalRow<'a> {
    pub table: &'a str,
    pub columns: &'a [&'a str],
    pub key: &'a [DbValue],
    pub values: &'a [DbValue],
}

pub fn encode_row(row: &CanonicalRow<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(b"SSCR");
    out.extend_from_slice(&CANONICAL_ENCODING_VERSION.to_be_bytes());
    frame(&mut out, row.table.as_bytes());
    count(&mut out, row.columns.len());
    for column in row.columns {
        frame(&mut out, column.as_bytes());
    }
    values(&mut out, row.key);
    values(&mut out, row.values);
    out
}

pub fn digest_row(row: &CanonicalRow<'_>) -> [u8; 32] {
    Sha256::digest(encode_row(row)).into()
}

pub fn digest_rows<'a>(rows: impl IntoIterator<Item = &'a CanonicalRow<'a>>) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"SSCS");
    digest.update(CANONICAL_ENCODING_VERSION.to_be_bytes());
    for row in rows {
        let encoded = encode_row(row);
        digest.update((encoded.len() as u64).to_be_bytes());
        digest.update(encoded);
    }
    digest.finalize().into()
}

fn count(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&(value as u64).to_be_bytes());
}

fn frame(out: &mut Vec<u8>, bytes: &[u8]) {
    count(out, bytes.len());
    out.extend_from_slice(bytes);
}

fn values(out: &mut Vec<u8>, input: &[DbValue]) {
    count(out, input.len());
    for value in input {
        encode_value(out, value);
    }
}

fn tagged(out: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    out.push(tag);
    frame(out, payload);
}

fn encode_value(out: &mut Vec<u8>, value: &DbValue) {
    match value {
        DbValue::Null => tagged(out, 0, &[]),
        DbValue::Bool(value) => tagged(out, 1, &[*value as u8]),
        DbValue::Signed(value) => tagged(out, 2, &value.to_be_bytes()),
        DbValue::Unsigned(value) => tagged(out, 3, &value.to_be_bytes()),
        DbValue::Decimal { coefficient, scale } => {
            let mut payload = scale.to_be_bytes().to_vec();
            frame(&mut payload, coefficient);
            tagged(out, 4, &payload);
        }
        DbValue::Float32(bits) => tagged(out, 5, &bits.to_be_bytes()),
        DbValue::Float64(bits) => tagged(out, 6, &bits.to_be_bytes()),
        DbValue::Text(value) => tagged(out, 7, value.as_bytes()),
        DbValue::Bytes(value) => tagged(out, 8, value),
        DbValue::Json(value) => tagged(out, 9, value),
        DbValue::Date { year, month, day } => {
            let mut payload = year.to_be_bytes().to_vec();
            payload.extend_from_slice(&[*month, *day]);
            tagged(out, 10, &payload);
        }
        DbValue::Time { nanos } => tagged(out, 11, &nanos.to_be_bytes()),
        DbValue::Timestamp {
            local,
            offset_minutes,
            precision,
        } => {
            let mut payload = vec![*precision];
            match offset_minutes {
                Some(offset) => {
                    payload.push(1);
                    payload.extend_from_slice(&offset.to_be_bytes());
                }
                None => payload.push(0),
            }
            frame(&mut payload, local.as_bytes());
            tagged(out, 12, &payload);
        }
        DbValue::Vendor {
            type_id,
            format,
            bytes,
        } => {
            let mut payload = Vec::new();
            frame(&mut payload, type_id.as_bytes());
            let format = match format {
                ValueFormat::Binary => b"binary".as_slice(),
                ValueFormat::Text => b"text".as_slice(),
            };
            tagged(&mut payload, 0, format);
            frame(&mut payload, bytes);
            tagged(out, 13, &payload);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framing_distinguishes_boundaries_and_types() {
        let columns = ["v"];
        let a = CanonicalRow {
            table: "ab",
            columns: &columns,
            key: &[],
            values: &[DbValue::Text("c".into())],
        };
        let b = CanonicalRow {
            table: "a",
            columns: &columns,
            key: &[],
            values: &[DbValue::Text("bc".into())],
        };
        let c = CanonicalRow {
            table: "ab",
            columns: &columns,
            key: &[],
            values: &[DbValue::Bytes(b"c".to_vec())],
        };
        assert_ne!(digest_row(&a), digest_row(&b));
        assert_ne!(digest_row(&a), digest_row(&c));
    }

    #[test]
    fn float_bits_remain_exact() {
        let columns = ["v"];
        let positive = CanonicalRow {
            table: "t",
            columns: &columns,
            key: &[],
            values: &[DbValue::Float64(0.0f64.to_bits())],
        };
        let negative = CanonicalRow {
            table: "t",
            columns: &columns,
            key: &[],
            values: &[DbValue::Float64((-0.0f64).to_bits())],
        };
        assert_ne!(digest_row(&positive), digest_row(&negative));
    }
}

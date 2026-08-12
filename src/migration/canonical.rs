//! Versioned canonical framing for durable migration comparisons.

use std::collections::BTreeMap;

use sha2::{Digest, Sha256};
use thiserror::Error;

use super::model::{DbValue, ValueFormat};

pub const CANONICAL_ENCODING_VERSION: u16 = 2;
const MAX_JSON_NESTING_DEPTH: usize = 128;

#[derive(Debug, Error)]
pub enum CanonicalJsonError {
    #[error("invalid JSON at byte {offset}: {reason}")]
    Invalid { offset: usize, reason: String },
    #[error("JSON object contains a duplicate decoded key")]
    DuplicateKey,
    #[error("JSON number exponent is outside the canonical encoding range")]
    NumberExponentOutOfRange,
}

/// Parse JSON into the canonical migration representation.
///
/// Object keys are sorted by their decoded Unicode value, array order is
/// retained, strings retain their Unicode code points, and mathematically
/// equal JSON numbers use one exact decimal representation. Duplicate object
/// keys are rejected at every nesting level.
pub fn canonicalize_json(input: &[u8]) -> Result<Vec<u8>, CanonicalJsonError> {
    JsonParser::new(input).parse()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalRow<'a> {
    pub table: &'a str,
    pub columns: &'a [&'a str],
    pub key: &'a [DbValue],
    pub values: &'a [DbValue],
}

pub fn encode_row(row: &CanonicalRow<'_>) -> Result<Vec<u8>, CanonicalJsonError> {
    let mut out = Vec::new();
    out.extend_from_slice(b"SSCR");
    out.extend_from_slice(&CANONICAL_ENCODING_VERSION.to_be_bytes());
    frame(&mut out, row.table.as_bytes());
    count(&mut out, row.columns.len());
    for column in row.columns {
        frame(&mut out, column.as_bytes());
    }
    values(&mut out, row.key)?;
    values(&mut out, row.values)?;
    Ok(out)
}

pub fn digest_row(row: &CanonicalRow<'_>) -> Result<[u8; 32], CanonicalJsonError> {
    Ok(Sha256::digest(encode_row(row)?).into())
}

pub fn digest_rows<'a>(
    rows: impl IntoIterator<Item = &'a CanonicalRow<'a>>,
) -> Result<[u8; 32], CanonicalJsonError> {
    let mut digest = Sha256::new();
    digest.update(b"SSCS");
    digest.update(CANONICAL_ENCODING_VERSION.to_be_bytes());
    for row in rows {
        let encoded = encode_row(row)?;
        digest.update((encoded.len() as u64).to_be_bytes());
        digest.update(encoded);
    }
    Ok(digest.finalize().into())
}

fn count(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&(value as u64).to_be_bytes());
}

fn frame(out: &mut Vec<u8>, bytes: &[u8]) {
    count(out, bytes.len());
    out.extend_from_slice(bytes);
}

fn values(out: &mut Vec<u8>, input: &[DbValue]) -> Result<(), CanonicalJsonError> {
    count(out, input.len());
    for value in input {
        encode_value(out, value)?;
    }
    Ok(())
}

fn tagged(out: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    out.push(tag);
    frame(out, payload);
}

fn encode_value(out: &mut Vec<u8>, value: &DbValue) -> Result<(), CanonicalJsonError> {
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
        DbValue::Json(value) => tagged(out, 9, &canonicalize_json(value)?),
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
    Ok(())
}

struct JsonParser<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn parse(mut self) -> Result<Vec<u8>, CanonicalJsonError> {
        self.skip_whitespace();
        let value = self.value(0)?;
        self.skip_whitespace();
        if self.offset != self.input.len() {
            return Err(self.invalid("trailing content"));
        }
        Ok(value)
    }

    fn value(&mut self, depth: usize) -> Result<Vec<u8>, CanonicalJsonError> {
        if depth > MAX_JSON_NESTING_DEPTH {
            return Err(self.invalid("JSON nesting depth exceeds the canonical limit"));
        }
        self.skip_whitespace();
        match self.peek() {
            Some(b'n') => self.literal(b"null"),
            Some(b't') => self.literal(b"true"),
            Some(b'f') => self.literal(b"false"),
            Some(b'\"') => {
                serde_json::to_vec(&self.string()?).map_err(|error| self.invalid(error.to_string()))
            }
            Some(b'[') => self.array(depth),
            Some(b'{') => self.object(depth),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(_) => Err(self.invalid("expected a JSON value")),
            None => Err(self.invalid("unexpected end of input")),
        }
    }

    fn literal(&mut self, literal: &[u8]) -> Result<Vec<u8>, CanonicalJsonError> {
        if self.input.get(self.offset..self.offset + literal.len()) != Some(literal) {
            return Err(self.invalid("invalid literal"));
        }
        self.offset += literal.len();
        Ok(literal.to_vec())
    }

    fn string(&mut self) -> Result<String, CanonicalJsonError> {
        let start = self.offset;
        if self.take() != Some(b'\"') {
            return Err(self.invalid("expected a JSON string"));
        }
        let mut escaped = false;
        while let Some(byte) = self.take() {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'\"' {
                return serde_json::from_slice(&self.input[start..self.offset]).map_err(|error| {
                    CanonicalJsonError::Invalid {
                        offset: start + error.column().saturating_sub(1),
                        reason: error.to_string(),
                    }
                });
            }
        }
        Err(CanonicalJsonError::Invalid {
            offset: start,
            reason: "unterminated JSON string".into(),
        })
    }

    fn array(&mut self, depth: usize) -> Result<Vec<u8>, CanonicalJsonError> {
        self.offset += 1;
        self.skip_whitespace();
        let mut values = Vec::new();
        if self.peek() == Some(b']') {
            self.offset += 1;
            return Ok(b"[]".to_vec());
        }
        loop {
            values.push(self.value(depth + 1)?);
            self.skip_whitespace();
            match self.take() {
                Some(b',') => self.skip_whitespace(),
                Some(b']') => break,
                _ => return Err(self.invalid("expected ',' or ']'")),
            }
        }
        let payload_len = values.iter().map(Vec::len).sum::<usize>() + values.len();
        let mut canonical = Vec::with_capacity(payload_len + 1);
        canonical.push(b'[');
        for (index, value) in values.iter().enumerate() {
            if index != 0 {
                canonical.push(b',');
            }
            canonical.extend_from_slice(value);
        }
        canonical.push(b']');
        Ok(canonical)
    }

    fn object(&mut self, depth: usize) -> Result<Vec<u8>, CanonicalJsonError> {
        self.offset += 1;
        self.skip_whitespace();
        let mut members = BTreeMap::new();
        if self.peek() == Some(b'}') {
            self.offset += 1;
            return Ok(b"{}".to_vec());
        }
        loop {
            let key = self.string()?;
            self.skip_whitespace();
            if self.take() != Some(b':') {
                return Err(self.invalid("expected ':'"));
            }
            let value = self.value(depth + 1)?;
            if members.insert(key.clone(), value).is_some() {
                return Err(CanonicalJsonError::DuplicateKey);
            }
            self.skip_whitespace();
            match self.take() {
                Some(b',') => self.skip_whitespace(),
                Some(b'}') => break,
                _ => return Err(self.invalid("expected ',' or '}'")),
            }
        }
        let mut canonical = Vec::new();
        canonical.push(b'{');
        for (index, (key, value)) in members.iter().enumerate() {
            if index != 0 {
                canonical.push(b',');
            }
            canonical.extend_from_slice(
                &serde_json::to_vec(key).map_err(|error| self.invalid(error.to_string()))?,
            );
            canonical.push(b':');
            canonical.extend_from_slice(value);
        }
        canonical.push(b'}');
        Ok(canonical)
    }

    fn number(&mut self) -> Result<Vec<u8>, CanonicalJsonError> {
        let start = self.offset;
        let negative = self.peek() == Some(b'-');
        if negative {
            self.offset += 1;
        }
        match self.take() {
            Some(b'0') if self.peek().is_some_and(|byte| byte.is_ascii_digit()) => {
                return Err(self.invalid("leading zero in JSON number"));
            }
            Some(b'0' | b'1'..=b'9') => {}
            _ => return Err(self.invalid("invalid JSON number")),
        }
        while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
            self.offset += 1;
        }
        let integer_end = self.offset;
        let mut fraction_digits = 0usize;
        if self.peek() == Some(b'.') {
            self.offset += 1;
            let fraction_start = self.offset;
            while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                self.offset += 1;
            }
            fraction_digits = self.offset - fraction_start;
            if fraction_digits == 0 {
                return Err(self.invalid("JSON fraction has no digits"));
            }
        }
        let fraction_end = self.offset;
        let mut explicit_exponent = 0i64;
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.offset += 1;
            let exponent_negative = match self.peek() {
                Some(b'+') => {
                    self.offset += 1;
                    false
                }
                Some(b'-') => {
                    self.offset += 1;
                    true
                }
                _ => false,
            };
            let exponent_start = self.offset;
            while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                let digit = i64::from(self.input[self.offset] - b'0');
                explicit_exponent = explicit_exponent
                    .checked_mul(10)
                    .and_then(|value| value.checked_add(digit))
                    .ok_or(CanonicalJsonError::NumberExponentOutOfRange)?;
                self.offset += 1;
            }
            if self.offset == exponent_start {
                return Err(self.invalid("JSON exponent has no digits"));
            }
            if exponent_negative {
                explicit_exponent = explicit_exponent
                    .checked_neg()
                    .ok_or(CanonicalJsonError::NumberExponentOutOfRange)?;
            }
        }
        if self
            .peek()
            .is_some_and(|byte| !matches!(byte, b' ' | b'\n' | b'\r' | b'\t' | b',' | b']' | b'}'))
        {
            return Err(self.invalid("invalid character after JSON number"));
        }

        let integer_start = start + usize::from(negative);
        let mut digits = self.input[integer_start..integer_end].to_vec();
        if fraction_digits != 0 {
            digits.extend_from_slice(&self.input[integer_end + 1..fraction_end]);
        }
        let first_nonzero = digits.iter().position(|digit| *digit != b'0');
        let Some(first_nonzero) = first_nonzero else {
            return Ok(b"0".to_vec());
        };
        if first_nonzero != 0 {
            digits.drain(..first_nonzero);
        }
        let fraction_digits = i64::try_from(fraction_digits)
            .map_err(|_| CanonicalJsonError::NumberExponentOutOfRange)?;
        let mut exponent = explicit_exponent
            .checked_sub(fraction_digits)
            .ok_or(CanonicalJsonError::NumberExponentOutOfRange)?;
        while digits.last() == Some(&b'0') {
            digits.pop();
            exponent = exponent
                .checked_add(1)
                .ok_or(CanonicalJsonError::NumberExponentOutOfRange)?;
        }
        let scientific_exponent = exponent
            .checked_add(
                i64::try_from(digits.len() - 1)
                    .map_err(|_| CanonicalJsonError::NumberExponentOutOfRange)?,
            )
            .ok_or(CanonicalJsonError::NumberExponentOutOfRange)?;
        let mut canonical = Vec::with_capacity(digits.len() + 24);
        if negative {
            canonical.push(b'-');
        }
        canonical.push(digits[0]);
        if digits.len() > 1 {
            canonical.push(b'.');
            canonical.extend_from_slice(&digits[1..]);
        }
        if scientific_exponent != 0 {
            canonical.push(b'e');
            canonical.extend_from_slice(scientific_exponent.to_string().as_bytes());
        }
        Ok(canonical)
    }

    fn skip_whitespace(&mut self) {
        while self
            .peek()
            .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.offset += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.offset).copied()
    }

    fn take(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.offset += 1;
        Some(byte)
    }

    fn invalid(&self, reason: impl Into<String>) -> CanonicalJsonError {
        CanonicalJsonError::Invalid {
            offset: self.offset,
            reason: reason.into(),
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
        assert_ne!(digest_row(&a).unwrap(), digest_row(&b).unwrap());
        assert_ne!(digest_row(&a).unwrap(), digest_row(&c).unwrap());
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
        assert_ne!(
            digest_row(&positive).unwrap(),
            digest_row(&negative).unwrap()
        );
    }

    #[test]
    fn nonfinite_float_bits_remain_exact() {
        let columns = ["v"];
        let digest = |value| {
            digest_row(&CanonicalRow {
                table: "t",
                columns: &columns,
                key: &[],
                values: &[value],
            })
            .unwrap()
        };

        assert_ne!(
            digest(DbValue::Float32(f32::INFINITY.to_bits())),
            digest(DbValue::Float32(f32::NEG_INFINITY.to_bits()))
        );
        assert_ne!(
            digest(DbValue::Float32(0x7fc0_0001)),
            digest(DbValue::Float32(0x7fc0_0002))
        );
        assert_ne!(
            digest(DbValue::Float64(f64::INFINITY.to_bits())),
            digest(DbValue::Float64(f64::NEG_INFINITY.to_bits()))
        );
        assert_ne!(
            digest(DbValue::Float64(0x7ff8_0000_0000_0001)),
            digest(DbValue::Float64(0x7ff8_0000_0000_0002))
        );
    }

    #[test]
    fn json_canonicalization_sorts_keys_and_normalizes_exact_numbers() {
        let first =
            canonicalize_json(br#"{ "z": [1, 1.0, 10e-1], "a": {"water":"\u6c34", "n":-0.0100} }"#)
                .unwrap();
        let second = canonicalize_json(
            "{\"a\":{\"n\":-1e-2,\"water\":\"水\"},\"z\":[1e0,1.00,0.1e1]}".as_bytes(),
        )
        .unwrap();
        assert_eq!(first, second);
        assert_eq!(
            String::from_utf8(first).unwrap(),
            r#"{"a":{"n":-1e-2,"water":"水"},"z":[1,1,1]}"#
        );
        let columns = ["payload"];
        let left = CanonicalRow {
            table: "t",
            columns: &columns,
            key: &[],
            values: &[DbValue::Json(br#"{"b":1.00,"a":1e0}"#.to_vec())],
        };
        let right = CanonicalRow {
            table: "t",
            columns: &columns,
            key: &[],
            values: &[DbValue::Json(br#"{"a":1,"b":1}"#.to_vec())],
        };
        assert_eq!(digest_row(&left).unwrap(), digest_row(&right).unwrap());
    }

    #[test]
    fn json_canonicalization_rejects_duplicate_decoded_keys_at_any_depth() {
        assert!(matches!(
            canonicalize_json(br#"{"outer":{"a":1,"\u0061":2}}"#),
            Err(CanonicalJsonError::DuplicateKey)
        ));
        let columns = ["payload"];
        let row = CanonicalRow {
            table: "t",
            columns: &columns,
            key: &[],
            values: &[DbValue::Json(br#"{"a":1,"a":2}"#.to_vec())],
        };
        assert!(matches!(
            encode_row(&row),
            Err(CanonicalJsonError::DuplicateKey)
        ));
    }

    #[test]
    fn json_canonicalization_rejects_invalid_and_unbounded_numbers() {
        assert!(matches!(
            canonicalize_json(br#"{"n":01}"#),
            Err(CanonicalJsonError::Invalid { .. })
        ));
        let exponent = format!("1e{}", "9".repeat(32));
        assert!(matches!(
            canonicalize_json(exponent.as_bytes()),
            Err(CanonicalJsonError::NumberExponentOutOfRange)
        ));
        let nested = format!("{}0{}", "[".repeat(130), "]".repeat(130));
        assert!(matches!(
            canonicalize_json(nested.as_bytes()),
            Err(CanonicalJsonError::Invalid { .. })
        ));
    }
}

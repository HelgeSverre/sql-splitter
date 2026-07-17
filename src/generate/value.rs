//! Typed representation of a single generated value.
//!
//! Generators produce [`GeneratedValue`] rather than writing SQL literals
//! directly, so downstream stages (formatting, redaction interop, planner
//! arithmetic) can inspect and combine values without re-parsing text. Money
//! and other fixed-point quantities are represented as integer minor units
//! (e.g. cents) plus a scale, so planner arithmetic never touches floats.

use std::fmt;

use crate::diagnostic::DiagnosticBag;
use crate::synthetic::schema::SqlTypeFamily;

/// A single value produced by a generator, independent of any target SQL
/// dialect's literal syntax.
#[derive(Debug, Clone, PartialEq)]
pub enum GeneratedValue {
    /// SQL `NULL`.
    Null,
    /// The column's `DEFAULT` expression, left for the writer to emit.
    Default,
    Boolean(bool),
    /// Any whole-number value, wide enough for `BIGINT` and beyond.
    Integer(i128),
    /// A fixed-point number as integer minor units (e.g. cents) plus scale,
    /// e.g. `{ minor: 1050, scale: 2 }` for `10.50`.
    Decimal {
        minor: i128,
        scale: u32,
    },
    Text(String),
    Bytes(Vec<u8>),
    /// A pre-formatted date/time/timestamp literal.
    DateTime(String),
    /// A pre-serialized JSON document.
    Json(String),
}

impl GeneratedValue {
    /// Whether this value represents SQL `NULL`.
    pub fn is_null(&self) -> bool {
        matches!(self, GeneratedValue::Null)
    }

    /// Borrow the value as a boolean, or a [`GenerateError::TypeMismatch`]
    /// if it isn't one.
    pub fn as_boolean(&self) -> Result<bool, GenerateError> {
        match self {
            GeneratedValue::Boolean(b) => Ok(*b),
            other => Err(GenerateError::type_mismatch("Boolean", other)),
        }
    }

    /// Borrow the value as an integer, or a [`GenerateError::TypeMismatch`]
    /// if it isn't one.
    pub fn as_integer(&self) -> Result<i128, GenerateError> {
        match self {
            GeneratedValue::Integer(i) => Ok(*i),
            other => Err(GenerateError::type_mismatch("Integer", other)),
        }
    }

    /// Borrow the value as `(minor units, scale)`, or a
    /// [`GenerateError::TypeMismatch`] if it isn't a decimal.
    pub fn as_decimal(&self) -> Result<(i128, u32), GenerateError> {
        match self {
            GeneratedValue::Decimal { minor, scale } => Ok((*minor, *scale)),
            other => Err(GenerateError::type_mismatch("Decimal", other)),
        }
    }

    /// Borrow the value as text, or a [`GenerateError::TypeMismatch`] if it
    /// isn't one.
    pub fn as_text(&self) -> Result<&str, GenerateError> {
        match self {
            GeneratedValue::Text(s) => Ok(s.as_str()),
            other => Err(GenerateError::type_mismatch("Text", other)),
        }
    }

    /// Borrow the value as bytes, or a [`GenerateError::TypeMismatch`] if it
    /// isn't one.
    pub fn as_bytes(&self) -> Result<&[u8], GenerateError> {
        match self {
            GeneratedValue::Bytes(b) => Ok(b.as_slice()),
            other => Err(GenerateError::type_mismatch("Bytes", other)),
        }
    }

    /// Borrow the value as a pre-formatted date/time literal, or a
    /// [`GenerateError::TypeMismatch`] if it isn't one.
    pub fn as_date_time(&self) -> Result<&str, GenerateError> {
        match self {
            GeneratedValue::DateTime(s) => Ok(s.as_str()),
            other => Err(GenerateError::type_mismatch("DateTime", other)),
        }
    }

    /// Borrow the value as a pre-serialized JSON document, or a
    /// [`GenerateError::TypeMismatch`] if it isn't one.
    pub fn as_json(&self) -> Result<&str, GenerateError> {
        match self {
            GeneratedValue::Json(s) => Ok(s.as_str()),
            other => Err(GenerateError::type_mismatch("Json", other)),
        }
    }

    /// Whether this value is a legal payload for a column of `family`,
    /// independent of any particular generator. `Null` and `Default` are
    /// legal for every family; every other variant is checked against the
    /// one or two families it can represent (e.g. a `Uuid`-family column is
    /// represented as `Text`, since [`GeneratedValue`] has no dedicated
    /// UUID variant).
    pub fn compatible_with(&self, family: &SqlTypeFamily) -> bool {
        match self {
            GeneratedValue::Null | GeneratedValue::Default => true,
            GeneratedValue::Boolean(_) => matches!(family, SqlTypeFamily::Boolean),
            GeneratedValue::Integer(_) => {
                matches!(family, SqlTypeFamily::Integer | SqlTypeFamily::BigInteger)
            }
            GeneratedValue::Decimal { .. } => matches!(family, SqlTypeFamily::Decimal),
            GeneratedValue::Text(_) => matches!(
                family,
                SqlTypeFamily::Text | SqlTypeFamily::Uuid | SqlTypeFamily::Other
            ),
            GeneratedValue::Bytes(_) => matches!(family, SqlTypeFamily::Bytes),
            GeneratedValue::DateTime(_) => matches!(family, SqlTypeFamily::DateTime),
            GeneratedValue::Json(_) => matches!(family, SqlTypeFamily::Json),
        }
    }

    /// The name of this value's variant, for error messages built outside
    /// this module (e.g. a modifier reporting a type mismatch it detected
    /// itself, rather than through one of the `as_*` accessors above).
    pub fn type_name(&self) -> &'static str {
        self.kind_name()
    }

    /// The name of this value's variant, used in error messages.
    fn kind_name(&self) -> &'static str {
        match self {
            GeneratedValue::Null => "Null",
            GeneratedValue::Default => "Default",
            GeneratedValue::Boolean(_) => "Boolean",
            GeneratedValue::Integer(_) => "Integer",
            GeneratedValue::Decimal { .. } => "Decimal",
            GeneratedValue::Text(_) => "Text",
            GeneratedValue::Bytes(_) => "Bytes",
            GeneratedValue::DateTime(_) => "DateTime",
            GeneratedValue::Json(_) => "Json",
        }
    }
}

/// Errors returned while running a compiled generator or modifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GenerateError {
    /// A typed accessor was called on a value of a different variant.
    TypeMismatch {
        expected: &'static str,
        found: &'static str,
    },
    /// A generator's internal state (e.g. a `sequence`'s counter) could not
    /// advance without wrapping past its representable range.
    Overflow(String),
    /// A modifier (e.g. `unique`) could not satisfy its constraint within
    /// its configured attempt budget.
    Exhausted(String),
    /// A generator could not make sense of a value it depends on at row
    /// time (e.g. `before`/`after` reading an unparsable source column).
    InvalidInput(String),
    /// Structured diagnostics from loading, merging, or compiling a model,
    /// surfaced through the public [`crate::generate::Generate`] API (e.g.
    /// `ModelCompiler::compile` errors, or `ModelMerger::merge` errors).
    /// Carries the full bag, including any warnings, so a caller can report
    /// every problem rather than just the first.
    Diagnostics(DiagnosticBag),
    /// `--verify` audited the generated SQL and one or more exact checks
    /// failed; the prior destination is left untouched. Carries a value-free
    /// summary of each failed check.
    VerificationFailed(Vec<String>),
}

impl GenerateError {
    fn type_mismatch(expected: &'static str, found: &GeneratedValue) -> Self {
        GenerateError::TypeMismatch {
            expected,
            found: found.kind_name(),
        }
    }
}

impl fmt::Display for GenerateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GenerateError::TypeMismatch { expected, found } => {
                write!(f, "expected a {expected} value, found {found}")
            }
            GenerateError::Overflow(message) => write!(f, "{message}"),
            GenerateError::Exhausted(message) => write!(f, "{message}"),
            GenerateError::InvalidInput(message) => write!(f, "{message}"),
            GenerateError::Diagnostics(bag) => write!(f, "{bag}"),
            GenerateError::VerificationFailed(failures) => write!(
                f,
                "GEN-VERIFY-FAILED: generated output failed verification and was not published; \
                 {} check(s) failed: {}",
                failures.len(),
                failures.join("; ")
            ),
        }
    }
}

impl std::error::Error for GenerateError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessors_return_the_matching_variant() {
        assert_eq!(GeneratedValue::Boolean(true).as_boolean(), Ok(true));
        assert_eq!(GeneratedValue::Integer(7).as_integer(), Ok(7));
        assert_eq!(
            GeneratedValue::Decimal {
                minor: 1050,
                scale: 2
            }
            .as_decimal(),
            Ok((1050, 2))
        );
        assert_eq!(GeneratedValue::Text("hi".into()).as_text(), Ok("hi"));
        assert_eq!(
            GeneratedValue::Bytes(vec![1, 2, 3]).as_bytes(),
            Ok([1u8, 2, 3].as_slice())
        );
        assert_eq!(
            GeneratedValue::DateTime("2024-01-01".into()).as_date_time(),
            Ok("2024-01-01")
        );
        assert_eq!(GeneratedValue::Json("{}".into()).as_json(), Ok("{}"));
    }

    #[test]
    fn accessors_return_type_mismatch_instead_of_panicking() {
        let value = GeneratedValue::Null;
        assert_eq!(
            value.as_integer(),
            Err(GenerateError::TypeMismatch {
                expected: "Integer",
                found: "Null",
            })
        );
    }

    #[test]
    fn is_null_reports_only_the_null_variant() {
        assert!(GeneratedValue::Null.is_null());
        assert!(!GeneratedValue::Default.is_null());
        assert!(!GeneratedValue::Integer(0).is_null());
    }

    #[test]
    fn compatible_with_accepts_null_and_default_for_every_family() {
        assert!(GeneratedValue::Null.compatible_with(&SqlTypeFamily::Integer));
        assert!(GeneratedValue::Default.compatible_with(&SqlTypeFamily::Json));
    }

    #[test]
    fn compatible_with_matches_variant_to_family() {
        assert!(GeneratedValue::Integer(1).compatible_with(&SqlTypeFamily::BigInteger));
        assert!(!GeneratedValue::Integer(1).compatible_with(&SqlTypeFamily::Text));
        assert!(GeneratedValue::Text("id".into()).compatible_with(&SqlTypeFamily::Uuid));
        assert!(!GeneratedValue::Json("{}".into()).compatible_with(&SqlTypeFamily::Text));
    }
}

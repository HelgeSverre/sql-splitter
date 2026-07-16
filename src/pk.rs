//! Shared primary key hashing and formatting utilities.
//!
//! This module is the single home for compact 64-bit hash representations of
//! primary keys and for human-readable PK formatting, used by both the
//! `validate` and `diff` commands for memory-efficient PK tracking. Keeping
//! one implementation matters: these hashes are compared across modules, so
//! divergent copies would silently break PK/FK and row comparisons.

use crate::parser::mysql_insert::PkValue;
use std::hash::{Hash, Hasher};

/// Compact primary key hash (64-bit).
/// Collision risk is negligible for realistic dump sizes.
pub type PkHash = u64;

/// Feed a single PK/FK value into `hasher`, tag-discriminated so that e.g.
/// `Int(1)` and `Text("1")` hash differently.
pub fn hash_pk_value_into<H: Hasher>(value: &PkValue, hasher: &mut H) {
    match value {
        PkValue::Int(i) => {
            0u8.hash(hasher);
            i.hash(hasher);
        }
        PkValue::BigInt(i) => {
            1u8.hash(hasher);
            i.hash(hasher);
        }
        PkValue::Text(s) => {
            2u8.hash(hasher);
            s.hash(hasher);
        }
        PkValue::Null => {
            3u8.hash(hasher);
        }
    }
}

/// Hash a list of PK/FK values into a compact 64-bit hash.
/// Uses AHash for fast, high-quality hashing.
pub fn hash_pk_values(values: &[PkValue]) -> PkHash {
    let mut hasher = ahash::AHasher::default();

    // Include arity (number of columns) in the hash to distinguish (1) from (1, NULL)
    (values.len() as u8).hash(&mut hasher);

    for v in values {
        hash_pk_value_into(v, &mut hasher);
    }

    hasher.finish()
}

/// Format a single PK value as a string.
pub fn format_single_pk(v: &PkValue) -> String {
    match v {
        PkValue::Int(i) => i.to_string(),
        PkValue::BigInt(i) => i.to_string(),
        PkValue::Text(s) => s.to_string(),
        PkValue::Null => "NULL".to_string(),
    }
}

/// Format a PK tuple as a string (single value as-is, composite as "(val1, val2)").
pub fn format_pk_tuple(pk: &[PkValue]) -> String {
    if pk.len() == 1 {
        format_single_pk(&pk[0])
    } else {
        let parts: Vec<String> = pk.iter().map(format_single_pk).collect();
        format!("({})", parts.join(", "))
    }
}

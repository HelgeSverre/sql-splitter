//! Shared primary key hashing utilities.
//!
//! This module provides compact 64-bit hash representations for primary keys,
//! used by both the `validate` and `diff` commands for memory-efficient
//! PK tracking.

use crate::parser::mysql_insert;
use std::hash::{Hash, Hasher};

/// Compact primary key hash (64-bit).
/// Collision risk is negligible for realistic dump sizes.
pub type PkHash = u64;

/// Hash a list of PK/FK values into a compact 64-bit hash.
/// Uses AHash for fast, high-quality hashing.
pub fn hash_pk_values(values: &smallvec::SmallVec<[mysql_insert::PkValue; 2]>) -> PkHash {
    let mut hasher = ahash::AHasher::default();

    // Include arity (number of columns) in the hash to distinguish (1) from (1, NULL)
    (values.len() as u8).hash(&mut hasher);

    for v in values {
        match v {
            mysql_insert::PkValue::Int(i) => {
                0u8.hash(&mut hasher);
                i.hash(&mut hasher);
            }
            mysql_insert::PkValue::BigInt(i) => {
                1u8.hash(&mut hasher);
                i.hash(&mut hasher);
            }
            mysql_insert::PkValue::Text(s) => {
                2u8.hash(&mut hasher);
                s.hash(&mut hasher);
            }
            mysql_insert::PkValue::Null => {
                3u8.hash(&mut hasher);
            }
        }
    }

    hasher.finish()
}

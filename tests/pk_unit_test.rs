//! Unit tests for the pk (primary key hashing) module.

use smallvec::smallvec;
use sql_splitter::parser::mysql_insert::PkValue;
use sql_splitter::pk::hash_pk_values;

#[test]
fn test_hash_single_int() {
    let values = smallvec![PkValue::Int(42)];
    let hash = hash_pk_values(&values);
    assert_ne!(hash, 0);
}

#[test]
fn test_hash_deterministic() {
    let values1 = smallvec![PkValue::Int(42)];
    let values2 = smallvec![PkValue::Int(42)];
    assert_eq!(hash_pk_values(&values1), hash_pk_values(&values2));
}

#[test]
fn test_hash_different_values_differ() {
    let v1 = smallvec![PkValue::Int(1)];
    let v2 = smallvec![PkValue::Int(2)];
    assert_ne!(hash_pk_values(&v1), hash_pk_values(&v2));
}

#[test]
fn test_hash_different_types_differ() {
    let int_val = smallvec![PkValue::Int(1)];
    let bigint_val = smallvec![PkValue::BigInt(1)];
    let text_val = smallvec![PkValue::Text("1".into())];

    let h1 = hash_pk_values(&int_val);
    let h2 = hash_pk_values(&bigint_val);
    let h3 = hash_pk_values(&text_val);

    assert_ne!(h1, h2);
    assert_ne!(h1, h3);
    assert_ne!(h2, h3);
}

#[test]
fn test_hash_null_value() {
    let values = smallvec![PkValue::Null];
    let hash = hash_pk_values(&values);
    assert_ne!(hash, 0);
}

#[test]
fn test_hash_composite_key() {
    let values = smallvec![PkValue::Int(1), PkValue::Text("abc".into())];
    let hash = hash_pk_values(&values);
    assert_ne!(hash, 0);
}

#[test]
fn test_hash_arity_matters() {
    let single = smallvec![PkValue::Int(1)];
    let double = smallvec![PkValue::Int(1), PkValue::Null];
    assert_ne!(hash_pk_values(&single), hash_pk_values(&double));
}

#[test]
fn test_hash_text_values() {
    let v1 = smallvec![PkValue::Text("hello".into())];
    let v2 = smallvec![PkValue::Text("world".into())];
    assert_ne!(hash_pk_values(&v1), hash_pk_values(&v2));
}

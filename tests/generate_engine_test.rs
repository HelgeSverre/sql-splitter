//! Tests for stable seed derivation and typed generated values in
//! `sql_splitter::generate`, and for the allocation-lean renderer primitives
//! in `sql_splitter::render`.

use rand::Rng;
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use sql_splitter::generate::seed::{derive_seed, SeedRoot, StreamId};
use sql_splitter::generate::value::{GenerateError, GeneratedValue};
use sql_splitter::parser::SqlDialect;
use sql_splitter::render::{RandomBlock, RowBatch, SqlString};

#[test]
fn unrelated_streams_do_not_perturb_existing_values() {
    let root = SeedRoot::new(42);
    let mut before = root.stream(StreamId::column("users", "email", "internet.email"));
    let expected = before.next_u64();

    let mut unrelated = root.stream(StreamId::column("orders", "status", "weighted_choice"));
    let _ = unrelated.next_u64();

    let mut after = root.stream(StreamId::column("users", "email", "internet.email"));
    assert_eq!(after.next_u64(), expected);
}

#[test]
fn same_stream_id_from_the_same_root_is_reproducible() {
    let root = SeedRoot::new(7);
    let mut first = root.stream(StreamId::table("orders"));
    let mut second = root.stream(StreamId::table("orders"));
    assert_eq!(first.next_u64(), second.next_u64());
}

#[test]
fn different_roots_produce_different_streams() {
    let mut a = SeedRoot::new(1).stream(StreamId::planner("orders"));
    let mut b = SeedRoot::new(2).stream(StreamId::planner("orders"));
    assert_ne!(a.next_u64(), b.next_u64());
}

#[test]
fn seed_golden_hex_is_stable() {
    // Checked-in golden for derive_seed(42, ["users", "email", "internet.email"]).
    // If this assertion ever fails, the derivation algorithm changed and every
    // previously generated dataset would silently diverge - update deliberately.
    let golden = "122016424915fd14795335972fba7c8cc3f69d38deffd59c49e22a4f477ce584";
    let actual = hex::encode(derive_seed(42, &["users", "email", "internet.email"]));
    assert_eq!(actual, golden);
}

#[test]
fn operator_stream_identity_is_stable_across_lookups() {
    // Two independently constructed StreamIds for the same logical identity
    // (table/column/operator name, no traversal index) must derive the same
    // stream from the same root.
    let root = SeedRoot::new(9);
    let id_a = StreamId::operator("users", "email", "faker.internet.email");
    let id_b = StreamId::operator("users", "email", "faker.internet.email");
    let mut a = root.stream(id_a);
    let mut b = root.stream(id_b);
    assert_eq!(a.next_u64(), b.next_u64());
}

#[test]
fn generated_value_accessors_return_typed_errors_instead_of_panicking() {
    let value = GeneratedValue::Text("hello".to_string());
    assert_eq!(value.as_text(), Ok("hello"));
    assert!(matches!(
        value.as_integer(),
        Err(GenerateError::TypeMismatch { .. })
    ));

    let int_value = GeneratedValue::Integer(42);
    assert_eq!(int_value.as_integer(), Ok(42));
    assert!(matches!(
        int_value.as_text(),
        Err(GenerateError::TypeMismatch { .. })
    ));

    assert!(GeneratedValue::Null.is_null());
    assert!(!GeneratedValue::Integer(0).is_null());
}

#[test]
fn sql_string_escapes_each_dialect_without_intermediate_contract_changes() {
    let input = "a'b\\c\n\r\t";
    assert_eq!(
        SqlString::new(SqlDialect::MySql, input).to_string(),
        "'a\\'b\\\\c\\n\\r\\t'"
    );
    assert_eq!(
        SqlString::new(SqlDialect::Postgres, input).to_string(),
        "'a''b\\c\n\r\t'"
    );
    assert_eq!(
        SqlString::new(SqlDialect::Sqlite, input).to_string(),
        "'a''b\\c\n\r\t'"
    );
    assert_eq!(
        SqlString::new(SqlDialect::Mssql, input).to_string(),
        "N'a''b\\c\n\r\t'"
    );
}

#[test]
fn row_batch_reuses_capacity_after_clear() {
    let mut batch = RowBatch::with_capacity(4, 256);
    batch.push_fmt(format_args!("(1, 'a')")).unwrap();
    batch.push_fmt(format_args!("(2, 'b')")).unwrap();
    let capacity = batch.capacity();
    assert_eq!(batch.as_str(), "(1, 'a'),\n(2, 'b')");
    batch.clear();
    assert!(batch.capacity() >= capacity);
}

#[test]
fn random_block_samples_stay_in_alphabet_and_are_seed_reproducible() {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";

    let mut a = RandomBlock::new(ChaCha8Rng::from_seed([7u8; 32]));
    let mut b = RandomBlock::new(ChaCha8Rng::from_seed([7u8; 32]));

    let bytes_a: Vec<u8> = (0..10_000).map(|_| a.next_alphanumeric()).collect();
    let bytes_b: Vec<u8> = (0..10_000).map(|_| b.next_alphanumeric()).collect();

    assert!(bytes_a.iter().all(|byte| ALPHABET.contains(byte)));
    assert_eq!(bytes_a, bytes_b);
}

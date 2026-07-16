//! Tests for stable seed derivation and typed generated values in
//! `sql_splitter::generate`.

use rand::Rng;
use sql_splitter::generate::seed::{derive_seed, SeedRoot, StreamId};
use sql_splitter::generate::value::{GenerateError, GeneratedValue};

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

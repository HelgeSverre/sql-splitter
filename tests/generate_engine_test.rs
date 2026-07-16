//! Tests for stable seed derivation and typed generated values in
//! `sql_splitter::generate`, and for the allocation-lean renderer primitives
//! in `sql_splitter::render`.

use chrono::Datelike;
use rand::Rng;
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use sql_splitter::generate::seed::{derive_seed, SeedRoot, StreamId};
use sql_splitter::generate::value::{GenerateError, GeneratedValue};
use sql_splitter::generate::{
    CompileContext, CompiledModifier, ExtensionRegistry, RowContext, RowView,
};
use sql_splitter::parser::SqlDialect;
use sql_splitter::render::{RandomBlock, RowBatch, SqlString};
use sql_splitter::synthetic::{
    GeneratorConfig, ModifierConfig, PortableColumn, PortableTable, SqlTypeFamily,
};
use std::collections::BTreeMap;

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

// --- Task 11: Phase 1 core generators and modifiers -------------------------

fn yaml(source: &str) -> GeneratorConfig {
    serde_yaml_ng::from_str(source).expect("valid generator config yaml")
}

fn modifier_yaml(source: &str) -> ModifierConfig {
    serde_yaml_ng::from_str(source).expect("valid modifier config yaml")
}

fn portable_column(name: &str, family: SqlTypeFamily, nullable: bool) -> PortableColumn {
    PortableColumn {
        name: name.to_string(),
        source_type: "text".to_string(),
        family,
        nullable,
        primary_key: false,
        unique: false,
        default_sql: None,
        generated: false,
        identity: false,
        collation: None,
    }
}

fn portable_table(name: &str, columns: Vec<PortableColumn>) -> PortableTable {
    PortableTable {
        name: name.to_string(),
        columns,
        primary_key: Vec::new(),
        unique_constraints: Vec::new(),
        check_constraints: Vec::new(),
        indexes: Vec::new(),
        create_statement: None,
        relationships: Vec::new(),
    }
}

/// A [`RowView`] with no sibling values, for generators/modifiers that never
/// read another column.
struct EmptyRow;

impl RowView for EmptyRow {
    fn get(&self, _column: &str) -> Option<&GeneratedValue> {
        None
    }
}

/// A [`RowView`] backed by a fixed map, for exercising `copy`/`template`.
struct StubRow(BTreeMap<String, GeneratedValue>);

impl RowView for StubRow {
    fn get(&self, column: &str) -> Option<&GeneratedValue> {
        self.0.get(column)
    }
}

/// Compile `name` against a single-column table of `family` and generate the
/// first three rows, or the compile/generate error rendered as a string.
fn generate_three(
    name: &str,
    config: GeneratorConfig,
    family: SqlTypeFamily,
    seed: u64,
) -> Result<Vec<GeneratedValue>, String> {
    let registry = ExtensionRegistry::standard();
    let factory = registry
        .generator(name)
        .ok_or_else(|| format!("no generator registered for `{name}`"))?;
    let column = portable_column("value", family, true);
    let table = portable_table("t", vec![column.clone()]);
    let path = "tables.t.columns.value.generator".to_string();
    let context = CompileContext::for_column(&table, &column, SeedRoot::new(seed), &path);
    let mut compiled = factory
        .compile(&config, &context)
        .map_err(|bag| bag.to_string())?;
    let empty = EmptyRow;
    let mut values = Vec::with_capacity(3);
    for row_index in 0..3u64 {
        let row = RowContext::new(row_index, &empty);
        let mut output = GeneratedValue::Null;
        compiled
            .generate(&row, &mut output)
            .map_err(|error| error.to_string())?;
        values.push(output);
    }
    Ok(values)
}

/// Compile a single modifier against a one-column table.
fn compile_modifier(
    kind: &str,
    config: ModifierConfig,
    column: &PortableColumn,
    table: &PortableTable,
    seed: u64,
) -> Result<Box<dyn CompiledModifier>, String> {
    let registry = ExtensionRegistry::standard();
    let factory = registry
        .modifier(kind)
        .ok_or_else(|| format!("no modifier registered for `{kind}`"))?;
    let path = format!("tables.{}.columns.{}.modifiers[0]", table.name, column.name);
    let context = CompileContext::for_column(table, column, SeedRoot::new(seed), &path);
    factory
        .compile(&config, &context)
        .map_err(|bag| bag.to_string())
}

#[test]
fn phase_one_generators_produce_type_safe_values() {
    let cases = [
        (
            "constant",
            yaml("{ kind: constant, value: 7 }"),
            SqlTypeFamily::Integer,
        ),
        (
            "sequence",
            yaml("{ kind: sequence, start: 10, step: 2 }"),
            SqlTypeFamily::Integer,
        ),
        (
            "choice",
            yaml("{ kind: choice, values: [a, b] }"),
            SqlTypeFamily::Text,
        ),
        ("uuid", yaml("{ kind: uuid }"), SqlTypeFamily::Uuid),
        (
            "json_value",
            yaml("{ kind: json_value }"),
            SqlTypeFamily::Json,
        ),
    ];
    for (name, config, family) in cases {
        let values = generate_three(name, config, family.clone(), 42).unwrap();
        assert_eq!(values.len(), 3);
        assert!(values.iter().all(|v| v.compatible_with(&family)));
    }
}

#[test]
fn null_generator_rejects_non_nullable_column() {
    let registry = ExtensionRegistry::standard();
    let factory = registry.generator("null").unwrap();
    let column = portable_column("value", SqlTypeFamily::Text, false);
    let table = portable_table("t", vec![column.clone()]);
    let path = "tables.t.columns.value.generator".to_string();
    let context = CompileContext::for_column(&table, &column, SeedRoot::new(1), &path);
    let err = factory
        .compile(&yaml("{ kind: null }"), &context)
        .err()
        .expect("null on a non-nullable column must fail to compile");
    assert!(err.to_string().contains("GEN-NULL-ON-NON-NULLABLE"));
}

#[test]
fn choice_generator_rejects_empty_values() {
    let err = generate_three(
        "choice",
        yaml("{ kind: choice, values: [] }"),
        SqlTypeFamily::Text,
        1,
    )
    .unwrap_err();
    assert!(err.contains("GEN-CHOICE-EMPTY"));
}

#[test]
fn choice_generator_rejects_a_value_that_does_not_coerce_to_the_column_family() {
    let err = generate_three(
        "choice",
        yaml("{ kind: choice, values: [not-a-number] }"),
        SqlTypeFamily::Integer,
        1,
    )
    .unwrap_err();
    assert!(err.contains("GEN-CHOICE-INVALID-VALUE"));
}

#[test]
fn decimal_generator_respects_bounds_and_scale() {
    let values = generate_three(
        "decimal",
        yaml("{ kind: decimal, min: 1.00, max: 2.00, scale: 2 }"),
        SqlTypeFamily::Decimal,
        7,
    )
    .unwrap();
    for value in values {
        let (minor, scale) = value.as_decimal().unwrap();
        assert_eq!(scale, 2);
        assert!((100..=200).contains(&minor), "minor {minor} out of range");
    }
}

#[test]
fn sequence_generator_reports_overflow_instead_of_wrapping() {
    let registry = ExtensionRegistry::standard();
    let factory = registry.generator("sequence").unwrap();
    let column = portable_column("value", SqlTypeFamily::Integer, true);
    let table = portable_table("t", vec![column.clone()]);
    let path = "tables.t.columns.value.generator".to_string();
    let context = CompileContext::for_column(&table, &column, SeedRoot::new(1), &path);
    // `serde_yaml_ng` cannot parse a bare `i128`-sized integer literal, so
    // pass `start` as a string; `sequence`'s `parse_i128` accepts both.
    let config = yaml(&format!(
        "{{ kind: sequence, start: \"{}\", step: 1 }}",
        i128::MAX - 1
    ));
    let mut compiled = factory.compile(&config, &context).unwrap();
    let empty = EmptyRow;
    let mut output = GeneratedValue::Null;

    // Row 0 emits i128::MAX - 1 and advances the counter to i128::MAX.
    compiled
        .generate(&RowContext::new(0, &empty), &mut output)
        .unwrap();
    assert_eq!(output, GeneratedValue::Integer(i128::MAX - 1));

    // Row 1 emits i128::MAX; advancing past it overflows i128.
    compiled
        .generate(&RowContext::new(1, &empty), &mut output)
        .unwrap();
    assert_eq!(output, GeneratedValue::Integer(i128::MAX));

    // Row 2 has no representable value left to emit.
    let err = compiled
        .generate(&RowContext::new(2, &empty), &mut output)
        .unwrap_err();
    assert!(matches!(err, GenerateError::Overflow(_)));
}

#[test]
fn template_generator_rejects_unknown_sibling_field() {
    let registry = ExtensionRegistry::standard();
    let factory = registry.generator("template").unwrap();
    let column = portable_column("full_name", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![column.clone()]);
    let path = "tables.t.columns.full_name.generator".to_string();
    let context = CompileContext::for_column(&table, &column, SeedRoot::new(1), &path);
    let config = yaml("{ kind: template, parts: [{ field: missing_column }] }");
    let err = factory
        .compile(&config, &context)
        .err()
        .expect("an unknown template field must fail to compile");
    assert!(err.to_string().contains("GEN-TEMPLATE-UNKNOWN-FIELD"));
}

#[test]
fn template_generator_joins_literals_and_sibling_fields() {
    let registry = ExtensionRegistry::standard();
    let factory = registry.generator("template").unwrap();
    let first_name = portable_column("first_name", SqlTypeFamily::Text, true);
    let full_name = portable_column("full_name", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![first_name.clone(), full_name.clone()]);
    let path = "tables.t.columns.full_name.generator".to_string();
    let context = CompileContext::for_column(&table, &full_name, SeedRoot::new(1), &path);
    let config = yaml("{ kind: template, parts: [\"Hello, \", { field: first_name }, \"!\"] }");
    let mut compiled = factory.compile(&config, &context).unwrap();

    let mut siblings = BTreeMap::new();
    siblings.insert(
        "first_name".to_string(),
        GeneratedValue::Text("Ada".to_string()),
    );
    let row = StubRow(siblings);
    let mut output = GeneratedValue::Null;
    compiled
        .generate(&RowContext::new(0, &row), &mut output)
        .unwrap();
    assert_eq!(output, GeneratedValue::Text("Hello, Ada!".to_string()));
}

#[test]
fn unique_modifier_errors_when_exhausted_with_on_exhaustion_error() {
    let column = portable_column("code", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![column.clone()]);
    let config = modifier_yaml("{ kind: unique, max_attempts: 2, on_exhaustion: error }");
    let mut modifier = compile_modifier("unique", config, &column, &table, 1).unwrap();
    let empty = EmptyRow;
    let row = RowContext::new(0, &empty);

    // Under max_attempts = 2, the reachable domain for repeated submissions
    // of "dup" is {"dup", "dup-1", "dup-2"}; the first three each claim one.
    for _ in 0..3 {
        let mut value = GeneratedValue::Text("dup".to_string());
        modifier.apply(&row, &mut value).unwrap();
    }
    // The fourth submission has no unclaimed candidate left.
    let mut value = GeneratedValue::Text("dup".to_string());
    let err = modifier.apply(&row, &mut value).unwrap_err();
    assert!(matches!(err, GenerateError::Exhausted(_)));
}

#[test]
fn unique_modifier_accepts_the_duplicate_on_warn_instead_of_erroring() {
    let column = portable_column("code", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![column.clone()]);
    let config = modifier_yaml("{ kind: unique, max_attempts: 0, on_exhaustion: warn }");
    let mut modifier = compile_modifier("unique", config, &column, &table, 1).unwrap();
    let empty = EmptyRow;
    let row = RowContext::new(0, &empty);

    let mut first = GeneratedValue::Text("dup".to_string());
    modifier.apply(&row, &mut first).unwrap();
    let mut second = GeneratedValue::Text("dup".to_string());
    modifier.apply(&row, &mut second).unwrap();
    assert_eq!(second, GeneratedValue::Text("dup".to_string()));
}

#[test]
fn unique_modifier_rejects_widen_for_families_without_a_mutation_strategy() {
    let column = portable_column("flag", SqlTypeFamily::Boolean, true);
    let table = portable_table("t", vec![column.clone()]);
    let config = modifier_yaml("{ kind: unique, on_exhaustion: widen }");
    let err = compile_modifier("unique", config, &column, &table, 1)
        .err()
        .expect("on_exhaustion: widen on a boolean column must fail to compile");
    assert!(err.contains("GEN-UNIQUE-WIDEN-UNSUPPORTED"));
}

#[test]
fn unique_modifier_errors_when_the_max_tracked_budget_is_exhausted() {
    // Memory must not grow proportionally with total input rows: `unique`
    // remembers claimed values only up to an explicit `max_tracked` budget,
    // never an unbounded set. A tiny budget of 3 lets this test trip that
    // budget deterministically without emitting millions of rows.
    let column = portable_column("code", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![column.clone()]);
    let config =
        modifier_yaml("{ kind: unique, max_attempts: 0, max_tracked: 3, on_exhaustion: error }");
    let mut modifier = compile_modifier("unique", config, &column, &table, 1).unwrap();
    let empty = EmptyRow;
    let row = RowContext::new(0, &empty);

    // Three distinct values exactly fill the tracking budget.
    for text in ["a", "b", "c"] {
        let mut value = GeneratedValue::Text(text.to_string());
        modifier.apply(&row, &mut value).unwrap();
    }
    // A fourth, never-before-seen value has no room left to be tracked, even
    // though it is not itself a duplicate.
    let mut value = GeneratedValue::Text("d".to_string());
    let err = modifier.apply(&row, &mut value).unwrap_err();
    assert!(matches!(err, GenerateError::Exhausted(_)));
    assert!(err.to_string().contains("max_tracked"));
}

#[test]
fn unique_modifier_accepts_the_value_on_warn_when_the_max_tracked_budget_is_exhausted() {
    let column = portable_column("code", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![column.clone()]);
    let config =
        modifier_yaml("{ kind: unique, max_attempts: 0, max_tracked: 3, on_exhaustion: warn }");
    let mut modifier = compile_modifier("unique", config, &column, &table, 1).unwrap();
    let empty = EmptyRow;
    let row = RowContext::new(0, &empty);

    for text in ["a", "b", "c"] {
        let mut value = GeneratedValue::Text(text.to_string());
        modifier.apply(&row, &mut value).unwrap();
    }
    // The budget is exhausted, but `warn` passes the value through instead
    // of erroring.
    let mut value = GeneratedValue::Text("d".to_string());
    modifier.apply(&row, &mut value).unwrap();
    assert_eq!(value, GeneratedValue::Text("d".to_string()));
}

#[test]
fn modifiers_apply_in_the_configured_order() {
    let column = portable_column("code", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![column.clone()]);
    let empty = EmptyRow;
    let row = RowContext::new(0, &empty);

    let suffix_then_truncate = {
        let mut suffix = compile_modifier(
            "suffix",
            modifier_yaml("{ kind: suffix, value: XYZ }"),
            &column,
            &table,
            1,
        )
        .unwrap();
        let mut truncate = compile_modifier(
            "truncate",
            modifier_yaml("{ kind: truncate, max_length: 5 }"),
            &column,
            &table,
            1,
        )
        .unwrap();
        let mut value = GeneratedValue::Text("abcde".to_string());
        suffix.apply(&row, &mut value).unwrap();
        truncate.apply(&row, &mut value).unwrap();
        value
    };

    let truncate_then_suffix = {
        let mut truncate = compile_modifier(
            "truncate",
            modifier_yaml("{ kind: truncate, max_length: 5 }"),
            &column,
            &table,
            1,
        )
        .unwrap();
        let mut suffix = compile_modifier(
            "suffix",
            modifier_yaml("{ kind: suffix, value: XYZ }"),
            &column,
            &table,
            1,
        )
        .unwrap();
        let mut value = GeneratedValue::Text("abcde".to_string());
        truncate.apply(&row, &mut value).unwrap();
        suffix.apply(&row, &mut value).unwrap();
        value
    };

    // Suffix-then-truncate discards the suffix entirely; truncate-then-suffix
    // keeps it. Order is observable, exactly as the pipeline promises.
    assert_eq!(
        suffix_then_truncate,
        GeneratedValue::Text("abcde".to_string())
    );
    assert_eq!(
        truncate_then_suffix,
        GeneratedValue::Text("abcdeXYZ".to_string())
    );
    assert_ne!(suffix_then_truncate, truncate_then_suffix);
}

#[test]
fn weighted_choice_rejects_all_zero_weights() {
    let err = generate_three(
        "weighted_choice",
        yaml("{ kind: weighted_choice, choices: [{ value: a, weight: 0 }, { value: b, weight: 0 }] }"),
        SqlTypeFamily::Text,
        1,
    )
    .unwrap_err();
    assert!(err.contains("GEN-WEIGHTED-CHOICE-ALL-ZERO"));
}

#[test]
fn weighted_choice_rejects_a_negative_weight() {
    let err = generate_three(
        "weighted_choice",
        yaml("{ kind: weighted_choice, choices: [{ value: a, weight: -1 }, { value: b, weight: 1 }] }"),
        SqlTypeFamily::Text,
        1,
    )
    .unwrap_err();
    assert!(err.contains("GEN-WEIGHTED-CHOICE-INVALID-WEIGHT"));
}

#[test]
fn weighted_choice_rejects_a_nan_weight() {
    let err = generate_three(
        "weighted_choice",
        yaml("{ kind: weighted_choice, choices: [{ value: a, weight: .nan }, { value: b, weight: 1 }] }"),
        SqlTypeFamily::Text,
        1,
    )
    .unwrap_err();
    assert!(err.contains("GEN-WEIGHTED-CHOICE-INVALID-WEIGHT"));
}

#[test]
fn weighted_choice_rejects_an_infinite_weight() {
    let err = generate_three(
        "weighted_choice",
        yaml("{ kind: weighted_choice, choices: [{ value: a, weight: .inf }, { value: b, weight: 1 }] }"),
        SqlTypeFamily::Text,
        1,
    )
    .unwrap_err();
    assert!(err.contains("GEN-WEIGHTED-CHOICE-INVALID-WEIGHT"));
}

#[test]
fn null_rate_modifier_requires_a_rate_argument() {
    let column = portable_column("note", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![column.clone()]);
    let err = compile_modifier(
        "null_rate",
        modifier_yaml("{ kind: null_rate }"),
        &column,
        &table,
        1,
    )
    .err()
    .expect("null_rate with no rate must fail to compile, not silently no-op");
    assert!(err.contains("GEN-NULL-RATE-MISSING-RATE"));
}

#[test]
fn boolean_generator_stays_within_a_tolerant_band_of_its_configured_probability() {
    let registry = ExtensionRegistry::standard();
    let factory = registry.generator("boolean").unwrap();
    let column = portable_column("flag", SqlTypeFamily::Boolean, true);
    let table = portable_table("t", vec![column.clone()]);
    let path = "tables.t.columns.flag.generator".to_string();
    let context = CompileContext::for_column(&table, &column, SeedRoot::new(99), &path);
    let config = yaml("{ kind: boolean, probability: 0.25 }");
    let mut compiled = factory.compile(&config, &context).unwrap();
    let empty = EmptyRow;

    let mut trues = 0u32;
    for i in 0..10_000u64 {
        let row = RowContext::new(i, &empty);
        let mut output = GeneratedValue::Null;
        compiled.generate(&row, &mut output).unwrap();
        if output.as_boolean().unwrap() {
            trues += 1;
        }
    }
    // A tolerant band around the configured p = 0.25, not an exact count:
    // this is a statistical smoke test, not a determinism test.
    let rate = f64::from(trues) / 10_000.0;
    assert!((0.22..0.28).contains(&rate), "observed rate {rate}");
}

#[test]
fn weighted_choice_majority_value_stays_within_a_tolerant_band() {
    let registry = ExtensionRegistry::standard();
    let factory = registry.generator("weighted_choice").unwrap();
    let column = portable_column("tier", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![column.clone()]);
    let path = "tables.t.columns.tier.generator".to_string();
    let context = CompileContext::for_column(&table, &column, SeedRoot::new(123), &path);
    let config = yaml(
        "{ kind: weighted_choice, choices: [{ value: common, weight: 9 }, { value: rare, weight: 1 }] }",
    );
    let mut compiled = factory.compile(&config, &context).unwrap();
    let empty = EmptyRow;

    let mut common = 0u32;
    for i in 0..10_000u64 {
        let row = RowContext::new(i, &empty);
        let mut output = GeneratedValue::Null;
        compiled.generate(&row, &mut output).unwrap();
        if output.as_text().unwrap() == "common" {
            common += 1;
        }
    }
    // 9:1 weights -> tolerant band around 0.9, not an exact count.
    let rate = f64::from(common) / 10_000.0;
    assert!((0.86..0.94).contains(&rate), "observed rate {rate}");
}

#[test]
fn same_seed_reproduces_the_same_generator_sequence() {
    let config = yaml("{ kind: string, min_length: 6, max_length: 6 }");
    let column = portable_column("code", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![column.clone()]);

    let run = || {
        let registry = ExtensionRegistry::standard();
        let factory = registry.generator("string").unwrap();
        let path = "tables.t.columns.code.generator".to_string();
        let context = CompileContext::for_column(&table, &column, SeedRoot::new(55), &path);
        let mut compiled = factory.compile(&config, &context).unwrap();
        let empty = EmptyRow;
        let mut values = Vec::new();
        for i in 0..5u64 {
            let row = RowContext::new(i, &empty);
            let mut output = GeneratedValue::Null;
            compiled.generate(&row, &mut output).unwrap();
            values.push(output);
        }
        values
    };

    assert_eq!(run(), run());
}

// --- Task 12: semantic, credential, and temporal generators -----------------

/// Compile `kind` with `config` against a single `Text`-family column and
/// generate `count` rows, returning each row's text. Every Task 12 catalog
/// generator emits `Text` when its column's family is `Text` (numeric- and
/// timestamp-shaped generators fall back to a formatted string), so this one
/// helper covers the whole catalog for shape assertions.
fn generate_text_with(kind: &str, seed: u64, count: usize, config: GeneratorConfig) -> Vec<String> {
    let registry = ExtensionRegistry::standard();
    let factory = registry
        .generator(kind)
        .unwrap_or_else(|| panic!("no generator registered for `{kind}`"));
    let column = portable_column("value", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![column.clone()]);
    let path = "tables.t.columns.value.generator".to_string();
    let context = CompileContext::for_column(&table, &column, SeedRoot::new(seed), &path);
    let mut compiled = factory
        .compile(&config, &context)
        .unwrap_or_else(|bag| panic!("`{kind}` failed to compile: {bag}"));
    let empty = EmptyRow;
    (0..count as u64)
        .map(|i| {
            let row = RowContext::new(i, &empty);
            let mut output = GeneratedValue::Null;
            compiled
                .generate(&row, &mut output)
                .unwrap_or_else(|error| panic!("`{kind}` failed to generate: {error}"));
            output
                .as_text()
                .unwrap_or_else(|_| panic!("`{kind}` did not emit Text"))
                .to_string()
        })
        .collect()
}

/// [`generate_text_with`] with `{ kind: <kind> }` as the config — every
/// argument left at its default.
fn generate_text(kind: &str, seed: u64, count: usize) -> Vec<String> {
    generate_text_with(kind, seed, count, yaml(&format!("{{ kind: {kind} }}")))
}

#[test]
fn semantic_generators_are_seeded_and_shape_valid() {
    let a = generate_text("internet.email", 42, 20);
    let b = generate_text("internet.email", 42, 20);
    assert_eq!(a, b);
    assert!(a.iter().all(|value| value.contains('@')));

    let token = generate_text_with(
        "credential.token",
        42,
        1,
        yaml("{ kind: credential.token, length: 64, alphabet: alphanumeric }"),
    );
    assert_eq!(token[0].len(), 64);
    assert!(token[0].chars().all(|c| c.is_ascii_alphanumeric()));
}

/// `(kind, shape predicate)` pairs for
/// [`every_catalog_family_has_a_shape_valid_representative`].
type CatalogShapeCase = (&'static str, fn(&str) -> bool);

#[test]
fn every_catalog_family_has_a_shape_valid_representative() {
    // One assertion per Phase 1 catalog family from the task brief, proving
    // every family is registered and produces a plausible shape.
    let cases: &[CatalogShapeCase] = &[
        ("person.full_name", |v| v.contains(' ')),
        ("internet.email", |v| v.contains('@')),
        ("phone.number", |v| !v.is_empty()),
        ("company.name", |v| !v.is_empty()),
        ("address.city", |v| !v.is_empty()),
        ("commerce.product_name", |v| v.contains(' ')),
        ("text.word", |v| !v.is_empty()),
        ("identifier.ulid", |v| v.len() == 26),
        ("file.name", |v| !v.is_empty()),
        ("network.mac", |v| v.contains(':')),
        ("credential.password_hash", |v| {
            v.starts_with("$synthetic$") && v.len() == "$synthetic$".len() + 64
        }),
        ("date", |v| v.len() == 10 && v.matches('-').count() == 2),
    ];
    for (kind, shape_ok) in cases {
        let values = generate_text(kind, 7, 3);
        assert_eq!(values.len(), 3, "`{kind}` did not produce 3 rows");
        for value in &values {
            assert!(
                shape_ok(value),
                "`{kind}` produced an unexpected shape: {value:?}"
            );
        }
    }
}

#[test]
fn unsupported_locale_is_a_compile_failure_not_a_runtime_one() {
    // `sql_splitter::fake_data::Locale` is a closed, crate-private enum with
    // a single `En` variant today; naming any other locale (`Locale::De`,
    // etc.) does not compile. There is nothing to assert at the integration-
    // test level for a crate-private type, so this test documents the
    // property: every semantic generator kind resolves and generates without
    // ever accepting a locale argument that could name an unsupported value.
    let values = generate_text("person.first_name", 1, 1);
    assert_eq!(values.len(), 1);
}

#[test]
fn credential_placeholder_is_unmistakably_not_a_valid_private_key() {
    let values = generate_text("credential.placeholder", 1, 3);
    for value in values {
        assert!(
            !value.contains("-----BEGIN"),
            "looks like a real PEM header: {value}"
        );
        assert!(
            !value.contains("-----END"),
            "looks like a real PEM footer: {value}"
        );
        assert!(
            value.to_uppercase().contains("PLACEHOLDER")
                || value.to_uppercase().contains("SYNTHETIC"),
            "placeholder does not self-identify as synthetic: {value}"
        );
    }
}

#[test]
fn date_and_datetime_generators_stay_within_bounds() {
    for value in generate_text("date", 3, 50) {
        let parsed = chrono::NaiveDate::parse_from_str(&value, "%Y-%m-%d")
            .unwrap_or_else(|_| panic!("`date` produced an unparsable value: {value}"));
        assert!(parsed.year() >= 1970 && parsed.year() <= 2035);
    }
    for value in generate_text("datetime", 3, 50) {
        chrono::NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
            .unwrap_or_else(|_| panic!("`datetime` produced an unparsable value: {value}"));
    }
    for value in generate_text("time", 3, 50) {
        chrono::NaiveTime::parse_from_str(&value, "%H:%M:%S")
            .unwrap_or_else(|_| panic!("`time` produced an unparsable value: {value}"));
    }
}

/// Compile `kind` (`before` or `after`) against a two-column table — a
/// `source` timestamp column and the relative column under test — and
/// generate one row, reading `source` from a fixed [`StubRow`].
fn generate_relative(kind: &str, config: GeneratorConfig, source_value: &str, seed: u64) -> String {
    let registry = ExtensionRegistry::standard();
    let factory = registry
        .generator(kind)
        .unwrap_or_else(|| panic!("no generator registered for `{kind}`"));
    let source = portable_column("created_at", SqlTypeFamily::Text, true);
    let relative = portable_column("value", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![source.clone(), relative.clone()]);
    let path = "tables.t.columns.value.generator".to_string();
    let context = CompileContext::for_column(&table, &relative, SeedRoot::new(seed), &path);
    let mut compiled = factory
        .compile(&config, &context)
        .unwrap_or_else(|bag| panic!("`{kind}` failed to compile: {bag}"));
    let mut siblings = BTreeMap::new();
    siblings.insert(
        "created_at".to_string(),
        GeneratedValue::Text(source_value.to_string()),
    );
    let row = StubRow(siblings);
    let mut output = GeneratedValue::Null;
    compiled
        .generate(&RowContext::new(0, &row), &mut output)
        .unwrap_or_else(|error| panic!("`{kind}` failed to generate: {error}"));
    output.as_text().unwrap().to_string()
}

#[test]
fn after_generates_a_timestamp_at_or_past_its_source() {
    let source = "2024-01-15 10:00:00";
    let source_ts = chrono::NaiveDateTime::parse_from_str(source, "%Y-%m-%d %H:%M:%S").unwrap();
    for seed in 0..10u64 {
        let value = generate_relative(
            "after",
            yaml("{ kind: after, source: created_at, min_seconds: 1, max_seconds: 1000 }"),
            source,
            seed,
        );
        let ts = chrono::NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S").unwrap();
        assert!(
            ts >= source_ts,
            "`after` value {ts} is not >= source {source_ts}"
        );
    }
}

#[test]
fn before_generates_a_timestamp_at_or_before_its_source() {
    let source = "2024-01-15 10:00:00";
    let source_ts = chrono::NaiveDateTime::parse_from_str(source, "%Y-%m-%d %H:%M:%S").unwrap();
    for seed in 0..10u64 {
        let value = generate_relative(
            "before",
            yaml("{ kind: before, source: created_at, min_seconds: 1, max_seconds: 1000 }"),
            source,
            seed,
        );
        let ts = chrono::NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S").unwrap();
        assert!(
            ts <= source_ts,
            "`before` value {ts} is not <= source {source_ts}"
        );
    }
}

#[test]
fn relative_generator_requires_a_declared_source_column() {
    let registry = ExtensionRegistry::standard();
    let factory = registry.generator("after").unwrap();
    let column = portable_column("value", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![column.clone()]);
    let path = "tables.t.columns.value.generator".to_string();
    let context = CompileContext::for_column(&table, &column, SeedRoot::new(1), &path);
    let err = factory
        .compile(&yaml("{ kind: after }"), &context)
        .err()
        .expect("`after` with no `source` must fail to compile");
    assert!(err.to_string().contains("GEN-RELATIVE-MISSING-SOURCE"));
}

#[test]
fn relative_generator_rejects_an_unknown_source_column() {
    let registry = ExtensionRegistry::standard();
    let factory = registry.generator("before").unwrap();
    let column = portable_column("value", SqlTypeFamily::Text, true);
    let table = portable_table("t", vec![column.clone()]);
    let path = "tables.t.columns.value.generator".to_string();
    let context = CompileContext::for_column(&table, &column, SeedRoot::new(1), &path);
    let err = factory
        .compile(&yaml("{ kind: before, source: missing_column }"), &context)
        .err()
        .expect("`before` referencing an unknown column must fail to compile");
    assert!(err.to_string().contains("GEN-RELATIVE-UNKNOWN-SOURCE"));
}

#[test]
fn descriptors_declare_the_families_they_accept() {
    let registry = ExtensionRegistry::standard();
    let email = registry.generator("internet.email").unwrap().descriptor();
    assert!(email.accepts.contains(&SqlTypeFamily::Text));

    let latitude = registry.generator("address.latitude").unwrap().descriptor();
    assert!(latitude.accepts.contains(&SqlTypeFamily::Decimal));
    assert!(latitude.accepts.contains(&SqlTypeFamily::Text));

    let port = registry.generator("network.port").unwrap().descriptor();
    assert!(port.accepts.contains(&SqlTypeFamily::Integer));
}

/// Every catalog kind the Task 12 brief lists must be registered — a
/// regression here means a kind silently dropped out of `register_all`.
#[test]
fn every_brief_catalog_kind_is_registered() {
    const EXPECTED: &[&str] = &[
        "person.first_name",
        "person.last_name",
        "person.full_name",
        "person.username",
        "person.title",
        "internet.email",
        "internet.domain",
        "internet.url",
        "internet.ipv4",
        "internet.ipv6",
        "internet.user_agent",
        "phone.number",
        "phone.country_code",
        "company.name",
        "company.department",
        "company.job_title",
        "address.line1",
        "address.line2",
        "address.city",
        "address.region",
        "address.postcode",
        "address.country",
        "address.latitude",
        "address.longitude",
        "commerce.product_name",
        "commerce.sku",
        "commerce.currency",
        "commerce.money",
        "commerce.quantity",
        "text.word",
        "text.sentence",
        "text.paragraph",
        "text.slug",
        "identifier.ulid",
        "identifier.nanoid",
        "identifier.token",
        "identifier.hash",
        "file.name",
        "file.extension",
        "file.mime_type",
        "file.size",
        "network.mac",
        "network.port",
        "credential.password_hash",
        "credential.token",
        "credential.api_key",
        "credential.secret",
        "credential.placeholder",
        "date",
        "time",
        "datetime",
        "duration",
        "before",
        "after",
    ];
    let registry = ExtensionRegistry::standard();
    for kind in EXPECTED {
        assert!(
            registry.generator(kind).is_some(),
            "`{kind}` from the Task 12 catalog is not registered"
        );
    }
    assert_eq!(
        EXPECTED.len(),
        54,
        "the brief's catalog has exactly 54 kinds"
    );
}

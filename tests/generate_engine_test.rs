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

// --- Core generators and modifiers ------------------------------------------

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
fn observed_numeric_generators_reject_out_of_range_decimal_scale() {
    // scale > 18 makes 10i128.pow(scale) overflow at render; the observed
    // normal/lognormal/histogram factories must reject it at compile time, like
    // the core decimal generator.
    let cases: [(&str, GeneratorConfig); 3] = [
        ("normal", yaml("{ kind: normal, mean: 0, std: 1, scale: 39 }")),
        (
            "lognormal",
            yaml("{ kind: lognormal, mu: 0, sigma: 1, scale: 39 }"),
        ),
        (
            "histogram",
            yaml("{ kind: histogram, bins: [{ min: 0, max: 10, count: 1 }], scale: 39 }"),
        ),
    ];
    for (name, config) in cases {
        let err = generate_three(name, config, SqlTypeFamily::Decimal, 42)
            .expect_err(&format!("{name} scale 39 must be a compile error"));
        assert!(
            err.to_lowercase().contains("scale"),
            "{name}: expected a scale diagnostic, got: {err}"
        );
    }
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

// --- Semantic, credential, and temporal generators --------------------------

/// Compile `kind` with `config` against a single `Text`-family column and
/// generate `count` rows, returning each row's text. Every semantic catalog
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
    // One assertion per semantic generator catalog family, proving
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

/// Every semantic catalog kind must be registered — a
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
            "`{kind}` from the semantic generator catalog is not registered"
        );
    }
    assert_eq!(
        EXPECTED.len(),
        54,
        "the brief's catalog has exactly 54 kinds"
    );
}

// --- Relational execution ---------------------------------------------------

use sql_splitter::generate::{
    CompileOptions, GeneratedRow, GenerationEngine, GenerationPlan, ModelCompiler, PlannedTable,
    RowSink,
};
use sql_splitter::synthetic::SyntheticFile;
use std::collections::HashSet;

/// A [`RowSink`] that records table order and every generated row, so tests can
/// assert on referential integrity and per-column value sets.
#[derive(Default)]
struct CollectingSink {
    order: Vec<String>,
    columns: BTreeMap<String, Vec<String>>,
    rows: BTreeMap<String, Vec<Vec<GeneratedValue>>>,
}

impl RowSink for CollectingSink {
    fn begin_table(&mut self, table: &PlannedTable) -> Result<(), GenerateError> {
        self.order.push(table.name.clone());
        self.columns.insert(
            table.name.clone(),
            table
                .columns
                .iter()
                .map(|c| c.schema.name.clone())
                .collect(),
        );
        self.rows.insert(table.name.clone(), Vec::new());
        Ok(())
    }

    fn write_row(&mut self, table: &PlannedTable, row: &GeneratedRow) -> Result<(), GenerateError> {
        self.rows
            .get_mut(&table.name)
            .expect("table was begun before rows were written")
            .push(row.values.clone());
        Ok(())
    }

    fn end_table(&mut self, _table: &PlannedTable) -> Result<(), GenerateError> {
        Ok(())
    }
}

impl CollectingSink {
    fn table_order(&self) -> Vec<&str> {
        self.order.iter().map(String::as_str).collect()
    }

    fn column_index(&self, table: &str, column: &str) -> usize {
        self.columns[table]
            .iter()
            .position(|c| c == column)
            .unwrap_or_else(|| panic!("no column `{column}` in table `{table}`"))
    }

    fn values<'a>(
        &'a self,
        table: &str,
        column: &str,
    ) -> impl Iterator<Item = &'a GeneratedValue> + 'a {
        let idx = self.column_index(table, column);
        self.rows[table].iter().map(move |row| &row[idx])
    }

    fn integers(&self, table: &str, column: &str) -> Vec<i128> {
        self.values(table, column)
            .map(|v| v.as_integer().expect("integer column"))
            .collect()
    }
}

/// Parse and compile a model to a plan against the standard registry.
fn compile(model_yaml: &str) -> GenerationPlan {
    let model = SyntheticFile::parse_str(model_yaml)
        .expect("valid model YAML")
        .into_model()
        .expect("document is a model");
    ModelCompiler::standard()
        .compile(model, CompileOptions::default())
        .expect("model compiles cleanly")
}

fn empty_parent_model(child_nullable: bool) -> String {
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: disabled }}
seed: 7
tables:
  parents:
    rows: {{ kind: fixed, count: 0 }}
    schema:
      name: parents
      primary_key: [id]
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
    columns:
      id: {{ generator: {{ kind: sequence, start: 1 }} }}
  children:
    rows: {{ kind: fixed, count: 1 }}
    schema:
      name: children
      primary_key: [id]
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: parent_id, type: bigint, nullable: {child_nullable} }}
      relationships:
        - {{ name: children_parent, columns: [parent_id], referenced_table: parents, referenced_columns: [id] }}
    columns:
      id: {{ generator: {{ kind: sequence, start: 1 }} }}
      parent_id: {{ generator: {{ kind: relation.foreign_key, relationship: children_parent }} }}
    relationships:
      - {{ name: children_parent, columns: [parent_id], references: {{ table: parents, columns: [id] }} }}
"#
    )
}

#[test]
fn required_child_rows_against_an_empty_parent_are_rejected() {
    let model = SyntheticFile::parse_str(&empty_parent_model(false))
        .unwrap()
        .into_model()
        .unwrap();
    let error = ModelCompiler::standard()
        .compile(model, CompileOptions::default())
        .expect_err("a required foreign key cannot target an empty parent domain");

    assert!(error.has_code("GEN-FOREIGN-KEY-UNRESOLVED"));
}

#[test]
fn nullable_child_rows_against_an_empty_parent_receive_null() {
    let plan = compile(&empty_parent_model(true));
    let mut sink = CollectingSink::default();
    GenerationEngine::new(plan).run(&mut sink).unwrap();

    assert!(matches!(
        sink.values("children", "parent_id").next(),
        Some(GeneratedValue::Null)
    ));
}

#[test]
fn partially_nullable_composite_fk_against_an_empty_parent_is_rejected() {
    let model = SyntheticFile::parse_str(
        r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 7
tables:
  cells:
    rows: { kind: fixed, count: 0 }
    schema:
      name: cells
      primary_key: [x, y]
      columns:
        - { name: x, type: bigint, nullable: false, primary_key: true }
        - { name: y, type: bigint, nullable: false, primary_key: true }
  readings:
    rows: { kind: fixed, count: 1 }
    schema:
      name: readings
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: cell_x, type: bigint, nullable: true }
        - { name: cell_y, type: bigint, nullable: false }
    relationships:
      - { columns: [cell_x, cell_y], references: { table: cells, columns: [x, y] } }
"#,
    )
    .unwrap()
    .into_model()
    .unwrap();
    let error = ModelCompiler::standard()
        .compile(model, CompileOptions::default())
        .expect_err("every component must be nullable to represent an absent parent");

    assert!(error.has_code("GEN-FOREIGN-KEY-UNRESOLVED"));
}

/// A `customers` (10 rows) → `orders` (fan-out 4 = 40 rows) model whose FK is a
/// generation relationship carrying an optional assignment `distribution`.
fn customers_orders(seed: u64, distribution: Option<&str>) -> String {
    let dist = distribution
        .map(|d| format!(", distribution: {d}"))
        .unwrap_or_default();
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: schema }}
seed: {seed}
tables:
  customers:
    rows: {{ kind: fixed, count: 10 }}
    schema:
      name: customers
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
  orders:
    rows:
      kind: relation.children
      parent: customers
      count: 40
      distribution: {{ kind: fixed, mean: 4.0, min: 1.0, max: 1000000.0 }}
    schema:
      name: orders
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: customer_id, type: bigint, nullable: false }}
    relationships:
      - {{ columns: [customer_id], references: {{ table: customers, columns: [id] }}{dist} }}
"#
    )
}

fn run_customer_ids(seed: u64, distribution: Option<&str>) -> Vec<i128> {
    let plan = compile(&customers_orders(seed, distribution));
    let mut sink = CollectingSink::default();
    GenerationEngine::new(plan).run(&mut sink).unwrap();
    sink.integers("orders", "customer_id")
}

#[test]
fn engine_generates_parent_before_child_with_valid_foreign_keys() {
    let plan = compile(&customers_orders(7, None));
    let mut sink = CollectingSink::default();
    let report = GenerationEngine::new(plan).run(&mut sink).unwrap();

    assert_eq!(report.rows_written, 50);
    assert_eq!(sink.table_order(), ["customers", "orders"]);

    let customer_ids: HashSet<i128> = sink.integers("customers", "id").into_iter().collect();
    assert_eq!(customer_ids.len(), 10);
    assert!(sink
        .integers("orders", "customer_id")
        .iter()
        .all(|id| customer_ids.contains(id)));
}

#[test]
fn sequential_distribution_assigns_parent_by_child_row_modulo_parent_count() {
    let ids = run_customer_ids(7, Some("sequential"));
    assert_eq!(ids.len(), 40);
    for (row, id) in ids.iter().enumerate() {
        // Dense parent ids are 1..=10, so row r references parent (r % 10) + 1.
        assert_eq!(*id, 1 + (row as i128 % 10));
    }
}

#[test]
fn uniform_distribution_produces_valid_and_reproducible_keys() {
    let a = run_customer_ids(7, Some("uniform"));
    let b = run_customer_ids(7, Some("uniform"));
    assert_eq!(a, b, "same seed must reproduce the same assignment");
    assert!(a.iter().all(|id| (1..=10).contains(id)));
    // Uniform spreads across parents rather than collapsing to one.
    assert!(a.iter().any(|id| *id != a[0]));
}

#[test]
fn weighted_distribution_is_valid_reproducible_and_skewed() {
    let a = run_customer_ids(7, Some("weighted"));
    let b = run_customer_ids(7, Some("weighted"));
    assert_eq!(
        a, b,
        "same seed must reproduce the same weighted assignment"
    );
    assert!(a.iter().all(|id| (1..=10).contains(id)));

    // A bounded histogram concentrates mass: the busiest parent takes far more
    // than the uniform expectation of 4 of the 40 children.
    let mut counts = [0usize; 11];
    for id in &a {
        counts[*id as usize] += 1;
    }
    let max = *counts.iter().max().unwrap();
    assert!(max >= 8, "weighted assignment is not skewed: {counts:?}");
}

#[test]
fn composite_key_selection_is_atomic_across_components() {
    let model = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 7
tables:
  cells:
    rows: { kind: fixed, count: 6 }
    schema:
      name: cells
      columns:
        - { name: x, type: bigint, nullable: false, primary_key: true }
        - { name: y, type: bigint, nullable: false, primary_key: true }
  readings:
    rows:
      kind: relation.children
      parent: cells
      count: 18
      distribution: { kind: fixed, mean: 3.0, min: 1.0, max: 1000000.0 }
    schema:
      name: readings
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: cell_x, type: bigint, nullable: false }
        - { name: cell_y, type: bigint, nullable: false }
    relationships:
      - { columns: [cell_x, cell_y], references: { table: cells, columns: [x, y] } }
"#;
    let plan = compile(model);
    let mut sink = CollectingSink::default();
    GenerationEngine::new(plan).run(&mut sink).unwrap();

    let parent_pairs: HashSet<(i128, i128)> = sink
        .integers("cells", "x")
        .into_iter()
        .zip(sink.integers("cells", "y"))
        .collect();
    let child_x = sink.integers("readings", "cell_x");
    let child_y = sink.integers("readings", "cell_y");
    assert_eq!(child_x.len(), 18);
    for (x, y) in child_x.iter().zip(&child_y) {
        // Both components come from one chosen parent row, so the pair always
        // exists among the parents. Independent per-component choice would break
        // this because the parent pairs are the diagonal (x == y).
        assert!(
            parent_pairs.contains(&(*x, *y)),
            "child pair ({x}, {y}) is not a parent row"
        );
        assert_eq!(x, y, "composite components must share one parent row index");
    }
}

#[test]
fn nullable_foreign_key_mixes_nulls_and_valid_keys() {
    let model = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 7
tables:
  customers:
    rows: { kind: fixed, count: 10 }
    schema:
      name: customers
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
  orders:
    rows:
      kind: relation.children
      parent: customers
      count: 40
      distribution: { kind: fixed, mean: 4.0, min: 1.0, max: 1000000.0 }
    schema:
      name: orders
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: customer_id, type: bigint, nullable: true }
    columns:
      customer_id:
        generator: { kind: relation.foreign_key, relationship: fk_cust, null_rate: 0.5 }
    relationships:
      - { name: fk_cust, columns: [customer_id], references: { table: customers, columns: [id] } }
"#;
    let plan = compile(model);
    let mut sink = CollectingSink::default();
    GenerationEngine::new(plan).run(&mut sink).unwrap();

    let values: Vec<&GeneratedValue> = sink.values("orders", "customer_id").collect();
    let nulls = values.iter().filter(|v| v.is_null()).count();
    assert!(
        nulls > 0 && nulls < values.len(),
        "expected a mix of nulls and keys, got {nulls} nulls of {}",
        values.len()
    );
    assert!(values
        .iter()
        .filter(|v| !v.is_null())
        .all(|v| (1..=10).contains(&v.as_integer().unwrap())));
}

#[test]
fn same_seed_reproduces_and_different_seed_diverges() {
    assert_eq!(
        run_customer_ids(7, Some("uniform")),
        run_customer_ids(7, Some("uniform"))
    );
    assert_ne!(
        run_customer_ids(7, Some("uniform")),
        run_customer_ids(9, Some("uniform"))
    );
}

// --- Column modifiers execute end-to-end ------------------------------------

/// A single `metrics` table whose `score` column carries `generator` +
/// `modifiers`, so the engine's owner→modifier→sink pipeline is observable.
fn metrics_model(seed: u64, generator: &str, modifiers: &str) -> String {
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: schema }}
seed: {seed}
tables:
  metrics:
    rows: {{ kind: fixed, count: 50 }}
    schema:
      name: metrics
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: score, type: {score_type}, nullable: true }}
    columns:
      score:
        generator: {generator}
        modifiers: {modifiers}
"#,
        score_type = if generator.contains("integer") {
            "bigint"
        } else {
            "text"
        }
    )
}

fn run_metrics(seed: u64, generator: &str, modifiers: &str) -> Vec<GeneratedValue> {
    let plan = compile(&metrics_model(seed, generator, modifiers));
    let mut sink = CollectingSink::default();
    GenerationEngine::new(plan).run(&mut sink).unwrap();
    sink.values("metrics", "score").cloned().collect()
}

#[test]
fn clamp_modifier_bounds_every_generated_value() {
    // A wide random range clamped to [0, 100]: every emitted value stays in
    // range, which only holds if the modifier actually runs.
    let scores = run_metrics(
        7,
        "{ kind: integer, min: -100000, max: 100000 }",
        "[{ kind: clamp, min: 0, max: 100 }]",
    );
    assert_eq!(scores.len(), 50);
    for value in &scores {
        let n = value.as_integer().unwrap();
        assert!((0..=100).contains(&n), "value {n} escaped the clamp");
    }
}

#[test]
fn null_rate_modifier_introduces_reproducible_nulls() {
    let modifiers = "[{ kind: null_rate, rate: 0.5 }]";
    let a = run_metrics(7, "{ kind: integer, min: 1, max: 9 }", modifiers);
    let b = run_metrics(7, "{ kind: integer, min: 1, max: 9 }", modifiers);
    // Same seed reproduces the same null positions exactly.
    assert_eq!(a, b);
    let nulls = a.iter().filter(|v| v.is_null()).count();
    assert!(
        nulls > 0 && nulls < a.len(),
        "expected a moderate mix of nulls, got {nulls} of {}",
        a.len()
    );
}

#[test]
fn modifier_pipeline_applies_in_declared_order_through_the_engine() {
    // suffix-then-truncate discards the suffix; truncate-then-suffix keeps it.
    let suffix_then_truncate = run_metrics(
        1,
        "{ kind: constant, value: abcde }",
        "[{ kind: suffix, value: XYZ }, { kind: truncate, max_length: 5 }]",
    );
    let truncate_then_suffix = run_metrics(
        1,
        "{ kind: constant, value: abcde }",
        "[{ kind: truncate, max_length: 5 }, { kind: suffix, value: XYZ }]",
    );
    assert!(suffix_then_truncate
        .iter()
        .all(|v| v.as_text().unwrap() == "abcde"));
    assert!(truncate_then_suffix
        .iter()
        .all(|v| v.as_text().unwrap() == "abcdeXYZ"));
}

// --- Random-access (UUID) parent key domains --------------------------------

#[test]
fn uuid_parent_key_children_reference_real_generated_ids() {
    let model = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 7
tables:
  customers:
    rows: { kind: fixed, count: 10 }
    schema:
      name: customers
      columns:
        - { name: id, type: uuid, nullable: false, primary_key: true }
    columns:
      id:
        generator: { kind: uuid }
  orders:
    rows:
      kind: relation.children
      parent: customers
      count: 40
      distribution: { kind: fixed, mean: 4.0, min: 1.0, max: 1000000.0 }
    schema:
      name: orders
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: customer_id, type: uuid, nullable: false }
    relationships:
      - { columns: [customer_id], references: { table: customers, columns: [id] } }
"#;
    let run = || {
        let plan = compile(model);
        let mut sink = CollectingSink::default();
        GenerationEngine::new(plan).run(&mut sink).unwrap();
        let ids: Vec<String> = sink
            .values("customers", "id")
            .map(|v| v.as_text().unwrap().to_string())
            .collect();
        let fks: Vec<String> = sink
            .values("orders", "customer_id")
            .map(|v| v.as_text().unwrap().to_string())
            .collect();
        (ids, fks)
    };
    let (ids, fks) = run();
    // The parent renders exactly the keys children reference.
    let id_set: HashSet<&String> = ids.iter().collect();
    assert_eq!(id_set.len(), 10);
    let uuid_shape = |s: &str| s.len() == 36 && s.matches('-').count() == 4;
    for id in &ids {
        assert!(uuid_shape(id), "customer id not a uuid: {id}");
    }
    assert_eq!(fks.len(), 40);
    for fk in &fks {
        assert!(uuid_shape(fk), "customer_id not a uuid: {fk}");
        assert!(id_set.contains(fk), "fk {fk} not an existing customer id");
    }
    // Same-seed reproduces.
    assert_eq!(run(), (ids, fks));
}

#[test]
fn explicit_sequence_primary_key_children_reference_real_generated_ids() {
    // The mainline spec shape: an explicit `{ kind: sequence, start: 1 }` PK
    // referenced by a child FK. It is random-access (row n renders start + n*step)
    // so it must compile and generate valid FK chains, not error.
    let model = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 7
tables:
  customers:
    rows: { kind: fixed, count: 10 }
    schema:
      name: customers
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
    columns:
      id:
        generator: { kind: sequence, start: 1, step: 1 }
  orders:
    rows:
      kind: relation.children
      parent: customers
      count: 40
      distribution: { kind: fixed, mean: 4.0, min: 1.0, max: 1000000.0 }
    schema:
      name: orders
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: customer_id, type: bigint, nullable: false }
    relationships:
      - { columns: [customer_id], references: { table: customers, columns: [id] } }
"#;
    let run = || {
        let plan = compile(model);
        let mut sink = CollectingSink::default();
        GenerationEngine::new(plan).run(&mut sink).unwrap();
        (
            sink.integers("customers", "id"),
            sink.integers("orders", "customer_id"),
        )
    };
    let (ids, fks) = run();
    let id_set: HashSet<i128> = ids.iter().copied().collect();
    // The parent renders exactly what children reference: sequence start 1 -> 1..=10.
    assert_eq!(id_set, (1..=10).collect());
    assert!(fks.iter().all(|fk| id_set.contains(fk)));
    // Same seed reproduces.
    assert_eq!(run(), (ids, fks));
}

#[test]
fn stateful_parent_key_is_unsupported() {
    // A `string` primary key advances a per-row stream and is not random-access
    // (it describes no key recipe), so a child referencing it errors rather than
    // silently rendering DEFAULT.
    let model = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 7
tables:
  customers:
    rows: { kind: fixed, count: 10 }
    schema:
      name: customers
      columns:
        - { name: id, type: text, nullable: false, primary_key: true }
    columns:
      id:
        generator: { kind: string, min_length: 8, max_length: 8 }
  orders:
    rows:
      kind: relation.children
      parent: customers
      count: 40
      distribution: { kind: fixed, mean: 4.0, min: 1.0, max: 1000000.0 }
    schema:
      name: orders
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: customer_id, type: text, nullable: false }
    relationships:
      - { columns: [customer_id], references: { table: customers, columns: [id] } }
"#;
    let plan = compile(model);
    let mut sink = CollectingSink::default();
    let err = GenerationEngine::new(plan).run(&mut sink).unwrap_err();
    assert!(
        err.to_string().contains("GEN-KEY-DOMAIN-UNSUPPORTED"),
        "unexpected error: {err}"
    );
}

#[test]
fn canonical_three_table_sequence_pk_fk_chain_generates_end_to_end() {
    // The spec's canonical shape: customers (seq PK) <- orders (seq PK +
    // customer_id FK) <- order_items (seq PK + order_id FK). Every FK link in the
    // chain must resolve to a real generated parent id.
    let model = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 7
tables:
  customers:
    rows: { kind: fixed, count: 8 }
    schema:
      name: customers
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
    columns:
      id:
        generator: { kind: sequence, start: 1, step: 1 }
  orders:
    rows:
      kind: relation.children
      parent: customers
      count: 24
      distribution: { kind: fixed, mean: 3.0, min: 1.0, max: 1000000.0 }
    schema:
      name: orders
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: customer_id, type: bigint, nullable: false }
    columns:
      id:
        generator: { kind: sequence, start: 1, step: 1 }
    relationships:
      - { columns: [customer_id], references: { table: customers, columns: [id] } }
  order_items:
    rows:
      kind: relation.children
      parent: orders
      count: 60
      distribution: { kind: fixed, mean: 2.5, min: 1.0, max: 1000000.0 }
    schema:
      name: order_items
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: order_id, type: bigint, nullable: false }
    columns:
      id:
        generator: { kind: sequence, start: 1, step: 1 }
    relationships:
      - { columns: [order_id], references: { table: orders, columns: [id] } }
"#;
    let plan = compile(model);
    let mut sink = CollectingSink::default();
    let report = GenerationEngine::new(plan).run(&mut sink).unwrap();
    assert_eq!(report.rows_written, 8 + 24 + 60);
    assert_eq!(sink.table_order(), ["customers", "orders", "order_items"]);

    let customer_ids: HashSet<i128> = sink.integers("customers", "id").into_iter().collect();
    let order_ids: HashSet<i128> = sink.integers("orders", "id").into_iter().collect();
    assert!(sink
        .integers("orders", "customer_id")
        .iter()
        .all(|id| customer_ids.contains(id)));
    assert!(sink
        .integers("order_items", "order_id")
        .iter()
        .all(|id| order_ids.contains(id)));
}

// --- Unresolved FK generator is a compile error -----------------------------

#[test]
fn foreign_key_generator_without_a_relationship_is_a_compile_error() {
    let model = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 7
tables:
  orders:
    rows: { kind: fixed, count: 5 }
    schema:
      name: orders
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: customer_id, type: bigint, nullable: true }
    columns:
      customer_id:
        generator: { kind: relation.foreign_key }
"#;
    let model = SyntheticFile::parse_str(model)
        .expect("valid model YAML")
        .into_model()
        .expect("document is a model");
    let err = ModelCompiler::standard()
        .compile(model, CompileOptions::default())
        .unwrap_err();
    assert!(
        err.to_string().contains("GEN-FOREIGN-KEY-UNRESOLVED"),
        "unexpected error: {err}"
    );
}

// --- Render model-driven SQL ------------------------------------------------

use sql_splitter::render::{RenderOptions, SqlRenderer};
use sql_splitter::synthetic::OutputMode;
use sql_splitter::validate::{ValidateOptions, Validator};
use std::io::Write as _;
use tempfile::NamedTempFile;

const SIMPLE_FIXTURE: &str = "tests/fixtures/generate/simple.yaml";

/// Compile `model_yaml`, render it under `configure`d options, and return the
/// full rendered SQL text.
fn render_model_with(model_yaml: &str, configure: impl FnOnce(&mut RenderOptions)) -> String {
    let plan = compile(model_yaml);
    let mut options = RenderOptions {
        source_dialect: plan.input_dialect,
        batch_size: 4,
        ..RenderOptions::default()
    };
    configure(&mut options);
    let mut renderer = SqlRenderer::new(Vec::new(), options);
    GenerationEngine::new(plan)
        .run(&mut renderer)
        .expect("renders cleanly");
    let bytes = renderer.finish().expect("finish flushes cleanly");
    String::from_utf8(bytes).expect("rendered SQL is valid UTF-8")
}

fn render_model(model_yaml: &str, dialect: SqlDialect) -> String {
    render_model_with(model_yaml, |options| options.dialect = dialect)
}

#[test]
fn random_table_seed_draws_fresh_entropy_for_each_compilation() {
    let model = r#"
version: 1
kind: model
defaults: { inference: disabled }
seed: 42
tables:
  events:
    seed: null
    rows: { kind: fixed, count: 3 }
    schema:
      name: events
      columns:
        - { name: token, type: "varchar(16)", nullable: false }
    columns:
      token: { generator: { kind: string, min_length: 16, max_length: 16 } }
"#;

    let first = render_model(model, SqlDialect::MySql);
    let second = render_model(model, SqlDialect::MySql);

    assert_ne!(first, second, "`seed: null` must not use a fixed zero root");
}

fn render_fixture_with(path: &str, configure: impl FnOnce(&mut RenderOptions)) -> String {
    let yaml = std::fs::read_to_string(path).expect("fixture readable");
    render_model_with(&yaml, configure)
}

fn render_fixture(path: &str, dialect: SqlDialect) -> String {
    render_fixture_with(path, |options| options.dialect = dialect)
}

/// Mirrors `render::sql`'s COPY-text escaping, independently, so the test
/// doesn't just restate the production code's own claim about itself.
fn copy_escape(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            other => out.push(other),
        }
    }
    out
}

#[test]
fn simple_model_renders_valid_dialect_shapes() {
    for dialect in [
        SqlDialect::MySql,
        SqlDialect::Postgres,
        SqlDialect::Sqlite,
        SqlDialect::Mssql,
    ] {
        let sql = render_fixture(SIMPLE_FIXTURE, dialect);
        assert!(sql.contains("CREATE TABLE"), "{dialect:?}: {sql}");
        match dialect {
            SqlDialect::Postgres => assert!(sql.contains("COPY "), "{sql}"),
            SqlDialect::Mssql => assert!(sql.contains("N'"), "{sql}"),
            _ => assert!(sql.contains("INSERT INTO"), "{sql}"),
        }
    }
}

#[test]
fn identifiers_are_quoted_per_dialect() {
    assert!(render_fixture(SIMPLE_FIXTURE, SqlDialect::MySql).contains("`customers`"));
    assert!(render_fixture(SIMPLE_FIXTURE, SqlDialect::Postgres).contains("\"customers\""));
    assert!(render_fixture(SIMPLE_FIXTURE, SqlDialect::Sqlite).contains("\"customers\""));
    assert!(render_fixture(SIMPLE_FIXTURE, SqlDialect::Mssql).contains("[customers]"));
}

#[test]
fn foreign_key_and_index_render_after_the_create_table() {
    let sql = render_fixture(SIMPLE_FIXTURE, SqlDialect::Postgres);
    assert!(
        sql.contains(
            "ALTER TABLE \"orders\" ADD CONSTRAINT \"fk_orders_customer\" FOREIGN KEY (\"customer_id\") REFERENCES \"customers\" (\"id\");"
        ),
        "{sql}"
    );
    assert!(
        sql.contains("CREATE INDEX \"idx_orders_customer_id\" ON \"orders\" (\"customer_id\");"),
        "{sql}"
    );
}

#[test]
fn default_column_is_omitted_from_the_insert_column_list_and_values() {
    let sql = render_fixture(SIMPLE_FIXTURE, SqlDialect::MySql);
    // `orders.status` always renders as DEFAULT (a `database_default`
    // generator); the renderer omits it from the column list and VALUES
    // entirely rather than repeating the DEFAULT keyword every row.
    let orders_insert = sql
        .lines()
        .find(|line| line.starts_with("INSERT INTO `orders`"))
        .expect("orders INSERT statement");
    assert!(!orders_insert.contains("status"), "{orders_insert}");
}

#[test]
fn null_default_bytes_decimal_date_json_render_correctly() {
    let model = r#"
version: 1
kind: model
defaults: { inference: disabled }
seed: 11
tables:
  widgets:
    rows: { kind: fixed, count: 1 }
    schema:
      name: widgets
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: amount, type: "decimal(5,2)", nullable: false }
        - { name: payload, type: "varbinary(4)", nullable: false }
        - { name: notes, type: "varchar(50)", nullable: true }
        - { name: metadata, type: json, nullable: false }
        - { name: created_at, type: datetime, nullable: false }
        - { name: status, type: "varchar(20)", nullable: false, default_sql: "'pending'" }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
      amount: { generator: { kind: decimal, min: 12.34, max: 12.34, scale: 2 } }
      payload: { generator: { kind: bytes, min_length: 3, max_length: 3 } }
      notes: { generator: { kind: constant } }
      metadata: { generator: { kind: json_value, value: { ok: true } } }
      created_at: { generator: { kind: datetime } }
      status: { generator: { kind: database_default } }
"#;
    let sql = render_model(model, SqlDialect::MySql);
    assert!(sql.contains("12.34"), "decimal: {sql}");
    assert!(sql.contains("X'"), "bytes: {sql}");
    assert!(sql.contains("NULL"), "null: {sql}");
    assert!(sql.contains(r#"'{"ok":true}'"#), "json: {sql}");
    assert!(
        regex::Regex::new(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
            .unwrap()
            .is_match(&sql),
        "date: {sql}"
    );
    // `status` always renders DEFAULT: the column still appears in the DDL
    // (it needs a type), but is dropped from the INSERT column list/VALUES.
    let insert_line = sql
        .lines()
        .find(|line| line.starts_with("INSERT INTO"))
        .expect("widgets INSERT statement");
    assert!(!insert_line.contains("status"), "{insert_line}");
}

#[test]
fn postgres_copy_escaping_differs_from_insert_string_escaping() {
    let original = "back\\slash\ttab\nnewline";
    let yaml_value = serde_yaml_ng::to_string(&original).expect("string always serializes");
    let model = format!(
        r#"
version: 1
kind: model
defaults: {{ inference: disabled }}
seed: 3
tables:
  notes:
    rows: {{ kind: fixed, count: 1 }}
    schema:
      name: notes
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: body, type: text, nullable: false }}
    columns:
      id: {{ generator: {{ kind: sequence, start: 1 }} }}
      body: {{ generator: {{ kind: constant, value: {} }} }}
"#,
        yaml_value.trim()
    );

    let copy_sql = render_model(&model, SqlDialect::Postgres);
    assert!(copy_sql.contains("COPY "), "{copy_sql}");
    assert!(copy_sql.contains(&copy_escape(original)), "{copy_sql}");
    assert!(!copy_sql.contains(&format!("'{original}'")), "{copy_sql}");

    let insert_sql = render_model_with(&model, |options| {
        options.dialect = SqlDialect::Postgres;
        options.no_copy = true;
    });
    assert!(insert_sql.contains("INSERT INTO"), "{insert_sql}");
    // Same value, INSERT-literal escaped: quoted verbatim (Postgres string
    // literals leave `\t`/`\n`/`\\` untouched; only `'` would be doubled).
    assert!(
        insert_sql.contains(&format!("'{original}'")),
        "{insert_sql}"
    );
}

#[test]
fn mssql_renders_unicode_string_literals_and_go_batch_separators() {
    let model = r#"
version: 1
kind: model
defaults: { inference: disabled }
seed: 5
tables:
  notes:
    rows: { kind: fixed, count: 3 }
    schema:
      name: notes
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: body, type: text, nullable: false }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
      body: { generator: { kind: constant, value: "héllo wörld" } }
"#;
    let sql = render_model_with(model, |options| {
        options.dialect = SqlDialect::Mssql;
        options.batch_size = 2;
    });
    assert!(sql.contains("N'héllo wörld'"), "{sql}");
    // One GO after the CREATE TABLE, one after each of the two INSERT
    // batches that 3 rows split into at batch_size 2 (2 rows, then 1).
    assert_eq!(sql.matches("\nGO\n").count(), 3, "{sql}");
    assert_eq!(sql.matches("INSERT INTO").count(), 2, "{sql}");
}

#[test]
fn batch_size_splits_rows_into_multiple_insert_statements() {
    let model = r#"
version: 1
kind: model
defaults: { inference: disabled }
seed: 9
tables:
  widgets:
    rows: { kind: fixed, count: 7 }
    schema:
      name: widgets
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
"#;
    let sql = render_model_with(model, |options| {
        options.dialect = SqlDialect::MySql;
        options.batch_size = 3;
    });
    assert_eq!(sql.matches("INSERT INTO").count(), 3, "{sql}");
    let ids: Vec<i64> = regex::Regex::new(r"\((\d+)\)")
        .unwrap()
        .captures_iter(&sql)
        .map(|c| c[1].parse().unwrap())
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5, 6, 7]);
}

#[test]
fn schema_only_mode_emits_ddl_without_any_row_data() {
    let sql = render_fixture_with(SIMPLE_FIXTURE, |options| {
        options.mode = OutputMode::SchemaOnly
    });
    assert!(sql.contains("CREATE TABLE"), "{sql}");
    assert!(!sql.contains("INSERT INTO"), "{sql}");
    assert!(!sql.contains("COPY "), "{sql}");
}

#[test]
fn data_only_mode_emits_row_data_without_any_ddl() {
    let sql = render_fixture_with(SIMPLE_FIXTURE, |options| {
        options.mode = OutputMode::DataOnly
    });
    assert!(!sql.contains("CREATE TABLE"), "{sql}");
    assert!(sql.contains("INSERT INTO"), "{sql}");
}

#[test]
fn raw_ddl_is_preserved_only_when_the_target_dialect_matches_the_source() {
    let model = r#"
version: 1
kind: model
defaults: { inference: disabled }
seed: 1
source: { dialect: mysql }
tables:
  widgets:
    rows: { kind: fixed, count: 1 }
    schema:
      name: widgets
      create_statement: "CREATE TABLE `widgets` (\n  `id` bigint NOT NULL\n) ENGINE=InnoDB;"
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
"#;
    let same_dialect = render_model(model, SqlDialect::MySql);
    assert!(same_dialect.contains("ENGINE=InnoDB"), "{same_dialect}");

    let cross_dialect = render_model(model, SqlDialect::Postgres);
    assert!(!cross_dialect.contains("ENGINE=InnoDB"), "{cross_dialect}");
    assert!(
        cross_dialect.contains("CREATE TABLE \"widgets\""),
        "{cross_dialect}"
    );
}

#[test]
fn rendered_mysql_output_passes_the_existing_validator() {
    let sql = render_fixture(SIMPLE_FIXTURE, SqlDialect::MySql);
    let mut file = NamedTempFile::new().expect("temp file");
    file.write_all(sql.as_bytes()).expect("write rendered SQL");
    file.flush().expect("flush temp file");

    let options = ValidateOptions {
        path: file.path().to_path_buf(),
        dialect: Some(SqlDialect::MySql),
        progress: false,
        strict: false,
        json: false,
        max_rows_per_table: 1_000_000,
        fk_checks_enabled: true,
        max_pk_fk_keys: None,
    };
    let summary = Validator::new(options).validate().expect("validate runs");
    assert_eq!(summary.summary.errors, 0, "{summary:?}");
}

// --- Protected family spooling budget and spill -----------------------------

use sql_splitter::generate::output::{
    FamilyBudget, FamilyBuffer, FamilyState, SpillKind, SpooledRow, TempConfig,
};

/// A family child row of roughly known size, so a budget can be sized to force
/// (or avoid) a spill deterministically.
fn family_child_row(row_index: u64) -> SpooledRow {
    SpooledRow {
        table_id: 2,
        row_index,
        values: vec![
            GeneratedValue::Integer(row_index as i128),
            GeneratedValue::Text(format!("child-{row_index}")),
        ],
    }
}

#[test]
fn family_buffer_within_budget_stays_in_parent_state() {
    let mut buffer = FamilyBuffer::new(
        FamilyBudget { max_bytes: 1 << 20 },
        2,
        TempConfig::default(),
        SpillKind::Child,
    );
    let rows: Vec<SpooledRow> = (0..8).map(family_child_row).collect();
    for row in &rows {
        buffer.push(row.clone()).unwrap();
    }
    assert!(matches!(buffer.state(), FamilyState::ParentState(_)));
    assert_eq!(buffer.drain_rows().unwrap(), rows);
}

#[test]
fn family_buffer_crossing_budget_spills_every_child_row_in_order() {
    // A budget below a single row's footprint forces a spill; no child row may
    // be dropped and none may be retained in an unbounded in-memory Vec.
    let mut buffer = FamilyBuffer::new(
        FamilyBudget { max_bytes: 8 },
        2,
        TempConfig::default(),
        SpillKind::Child,
    );
    let rows: Vec<SpooledRow> = (0..64).map(family_child_row).collect();
    for row in &rows {
        buffer.push(row.clone()).unwrap();
    }
    assert!(buffer.is_spilled());
    assert!(matches!(buffer.state(), FamilyState::ChildSpool(_)));
    assert_eq!(buffer.drain_rows().unwrap(), rows);
}

#[test]
fn family_buffer_with_large_estimate_spills_before_the_first_row() {
    // A family known up front to exceed its budget spills before generation,
    // choosing a protected spool rather than a transient in-memory spike.
    let mut buffer = FamilyBuffer::with_estimate(
        FamilyBudget { max_bytes: 1024 },
        5,
        TempConfig::default(),
        SpillKind::Table,
        1_000_000,
    )
    .unwrap();
    assert!(buffer.is_spilled());
    assert!(matches!(buffer.state(), FamilyState::TableSpool(_)));
    let rows: Vec<SpooledRow> = (0..10).map(family_child_row).collect();
    for row in &rows {
        buffer.push(row.clone()).unwrap();
    }
    assert_eq!(buffer.drain_rows().unwrap(), rows);
}

// --- Commerce order-family execution and spill ------------------------------

/// Compile `model_yaml` pinning the family memory budget, then render it to SQL.
fn render_family_at_budget(model_yaml: &str, family_budget_bytes: u64) -> String {
    let model = SyntheticFile::parse_str(model_yaml)
        .expect("valid model YAML")
        .into_model()
        .expect("document is a model");
    let plan = ModelCompiler::standard()
        .compile(
            model,
            CompileOptions {
                family_budget_bytes: Some(family_budget_bytes),
                ..CompileOptions::default()
            },
        )
        .expect("model compiles cleanly");
    let options = RenderOptions {
        dialect: SqlDialect::Postgres,
        batch_size: 8,
        no_copy: true,
        ..RenderOptions::default()
    };
    let mut renderer = SqlRenderer::new(Vec::new(), options);
    GenerationEngine::new(plan)
        .run(&mut renderer)
        .expect("engine runs the family");
    let bytes = renderer.finish().expect("finish flushes cleanly");
    String::from_utf8(bytes).expect("rendered SQL is valid UTF-8")
}

const ORDER_FAMILY_MODEL: &str = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 4242
tables:
  orders:
    rows: { kind: fixed, count: 200 }
    schema:
      name: orders
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: subtotal, type: "decimal(18,2)", nullable: false }
        - { name: tax_total, type: "decimal(18,2)", nullable: false }
        - { name: grand_total, type: "decimal(18,2)", nullable: false }
    columns:
      id:
        generator: { kind: sequence, start: 1 }
    planners:
      - kind: commerce.order_family
        children: order_items
        relationship: order_items_order
        columns:
          subtotal: subtotal
          tax: tax_total
          total: grand_total
        child_columns:
          quantity: quantity
          unit_price: unit_price
          tax: tax_amount
          line_total: line_total
        currency_scale: 2
        rounding: largest_remainder
        quantity: { min: 1, max: 6 }
        unit_price: { min_minor: 100, max_minor: 90000 }
        tax:
          kind: weighted_choice
          rates: [0.0, 0.08, 0.25]
          weights: [0.1, 0.3, 0.6]
  order_items:
    rows:
      kind: relation.children
      parent: orders
      count: 0
      distribution: { kind: fixed, mean: 4.0, min: 1.0, max: 12.0 }
    schema:
      name: order_items
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: order_id, type: bigint, nullable: false }
        - { name: quantity, type: integer, nullable: false }
        - { name: unit_price, type: "decimal(18,2)", nullable: false }
        - { name: tax_amount, type: "decimal(18,2)", nullable: false }
        - { name: line_total, type: "decimal(18,2)", nullable: false }
    relationships:
      - name: order_items_order
        columns: [order_id]
        references: { table: orders, columns: [id] }
    columns:
      id:
        generator: { kind: sequence, start: 1 }
      order_id:
        generator: { kind: relation.foreign_key, relationship: order_items_order }
"#;

#[test]
fn family_output_is_byte_identical_across_a_tiny_and_a_huge_budget() {
    // A 1 KiB budget forces the child buffer to spill to a protected spool many
    // times; a 1 GiB budget keeps every family in memory. Because each family is
    // seeded by its parent row index, the spill changes only where rows live —
    // never their values — so the rendered SQL is byte-for-byte identical.
    let tiny = render_family_at_budget(ORDER_FAMILY_MODEL, 1024);
    let huge = render_family_at_budget(ORDER_FAMILY_MODEL, 1 << 30);
    assert_eq!(tiny, huge, "family spill must not change generated output");
    // Sanity: the family actually produced both tables and non-trivial data.
    assert!(
        tiny.contains("INSERT INTO \"order_items\""),
        "child rows rendered"
    );
    assert!(
        tiny.contains("INSERT INTO \"orders\""),
        "parent rows rendered"
    );

    // Regression guard: prove the 1 KiB budget actually EXERCISES the spill path
    // (a future byte-estimate change must not silently keep the family in memory
    // while the byte-identical assertion above still passes). Push the real child
    // rows the engine generated through a FamilyBuffer at each budget.
    let child_rows = collect_family_child_rows(1 << 30);
    assert!(!child_rows.is_empty(), "the family produced child rows");
    let spool_rows = || {
        child_rows.iter().enumerate().map(|(i, values)| SpooledRow {
            table_id: 1,
            row_index: i as u64,
            values: values.clone(),
        })
    };
    let mut tiny_buffer = FamilyBuffer::new(
        FamilyBudget { max_bytes: 1024 },
        1,
        TempConfig::default(),
        SpillKind::Child,
    );
    for row in spool_rows() {
        tiny_buffer.push(row).unwrap();
    }
    assert!(
        tiny_buffer.is_spilled(),
        "the 1 KiB family budget must spill the child rows to a protected spool"
    );
    let mut huge_buffer = FamilyBuffer::new(
        FamilyBudget { max_bytes: 1 << 30 },
        1,
        TempConfig::default(),
        SpillKind::Child,
    );
    for row in spool_rows() {
        huge_buffer.push(row).unwrap();
    }
    assert!(
        !huge_buffer.is_spilled(),
        "the 1 GiB family budget must keep the family in memory"
    );
}

/// Compile the family model at `family_budget_bytes`, run it into a
/// [`CollectingSink`], and return the actual `order_items` child rows the engine
/// generated — the rows a [`FamilyBuffer`] would spool.
fn collect_family_child_rows(family_budget_bytes: u64) -> Vec<Vec<GeneratedValue>> {
    let model = SyntheticFile::parse_str(ORDER_FAMILY_MODEL)
        .expect("valid model YAML")
        .into_model()
        .expect("document is a model");
    let plan = ModelCompiler::standard()
        .compile(
            model,
            CompileOptions {
                family_budget_bytes: Some(family_budget_bytes),
                ..CompileOptions::default()
            },
        )
        .expect("model compiles cleanly");
    let mut sink = CollectingSink::default();
    GenerationEngine::new(plan).run(&mut sink).unwrap();
    sink.rows["order_items"].clone()
}

#[test]
fn family_child_rows_reference_their_producing_parent() {
    // Every order_items.order_id must be a real orders.id in 1..=200.
    let sql = render_family_at_budget(ORDER_FAMILY_MODEL, 1 << 30);
    assert!(sql.contains("INSERT INTO \"order_items\""));
    // The child table is generated after its parent (dependency order).
    let orders_pos = sql.find("INSERT INTO \"orders\"").expect("orders insert");
    let items_pos = sql
        .find("INSERT INTO \"order_items\"")
        .expect("items insert");
    assert!(
        orders_pos < items_pos,
        "parent rows render before child rows"
    );
}

/// Real-test equivalent of the former staged-API doctest in
/// `src/generate/mod.rs`: assemble the four public stages by hand
/// (registry -> compiler -> engine -> renderer) and render into an in-memory
/// sink.
#[test]
fn staged_api_renders_inserts() {
    let model = sql_splitter::synthetic::SyntheticFile::parse_str(
        r#"
version: 1
kind: model
defaults: { inference: disabled }
seed: 1
tables:
  users:
    rows: { kind: fixed, count: 3 }
    schema:
      name: users
      primary_key: [id]
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: name, type: "varchar(50)", nullable: false }
    columns:
      id:
        generator: { kind: sequence, start: 1 }
      name:
        generator: { kind: string, min_length: 3, max_length: 8 }
"#,
    )
    .expect("parses")
    .into_model()
    .expect("is a complete model");

    let registry = sql_splitter::generate::ExtensionRegistry::standard();
    let plan = sql_splitter::generate::ModelCompiler::new(registry)
        .compile(model, sql_splitter::generate::CompileOptions::default())
        .expect("compiles");

    let mut renderer = SqlRenderer::new(Vec::new(), RenderOptions::default());
    GenerationEngine::new(plan)
        .run(&mut renderer)
        .expect("generates");
    let bytes = renderer.finish().expect("finish flushes cleanly");

    assert!(String::from_utf8(bytes)
        .expect("rendered SQL is valid UTF-8")
        .contains("INSERT INTO"));
}

//! Tests for the synthetic-data-generation YAML document model
//! (`SyntheticFile`, `SyntheticModel`, `SyntheticOverrides`, and the
//! tri-state seed types).

use sql_splitter::synthetic::{
    RootSeedOverride, SyntheticFile, SyntheticOverrides, TableSeed, TableSeedOverride,
};

#[test]
fn document_role_and_table_seed_are_unambiguous() {
    let yaml = r#"
version: 1
kind: model
defaults: { inference: disabled }
output: { dialect: mysql }
tables:
  inherited: { rows: { kind: fixed, count: 1 }, schema: { name: inherited, columns: [] } }
  random: { seed: null, rows: { kind: fixed, count: 1 }, schema: { name: random, columns: [] } }
  fixed: { seed: 9, rows: { kind: fixed, count: 1 }, schema: { name: fixed, columns: [] } }
"#;
    let file = SyntheticFile::parse_str(yaml).unwrap();
    let model = file.into_model().unwrap();
    assert_eq!(model.tables["inherited"].seed, TableSeed::Inherit);
    assert_eq!(model.tables["random"].seed, TableSeed::Random);
    assert_eq!(model.tables["fixed"].seed, TableSeed::Fixed(9));
}

#[test]
fn unknown_model_fields_fail() {
    let err = SyntheticFile::parse_str(
        "version: 1\nkind: model\ndefaults: { inference: disabled }\noutput: { dialect: mysql }\ntables: {}\ntabels: {}\n",
    )
    .unwrap_err();
    assert!(err.to_string().contains("unknown field"));
}

#[test]
fn duplicate_mapping_keys_are_rejected() {
    let err = SyntheticFile::parse_str(
        "version: 1\nkind: model\ndefaults: { inference: disabled }\noutput: { dialect: mysql }\ntables: {}\ntables: {}\n",
    )
    .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("duplicate"));
}

#[test]
fn unsupported_version_is_rejected() {
    let err = SyntheticFile::parse_str(
        "version: 2\nkind: model\ndefaults: { inference: disabled }\noutput: { dialect: mysql }\ntables: {}\n",
    )
    .unwrap_err();
    assert!(err.to_string().contains("unsupported version"));
}

#[test]
fn wrong_role_conversions_error() {
    let model_yaml = "version: 1\nkind: model\ndefaults: { inference: disabled }\noutput: { dialect: mysql }\ntables: {}\n";
    let file = SyntheticFile::parse_str(model_yaml).unwrap();
    assert!(file.into_overrides().is_err());

    let overrides_yaml = "version: 1\nkind: overrides\n";
    let file = SyntheticFile::parse_str(overrides_yaml).unwrap();
    assert!(file.into_model().is_err());
}

#[test]
fn overrides_role_and_seed_states_are_unambiguous() {
    let yaml = r#"
version: 1
kind: overrides
seed: null
tables:
  inherited: {}
  random: { seed: null }
  fixed: { seed: 9 }
"#;
    let file = SyntheticFile::parse_str(yaml).unwrap();
    let overrides = file.into_overrides().unwrap();
    assert_eq!(overrides.seed, RootSeedOverride::Random);
    assert_eq!(
        overrides.tables["inherited"].seed,
        TableSeedOverride::Inherit
    );
    assert_eq!(overrides.tables["random"].seed, TableSeedOverride::Random);
    assert_eq!(overrides.tables["fixed"].seed, TableSeedOverride::Fixed(9));
}

#[test]
fn unknown_overrides_fields_fail() {
    let err = SyntheticFile::parse_str("version: 1\nkind: overrides\nbogus: true\n").unwrap_err();
    assert!(err.to_string().contains("unknown field"));
}

#[test]
fn table_seed_round_trips_through_serialization() {
    let yaml = r#"
version: 1
kind: model
defaults: { inference: disabled }
output: { dialect: mysql }
tables:
  inherited: { rows: { kind: fixed, count: 1 }, schema: { name: inherited, columns: [] } }
  random: { seed: null, rows: { kind: fixed, count: 1 }, schema: { name: random, columns: [] } }
  fixed: { seed: 9, rows: { kind: fixed, count: 1 }, schema: { name: fixed, columns: [] } }
"#;
    let model = SyntheticFile::parse_str(yaml)
        .unwrap()
        .into_model()
        .unwrap();
    let rendered = serde_yaml_ng::to_string(&model).unwrap();

    assert!(!rendered.contains("inherited:\n    seed"));
    assert!(rendered.contains("random:\n    seed: null"));
    assert!(rendered.contains("fixed:\n    seed: 9"));

    let reparsed: sql_splitter::synthetic::SyntheticModel =
        serde_yaml_ng::from_str(&rendered).unwrap();
    assert_eq!(reparsed.tables["inherited"].seed, TableSeed::Inherit);
    assert_eq!(reparsed.tables["random"].seed, TableSeed::Random);
    assert_eq!(reparsed.tables["fixed"].seed, TableSeed::Fixed(9));
}

#[test]
fn root_seed_override_round_trips_through_serialization() {
    let inherit_yaml = "version: 1\nkind: overrides\n";
    let overrides: SyntheticOverrides = serde_yaml_ng::from_str(inherit_yaml).unwrap();
    let rendered = serde_yaml_ng::to_string(&overrides).unwrap();
    assert!(!rendered.contains("seed"));

    let random_yaml = "version: 1\nkind: overrides\nseed: null\n";
    let overrides: SyntheticOverrides = serde_yaml_ng::from_str(random_yaml).unwrap();
    let rendered = serde_yaml_ng::to_string(&overrides).unwrap();
    assert!(rendered.contains("seed: null"));

    let fixed_yaml = "version: 1\nkind: overrides\nseed: 42\n";
    let overrides: SyntheticOverrides = serde_yaml_ng::from_str(fixed_yaml).unwrap();
    let rendered = serde_yaml_ng::to_string(&overrides).unwrap();
    assert!(rendered.contains("seed: 42"));
}

#[test]
fn overrides_example_parses() {
    let yaml = r#"
version: 1
kind: overrides

seed: 42

tables:
  audit_events:
    rows: { kind: observed, scale: 0.01 }
  users:
    columns:
      email:
        generator: { kind: internet.email }
"#;
    let overrides = SyntheticFile::parse_str(yaml)
        .unwrap()
        .into_overrides()
        .unwrap();
    assert_eq!(overrides.seed, RootSeedOverride::Fixed(42));
    let audit_events = &overrides.tables["audit_events"];
    let rows = audit_events.rows.as_ref().unwrap();
    assert_eq!(rows.scale, Some(0.01));
    assert!(overrides.tables["users"].columns["email"]
        .generator
        .is_some());
}

#[test]
fn complete_model_example_parses() {
    // Copied from the "Complete model example" in
    // docs/superpowers/specs/2026-07-16-synthetic-data-generation-design.md,
    // including its `type:` schema shorthand — see design decision D1 and
    // `PortableColumn`'s `#[serde(try_from = "PortableColumnInput")]` in
    // src/synthetic/schema.rs, which derives `family` from `type`/
    // `source_type` when it is absent.
    //
    // One byte-for-byte deviation: `type: decimal(12,2)` is quoted here as
    // `type: "decimal(12,2)"`. Unquoted, the embedding flow mapping
    // `{ name: ..., type: decimal(12,2), nullable: false }` is not valid
    // YAML — the comma inside the unquoted scalar is a flow-mapping
    // separator, so every conformant YAML parser (confirmed against both
    // serde_yaml_ng and Python's PyYAML) splits `decimal(12,2)` into a
    // `type: decimal(12` pair and a stray `2)` key. This is a pre-existing
    // quoting gap in the design doc's example, unrelated to fix #2; it is
    // flagged in task-3-report.md rather than silently worked around.
    let yaml = r#"
version: 1
kind: model

source:
  dialect: mysql
  fingerprint: sha256:0123456789abcdef
  fingerprint_policy: warn        # ignore | warn | require

output:
  dialect: postgres
  mode: schema_and_data            # schema_and_data | schema_only | data_only
  inserts: auto                    # auto | insert | copy
  batch_size: 1000

seed: 840219

defaults:
  inference: disabled              # schema | disabled

tables:
  customers:
    rows: { kind: observed, count: 50000 }
    schema:
      name: customers
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: email, type: varchar(255), nullable: false, unique: true }
        - { name: status, type: varchar(32), nullable: false }
    columns:
      id:
        generator: { kind: sequence, start: 1 }
      email:
        generator: { kind: internet.email }
        modifiers:
          - { kind: unique }
      status:
        generator:
          kind: weighted_choice
          values:
            - { value: active, weight: 0.86 }
            - { value: paused, weight: 0.09 }
            - { value: closed, weight: 0.05 }

  orders:
    seed: null                      # random even though the model has a seed
    rows:
      kind: relation.children
      parent: customers
      count: 210000
      distribution: { kind: observed, mean: 4.2, min: 0, max: 30 }
    schema:
      name: orders
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: customer_id, type: bigint, nullable: false }
        - { name: subtotal, type: "decimal(12,2)", nullable: false }
        - { name: tax_total, type: "decimal(12,2)", nullable: false }
        - { name: grand_total, type: "decimal(12,2)", nullable: false }
    relationships:
      - name: orders_customer
        columns: [customer_id]
        references: { table: customers, columns: [id] }
        distribution: observed
    columns:
      id:
        generator: { kind: sequence, start: 1 }
      customer_id:
        generator: { kind: relation.foreign_key, relationship: orders_customer }
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

  order_items:
    rows:
      kind: relation.children
      parent: orders
      count: 714000
      distribution: { kind: observed, mean: 3.4, min: 1, max: 50 }
    schema:
      name: order_items
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: order_id, type: bigint, nullable: false }
        - { name: quantity, type: integer, nullable: false }
        - { name: unit_price, type: "decimal(12,2)", nullable: false }
        - { name: tax_amount, type: "decimal(12,2)", nullable: false }
        - { name: line_total, type: "decimal(12,2)", nullable: false }
    relationships:
      - name: order_items_order
        columns: [order_id]
        references: { table: orders, columns: [id] }
    columns:
      id:
        generator: { kind: sequence, start: 1 }
      order_id:
        generator: { kind: relation.foreign_key, relationship: order_items_order }
      # quantity, prices, tax, and line_total are owned by commerce.order_family.

profiles:
  customers.status:
    rows: 182340
    null_fraction: 0.0
    distinct_estimate: 3
    inference:
      selected: weighted_choice
      confidence: high
      reasons: [low_cardinality, stable_top_values, status_name]
"#;

    let model = SyntheticFile::parse_str(yaml)
        .unwrap()
        .into_model()
        .unwrap();

    assert_eq!(model.seed, Some(840219));
    assert_eq!(model.output.dialect.as_deref(), Some("postgres"));
    assert_eq!(model.tables.len(), 3);
    assert_eq!(model.tables["orders"].seed, TableSeed::Random);
    assert_eq!(
        model.tables["customers"].columns["status"]
            .generator
            .as_ref()
            .unwrap()
            .kind,
        "weighted_choice"
    );
    assert_eq!(model.tables["orders"].planners.len(), 1);
    assert_eq!(
        model.tables["orders"].relationships[0].name.as_deref(),
        Some("orders_customer")
    );
    assert_eq!(model.profiles["customers.status"].distinct_estimate, 3);

    // The `type:` shorthand columns resolved to the canonical fields.
    let customers_schema = &model.tables["customers"].schema;
    let id_column = customers_schema
        .columns
        .iter()
        .find(|c| c.name == "id")
        .unwrap();
    assert_eq!(id_column.source_type, "bigint");
    assert_eq!(
        id_column.family,
        sql_splitter::synthetic::SqlTypeFamily::BigInteger
    );

    // Round-trip: re-serialize and re-parse without loss of the values we
    // depend on downstream (the compiler task). Re-serialization always
    // emits the canonical source_type/family form, never the `type:`
    // shorthand this test parsed.
    let rendered = serde_yaml_ng::to_string(&model).unwrap();
    assert!(rendered.contains("source_type: bigint"));
    let reparsed: sql_splitter::synthetic::SyntheticModel =
        serde_yaml_ng::from_str(&rendered).unwrap();
    assert_eq!(reparsed.tables.len(), model.tables.len());
    assert_eq!(reparsed.tables["orders"].seed, TableSeed::Random);
}

#[test]
fn rows_and_child_distribution_reject_unknown_fields() {
    let yaml = "version: 1\nkind: model\ndefaults: { inference: disabled }\noutput: { dialect: mysql }\ntables:\n  t: { rows: { kind: fixed, count: 1, bogus: true }, schema: { name: t, columns: [] } }\n";
    let err = SyntheticFile::parse_str(yaml).unwrap_err();
    assert!(err.to_string().contains("unknown field"));

    let yaml = "version: 1\nkind: model\ndefaults: { inference: disabled }\noutput: { dialect: mysql }\ntables:\n  t: { rows: { kind: relation.children, parent: p, count: 1, distribution: { kind: observed, mean: 1.0, min: 0.0, max: 1.0, bogus: true } }, schema: { name: t, columns: [] } }\n";
    let err = SyntheticFile::parse_str(yaml).unwrap_err();
    assert!(err.to_string().contains("unknown field"));
}

#[test]
fn minimal_model_without_output_or_defaults_parses() {
    let yaml = "version: 1\nkind: model\ntables: {}\n";
    let model = SyntheticFile::parse_str(yaml)
        .unwrap()
        .into_model()
        .unwrap();

    assert_eq!(
        model.defaults.inference,
        sql_splitter::synthetic::InferenceMode::Disabled
    );
    assert_eq!(model.output.dialect, None);
}

#[test]
fn overrides_with_defaults_and_source_parses() {
    let yaml = r#"
version: 1
kind: overrides

source:
  dialect: mysql

defaults:
  inference: schema
"#;
    let overrides = SyntheticFile::parse_str(yaml)
        .unwrap()
        .into_overrides()
        .unwrap();

    assert_eq!(overrides.source.unwrap().dialect, "mysql");
    assert_eq!(
        overrides.defaults.unwrap().inference,
        sql_splitter::synthetic::InferenceMode::Schema
    );
}

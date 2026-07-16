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
    // Adapted from the "Complete model example" in
    // docs/superpowers/specs/2026-07-16-synthetic-data-generation-design.md.
    // The design doc uses a `type: bigint` schema shorthand for readability;
    // this test uses `PortableColumn`'s actual `source_type`/`family` fields
    // since the model reuses that type directly (see task-3-report.md).
    let yaml = r#"
version: 1
kind: model

source:
  dialect: mysql
  fingerprint: sha256:0123456789abcdef
  fingerprint_policy: warn

output:
  dialect: postgres
  mode: schema_and_data
  inserts: auto
  batch_size: 1000

seed: 840219

defaults:
  inference: disabled

tables:
  customers:
    rows: { kind: observed, count: 50000 }
    schema:
      name: customers
      columns:
        - { name: id, source_type: bigint, family: big_integer, nullable: false, primary_key: true }
        - { name: email, source_type: "varchar(255)", family: text, nullable: false, unique: true }
        - { name: status, source_type: "varchar(32)", family: text, nullable: false }
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
    seed: null
    rows:
      kind: relation.children
      parent: customers
      count: 210000
      distribution: { kind: observed, mean: 4.2, min: 0, max: 30 }
    schema:
      name: orders
      columns:
        - { name: id, source_type: bigint, family: big_integer, nullable: false, primary_key: true }
        - { name: customer_id, source_type: bigint, family: big_integer, nullable: false }
        - { name: subtotal, source_type: "decimal(12,2)", family: decimal, nullable: false }
        - { name: tax_total, source_type: "decimal(12,2)", family: decimal, nullable: false }
        - { name: grand_total, source_type: "decimal(12,2)", family: decimal, nullable: false }
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
        - { name: id, source_type: bigint, family: big_integer, nullable: false, primary_key: true }
        - { name: order_id, source_type: bigint, family: big_integer, nullable: false }
        - { name: quantity, source_type: integer, family: integer, nullable: false }
        - { name: unit_price, source_type: "decimal(12,2)", family: decimal, nullable: false }
        - { name: tax_amount, source_type: "decimal(12,2)", family: decimal, nullable: false }
        - { name: line_total, source_type: "decimal(12,2)", family: decimal, nullable: false }
    relationships:
      - name: order_items_order
        columns: [order_id]
        references: { table: orders, columns: [id] }
    columns:
      id:
        generator: { kind: sequence, start: 1 }
      order_id:
        generator: { kind: relation.foreign_key, relationship: order_items_order }

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
    assert_eq!(model.output.dialect, "postgres");
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

    // Round-trip: re-serialize and re-parse without loss of the values we
    // depend on downstream (the compiler task).
    let rendered = serde_yaml_ng::to_string(&model).unwrap();
    let reparsed: sql_splitter::synthetic::SyntheticModel =
        serde_yaml_ng::from_str(&rendered).unwrap();
    assert_eq!(reparsed.tables.len(), model.tables.len());
    assert_eq!(reparsed.tables["orders"].seed, TableSeed::Random);
}

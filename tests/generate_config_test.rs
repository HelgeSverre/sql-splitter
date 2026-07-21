//! Tests for the synthetic-data-generation YAML document model
//! (`SyntheticFile`, `SyntheticModel`, `SyntheticOverrides`, and the
//! tri-state seed types) and its local-import loader (`ConfigLoader`).

use std::fs;

use sql_splitter::synthetic::{
    ConfigLoader, ModelMerger, RootSeedOverride, SyntheticFile, SyntheticModel, SyntheticOverrides,
    TableSeed, TableSeedOverride,
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
    // `type: decimal(12` pair and a stray `2)` key. This test quotes the value
    // explicitly to preserve the intended scalar without hiding the design
    // example's pre-existing quoting gap.
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

    // Round-trip: re-serialize and re-parse without loss of the values the
    // compiler depends on. Re-serialization always emits the canonical
    // source_type/family form, never the `type:` shorthand this test parsed.
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

#[test]
fn imports_reject_collisions_but_root_may_override() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("a.yaml"),
        "version: 1\nkind: overrides\ntables:\n  users:\n    seed: 1\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("b.yaml"),
        "version: 1\nkind: overrides\ntables:\n  users:\n    seed: 2\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("bad.yaml"),
        "version: 1\nkind: overrides\nimports: [a.yaml, b.yaml]\ntables: {}\n",
    )
    .unwrap();
    let err = ConfigLoader::load(&dir.path().join("bad.yaml")).unwrap_err();
    assert!(err.to_string().contains("GEN-IMPORT-COLLISION"));
    assert!(err.to_string().contains("tables.users.seed"));
    assert!(err.to_string().contains("a.yaml"));
    assert!(err.to_string().contains("b.yaml"));

    fs::write(
        dir.path().join("good.yaml"),
        "version: 1\nkind: overrides\nimports: [a.yaml]\ntables:\n  users:\n    seed: 9\n",
    )
    .unwrap();
    let loaded = ConfigLoader::load(&dir.path().join("good.yaml")).unwrap();
    assert_eq!(
        loaded.into_overrides().unwrap().tables["users"].seed,
        TableSeedOverride::Fixed(9)
    );
}

#[test]
fn import_paths_must_be_local_and_relative() {
    let dir = tempfile::tempdir().unwrap();
    let absolute = dir.path().join("shared.yaml");
    fs::write(&absolute, "version: 1\nkind: overrides\ntables: {}\n").unwrap();
    let root_yaml = format!(
        "version: 1\nkind: overrides\nimports: [\"{}\"]\ntables: {{}}\n",
        absolute.display()
    );
    fs::write(dir.path().join("root.yaml"), root_yaml).unwrap();

    let err = ConfigLoader::load(&dir.path().join("root.yaml")).unwrap_err();
    assert!(err.to_string().contains("GEN-IMPORT-REMOTE"));
}

#[test]
fn import_paths_must_not_traverse_out_of_the_model_directory() {
    let dir = tempfile::tempdir().unwrap();
    // A file OUTSIDE the model directory.
    fs::write(
        dir.path().join("secret.yaml"),
        "version: 1\nkind: overrides\ntables: {}\n",
    )
    .unwrap();
    // The model lives in a subdirectory; a `../secret.yaml` import escapes it.
    let sub = dir.path().join("model");
    fs::create_dir(&sub).unwrap();
    fs::write(
        sub.join("root.yaml"),
        "version: 1\nkind: overrides\nimports: [\"../secret.yaml\"]\ntables: {}\n",
    )
    .unwrap();

    let err = ConfigLoader::load(&sub.join("root.yaml")).unwrap_err();
    assert!(
        err.to_string().contains("GEN-IMPORT-REMOTE"),
        "a traversing import path must be rejected: {err}"
    );
}

#[test]
fn imports_cannot_themselves_import() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("leaf.yaml"),
        "version: 1\nkind: overrides\ntables: {}\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("nested.yaml"),
        "version: 1\nkind: overrides\nimports: [leaf.yaml]\ntables: {}\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("root.yaml"),
        "version: 1\nkind: overrides\nimports: [nested.yaml]\ntables: {}\n",
    )
    .unwrap();

    let err = ConfigLoader::load(&dir.path().join("root.yaml")).unwrap_err();
    assert!(err.to_string().contains("GEN-IMPORT-NESTED"));
}

#[test]
fn imported_model_kind_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("model.yaml"),
        "version: 1\nkind: model\ntables: {}\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("root.yaml"),
        "version: 1\nkind: overrides\nimports: [model.yaml]\ntables: {}\n",
    )
    .unwrap();

    let err = ConfigLoader::load(&dir.path().join("root.yaml")).unwrap_err();
    assert!(err.to_string().contains("GEN-IMPORT-KIND"));
}

#[test]
fn duplicate_keys_in_an_imported_file_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("dupe.yaml"),
        "version: 1\nkind: overrides\ntables: {}\ntables: {}\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("root.yaml"),
        "version: 1\nkind: overrides\nimports: [dupe.yaml]\ntables: {}\n",
    )
    .unwrap();

    let err = ConfigLoader::load(&dir.path().join("root.yaml")).unwrap_err();
    assert!(err.to_string().to_lowercase().contains("duplicate"));
}

#[test]
fn root_lists_replace_rather_than_concatenate_imported_lists() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("base.yaml"),
        "version: 1\nkind: overrides\ntables:\n  orders:\n    relationships:\n      - { columns: [customer_id], references: { table: customers, columns: [id] } }\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("root.yaml"),
        "version: 1\nkind: overrides\nimports: [base.yaml]\ntables:\n  orders:\n    relationships:\n      - { columns: [warehouse_id], references: { table: warehouses, columns: [id] } }\n",
    )
    .unwrap();

    let loaded = ConfigLoader::load(&dir.path().join("root.yaml")).unwrap();
    let overrides = loaded.into_overrides().unwrap();
    let relationships = overrides.tables["orders"].relationships.as_ref().unwrap();
    assert_eq!(relationships.len(), 1);
    assert_eq!(relationships[0].columns, vec!["warehouse_id".to_string()]);
}

#[test]
fn imports_disagreeing_on_shape_at_a_shared_path_collide_leaf_then_map() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("a.yaml"),
        "version: 1\nkind: overrides\ntables:\n  users:\n    schema: null\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("b.yaml"),
        "version: 1\nkind: overrides\ntables:\n  users:\n    schema: { name: users }\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("root.yaml"),
        "version: 1\nkind: overrides\nimports: [a.yaml, b.yaml]\ntables: {}\n",
    )
    .unwrap();

    let err = ConfigLoader::load(&dir.path().join("root.yaml")).unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("GEN-IMPORT-COLLISION"));
    assert!(rendered.contains("tables.users.schema"));
    assert!(rendered.contains("a.yaml"));
    assert!(rendered.contains("b.yaml"));
}

#[test]
fn imports_disagreeing_on_shape_at_a_shared_path_collide_map_then_leaf() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("a.yaml"),
        "version: 1\nkind: overrides\ntables:\n  users:\n    schema: { name: users }\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("b.yaml"),
        "version: 1\nkind: overrides\ntables:\n  users:\n    schema: null\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("root.yaml"),
        "version: 1\nkind: overrides\nimports: [a.yaml, b.yaml]\ntables: {}\n",
    )
    .unwrap();

    let err = ConfigLoader::load(&dir.path().join("root.yaml")).unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("GEN-IMPORT-COLLISION"));
    assert!(rendered.contains("tables.users.schema"));
    assert!(rendered.contains("a.yaml"));
    assert!(rendered.contains("b.yaml"));
}

#[test]
fn imports_writing_disjoint_nested_keys_under_shared_map_paths_merge_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    fs::write(
        dir.path().join("a.yaml"),
        "version: 1\nkind: overrides\ntables:\n  users:\n    seed: 1\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("b.yaml"),
        "version: 1\nkind: overrides\ntables:\n  orders:\n    seed: 2\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("root.yaml"),
        "version: 1\nkind: overrides\nimports: [a.yaml, b.yaml]\ntables: {}\n",
    )
    .unwrap();

    let loaded = ConfigLoader::load(&dir.path().join("root.yaml")).unwrap();
    let overrides = loaded.into_overrides().unwrap();
    assert_eq!(overrides.tables["users"].seed, TableSeedOverride::Fixed(1));
    assert_eq!(overrides.tables["orders"].seed, TableSeedOverride::Fixed(2));
}

// --- ModelMerger: merging a base model with a typed overrides patch ---
//
// `PortableTableOverride` (src/synthetic/overrides.rs) carries only `name`
// and `create_statement` — there is no column-level type/nullability/key
// field to override at all, so the type system alone already forecloses
// column-structural overrides. The one table-level structural fact these
// tests can assert-and-reject is `schema.name`: an override may assert it
// matches the base, but not silently rename the table.

fn base_users_model(email_type: &str) -> SyntheticModel {
    let yaml = format!(
        r#"
version: 1
kind: model
tables:
  users:
    rows: {{ kind: fixed, count: 10 }}
    schema:
      name: users
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: email, type: "{email_type}", nullable: false }}
    columns:
      email:
        generator: {{ kind: static, value: "a@example.com" }}
"#
    );
    SyntheticFile::parse_str(&yaml)
        .unwrap()
        .into_model()
        .unwrap()
}

fn overrides_from_yaml(yaml: &str) -> SyntheticOverrides {
    SyntheticFile::parse_str(yaml)
        .unwrap()
        .into_overrides()
        .unwrap()
}

#[test]
fn overrides_change_rules_but_not_source_schema() {
    let base = base_users_model("varchar(255)");

    let rules = overrides_from_yaml(
        r#"
version: 1
kind: overrides
tables:
  users:
    columns:
      email:
        generator: { kind: internet.email }
"#,
    );
    let (merged, warnings) = ModelMerger::merge(base.clone(), rules).unwrap();
    assert!(warnings.diagnostics.is_empty());
    assert_eq!(
        merged.tables["users"].columns["email"]
            .generator
            .as_ref()
            .unwrap()
            .kind,
        "internet.email"
    );

    let structural = overrides_from_yaml(
        r#"
version: 1
kind: overrides
tables:
  users:
    schema: { name: people }
"#,
    );
    let err = ModelMerger::merge(base, structural).unwrap_err();
    assert!(err.to_string().contains("GEN-SCHEMA-MISMATCH"));
}

#[test]
fn overrides_referencing_missing_table_report_diagnostic() {
    let base = base_users_model("varchar(255)");
    let overrides = overrides_from_yaml(
        r#"
version: 1
kind: overrides
tables:
  ghosts:
    seed: 1
"#,
    );
    let err = ModelMerger::merge(base, overrides).unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("GEN-MISSING-TABLE"));
    assert!(rendered.contains("tables.ghosts"));
}

#[test]
fn overrides_referencing_missing_column_report_diagnostic() {
    let base = base_users_model("varchar(255)");
    let overrides = overrides_from_yaml(
        r#"
version: 1
kind: overrides
tables:
  users:
    columns:
      bogus:
        semantic: name
"#,
    );
    let err = ModelMerger::merge(base, overrides).unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("GEN-MISSING-COLUMN"));
    assert!(rendered.contains("tables.users.columns.bogus"));
}

#[test]
fn merge_gathers_independent_diagnostics_rather_than_stopping_at_the_first() {
    let base = base_users_model("varchar(255)");
    let overrides = overrides_from_yaml(
        r#"
version: 1
kind: overrides
tables:
  users:
    columns:
      bogus:
        semantic: name
  ghosts:
    seed: 1
"#,
    );
    let err = ModelMerger::merge(base, overrides).unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("GEN-MISSING-COLUMN"));
    assert!(rendered.contains("GEN-MISSING-TABLE"));
}

fn base_orders_model() -> SyntheticModel {
    let yaml = r#"
version: 1
kind: model
tables:
  customers:
    rows: { kind: fixed, count: 1 }
    schema:
      name: customers
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
  orders:
    rows: { kind: fixed, count: 1 }
    schema:
      name: orders
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: customer_id, type: bigint, nullable: false }
    relationships:
      - name: orders_customer
        columns: [customer_id]
        references: { table: customers, columns: [id] }
"#;
    SyntheticFile::parse_str(yaml)
        .unwrap()
        .into_model()
        .unwrap()
}

#[test]
fn table_override_relationships_replace_rather_than_append() {
    let base = base_orders_model();
    assert_eq!(base.tables["orders"].relationships.len(), 1);

    let overrides = overrides_from_yaml(
        r#"
version: 1
kind: overrides
tables:
  orders:
    relationships:
      - columns: [warehouse_id]
        references: { table: warehouses, columns: [id] }
"#,
    );
    let (merged, warnings) = ModelMerger::merge(base, overrides).unwrap();
    assert!(warnings.diagnostics.is_empty());
    let relationships = &merged.tables["orders"].relationships;
    assert_eq!(relationships.len(), 1);
    assert_eq!(relationships[0].columns, vec!["warehouse_id".to_string()]);
    assert!(relationships[0].name.is_none());
}

#[test]
fn table_override_seed_and_rows_apply_over_base() {
    let base = base_orders_model();
    let overrides = overrides_from_yaml(
        r#"
version: 1
kind: overrides
tables:
  orders:
    seed: 7
    rows: { kind: fixed, count: 500 }
"#,
    );
    let (merged, warnings) = ModelMerger::merge(base, overrides).unwrap();
    assert!(warnings.diagnostics.is_empty());
    assert_eq!(merged.tables["orders"].seed, TableSeed::Fixed(7));
    assert_eq!(
        merged.tables["orders"].rows,
        sql_splitter::synthetic::RowsModel::Fixed { count: 500 }
    );
}

fn base_with_source(policy: &str) -> SyntheticModel {
    let yaml = format!(
        r#"
version: 1
kind: model
source:
  dialect: mysql
  fingerprint: "sha256:aaa"
  fingerprint_policy: {policy}
tables: {{}}
"#
    );
    SyntheticFile::parse_str(&yaml)
        .unwrap()
        .into_model()
        .unwrap()
}

fn overrides_with_new_fingerprint() -> SyntheticOverrides {
    overrides_from_yaml(
        r#"
version: 1
kind: overrides
source:
  dialect: mysql
  fingerprint: "sha256:bbb"
"#,
    )
}

#[test]
fn fingerprint_mismatch_under_ignore_policy_applies_silently() {
    let base = base_with_source("ignore");
    let (merged, warnings) = ModelMerger::merge(base, overrides_with_new_fingerprint()).unwrap();
    assert!(warnings.diagnostics.is_empty());
    assert_eq!(
        merged.source.unwrap().fingerprint.as_deref(),
        Some("sha256:bbb")
    );
}

#[test]
fn fingerprint_mismatch_under_warn_policy_does_not_block_the_merge() {
    let base = base_with_source("warn");
    let (merged, warnings) = ModelMerger::merge(base, overrides_with_new_fingerprint()).unwrap();
    assert!(!warnings.diagnostics.is_empty());
    assert_eq!(
        merged.source.unwrap().fingerprint.as_deref(),
        Some("sha256:bbb")
    );
}

#[test]
fn fingerprint_warn_on_successful_merge_surfaces_warning() {
    // The signature is `Result<(SyntheticModel, DiagnosticBag), DiagnosticBag>`
    // specifically so a `warn`-policy mismatch is observable even when
    // nothing else about the merge fails: the model comes back in `Ok`,
    // and so does the warning, rather than the warning being silently
    // dropped by an `Ok(SyntheticModel)`-only success type.
    let base = base_with_source("warn");
    let (merged, warnings) = ModelMerger::merge(base, overrides_with_new_fingerprint()).unwrap();

    assert!(!warnings.has_errors());
    assert_eq!(
        merged.source.unwrap().fingerprint.as_deref(),
        Some("sha256:bbb")
    );
    assert!(warnings
        .diagnostics
        .iter()
        .any(|d| d.code == "GEN-SOURCE-FINGERPRINT"));
}

#[test]
fn fingerprint_mismatch_under_warn_policy_is_reported_alongside_other_errors() {
    // Even when a warning would already surface on its own (see
    // `fingerprint_warn_on_successful_merge_surfaces_warning`), an
    // unrelated hard error must still force the `Err` path and keep every
    // diagnostic, warnings included.
    let base = base_with_source("warn");
    let overrides = overrides_from_yaml(
        r#"
version: 1
kind: overrides
source:
  dialect: mysql
  fingerprint: "sha256:bbb"
tables:
  ghosts:
    seed: 1
"#,
    );
    let err = ModelMerger::merge(base, overrides).unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("GEN-SOURCE-FINGERPRINT"));
    assert!(rendered.contains("GEN-MISSING-TABLE"));
}

#[test]
fn fingerprint_mismatch_under_require_policy_blocks_the_merge() {
    let base = base_with_source("require");
    let err = ModelMerger::merge(base, overrides_with_new_fingerprint()).unwrap_err();
    assert!(err.to_string().contains("GEN-SOURCE-FINGERPRINT"));
}

#[test]
fn matching_fingerprint_produces_no_diagnostic_even_under_require_policy() {
    let base = base_with_source("require");
    let overrides = overrides_from_yaml(
        r#"
version: 1
kind: overrides
source:
  dialect: mysql
  fingerprint: "sha256:aaa"
"#,
    );
    let (merged, warnings) = ModelMerger::merge(base, overrides).unwrap();
    assert!(warnings.diagnostics.is_empty());
    assert_eq!(
        merged.source.unwrap().fingerprint.as_deref(),
        Some("sha256:aaa")
    );
}

// --- GEN-INCOMPLETE-ROWS: a `rows:` override switching `kind` without
// supplying every field that kind needs, with no base value of the same
// kind to fall back on. One test per `RowsKind` branch that can emit it. ---

fn base_with_rows(rows_yaml: &str) -> SyntheticModel {
    let yaml = format!(
        r#"
version: 1
kind: model
tables:
  t:
    rows: {rows_yaml}
    schema:
      name: t
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
"#
    );
    SyntheticFile::parse_str(&yaml)
        .unwrap()
        .into_model()
        .unwrap()
}

#[test]
fn rows_override_switching_to_fixed_without_count_is_incomplete() {
    // Base rows are `observed`, not `fixed`, so there is no base count to
    // fall back on.
    let base = base_with_rows("{ kind: observed, count: 100 }");
    let overrides = overrides_from_yaml(
        r#"
version: 1
kind: overrides
tables:
  t:
    rows: { kind: fixed }
"#,
    );
    let err = ModelMerger::merge(base, overrides).unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("GEN-INCOMPLETE-ROWS"));
    assert!(rendered.contains("tables.t.rows"));
}

#[test]
fn rows_override_switching_to_observed_without_count_or_scale_is_incomplete() {
    // Base rows are `fixed`, not `observed`, so there is no base observed
    // count for a `scale` to multiply, and no `count` is given either.
    let base = base_with_rows("{ kind: fixed, count: 10 }");
    let overrides = overrides_from_yaml(
        r#"
version: 1
kind: overrides
tables:
  t:
    rows: { kind: observed }
"#,
    );
    let err = ModelMerger::merge(base, overrides).unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("GEN-INCOMPLETE-ROWS"));
    assert!(rendered.contains("tables.t.rows"));
}

#[test]
fn rows_override_switching_to_scale_without_base_or_factor_is_incomplete() {
    let base = base_with_rows("{ kind: fixed, count: 10 }");
    let overrides = overrides_from_yaml(
        r#"
version: 1
kind: overrides
tables:
  t:
    rows: { kind: scale }
"#,
    );
    let err = ModelMerger::merge(base, overrides).unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("GEN-INCOMPLETE-ROWS"));
    assert!(rendered.contains("tables.t.rows"));
}

#[test]
fn rows_override_switching_to_relation_children_without_required_fields_is_incomplete() {
    let base = base_with_rows("{ kind: fixed, count: 10 }");
    let overrides = overrides_from_yaml(
        r#"
version: 1
kind: overrides
tables:
  t:
    rows: { kind: relation.children }
"#,
    );
    let err = ModelMerger::merge(base, overrides).unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("GEN-INCOMPLETE-ROWS"));
    assert!(rendered.contains("tables.t.rows"));
}

#[test]
fn rows_override_switching_kind_with_all_required_fields_succeeds() {
    // The counterpart to the four incomplete-rows tests above: supplying
    // every field the new `kind` needs is legal even though the base is a
    // different `kind`.
    let base = base_with_rows("{ kind: fixed, count: 10 }");
    let overrides = overrides_from_yaml(
        r#"
version: 1
kind: overrides
tables:
  t:
    rows: { kind: scale, base: 10, factor: 2.5 }
"#,
    );
    let (merged, warnings) = ModelMerger::merge(base, overrides).unwrap();
    assert!(warnings.diagnostics.is_empty());
    assert_eq!(
        merged.tables["t"].rows,
        sql_splitter::synthetic::RowsModel::Scale {
            base: 10,
            factor: 2.5,
            count: 25,
        }
    );
}

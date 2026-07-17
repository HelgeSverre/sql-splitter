# Config schema reuse assessment: sample, shard, redact

**Status:** Assessment only — no implementation authorized by this document

**Product surface:** `sql-splitter sample`, `sql-splitter shard`, `sql-splitter redact` YAML `--config` files

**Context:** Task 35 added `generate_config_schema()` (`src/json_schema.rs:154-215`), a registry-descriptor→`oneOf` composition helper that publishes `generate-config.schema.json` for the `generate` command's `kind: model` / `kind: overrides` YAML. This document evaluates whether that helper, or JSON Schema publication generally, should be reused for the other YAML-backed commands, per Task 36 of the synthetic-data-generation plan.

## Inventory

Every command was checked for a `--config`-style YAML flag (`grep -rn -- "--config\|config_file\|serde_yaml_ng::from_str" src/cmd/*.rs`). Only three non-`generate` commands take a YAML config file at all; the rest (`analyze`, `convert`, `diff`, `merge`, `graph`, `order`, `query`, `split`, `validate`) are CLI-flags-only and are out of scope for this assessment.

| Command  | YAML config flag | Config type(s)                                                                 | `deny_unknown_fields`? | Registry-like extension surface? | Committed fixture corpus? |
| -------- | ----------------- | -------------------------------------------------------------------------------- | ----------------------- | --------------------------------- | -------------------------- |
| generate | `generate <file>` (the YAML itself, not a side `--config`) | `SyntheticModel`, `SyntheticOverrides` (`src/synthetic/model.rs`, `src/synthetic/overrides.rs`, `src/synthetic/schema.rs`) | Yes — used throughout `src/synthetic/model.rs`, `src/synthetic/overrides.rs`, `src/synthetic/schema.rs` | Yes — `ExtensionRegistry` (`src/generate/registry.rs`) with generator/modifier/planner factories and `{kind, ...args}` shapes | Yes, extensively (`tests/generate_*`) |
| redact   | `-c/--config <file>` (`src/cmd/redact.rs:23-24`) | `RedactYamlConfig`, `Rule`, `Defaults` (`src/redactor/config.rs:322-366`); strategy payload is `StrategyKind` (`src/redactor/strategy/mod.rs:33-70`) | No — grep for `deny_unknown_fields` across the repo returns only `src/synthetic/{model,overrides,schema}.rs` | No — `StrategyKind` is a fixed, compile-time, `#[serde(tag = "strategy")]` enum with 7 variants; there is no runtime registration or factory list for it | No — only inline YAML strings inside test bodies (e.g. `tests/redact_test.rs:671-675`) |
| sample   | `-c/--config <file>` (`src/cmd/sample.rs:27`) | `SampleYamlConfig`, `TableConfig`, `DefaultConfig`, `ClassificationConfig` (`src/sample/config.rs:67-119`), loaded via `serde_yaml_ng::from_str` at `src/sample/config.rs:123-127` | No | No — plain nested structs/enums/`HashMap<String, TableConfig>`, no factories | No — inline YAML string built in the test body (`tests/sample_integration_test.rs:251-271`) |
| shard    | `-c/--config <file>` (`src/cmd/shard.rs:27`) | `ShardYamlConfig`, `TableOverride`, `TenantConfig` (`src/shard/config.rs:82-118`), loaded via `serde_yaml_ng::from_str` at `src/shard/config.rs:120-126` | No | No — same shape as `sample`: plain structs/enums/`HashMap<String, TableOverride>` | No — no config-file fixture found in `tests/shard_integration_test.rs`/`tests/shard_unit_test.rs` |

None of `RedactYamlConfig`, `Rule`, `Defaults`, `StrategyKind`, `SampleYamlConfig`, `TableConfig`, `DefaultConfig`, `ClassificationConfig`, `ShardYamlConfig`, `TableOverride`, or `TenantConfig` derive `JsonSchema` today (`grep -n "JsonSchema" src/{sample,shard}/config.rs src/redactor/config.rs src/redactor/strategy/mod.rs` returns nothing).

**Important naming caveat:** `src/json_schema.rs:52-71` already registers schema entries named `"redact"`, `"sample"`, and `"shard"` in `all_schemas()`. Those are the commands' `--json` *output* schemas (`RedactStats`, `SampleJsonOutput`, `ShardJsonOutput`), validated today by `tests/json_schema_tests.rs`. They are unrelated to the YAML *input* config files discussed here and must not be confused with them — an input-config schema for any of these three commands would need a distinct name (e.g. `redact-config`, `sample-config`, `shard-config`) to avoid a collision with the existing output-schema entry.

## Comparing reuse of the Task 35 helper

Task 35's helper (`generate_config_schema()` plus `operator_branch`/`replace_def`, `src/json_schema.rs:154-256`) exists to solve one specific problem: `GeneratorConfig`/`ModifierConfig`/`PlannerConfig` values are `{kind, ...args}` where `args` is an untyped, registry-resolved `BTreeMap<String, serde_yaml_ng::Value>` (see doc comment at `src/json_schema.rs:107-121`). `schemars` alone can only describe that as "any object," so the helper walks `ExtensionRegistry::standard()`'s descriptors and synthesizes a `oneOf` branch per registered `kind`, keyed on declared `ArgumentSpec`s. That machinery is only valuable when a config field's shape is determined by a **runtime registry of factories**, not by a fixed Rust type.

None of `redact`, `sample`, or `shard` has that shape:

- **redact's `StrategyKind`** is a closed, compile-time `#[serde(tag = "strategy")]` enum (`Null | Constant | Hash | Mask | Shuffle | Fake | Skip`). `#[derive(JsonSchema)]` on an internally-tagged enum already produces a correct `oneOf` natively in `schemars` — no registry walk, no custom `oneOf` composition, no `operator_branch`-style helper needed. Reuse here means: derive `JsonSchema` directly on `StrategyKind`, `Rule`, `Defaults`, `RedactYamlConfig`; nothing from `json_schema.rs`'s registry-specific code is applicable.
- **sample's and shard's YAML configs** are plain nested structs, `Copy` enums, and `HashMap<String, T>` maps with no factory/`kind` discriminator at all. Reuse here means: derive `JsonSchema` on the existing types and add one `all_schemas()`-style entry function analogous in *shape* to `generate_config_schema()` (a title/`$id`/description wrapper around `generator.into_root_schema_for::<T>()`), but with none of the `oneOf`/registry composition body — that body would have nothing to compose against.

Do not merge these into one model language: `generate`'s registry-driven `{kind, ...args}` shape and `redact`/`sample`/`shard`'s closed typed structs are different design points for different reasons (open extension vs. closed built-in option set), and Task 35's helper should stay scoped to the registry case it was built for.

## Per-command decision

### redact — LATER

**Prerequisite (exact):**
1. Add `#[serde(deny_unknown_fields)]` to `RedactYamlConfig`, `Rule`, and `Defaults` in `src/redactor/config.rs` (today a typo such as `stratgey:` is silently ignored by `serde_yaml_ng::from_str`, not rejected — see `src/redactor/config.rs:349`).
2. Derive `JsonSchema` on `RedactYamlConfig`, `Rule`, `Defaults`, and `StrategyKind`, and add a `redact-config` entry (distinct from the existing `redact` output-schema entry) to `all_schemas()` in `src/json_schema.rs`.
3. Commit at least one fixture YAML file exercising every `StrategyKind` variant (none exists today; current coverage is inline test-body YAML in `tests/redact_test.rs:671-675`).

**Acceptance test:** A `jsonschema`-crate test (following the pattern in `tests/json_schema_tests.rs`) that (a) validates the committed fixture(s) against `redact-config.schema.json` with zero errors, and (b) asserts that a mutated copy of the fixture with an unrecognized top-level key or an unrecognized `strategy` value is rejected by both the schema and the real `RedactYamlConfig::load` parse path (parity check — once step 1 above lands, "rejected by schema" and "rejected at runtime" must agree).

### sample — LATER

**Prerequisite (exact):**
1. Add `#[serde(deny_unknown_fields)]` to `SampleYamlConfig`, `TableConfig`, `DefaultConfig`, and `ClassificationConfig` in `src/sample/config.rs` (today all four use `#[serde(default)]` only, so unknown fields are silently dropped rather than rejected).
2. Derive `JsonSchema` on those four types and add a `sample-config` entry to `all_schemas()`.
3. Commit a fixture YAML file with `default`, `classification`, and at least one `tables` entry (today coverage is only the inline YAML string in `tests/sample_integration_test.rs:251-271`).

**Acceptance test:** Validate the committed fixture against `sample-config.schema.json` with zero errors, and assert a mutated copy with an unknown field (e.g. a typo'd `precent:` instead of `percent:`) is rejected by both the schema and `SampleYamlConfig::load`.

### shard — LATER

**Prerequisite (exact):**
1. Add `#[serde(deny_unknown_fields)]` to `ShardYamlConfig`, `TableOverride`, and `TenantConfig` in `src/shard/config.rs` (same silent-unknown-field gap as `sample`, since all three use `#[serde(default)]` only).
2. Derive `JsonSchema` on those three types plus `ShardTableClassification`/`GlobalTableMode`, and add a `shard-config` entry to `all_schemas()`.
3. Commit a fixture YAML file covering `tenant`, at least one `tables` override, and `include_global` (no config-file fixture exists in `tests/shard_integration_test.rs` or `tests/shard_unit_test.rs` today).

**Acceptance test:** Validate the committed fixture against `shard-config.schema.json` with zero errors, and assert a mutated copy with an unknown key (e.g. `roles:` instead of `role:` inside a `TableOverride`) is rejected by both the schema and `ShardYamlConfig::load`.

## Why none of the three is an immediate (YES) fit

Publishing a schema ahead of strict runtime parsing would let the schema and the real parser disagree about what's valid: today an unknown key in any of these three configs is silently ignored by `serde_yaml_ng::from_str`, so a schema that reports it as an error would contradict actual runtime behavior, while a schema left permissive enough to match today's lax parsing would not catch the typos it exists to catch. Each per-command prerequisite above therefore starts with closing that gap (`deny_unknown_fields`) before deriving a schema — matching the design's "runtime validation remains authoritative" principle already applied to `generate`.

None is a hard **NO** either: unlike the plan's deferred packets, these three commands are plain typed structs (or, for `redact`, one closed enum) with no registry to build, no new dependency, and no expression language risk. Once the `deny_unknown_fields` prerequisite lands for a given command, adding its `#[derive(JsonSchema)]` and one `all_schemas()` entry is a small, mechanical, single-command change — closer in effort to `analyze`/`convert`/`split`'s existing output-schema entries than to Task 35's registry work.

## If a command becomes an immediate fit

None does today. If a future task closes the `deny_unknown_fields` gap for `redact`, `sample`, or `shard`, schema publication for that command still needs its own **separately approved implementation plan** — scoped to one command per plan/commit, per the project convention of not combining unrelated task commits. This assessment does not authorize writing that code.

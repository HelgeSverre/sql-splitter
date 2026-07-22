//! Verify generated SQL before publication.
//!
//! These tests reparse generated SQL with the production parser and assert that
//! each declared constraint is audited exactly: a single corrupted row for
//! arity, non-null, primary-key, unique, foreign-key, composite foreign-key,
//! interval, progress, and order-family invariants makes the *named* exact check
//! fail, and the report labels an approximate distribution comparison as sampled
//! (never exact). The full `--verify` lifecycle publishes atomically only after a
//! passing audit and leaves a prior destination untouched on failure.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use sql_splitter::generate::{
    AtomicOutput, CheckStatus, CompileOptions, DistributionExpectation, Generate, GenerationEngine,
    GenerationPlan, GenerationVerifier, ModelCompiler, RenderOptions,
};
use sql_splitter::parser::SqlDialect;
use sql_splitter::render::SqlRenderer;
use sql_splitter::synthetic::SyntheticFile;

fn compile(model_yaml: &str) -> GenerationPlan {
    let model = SyntheticFile::parse_str(model_yaml)
        .expect("valid model YAML")
        .into_model()
        .expect("document is a model");
    ModelCompiler::standard()
        .compile(model, CompileOptions::default())
        .expect("model compiles cleanly")
}

/// Render a compiled plan to a MySQL SQL string (consumes the plan).
fn render(plan: GenerationPlan) -> String {
    let mut buffer = Vec::new();
    let mut renderer = SqlRenderer::new(&mut buffer, RenderOptions::default());
    GenerationEngine::new(plan).run(&mut renderer).unwrap();
    renderer.finish().unwrap();
    String::from_utf8(buffer).unwrap()
}

fn write(dir: &Path, name: &str, contents: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, contents).unwrap();
    path
}

/// Verify a corrupted copy of the valid SQL and return the report.
fn verify_corrupted(
    plan: GenerationPlan,
    dir: &Path,
    mutate: impl FnOnce(String) -> String,
) -> sql_splitter::generate::VerificationReport {
    let verifier = GenerationVerifier::new(&plan);
    let sql = render(plan);
    let corrupted = mutate(sql);
    let path = write(dir, "corrupt.sql", &corrupted);
    verifier.verify_path(&path).unwrap()
}

/// Replace the first occurrence of `from` with `to`, asserting it was present so
/// a corruption test never silently no-ops.
fn replace_once(sql: String, from: &str, to: &str) -> String {
    assert!(sql.contains(from), "expected `{from}` in generated SQL");
    sql.replacen(from, to, 1)
}

/// Rewrite the `index`-th value of the first tuple of the INSERT beginning with
/// `marker`. Safe for the numeric/timestamp/decimal columns these tests target
/// (no `", "` inside a value).
fn rewrite_first_tuple_value(sql: String, marker: &str, index: usize, new_value: &str) -> String {
    let ins = sql
        .find(marker)
        .unwrap_or_else(|| panic!("`{marker}` present"));
    let values = sql[ins..].find("VALUES").expect("VALUES keyword") + ins;
    let open = sql[values..].find('(').expect("first tuple open") + values;
    let close = sql[open..].find(')').expect("first tuple close") + open;
    let mut parts: Vec<String> = sql[open + 1..close].split(", ").map(String::from).collect();
    assert!(index < parts.len(), "tuple has no value at index {index}");
    parts[index] = new_value.to_string();
    format!(
        "{}({}){}",
        &sql[..open],
        parts.join(", "),
        &sql[close + 1..]
    )
}

/// Rewrite the `index`-th value of the *last* tuple of the INSERT beginning with
/// `marker` (the statement ending at the first `;` after `VALUES`). Lets a test
/// corrupt a different row than [`rewrite_first_tuple_value`].
fn rewrite_last_tuple_value(sql: String, marker: &str, index: usize, new_value: &str) -> String {
    let ins = sql
        .find(marker)
        .unwrap_or_else(|| panic!("`{marker}` present"));
    let values = sql[ins..].find("VALUES").expect("VALUES keyword") + ins;
    let stmt_end = sql[values..].find(';').expect("statement terminator") + values;
    let open = sql[values..stmt_end].rfind('(').expect("last tuple open") + values;
    let close = sql[open..stmt_end].find(')').expect("last tuple close") + open;
    let mut parts: Vec<String> = sql[open + 1..close].split(", ").map(String::from).collect();
    assert!(index < parts.len(), "tuple has no value at index {index}");
    parts[index] = new_value.to_string();
    format!(
        "{}({}){}",
        &sql[..open],
        parts.join(", "),
        &sql[close + 1..]
    )
}

// A model exercising PK, single-column UNIQUE, and a foreign key. Integer
// sequence columns keep the generated ids deterministic (1.. and 100..), so a
// corruption test can target exact literals.
const CORE: &str = r#"
version: 1
kind: model
defaults: { inference: disabled }
seed: 7
tables:
  users:
    rows: { kind: fixed, count: 4 }
    schema:
      name: users
      primary_key: [id]
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: code, type: bigint, nullable: false, unique: true }
        - { name: name, type: "varchar(32)", nullable: false }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
      code: { generator: { kind: sequence, start: 100 } }
      name: { generator: { kind: string, min_length: 4, max_length: 4 } }
  orders:
    rows:
      kind: relation.children
      parent: users
      count: 6
      distribution: { kind: fixed, mean: 1.5, min: 1, max: 100 }
    schema:
      name: orders
      primary_key: [id]
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: user_id, type: bigint, nullable: false }
    relationships:
      - { name: orders_user, columns: [user_id], references: { table: users, columns: [id] } }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
      user_id: { generator: { kind: relation.foreign_key, relationship: orders_user } }
"#;

#[test]
fn valid_output_passes_every_exact_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    let verifier = GenerationVerifier::new(&plan);
    let sql = render(plan);
    let path = write(dir.path(), "ok.sql", &sql);

    let report = verifier.verify_path(&path).unwrap();
    assert!(
        report.passed(),
        "valid output should verify; failures: {:?}",
        report.failures().collect::<Vec<_>>()
    );
    assert_eq!(
        report.status_of("row_count:users"),
        Some(CheckStatus::Exact)
    );
    assert_eq!(
        report.status_of("foreign_key:orders"),
        Some(CheckStatus::Exact)
    );
}

#[test]
fn corrupt_non_null_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    // Turn the first user's code value (100) into NULL.
    let report = verify_corrupted(plan, dir.path(), |sql| {
        replace_once(sql, "(1, 100,", "(1, NULL,")
    });
    assert!(report.failed("non_null:users"), "{:?}", report.checks);
}

#[test]
fn corrupt_arity_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    // Drop the code value from the first user row so its arity is wrong.
    let report = verify_corrupted(plan, dir.path(), |sql| {
        replace_once(sql, "(1, 100, ", "(1, ")
    });
    assert!(report.failed("arity:users"), "{:?}", report.checks);
}

#[test]
fn omitting_a_required_column_from_every_row_fails_exact_checks() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    let report = verify_corrupted(plan, dir.path(), |sql| {
        let sql = replace_once(
            sql,
            "INSERT INTO `users` (`id`, `code`, `name`) VALUES",
            "INSERT INTO `users` (`id`, `code`) VALUES",
        );
        let start = sql.find("INSERT INTO `users`").expect("users insert");
        let end = sql[start..].find(";\n").expect("users insert end") + start + 2;
        let rows = regex::Regex::new(r", '[^']*'\)")
            .unwrap()
            .replace_all(&sql[start..end], ")");
        format!("{}{}{}", &sql[..start], rows, &sql[end..])
    });
    assert!(report.failed("arity:users"), "{:?}", report.checks);
    assert!(report.failed("non_null:users"), "{:?}", report.checks);
}

#[test]
fn corrupt_primary_key_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    // Make the second user's id collide with the first.
    let report = verify_corrupted(plan, dir.path(), |sql| {
        replace_once(sql, "(2, 101,", "(1, 101,")
    });
    assert!(report.failed("primary_key:users"), "{:?}", report.checks);
}

#[test]
fn corrupt_unique_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    // Make the second user's code collide with the first (100).
    let report = verify_corrupted(plan, dir.path(), |sql| {
        replace_once(sql, "(2, 101,", "(2, 100,")
    });
    assert!(report.failed("unique:users"), "{:?}", report.checks);
}

#[test]
fn corrupt_foreign_key_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    // Repoint the first order's user_id to a non-existent parent (999).
    let report = verify_corrupted(plan, dir.path(), corrupt_first_order_fk);
    assert!(report.failed("foreign_key:orders"), "{:?}", report.checks);
}

const DUPLICATE_RELATIONSHIP_SLUGS: &str = r#"
version: 1
kind: model
defaults: { inference: disabled }
seed: 7
tables:
  users:
    rows: { kind: fixed, count: 2 }
    schema:
      name: users
      primary_key: [id]
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
  accounts:
    rows: { kind: fixed, count: 2 }
    schema:
      name: accounts
      primary_key: [id]
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
  orders:
    rows: { kind: fixed, count: 2 }
    schema:
      name: orders
      primary_key: [id]
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: user_id, type: bigint, nullable: false }
        - { name: account_id, type: bigint, nullable: false }
    relationships:
      - { name: shared_parent, columns: [user_id], references: { table: users, columns: [id] } }
      - { name: shared_parent, columns: [account_id], references: { table: accounts, columns: [id] } }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
      user_id: { generator: { kind: relation.foreign_key } }
      account_id: { generator: { kind: relation.foreign_key } }
"#;

#[test]
fn duplicate_relationship_slugs_do_not_overwrite_an_earlier_fk_failure() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(DUPLICATE_RELATIONSHIP_SLUGS);
    let orders = plan.table("orders").expect("orders table");
    assert_eq!(orders.relationships.len(), 2);
    assert!(orders
        .relationships
        .iter()
        .all(|relationship| relationship.name.as_deref() == Some("shared_parent")));

    // Only the earlier users relationship is invalid. The later accounts
    // relationship remains valid and must not overwrite the earlier failure.
    let report = verify_corrupted(plan, dir.path(), |sql| {
        rewrite_first_tuple_value(sql, "INSERT INTO `orders`", 1, "999")
    });
    let shared_checks: Vec<_> = report
        .checks
        .iter()
        .filter(|check| check.name == "foreign_key:orders:shared_parent")
        .collect();

    assert_eq!(shared_checks.len(), 2, "{:?}", report.checks);
    assert!(!shared_checks[0].passed, "{:?}", report.checks);
    assert!(shared_checks[1].passed, "{:?}", report.checks);
    assert!(!report.passed(), "{:?}", report.checks);
}

/// Repoint the first `orders` row's `user_id` to a non-existent parent by
/// rewriting the orders INSERT's first tuple to `(1,999)`.
fn corrupt_first_order_fk(sql: String) -> String {
    let idx = sql
        .find("INSERT INTO `orders`")
        .expect("orders insert present");
    let (head, tail) = sql.split_at(idx);
    let open = tail.find("(1, ").expect("first order tuple");
    let close = tail[open..].find(')').unwrap() + open;
    format!("{head}{}(1, 999){}", &tail[..open], &tail[close + 1..])
}

#[test]
fn full_verify_lifecycle_publishes_atomically_on_pass() {
    let dir = tempfile::tempdir().unwrap();
    let config = write(dir.path(), "model.yaml", CORE);
    let out = dir.path().join("published.sql");

    let report = Generate::builder()
        .config(&config)
        .output(&out)
        .verify(true)
        .run()
        .expect("verify + publish should succeed");

    assert!(report.rows_written > 0);
    assert!(out.exists(), "verified output should be published");
    assert!(fs::read_to_string(&out).unwrap().contains("INSERT INTO"));
}

// --- Composite foreign key -------------------------------------------------

const COMPOSITE: &str = r#"
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

#[test]
fn corrupt_composite_foreign_key_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(COMPOSITE);
    // Break the first reading's (cell_x, cell_y) pair so it references no cell.
    let report = verify_corrupted(plan, dir.path(), |sql| {
        rewrite_first_tuple_value(sql, "INSERT INTO `readings`", 1, "999999")
    });
    assert!(
        report.failed("composite_foreign_key:readings"),
        "{:?}",
        report.checks
    );
}

#[test]
fn partial_null_composite_foreign_key_is_not_a_false_failure() {
    let dir = tempfile::tempdir().unwrap();
    // Nullable composite FK columns so a partial NULL is legal.
    let model = COMPOSITE
        .replace(
            "{ name: cell_x, type: bigint, nullable: false }",
            "{ name: cell_x, type: bigint, nullable: true }",
        )
        .replace(
            "{ name: cell_y, type: bigint, nullable: false }",
            "{ name: cell_y, type: bigint, nullable: true }",
        );
    let plan = compile(&model);
    // NULL the first reading's cell_x while cell_y stays valid: a partial-null
    // composite key. Under SQL MATCH SIMPLE such a key is unenforced, so the FK
    // check must not fail.
    let report = verify_corrupted(plan, dir.path(), |sql| {
        rewrite_first_tuple_value(sql, "INSERT INTO `readings`", 0, "NULL")
    });
    assert!(
        !report.failed("composite_foreign_key:readings"),
        "a partial-null composite FK must not fail under MATCH SIMPLE: {:?}",
        report.checks
    );
}

// --- temporal.interval equation --------------------------------------------

const INTERVAL: &str = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 5
tables:
  jobs:
    rows: { kind: fixed, count: 4 }
    schema:
      name: jobs
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: started_at, type: timestamp, nullable: false }
        - { name: ended_at, type: timestamp, nullable: true }
        - { name: duration_seconds, type: bigint, nullable: true }
        - { name: is_running, type: boolean, nullable: false }
    planners:
      - kind: temporal.interval
        columns:
          start: started_at
          end: ended_at
          duration: duration_seconds
          open: is_running
        start: { kind: range, min: "2024-01-01T00:00:00Z", max: "2026-01-01T00:00:00Z" }
        duration: { kind: uniform, unit: seconds, min: 30, max: 43200 }
        end_inclusive: false
        timezone: utc
"#;

const INTERVAL_INCLUSIVE: &str = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 3
tables:
  jobs:
    rows: { kind: fixed, count: 50 }
    schema:
      name: jobs
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: started_at, type: timestamp, nullable: false }
        - { name: ended_at, type: timestamp, nullable: false }
        - { name: duration_seconds, type: bigint, nullable: false }
    planners:
      - kind: temporal.interval
        columns: { start: started_at, end: ended_at, duration: duration_seconds }
        start: { kind: range, min: "2024-01-01T00:00:00Z", max: "2025-01-01T00:00:00Z" }
        duration: { kind: uniform, unit: seconds, min: 60, max: 3600 }
        end_inclusive: true
        timezone: utc
"#;

#[test]
fn interval_end_inclusive_output_passes_its_own_verification() {
    // An inclusive interval renders `end = start + duration - 1` at second
    // precision. The rendered end must satisfy the verifier's equation
    // regardless of the sub-second component of the drawn start.
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(INTERVAL_INCLUSIVE);
    let verifier = GenerationVerifier::new(&plan);
    let sql = render(plan);
    let path = write(dir.path(), "incl.sql", &sql);
    let report = verifier.verify_path(&path).unwrap();
    assert!(
        report.passed(),
        "end_inclusive output must pass its own verification: {:?}",
        report.failures().collect::<Vec<_>>()
    );
}

#[test]
fn corrupt_interval_equation_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(INTERVAL);
    // Break end = start + duration by moving the first row's ended_at far away.
    // The rendered INSERT omits the database-produced `id`, so the tuple is
    // (started_at, ended_at, duration_seconds, is_running) — ended_at is index 1.
    let report = verify_corrupted(plan, dir.path(), |sql| {
        rewrite_first_tuple_value(sql, "INSERT INTO `jobs`", 1, "'2099-12-31 00:00:00'")
    });
    assert!(
        report.failed("planner_equation:jobs"),
        "{:?}",
        report.checks
    );
}

// --- workflow.progress_counters --------------------------------------------

const PROGRESS: &str = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 9
tables:
  jobs:
    rows: { kind: fixed, count: 4 }
    schema:
      name: jobs
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: total_rows, type: bigint, nullable: false }
        - { name: processed_rows, type: bigint, nullable: false }
        - { name: imported_rows, type: bigint, nullable: false }
        - { name: failed_rows, type: bigint, nullable: false }
        - { name: pending_rows, type: bigint, nullable: false }
        - { name: status, type: text, nullable: false }
        - { name: completed_at, type: timestamp, nullable: true }
    planners:
      - kind: workflow.progress_counters
        columns:
          total: total_rows
          processed: processed_rows
          succeeded: imported_rows
          failed: failed_rows
          pending: pending_rows
          status: status
          completed_at: completed_at
        total: { kind: uniform, min: 10, max: 1000 }
        progress: { kind: mixture, complete_weight: 0.5, active_weight: 0.3, not_started_weight: 0.2 }
        partition: exact
        completed_statuses: [completed, failed]
        active_statuses: [queued, running]
"#;

#[test]
fn corrupt_progress_counter_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(PROGRESS);
    // Break processed + pending == total by corrupting the first pending value.
    // The INSERT omits the database-produced `id`, so the tuple is
    // (total, processed, imported, failed, pending, status, completed_at) —
    // pending_rows is index 4.
    let report = verify_corrupted(plan, dir.path(), |sql| {
        rewrite_first_tuple_value(sql, "INSERT INTO `jobs`", 4, "424242")
    });
    assert!(
        report.failed("planner_counter_sum:jobs"),
        "{:?}",
        report.checks
    );
}

#[test]
fn progress_predicate_with_unparseable_input_is_not_a_silent_pass() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(PROGRESS);
    // Corrupt the first row's total_rows (index 0) to a non-numeric value: the
    // non-negative predicate can no longer evaluate that (present, non-null)
    // counter. A predicate that couldn't be evaluated must not be reported as a
    // passing Exact check.
    let report = verify_corrupted(plan, dir.path(), |sql| {
        rewrite_first_tuple_value(sql, "INSERT INTO `jobs`", 0, "'not-a-number'")
    });
    assert_eq!(
        report.status_of("planner_non_negative:jobs"),
        Some(CheckStatus::NotChecked),
        "an unparseable predicate input must not pass as Exact: {:?}",
        report.checks
    );
}

// --- temporal.timestamps ordering ------------------------------------------
//
// `temporal.timestamps` guarantees created_at <= updated_at by construction
// (see structural.rs), surfaced to the verifier as a `PlannerPredicate::
// Ordering`. These tests prove the invariant is actually *checked*, not just
// present in the predicate Vec: a clean dump verifies the ordering check
// Exact, and a corrupted one (updated_at rewritten to precede created_at)
// fails the named `planner_ordering:accounts` check.

const TIMESTAMPS: &str = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 3
tables:
  accounts:
    rows: { kind: fixed, count: 4 }
    schema:
      name: accounts
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: created_at, type: timestamp, nullable: false }
        - { name: updated_at, type: timestamp, nullable: false }
    planners:
      - kind: temporal.timestamps
        columns:
          created_at: created_at
          updated_at: updated_at
        created: { kind: range, min: "2024-01-01T00:00:00Z", max: "2026-01-01T00:00:00Z" }
        update_delay: { kind: uniform, unit: seconds, min: 0, max: 86400 }
"#;

#[test]
fn timestamps_ordering_is_exact_and_passes_for_valid_output() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(TIMESTAMPS);
    let verifier = GenerationVerifier::new(&plan);
    let sql = render(plan);
    let path = write(dir.path(), "ts.sql", &sql);
    let report = verifier.verify_path(&path).unwrap();
    assert_eq!(
        report.status_of("planner_ordering:accounts"),
        Some(CheckStatus::Exact),
        "ordering must be an exact check"
    );
    assert!(
        report.passed(),
        "{:?}",
        report.failures().collect::<Vec<_>>()
    );
}

#[test]
fn corrupt_timestamps_ordering_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(TIMESTAMPS);
    // Rewrite the first row's updated_at to a date far before the configured
    // 2024..2026 created_at range, so it precedes created_at regardless of the
    // drawn value. The INSERT omits the database-produced `id`, so the tuple
    // is (created_at, updated_at) — updated_at is index 1.
    let report = verify_corrupted(plan, dir.path(), |sql| {
        rewrite_first_tuple_value(sql, "INSERT INTO `accounts`", 1, "'2000-01-01 00:00:00'")
    });
    assert!(
        report.failed("planner_ordering:accounts"),
        "{:?}",
        report.checks
    );
}

#[test]
fn forward_referenced_derived_chain_passes_verification() {
    // A `id -> title -> slug` derivation where slug and title are declared before
    // their sources. Topological evaluation produces real, distinct slugs, so the
    // UNIQUE and non-null checks hold through the full reparse-and-verify path.
    let model = r#"
version: 1
kind: model
defaults: { inference: disabled }
seed: 11
tables:
  posts:
    rows: { kind: fixed, count: 5 }
    schema:
      name: posts
      primary_key: [id]
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: slug, type: "varchar(64)", nullable: false, unique: true }
        - { name: title, type: "varchar(64)", nullable: false }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
      slug: { generator: { kind: slug, source: title }, modifiers: [{ kind: unique }] }
      title: { generator: { kind: template, parts: ["Post ", { field: id }] } }
"#;
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(model);
    let verifier = GenerationVerifier::new(&plan);
    let sql = render(plan);
    let path = write(dir.path(), "posts.sql", &sql);
    let report = verifier.verify_path(&path).unwrap();
    assert!(
        report.passed(),
        "{:?}",
        report.failures().collect::<Vec<_>>()
    );
    assert!(
        sql.contains("'post-1'"),
        "slug must derive from a template that itself reads id: {sql}"
    );
}

// --- commerce.order_family --------------------------------------------------

#[test]
fn stochastic_order_family_passes_verification() {
    // A stochastic (poisson) per-order line count makes the family child's total
    // drift from the compile-time `parents x mean` estimate: the order_family
    // planner draws each order's line count itself and spools a variable total,
    // so `planned.rows` (parents x mean = 240) is only an estimate. Seed 4
    // realizes 216 lines. That estimate is not an authoritative target for a
    // planner-generated family child (a grossly-wrong count is still caught by
    // the exact family-sum, arity, and FK checks), so verification must pass and
    // the row-count check must be reported as sampled, never a failing exact.
    let dir = tempfile::tempdir().unwrap();
    let model = ORDER_FAMILY
        .replace("count: 4 }", "count: 60 }")
        .replace(
            "distribution: { kind: fixed, mean: 4.0, min: 1.0, max: 12.0 }",
            "distribution: { kind: poisson, mean: 4.0, min: 1.0, max: 12.0 }",
        )
        .replace("seed: 4242", "seed: 4");
    let plan = compile(&model);
    let verifier = GenerationVerifier::new(&plan);
    let sql = render(plan);
    let path = write(dir.path(), "ofp.sql", &sql);
    let report = verifier.verify_path(&path).unwrap();
    assert_eq!(
        report.status_of("row_count:order_items"),
        Some(CheckStatus::Sampled),
        "a family child's planner-realized count is sampled, not exact"
    );
    assert!(
        report.passed(),
        "a stochastic order family must still verify: {:?}",
        report.failures().collect::<Vec<_>>()
    );
}

const ORDER_FAMILY: &str = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 4242
tables:
  orders:
    rows: { kind: fixed, count: 4 }
    schema:
      name: orders
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: subtotal, type: "decimal(18,2)", nullable: false }
        - { name: tax_total, type: "decimal(18,2)", nullable: false }
        - { name: grand_total, type: "decimal(18,2)", nullable: false }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
    planners:
      - kind: commerce.order_family
        children: order_items
        relationship: order_items_order
        columns: { subtotal: subtotal, tax: tax_total, total: grand_total }
        child_columns: { quantity: quantity, unit_price: unit_price, tax: tax_amount, line_total: line_total }
        currency_scale: 2
        rounding: largest_remainder
        quantity: { min: 1, max: 6 }
        unit_price: { min_minor: 100, max_minor: 90000 }
        tax: { kind: weighted_choice, rates: [0.08, 0.25], weights: [0.4, 0.6] }
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
      id: { generator: { kind: sequence, start: 1 } }
      order_id: { generator: { kind: relation.foreign_key, relationship: order_items_order } }
"#;

#[test]
fn order_family_sum_check_is_exact_and_passes_for_valid_output() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(ORDER_FAMILY);
    let verifier = GenerationVerifier::new(&plan);
    let sql = render(plan);
    let path = write(dir.path(), "of.sql", &sql);
    let report = verifier.verify_path(&path).unwrap();
    assert_eq!(
        report.status_of("family_sum:orders"),
        Some(CheckStatus::Exact),
        "family sum must be an exact check"
    );
    assert!(
        report.passed(),
        "{:?}",
        report.failures().collect::<Vec<_>>()
    );
}

#[test]
fn family_sum_violation_is_not_masked_by_an_unrelated_inexact_row() {
    // One order's parent tax_total is (legally) NULL, which makes the family
    // "inexact"; a *different* order has a genuine child-sum disagreement. The
    // inexact row must not mask the real violation: the report must fail.
    let dir = tempfile::tempdir().unwrap();
    // Allow the parent money column to be NULL so the NULL trips `inexact`
    // without failing an unrelated non-null check.
    let model = ORDER_FAMILY.replace(
        r#"{ name: tax_total, type: "decimal(18,2)", nullable: false }"#,
        r#"{ name: tax_total, type: "decimal(18,2)", nullable: true }"#,
    );
    let plan = compile(&model);
    let report = verify_corrupted(plan, dir.path(), |sql| {
        // First order_items line: a real tax disagreement on order 1.
        let sql = rewrite_first_tuple_value(sql, "INSERT INTO `order_items`", 4, "999.99");
        // Last order: NULL parent tax_total → family marked inexact.
        rewrite_last_tuple_value(sql, "INSERT INTO `orders`", 2, "NULL")
    });
    assert!(
        !report.passed(),
        "a real family-sum violation must fail even when another row is inexact: {:?}",
        report.checks
    );
}

#[test]
fn corrupt_order_family_sum_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(ORDER_FAMILY);
    // Corrupt the first order_items line's tax_amount so the child tax sum no
    // longer equals its parent's tax_total.
    let report = verify_corrupted(plan, dir.path(), |sql| {
        rewrite_first_tuple_value(sql, "INSERT INTO `order_items`", 4, "999.99")
    });
    assert!(report.failed("family_sum:orders"), "{:?}", report.checks);
}

#[test]
fn foreign_key_to_an_ungenerated_parent_is_not_a_silent_pass() {
    // If a child relationship's parent table is not among the generated tables
    // (e.g. an externally-referenced parent), the verifier cannot establish FK
    // membership. It must surface that as NotChecked — never a green Exact pass.
    let dir = tempfile::tempdir().unwrap();
    let sql = render(compile(CORE));
    let path = write(dir.path(), "core.sql", &sql);

    // Drop `users` from the plan the verifier sees, keeping orders' relationship
    // to it: `self.tables` no longer holds the parent membership index.
    let mut plan = compile(CORE);
    plan.tables.retain(|table| table.name != "users");
    let report = GenerationVerifier::new(&plan).verify_path(&path).unwrap();

    assert_eq!(
        report.status_of("foreign_key:orders"),
        Some(CheckStatus::NotChecked),
        "an unverifiable FK must be NotChecked, not a green Exact pass: {:?}",
        report.checks
    );
}

// --- Bounded membership: spill path -----------------------------------------

#[test]
fn membership_indexes_spill_and_still_verify_exactly() {
    let dir = tempfile::tempdir().unwrap();

    // A zero-byte budget forces every uniqueness/membership index to spill its
    // keys to a protected spool immediately.
    let plan = compile(CORE);
    let verifier = GenerationVerifier::new(&plan).membership_budget_bytes(0);
    let sql = render(plan);
    let path = write(dir.path(), "ok.sql", &sql);
    assert!(
        verifier.verify_path(&path).unwrap().passed(),
        "valid output must still verify under the spill path"
    );

    // And a corrupted foreign key is still caught after spilling.
    let plan = compile(CORE);
    let verifier = GenerationVerifier::new(&plan).membership_budget_bytes(0);
    let corrupted = corrupt_first_order_fk(render(plan));
    let path = write(dir.path(), "bad.sql", &corrupted);
    assert!(verifier
        .verify_path(&path)
        .unwrap()
        .failed("foreign_key:orders"));
}

#[test]
fn membership_index_io_errors_abort_verification() {
    let dir = tempfile::tempdir().unwrap();
    let missing_temp = dir.path().join("missing-temp-directory");
    let plan = compile(CORE);
    let verifier = GenerationVerifier::new(&plan)
        .membership_budget_bytes(0)
        .temp_directory(&missing_temp);
    let sql = render(plan);
    let path = write(dir.path(), "membership-io.sql", &sql);

    let error = verifier
        .verify_path(&path)
        .expect_err("membership spool creation must be propagated");
    assert!(error.to_string().contains("membership index"), "{error}");
}

#[test]
fn family_index_io_errors_abort_verification() {
    let dir = tempfile::tempdir().unwrap();
    let missing_temp = dir.path().join("missing-temp-directory");
    let plan = compile(ORDER_FAMILY);
    let verifier = GenerationVerifier::new(&plan).temp_directory(&missing_temp);
    let sql = render(plan);
    let path = write(dir.path(), "family-io.sql", &sql);

    let error = verifier
        .verify_path(&path)
        .expect_err("family spool creation must be propagated");
    assert!(error.to_string().contains("family index"), "{error}");
}

#[test]
fn failed_verification_leaves_a_prior_destination_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("dest.sql");
    fs::write(&dest, "PRIOR CONTENTS").unwrap();

    let plan = compile(CORE);
    let verifier = GenerationVerifier::new(&plan);
    let corrupted = corrupt_first_order_fk(render(plan));

    // Stage a protected temp beside the destination, write the corrupted SQL,
    // audit it, and publish only on a passing report.
    let mut output = AtomicOutput::create(&dest).unwrap();
    output.writer().write_all(corrupted.as_bytes()).unwrap();
    let temp = output.temp_path().to_path_buf();

    let report = verifier.verify_path(&temp).unwrap();
    assert!(!report.passed(), "corrupted output must fail verification");
    // Publication is gated on a passing report; a failed audit publishes
    // nothing. Dropping the staged output removes the temp and leaves the
    // destination's prior bytes exactly as they were.
    drop(output);

    assert_eq!(fs::read_to_string(&dest).unwrap(), "PRIOR CONTENTS");
}

#[test]
fn sampled_distribution_is_labeled_sampled_not_exact() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    let verifier = GenerationVerifier::new(&plan).expect_distribution(DistributionExpectation {
        table: "users".into(),
        column: "code".into(),
        categories: vec![("100".into(), 0.25)],
        tolerance: 0.5,
    });
    let sql = render(plan);
    let path = write(dir.path(), "ok.sql", &sql);
    let report = verifier.verify_path(&path).unwrap();
    assert_eq!(
        report.status_of("distribution:users.code"),
        Some(CheckStatus::Sampled),
        "a distribution comparison must be labeled Sampled, never Exact"
    );
}

// ===========================================================================
// Postgres COPY output is audited through the SAME exact checks as INSERT.
// ===========================================================================

/// Render a plan to Postgres COPY output (default `inserts: auto`, no --no-copy).
fn render_pg_copy(plan: GenerationPlan) -> String {
    let options = RenderOptions {
        dialect: SqlDialect::Postgres,
        ..RenderOptions::default()
    };
    let mut buffer = Vec::new();
    let mut renderer = SqlRenderer::new(&mut buffer, options);
    GenerationEngine::new(plan).run(&mut renderer).unwrap();
    renderer.finish().unwrap();
    String::from_utf8(buffer).unwrap()
}

/// Verify a Postgres COPY dump, optionally mutating it first.
fn verify_pg_copy(
    plan: GenerationPlan,
    dir: &Path,
    mutate: impl FnOnce(String) -> String,
) -> sql_splitter::generate::VerificationReport {
    let verifier = GenerationVerifier::new(&plan).dialect(SqlDialect::Postgres);
    let sql = mutate(render_pg_copy(plan));
    let path = write(dir, "copy.sql", &sql);
    verifier.verify_path(&path).unwrap()
}

/// Rewrite the `index`-th tab-separated value of the first COPY data row after
/// the `COPY "<table>"` header.
fn rewrite_first_copy_value(sql: String, table: &str, index: usize, new_value: &str) -> String {
    let marker = format!("COPY \"{table}\"");
    let start = sql
        .find(&marker)
        .unwrap_or_else(|| panic!("`{marker}` present"));
    let header_end = sql[start..].find('\n').expect("COPY header newline") + start;
    let line_start = header_end + 1;
    let line_end = sql[line_start..].find('\n').expect("first COPY row") + line_start;
    let mut parts: Vec<String> = sql[line_start..line_end]
        .split('\t')
        .map(String::from)
        .collect();
    assert!(
        index < parts.len(),
        "COPY row has no value at index {index}"
    );
    parts[index] = new_value.to_string();
    format!(
        "{}{}{}",
        &sql[..line_start],
        parts.join("\t"),
        &sql[line_end..]
    )
}

#[test]
fn clean_postgres_copy_output_verifies_exactly_not_notchecked() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    let report = verify_pg_copy(plan, dir.path(), |sql| sql);
    assert!(
        report.passed(),
        "{:?}",
        report.failures().collect::<Vec<_>>()
    );
    // COPY tables are audited exactly, never left NotChecked.
    assert_eq!(
        report.status_of("row_count:users"),
        Some(CheckStatus::Exact)
    );
    assert_eq!(
        report.status_of("row_count:orders"),
        Some(CheckStatus::Exact)
    );
    assert_eq!(
        report.status_of("foreign_key:orders"),
        Some(CheckStatus::Exact)
    );
    assert!(
        !report
            .checks
            .iter()
            .any(|c| c.status == CheckStatus::NotChecked),
        "no COPY check should be NotChecked: {:?}",
        report.checks
    );
}

#[test]
fn corrupt_copy_non_null_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    // COPY NULL marker is \N; blank the first user's code.
    let report = verify_pg_copy(plan, dir.path(), |sql| {
        rewrite_first_copy_value(sql, "users", 1, "\\N")
    });
    assert!(report.failed("non_null:users"), "{:?}", report.checks);
}

#[test]
fn corrupt_copy_primary_key_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    // Second user's id (row 2) collides with the first (id 1).
    let report = verify_pg_copy(plan, dir.path(), |sql| {
        // Rewrite the SECOND data row's id: replace the "2\t101" line prefix.
        replace_once(sql, "\n2\t101\t", "\n1\t101\t")
    });
    assert!(report.failed("primary_key:users"), "{:?}", report.checks);
}

#[test]
fn corrupt_copy_unique_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    let report = verify_pg_copy(plan, dir.path(), |sql| {
        replace_once(sql, "\n2\t101\t", "\n2\t100\t")
    });
    assert!(report.failed("unique:users"), "{:?}", report.checks);
}

#[test]
fn corrupt_copy_arity_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    // Drop the name value from the first user COPY row.
    let report = verify_pg_copy(plan, dir.path(), |sql| {
        rewrite_first_copy_value(sql, "users", 2, "extra\tsplit")
    });
    assert!(report.failed("arity:users"), "{:?}", report.checks);
}

#[test]
fn corrupt_copy_foreign_key_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    // Point the first order's user_id (index 1) at a non-existent parent.
    let report = verify_pg_copy(plan, dir.path(), |sql| {
        rewrite_first_copy_value(sql, "orders", 1, "999")
    });
    assert!(report.failed("foreign_key:orders"), "{:?}", report.checks);
}

// Postgres COPY cannot emit `DEFAULT`, so a table whose PK renders as DEFAULT
// (a bare integer PK) can only render via multi-row INSERT. These COPY variants
// give every such key an explicit `sequence` generator so the family/planner
// tables render as COPY and get audited row-by-row.

const COMPOSITE_COPY: &str = r#"
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
    columns:
      x: { generator: { kind: sequence, start: 1 } }
      y: { generator: { kind: sequence, start: 1 } }
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
    columns:
      id: { generator: { kind: sequence, start: 1 } }
"#;

#[test]
fn corrupt_copy_composite_foreign_key_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(COMPOSITE_COPY);
    // readings COPY is (id, cell_x, cell_y); break cell_x (index 1).
    let report = verify_pg_copy(plan, dir.path(), |sql| {
        rewrite_first_copy_value(sql, "readings", 1, "999999")
    });
    assert!(
        report.failed("composite_foreign_key:readings"),
        "{:?}",
        report.checks
    );
}

const INTERVAL_COPY: &str = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 5
tables:
  jobs:
    rows: { kind: fixed, count: 4 }
    schema:
      name: jobs
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: started_at, type: timestamp, nullable: false }
        - { name: ended_at, type: timestamp, nullable: true }
        - { name: duration_seconds, type: bigint, nullable: true }
        - { name: is_running, type: boolean, nullable: false }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
    planners:
      - kind: temporal.interval
        columns:
          start: started_at
          end: ended_at
          duration: duration_seconds
          open: is_running
        start: { kind: range, min: "2024-01-01T00:00:00Z", max: "2026-01-01T00:00:00Z" }
        duration: { kind: uniform, unit: seconds, min: 30, max: 43200 }
        end_inclusive: false
        timezone: utc
"#;

#[test]
fn corrupt_copy_interval_equation_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(INTERVAL_COPY);
    // jobs COPY keeps the explicit `id`: (id, started_at, ended_at, ...) —
    // ended_at is index 2.
    let report = verify_pg_copy(plan, dir.path(), |sql| {
        rewrite_first_copy_value(sql, "jobs", 2, "2099-12-31 00:00:00")
    });
    assert!(
        report.failed("planner_equation:jobs"),
        "{:?}",
        report.checks
    );
}

const PROGRESS_COPY: &str = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 9
tables:
  jobs:
    rows: { kind: fixed, count: 4 }
    schema:
      name: jobs
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: total_rows, type: bigint, nullable: false }
        - { name: processed_rows, type: bigint, nullable: false }
        - { name: imported_rows, type: bigint, nullable: false }
        - { name: failed_rows, type: bigint, nullable: false }
        - { name: pending_rows, type: bigint, nullable: false }
        - { name: status, type: text, nullable: false }
        - { name: completed_at, type: timestamp, nullable: true }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
    planners:
      - kind: workflow.progress_counters
        columns:
          total: total_rows
          processed: processed_rows
          succeeded: imported_rows
          failed: failed_rows
          pending: pending_rows
          status: status
          completed_at: completed_at
        total: { kind: uniform, min: 10, max: 1000 }
        progress: { kind: mixture, complete_weight: 0.5, active_weight: 0.3, not_started_weight: 0.2 }
        partition: exact
        completed_statuses: [completed, failed]
        active_statuses: [queued, running]
"#;

#[test]
fn corrupt_copy_progress_counter_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(PROGRESS_COPY);
    // jobs COPY keeps `id`: (id, total, processed, imported, failed, pending,
    // status, completed_at) — pending_rows is index 5.
    let report = verify_pg_copy(plan, dir.path(), |sql| {
        rewrite_first_copy_value(sql, "jobs", 5, "424242")
    });
    assert!(
        report.failed("planner_counter_sum:jobs"),
        "{:?}",
        report.checks
    );
}

#[test]
fn corrupt_copy_order_family_sum_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(ORDER_FAMILY);
    // order_items keeps its explicit `id`; tax_amount is index 4.
    let report = verify_pg_copy(plan, dir.path(), |sql| {
        rewrite_first_copy_value(sql, "order_items", 4, "999.99")
    });
    assert!(report.failed("family_sum:orders"), "{:?}", report.checks);
}

// ===========================================================================
// MSSQL output (bracket identifiers, `GO` batch separators, `N'...'` literals)
// is delivered as whole INSERT statements, not streamed row-by-row; the
// verifier audits those rows through the same exact-check path as every dialect.
// ===========================================================================

/// Render a plan to MSSQL (`INSERT INTO [table] ... VALUES ...; GO`).
fn render_mssql(plan: GenerationPlan) -> String {
    let options = RenderOptions {
        dialect: SqlDialect::Mssql,
        ..RenderOptions::default()
    };
    let mut buffer = Vec::new();
    let mut renderer = SqlRenderer::new(&mut buffer, options);
    GenerationEngine::new(plan).run(&mut renderer).unwrap();
    renderer.finish().unwrap();
    String::from_utf8(buffer).unwrap()
}

fn verify_mssql(
    plan: GenerationPlan,
    dir: &Path,
    mutate: impl FnOnce(String) -> String,
) -> sql_splitter::generate::VerificationReport {
    let verifier = GenerationVerifier::new(&plan).dialect(SqlDialect::Mssql);
    let sql = mutate(render_mssql(plan));
    let path = write(dir, "mssql.sql", &sql);
    verifier.verify_path(&path).unwrap()
}

#[test]
fn mssql_output_is_audited_with_exact_row_counts() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    let report = verify_mssql(plan, dir.path(), |sql| sql);
    assert!(
        report.passed(),
        "{:?}",
        report.failures().collect::<Vec<_>>()
    );
    // Rows are actually audited: exact counts per table, never 0 or NotChecked.
    assert_eq!(
        report.status_of("row_count:users"),
        Some(CheckStatus::Exact)
    );
    assert_eq!(
        report.status_of("row_count:orders"),
        Some(CheckStatus::Exact)
    );
    assert!(
        report
            .checks
            .iter()
            .any(|c| c.name == "row_count:users" && c.passed && c.detail.contains("observed 4")),
        "users row count must observe 4 (not 0): {:?}",
        report.checks
    );
    assert_eq!(
        report.status_of("foreign_key:orders"),
        Some(CheckStatus::Exact)
    );
    assert!(
        !report
            .checks
            .iter()
            .any(|c| c.status == CheckStatus::NotChecked),
        "no MSSQL check should be NotChecked: {:?}",
        report.checks
    );
}

#[test]
fn corrupt_mssql_primary_key_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    // Second user's id collides with the first.
    let report = verify_mssql(plan, dir.path(), |sql| {
        replace_once(sql, "(2, 101,", "(1, 101,")
    });
    assert!(report.failed("primary_key:users"), "{:?}", report.checks);
}

#[test]
fn corrupt_mssql_foreign_key_fails_the_named_check() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(CORE);
    // Point the first order at a non-existent user id.
    let report = verify_mssql(plan, dir.path(), |sql| {
        let idx = sql.find("INSERT INTO [orders]").expect("orders insert");
        let (head, tail) = sql.split_at(idx);
        let open = tail.find("(1, ").expect("first order tuple");
        let close = tail[open..].find(')').unwrap() + open;
        format!("{head}{}(1, 999){}", &tail[..open], &tail[close + 1..])
    });
    assert!(report.failed("foreign_key:orders"), "{:?}", report.checks);
}

#[test]
fn mssql_full_verify_lifecycle_publishes_on_pass() {
    let dir = tempfile::tempdir().unwrap();
    let config = write(dir.path(), "model.yaml", CORE);
    let out = dir.path().join("out-mssql.sql");
    let report = Generate::builder()
        .config(&config)
        .output(&out)
        .output_dialect(SqlDialect::Mssql)
        .verify(true)
        .run()
        .expect("MSSQL verify + publish should succeed");
    assert!(report.rows_written > 0);
    assert!(out.exists(), "verified MSSQL output should be published");
    assert!(fs::read_to_string(&out)
        .unwrap()
        .contains("INSERT INTO [users]"));
}

// --- Task 34b: single-column key uniqueness by construction ----------------

// A single-column TEXT primary key backed by a `string` generator drawing from
// a tiny value space (one alphanumeric character, 62 possibilities) across many
// rows. Without uniqueness enforcement the birthday paradox all but guarantees a
// duplicate primary key; with the compiler's auto-attached `unique` modifier the
// emitted keys are distinct by construction.
const STRING_PK: &str = r#"
version: 1
kind: model
defaults: { inference: disabled }
seed: 7
tables:
  items:
    rows: { kind: fixed, count: 40 }
    schema:
      name: items
      primary_key: [code]
      columns:
        - { name: code, type: "varchar(16)", nullable: false, primary_key: true }
    columns:
      code: { generator: { kind: string, min_length: 1, max_length: 1 } }
"#;

#[test]
fn string_primary_key_is_distinct_by_construction() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(STRING_PK);
    let verifier = GenerationVerifier::new(&plan);
    let sql = render(plan);
    let path = write(dir.path(), "string-pk.sql", &sql);

    let report = verifier.verify_path(&path).unwrap();
    assert!(
        report.passed(),
        "string primary key must be unique by construction; failures: {:?}",
        report.failures().collect::<Vec<_>>()
    );
    assert_eq!(
        report.status_of("primary_key:items:code"),
        Some(CheckStatus::Exact)
    );
}

// A single-column UNIQUE (non-primary-key) TEXT column, again over a tiny value
// space. The compiler auto-attaches uniqueness the same way it does for a
// primary key, so the emitted values are distinct by construction.
const UNIQUE_TEXT: &str = r#"
version: 1
kind: model
defaults: { inference: disabled }
seed: 7
tables:
  items:
    rows: { kind: fixed, count: 40 }
    schema:
      name: items
      primary_key: [id]
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: slug, type: "varchar(16)", nullable: false, unique: true }
    columns:
      id: { generator: { kind: sequence, start: 1 } }
      slug: { generator: { kind: string, min_length: 1, max_length: 1 } }
"#;

#[test]
fn single_column_unique_text_is_distinct_by_construction() {
    let dir = tempfile::tempdir().unwrap();
    let plan = compile(UNIQUE_TEXT);
    let verifier = GenerationVerifier::new(&plan);
    let sql = render(plan);
    let path = write(dir.path(), "unique-text.sql", &sql);

    let report = verifier.verify_path(&path).unwrap();
    assert!(
        report.passed(),
        "single-column unique text must be distinct by construction; failures: {:?}",
        report.failures().collect::<Vec<_>>()
    );
    assert_eq!(
        report.status_of("unique:items:slug"),
        Some(CheckStatus::Exact)
    );
}

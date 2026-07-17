//! Tests for the `temporal.interval` planner (Task 23): its interval-equation
//! and open-row invariants under many rows across DST boundaries, seeded
//! reproducibility, and its compile-time diagnostics.

use chrono::DateTime;
use sql_splitter::generate::{
    CompileOptions, GeneratedRow, GeneratedValue, GenerationEngine, GenerationPlan, ModelCompiler,
    PlannedTable, RowSink,
};
use sql_splitter::synthetic::SyntheticFile;

// --- Test harness -----------------------------------------------------------

/// A [`RowSink`] that records every generated row of every table, positionally.
#[derive(Default)]
struct CollectingSink {
    columns: Vec<String>,
    rows: Vec<Vec<GeneratedValue>>,
}

impl RowSink for CollectingSink {
    fn begin_table(
        &mut self,
        table: &PlannedTable,
    ) -> Result<(), sql_splitter::generate::GenerateError> {
        self.columns = table
            .columns
            .iter()
            .map(|column| column.schema.name.clone())
            .collect();
        Ok(())
    }

    fn write_row(
        &mut self,
        _table: &PlannedTable,
        row: &GeneratedRow,
    ) -> Result<(), sql_splitter::generate::GenerateError> {
        self.rows.push(row.values.clone());
        Ok(())
    }

    fn end_table(
        &mut self,
        _table: &PlannedTable,
    ) -> Result<(), sql_splitter::generate::GenerateError> {
        Ok(())
    }
}

impl CollectingSink {
    fn index(&self, column: &str) -> usize {
        self.columns
            .iter()
            .position(|name| name == column)
            .unwrap_or_else(|| panic!("no column `{column}`"))
    }

    fn column<'a>(&'a self, column: &str) -> impl Iterator<Item = &'a GeneratedValue> + 'a {
        let idx = self.index(column);
        self.rows.iter().map(move |row| &row[idx])
    }
}

fn compile_result(
    model_yaml: &str,
) -> Result<GenerationPlan, sql_splitter::diagnostic::DiagnosticBag> {
    let model = SyntheticFile::parse_str(model_yaml)
        .expect("valid model YAML")
        .into_model()
        .expect("document is a model");
    ModelCompiler::standard().compile(model, CompileOptions::default())
}

fn run(model_yaml: &str) -> CollectingSink {
    let plan = compile_result(model_yaml).expect("model compiles cleanly");
    let mut sink = CollectingSink::default();
    GenerationEngine::new(plan)
        .run(&mut sink)
        .expect("engine runs");
    sink
}

/// A one-table model whose `jobs` table carries a `temporal.interval` planner.
/// `overrides` is spliced into the planner block so tests can vary one knob.
fn jobs_model(seed: u64, rows: u64, overrides: &str) -> String {
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: schema }}
seed: {seed}
tables:
  jobs:
    rows: {{ kind: fixed, count: {rows} }}
    schema:
      name: jobs
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: started_at, type: timestamp, nullable: false }}
        - {{ name: ended_at, type: timestamp, nullable: true }}
        - {{ name: duration_seconds, type: bigint, nullable: true }}
        - {{ name: is_running, type: boolean, nullable: false }}
    planners:
      - kind: temporal.interval
        columns:
          start: started_at
          end: ended_at
          duration: duration_seconds
          open: is_running
        start:
          kind: range
          min: "2024-01-01T00:00:00Z"
          max: "2026-01-01T00:00:00Z"
        duration:
          kind: uniform
          unit: seconds
          min: 30
          max: 43200
        end_inclusive: false
{overrides}
"#
    )
}

/// Parse a rendered timestamp literal (UTC `%Y-%m-%d %H:%M:%S` or a
/// zoned `...%:z`) into absolute epoch nanoseconds.
fn instant_ns(text: &str) -> i128 {
    if let Ok(dt) = DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%:z") {
        return i128::from(dt.timestamp()) * 1_000_000_000
            + i128::from(dt.timestamp_subsec_nanos());
    }
    let naive = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S")
        .unwrap_or_else(|_| panic!("unparseable timestamp `{text}`"));
    i128::from(naive.and_utc().timestamp()) * 1_000_000_000
}

fn datetime_text(value: &GeneratedValue) -> &str {
    match value {
        GeneratedValue::DateTime(text) => text.as_str(),
        other => panic!("expected DateTime, found {other:?}"),
    }
}

// --- Invariants -------------------------------------------------------------

#[test]
fn interval_equation_and_open_state_hold_across_dst_for_100k_rows() {
    // A named IANA zone renders wall clocks with an explicit offset, so start
    // and end round-trip to absolute instants even across the DST transitions
    // inside the 2024..2026 start range. open_probability makes some rows open.
    let sink = run(&jobs_model(
        42,
        100_000,
        "        open_probability: 0.07\n        timezone: America/New_York",
    ));

    let starts: Vec<&GeneratedValue> = sink.column("started_at").collect();
    let ends: Vec<&GeneratedValue> = sink.column("ended_at").collect();
    let durations: Vec<&GeneratedValue> = sink.column("duration_seconds").collect();
    let running: Vec<&GeneratedValue> = sink.column("is_running").collect();
    assert_eq!(starts.len(), 100_000);

    let min_ns = instant_ns("2024-01-01 00:00:00+00:00");
    let max_ns = instant_ns("2026-01-01 00:00:00+00:00");
    let mut open_rows = 0;

    for i in 0..starts.len() {
        // Start is never null and lies within the configured UTC range.
        let start_ns = instant_ns(datetime_text(starts[i]));
        assert!(
            start_ns >= min_ns && start_ns <= max_ns,
            "row {i}: start {start_ns} out of range"
        );

        let is_open = running[i].as_boolean().expect("boolean flag");
        if is_open {
            open_rows += 1;
            // Open rows: null end, null duration, running == true.
            assert!(ends[i].is_null(), "row {i}: open row must have null end");
            assert!(
                durations[i].is_null(),
                "row {i}: open row must have null duration"
            );
        } else {
            // Closed rows satisfy end = start + duration exactly (nanoseconds).
            let end_ns = instant_ns(datetime_text(ends[i]));
            let duration_s = durations[i].as_integer().expect("integer duration");
            assert!(
                (30..=43_200).contains(&duration_s),
                "row {i}: duration bounds"
            );
            assert_eq!(
                end_ns,
                start_ns + duration_s * 1_000_000_000,
                "row {i}: end != start + duration"
            );
        }
    }

    // 7% of 100k rows should be open — assert a broad band, not an exact count.
    assert!(
        (3_000..12_000).contains(&open_rows),
        "expected ~7% open rows, saw {open_rows}"
    );
}

#[test]
fn all_rows_are_closed_when_open_probability_is_zero() {
    let sink = run(&jobs_model(1, 500, "        timezone: utc"));
    for (i, (end, running)) in sink
        .column("ended_at")
        .zip(sink.column("is_running"))
        .enumerate()
    {
        assert!(
            !running.as_boolean().expect("boolean"),
            "row {i} should be closed"
        );
        assert!(!end.is_null(), "row {i}: closed row has a non-null end");
    }
}

#[test]
fn seeded_output_repeats_and_differs_by_seed() {
    let overrides = "        open_probability: 0.1\n        timezone: utc";
    let first = run(&jobs_model(7, 2_000, overrides));
    let again = run(&jobs_model(7, 2_000, overrides));
    let other = run(&jobs_model(8, 2_000, overrides));

    assert_eq!(first.rows, again.rows, "same seed must reproduce rows");
    assert_ne!(first.rows, other.rows, "a different seed must diverge");
}

#[test]
fn monotonic_start_is_strictly_increasing() {
    let yaml = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 3
tables:
  jobs:
    rows: { kind: fixed, count: 1000 }
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
        start:
          kind: monotonic
          min: "2024-01-01T00:00:00Z"
          step_seconds: 60
        duration:
          kind: fixed
          unit: seconds
          value: 120
        timezone: utc
"#;
    let sink = run(yaml);
    let mut previous = i128::MIN;
    for value in sink.column("started_at") {
        let ns = instant_ns(datetime_text(value));
        assert!(ns > previous, "monotonic start must strictly increase");
        previous = ns;
    }
}

#[test]
fn compiled_planner_returns_exact_verification_predicates() {
    use sql_splitter::generate::{PlannerPredicate, PredicateGuard};

    let plan = compile_result(&jobs_model(
        5,
        10,
        "        open_probability: 0.07\n        timezone: utc",
    ))
    .expect("model compiles cleanly");
    let planner = &plan.table("jobs").expect("jobs table").planners[0];
    let predicates = planner.verification_predicates();

    // The interval equation guarded to closed rows (is_running == false), the
    // null-end guarantee for open rows, and the start range bounds.
    assert!(predicates.iter().any(|p| matches!(
        p,
        PlannerPredicate::Equation {
            start, end, duration, guard: Some(PredicateGuard::Flag { column, value: false }), ..
        } if start == "started_at" && end == "ended_at" && duration == "duration_seconds" && column == "is_running"
    )));
    assert!(predicates.iter().any(|p| matches!(
        p,
        PlannerPredicate::NullWhen { column, guard: PredicateGuard::Flag { column: flag, value: true } }
            if column == "ended_at" && flag == "is_running"
    )));
    assert!(predicates
        .iter()
        .any(|p| matches!(p, PlannerPredicate::InRange { column, .. } if column == "started_at")));
}

/// A monotonic-start (second-aligned), fixed-duration model whose only variable
/// is `end_inclusive`, so the inclusive/exclusive `end` values are directly
/// comparable at second precision.
fn inclusive_model(end_inclusive: bool, duration_value: i64) -> String {
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: schema }}
seed: 11
tables:
  jobs:
    rows: {{ kind: fixed, count: 200 }}
    schema:
      name: jobs
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: started_at, type: timestamp, nullable: false }}
        - {{ name: ended_at, type: timestamp, nullable: true }}
        - {{ name: duration_seconds, type: bigint, nullable: true }}
    planners:
      - kind: temporal.interval
        columns:
          start: started_at
          end: ended_at
          duration: duration_seconds
        start:
          kind: monotonic
          min: "2024-01-01T00:00:00Z"
          step_seconds: 3600
        duration:
          kind: fixed
          unit: seconds
          value: {duration_value}
        end_inclusive: {end_inclusive}
        timezone: utc
"#
    )
}

#[test]
fn end_inclusive_shifts_the_generated_end_by_one_unit() {
    use sql_splitter::generate::PlannerPredicate;

    // Second-aligned starts + a whole-second duration make the internal 1 ns
    // shift observable at second precision: inclusive `end` is the last covered
    // second, exactly one second before the half-open exclusive boundary.
    let inclusive = run(&inclusive_model(true, 120));
    let exclusive = run(&inclusive_model(false, 120));

    let inclusive_ends: Vec<i128> = inclusive
        .column("ended_at")
        .map(|v| instant_ns(datetime_text(v)))
        .collect();
    let exclusive_ends: Vec<i128> = exclusive
        .column("ended_at")
        .map(|v| instant_ns(datetime_text(v)))
        .collect();
    assert_eq!(inclusive_ends.len(), 200);

    for (i, (incl, excl)) in inclusive_ends.iter().zip(&exclusive_ends).enumerate() {
        assert_ne!(
            incl, excl,
            "row {i}: end_inclusive must change the end value"
        );
        assert_eq!(
            excl - incl,
            1_000_000_000,
            "row {i}: inclusive end is one second (unit) before the exclusive boundary"
        );
    }

    // Each plan's Equation predicate encodes its own mode.
    let inclusive_flag = equation_end_inclusive(&inclusive_model(true, 120));
    let exclusive_flag = equation_end_inclusive(&inclusive_model(false, 120));
    assert!(
        inclusive_flag,
        "inclusive plan predicate must carry end_inclusive = true"
    );
    assert!(
        !exclusive_flag,
        "exclusive plan predicate must carry end_inclusive = false"
    );
    // Sanity: the predicate variant is the interval Equation with second units.
    assert!(matches!(
        first_equation(&inclusive_model(true, 120)),
        PlannerPredicate::Equation { duration_unit_nanos, .. } if duration_unit_nanos == 1_000_000_000
    ));
}

/// The `end_inclusive` flag of the first Equation predicate a model compiles to.
fn equation_end_inclusive(model_yaml: &str) -> bool {
    match first_equation(model_yaml) {
        sql_splitter::generate::PlannerPredicate::Equation { end_inclusive, .. } => end_inclusive,
        other => panic!("expected an Equation predicate, found {other:?}"),
    }
}

fn first_equation(model_yaml: &str) -> sql_splitter::generate::PlannerPredicate {
    let plan = compile_result(model_yaml).expect("model compiles cleanly");
    plan.table("jobs").expect("jobs table").planners[0]
        .verification_predicates()
        .into_iter()
        .find(|p| matches!(p, sql_splitter::generate::PlannerPredicate::Equation { .. }))
        .expect("an Equation predicate")
}

#[test]
fn zero_length_inclusive_interval_is_a_compile_error() {
    // A fixed zero-length duration cannot form an inclusive (closed) interval.
    assert!(
        compile_err_code(&inclusive_model(true, 0)).contains(&"GEN-INTERVAL-DURATION".to_string())
    );
    // The same duration is fine for a half-open interval.
    assert!(compile_result(&inclusive_model(false, 0)).is_ok());
}

// --- Compile diagnostics ----------------------------------------------------

fn compile_err_code(model_yaml: &str) -> Vec<String> {
    let bag = compile_result(model_yaml).expect_err("expected a compile error");
    bag.diagnostics.into_iter().map(|d| d.code).collect()
}

#[test]
fn non_nullable_end_with_open_probability_is_a_compile_error() {
    // Make ended_at non-nullable and keep a positive open probability.
    let yaml = jobs_model(
        1,
        10,
        "        open_probability: 0.1\n        timezone: utc",
    )
    .replace(
        "{ name: ended_at, type: timestamp, nullable: true }",
        "{ name: ended_at, type: timestamp, nullable: false }",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-INTERVAL-OPEN-END".to_string()));
}

#[test]
fn negative_duration_is_a_compile_error() {
    let yaml = jobs_model(1, 10, "        timezone: utc").replace("min: 30", "min: -5");
    assert!(compile_err_code(&yaml).contains(&"GEN-INTERVAL-DURATION".to_string()));
}

#[test]
fn overflowing_duration_is_a_compile_error() {
    // i128::MAX seconds cannot be converted to nanoseconds without overflow.
    let huge = format!("max: \"{}\"", i128::MAX);
    let yaml = jobs_model(1, 10, "        timezone: utc").replace("max: 43200", &huge);
    assert!(compile_err_code(&yaml).contains(&"GEN-INTERVAL-DURATION".to_string()));
}

#[test]
fn invalid_iana_zone_is_a_compile_error() {
    let yaml = jobs_model(1, 10, "        timezone: Mars/Olympus_Mons");
    assert!(compile_err_code(&yaml).contains(&"GEN-INTERVAL-TIMEZONE".to_string()));
}

#[test]
fn missing_owned_column_is_a_compile_error() {
    // The `duration` role names a column that does not exist in the schema.
    let yaml = jobs_model(1, 10, "        timezone: utc")
        .replace("duration: duration_seconds", "duration: nonexistent_column");
    assert!(compile_err_code(&yaml).contains(&"GEN-INTERVAL-COLUMN-MISSING".to_string()));
}

#[test]
fn ownership_collision_with_a_generator_is_a_compile_error() {
    // A generator on started_at collides with the planner's ownership of it.
    let yaml = jobs_model(1, 10, "        timezone: utc").replace(
        "    planners:",
        "    columns:\n      started_at:\n        generator: { kind: datetime }\n    planners:",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-COLUMN-OWNER-CONFLICT".to_string()));
}

// === workflow.progress_counters (Task 24) ===================================
//
// The progress-counter planner chooses a TOTAL and a lifecycle STATE per row,
// then partitions exact integer counter amounts. These tests pin the state
// machine invariants (with `partition: exact`), the exact integer partition,
// the compile-time diagnostics, and seeded reproducibility.

/// A one-table `jobs` model carrying a `workflow.progress_counters` planner.
/// `progress` is the indented body of the `progress:` block (choosing the
/// lifecycle mixture); `extra` appends planner-level keys.
fn progress_model(seed: u64, rows: u64, progress: &str, extra: &str) -> String {
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: schema }}
seed: {seed}
tables:
  jobs:
    rows: {{ kind: fixed, count: {rows} }}
    schema:
      name: jobs
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: total_rows, type: bigint, nullable: false }}
        - {{ name: processed_rows, type: bigint, nullable: false }}
        - {{ name: imported_rows, type: bigint, nullable: false }}
        - {{ name: failed_rows, type: bigint, nullable: false }}
        - {{ name: pending_rows, type: bigint, nullable: false }}
        - {{ name: status, type: text, nullable: false }}
        - {{ name: completed_at, type: timestamp, nullable: true }}
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
        total: {{ kind: uniform, min: 10, max: 1000 }}
        progress:
{progress}
        partition: exact
        completed_statuses: [completed, failed]
        active_statuses: [queued, running]
{extra}
"#
    )
}

/// The integer value of a counter cell.
fn counter(value: &GeneratedValue) -> i128 {
    value.as_integer().expect("integer counter")
}

/// The text of a status cell.
fn status_text(value: &GeneratedValue) -> &str {
    value.as_text().expect("text status")
}

/// Assert the exact partition invariants shared by every produced row.
fn assert_row_invariants(
    i: usize,
    total: i128,
    processed: i128,
    succeeded: i128,
    failed: i128,
    pending: i128,
) {
    // All counters non-negative.
    for (name, value) in [
        ("total", total),
        ("processed", processed),
        ("succeeded", succeeded),
        ("failed", failed),
        ("pending", pending),
    ] {
        assert!(value >= 0, "row {i}: {name} counter {value} is negative");
    }
    // Exact partition equations.
    assert_eq!(
        succeeded + failed,
        processed,
        "row {i}: succeeded + failed != processed"
    );
    assert_eq!(
        pending,
        total - processed,
        "row {i}: pending != total - processed"
    );
    // Ordered: succeeded, failed <= processed <= total.
    assert!(succeeded <= processed, "row {i}: succeeded > processed");
    assert!(failed <= processed, "row {i}: failed > processed");
    assert!(processed <= total, "row {i}: processed > total");
}

#[test]
fn progress_counters_complete_mixture_holds_exact_invariants() {
    let sink = run(&progress_model(4, 500, "          kind: complete", ""));
    for i in 0..sink.rows.len() {
        let total = counter(sink.rows[i].get(sink.index("total_rows")).unwrap());
        let processed = counter(sink.rows[i].get(sink.index("processed_rows")).unwrap());
        let succeeded = counter(sink.rows[i].get(sink.index("imported_rows")).unwrap());
        let failed = counter(sink.rows[i].get(sink.index("failed_rows")).unwrap());
        let pending = counter(sink.rows[i].get(sink.index("pending_rows")).unwrap());
        assert_row_invariants(i, total, processed, succeeded, failed, pending);

        // Completed rows: fully processed, nothing pending, non-null completion.
        assert_eq!(
            processed, total,
            "row {i}: completed must have processed=total"
        );
        assert_eq!(pending, 0, "row {i}: completed must have pending=0");
        let completed_at = sink.rows[i].get(sink.index("completed_at")).unwrap();
        assert!(
            !completed_at.is_null(),
            "row {i}: completed needs a timestamp"
        );
        let status = status_text(sink.rows[i].get(sink.index("status")).unwrap());
        assert!(
            ["completed", "failed"].contains(&status),
            "row {i}: status `{status}` not a completed status"
        );
    }
}

#[test]
fn progress_counters_active_mixture_is_incomplete() {
    let sink = run(&progress_model(5, 500, "          kind: in_progress", ""));
    for i in 0..sink.rows.len() {
        let total = counter(sink.rows[i].get(sink.index("total_rows")).unwrap());
        let processed = counter(sink.rows[i].get(sink.index("processed_rows")).unwrap());
        let succeeded = counter(sink.rows[i].get(sink.index("imported_rows")).unwrap());
        let failed = counter(sink.rows[i].get(sink.index("failed_rows")).unwrap());
        let pending = counter(sink.rows[i].get(sink.index("pending_rows")).unwrap());
        assert_row_invariants(i, total, processed, succeeded, failed, pending);

        // Active rows: incomplete (pending > 0), null completion.
        assert!(processed < total, "row {i}: active must be incomplete");
        assert!(pending > 0, "row {i}: active must have pending > 0");
        let completed_at = sink.rows[i].get(sink.index("completed_at")).unwrap();
        assert!(
            completed_at.is_null(),
            "row {i}: active must have null completion"
        );
        let status = status_text(sink.rows[i].get(sink.index("status")).unwrap());
        assert!(
            ["queued", "running"].contains(&status),
            "row {i}: status `{status}` not an active status"
        );
    }
}

#[test]
fn progress_counters_not_started_mixture_is_zeroed() {
    let sink = run(&progress_model(6, 300, "          kind: not_started", ""));
    for i in 0..sink.rows.len() {
        let total = counter(sink.rows[i].get(sink.index("total_rows")).unwrap());
        let processed = counter(sink.rows[i].get(sink.index("processed_rows")).unwrap());
        let succeeded = counter(sink.rows[i].get(sink.index("imported_rows")).unwrap());
        let failed = counter(sink.rows[i].get(sink.index("failed_rows")).unwrap());
        let pending = counter(sink.rows[i].get(sink.index("pending_rows")).unwrap());
        assert_row_invariants(i, total, processed, succeeded, failed, pending);

        assert_eq!(processed, 0, "row {i}: not_started must have processed=0");
        assert_eq!(succeeded, 0, "row {i}: not_started must have succeeded=0");
        assert_eq!(failed, 0, "row {i}: not_started must have failed=0");
        assert_eq!(
            pending, total,
            "row {i}: not_started must have pending=total"
        );
        let completed_at = sink.rows[i].get(sink.index("completed_at")).unwrap();
        assert!(
            completed_at.is_null(),
            "row {i}: not_started null completion"
        );
    }
}

#[test]
fn progress_counters_mixture_covers_all_states_with_invariants() {
    let progress = "          kind: mixture\n          complete_weight: 0.5\n          active_weight: 0.35\n          not_started_weight: 0.15";
    let sink = run(&progress_model(9, 4000, progress, ""));
    let (mut complete, mut active, mut not_started) = (0, 0, 0);
    for i in 0..sink.rows.len() {
        let total = counter(sink.rows[i].get(sink.index("total_rows")).unwrap());
        let processed = counter(sink.rows[i].get(sink.index("processed_rows")).unwrap());
        let succeeded = counter(sink.rows[i].get(sink.index("imported_rows")).unwrap());
        let failed = counter(sink.rows[i].get(sink.index("failed_rows")).unwrap());
        let pending = counter(sink.rows[i].get(sink.index("pending_rows")).unwrap());
        assert_row_invariants(i, total, processed, succeeded, failed, pending);

        let completed_at = sink.rows[i].get(sink.index("completed_at")).unwrap();
        if !completed_at.is_null() {
            // Completed: processed=total, non-null completion.
            assert_eq!(processed, total, "row {i}: completed processed=total");
            complete += 1;
        } else if processed == 0 {
            assert_eq!(pending, total, "row {i}: not_started pending=total");
            not_started += 1;
        } else {
            assert!(processed < total, "row {i}: active incomplete");
            active += 1;
        }
    }
    // Every configured state should appear across 4000 rows (broad bands).
    assert!(
        complete > 500,
        "expected many completed rows, saw {complete}"
    );
    assert!(active > 300, "expected many active rows, saw {active}");
    assert!(
        not_started > 50,
        "expected some not-started rows, saw {not_started}"
    );
}

#[test]
fn progress_counters_allow_unclassified_relaxes_the_split() {
    let yaml = progress_model(10, 400, "          kind: complete", "").replace(
        "partition: exact",
        "partition: allow_unclassified\n        unclassified_ratio: 0.2",
    );
    let sink = run(&yaml);
    let mut saw_unclassified = false;
    for i in 0..sink.rows.len() {
        let total = counter(sink.rows[i].get(sink.index("total_rows")).unwrap());
        let processed = counter(sink.rows[i].get(sink.index("processed_rows")).unwrap());
        let succeeded = counter(sink.rows[i].get(sink.index("imported_rows")).unwrap());
        let failed = counter(sink.rows[i].get(sink.index("failed_rows")).unwrap());
        let pending = counter(sink.rows[i].get(sink.index("pending_rows")).unwrap());
        // succeeded + failed may leave an unclassified remainder <= processed.
        assert!(
            succeeded + failed <= processed,
            "row {i}: classified exceeds processed"
        );
        assert_eq!(pending, total - processed, "row {i}: pending equation");
        assert!(succeeded >= 0 && failed >= 0, "row {i}: nonneg");
        if succeeded + failed < processed {
            saw_unclassified = true;
        }
    }
    assert!(
        saw_unclassified,
        "allow_unclassified should leave a remainder"
    );
}

#[test]
fn progress_counters_seeded_output_repeats_and_differs_by_seed() {
    let progress = "          kind: mixture\n          complete_weight: 0.5\n          active_weight: 0.4\n          not_started_weight: 0.1";
    let first = run(&progress_model(21, 1500, progress, ""));
    let again = run(&progress_model(21, 1500, progress, ""));
    let other = run(&progress_model(22, 1500, progress, ""));
    assert_eq!(first.rows, again.rows, "same seed must reproduce rows");
    assert_ne!(first.rows, other.rows, "a different seed must diverge");
}

#[test]
fn progress_counters_returns_exact_verification_predicates() {
    use sql_splitter::generate::{PlannerPredicate, PredicateGuard};

    let plan = compile_result(&progress_model(5, 10, "          kind: mixture\n          complete_weight: 0.6\n          active_weight: 0.3\n          not_started_weight: 0.1", ""))
        .expect("model compiles cleanly");
    let planner = &plan.table("jobs").expect("jobs table").planners[0];
    let predicates = planner.verification_predicates();

    // succeeded + failed == processed (exact partition).
    assert!(predicates.iter().any(|p| matches!(
        p,
        PlannerPredicate::CounterSum { addends, sum, guard: None }
            if addends.len() == 2
                && addends.contains(&"imported_rows".to_string())
                && addends.contains(&"failed_rows".to_string())
                && sum == "processed_rows"
    )));
    // processed + pending == total.
    assert!(predicates.iter().any(|p| matches!(
        p,
        PlannerPredicate::CounterSum { addends, sum, guard: None }
            if addends.contains(&"processed_rows".to_string())
                && addends.contains(&"pending_rows".to_string())
                && sum == "total_rows"
    )));
    // All counters non-negative.
    assert!(predicates
        .iter()
        .any(|p| matches!(p, PlannerPredicate::NonNegative { columns } if columns.contains(&"total_rows".to_string()))));
    // Completed rows carry a non-null completion timestamp.
    assert!(predicates.iter().any(|p| matches!(
        p,
        PlannerPredicate::NotNullWhen { column, guard: PredicateGuard::Equals { column: guard_col, .. } }
            if column == "completed_at" && guard_col == "status"
    )));
}

// --- Compile diagnostics ----------------------------------------------------

#[test]
fn progress_counters_overflow_is_a_compile_error() {
    // int counters cap at i32::MAX; a fixed total beyond that overflows.
    let yaml = r#"
version: 1
kind: model
defaults: { inference: schema }
seed: 1
tables:
  jobs:
    rows: { kind: fixed, count: 5 }
    schema:
      name: jobs
      columns:
        - { name: id, type: bigint, nullable: false, primary_key: true }
        - { name: total_rows, type: int, nullable: false }
        - { name: processed_rows, type: int, nullable: false }
    planners:
      - kind: workflow.progress_counters
        columns:
          total: total_rows
          processed: processed_rows
        total: { kind: fixed, value: 3000000000 }
        progress:
          kind: complete
        partition: exact
"#;
    assert!(compile_err_code(yaml).contains(&"GEN-PROGRESS-OVERFLOW".to_string()));
}

#[test]
fn progress_counters_absent_status_vocabulary_is_a_compile_error() {
    // A configured status column with an empty completed vocabulary, while the
    // mixture produces completed rows, cannot label those rows.
    let yaml = progress_model(1, 10, "          kind: complete", "").replace(
        "completed_statuses: [completed, failed]",
        "completed_statuses: []",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-PROGRESS-STATUS-VOCABULARY".to_string()));
}

#[test]
fn progress_counters_impossible_non_null_completion_is_a_compile_error() {
    // A non-nullable completion column cannot hold the null an active row needs.
    let progress = "          kind: mixture\n          complete_weight: 0.5\n          active_weight: 0.5\n          not_started_weight: 0.0";
    let yaml = progress_model(1, 10, progress, "").replace(
        "{ name: completed_at, type: timestamp, nullable: true }",
        "{ name: completed_at, type: timestamp, nullable: false }",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-PROGRESS-COMPLETION".to_string()));
}

#[test]
fn progress_counters_missing_column_is_a_compile_error() {
    let yaml = progress_model(1, 10, "          kind: complete", "")
        .replace("total: total_rows", "total: does_not_exist");
    assert!(compile_err_code(&yaml).contains(&"GEN-PROGRESS-COLUMN-MISSING".to_string()));
}

#[test]
fn progress_counters_exact_partition_with_absent_observed_evidence_is_a_compile_error() {
    // `observed` progress under `partition: exact` needs observed evidence; with
    // none available it cannot form an exact integer partition.
    let yaml = progress_model(1, 10, "          kind: observed", "");
    assert!(compile_err_code(&yaml).contains(&"GEN-PROGRESS-OBSERVED".to_string()));
}

#[test]
fn progress_counters_zero_total_weights_is_a_compile_error() {
    let progress = "          kind: mixture\n          complete_weight: 0.0\n          active_weight: 0.0\n          not_started_weight: 0.0";
    let yaml = progress_model(1, 10, progress, "");
    assert!(compile_err_code(&yaml).contains(&"GEN-PROGRESS-WEIGHTS".to_string()));
}

#[test]
fn progress_counters_ownership_collision_is_a_compile_error() {
    // A generator on total_rows collides with the planner's ownership of it.
    let yaml = progress_model(1, 10, "          kind: complete", "").replace(
        "    planners:",
        "    columns:\n      total_rows:\n        generator: { kind: constant, value: 1 }\n    planners:",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-COLUMN-OWNER-CONFLICT".to_string()));
}

// === commerce.order_family (Task 25) ========================================
//
// The order-family planner coordinates an `orders` parent and an `order_items`
// child as one family, computing exact minor-unit money. These tests pin the
// exact-sum invariants across tax rates, discounts, shipping, currency scales,
// mixed quantities, large values, and all three rounding modes, plus the
// compile-time diagnostics.

use std::collections::BTreeMap;

/// A [`RowSink`] that records every table's rows separately (unlike
/// [`CollectingSink`], which flattens them), so a cross-table family can be
/// checked parent-against-child.
#[derive(Default)]
struct MultiSink {
    tables: BTreeMap<String, (Vec<String>, Vec<Vec<GeneratedValue>>)>,
}

impl RowSink for MultiSink {
    fn begin_table(
        &mut self,
        table: &PlannedTable,
    ) -> Result<(), sql_splitter::generate::GenerateError> {
        let columns = table
            .columns
            .iter()
            .map(|column| column.schema.name.clone())
            .collect();
        self.tables
            .insert(table.name.clone(), (columns, Vec::new()));
        Ok(())
    }

    fn write_row(
        &mut self,
        table: &PlannedTable,
        row: &GeneratedRow,
    ) -> Result<(), sql_splitter::generate::GenerateError> {
        self.tables
            .get_mut(&table.name)
            .expect("table began")
            .1
            .push(row.values.clone());
        Ok(())
    }

    fn end_table(
        &mut self,
        _table: &PlannedTable,
    ) -> Result<(), sql_splitter::generate::GenerateError> {
        Ok(())
    }
}

impl MultiSink {
    fn index(&self, table: &str, column: &str) -> usize {
        self.tables[table]
            .0
            .iter()
            .position(|name| name == column)
            .unwrap_or_else(|| panic!("no column `{column}` on `{table}`"))
    }

    fn rows(&self, table: &str) -> &[Vec<GeneratedValue>] {
        &self.tables[table].1
    }
}

fn run_multi(model_yaml: &str) -> MultiSink {
    let plan = compile_result(model_yaml).expect("model compiles cleanly");
    let mut sink = MultiSink::default();
    GenerationEngine::new(plan)
        .run(&mut sink)
        .expect("engine runs");
    sink
}

/// The minor-unit integer behind a money value (decimal) or a plain integer.
fn minor(value: &GeneratedValue) -> i128 {
    match value {
        GeneratedValue::Decimal { minor, .. } => *minor,
        GeneratedValue::Integer(i) => *i,
        other => panic!("expected a money/integer value, found {other:?}"),
    }
}

fn int_of(value: &GeneratedValue) -> i128 {
    value.as_integer().expect("integer value")
}

/// Build an orders/order_items family model. `scale` sets the currency scale and
/// the declared money-column scale; `dist` is the child line-count distribution;
/// `extra` are extra planner keys (quantity/unit_price/tax/discount/shipping).
fn order_family_model(
    seed: u64,
    orders: u64,
    scale: u32,
    rounding: &str,
    dist: &str,
    extra: &str,
) -> String {
    let money = format!("decimal(18,{scale})");
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: schema }}
seed: {seed}
tables:
  orders:
    rows: {{ kind: fixed, count: {orders} }}
    schema:
      name: orders
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: subtotal, type: "{money}", nullable: false }}
        - {{ name: discount_total, type: "{money}", nullable: false }}
        - {{ name: tax_total, type: "{money}", nullable: false }}
        - {{ name: shipping_total, type: "{money}", nullable: false }}
        - {{ name: grand_total, type: "{money}", nullable: false }}
    columns:
      id:
        generator: {{ kind: sequence, start: 1 }}
    planners:
      - kind: commerce.order_family
        children: order_items
        relationship: order_items_order
        columns:
          subtotal: subtotal
          discount: discount_total
          tax: tax_total
          shipping: shipping_total
          total: grand_total
        child_columns:
          quantity: quantity
          unit_price: unit_price
          discount: discount_amount
          tax: tax_amount
          line_total: line_total
        currency_scale: {scale}
        rounding: {rounding}
{extra}
  order_items:
    rows:
      kind: relation.children
      parent: orders
      count: 0
      distribution: {dist}
    schema:
      name: order_items
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: order_id, type: bigint, nullable: false }}
        - {{ name: quantity, type: integer, nullable: false }}
        - {{ name: unit_price, type: "{money}", nullable: false }}
        - {{ name: discount_amount, type: "{money}", nullable: false }}
        - {{ name: tax_amount, type: "{money}", nullable: false }}
        - {{ name: line_total, type: "{money}", nullable: false }}
    relationships:
      - name: order_items_order
        columns: [order_id]
        references: {{ table: orders, columns: [id] }}
    columns:
      id:
        generator: {{ kind: sequence, start: 1 }}
      order_id:
        generator: {{ kind: relation.foreign_key, relationship: order_items_order }}
"#
    )
}

/// Assert every exact minor-unit family invariant across every order.
fn assert_family_exact(sink: &MultiSink, has_shipping: bool) {
    let (o_sub, o_disc, o_tax, o_ship, o_total, o_id) = (
        sink.index("orders", "subtotal"),
        sink.index("orders", "discount_total"),
        sink.index("orders", "tax_total"),
        sink.index("orders", "shipping_total"),
        sink.index("orders", "grand_total"),
        sink.index("orders", "id"),
    );
    let (c_order, c_qty, c_price, c_disc, c_tax, c_total) = (
        sink.index("order_items", "order_id"),
        sink.index("order_items", "quantity"),
        sink.index("order_items", "unit_price"),
        sink.index("order_items", "discount_amount"),
        sink.index("order_items", "tax_amount"),
        sink.index("order_items", "line_total"),
    );

    let mut checked_lines = 0usize;
    for order in sink.rows("orders") {
        let id = int_of(&order[o_id]);
        let subtotal = minor(&order[o_sub]);
        let discount = minor(&order[o_disc]);
        let tax = minor(&order[o_tax]);
        let shipping = minor(&order[o_ship]);
        let grand = minor(&order[o_total]);

        let lines: Vec<&Vec<GeneratedValue>> = sink
            .rows("order_items")
            .iter()
            .filter(|line| int_of(&line[c_order]) == id)
            .collect();

        let mut sum_sub = 0i128;
        let mut sum_disc = 0i128;
        let mut sum_tax = 0i128;
        let mut sum_total = 0i128;
        for line in &lines {
            let qty = int_of(&line[c_qty]);
            let price = minor(&line[c_price]);
            let d = minor(&line[c_disc]);
            let t = minor(&line[c_tax]);
            let lt = minor(&line[c_total]);
            assert!(
                qty >= 0 && price >= 0 && d >= 0 && t >= 0,
                "order {id}: negative line value"
            );
            assert_eq!(
                lt,
                qty * price - d + t,
                "order {id}: line_total != qty*price - disc + tax"
            );
            sum_sub += qty * price;
            sum_disc += d;
            sum_tax += t;
            sum_total += lt;
            checked_lines += 1;
        }

        assert_eq!(sum_sub, subtotal, "order {id}: line subtotals != subtotal");
        assert_eq!(
            sum_disc, discount,
            "order {id}: line discounts != discount_total"
        );
        assert_eq!(sum_tax, tax, "order {id}: line taxes != tax_total");
        assert_eq!(
            sum_total,
            subtotal - discount + tax,
            "order {id}: line totals != net"
        );
        assert_eq!(
            grand,
            subtotal - discount + tax + shipping,
            "order {id}: grand_total equation"
        );
        if has_shipping {
            assert!(shipping > 0, "order {id}: expected positive shipping");
        }
    }
    assert!(checked_lines > 0, "no child lines were generated");
}

#[test]
fn order_family_is_exact_across_taxes_discounts_shipping_scales_and_rounding() {
    let dist = "{ kind: fixed, mean: 3.0, min: 1.0, max: 6.0 }";
    let taxes = [
        "        tax: { kind: fixed, rate: 0.0 }",
        "        tax: { kind: fixed, rate: 0.08 }",
        "        tax: { kind: fixed, rate: 0.25 }",
    ];
    for rounding in ["largest_remainder", "last_line", "bankers"] {
        for scale in [0u32, 2, 3] {
            for tax in taxes {
                let extra = format!(
                    "        quantity: {{ min: 1, max: 7 }}\n        unit_price: {{ min_minor: 1, max_minor: 250000 }}\n{tax}\n        discount: {{ kind: fixed_rate, rate: 0.1 }}\n        shipping: {{ kind: fixed, amount_minor: 500 }}"
                );
                let sink = run_multi(&order_family_model(99, 40, scale, rounding, dist, &extra));
                assert_family_exact(&sink, true);
            }
        }
    }
}

#[test]
fn order_family_handles_large_values_without_overflow() {
    let dist = "{ kind: fixed, mean: 5.0, min: 3.0, max: 8.0 }";
    let extra = "        quantity: { min: 100, max: 1000 }\n        unit_price: { min_minor: 1000000, max_minor: 9000000 }\n        tax: { kind: fixed, rate: 0.25 }\n        discount: { kind: fixed_rate, rate: 0.15 }";
    let sink = run_multi(&order_family_model(
        7,
        25,
        2,
        "largest_remainder",
        dist,
        extra,
    ));
    assert_family_exact(&sink, false);
}

#[test]
fn order_family_weighted_tax_still_sums_exactly() {
    let dist = "{ kind: fixed, mean: 4.0, min: 1.0, max: 10.0 }";
    let extra = "        quantity: { min: 1, max: 5 }\n        unit_price: { min_minor: 100, max_minor: 20000 }\n        tax:\n          kind: weighted_choice\n          rates: [0.0, 0.08, 0.25]\n          weights: [0.05, 0.15, 0.80]";
    let sink = run_multi(&order_family_model(3, 60, 2, "bankers", dist, extra));
    assert_family_exact(&sink, false);
    // The weighted tax should land on more than one rate across 60 orders, so at
    // least some orders carry a non-zero tax and some a different non-zero tax.
    let o_tax = sink.index("orders", "tax_total");
    let distinct: std::collections::BTreeSet<i128> = sink
        .rows("orders")
        .iter()
        .map(|order| minor(&order[o_tax]))
        .collect();
    assert!(
        distinct.len() >= 2,
        "weighted tax should vary across orders"
    );
}

#[test]
fn order_family_seeded_output_repeats_and_differs_by_seed() {
    let dist = "{ kind: fixed, mean: 3.0, min: 1.0, max: 6.0 }";
    let extra = "        quantity: { min: 1, max: 5 }\n        unit_price: { min_minor: 100, max_minor: 50000 }\n        tax: { kind: fixed, rate: 0.08 }";
    let first = run_multi(&order_family_model(
        11,
        30,
        2,
        "largest_remainder",
        dist,
        extra,
    ));
    let again = run_multi(&order_family_model(
        11,
        30,
        2,
        "largest_remainder",
        dist,
        extra,
    ));
    let other = run_multi(&order_family_model(
        12,
        30,
        2,
        "largest_remainder",
        dist,
        extra,
    ));
    assert_eq!(
        first.rows("order_items"),
        again.rows("order_items"),
        "same seed reproduces"
    );
    assert_ne!(
        first.rows("order_items"),
        other.rows("order_items"),
        "different seed diverges"
    );
}

// --- Compile diagnostics ----------------------------------------------------

/// A minimal valid extra block for compile-error models.
const OF_EXTRA: &str =
    "        quantity: { min: 1, max: 3 }\n        unit_price: { min_minor: 100, max_minor: 1000 }";

#[test]
fn order_family_undefined_child_is_a_compile_error() {
    let dist = "{ kind: fixed, mean: 3.0, min: 1.0, max: 6.0 }";
    let yaml = order_family_model(1, 5, 2, "largest_remainder", dist, OF_EXTRA)
        .replace("children: order_items", "children: nope_items");
    assert!(compile_err_code(&yaml).contains(&"GEN-ORDER-FAMILY-CHILD-UNKNOWN".to_string()));
}

#[test]
fn order_family_relationship_on_another_table_is_a_compile_error() {
    let dist = "{ kind: fixed, mean: 3.0, min: 1.0, max: 6.0 }";
    let yaml = order_family_model(1, 5, 2, "largest_remainder", dist, OF_EXTRA).replace(
        "relationship: order_items_order\n        columns:",
        "relationship: not_a_real_rel\n        columns:",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-ORDER-FAMILY-RELATIONSHIP".to_string()));
}

#[test]
fn order_family_zero_lines_with_nonzero_minimum_is_a_compile_error() {
    // A distribution whose max floors to zero lines while its minimum is 2.
    let dist = "{ kind: fixed, mean: 1.0, min: 2.0, max: 0.4 }";
    let yaml = order_family_model(1, 5, 2, "largest_remainder", dist, OF_EXTRA);
    assert!(compile_err_code(&yaml).contains(&"GEN-ORDER-FAMILY-ZERO-LINES".to_string()));
}

#[test]
fn order_family_ambiguous_currency_scale_is_a_compile_error() {
    // Money columns are declared decimal(18,2) but currency_scale is 3.
    let dist = "{ kind: fixed, mean: 3.0, min: 1.0, max: 6.0 }";
    let yaml = order_family_model(1, 5, 2, "largest_remainder", dist, OF_EXTRA)
        .replace("currency_scale: 2", "currency_scale: 3");
    assert!(compile_err_code(&yaml).contains(&"GEN-ORDER-FAMILY-SCALE".to_string()));
}

#[test]
fn order_family_decimal_overflow_is_a_compile_error() {
    // Tiny precision decimal(4,2) cannot hold large line values.
    let dist = "{ kind: fixed, mean: 3.0, min: 1.0, max: 6.0 }";
    let extra = "        quantity: { min: 10, max: 100 }\n        unit_price: { min_minor: 100000, max_minor: 900000 }";
    let yaml = order_family_model(1, 5, 2, "largest_remainder", dist, extra)
        .replace("decimal(18,2)", "decimal(4,2)");
    assert!(compile_err_code(&yaml).contains(&"GEN-ORDER-FAMILY-OVERFLOW".to_string()));
}

#[test]
fn order_family_missing_mapped_column_is_a_compile_error() {
    let dist = "{ kind: fixed, mean: 3.0, min: 1.0, max: 6.0 }";
    let yaml = order_family_model(1, 5, 2, "largest_remainder", dist, OF_EXTRA)
        .replace("subtotal: subtotal", "subtotal: does_not_exist");
    assert!(compile_err_code(&yaml).contains(&"GEN-ORDER-FAMILY-COLUMN-MISSING".to_string()));
}

#[test]
fn order_family_child_ownership_conflict_is_a_compile_error() {
    // A generator on order_items.quantity collides with the planner's ownership.
    let dist = "{ kind: fixed, mean: 3.0, min: 1.0, max: 6.0 }";
    let yaml = order_family_model(1, 5, 2, "largest_remainder", dist, OF_EXTRA).replace(
        "      order_id:\n        generator: { kind: relation.foreign_key, relationship: order_items_order }",
        "      order_id:\n        generator: { kind: relation.foreign_key, relationship: order_items_order }\n      quantity:\n        generator: { kind: constant, value: 1 }",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-COLUMN-OWNER-CONFLICT".to_string()));
}

#[test]
fn order_family_old_flat_form_is_an_unknown_field_error() {
    let dist = "{ kind: fixed, mean: 3.0, min: 1.0, max: 6.0 }";
    let extra = format!("{OF_EXTRA}\n        line_total: line_total");
    let yaml = order_family_model(1, 5, 2, "largest_remainder", dist, &extra);
    assert!(compile_err_code(&yaml).contains(&"GEN-ORDER-FAMILY-UNKNOWN-FIELD".to_string()));
}

#[test]
fn order_family_line_count_tracks_the_child_distribution_mean() {
    // The child `rows.distribution` is the SOLE line-count source, and its SHAPE
    // (kind + mean) — not just its bounds — must drive the draw. With
    // `observed, mean: 3.4, min: 1, max: 50`, the average lines/order must track
    // 3.4, NOT the midpoint (1+50)/2 = 25.5.
    let dist = "{ kind: observed, mean: 3.4, min: 1.0, max: 50.0 }";
    let extra = "        quantity: { min: 1, max: 5 }\n        unit_price: { min_minor: 100, max_minor: 50000 }\n        tax: { kind: fixed, rate: 0.08 }";
    let sink = run_multi(&order_family_model(
        2024,
        5_000,
        2,
        "largest_remainder",
        dist,
        extra,
    ));

    let orders = sink.rows("orders").len();
    let lines = sink.rows("order_items").len();
    assert_eq!(orders, 5_000);
    let avg = lines as f64 / orders as f64;
    assert!(
        (3.0..3.8).contains(&avg),
        "average lines/order {avg} should track the declared mean 3.4, not the midpoint 25.5"
    );
    // Bounds are still respected per order (1..=50).
    let c_order = sink.index("order_items", "order_id");
    let o_id = sink.index("orders", "id");
    let mut counts: BTreeMap<i128, usize> = BTreeMap::new();
    for line in sink.rows("order_items") {
        *counts.entry(int_of(&line[c_order])).or_default() += 1;
    }
    for order in sink.rows("orders") {
        let id = int_of(&order[o_id]);
        let n = counts.get(&id).copied().unwrap_or(0);
        assert!((1..=50).contains(&n), "order {id}: {n} lines out of [1,50]");
    }
    // Money invariants must be unchanged for any line count.
    assert_family_exact(&sink, false);
}

// === temporal.timestamps / temporal.soft_delete / temporal.lifecycle (Task 27) ==
//
// Three small same-table planners sharing the interval/progress execution
// pattern: `temporal.timestamps` (created <= updated, plus optional trailing
// timestamps), `temporal.soft_delete` (a coherent deleted_at/is_deleted pair),
// and `temporal.lifecycle` (a status column that only ever reaches legal
// states, each carrying a correctly-ordered timestamp).

fn timestamps_model(seed: u64, rows: u64, overrides: &str, extra_columns: &str) -> String {
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: schema }}
seed: {seed}
tables:
  accounts:
    rows: {{ kind: fixed, count: {rows} }}
    schema:
      name: accounts
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: created_at, type: timestamp, nullable: false }}
        - {{ name: updated_at, type: timestamp, nullable: false }}
{extra_columns}
    planners:
      - kind: temporal.timestamps
        columns:
          created_at: created_at
          updated_at: updated_at
{overrides}
        created:
          kind: range
          min: "2024-01-01T00:00:00Z"
          max: "2026-01-01T00:00:00Z"
        update_delay:
          kind: uniform
          unit: seconds
          min: 0
          max: 86400
"#
    )
}

#[test]
fn timestamps_created_is_never_after_updated_for_many_rows() {
    let sink = run(&timestamps_model(1, 20_000, "", ""));
    let created: Vec<i128> = sink
        .column("created_at")
        .map(|v| instant_ns(datetime_text(v)))
        .collect();
    let updated: Vec<i128> = sink
        .column("updated_at")
        .map(|v| instant_ns(datetime_text(v)))
        .collect();
    assert_eq!(created.len(), 20_000);
    for (i, (c, u)) in created.iter().zip(&updated).enumerate() {
        assert!(c <= u, "row {i}: created_at {c} > updated_at {u}");
    }
    // The delay isn't degenerate: at least some rows show a real gap.
    assert!(
        created.iter().zip(&updated).any(|(c, u)| u > c),
        "expected at least one row with updated_at strictly after created_at"
    );
}

#[test]
fn timestamps_other_columns_are_never_before_created() {
    let overrides = "          last_login_at: last_login_at\n";
    let extra_columns = "        - { name: last_login_at, type: timestamp, nullable: false }";
    let sink = run(&timestamps_model(2, 5_000, overrides, extra_columns));
    let created: Vec<i128> = sink
        .column("created_at")
        .map(|v| instant_ns(datetime_text(v)))
        .collect();
    let logins: Vec<i128> = sink
        .column("last_login_at")
        .map(|v| instant_ns(datetime_text(v)))
        .collect();
    for (i, (c, l)) in created.iter().zip(&logins).enumerate() {
        assert!(c <= l, "row {i}: created_at {c} > last_login_at {l}");
    }
}

#[test]
fn timestamps_seeded_output_repeats_and_differs_by_seed() {
    let first = run(&timestamps_model(9, 1_000, "", ""));
    let again = run(&timestamps_model(9, 1_000, "", ""));
    let other = run(&timestamps_model(10, 1_000, "", ""));
    assert_eq!(first.rows, again.rows, "same seed must reproduce rows");
    assert_ne!(first.rows, other.rows, "a different seed must diverge");
}

#[test]
fn timestamps_returns_exact_verification_predicates() {
    use sql_splitter::generate::PlannerPredicate;

    let plan = compile_result(&timestamps_model(1, 10, "", "")).expect("model compiles cleanly");
    let predicates =
        plan.table("accounts").expect("accounts table").planners[0].verification_predicates();
    assert!(predicates.iter().any(|p| matches!(
        p,
        PlannerPredicate::Ordering { earlier, later, guard: None }
            if earlier == "created_at" && later == "updated_at"
    )));
}

#[test]
fn timestamps_missing_owned_column_is_a_compile_error() {
    let yaml = timestamps_model(1, 10, "", "")
        .replace("updated_at: updated_at", "updated_at: nonexistent_column");
    assert!(compile_err_code(&yaml).contains(&"GEN-TIMESTAMPS-COLUMN-MISSING".to_string()));
}

#[test]
fn timestamps_impossible_range_is_a_compile_error() {
    let yaml = timestamps_model(1, 10, "", "").replace(
        "max: \"2026-01-01T00:00:00Z\"",
        "max: \"2020-01-01T00:00:00Z\"",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-TIMESTAMPS-RANGE".to_string()));
}

#[test]
fn timestamps_ownership_collision_is_a_compile_error() {
    let yaml = timestamps_model(1, 10, "", "").replace(
        "    planners:",
        "    columns:\n      created_at:\n        generator: { kind: datetime }\n    planners:",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-COLUMN-OWNER-CONFLICT".to_string()));
}

// --- temporal.soft_delete ----------------------------------------------------

fn soft_delete_model(seed: u64, rows: u64, deleted_at_nullable: bool, probability: f64) -> String {
    let nullable = if deleted_at_nullable { "true" } else { "false" };
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: schema }}
seed: {seed}
tables:
  widgets:
    rows: {{ kind: fixed, count: {rows} }}
    schema:
      name: widgets
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: deleted_at, type: timestamp, nullable: {nullable} }}
        - {{ name: is_deleted, type: boolean, nullable: false }}
    planners:
      - kind: temporal.soft_delete
        columns:
          deleted_at: deleted_at
          is_deleted: is_deleted
        deletion_probability: {probability}
        deleted_range:
          kind: range
          min: "2024-01-01T00:00:00Z"
          max: "2026-01-01T00:00:00Z"
"#
    )
}

#[test]
fn soft_delete_null_and_flag_are_coherent_for_many_rows() {
    let sink = run(&soft_delete_model(1, 20_000, true, 0.3));
    let mut deleted_rows = 0;
    for (i, (deleted_at, is_deleted)) in sink
        .column("deleted_at")
        .zip(sink.column("is_deleted"))
        .enumerate()
    {
        let flag = is_deleted.as_boolean().expect("boolean flag");
        if flag {
            deleted_rows += 1;
            assert!(
                !deleted_at.is_null(),
                "row {i}: is_deleted=true must carry a non-null deleted_at"
            );
        } else {
            assert!(
                deleted_at.is_null(),
                "row {i}: is_deleted=false must carry a null deleted_at"
            );
        }
    }
    // ~30% of 20k rows should be deleted — assert a broad band.
    assert!(
        (4_000..8_000).contains(&deleted_rows),
        "expected ~30% deleted rows, saw {deleted_rows}"
    );
}

#[test]
fn soft_delete_all_rows_deleted_when_probability_is_one() {
    let sink = run(&soft_delete_model(2, 500, true, 1.0));
    for deleted_at in sink.column("deleted_at") {
        assert!(!deleted_at.is_null());
    }
    for is_deleted in sink.column("is_deleted") {
        assert!(is_deleted.as_boolean().expect("boolean"));
    }
}

#[test]
fn soft_delete_no_rows_deleted_when_probability_is_zero() {
    let sink = run(&soft_delete_model(3, 500, true, 0.0));
    for deleted_at in sink.column("deleted_at") {
        assert!(deleted_at.is_null());
    }
    for is_deleted in sink.column("is_deleted") {
        assert!(!is_deleted.as_boolean().expect("boolean"));
    }
}

#[test]
fn soft_delete_seeded_output_repeats_and_differs_by_seed() {
    let first = run(&soft_delete_model(4, 1_000, true, 0.4));
    let again = run(&soft_delete_model(4, 1_000, true, 0.4));
    let other = run(&soft_delete_model(5, 1_000, true, 0.4));
    assert_eq!(first.rows, again.rows, "same seed must reproduce rows");
    assert_ne!(first.rows, other.rows, "a different seed must diverge");
}

#[test]
fn soft_delete_returns_exact_verification_predicates() {
    use sql_splitter::generate::{PlannerPredicate, PredicateGuard};

    let plan =
        compile_result(&soft_delete_model(1, 10, true, 0.3)).expect("model compiles cleanly");
    let predicates =
        plan.table("widgets").expect("widgets table").planners[0].verification_predicates();
    assert!(predicates.iter().any(|p| matches!(
        p,
        PlannerPredicate::NotNullWhen { column, guard: PredicateGuard::Flag { column: flag, value: true } }
            if column == "deleted_at" && flag == "is_deleted"
    )));
    assert!(predicates.iter().any(|p| matches!(
        p,
        PlannerPredicate::NullWhen { column, guard: PredicateGuard::Flag { column: flag, value: false } }
            if column == "deleted_at" && flag == "is_deleted"
    )));
}

#[test]
fn soft_delete_non_nullable_deleted_at_with_partial_probability_is_a_compile_error() {
    let yaml = soft_delete_model(1, 10, false, 0.3);
    assert!(compile_err_code(&yaml).contains(&"GEN-SOFT-DELETE-NULLABILITY".to_string()));
    // A probability of 1.0 never needs a null deleted_at, so it's fine.
    assert!(compile_result(&soft_delete_model(1, 10, false, 1.0)).is_ok());
}

#[test]
fn soft_delete_missing_owned_column_is_a_compile_error() {
    let yaml = soft_delete_model(1, 10, true, 0.3)
        .replace("deleted_at: deleted_at", "deleted_at: nonexistent_column");
    assert!(compile_err_code(&yaml).contains(&"GEN-SOFT-DELETE-COLUMN-MISSING".to_string()));
}

#[test]
fn soft_delete_impossible_range_is_a_compile_error() {
    let yaml = soft_delete_model(1, 10, true, 0.3).replace(
        "max: \"2026-01-01T00:00:00Z\"",
        "max: \"2020-01-01T00:00:00Z\"",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-SOFT-DELETE-RANGE".to_string()));
}

#[test]
fn soft_delete_ownership_collision_is_a_compile_error() {
    let yaml = soft_delete_model(1, 10, true, 0.3).replace(
        "    planners:",
        "    columns:\n      deleted_at:\n        generator: { kind: datetime }\n    planners:",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-COLUMN-OWNER-CONFLICT".to_string()));
}

// --- temporal.lifecycle ------------------------------------------------------

fn lifecycle_model(seed: u64, rows: u64, archived_nullable: bool) -> String {
    let nullable = if archived_nullable { "true" } else { "false" };
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: schema }}
seed: {seed}
tables:
  orders:
    rows: {{ kind: fixed, count: {rows} }}
    schema:
      name: orders
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: status, type: text, nullable: false }}
        - {{ name: created_at, type: timestamp, nullable: false }}
        - {{ name: activated_at, type: timestamp, nullable: true }}
        - {{ name: archived_at, type: timestamp, nullable: {nullable} }}
    planners:
      - kind: temporal.lifecycle
        columns:
          status: status
          draft: created_at
          active: activated_at
          archived: archived_at
        states: [draft, active, archived]
        weights: [0.2, 0.5, 0.3]
        start:
          kind: range
          min: "2024-01-01T00:00:00Z"
          max: "2024-06-01T00:00:00Z"
        step:
          kind: uniform
          unit: seconds
          min: 60
          max: 86400
"#
    )
}

#[test]
fn lifecycle_only_reaches_legal_states_with_ordered_timestamps() {
    let sink = run(&lifecycle_model(1, 20_000, true));
    let statuses: Vec<&str> = sink.column("status").map(status_text).collect();
    let legal = ["draft", "active", "archived"];
    for (i, status) in statuses.iter().enumerate() {
        assert!(legal.contains(status), "row {i}: illegal status {status}");
    }
    let mut seen: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
    for (i, ((status, created), (activated, archived))) in statuses
        .iter()
        .zip(sink.column("created_at"))
        .zip(sink.column("activated_at").zip(sink.column("archived_at")))
        .enumerate()
    {
        seen.insert(status);
        assert!(!created.is_null(), "row {i}: created_at must be set");
        let created_ns = instant_ns(datetime_text(created));
        match *status {
            "draft" => {
                assert!(activated.is_null(), "row {i}: draft must not be activated");
                assert!(archived.is_null(), "row {i}: draft must not be archived");
            }
            "active" => {
                assert!(!activated.is_null(), "row {i}: active must be activated");
                assert!(archived.is_null(), "row {i}: active must not be archived");
                let activated_ns = instant_ns(datetime_text(activated));
                assert!(
                    created_ns <= activated_ns,
                    "row {i}: created after activated"
                );
            }
            "archived" => {
                assert!(
                    !activated.is_null(),
                    "row {i}: archived must have been activated"
                );
                assert!(
                    !archived.is_null(),
                    "row {i}: archived must carry archived_at"
                );
                let activated_ns = instant_ns(datetime_text(activated));
                let archived_ns = instant_ns(datetime_text(archived));
                assert!(
                    created_ns <= activated_ns,
                    "row {i}: created after activated"
                );
                assert!(
                    activated_ns <= archived_ns,
                    "row {i}: activated after archived"
                );
            }
            other => panic!("unexpected status {other}"),
        }
    }
    // With positive weights on all three states, a large sample reaches all of them.
    assert_eq!(
        seen.len(),
        3,
        "expected all three states to occur, saw {seen:?}"
    );
}

#[test]
fn lifecycle_seeded_output_repeats_and_differs_by_seed() {
    let first = run(&lifecycle_model(7, 1_000, true));
    let again = run(&lifecycle_model(7, 1_000, true));
    let other = run(&lifecycle_model(8, 1_000, true));
    assert_eq!(first.rows, again.rows, "same seed must reproduce rows");
    assert_ne!(first.rows, other.rows, "a different seed must diverge");
}

#[test]
fn lifecycle_returns_exact_verification_predicates() {
    use sql_splitter::generate::{PlannerPredicate, PredicateGuard};

    let plan = compile_result(&lifecycle_model(1, 10, true)).expect("model compiles cleanly");
    let predicates =
        plan.table("orders").expect("orders table").planners[0].verification_predicates();
    assert!(predicates.iter().any(|p| matches!(
        p,
        PlannerPredicate::NotNullWhen { column, guard: PredicateGuard::Equals { column: status, value } }
            if column == "activated_at" && status == "status" && value == "active"
    )));
    assert!(predicates.iter().any(|p| matches!(
        p,
        PlannerPredicate::NullWhen { column, guard: PredicateGuard::Equals { column: status, value } }
            if column == "activated_at" && status == "status" && value == "draft"
    )));
    assert!(predicates.iter().any(|p| matches!(
        p,
        PlannerPredicate::Ordering { earlier, later, .. }
            if earlier == "created_at" && later == "activated_at"
    )));
    assert!(predicates.iter().any(|p| matches!(
        p,
        PlannerPredicate::Ordering { earlier, later, .. }
            if earlier == "activated_at" && later == "archived_at"
    )));
}

#[test]
fn lifecycle_unknown_status_vocabulary_is_a_compile_error() {
    let yaml =
        lifecycle_model(1, 10, true).replace("archived: archived_at", "canceled: archived_at");
    assert!(compile_err_code(&yaml).contains(&"GEN-LIFECYCLE-STATUS-VOCABULARY".to_string()));
}

#[test]
fn lifecycle_impossible_nullability_is_a_compile_error() {
    // archived_at is non-nullable, but draft/active rows (weight > 0) leave it null.
    let yaml = lifecycle_model(1, 10, false);
    assert!(compile_err_code(&yaml).contains(&"GEN-LIFECYCLE-NULLABILITY".to_string()));
}

#[test]
fn lifecycle_impossible_range_is_a_compile_error() {
    let yaml =
        lifecycle_model(1, 10, true).replace("min: 60", "min: \"100000000000000000000000000\"");
    assert!(compile_err_code(&yaml).contains(&"GEN-LIFECYCLE-STEP".to_string()));
}

#[test]
fn lifecycle_impossible_start_range_is_a_compile_error() {
    // The `start` block's own `max` (distinct from the `step` block's) is
    // before its `min`, so no legal base instant exists.
    let yaml = lifecycle_model(1, 10, true).replace(
        "max: \"2024-06-01T00:00:00Z\"",
        "max: \"2020-01-01T00:00:00Z\"",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-LIFECYCLE-RANGE".to_string()));
}

#[test]
fn lifecycle_missing_owned_column_is_a_compile_error() {
    let yaml = lifecycle_model(1, 10, true).replace("status: status", "status: nonexistent_column");
    assert!(compile_err_code(&yaml).contains(&"GEN-LIFECYCLE-COLUMN-MISSING".to_string()));
}

#[test]
fn lifecycle_ownership_collision_is_a_compile_error() {
    let yaml = lifecycle_model(1, 10, true).replace(
        "    planners:",
        "    columns:\n      status:\n        generator: { kind: constant, value: draft }\n    planners:",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-COLUMN-OWNER-CONFLICT".to_string()));
}

// === hierarchy.tree (Task 28) ===============================================
//
// A self-referential parent_id tree: roots (configurable ratio) carry a null
// parent, and every non-root references an EARLIER row (parent-before-child by
// generation order) within a bounded depth and branching factor.

/// A `categories` self-tree model. `parent_type` toggles the parent column's
/// nullability; `overrides` are extra indented planner keys.
fn tree_model(seed: u64, rows: u64, parent_type: &str, overrides: &str) -> String {
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: schema }}
seed: {seed}
tables:
  categories:
    rows: {{ kind: fixed, count: {rows} }}
    schema:
      name: categories
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: parent_id, type: bigint, {parent_type} }}
    relationships:
      - name: category_parent
        columns: [parent_id]
        references: {{ table: categories, columns: [id] }}
    planners:
      - kind: hierarchy.tree
        columns:
          parent: parent_id
        relationship: category_parent
        root_ratio: 0.15
        max_depth: 4
{overrides}
"#
    )
}

/// The `(id -> parent_id option)` map of a generated `categories` tree.
fn tree_edges(sink: &CollectingSink) -> BTreeMap<i128, Option<i128>> {
    let ids = sink.index("id");
    let parents = sink.index("parent_id");
    sink.rows
        .iter()
        .map(|row| {
            let id = row[ids].as_integer().expect("integer id");
            let parent = match &row[parents] {
                GeneratedValue::Null => None,
                other => Some(other.as_integer().expect("integer parent")),
            };
            (id, parent)
        })
        .collect()
}

/// Depth of `id` by walking parents to a root; panics on a cycle.
fn tree_depth(edges: &BTreeMap<i128, Option<i128>>, id: i128) -> u32 {
    let mut depth = 0;
    let mut current = id;
    let mut seen = std::collections::BTreeSet::new();
    while let Some(Some(parent)) = edges.get(&current) {
        assert!(seen.insert(current), "cycle detected at {current}");
        depth += 1;
        current = *parent;
        assert!(depth < 10_000, "runaway depth for {id}");
    }
    depth
}

#[test]
fn tree_is_bounded_depth_with_roots_and_earlier_parents() {
    let sink = run(&tree_model(
        42,
        5_000,
        "nullable: true",
        "        max_branching: 5",
    ));
    let edges = tree_edges(&sink);
    assert_eq!(edges.len(), 5_000);

    let mut roots = 0;
    let mut child_counts: BTreeMap<i128, u32> = BTreeMap::new();
    for (&id, &parent) in &edges {
        match parent {
            None => roots += 1,
            Some(parent) => {
                // Parent must be a valid, EARLIER id (parent-before-child).
                assert!(
                    edges.contains_key(&parent),
                    "id {id}: parent {parent} absent"
                );
                assert!(parent < id, "id {id}: parent {parent} is not earlier");
                *child_counts.entry(parent).or_default() += 1;
            }
        }
        // Depth is bounded by max_depth.
        assert!(tree_depth(&edges, id) <= 4, "id {id}: exceeds max depth 4");
    }
    // Branching is bounded, and both roots and non-roots appear.
    assert!(
        child_counts.values().all(|&c| c <= 5),
        "a parent exceeded max_branching 5"
    );
    assert!(roots > 0, "a tree must have at least one root");
    assert!(roots < 5_000, "not every row can be a root");
}

#[test]
fn tree_seeded_output_repeats_and_differs_by_seed() {
    let overrides = "        max_branching: 4";
    let first = run(&tree_model(7, 2_000, "nullable: true", overrides));
    let again = run(&tree_model(7, 2_000, "nullable: true", overrides));
    let other = run(&tree_model(8, 2_000, "nullable: true", overrides));
    assert_eq!(first.rows, again.rows, "same seed reproduces");
    assert_ne!(first.rows, other.rows, "different seed diverges");
}

#[test]
fn tree_returns_nonnegative_parent_predicate() {
    use sql_splitter::generate::PlannerPredicate;
    let plan =
        compile_result(&tree_model(1, 10, "nullable: true", "")).expect("model compiles cleanly");
    let predicates =
        plan.table("categories").expect("categories").planners[0].verification_predicates();
    assert!(predicates.iter().any(|p| matches!(
        p,
        PlannerPredicate::NonNegative { columns } if columns.contains(&"parent_id".to_string())
    )));
}

#[test]
fn tree_non_nullable_parent_is_a_required_cycle_compile_error() {
    // A non-nullable self-FK cannot represent roots, so the tree is a required
    // non-null cycle with no constructible seed.
    let yaml = tree_model(1, 10, "nullable: false", "");
    assert!(compile_err_code(&yaml).contains(&"GEN-TREE-REQUIRED-CYCLE".to_string()));
}

#[test]
fn tree_zero_max_depth_is_a_compile_error() {
    let yaml = tree_model(1, 10, "nullable: true", "").replace("max_depth: 4", "max_depth: 0");
    assert!(compile_err_code(&yaml).contains(&"GEN-TREE-DEPTH".to_string()));
}

#[test]
fn tree_root_ratio_out_of_range_is_a_compile_error() {
    let yaml =
        tree_model(1, 10, "nullable: true", "").replace("root_ratio: 0.15", "root_ratio: 1.5");
    assert!(compile_err_code(&yaml).contains(&"GEN-TREE-ROOT-RATIO".to_string()));
}

#[test]
fn tree_missing_parent_column_is_a_compile_error() {
    let yaml = tree_model(1, 10, "nullable: true", "").replace("parent: parent_id", "parent: nope");
    assert!(compile_err_code(&yaml).contains(&"GEN-TREE-COLUMN-MISSING".to_string()));
}

#[test]
fn tree_unknown_relationship_is_a_compile_error() {
    // A same-table planner's `relationship` is validated by the compiler's
    // generic relationship-reference check.
    let yaml = tree_model(1, 10, "nullable: true", "")
        .replace("relationship: category_parent", "relationship: nope");
    assert!(compile_err_code(&yaml).contains(&"GEN-RELATIONSHIP-UNKNOWN".to_string()));
}

// === relation.junction_pair (Task 28) =======================================
//
// A junction row references two parents with UNIQUE (left, right) pairs. A
// deterministic pair-index permutation makes uniqueness hold by construction.

/// A `user_roles` junction between `users` and `roles`. `id_type` sets the
/// parents' key type/generator so a key-domain error can be provoked.
fn junction_model(seed: u64, users: u64, roles: u64, rows: u64, id_line: &str) -> String {
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: schema }}
seed: {seed}
tables:
  users:
    rows: {{ kind: fixed, count: {users} }}
    schema:
      name: users
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
    columns:
      id: {{ generator: {{ kind: sequence, start: 1 }} }}
  roles:
    rows: {{ kind: fixed, count: {roles} }}
    schema:
      name: roles
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
    columns:
      id: {{ generator: {{ kind: sequence, start: 1 }} }}
  user_roles:
    rows: {{ kind: fixed, count: {rows} }}
    schema:
      name: user_roles
      columns:
        - {{ name: user_id, type: bigint, nullable: false }}
        - {id_line}
    relationships:
      - name: junction_user
        columns: [user_id]
        references: {{ table: users, columns: [id] }}
      - name: junction_role
        columns: [role_id]
        references: {{ table: roles, columns: [id] }}
    planners:
      - kind: relation.junction_pair
        columns:
          left: user_id
          right: role_id
        left_relationship: junction_user
        right_relationship: junction_role
"#
    )
}

const ROLE_ID_LINE: &str = "{ name: role_id, type: bigint, nullable: false }";

#[test]
fn junction_pairs_are_unique_and_valid() {
    let sink = run_multi(&junction_model(42, 30, 20, 400, ROLE_ID_LINE));
    let u = sink.index("user_roles", "user_id");
    let r = sink.index("user_roles", "role_id");
    let rows = sink.rows("user_roles");
    assert_eq!(rows.len(), 400);

    let mut seen = std::collections::BTreeSet::new();
    for row in rows {
        let user = int_of(&row[u]);
        let role = int_of(&row[r]);
        assert!((1..=30).contains(&user), "user_id {user} out of range");
        assert!((1..=20).contains(&role), "role_id {role} out of range");
        assert!(seen.insert((user, role)), "duplicate edge ({user}, {role})");
    }
    // 400 distinct edges must span a good fraction of both parents.
    let users: std::collections::BTreeSet<i128> = rows.iter().map(|row| int_of(&row[u])).collect();
    assert!(
        users.len() > 10,
        "edges should span many users, saw {}",
        users.len()
    );
}

#[test]
fn junction_seeded_output_repeats_and_differs_by_seed() {
    let first = run_multi(&junction_model(7, 25, 15, 200, ROLE_ID_LINE));
    let again = run_multi(&junction_model(7, 25, 15, 200, ROLE_ID_LINE));
    let other = run_multi(&junction_model(8, 25, 15, 200, ROLE_ID_LINE));
    assert_eq!(
        first.rows("user_roles"),
        again.rows("user_roles"),
        "same seed reproduces"
    );
    assert_ne!(
        first.rows("user_roles"),
        other.rows("user_roles"),
        "different seed diverges"
    );
}

#[test]
fn junction_exhausted_pairs_is_a_compile_error() {
    // 2 x 2 = 4 possible pairs, but 5 rows are requested.
    let yaml = junction_model(1, 2, 2, 5, ROLE_ID_LINE);
    assert!(compile_err_code(&yaml).contains(&"GEN-JUNCTION-EXHAUSTED".to_string()));
}

#[test]
fn junction_missing_column_is_a_compile_error() {
    let yaml = junction_model(1, 5, 5, 5, ROLE_ID_LINE).replace("left: user_id", "left: nope");
    assert!(compile_err_code(&yaml).contains(&"GEN-JUNCTION-COLUMN-MISSING".to_string()));
}

#[test]
fn junction_unknown_relationship_is_a_compile_error() {
    let yaml = junction_model(1, 5, 5, 5, ROLE_ID_LINE).replace(
        "left_relationship: junction_user",
        "left_relationship: nope",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-JUNCTION-RELATIONSHIP".to_string()));
}

#[test]
fn junction_non_dense_key_is_a_compile_error() {
    // A UUID parent key has no dense integer key domain.
    let yaml = junction_model(1, 5, 5, 5, ROLE_ID_LINE)
        .replace(
            "        - { name: id, type: bigint, nullable: false, primary_key: true }\n    columns:\n      id: { generator: { kind: sequence, start: 1 } }\n  roles:",
            "        - { name: id, type: uuid, nullable: false, primary_key: true }\n    columns:\n      id: { generator: { kind: uuid } }\n  roles:",
        );
    assert!(compile_err_code(&yaml).contains(&"GEN-JUNCTION-KEY-UNSUPPORTED".to_string()));
}

// === relation.polymorphic_pair (Task 28) ====================================
//
// A (type, id) pair where the type selects one of several target tables
// (weighted) and the id is a VALID key in that target. Type and id are chosen
// together, never independently.

/// A `comments` table with a polymorphic (type, id) pair over `posts`/`photos`.
fn polymorphic_model(seed: u64, posts: u64, photos: u64, rows: u64, targets: &str) -> String {
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: schema }}
seed: {seed}
tables:
  posts:
    rows: {{ kind: fixed, count: {posts} }}
    schema:
      name: posts
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
    columns:
      id: {{ generator: {{ kind: sequence, start: 1 }} }}
  photos:
    rows: {{ kind: fixed, count: {photos} }}
    schema:
      name: photos
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
    columns:
      id: {{ generator: {{ kind: sequence, start: 1 }} }}
  comments:
    rows: {{ kind: fixed, count: {rows} }}
    schema:
      name: comments
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: commentable_type, type: text, nullable: false }}
        - {{ name: commentable_id, type: bigint, nullable: false }}
    columns:
      id: {{ generator: {{ kind: sequence, start: 1 }} }}
    planners:
      - kind: relation.polymorphic_pair
        columns:
          type: commentable_type
          id: commentable_id
        targets:
{targets}
"#
    )
}

const POLY_TARGETS: &str =
    "          - { table: posts, type: \"Post\", weight: 3 }\n          - { table: photos, type: \"Photo\", weight: 1 }";

#[test]
fn polymorphic_type_and_id_are_valid_in_the_chosen_target() {
    let sink = run_multi(&polymorphic_model(42, 20, 8, 2_000, POLY_TARGETS));
    let t = sink.index("comments", "commentable_type");
    let i = sink.index("comments", "commentable_id");
    let rows = sink.rows("comments");
    assert_eq!(rows.len(), 2_000);

    let (mut posts_seen, mut photos_seen) = (0, 0);
    for row in rows {
        let type_name = row[t].as_text().expect("text type");
        let id = int_of(&row[i]);
        match type_name {
            "Post" => {
                assert!((1..=20).contains(&id), "Post id {id} out of range");
                posts_seen += 1;
            }
            "Photo" => {
                assert!((1..=8).contains(&id), "Photo id {id} out of range");
                photos_seen += 1;
            }
            other => panic!("unexpected polymorphic type `{other}`"),
        }
    }
    // Both targets appear, and the 3:1 weighting makes Post dominate.
    assert!(photos_seen > 100, "photos should appear, saw {photos_seen}");
    assert!(
        posts_seen > photos_seen,
        "Post ({posts_seen}) should outnumber Photo ({photos_seen}) at weight 3:1"
    );
}

#[test]
fn polymorphic_seeded_output_repeats_and_differs_by_seed() {
    let first = run_multi(&polymorphic_model(7, 20, 8, 500, POLY_TARGETS));
    let again = run_multi(&polymorphic_model(7, 20, 8, 500, POLY_TARGETS));
    let other = run_multi(&polymorphic_model(8, 20, 8, 500, POLY_TARGETS));
    assert_eq!(
        first.rows("comments"),
        again.rows("comments"),
        "same seed reproduces"
    );
    assert_ne!(
        first.rows("comments"),
        other.rows("comments"),
        "different seed diverges"
    );
}

#[test]
fn polymorphic_missing_column_is_a_compile_error() {
    let yaml =
        polymorphic_model(1, 5, 5, 5, POLY_TARGETS).replace("type: commentable_type", "type: nope");
    assert!(compile_err_code(&yaml).contains(&"GEN-POLYMORPHIC-COLUMN-MISSING".to_string()));
}

#[test]
fn polymorphic_unknown_target_is_a_compile_error() {
    let targets = "          - { table: nonexistent, type: \"X\", weight: 1 }";
    let yaml = polymorphic_model(1, 5, 5, 5, targets);
    assert!(compile_err_code(&yaml).contains(&"GEN-POLYMORPHIC-TARGET-UNKNOWN".to_string()));
}

#[test]
fn polymorphic_non_dense_target_key_is_a_compile_error() {
    // A UUID-keyed target has no dense integer id domain.
    let yaml = polymorphic_model(1, 5, 5, 5, POLY_TARGETS).replace(
        "        - { name: id, type: bigint, nullable: false, primary_key: true }\n    columns:\n      id: { generator: { kind: sequence, start: 1 } }\n  photos:",
        "        - { name: id, type: uuid, nullable: false, primary_key: true }\n    columns:\n      id: { generator: { kind: uuid } }\n  photos:",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-POLYMORPHIC-KEY-UNSUPPORTED".to_string()));
}

// === relation.tenant_family (Task 28) =======================================
//
// A child references a parent that shares the child's tenant: the parent rows
// are partitioned into contiguous tenant blocks, and each child's FK is drawn
// from the block of the child's own tenant.

/// A `memberships` table whose FK to `customers` is drawn from the same tenant.
fn tenant_model(seed: u64, customers: u64, rows: u64, num_tenants: u64) -> String {
    format!(
        r#"
version: 1
kind: model
defaults: {{ inference: schema }}
seed: {seed}
tables:
  customers:
    rows: {{ kind: fixed, count: {customers} }}
    schema:
      name: customers
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
    columns:
      id: {{ generator: {{ kind: sequence, start: 1 }} }}
  memberships:
    rows: {{ kind: fixed, count: {rows} }}
    schema:
      name: memberships
      columns:
        - {{ name: id, type: bigint, nullable: false, primary_key: true }}
        - {{ name: tenant_id, type: bigint, nullable: false }}
        - {{ name: customer_id, type: bigint, nullable: false }}
    columns:
      id: {{ generator: {{ kind: sequence, start: 1 }} }}
    relationships:
      - name: membership_customer
        columns: [customer_id]
        references: {{ table: customers, columns: [id] }}
    planners:
      - kind: relation.tenant_family
        columns:
          tenant: tenant_id
          parent: customer_id
        relationship: membership_customer
        num_tenants: {num_tenants}
"#
    )
}

#[test]
fn tenant_family_selects_a_same_tenant_parent() {
    // 100 customers, 5 tenants => contiguous blocks of 20 parent rows each.
    let customers = 100u64;
    let tenants = 5u64;
    let sink = run_multi(&tenant_model(42, customers, 800, tenants));
    let t = sink.index("memberships", "tenant_id");
    let c = sink.index("memberships", "customer_id");
    let rows = sink.rows("memberships");
    assert_eq!(rows.len(), 800);

    let mut tenants_seen = std::collections::BTreeSet::new();
    for row in rows {
        let tenant = int_of(&row[t]);
        let customer = int_of(&row[c]);
        assert!(
            (1..=customers as i128).contains(&customer),
            "customer {customer} out of range"
        );
        // The parent row index (0-based) is key - 1 (dense start 1, step 1);
        // the parent's tenant is `p * T / count` (the contiguous block).
        let parent_row = customer - 1;
        let parent_tenant = parent_row * tenants as i128 / customers as i128;
        assert_eq!(
            tenant, parent_tenant,
            "child tenant {tenant} != parent tenant {parent_tenant} for customer {customer}"
        );
        tenants_seen.insert(tenant);
    }
    assert_eq!(
        tenants_seen.len(),
        tenants as usize,
        "every tenant should appear"
    );
}

#[test]
fn tenant_family_seeded_output_repeats_and_differs_by_seed() {
    let first = run_multi(&tenant_model(7, 60, 300, 4));
    let again = run_multi(&tenant_model(7, 60, 300, 4));
    let other = run_multi(&tenant_model(8, 60, 300, 4));
    assert_eq!(
        first.rows("memberships"),
        again.rows("memberships"),
        "same seed reproduces"
    );
    assert_ne!(
        first.rows("memberships"),
        other.rows("memberships"),
        "different seed diverges"
    );
}

#[test]
fn tenant_family_missing_column_is_a_compile_error() {
    let yaml = tenant_model(1, 20, 10, 4).replace("tenant: tenant_id", "tenant: nope");
    assert!(compile_err_code(&yaml).contains(&"GEN-TENANT-COLUMN-MISSING".to_string()));
}

#[test]
fn tenant_family_too_many_tenants_is_a_compile_error() {
    // More tenants than parent rows leaves a tenant with no parent to reference.
    let yaml = tenant_model(1, 3, 10, 8);
    assert!(compile_err_code(&yaml).contains(&"GEN-TENANT-PARTITION".to_string()));
}

#[test]
fn tenant_family_non_dense_key_is_a_compile_error() {
    let yaml = tenant_model(1, 20, 10, 4).replace(
        "        - { name: id, type: bigint, nullable: false, primary_key: true }\n    columns:\n      id: { generator: { kind: sequence, start: 1 } }\n  memberships:",
        "        - { name: id, type: uuid, nullable: false, primary_key: true }\n    columns:\n      id: { generator: { kind: uuid } }\n  memberships:",
    );
    assert!(compile_err_code(&yaml).contains(&"GEN-TENANT-KEY-UNSUPPORTED".to_string()));
}

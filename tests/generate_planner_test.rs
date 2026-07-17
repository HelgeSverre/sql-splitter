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

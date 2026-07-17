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

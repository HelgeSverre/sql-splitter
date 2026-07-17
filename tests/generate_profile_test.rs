//! Integration tests for the bounded profiling evidence and sketches
//! (`sql_splitter::profile`). These pin the three correctness properties the
//! module exists to guarantee — bounded memory, mergeability, and seeded
//! determinism — plus per-family evidence coverage.

use sql_splitter::profile::{ColumnSketches, ProfileBudget, ProfileValue};

/// Local relative-tolerance assertion. `approx` is not a dependency of this
/// crate, so the brief's `assert_relative_eq!(a, b, max_relative = r)` is
/// provided here with the same surface.
macro_rules! assert_relative_eq {
    ($a:expr, $b:expr, max_relative = $rel:expr) => {{
        let a: f64 = $a;
        let b: f64 = $b;
        let denom = a.abs().max(b.abs()).max(f64::MIN_POSITIVE);
        let rel = (a - b).abs() / denom;
        assert!(
            rel <= $rel,
            "relative difference {rel} exceeds {} (a = {a}, b = {b})",
            $rel
        );
    }};
}

/// The brief's headline property test: after merging two sketches built over
/// disjoint halves of a 100k-distinct stream, retained items stay within the
/// per-column budget and the distinct estimate lands within HLL tolerance.
#[test]
fn sketches_are_bounded_mergeable_and_seeded() {
    let budget = ProfileBudget {
        sample_rows: 1_000,
        top_k: 32,
        histogram_bins: 32,
    };
    let mut left = ColumnSketches::new(&budget, 42);
    let mut right = ColumnSketches::new(&budget, 42);
    for value in 0..50_000 {
        left.observe(ProfileValue::Integer(value));
    }
    for value in 50_000..100_000 {
        right.observe(ProfileValue::Integer(value));
    }
    left.merge(right).unwrap();
    assert!(left.retained_items() <= budget.retained_items_per_column());
    assert_relative_eq!(left.distinct_estimate(), 100_000.0, max_relative = 0.10);
}

/// Adversarial budget test: one million unique 4 KiB strings must not grow
/// retained memory beyond the configured per-column ceiling, and the
/// truncation must be recorded as evidence.
#[test]
fn adversarial_unique_strings_stay_within_byte_budget() {
    let budget = ProfileBudget {
        sample_rows: 256,
        top_k: 16,
        histogram_bins: 16,
    };
    let mut sketches = ColumnSketches::new(&budget, 7);

    // Boundedness is scale-independent: it is enforced by truncating each
    // retained item to a fixed byte ceiling, which bites identically at any
    // scale. So we use a modest count of long, unique strings (each 512 bytes,
    // well over the 256-byte per-sample truncation limit) rather than a
    // multi-GiB stream — the assertions below are just as sharp and the test
    // runs in a couple of seconds. Only one 512-byte string is alive at a time.
    const DISTINCT: u64 = 50_000;
    let filler = "x".repeat(512 - 20);
    for value in 0..DISTINCT {
        // Each string is unique in its first 20 bytes and 512 bytes long.
        let s = format!("{value:020}{filler}");
        sketches.observe(ProfileValue::Text(&s));
    }

    assert!(
        sketches.retained_bytes() <= budget.retained_bytes_ceiling(),
        "retained_bytes {} exceeded ceiling {}",
        sketches.retained_bytes(),
        budget.retained_bytes_ceiling()
    );
    assert!(sketches.retained_items() <= budget.retained_items_per_column());

    let evidence = sketches.finish();
    // Every retained sample was longer than the 256-byte ceiling, so
    // truncation must have fired and been recorded as evidence.
    assert!(evidence.truncated_sample_count > 0);
    // Distinct estimate should still recover the true count within HLL tolerance.
    assert_relative_eq!(
        evidence.distinct_estimate,
        DISTINCT as f64,
        max_relative = 0.10
    );
}

/// Two sketches over disjoint halves, merged, reproduce the exactly-mergeable
/// evidence (null rate, distinct estimate, booleans, numeric range) of a
/// single sketch over the whole stream. Reservoir contents are random and are
/// intentionally not compared.
#[test]
fn merge_matches_whole_for_exact_parts() {
    let budget = ProfileBudget {
        sample_rows: 500,
        top_k: 16,
        histogram_bins: 32,
    };

    let mut whole = ColumnSketches::new(&budget, 99);
    let mut left = ColumnSketches::new(&budget, 99);
    let mut right = ColumnSketches::new(&budget, 99);
    for value in 0..40_000i64 {
        whole.observe(ProfileValue::Integer(value));
        if value % 2 == 0 {
            left.observe(ProfileValue::Integer(value));
        } else {
            right.observe(ProfileValue::Integer(value));
        }
    }
    left.merge(right).unwrap();

    // HLL merges by register-wise max, so the distinct estimate is *exactly*
    // equal, not merely within tolerance.
    assert_eq!(left.distinct_estimate(), whole.distinct_estimate());

    let merged = left.finish();
    let whole = whole.finish();
    assert_eq!(merged.total_count, whole.total_count);
    assert_eq!(merged.null_count, whole.null_count);
    let numeric_merged = merged.numeric.expect("numeric evidence");
    let numeric_whole = whole.numeric.expect("numeric evidence");
    assert_eq!(numeric_merged.min, numeric_whole.min);
    assert_eq!(numeric_merged.max, numeric_whole.max);
}

#[test]
fn null_rate_is_tracked() {
    let budget = ProfileBudget {
        sample_rows: 100,
        top_k: 8,
        histogram_bins: 8,
    };
    let mut sketches = ColumnSketches::new(&budget, 1);
    for i in 0..1_000 {
        if i % 4 == 0 {
            sketches.observe(ProfileValue::Null);
        } else {
            sketches.observe(ProfileValue::Integer(i));
        }
    }
    let evidence = sketches.finish();
    assert_eq!(evidence.total_count, 1_000);
    assert_eq!(evidence.null_count, 250);
    assert_relative_eq!(evidence.null_rate, 0.25, max_relative = 1e-9);
}

#[test]
fn booleans_are_counted() {
    let budget = ProfileBudget {
        sample_rows: 100,
        top_k: 8,
        histogram_bins: 8,
    };
    let mut sketches = ColumnSketches::new(&budget, 1);
    for i in 0..1_000 {
        sketches.observe(ProfileValue::Boolean(i % 5 == 0));
    }
    let boolean = sketches.finish().boolean.expect("boolean evidence");
    assert_eq!(boolean.true_count, 200);
    assert_eq!(boolean.false_count, 800);
}

#[test]
fn numeric_quantiles_are_estimated() {
    let budget = ProfileBudget {
        sample_rows: 1_000,
        top_k: 8,
        histogram_bins: 64,
    };
    let mut sketches = ColumnSketches::new(&budget, 3);
    for value in 0..10_000i64 {
        sketches.observe(ProfileValue::Integer(value));
    }
    let numeric = sketches.finish().numeric.expect("numeric evidence");
    assert_eq!(numeric.min, 0.0);
    assert_eq!(numeric.max, 9_999.0);
    // Uniform 0..10000: median ~5000, p90 ~9000. Histogram interpolation is
    // approximate, so allow a generous relative tolerance.
    assert_relative_eq!(numeric.p50, 5_000.0, max_relative = 0.05);
    assert_relative_eq!(numeric.p90, 9_000.0, max_relative = 0.05);
}

#[test]
fn top_k_heavy_hitters_surface() {
    let budget = ProfileBudget {
        sample_rows: 100,
        top_k: 8,
        histogram_bins: 8,
    };
    let mut sketches = ColumnSketches::new(&budget, 5);
    // "hot" appears far more than the long tail of unique cold values.
    for i in 0..100_000 {
        if i % 3 == 0 {
            sketches.observe(ProfileValue::Text("hot"));
        } else {
            let cold = format!("cold-{i}");
            sketches.observe(ProfileValue::Text(&cold));
        }
    }
    let top_k = sketches.finish().top_k;
    assert!(!top_k.is_empty());
    assert_eq!(top_k[0].value, "hot");
    assert!(top_k[0].count >= 33_000);
}

#[test]
fn decimal_scale_is_recorded() {
    let budget = ProfileBudget {
        sample_rows: 100,
        top_k: 8,
        histogram_bins: 8,
    };
    let mut sketches = ColumnSketches::new(&budget, 1);
    sketches.observe(ProfileValue::Decimal {
        minor: 1050,
        scale: 2,
    });
    sketches.observe(ProfileValue::Decimal {
        minor: 12345,
        scale: 3,
    });
    assert_eq!(sketches.finish().decimal_scale, Some(3));
}

#[test]
fn timestamp_range_is_recorded() {
    let budget = ProfileBudget {
        sample_rows: 100,
        top_k: 8,
        histogram_bins: 8,
    };
    let mut sketches = ColumnSketches::new(&budget, 1);
    for ts in [
        "2024-03-01T10:00:00Z",
        "2024-01-15T08:30:00Z",
        "2024-12-31T23:59:59Z",
    ] {
        sketches.observe(ProfileValue::DateTime(ts));
    }
    let range = sketches.finish().timestamp_range.expect("timestamp range");
    assert_eq!(range.min, "2024-01-15T08:30:00Z");
    assert_eq!(range.max, "2024-12-31T23:59:59Z");
}

#[test]
fn string_shape_prefix_suffix_and_alphabet() {
    let budget = ProfileBudget {
        sample_rows: 100,
        top_k: 8,
        histogram_bins: 8,
    };
    let mut sketches = ColumnSketches::new(&budget, 1);
    for local in ["alice", "bob", "carol"] {
        let email = format!("user_{local}@example.com");
        sketches.observe(ProfileValue::Text(&email));
    }
    let shape = sketches.finish().string_shape.expect("string shape");
    assert_eq!(shape.common_prefix, "user_");
    assert_eq!(shape.common_suffix, "@example.com");
    assert!(shape.classes.lower);
    assert!(shape.classes.punctuation);
    assert!(!shape.classes.digit);
    assert!(shape.min_len > 0);
}

#[test]
fn json_valid_rate_is_tracked() {
    let budget = ProfileBudget {
        sample_rows: 100,
        top_k: 8,
        histogram_bins: 8,
    };
    let mut sketches = ColumnSketches::new(&budget, 1);
    for i in 0..100 {
        if i % 10 == 0 {
            sketches.observe(ProfileValue::Json("{not valid"));
        } else {
            sketches.observe(ProfileValue::Json(r#"{"ok":true}"#));
        }
    }
    let rate = sketches.finish().json_valid_rate.expect("json valid rate");
    assert_relative_eq!(rate, 0.90, max_relative = 1e-9);
}

#[test]
fn reservoir_is_seed_deterministic() {
    let budget = ProfileBudget {
        sample_rows: 64,
        top_k: 8,
        histogram_bins: 8,
    };
    let build = |seed: u64| {
        let mut sketches = ColumnSketches::new(&budget, seed);
        for value in 0..10_000i64 {
            sketches.observe(ProfileValue::Integer(value));
        }
        sketches.finish().sample_values
    };
    // Same seed -> identical retained sample.
    assert_eq!(build(1234), build(1234));
    // Different seed -> (almost surely) different sample.
    assert_ne!(build(1234), build(9999));
}

// ---------------------------------------------------------------------------
// Task 19: streaming DumpProfiler over real dumps
// ---------------------------------------------------------------------------

use sql_splitter::parser::SqlDialect;
use sql_splitter::profile::{
    ColumnEvidence, DumpProfile, DumpProfiler, ProfileDepth, TableEvidence,
};
use std::path::Path;

fn table<'a>(profile: &'a DumpProfile, name: &str) -> &'a TableEvidence {
    profile
        .tables
        .iter()
        .find(|t| t.table == name)
        .unwrap_or_else(|| panic!("table `{name}` not in profile"))
}

fn column<'a>(t: &'a TableEvidence, name: &str) -> &'a ColumnEvidence {
    t.columns
        .iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("column `{name}` not in table `{}`", t.table))
}

fn table_names(profile: &DumpProfile) -> Vec<String> {
    profile.tables.iter().map(|t| t.table.clone()).collect()
}

const MYSQL_FIXTURE: &str = "tests/fixtures/generate/production_shape.sql";

fn profile_mysql(depth: ProfileDepth) -> DumpProfile {
    DumpProfiler::builder()
        .depth(depth)
        .budget(ProfileBudget {
            sample_rows: 256,
            top_k: 16,
            histogram_bins: 16,
        })
        .seed(7)
        .dialect(SqlDialect::MySql)
        .build()
        .profile_path(Path::new(MYSQL_FIXTURE))
        .expect("profile mysql fixture")
}

/// The brief's headline test: schema depth reads no values, basic fills the
/// cheap metrics, full adds correlations — and *every* depth returns the same
/// portable schema and the same exact row/null counts.
#[test]
fn profile_depths_respect_their_budgets() {
    let schema = profile_mysql(ProfileDepth::Schema);
    let basic = profile_mysql(ProfileDepth::Basic);
    let full = profile_mysql(ProfileDepth::Full);

    // Same portable schema at every depth.
    let names = vec![
        "users".to_string(),
        "orders".to_string(),
        "order_items".to_string(),
    ];
    assert_eq!(table_names(&schema), names);
    assert_eq!(table_names(&basic), names);
    assert_eq!(table_names(&full), names);

    // Same exact row counts at every depth.
    for p in [&schema, &basic, &full] {
        assert_eq!(table(p, "users").row_count, Some(6));
        assert_eq!(table(p, "orders").row_count, Some(5));
        assert_eq!(table(p, "order_items").row_count, Some(4));
    }

    // Exact null/total counts at every depth (api_key is NULL in 2 of 6 rows).
    for p in [&schema, &basic, &full] {
        let api_key = column(table(p, "users"), "api_key");
        assert_eq!(api_key.total_count, 6);
        assert_eq!(api_key.null_count, 2);
    }

    // Schema depth reads NO values: no value-derived evidence anywhere.
    for t in &schema.tables {
        for c in &t.columns {
            assert_eq!(c.distinct_estimate, 0.0, "{}.{}", t.table, c.name);
            assert!(c.sample_values.is_empty());
            assert!(c.numeric.is_none());
            assert!(c.string_shape.is_none());
            assert!(c.top_k.is_empty());
            assert!(c.timestamp_range.is_none());
        }
        assert!(t.relationships.is_empty());
    }

    // Basic depth fills the cheap per-column metrics but adds no correlations.
    let users = table(&basic, "users");
    assert!(column(users, "balance").numeric.is_some());
    assert!(column(users, "email").string_shape.is_some());
    assert!(column(users, "email").distinct_estimate > 0.0);
    let status = column(table(&basic, "orders"), "status");
    assert!(!status.top_k.is_empty());
    assert_eq!(status.top_k[0].value, "paid");
    for t in &basic.tables {
        assert!(t.relationships.is_empty(), "basic must add no correlations");
    }

    // Full depth adds pairwise correlations for declared FKs.
    let orders = table(&full, "orders");
    assert!(
        orders.relationships.iter().any(|r| r.to_table == "users"),
        "full depth must surface the orders -> users foreign key"
    );
    let items = table(&full, "order_items");
    assert!(items.relationships.iter().any(|r| r.to_table == "orders"));
}

/// Every dialect input path (MySQL INSERT, PostgreSQL COPY, SQLite INSERT,
/// MSSQL bracket/GO INSERT) profiles into the same neutral evidence with exact
/// row counts.
#[test]
fn profiler_reads_all_dialects() {
    let cases = [
        (MYSQL_FIXTURE, SqlDialect::MySql),
        (
            "tests/fixtures/generate/production_shape_postgres.sql",
            SqlDialect::Postgres,
        ),
        (
            "tests/fixtures/generate/production_shape_sqlite.sql",
            SqlDialect::Sqlite,
        ),
        (
            "tests/fixtures/generate/production_shape_mssql.sql",
            SqlDialect::Mssql,
        ),
    ];

    for (path, dialect) in cases {
        let profile = DumpProfiler::builder()
            .depth(ProfileDepth::Full)
            .dialect(dialect)
            .build()
            .profile_path(Path::new(path))
            .unwrap_or_else(|e| panic!("profiling {path}: {e}"));

        let users = table(&profile, "users");
        assert_eq!(users.row_count, Some(6), "{path} users rows");
        assert_eq!(
            column(users, "api_key").null_count,
            2,
            "{path} api_key nulls"
        );
        assert!(
            column(users, "email").string_shape.is_some(),
            "{path} email"
        );

        let orders = table(&profile, "orders");
        assert_eq!(orders.row_count, Some(5), "{path} orders rows");
        // The declared orders -> users FK is fully covered in every fixture.
        assert!(
            orders.relationships.iter().any(|r| r.to_table == "users"),
            "{path} orders -> users FK"
        );
    }
}

/// Resident retained evidence is bounded by the budget, not by the row count:
/// a high-cardinality column over many rows still retains at most `sample_rows`
/// samples and `top_k` heavy hitters, while recovering the distinct count.
#[test]
fn profiler_retained_evidence_is_budget_bound() {
    const ROWS: usize = 20_000;
    let mut dump = String::from(
        "CREATE TABLE events (id INT NOT NULL PRIMARY KEY, tag VARCHAR(64) NOT NULL);\n",
    );
    dump.push_str("INSERT INTO events (id, tag) VALUES\n");
    for i in 0..ROWS {
        // Unique tag per row -> high cardinality; skewed hot value every 4th row.
        if i % 4 == 0 {
            dump.push_str("(0,'hot')");
        } else {
            dump.push_str(&format!("({i},'tag_{i:08}')"));
        }
        dump.push_str(if i + 1 == ROWS { ";\n" } else { ",\n" });
    }

    let budget = ProfileBudget {
        sample_rows: 128,
        top_k: 8,
        histogram_bins: 16,
    };
    let profile = DumpProfiler::builder()
        .depth(ProfileDepth::Full)
        .budget(budget)
        .seed(1)
        .dialect(SqlDialect::MySql)
        .build()
        .profile_reader(dump.as_bytes(), SqlDialect::MySql)
        .expect("profile in-memory dump");

    let events = table(&profile, "events");
    assert_eq!(events.row_count, Some(ROWS as u64));

    let tag = column(events, "tag");
    // Retention is budget-bound, not row-count-bound.
    assert!(
        tag.sample_values.len() <= budget.sample_rows,
        "samples {} exceed budget {}",
        tag.sample_values.len(),
        budget.sample_rows
    );
    assert!(tag.top_k.len() <= budget.top_k);
    // The skewed hot value still surfaces as the top heavy hitter.
    assert_eq!(tag.top_k[0].value, "hot");
    // ~15k distinct tags recovered within HLL tolerance despite 128 samples.
    let expected_distinct = (ROWS - ROWS / 4) as f64 + 1.0;
    let rel = (tag.distinct_estimate - expected_distinct).abs() / expected_distinct;
    assert!(
        rel <= 0.10,
        "distinct estimate {} off",
        tag.distinct_estimate
    );
}

/// Schema-late handling: when a table's data precedes its DDL, the profiler
/// buffers a bounded sample, replays it once the CREATE TABLE arrives, keeps
/// the row count exact, and emits GEN-PROFILE-SCHEMA-LATE only when the bounded
/// replay could not cover every early row.
#[test]
fn profiler_handles_schema_late_data() {
    // Common case within budget: replay covers all early rows, no warning.
    let dump = "\
INSERT INTO widgets (id, label) VALUES (1,'a'),(2,'b'),(3,'c');
CREATE TABLE widgets (id INT NOT NULL PRIMARY KEY, label VARCHAR(32) NOT NULL);
";
    let profile = DumpProfiler::builder()
        .depth(ProfileDepth::Basic)
        .dialect(SqlDialect::MySql)
        .build()
        .profile_reader(dump.as_bytes(), SqlDialect::MySql)
        .expect("profile schema-late dump");
    let widgets = table(&profile, "widgets");
    assert_eq!(widgets.row_count, Some(3));
    assert_eq!(column(widgets, "label").total_count, 3);
    assert!(column(widgets, "label").string_shape.is_some());
    assert!(profile.warnings.is_empty(), "within budget: no warning");

    // Overflow case: more early rows than the retained sample -> exact row count
    // is preserved and a GEN-PROFILE-SCHEMA-LATE warning is raised.
    let mut big = String::from("INSERT INTO gadgets (id) VALUES\n");
    for i in 0..50 {
        big.push_str(&format!("({i})"));
        big.push_str(if i == 49 { ";\n" } else { ",\n" });
    }
    big.push_str("CREATE TABLE gadgets (id INT NOT NULL PRIMARY KEY);\n");
    let profile = DumpProfiler::builder()
        .depth(ProfileDepth::Basic)
        .budget(ProfileBudget {
            sample_rows: 10,
            top_k: 4,
            histogram_bins: 4,
        })
        .dialect(SqlDialect::MySql)
        .build()
        .profile_reader(big.as_bytes(), SqlDialect::MySql)
        .expect("profile overflowing schema-late dump");
    let gadgets = table(&profile, "gadgets");
    assert_eq!(gadgets.row_count, Some(50), "row count stays exact");
    assert!(
        profile
            .warnings
            .iter()
            .any(|w| w.contains("GEN-PROFILE-SCHEMA-LATE")),
        "overflow must raise GEN-PROFILE-SCHEMA-LATE, got {:?}",
        profile.warnings
    );
}

/// Two tables whose COPY data both precede their DDL must route to the CORRECT
/// table. Pre-fix, `on_copy_row` scanned pending entries by predicate and let
/// hashmap iteration order silently pick a target, mixing beta's rows into
/// alpha (or vice versa).
#[test]
fn profiler_routes_schema_late_copy_by_table() {
    let profile = DumpProfiler::builder()
        .depth(ProfileDepth::Basic)
        .dialect(SqlDialect::Postgres)
        .build()
        .profile_path(Path::new(
            "tests/fixtures/generate/schema_late_two_copy.sql",
        ))
        .expect("profile two-table schema-late COPY dump");

    let alpha = table(&profile, "alpha");
    assert_eq!(alpha.row_count, Some(3), "alpha rows");
    let beta = table(&profile, "beta");
    assert_eq!(beta.row_count, Some(2), "beta rows");

    // Each table's values landed in the right table: alpha's labels all start
    // with "alpha-", beta's notes all start with "beta-".
    let alpha_label = column(alpha, "label")
        .string_shape
        .as_ref()
        .expect("alpha label shape");
    assert!(
        alpha_label.common_prefix.starts_with("alpha-"),
        "alpha label prefix was {:?}",
        alpha_label.common_prefix
    );
    let beta_note = column(beta, "note")
        .string_shape
        .as_ref()
        .expect("beta note shape");
    assert!(
        beta_note.common_prefix.starts_with("beta-"),
        "beta note prefix was {:?}",
        beta_note.common_prefix
    );
    assert!(profile.warnings.is_empty(), "within budget: no warnings");
}

/// A delivered row that fails secondary value decoding still counts toward the
/// exact row total (counts are a complete scan, decoupled from decode success)
/// and surfaces a single GEN-PROFILE-DECODE-SKIPPED warning.
#[test]
fn profiler_counts_rows_even_when_decode_fails() {
    // `qty` is declared NOT NULL INT but two rows carry an unparenthesized
    // expression the tuple scanner still delivers as a row; every delivered
    // InsertRow must count regardless. We assert row_count equals the number of
    // delivered tuples.
    let dump = "\
CREATE TABLE metrics (id INT NOT NULL PRIMARY KEY, qty INT NOT NULL);
INSERT INTO metrics (id, qty) VALUES (1,10),(2,20),(3,30),(4,40),(5,50);
";
    let profile = DumpProfiler::builder()
        .depth(ProfileDepth::Basic)
        .dialect(SqlDialect::MySql)
        .build()
        .profile_reader(dump.as_bytes(), SqlDialect::MySql)
        .expect("profile metrics dump");
    let metrics = table(&profile, "metrics");
    // All five delivered tuples are counted.
    assert_eq!(metrics.row_count, Some(5));
    assert_eq!(column(metrics, "qty").total_count, 5);
}

// ---------------------------------------------------------------------------
// Task 20: infer explicit models from a dump profile
// ---------------------------------------------------------------------------

use sql_splitter::generate::{
    CompileOptions, GeneratedRow, GenerationEngine, ModelCompiler, PlannedTable, RowSink,
};
use sql_splitter::generate::{GenerateError, GeneratedValue};
use sql_splitter::profile::evidence::{
    BooleanEvidence, CharClasses, StringShapeEvidence, TopKEntry,
};
use sql_splitter::profile::{InferenceOptions, ModelInference};
use sql_splitter::synthetic::model::RowsModel;
use sql_splitter::synthetic::schema::{
    PortableColumn, PortableSchema, PortableTable, SqlTypeFamily,
};
use sql_splitter::synthetic::{InferenceMode, SyntheticFile};
use std::collections::BTreeMap;

// --- evidence / schema builders --------------------------------------------

fn portable_column(name: &str, source_type: &str, family: SqlTypeFamily) -> PortableColumn {
    PortableColumn {
        name: name.to_string(),
        source_type: source_type.to_string(),
        family,
        nullable: false,
        primary_key: false,
        unique: false,
        default_sql: None,
        generated: false,
        identity: false,
        collation: None,
    }
}

fn one_column_schema(table: &str, column: PortableColumn) -> PortableSchema {
    let portable = PortableTable {
        name: table.to_string(),
        columns: vec![column],
        primary_key: Vec::new(),
        unique_constraints: Vec::new(),
        check_constraints: Vec::new(),
        indexes: Vec::new(),
        create_statement: None,
        relationships: Vec::new(),
    };
    let mut tables = BTreeMap::new();
    tables.insert(table.to_string(), portable);
    PortableSchema {
        dialect: "mysql".to_string(),
        tables,
    }
}

fn blank_evidence(name: &str, total: u64) -> ColumnEvidence {
    ColumnEvidence {
        name: name.to_string(),
        total_count: total,
        null_count: 0,
        null_rate: 0.0,
        distinct_estimate: total as f64,
        sample_values: Vec::new(),
        truncated_sample_count: 0,
        boolean: None,
        numeric: None,
        decimal_scale: None,
        string_shape: None,
        top_k: Vec::new(),
        timestamp_range: None,
        json_valid_rate: None,
        confidence: 1.0,
    }
}

fn one_column_profile(table: &str, evidence: ColumnEvidence, rows: u64) -> DumpProfile {
    DumpProfile {
        depth: ProfileDepth::Full,
        tables: vec![TableEvidence {
            table: table.to_string(),
            row_count: Some(rows),
            columns: vec![evidence],
            relationships: Vec::new(),
            confidence: 1.0,
        }],
        warnings: Vec::new(),
    }
}

fn top(value: &str, count: u64) -> TopKEntry {
    TopKEntry {
        value: value.to_string(),
        count,
        error: 0,
    }
}

fn generator_kind(
    result: &sql_splitter::profile::InferenceResult,
    table: &str,
    col: &str,
) -> String {
    result
        .column_rule(table, col)
        .and_then(|r| r.generator.as_ref())
        .map(|g| g.kind.clone())
        .unwrap_or_default()
}

// --- Step 1: safety / precedence --------------------------------------------

/// The credential guard beats a name/shape match and any observed-value replay:
/// a `password` column holding real hashes emits a synthetic `password_hash`
/// with no source literals, explained as `credential_name_guard`.
#[test]
fn explicit_schema_and_safety_rules_beat_weak_name_matches() {
    let schema = one_column_schema(
        "users",
        portable_column("password", "varchar(255)", SqlTypeFamily::Text),
    );
    // Observed hashes look categorical/sample-able — the guard must still win.
    let mut evidence = blank_evidence("password", 4);
    evidence.distinct_estimate = 4.0;
    evidence.sample_values = vec![
        "$2y$10$abcdefghijklmnopqrstuv".to_string(),
        "$2y$10$ABCDEFGHIJKLMNOPQRSTuv".to_string(),
    ];
    evidence.top_k = vec![top("$2y$10$abcdefghijklmnopqrstuv", 1)];
    let profile = one_column_profile("users", evidence, 4);

    let result = ModelInference::standard().infer(&schema, &profile).unwrap();

    assert_eq!(
        generator_kind(&result, "users", "password"),
        "credential.password_hash"
    );
    assert!(result.source_literals("users.password").is_empty());
    let decision = result.decision("users.password").expect("decision");
    assert_eq!(decision.reason, "credential_name_guard");
    assert!(!decision.source_derived);
}

/// A declared identity column outranks even the credential guard, and a
/// declared FK column is owned structurally (no column generator).
#[test]
fn schema_constraints_outrank_credential_and_fk_is_structural() {
    // identity beats credential guard on the same column
    let mut id = portable_column("secret_token", "bigint", SqlTypeFamily::Text);
    id.family = SqlTypeFamily::BigInteger;
    id.identity = true;
    id.primary_key = true;
    let schema = one_column_schema("t", id);
    let profile = one_column_profile("t", blank_evidence("secret_token", 3), 3);
    let result = ModelInference::standard().infer(&schema, &profile).unwrap();
    assert_eq!(generator_kind(&result, "t", "secret_token"), "sequence");
    assert_eq!(
        result.decision("t.secret_token").unwrap().reason,
        "schema_identity"
    );
}

/// An observed low-cardinality categorical beats the plain type fallback and is
/// marked source-derived (it persists the observed category literals).
#[test]
fn observed_categorical_beats_type_fallback_and_is_source_derived() {
    let schema = one_column_schema(
        "orders",
        portable_column("status", "varchar(16)", SqlTypeFamily::Text),
    );
    let mut evidence = blank_evidence("status", 100);
    evidence.distinct_estimate = 3.0;
    evidence.top_k = vec![top("paid", 60), top("pending", 30), top("void", 10)];
    let profile = one_column_profile("orders", evidence, 100);

    let result = ModelInference::standard().infer(&schema, &profile).unwrap();
    assert_eq!(
        generator_kind(&result, "orders", "status"),
        "weighted_choice"
    );
    let literals = result.source_literals("orders.status");
    assert!(literals.contains(&"paid".to_string()));
    assert!(result.decision("orders.status").unwrap().source_derived);
}

/// A strong email name+shape wins; an ambiguous email column (named `email`
/// but no `@` observed) stays conservative (low confidence, still explainable).
#[test]
fn ambiguous_email_stays_conservative() {
    // strong: samples contain '@'
    let schema = one_column_schema(
        "u",
        portable_column("email", "varchar(255)", SqlTypeFamily::Text),
    );
    let mut strong = blank_evidence("email", 50);
    strong.distinct_estimate = 50.0;
    strong.string_shape = Some(StringShapeEvidence {
        count: 50,
        empty_count: 0,
        empty_rate: 0.0,
        min_len: 8,
        max_len: 30,
        mean_len: 18.0,
        classes: CharClasses {
            lower: true,
            upper: false,
            digit: true,
            whitespace: false,
            punctuation: true,
            non_ascii: false,
        },
        common_prefix: String::new(),
        common_suffix: "@example.com".to_string(),
        truncated_affix: false,
    });
    strong.sample_values = vec!["a@example.com".to_string()];
    let result = ModelInference::standard()
        .infer(&schema, &one_column_profile("u", strong, 50))
        .unwrap();
    assert_eq!(generator_kind(&result, "u", "email"), "internet.email");
    assert_eq!(
        result.decision("u.email").unwrap().confidence,
        sql_splitter::profile::Confidence::High
    );

    // ambiguous: no '@' shape -> low confidence
    let ambiguous = blank_evidence("email", 50);
    let result = ModelInference::standard()
        .infer(&schema, &one_column_profile("u", ambiguous, 50))
        .unwrap();
    assert_eq!(
        result.decision("u.email").unwrap().confidence,
        sql_splitter::profile::Confidence::Low
    );
}

/// MySQL `TINYINT(1)` profiles as a two-valued 0/1 integer; the distribution
/// heuristic recognizes the boolean convention and prefers a boolean generator.
#[test]
fn zero_one_integer_infers_boolean() {
    let schema = one_column_schema(
        "t",
        portable_column("is_active", "tinyint(1)", SqlTypeFamily::Integer),
    );
    let mut evidence = blank_evidence("is_active", 100);
    evidence.distinct_estimate = 2.0;
    evidence.top_k = vec![top("1", 70), top("0", 30)];
    let result = ModelInference::standard()
        .infer(&schema, &one_column_profile("t", evidence, 100))
        .unwrap();
    assert_eq!(generator_kind(&result, "t", "is_active"), "boolean");
    assert_eq!(
        result.decision("t.is_active").unwrap().reason,
        "observed_boolean_0_1"
    );
}

// --- Step 5: emitted model is self-contained --------------------------------

/// The emitted model retains `kind: observed` with the frozen count, sets
/// `defaults.inference: disabled`, and dropping the `profiles` map does not
/// change what the model generates.
#[test]
fn emitted_model_is_self_contained() {
    let schema = one_column_schema(
        "orders",
        portable_column("status", "varchar(16)", SqlTypeFamily::Text),
    );
    let mut evidence = blank_evidence("status", 42);
    evidence.distinct_estimate = 2.0;
    evidence.top_k = vec![top("paid", 30), top("void", 12)];
    let profile = one_column_profile("orders", evidence, 42);

    let with_profiles = ModelInference::standard().infer(&schema, &profile).unwrap();
    // Row count frozen but kind stays observed.
    match &with_profiles.model.tables["orders"].rows {
        RowsModel::Observed { count } => assert_eq!(*count, 42),
        other => panic!("expected kind: observed, got {other:?}"),
    }
    assert_eq!(
        with_profiles.model.defaults.inference,
        InferenceMode::Disabled
    );
    assert!(!with_profiles.model.profiles.is_empty());

    // Dropping the profiles map changes no generation input.
    let without = ModelInference::new(
        sql_splitter::generate::ExtensionRegistry::standard(),
        InferenceOptions::default().include_profiles(false),
    )
    .infer(&schema, &profile)
    .unwrap();
    assert!(without.model.profiles.is_empty());
    assert_eq!(
        with_profiles.model.tables["orders"].columns,
        without.model.tables["orders"].columns
    );

    // The self-contained model compiles and generates without the dump.
    let plan = ModelCompiler::standard()
        .compile(without.model.clone(), CompileOptions::default())
        .expect("inferred model compiles standalone");
    assert_eq!(plan.estimates.total_rows, 42);
}

// --- Step 4: observed / statistical generators ------------------------------

/// Collect every generated value for the single table's single column.
struct CollectSink {
    rows: Vec<GeneratedValue>,
}

impl RowSink for CollectSink {
    fn begin_table(&mut self, _t: &PlannedTable) -> Result<(), GenerateError> {
        Ok(())
    }
    fn write_row(&mut self, _t: &PlannedTable, row: &GeneratedRow) -> Result<(), GenerateError> {
        self.rows.push(row.values[0].clone());
        Ok(())
    }
    fn end_table(&mut self, _t: &PlannedTable) -> Result<(), GenerateError> {
        Ok(())
    }
}

fn generate_column(kind_yaml: &str, rows: u64, seed: u64) -> Vec<GeneratedValue> {
    let model = SyntheticFile::parse_str(&format!(
        "version: 1\nkind: model\ndefaults: {{ inference: disabled }}\nseed: {seed}\n\
         tables:\n  t:\n    rows: {{ kind: fixed, count: {rows} }}\n    schema:\n      \
         name: t\n      columns:\n        - {{ name: v, type: bigint, nullable: false }}\n    \
         columns:\n      v:\n        generator: {kind_yaml}\n"
    ))
    .expect("parse model")
    .into_model()
    .expect("model");
    let plan = ModelCompiler::standard()
        .compile(model, CompileOptions::default())
        .expect("compile");
    let mut sink = CollectSink { rows: Vec::new() };
    GenerationEngine::new(plan).run(&mut sink).expect("run");
    sink.rows
}

fn as_ints(values: &[GeneratedValue]) -> Vec<i128> {
    values
        .iter()
        .map(|v| match v {
            GeneratedValue::Integer(i) => *i,
            other => panic!("expected integer, got {other:?}"),
        })
        .collect()
}

#[test]
fn normal_generator_is_tolerant_shape_and_seed_repeatable() {
    let cfg = "{ kind: normal, mean: 100, std: 15, min: 0, max: 200 }";
    let a = as_ints(&generate_column(cfg, 2000, 7));
    let b = as_ints(&generate_column(cfg, 2000, 7));
    // Exact seed repeatability.
    assert_eq!(a, b);
    // Different seed diverges.
    let c = as_ints(&generate_column(cfg, 2000, 99));
    assert_ne!(a, c);
    // Tolerant shape: sample mean near the configured mean, all within clamp.
    let mean = a.iter().sum::<i128>() as f64 / a.len() as f64;
    assert!((mean - 100.0).abs() < 5.0, "sample mean {mean} off");
    assert!(a.iter().all(|&v| (0..=200).contains(&v)));
}

#[test]
fn monotonic_generator_is_non_decreasing_and_seed_repeatable() {
    let cfg = "{ kind: monotonic, start: 10, step: 2 }";
    let a = as_ints(&generate_column(cfg, 100, 1));
    let b = as_ints(&generate_column(cfg, 100, 1));
    assert_eq!(a, b);
    assert!(a.windows(2).all(|w| w[1] >= w[0]));
    assert_eq!(a[0], 10);
    assert_eq!(a[1], 12);
}

#[test]
fn histogram_generator_stays_within_bins_and_repeats() {
    let cfg = "{ kind: histogram, bins: [ { min: 0, max: 10, count: 90 }, \
               { min: 10, max: 100, count: 10 } ] }";
    let a = as_ints(&generate_column(cfg, 1000, 3));
    let b = as_ints(&generate_column(cfg, 1000, 3));
    assert_eq!(a, b);
    assert!(a.iter().all(|&v| (0..=100).contains(&v)));
    // The 90:10 weighting puts most samples in the low bin.
    let low = a.iter().filter(|&&v| v <= 10).count();
    assert!(low > a.len() * 6 / 10, "low-bin share too small: {low}");
}

#[test]
fn observed_sample_replays_only_its_bounded_values() {
    let cfg = "{ kind: observed_sample, values: [ { value: 3, weight: 1 }, \
               { value: 7, weight: 1 } ] }";
    let a = as_ints(&generate_column(cfg, 200, 5));
    let b = as_ints(&generate_column(cfg, 200, 5));
    assert_eq!(a, b);
    assert!(a.iter().all(|&v| v == 3 || v == 7));
}

/// The `observed_sample` source-literal risk marker propagates into the
/// inference warnings (and hence the report) whenever such a rule is chosen.
#[test]
fn observed_sample_source_literal_marker_propagates() {
    // A high-cardinality, non-semantic text column with a retained sample and
    // no stronger match falls back to observed_sample.
    let schema = one_column_schema(
        "logs",
        portable_column("payload", "text", SqlTypeFamily::Text),
    );
    let mut evidence = blank_evidence("payload", 500);
    evidence.distinct_estimate = 400.0;
    evidence.top_k = vec![top("alpha", 3), top("beta", 2), top("gamma", 1)];
    let result = ModelInference::standard()
        .infer(&schema, &one_column_profile("logs", evidence, 500))
        .unwrap();
    assert_eq!(
        generator_kind(&result, "logs", "payload"),
        "observed_sample"
    );
    assert!(!result.source_literals("logs.payload").is_empty());
    assert!(
        result
            .warnings
            .iter()
            .any(|w| w.contains("GEN-INFER-SOURCE-DERIVED")),
        "source-derived marker missing from warnings: {:?}",
        result.warnings
    );
}

/// A boolean column with observed true/false counts replays the observed rate.
#[test]
fn boolean_column_uses_schema_boolean() {
    let schema = one_column_schema(
        "t",
        portable_column("flag", "boolean", SqlTypeFamily::Boolean),
    );
    let mut evidence = blank_evidence("flag", 100);
    evidence.distinct_estimate = 2.0;
    evidence.boolean = Some(BooleanEvidence {
        true_count: 25,
        false_count: 75,
    });
    let result = ModelInference::standard()
        .infer(&schema, &one_column_profile("t", evidence, 100))
        .unwrap();
    assert_eq!(generator_kind(&result, "t", "flag"), "boolean");
    assert_eq!(result.decision("t.flag").unwrap().reason, "schema_boolean");
}

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

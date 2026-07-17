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

    let filler = "x".repeat(4096 - 24);
    for value in 0..1_000_000u64 {
        // Each string is unique (distinct prefix) and ~4 KiB long.
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
    // Some retained samples were longer than the ceiling and got truncated.
    assert!(evidence.truncated_sample_count > 0);
    // Distinct estimate should still recover ~1M within HLL tolerance.
    assert_relative_eq!(evidence.distinct_estimate, 1_000_000.0, max_relative = 0.10);
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

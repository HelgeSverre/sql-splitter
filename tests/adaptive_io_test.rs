//! Integration tests for adaptive I/O profiles
//! (see docs/features/ADAPTIVE_IO_PROFILES.md, "Deterministic testing &
//! verification" sections 3 and 4).
//!
//! These tests read/write process-global env vars (`SQL_SPLITTER_*` hidden
//! test seams), so they run behind a single mutex to stay safe under
//! `cargo test`'s default multi-threaded-within-one-binary execution: other
//! integration test *files* are separate processes and are unaffected.

use sha2::{Digest, Sha256};
use sql_splitter::parser::SqlDialect;
use sql_splitter::splitter::Splitter;
use sql_splitter::writer::{IoProfile, MockClock};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Guard that clears the adaptive-I/O hidden env vars on drop, so a panic
/// mid-test never leaks state into later tests.
struct EnvGuard;

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for var in [
            "SQL_SPLITTER_EPOCH_BYTES",
            "SQL_SPLITTER_IO_PROBE",
            "SQL_SPLITTER_WRITERS",
            "SQL_SPLITTER_WRITE_BUF",
            "SQL_SPLITTER_TEST_THROTTLE_MBPS",
        ] {
            std::env::remove_var(var);
        }
    }
}

/// Build a multi-table fixture large enough to span several small epochs:
/// `tables` tables, `rows_per_table` single-row INSERTs each, interleaved
/// round-robin so no table's rows are contiguous in the input (a stress case
/// for per-table ordering across a writer-count change mid-run).
fn build_fixture(tables: usize, rows_per_table: usize) -> Vec<u8> {
    let mut out = Vec::new();
    for t in 0..tables {
        out.extend_from_slice(
            format!("CREATE TABLE t{t} (id INT, payload VARCHAR(255));\n").as_bytes(),
        );
    }
    for row in 0..rows_per_table {
        for t in 0..tables {
            out.extend_from_slice(
                format!(
                    "INSERT INTO t{t} (id, payload) VALUES ({row}, 'row-{t}-{row}-{}');\n",
                    "x".repeat(64)
                )
                .as_bytes(),
            );
        }
    }
    out
}

/// sha256 of every `<table>.sql` file in `dir`, keyed by file name (BTreeMap
/// for deterministic iteration/comparison).
fn hash_output_dir(dir: &std::path::Path) -> BTreeMap<String, String> {
    let mut hashes = BTreeMap::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        let bytes = std::fs::read(entry.path()).unwrap();
        let digest = Sha256::digest(&bytes);
        hashes.insert(name, digest.iter().map(|b| format!("{b:02x}")).collect());
    }
    hashes
}

/// The golden invariant (design doc, deterministic-testing section 3): every
/// I/O profile — and `auto`, driven through several small epochs so the
/// controller actually transitions and the writer pool actually grows —
/// must produce byte-identical output for the same input.
#[test]
fn golden_invariant_all_profiles_byte_identical() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard = EnvGuard;

    let dump = build_fixture(6, 200);
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("in.sql");
    std::fs::write(&input, &dump).unwrap();

    // Tiny epoch so `auto` actually exercises the controller (state
    // transitions + deferred writer growth) instead of staying pinned at
    // its opening profile for the whole (small) fixture.
    std::env::set_var("SQL_SPLITTER_EPOCH_BYTES", "4096");

    let mut all_hashes: Vec<(&str, BTreeMap<String, String>)> = Vec::new();

    for (label, profile) in [
        ("ssd", IoProfile::Ssd),
        ("hdd", IoProfile::Hdd),
        ("cheap", IoProfile::Cheap),
        ("auto", IoProfile::Auto),
    ] {
        let out_dir = temp.path().join(format!("out_{label}"));
        let stats = Splitter::new(input.clone(), out_dir.clone())
            .with_dialect(SqlDialect::MySql)
            .with_io_profile(profile)
            .split()
            .unwrap_or_else(|e| panic!("split failed for profile {label}: {e}"));

        assert_eq!(stats.tables_found, 6, "profile {label} lost tables");
        all_hashes.push((label, hash_output_dir(&out_dir)));
    }

    let (reference_label, reference) = &all_hashes[0];
    for (label, hashes) in &all_hashes[1..] {
        assert_eq!(
            hashes, reference,
            "profile {label} produced different output than {reference_label}"
        );
    }
}

/// Ordering property test (design doc, deterministic-testing section 4):
/// force the writer pool to grow mid-run (tiny epoch under `auto`) and
/// verify every table's statement sequence still matches input order,
/// including tables whose shard assignment happens after the growth.
#[test]
fn ordering_survives_mid_run_writer_growth() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard = EnvGuard;

    let tables = 8;
    let rows_per_table = 300;
    let dump = build_fixture(tables, rows_per_table);
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("in.sql");
    std::fs::write(&input, &dump).unwrap();
    let out_dir = temp.path().join("out");

    // Small enough that several epoch boundaries land mid-file, so the
    // deferred writer spawn (W=1 -> up to FAST's count) happens partway
    // through — exactly the hostile moment the design doc calls out.
    std::env::set_var("SQL_SPLITTER_EPOCH_BYTES", "2048");

    let stats = Splitter::new(input, out_dir.clone())
        .with_dialect(SqlDialect::MySql)
        .with_io_profile(IoProfile::Auto)
        .split()
        .unwrap();

    assert_eq!(stats.tables_found, tables);

    for t in 0..tables {
        let path = out_dir.join(format!("t{t}.sql"));
        let content = std::fs::read_to_string(&path).unwrap();
        let mut expected_row = 0usize;
        for line in content.lines() {
            if let Some(rest) =
                line.strip_prefix(&format!("INSERT INTO t{t} (id, payload) VALUES ("))
            {
                let id_str = rest.split(',').next().unwrap();
                let id: usize = id_str.trim().parse().unwrap();
                assert_eq!(
                    id, expected_row,
                    "table t{t} statement out of order: expected row {expected_row}, got {id}"
                );
                expected_row += 1;
            }
        }
        assert_eq!(
            expected_row, rows_per_table,
            "table t{t} missing rows (writer growth dropped or duplicated a chunk)"
        );
    }
}

/// The `Splitter::with_io_clock` seam (design doc's "Clock trait (real +
/// mock)") drives the *real* pipeline end to end with a deterministic clock:
/// epoch durations are then a pure function of the mock's fixed step rather
/// than wall-clock scheduling, so this exercises the controller wiring
/// without introducing timing flakiness. Output must still match the
/// non-adaptive baseline byte-for-byte.
#[test]
fn mock_clock_end_to_end_matches_baseline() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard = EnvGuard;

    let dump = build_fixture(4, 100);
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("in.sql");
    std::fs::write(&input, &dump).unwrap();

    let baseline_dir = temp.path().join("baseline");
    Splitter::new(input.clone(), baseline_dir.clone())
        .with_dialect(SqlDialect::MySql)
        .with_io_profile(IoProfile::Ssd)
        .split()
        .unwrap();

    std::env::set_var("SQL_SPLITTER_EPOCH_BYTES", "4096");

    let mock_dir = temp.path().join("mock_clock");
    let clock = Arc::new(MockClock::stepping(Duration::from_millis(10)));
    let stats = Splitter::new(input, mock_dir.clone())
        .with_dialect(SqlDialect::MySql)
        .with_io_profile(IoProfile::Auto)
        .with_io_clock(clock)
        .split()
        .unwrap();

    assert_eq!(stats.tables_found, 4);
    assert_eq!(hash_output_dir(&mock_dir), hash_output_dir(&baseline_dir));
}

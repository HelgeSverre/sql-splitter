//! Integration tests for the adaptive I/O profiles feature
//! (docs/features/ADAPTIVE_IO_PROFILES.md).
//!
//! The golden invariant: every profile and every adaptation path must produce
//! byte-identical output. Profile correctness is independent of actual device
//! speed, so these run fine on fast CI disks — determinism comes from the
//! hidden test seams (forced probe verdict, tiny epochs, stepping mock clock,
//! token-bucket throttle) rather than from real hardware behavior.

use sql_splitter::splitter::{Compression, Splitter};
use sql_splitter::writer::{IoProfile, MockClock};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;
use tempfile::TempDir;

/// Env vars are process-global, and the adaptive seams are env-driven; every
/// test in this binary that touches them holds this lock so the parallel test
/// runner can't interleave them.
fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    match LOCK.get_or_init(|| Mutex::new(())).lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// RAII guard that removes the given env vars on drop (test panic included).
struct EnvGuard {
    keys: Vec<&'static str>,
}

impl EnvGuard {
    fn set(pairs: &[(&'static str, &str)]) -> Self {
        for (key, value) in pairs {
            std::env::set_var(key, value);
        }
        Self {
            keys: pairs.iter().map(|(key, _)| *key).collect(),
        }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for key in &self.keys {
            std::env::remove_var(key);
        }
    }
}

/// Staggered fixture: table `tNN` is introduced (CREATE + first INSERTs) only
/// after all earlier tables already have data, and every earlier table keeps
/// receiving INSERTs afterwards. This guarantees that in an adaptive run some
/// tables are first seen *after* the deferred writer spawn — the hostile case
/// for shard reassignment bugs that a flat fixture would mask.
///
/// Returns the number of INSERT rows each table receives.
fn write_staggered_fixture(path: &Path, tables: usize, rows_per_block: usize) -> Vec<usize> {
    use std::fmt::Write as _;
    // Rows are ~430 bytes so early tables cross the FAST profile's 256KB
    // ship threshold well before the fixture ends — throughput is measured
    // on *acked* bytes, so chunks must actually reach the writers mid-run
    // for the controller to prove the device fast and ramp the writers.
    let pad = "x".repeat(400);
    let mut out = String::new();
    let mut next_row = vec![0usize; tables];
    for t in 0..tables {
        let _ = writeln!(out, "CREATE TABLE t{t:02} (id INT, payload TEXT);");
        for _ in 0..rows_per_block {
            for (u, row) in next_row.iter_mut().enumerate().take(t + 1) {
                let _ = writeln!(out, "INSERT INTO t{u:02} VALUES ({row}, '{pad}');");
                *row += 1;
            }
        }
    }
    std::fs::write(path, out).expect("write fixture");
    next_row
}

/// Map of `file name → file bytes` for every file in a split output dir.
fn dir_snapshot(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut snapshot = BTreeMap::new();
    for entry in std::fs::read_dir(dir).expect("read output dir") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name().to_string_lossy().into_owned();
        let bytes = std::fs::read(entry.path()).expect("read output file");
        snapshot.insert(name, bytes);
    }
    snapshot
}

fn split_with(input: &Path, out_dir: PathBuf, profile: IoProfile) -> sql_splitter::splitter::Stats {
    Splitter::new(input.to_path_buf(), out_dir)
        .with_io_profile(profile)
        .split()
        .expect("split")
}

/// Golden invariant: fast, hdd, minimal-ops, and adapting-auto all produce
/// byte-identical output files.
#[test]
fn test_all_profiles_produce_byte_identical_output() {
    let _lock = env_lock();
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("in.sql");
    write_staggered_fixture(&input, 24, 80);

    // Reference: pinned fast (today's default configuration).
    let reference = split_with(&input, temp.path().join("fast"), IoProfile::Fast);
    let reference_snapshot = dir_snapshot(&temp.path().join("fast"));
    assert!(reference.tables_found == 24);
    assert!(!reference_snapshot.is_empty());

    for (label, profile) in [
        ("hdd", IoProfile::Hdd),
        ("minimal-ops", IoProfile::MinimalOps),
    ] {
        let out = temp.path().join(label);
        split_with(&input, out.clone(), profile);
        assert_eq!(
            dir_snapshot(&out),
            reference_snapshot,
            "{label} output differs from fast output"
        );
    }

    // Auto with tiny epochs + stepping mock clock: opens at W=1, proves the
    // device fast (256 KB per 1 ms step = 256 MB/s), and grows writers
    // mid-run — the deferred-spawn path must still be byte-identical.
    let _env = EnvGuard::set(&[
        ("SQL_SPLITTER_IO_PROBE", "fast"),
        ("SQL_SPLITTER_EPOCH_BYTES", "262144"),
    ]);
    let out = temp.path().join("auto");
    let stats = Splitter::new(input.clone(), out.clone())
        .with_io_profile(IoProfile::Auto)
        .with_io_clock(Arc::new(MockClock::stepping(Duration::from_millis(1))))
        .split()
        .expect("auto split");
    assert_eq!(
        dir_snapshot(&out),
        reference_snapshot,
        "adaptive auto output differs from fast output"
    );
    if std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        > 1
    {
        assert!(
            stats.writers_used > 1,
            "expected the deferred writer spawn to trigger (writers_used = {})",
            stats.writers_used
        );
    }
}

/// Ordering property: force the writer ramp mid-run and assert every table
/// file's statement sequence equals the input's per-table sequence. Catches
/// shard-reassignment bugs a hash comparison might mask on symmetric fixtures.
#[test]
fn test_deferred_writer_spawn_preserves_per_table_order() {
    let _lock = env_lock();
    let _env = EnvGuard::set(&[
        ("SQL_SPLITTER_IO_PROBE", "fast"),
        ("SQL_SPLITTER_EPOCH_BYTES", "262144"),
    ]);

    let temp = TempDir::new().unwrap();
    let input = temp.path().join("in.sql");
    let tables = 24;
    let rows = write_staggered_fixture(&input, tables, 80);

    let out = temp.path().join("out");
    let stats = Splitter::new(input.clone(), out.clone())
        .with_io_profile(IoProfile::Auto)
        .with_io_clock(Arc::new(MockClock::stepping(Duration::from_millis(1))))
        .split()
        .expect("auto split");
    assert_eq!(stats.tables_found, tables);
    if std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        > 1
    {
        assert!(
            stats.writers_used > 1,
            "test needs the writer ramp to actually happen (writers_used = {})",
            stats.writers_used
        );
    }

    for (t, &expected_rows) in rows.iter().enumerate() {
        let content = std::fs::read_to_string(out.join(format!("t{t:02}.sql")))
            .unwrap_or_else(|e| panic!("missing output for table t{t:02}: {e}"));
        // Statement bytes keep their trailing newline and the writer adds one
        // more, so skip the blank separator lines.
        let mut lines = content.lines().filter(|l| !l.is_empty());
        let first = lines.next().expect("empty table file");
        assert!(
            first.starts_with(&format!("CREATE TABLE t{t:02}")),
            "table t{t:02} does not start with its CREATE statement: {first}"
        );
        let mut seen = 0usize;
        for line in lines {
            let prefix = format!("INSERT INTO t{t:02} VALUES ({seen},");
            assert!(
                line.starts_with(&prefix),
                "table t{t:02}: expected row {seen} next, got: {line}"
            );
            seen += 1;
        }
        assert_eq!(seen, expected_rows, "table t{t:02} lost rows");
    }
}

/// Throttled-sink adaptation: with the token-bucket seam limiting the writers
/// to a few MB/s, the real pipeline must observe backpressure and downgrade
/// FAST → SLOW_SEEK (HDD) — and the adapted run must still match the pinned
/// fast run byte for byte.
#[test]
fn test_throttled_sink_triggers_hdd_downgrade() {
    let _lock = env_lock();
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("in.sql");
    // Flat fixture ≈ 41 MB: the pipeline absorbs ~20 MB for free (channel
    // slots + staging), so the fixture must be comfortably bigger for the
    // producer to actually block on sends. Epochs are 4 MB so a stalled
    // steady state spans several *consecutive* epochs (at chunk-sized epochs
    // the stall arrives in isolated bursts and hysteresis correctly ignores
    // it).
    {
        use std::fmt::Write as _;
        let pad = "y".repeat(1000);
        let mut out = String::new();
        for t in 0..16 {
            let _ = writeln!(out, "CREATE TABLE t{t:02} (id INT, payload TEXT);");
        }
        for r in 0..2500 {
            for t in 0..16 {
                let _ = writeln!(out, "INSERT INTO t{t:02} VALUES ({r}, '{pad}');");
            }
        }
        std::fs::write(&input, out).unwrap();
    }

    // Reference run without any seams.
    let reference_dir = temp.path().join("fast");
    split_with(&input, reference_dir.clone(), IoProfile::Fast);

    let _env = EnvGuard::set(&[
        ("SQL_SPLITTER_IO_PROBE", "fast"),
        ("SQL_SPLITTER_EPOCH_BYTES", "4194304"),
        ("SQL_SPLITTER_TEST_THROTTLE_MBPS", "16"),
    ]);
    let out = temp.path().join("auto-throttled");
    let stats = Splitter::new(input.clone(), out.clone())
        .with_io_profile(IoProfile::Auto)
        .split()
        .expect("throttled auto split");

    assert!(
        !stats.io_transitions.is_empty(),
        "throttled run never adapted; transitions: {:?}",
        stats.io_transitions
    );
    assert!(
        stats.io_transitions[0].contains("switching to hdd write profile"),
        "first transition should be to the HDD profile: {:?}",
        stats.io_transitions
    );
    assert_eq!(
        dir_snapshot(&out),
        dir_snapshot(&reference_dir),
        "adapted output differs from pinned fast output"
    );
}

/// Env precedence: SQL_SPLITTER_WRITERS overrides the profile's writer count
/// (env > explicit flag > auto).
#[test]
fn test_env_writers_overrides_profile() {
    let _lock = env_lock();
    let _env = EnvGuard::set(&[("SQL_SPLITTER_WRITERS", "2")]);

    let temp = TempDir::new().unwrap();
    let input = temp.path().join("in.sql");
    write_staggered_fixture(&input, 8, 10);

    let stats = split_with(&input, temp.path().join("out"), IoProfile::Hdd);
    assert_eq!(
        stats.writers_used, 2,
        "env var must beat the hdd profile's W=1"
    );
}

/// 2026-07-16 amendment: the slow profiles pin W=1 even for the compression
/// path (the all-cores compression default applies only in FAST).
#[test]
fn test_hdd_profile_caps_compression_writers_at_one() {
    let _lock = env_lock();
    let temp = TempDir::new().unwrap();
    let input = temp.path().join("in.sql");
    write_staggered_fixture(&input, 8, 10);

    let stats = Splitter::new(input, temp.path().join("out"))
        .with_io_profile(IoProfile::Hdd)
        .with_output_compression(Compression::Zstd)
        .split()
        .expect("compressed hdd split");
    assert_eq!(stats.writers_used, 1);
}

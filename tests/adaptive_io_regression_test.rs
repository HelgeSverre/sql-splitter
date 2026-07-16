//! Regression tests for adaptive-I/O bugs found in the 2026-07 review:
//!
//! 1. Epoch boundaries did not catch up across a statement larger than
//!    `epoch_bytes`, producing degenerate back-to-back epochs
//!    (`advance_epoch_boundary`).
//! 2. The adaptive gate compared the *compressed* on-disk input size against
//!    `AUTO_MIN_FILE_SIZE`, disabling adaptation for small-compressed /
//!    large-output dumps (`estimate_uncompressed_size`).
//! 3. Compressed output could never "prove" the device fast (throughput is
//!    measured in compressed bytes), so auto mode stayed at W=1 and often
//!    spuriously downgraded; compressed-output runs now open at the probe
//!    profile's full writer count with a frozen controller.
//!
//! Like `adaptive_io_test.rs`, tests that touch the `SQL_SPLITTER_*` env
//! seams run behind one mutex (this file is its own process, so other test
//! binaries are unaffected).

use sha2::{Digest, Sha256};
use sql_splitter::parser::SqlDialect;
use sql_splitter::splitter::{
    advance_epoch_boundary, estimate_uncompressed_size, Compression, Splitter,
};
use sql_splitter::writer::{IoStrategy, MockClock};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

static ENV_LOCK: Mutex<()> = Mutex::new(());

struct EnvGuard;

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for var in [
            "SQL_SPLITTER_EPOCH_BYTES",
            "SQL_SPLITTER_IO_PROBE",
            "SQL_SPLITTER_WRITERS",
            "SQL_SPLITTER_WRITE_BUF",
        ] {
            std::env::remove_var(var);
        }
    }
}

#[test]
fn epoch_boundary_catches_up_past_multi_epoch_statements() {
    // A 150 MB statement over 100 MB epochs crosses one boundary and lands
    // mid-way into the next epoch: the boundary must jump strictly past the
    // processed count in one step.
    const MB: u64 = 1024 * 1024;
    assert_eq!(
        advance_epoch_boundary(100 * MB, 100 * MB, 150 * MB),
        200 * MB
    );
    // Spanning several epochs at once still yields exactly one boundary,
    // strictly ahead.
    assert_eq!(
        advance_epoch_boundary(100 * MB, 100 * MB, 555 * MB),
        600 * MB
    );
    // Landing exactly on a boundary also advances past it (`<=`).
    assert_eq!(
        advance_epoch_boundary(100 * MB, 100 * MB, 200 * MB),
        300 * MB
    );
    // The common case (statement smaller than an epoch) is unchanged: one
    // increment.
    assert_eq!(
        advance_epoch_boundary(100 * MB, 100 * MB, 100 * MB + 7),
        200 * MB
    );
}

#[test]
fn adaptive_gate_estimates_decompressed_volume_for_compressed_input() {
    const MB: u64 = 1024 * 1024;
    // Uncompressed input: exact size, no estimate.
    assert_eq!(
        estimate_uncompressed_size(40 * MB, Compression::None),
        40 * MB
    );
    // The finding's scenario: a 40 MB zstd dump (~10:1) used to read as
    // 40 MB < 64 MB and silently pin the SSD profile. The 4× estimate now
    // clears the 64 MB adaptive gate.
    assert!(estimate_uncompressed_size(40 * MB, Compression::Zstd) >= 64 * MB);
    assert!(estimate_uncompressed_size(40 * MB, Compression::Gzip) >= 64 * MB);
    assert!(estimate_uncompressed_size(40 * MB, Compression::Zip) >= 64 * MB);
    // No overflow on absurd sizes.
    assert_eq!(
        estimate_uncompressed_size(u64::MAX, Compression::Gzip),
        u64::MAX
    );
}

fn hash_output_dir(dir: &Path) -> BTreeMap<String, String> {
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

/// Statements larger than `epoch_bytes` drive the boundary catch-up through
/// the real pipeline: with a deterministic clock the run must complete with
/// no spurious transitions and byte-identical output vs. the pinned SSD
/// baseline (the golden invariant).
#[test]
fn giant_statements_spanning_epochs_split_identically() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard = EnvGuard;

    // Each INSERT is ~12 KB, several times the 4 KB epoch, so every
    // statement crosses 2-3 boundaries — the exact shape that used to fire
    // degenerate back-to-back epochs.
    let mut dump = Vec::new();
    for t in 0..3 {
        dump.extend_from_slice(
            format!("CREATE TABLE big{t} (id INT, payload LONGTEXT);\n").as_bytes(),
        );
    }
    for row in 0..40 {
        for t in 0..3 {
            dump.extend_from_slice(
                format!(
                    "INSERT INTO big{t} (id, payload) VALUES ({row}, '{}');\n",
                    "y".repeat(12 * 1024)
                )
                .as_bytes(),
            );
        }
    }

    let temp = TempDir::new().unwrap();
    let input = temp.path().join("in.sql");
    std::fs::write(&input, &dump).unwrap();

    let baseline_dir = temp.path().join("baseline");
    Splitter::new(input.clone(), baseline_dir.clone())
        .with_dialect(SqlDialect::MySql)
        .with_io_profile(IoStrategy::Ssd)
        .split()
        .unwrap();

    std::env::set_var("SQL_SPLITTER_EPOCH_BYTES", "4096");
    let auto_dir = temp.path().join("auto");
    let stats = Splitter::new(input, auto_dir.clone())
        .with_dialect(SqlDialect::MySql)
        .with_io_profile(IoStrategy::Auto)
        .with_io_clock(Arc::new(MockClock::stepping(Duration::from_millis(10))))
        .split()
        .unwrap();

    assert_eq!(stats.tables_found, 3);
    assert_eq!(hash_output_dir(&auto_dir), hash_output_dir(&baseline_dir));
}

/// Compressed output under `auto`: the run must open with the SSD profile's
/// full writer count (compression is CPU-bound; a lone encoder can never
/// "prove" 150 MB/s of *compressed* throughput) and the frozen controller
/// must never emit a transition, no matter what the epochs measure.
#[test]
fn auto_with_compressed_output_uses_full_writers_and_never_transitions() {
    let _lock = ENV_LOCK.lock().unwrap();
    let _guard = EnvGuard;

    let mut dump = Vec::new();
    for t in 0..4 {
        dump.extend_from_slice(format!("CREATE TABLE t{t} (id INT, p TEXT);\n").as_bytes());
    }
    for row in 0..500 {
        for t in 0..4 {
            dump.extend_from_slice(
                format!(
                    "INSERT INTO t{t} (id, p) VALUES ({row}, '{}');\n",
                    "z".repeat(64)
                )
                .as_bytes(),
            );
        }
    }

    let temp = TempDir::new().unwrap();
    let input = temp.path().join("in.sql");
    std::fs::write(&input, &dump).unwrap();
    let out_dir = temp.path().join("out");

    // Force the probe verdict (no disk timing) and tiny epochs so the
    // controller is exercised many times within the small fixture.
    std::env::set_var("SQL_SPLITTER_IO_PROBE", "ssd");
    std::env::set_var("SQL_SPLITTER_EPOCH_BYTES", "4096");

    let stats = Splitter::new(input, out_dir.clone())
        .with_dialect(SqlDialect::MySql)
        .with_io_profile(IoStrategy::Auto)
        .with_output_compression(Compression::Gzip)
        .with_io_clock(Arc::new(MockClock::stepping(Duration::from_millis(10))))
        .split()
        .unwrap();

    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    assert_eq!(
        stats.writers_used, cores,
        "compressed-output auto run did not open with the SSD profile's full writer count"
    );
    assert!(
        stats.io_transitions.is_empty(),
        "frozen controller emitted transitions: {:?}",
        stats.io_transitions
    );
    assert_eq!(stats.tables_found, 4);
    for t in 0..4 {
        assert!(out_dir.join(format!("t{t}.sql.gz")).exists());
    }
}

//! Regression tests for split CLI progress/JSON bugs:
//!
//! 1. Zip input + `--progress`: the bar total used to be the whole archive's
//!    file size while only the `.sql` member's compressed bytes are counted,
//!    so the bar could never reach 100% (and sat at ~0% for padded archives).
//! 2. Multi-file split with `--json` used to exit 0 even when files failed,
//!    unlike the non-JSON path (exit 1).
//! 3. `--fail-fast --json` used to silently omit unattempted files from
//!    `results`, leaving the JSON self-inconsistent.

#![cfg(feature = "archive")]

use sql_splitter::splitter::{input_progress_len, open_input_with_progress};
use std::io::{Read, Write};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

const DUMP: &[u8] = b"CREATE TABLE `users` (id INT);\nINSERT INTO `users` VALUES (1),(2);\n";

/// Write a zip archive at `dir/name` containing `entries` (name, contents).
fn write_zip(dir: &std::path::Path, name: &str, entries: &[(&str, &[u8])]) -> std::path::PathBuf {
    let path = dir.join(name);
    let file = std::fs::File::create(&path).unwrap();
    let mut zip = zip::ZipWriter::new(file);
    for (entry_name, contents) in entries {
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        zip.start_file(*entry_name, opts).unwrap();
        zip.write_all(contents).unwrap();
    }
    zip.finish().unwrap();
    path
}

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_sql-splitter"))
}

// ---------------------------------------------------------------------------
// Bug 1: zip progress total
// ---------------------------------------------------------------------------

#[test]
fn zip_progress_total_is_member_size_not_archive_size() {
    let dir = TempDir::new().unwrap();
    // 1 MB of incompressible-ish junk alongside a small dump: the archive is
    // much bigger than the .sql member, so a bar sized from the archive
    // would finish at a few percent.
    let junk: Vec<u8> = (0..1_000_000u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let zip_path = write_zip(
        dir.path(),
        "padded.zip",
        &[("junk.bin", &junk), ("dump.sql", DUMP)],
    );

    let archive_size = std::fs::metadata(&zip_path).unwrap().len();
    let total = input_progress_len(&zip_path);
    assert!(
        total < archive_size / 2,
        "progress total {total} should be the member's compressed size, \
         not the {archive_size}-byte archive"
    );

    // The bar must reach exactly 100%: reading the member to EOF reports
    // precisely `total` bytes through the progress callback.
    let reported = Arc::new(AtomicU64::new(0));
    let reported_cb = reported.clone();
    let mut reader = open_input_with_progress(
        &zip_path,
        Box::new(move |bytes| {
            reported_cb.store(bytes, Ordering::Relaxed);
        }),
    )
    .unwrap();
    let mut out = Vec::new();
    reader.read_to_end(&mut out).unwrap();
    assert_eq!(out, DUMP);
    assert_eq!(
        reported.load(Ordering::Relaxed),
        total,
        "progress callback must end exactly at the progress total"
    );
}

#[test]
fn non_zip_progress_total_is_file_size() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dump.sql");
    std::fs::write(&path, DUMP).unwrap();
    assert_eq!(input_progress_len(&path), DUMP.len() as u64);
}

#[test]
fn invalid_zip_progress_total_falls_back_to_file_size() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("broken.zip");
    std::fs::write(&path, b"not a zip archive at all").unwrap();
    // The real error surfaces when the input is opened; the progress total
    // just falls back to the on-disk size.
    assert_eq!(input_progress_len(&path), 24);
}

// ---------------------------------------------------------------------------
// Bugs 2 + 3: multi-file --json exit code and --fail-fast skipped entries
// ---------------------------------------------------------------------------

/// Three-zip fixture where `b.zip` is invalid (two .sql members).
fn multi_zip_fixture() -> TempDir {
    let dir = TempDir::new().unwrap();
    write_zip(dir.path(), "a.zip", &[("a.sql", DUMP)]);
    write_zip(dir.path(), "b.zip", &[("x.sql", DUMP), ("y.sql", DUMP)]);
    write_zip(dir.path(), "c.zip", &[("c.sql", DUMP)]);
    dir
}

fn run_split_json(
    dir: &TempDir,
    extra_args: &[&str],
) -> (std::process::ExitStatus, serde_json::Value) {
    let pattern = dir.path().join("*.zip");
    let out_dir = dir.path().join("out");
    let output = bin()
        .arg("split")
        .arg(&pattern)
        .arg("-o")
        .arg(&out_dir)
        .arg("--json")
        .args(extra_args)
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value =
        serde_json::from_str(&stdout).unwrap_or_else(|e| panic!("bad JSON ({e}): {stdout}"));
    (output.status, json)
}

#[test]
fn multi_file_json_exits_nonzero_when_files_failed() {
    let dir = multi_zip_fixture();
    let (status, json) = run_split_json(&dir, &[]);

    assert_eq!(json["total_files"], 3);
    assert_eq!(json["succeeded"], 2);
    assert_eq!(json["failed"], 1);
    assert_eq!(json["skipped"], 0);
    assert_eq!(
        status.code(),
        Some(1),
        "--json batch with failures must exit 1 like the non-JSON path"
    );
}

#[test]
fn multi_file_json_exits_zero_when_all_succeed() {
    let dir = TempDir::new().unwrap();
    write_zip(dir.path(), "a.zip", &[("a.sql", DUMP)]);
    write_zip(dir.path(), "c.zip", &[("c.sql", DUMP)]);
    let (status, json) = run_split_json(&dir, &[]);

    assert_eq!(json["failed"], 0);
    assert_eq!(json["skipped"], 0);
    assert_eq!(status.code(), Some(0));
}

#[test]
fn fail_fast_json_reports_unattempted_files_as_skipped() {
    let dir = multi_zip_fixture();
    let (status, json) = run_split_json(&dir, &["--fail-fast"]);

    // Glob order is sorted: a.zip succeeds, b.zip fails, c.zip is skipped.
    assert_eq!(json["total_files"], 3);
    assert_eq!(json["succeeded"], 1);
    assert_eq!(json["failed"], 1);
    assert_eq!(json["skipped"], 1);
    assert_eq!(status.code(), Some(1), "--fail-fast --json must exit 1");

    let results = json["results"].as_array().unwrap();
    assert_eq!(
        results.len(),
        3,
        "every input file must appear in results: {results:?}"
    );
    let statuses: Vec<&str> = results
        .iter()
        .map(|r| r["status"].as_str().unwrap())
        .collect();
    assert_eq!(statuses, ["success", "failed", "skipped"]);
    let skipped_entry = &results[2];
    assert!(
        skipped_entry["file"].as_str().unwrap().ends_with("c.zip"),
        "{skipped_entry:?}"
    );
    assert!(skipped_entry["error"].is_null());
}

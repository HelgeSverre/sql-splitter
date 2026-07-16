//! Integration tests for zip-file input (v1.16.0).
//!
//! Builds zip fixtures on the fly with the `zip` crate (no binary files
//! committed to the repo) and drives them through the public `Splitter` and
//! `Validator`/`Analyzer` APIs.

#![cfg(feature = "archive")]

use sql_splitter::analyzer::Analyzer;
use sql_splitter::splitter::{open_input, Compression, Splitter};
use sql_splitter::validate::{ValidateOptions, Validator};
use std::io::{Read, Write};
use tempfile::TempDir;

/// Write a zip archive at `dir/name` containing `entries` (name, contents,
/// method).
fn write_zip(
    dir: &std::path::Path,
    name: &str,
    entries: &[(&str, &[u8], zip::CompressionMethod)],
) -> std::path::PathBuf {
    let path = dir.join(name);
    let file = std::fs::File::create(&path).unwrap();
    let mut zip = zip::ZipWriter::new(file);
    for (entry_name, contents, method) in entries {
        let opts = zip::write::SimpleFileOptions::default().compression_method(*method);
        zip.start_file(*entry_name, opts).unwrap();
        zip.write_all(contents).unwrap();
    }
    zip.finish().unwrap();
    path
}

const DUMP: &[u8] = b"CREATE TABLE `users` (id INT);\nINSERT INTO `users` VALUES (1),(2);\nCREATE TABLE `orders` (id INT);\nINSERT INTO `orders` VALUES (10);\n";

fn read_dir_files(dir: &std::path::Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    let mut out = std::collections::BTreeMap::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            out.insert(
                entry.file_name().to_string_lossy().into_owned(),
                std::fs::read(entry.path()).unwrap(),
            );
        }
    }
    out
}

#[test]
fn test_compression_detects_zip() {
    assert_eq!(
        Compression::from_path(std::path::Path::new("dump.zip")),
        Compression::Zip
    );
}

#[test]
fn test_split_zip_deflated_matches_plain() {
    let temp = TempDir::new().unwrap();

    let plain_input = temp.path().join("plain.sql");
    std::fs::write(&plain_input, DUMP).unwrap();
    let plain_dir = temp.path().join("plain_out");
    Splitter::new(plain_input, plain_dir.clone())
        .split()
        .unwrap();

    let zip_path = write_zip(
        temp.path(),
        "dump.zip",
        &[("dump.sql", DUMP, zip::CompressionMethod::Deflated)],
    );
    let zip_out = temp.path().join("zip_out");
    let stats = Splitter::new(zip_path, zip_out.clone()).split().unwrap();

    assert_eq!(stats.tables_found, 2);
    assert_eq!(read_dir_files(&plain_dir), read_dir_files(&zip_out));
}

#[test]
fn test_split_zip_stored_matches_plain() {
    let temp = TempDir::new().unwrap();

    let plain_input = temp.path().join("plain.sql");
    std::fs::write(&plain_input, DUMP).unwrap();
    let plain_dir = temp.path().join("plain_out");
    Splitter::new(plain_input, plain_dir.clone())
        .split()
        .unwrap();

    let zip_path = write_zip(
        temp.path(),
        "dump.zip",
        &[("dump.sql", DUMP, zip::CompressionMethod::Stored)],
    );
    let zip_out = temp.path().join("zip_out");
    Splitter::new(zip_path, zip_out.clone()).split().unwrap();

    assert_eq!(read_dir_files(&plain_dir), read_dir_files(&zip_out));
}

#[test]
fn test_split_zip_tolerates_junk_entries() {
    let temp = TempDir::new().unwrap();

    let zip_path = write_zip(
        temp.path(),
        "dump.zip",
        &[
            ("__MACOSX/dump.sql", b"junk", zip::CompressionMethod::Stored),
            (".DS_Store", b"junk", zip::CompressionMethod::Stored),
            ("dump.sql", DUMP, zip::CompressionMethod::Deflated),
        ],
    );

    let out_dir = temp.path().join("out");
    let stats = Splitter::new(zip_path, out_dir.clone()).split().unwrap();

    assert_eq!(stats.tables_found, 2);
    assert!(out_dir.join("users.sql").exists());
    assert!(out_dir.join("orders.sql").exists());
}

#[test]
fn test_split_zip_multiple_sql_members_errors() {
    let temp = TempDir::new().unwrap();

    let zip_path = write_zip(
        temp.path(),
        "dump.zip",
        &[
            ("a.sql", DUMP, zip::CompressionMethod::Stored),
            ("b.sql", DUMP, zip::CompressionMethod::Stored),
        ],
    );

    let out_dir = temp.path().join("out");
    let err = match Splitter::new(zip_path, out_dir).split() {
        Ok(_) => panic!("expected split to fail"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(msg.contains("a.sql"), "{msg}");
    assert!(msg.contains("b.sql"), "{msg}");
}

#[test]
fn test_split_zip_no_sql_member_errors() {
    let temp = TempDir::new().unwrap();

    let zip_path = write_zip(
        temp.path(),
        "dump.zip",
        &[("notes.txt", b"hello", zip::CompressionMethod::Stored)],
    );

    let out_dir = temp.path().join("out");
    let err = match Splitter::new(zip_path, out_dir).split() {
        Ok(_) => panic!("expected split to fail"),
        Err(err) => err,
    };
    assert!(err.to_string().contains("no .sql member"));
}

#[test]
fn test_open_input_reads_zip_member() {
    let temp = TempDir::new().unwrap();
    let zip_path = write_zip(
        temp.path(),
        "dump.zip",
        &[("dump.sql", DUMP, zip::CompressionMethod::Deflated)],
    );

    let mut reader = open_input(&zip_path).unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, DUMP);
}

#[test]
fn test_validate_zip_input() {
    let temp = TempDir::new().unwrap();
    let zip_path = write_zip(
        temp.path(),
        "dump.zip",
        &[("dump.sql", DUMP, zip::CompressionMethod::Deflated)],
    );

    let options = ValidateOptions {
        path: zip_path,
        dialect: None,
        progress: false,
        strict: false,
        json: false,
        max_rows_per_table: 1_000_000,
        fk_checks_enabled: true,
        max_pk_fk_keys: None,
    };
    let summary = Validator::new(options).validate().unwrap();
    assert_eq!(summary.summary.tables_scanned, 2);
}

#[test]
fn test_analyze_zip_input() {
    let temp = TempDir::new().unwrap();
    let zip_path = write_zip(
        temp.path(),
        "dump.zip",
        &[("dump.sql", DUMP, zip::CompressionMethod::Deflated)],
    );

    let stats = Analyzer::new(zip_path).analyze().unwrap();
    let names: Vec<&str> = stats.iter().map(|s| s.table_name.as_str()).collect();
    assert!(names.contains(&"users"));
    assert!(names.contains(&"orders"));
}

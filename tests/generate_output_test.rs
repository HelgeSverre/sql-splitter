//! Tests for the protected family-state and atomic-output primitives in
//! `sql_splitter::generate::output`.
//!
//! These lock down the security-critical properties the primitives exist for:
//! owner-only spool permissions even under a permissive umask, atomic
//! publication that never truncates an existing destination before success,
//! deterministic cleanup of every registered temporary file on failure, and a
//! spool reader that rejects hostile framing before it allocates.

use std::io::Write;

use sql_splitter::generate::output::{
    AtomicOutput, CancellationToken, FamilyBudget, FamilyBuffer, FamilyState, ProtectedSpool,
    PublicationSet, SpillKind, SpoolWriter, SpooledRow, TempConfig,
};
use sql_splitter::generate::value::GeneratedValue;

/// A spooled row carrying one of every `GeneratedValue` shape, so encode/decode
/// round-trips exercise every field tag.
fn every_shape_row(row_index: u64) -> SpooledRow {
    SpooledRow {
        table_id: 3,
        row_index,
        values: vec![
            GeneratedValue::Null,
            GeneratedValue::Default,
            GeneratedValue::Boolean(true),
            GeneratedValue::Integer(-42),
            GeneratedValue::Decimal {
                minor: 1050,
                scale: 2,
            },
            GeneratedValue::Text("hello, spool".to_string()),
            GeneratedValue::Bytes(vec![0, 1, 2, 255]),
            GeneratedValue::DateTime("2024-01-01 00:00:00".to_string()),
            GeneratedValue::Json("{\"k\":1}".to_string()),
        ],
    }
}

#[cfg(unix)]
fn mode_of(path: &std::path::Path) -> u32 {
    use std::os::unix::fs::MetadataExt;
    std::fs::metadata(path).unwrap().mode() & 0o777
}

/// `umask(2)` is process-global, but integration tests run in parallel threads
/// of one process — so any test that mutates the umask must hold this lock to
/// keep a concurrent test from observing the wrong ambient umask.
#[cfg(unix)]
static UMASK_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
#[cfg(unix)]
fn protected_spools_are_owner_only_under_a_permissive_umask() {
    // A permissive umask must NOT widen the spool: the mode is set explicitly,
    // so the file is 0600 (owner-only) regardless of the ambient umask.
    let _guard = UMASK_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let previous = unsafe { libc::umask(0) };
    let spool = ProtectedSpool::create(&TempConfig::default()).unwrap();
    let mode = mode_of(spool.path());
    unsafe {
        libc::umask(previous);
    }
    assert_eq!(mode, 0o600, "spool must be owner-only even under umask 000");
}

#[test]
fn protected_spool_round_trips_length_prefixed_rows_in_order() {
    let mut spool = ProtectedSpool::create(&TempConfig::default()).unwrap();
    let written: Vec<SpooledRow> = (0..5).map(every_shape_row).collect();
    for row in &written {
        spool.write_row(row).unwrap();
    }

    let mut reader = spool.rewind().unwrap();
    let mut read_back = Vec::new();
    while let Some(row) = reader.read_row().unwrap() {
        read_back.push(row);
    }
    assert_eq!(read_back, written);
}

#[test]
fn spool_reader_rejects_an_oversized_length_prefix_before_allocating() {
    // A valid version byte followed by a 4 GiB length must be refused up front,
    // never turned into a multi-gigabyte allocation.
    let mut framed = Vec::new();
    framed.push(1u8); // SPOOL_VERSION
    framed.extend_from_slice(&u32::MAX.to_le_bytes());
    let mut reader = sql_splitter::generate::output::SpoolReader::new(framed.as_slice());
    let err = reader
        .read_row()
        .expect_err("an oversized length prefix must be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn spool_reader_rejects_a_version_mismatch_before_allocating() {
    let mut framed = Vec::new();
    framed.push(99u8); // wrong version
    framed.extend_from_slice(&16u32.to_le_bytes());
    framed.extend_from_slice(&[0u8; 16]);
    let mut reader = sql_splitter::generate::output::SpoolReader::new(framed.as_slice());
    let err = reader
        .read_row()
        .expect_err("a version mismatch must be rejected");
    assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn atomic_output_never_truncates_an_existing_destination_on_failure() {
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.sql");
    std::fs::write(&dest, b"ORIGINAL CONTENT").unwrap();

    {
        let mut output = AtomicOutput::create(&dest).unwrap();
        output.writer().write_all(b"HALF-WRITTEN GARBAGE").unwrap();
        // Simulate a generation/verification failure: drop WITHOUT committing.
    }

    // The destination retains its original bytes, byte-for-byte.
    assert_eq!(std::fs::read(&dest).unwrap(), b"ORIGINAL CONTENT");
    // And no stray temp files were left behind next to it.
    let strays: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| e.file_name() != std::ffi::OsStr::new("out.sql"))
        .collect();
    assert!(strays.is_empty(), "temp files leaked: {strays:?}");
}

#[test]
fn atomic_output_publishes_new_content_only_after_commit() {
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.sql");
    std::fs::write(&dest, b"ORIGINAL").unwrap();

    let mut output = AtomicOutput::create(&dest).unwrap();
    output.writer().write_all(b"PUBLISHED").unwrap();
    // Before commit the destination is unchanged.
    assert_eq!(std::fs::read(&dest).unwrap(), b"ORIGINAL");
    output.commit().unwrap();
    // After commit it holds exactly the new bytes.
    assert_eq!(std::fs::read(&dest).unwrap(), b"PUBLISHED");
}

#[test]
fn atomic_output_writes_a_brand_new_destination() {
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("fresh.sql");
    let mut output = AtomicOutput::create(&dest).unwrap();
    output.writer().write_all(b"NEW FILE").unwrap();
    assert!(!dest.exists(), "destination must not exist before commit");
    output.commit().unwrap();
    assert_eq!(std::fs::read(&dest).unwrap(), b"NEW FILE");
}

#[test]
#[cfg(unix)]
fn atomic_output_preserves_an_existing_destination_mode() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("out.sql");
    std::fs::write(&dest, b"ORIGINAL").unwrap();
    std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o640)).unwrap();

    let mut output = AtomicOutput::create(&dest).unwrap();
    // While being written, the temp output is owner-only (0600), never 0640.
    assert_eq!(mode_of(output.temp_path()), 0o600);
    output.writer().write_all(b"PUBLISHED").unwrap();
    output.commit().unwrap();

    // The published file follows the destination's ORIGINAL mode, not 0600.
    assert_eq!(mode_of(&dest), 0o640);
}

#[test]
#[cfg(unix)]
fn atomic_output_gives_a_new_destination_normal_output_permissions() {
    // A new destination follows normal output-file permissions (umask-adjusted),
    // NOT the owner-only 0600 the temp carried while being written.
    let _guard = UMASK_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let previous = unsafe { libc::umask(0o022) };
    let dir = tempfile::tempdir().unwrap();
    let dest = dir.path().join("fresh.sql");
    let mut output = AtomicOutput::create(&dest).unwrap();
    output.writer().write_all(b"NEW").unwrap();
    output.commit().unwrap();
    let mode = mode_of(&dest);
    unsafe {
        libc::umask(previous);
    }
    assert_eq!(mode, 0o644, "new file should follow umask-adjusted 0666");
}

#[test]
fn publication_set_leaves_originals_intact_until_published() {
    let dir = tempfile::tempdir().unwrap();
    let sql = dir.path().join("data.sql");
    let model = dir.path().join("model.yaml");
    std::fs::write(&sql, b"OLD SQL").unwrap();
    std::fs::write(&model, b"OLD MODEL").unwrap();

    {
        let mut set = PublicationSet::new();
        set.stage(&sql)
            .unwrap()
            .writer()
            .write_all(b"NEW SQL")
            .unwrap();
        set.stage(&model)
            .unwrap()
            .writer()
            .write_all(b"NEW MODEL")
            .unwrap();
        // Dropped without publishing: both originals survive.
    }
    assert_eq!(std::fs::read(&sql).unwrap(), b"OLD SQL");
    assert_eq!(std::fs::read(&model).unwrap(), b"OLD MODEL");

    let mut set = PublicationSet::new();
    set.stage(&sql)
        .unwrap()
        .writer()
        .write_all(b"NEW SQL")
        .unwrap();
    set.stage(&model)
        .unwrap()
        .writer()
        .write_all(b"NEW MODEL")
        .unwrap();
    set.publish().unwrap();
    assert_eq!(std::fs::read(&sql).unwrap(), b"NEW SQL");
    assert_eq!(std::fs::read(&model).unwrap(), b"NEW MODEL");
}

#[test]
fn dropped_spool_removes_its_backing_file() {
    let path = {
        let spool = ProtectedSpool::create(&TempConfig::default()).unwrap();
        spool.path().to_path_buf()
    };
    assert!(!path.exists(), "spool file must be cleaned up on drop");
}

#[test]
fn cancellation_token_makes_a_loop_return_a_cancellation_error() {
    let token = CancellationToken::new();
    assert!(token.check().is_ok());

    let mut produced = 0u32;
    for i in 0..1000u32 {
        if token.check().is_err() {
            break;
        }
        if i == 5 {
            token.cancel();
        }
        produced += 1;
    }
    // The loop stops as soon as it observes cancellation on the next check.
    assert_eq!(produced, 6);
    assert!(token.is_cancelled());
    assert!(token.check().is_err());
}

#[test]
fn family_buffer_keeps_small_families_in_memory() {
    let temp = TempConfig::default();
    let budget = FamilyBudget { max_bytes: 1 << 20 };
    let mut buffer = FamilyBuffer::new(budget, 1, temp, SpillKind::Child);
    let rows: Vec<SpooledRow> = (0..4).map(every_shape_row).collect();
    for row in &rows {
        buffer.push(row.clone()).unwrap();
    }
    assert!(!buffer.is_spilled(), "a tiny family must stay in memory");
    assert_eq!(buffer.drain_rows().unwrap(), rows);
}

#[test]
fn family_spill_bytes_match_the_canonical_frame_encoding() {
    // The spilled byte stream must be exactly the canonical length-prefixed
    // frames — the buffered, single-writer spill path changes nothing on disk
    // versus encoding each row through a plain SpoolWriter in push order.
    let rows: Vec<SpooledRow> = (0..16).map(every_shape_row).collect();

    // Canonical reference: encode every row, in order, with a plain writer.
    let mut expected = Vec::new();
    let mut writer = SpoolWriter::new(&mut expected);
    for row in &rows {
        writer.write_row(row).unwrap();
    }
    writer.flush().unwrap();

    // Spill the same rows through a FamilyBuffer with a tiny budget.
    let mut buffer = FamilyBuffer::new(
        FamilyBudget { max_bytes: 8 },
        3,
        TempConfig::default(),
        SpillKind::Child,
    );
    for row in &rows {
        buffer.push(row.clone()).unwrap();
    }
    // Draining flushes the spool's buffered writer to its backing file.
    assert_eq!(buffer.drain_rows().unwrap(), rows);

    let spool_path = match buffer.state() {
        FamilyState::ChildSpool(spool) => spool.path().to_path_buf(),
        FamilyState::ParentState(_) | FamilyState::TableSpool(_) => {
            panic!("expected a child spool after crossing the budget")
        }
    };
    let on_disk = std::fs::read(&spool_path).unwrap();
    assert_eq!(
        on_disk, expected,
        "spilled bytes are not the canonical frames"
    );
}

#[test]
fn family_buffer_spills_deterministically_when_it_crosses_its_budget() {
    let temp = TempConfig::default();
    // A budget far smaller than the rows forces a spill after the first row.
    let budget = FamilyBudget { max_bytes: 32 };
    let mut buffer = FamilyBuffer::new(budget, 7, temp, SpillKind::Child);
    let rows: Vec<SpooledRow> = (0..20).map(every_shape_row).collect();
    for row in &rows {
        buffer.push(row.clone()).unwrap();
    }
    assert!(
        buffer.is_spilled(),
        "crossing the budget must spill to disk"
    );
    // Every child row survives the spill, in order — never dropped, never
    // retained in an unbounded in-memory Vec.
    assert_eq!(buffer.drain_rows().unwrap(), rows);
}

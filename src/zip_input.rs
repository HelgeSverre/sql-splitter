//! Zip archive input support (feature `archive`).
//!
//! Zip is an archive of multiple members rather than a single compressed
//! stream, and the `zip` crate needs `Read + Seek` to parse the central
//! directory — so it can't be plugged in as another
//! [`crate::splitter::Compression`] streaming decoder the way gzip/bzip2/xz/
//! zstd are. Instead this module does a two-phase open: `zip::ZipArchive`
//! walks the central directory on a seekable `File` to locate the sole
//! `.sql` member (this gets zip64 support and tolerance of junk entries like
//! `__MACOSX/` for free, since it's the same parser `zip` uses everywhere
//! else), then the *same* `File` handle is recovered from the archive and
//! seeked to that member's data offset, and the tail is streamed through an
//! ordinary decompressor. Reusing the handle (rather than reopening `path`)
//! means a file concurrently replaced on disk cannot be decoded from a stale
//! offset — the scan and the data read always see the same bytes.
//! Downstream code (parser, dialect detection, ...) never touches the `zip`
//! crate directly — it just sees a `Box<dyn Read>`, same as every other
//! input format.

use anyhow::{bail, Context};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Location and encoding of the single `.sql` member inside a zip archive,
/// as found by [`locate_sql_member`].
#[derive(Debug)]
struct ZipSqlMember {
    name: String,
    data_start: u64,
    compressed_size: u64,
    method: zip::CompressionMethod,
}

/// Ignore well-known junk that archivers/OSes add: macOS resource-fork
/// entries (`__MACOSX/...` and AppleDouble `._*` files) and `.DS_Store`.
/// Deliberately narrow — a dump legitimately named `.hidden.sql` is still a
/// valid candidate.
fn is_junk_entry(name: &str) -> bool {
    if name.starts_with("__MACOSX/") {
        return true;
    }
    let base = name.rsplit('/').next().unwrap_or(name);
    base.is_empty() || base == ".DS_Store" || base.starts_with("._")
}

/// Walk the central directory of the zip archive at `path` and locate the
/// sole `.sql` member (case-insensitive extension match, junk entries and
/// directories ignored).
///
/// Returns the still-open [`zip::ZipArchive`] alongside the member metadata
/// so the caller can keep reading from the very same `File` handle the
/// central directory was parsed from (avoiding a reopen-of-`path` race).
///
/// The scan is metadata-only (`name_for_index`): it never opens entry data,
/// so encrypted or exotically-compressed *sibling* entries cannot abort it.
/// Encryption/compression-method checks apply only to the chosen candidate.
fn locate_sql_member(path: &Path) -> anyhow::Result<(zip::ZipArchive<File>, ZipSqlMember)> {
    let file = File::open(path).with_context(|| format!("Failed to open zip archive {path:?}"))?;
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("Failed to read zip central directory in {path:?}"))?;

    let mut candidates: Vec<usize> = Vec::new();
    for i in 0..archive.len() {
        // Non-UTF-8 names can't match `*.sql`; skip them.
        let Some(name) = archive.name_for_index(i) else {
            continue;
        };
        if name.ends_with('/') || name.ends_with('\\') || is_junk_entry(name) {
            continue;
        }
        if name.to_ascii_lowercase().ends_with(".sql") {
            candidates.push(i);
        }
    }

    match candidates.len() {
        0 => bail!("zip archive {path:?} contains no .sql member"),
        1 => {
            // Raw open: parses the local header (for data_start) but never
            // touches decompression or decryption, so it succeeds even for
            // entries the crate can't decode — letting the specific checks
            // below produce their intended error messages.
            let member = {
                let entry = archive
                    .by_index_raw(candidates[0])
                    .with_context(|| format!("Failed to read zip entry in {path:?}"))?;
                if entry
                    .unix_mode()
                    .is_some_and(|mode| mode & 0o170_000 == 0o120_000)
                {
                    bail!(
                        "zip member '{}' in {path:?} is a symlink; symlink members are not supported",
                        entry.name()
                    );
                }
                if entry.encrypted() {
                    bail!(
                        "zip member '{}' in {path:?} is encrypted; encrypted zip input is not supported",
                        entry.name()
                    );
                }
                let method = entry.compression();
                if !matches!(
                    method,
                    zip::CompressionMethod::Stored | zip::CompressionMethod::Deflated
                ) {
                    bail!(
                        "zip member '{}' in {path:?} uses unsupported compression method {method}",
                        entry.name()
                    );
                }
                ZipSqlMember {
                    name: entry.name().to_string(),
                    data_start: entry.data_start(),
                    compressed_size: entry.compressed_size(),
                    method,
                }
            };
            Ok((archive, member))
        }
        _ => {
            let names: Vec<&str> = candidates
                .iter()
                .filter_map(|&i| archive.name_for_index(i))
                .collect();
            bail!(
                "zip archive {path:?} contains multiple .sql members ({}); expected exactly one",
                names.join(", ")
            );
        }
    }
}

/// Open the sole `.sql` member of the zip archive at `path` as a streaming
/// reader. If `progress_fn` is given, it is fed the count of raw
/// (compressed) bytes read from disk, matching how progress is tracked for
/// the other compressed-input formats.
pub(crate) fn open_zip_member(
    path: &Path,
    progress_fn: Option<Box<dyn Fn(u64)>>,
) -> anyhow::Result<Box<dyn Read>> {
    let (archive, member) = locate_sql_member(path)?;

    // Keep reading from the same File handle the central directory was
    // parsed from. Reopening `path` here would race with concurrent
    // replacement of the file (symlink swap, re-download, in-progress
    // write): the fresh handle could see different bytes at the stale
    // `data_start` and silently decode garbage.
    let mut file = archive.into_inner();
    file.seek(SeekFrom::Start(member.data_start))
        .with_context(|| format!("Failed to seek to zip member '{}' data", member.name))?;

    let inner: Box<dyn Read> = match progress_fn {
        Some(cb) => Box::new(crate::progress::ProgressReader::new(file, cb)),
        None => Box::new(file),
    };
    // Bound the stream to just this member's compressed bytes so a stored
    // (uncompressed) member doesn't read into whatever follows it in the
    // archive (central directory, next entry, ...).
    let bounded: Box<dyn Read> = Box::new(inner.take(member.compressed_size));

    Ok(match member.method {
        zip::CompressionMethod::Stored => bounded,
        zip::CompressionMethod::Deflated => Box::new(flate2::read::DeflateDecoder::new(bounded)),
        // locate_sql_member() already rejected every other method.
        _ => unreachable!("unsupported zip compression method should have been rejected earlier"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_zip(entries: &[(&str, &[u8], zip::CompressionMethod)]) -> tempfile::TempDir {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("input.zip");
        let file = File::create(&path).unwrap();
        let mut zip = zip::ZipWriter::new(file);
        for (name, contents, method) in entries {
            let opts = zip::write::SimpleFileOptions::default().compression_method(*method);
            zip.start_file(*name, opts).unwrap();
            zip.write_all(contents).unwrap();
        }
        zip.finish().unwrap();
        dir
    }

    #[test]
    fn locates_single_sql_member() {
        let dir = write_zip(&[("dump.sql", b"SELECT 1;", zip::CompressionMethod::Deflated)]);
        let (_, member) = locate_sql_member(&dir.path().join("input.zip")).unwrap();
        assert_eq!(member.name, "dump.sql");
    }

    #[test]
    fn locates_dot_basename_sql_member() {
        // Regression: the junk filter used to treat every dot-basename as
        // junk, so a zip whose sole member was '.hidden.sql' reported
        // 'contains no .sql member'.
        let dir = write_zip(&[(
            ".hidden.sql",
            b"SELECT 1;",
            zip::CompressionMethod::Deflated,
        )]);
        let (_, member) = locate_sql_member(&dir.path().join("input.zip")).unwrap();
        assert_eq!(member.name, ".hidden.sql");
    }

    #[test]
    fn ignores_appledouble_sql_sibling() {
        let dir = write_zip(&[
            ("._dump.sql", b"junk", zip::CompressionMethod::Stored),
            ("dump.sql", b"SELECT 1;", zip::CompressionMethod::Stored),
        ]);
        let (_, member) = locate_sql_member(&dir.path().join("input.zip")).unwrap();
        assert_eq!(member.name, "dump.sql");
    }

    #[test]
    fn errors_on_symlink_sql_member() {
        // Regression: a symlink member's stored data is the target path
        // string; it used to be split as (non-)SQL text, 'succeeding' with 0
        // tables. It must be rejected with a clear error instead.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("input.zip");
        let mut zip = zip::ZipWriter::new(File::create(&path).unwrap());
        zip.add_symlink(
            "dump.sql",
            "real.sql",
            zip::write::SimpleFileOptions::default(),
        )
        .unwrap();
        zip.finish().unwrap();

        let err = locate_sql_member(&path).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("is a symlink"), "{msg}");
        assert!(msg.contains("dump.sql"), "{msg}");
    }

    #[test]
    fn scan_handle_survives_file_replacement() {
        // Regression (TOCTOU): the data read must go through the same File
        // handle the central directory was scanned from, so replacing the
        // archive on disk between scan and read cannot redirect the read.
        let dir = write_zip(&[("dump.sql", b"SELECT 42;", zip::CompressionMethod::Stored)]);
        let path = dir.path().join("input.zip");
        let (archive, member) = locate_sql_member(&path).unwrap();

        // Rename-replace (new inode), as done by rsync/browsers/atomic writers.
        let replacement = dir.path().join("replacement");
        std::fs::write(&replacement, b"not a zip at all, just garbage bytes").unwrap();
        std::fs::rename(&replacement, &path).unwrap();

        let mut file = archive.into_inner();
        file.seek(SeekFrom::Start(member.data_start)).unwrap();
        let mut buf = vec![0u8; member.compressed_size as usize];
        file.read_exact(&mut buf).unwrap();
        assert_eq!(buf, b"SELECT 42;");
    }

    #[test]
    fn open_reader_survives_file_replacement() {
        let dir = write_zip(&[("dump.sql", b"SELECT 42;", zip::CompressionMethod::Deflated)]);
        let path = dir.path().join("input.zip");
        let mut reader = open_zip_member(&path, None).unwrap();

        let replacement = dir.path().join("replacement");
        std::fs::write(&replacement, b"garbage").unwrap();
        std::fs::rename(&replacement, &path).unwrap();

        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, b"SELECT 42;");
    }

    #[test]
    fn ignores_junk_entries() {
        let dir = write_zip(&[
            ("__MACOSX/dump.sql", b"junk", zip::CompressionMethod::Stored),
            (".DS_Store", b"junk", zip::CompressionMethod::Stored),
            ("dump.sql", b"SELECT 1;", zip::CompressionMethod::Stored),
        ]);
        let (_, member) = locate_sql_member(&dir.path().join("input.zip")).unwrap();
        assert_eq!(member.name, "dump.sql");
    }

    #[test]
    fn errors_on_multiple_sql_members() {
        let dir = write_zip(&[
            ("a.sql", b"SELECT 1;", zip::CompressionMethod::Stored),
            ("b.sql", b"SELECT 2;", zip::CompressionMethod::Stored),
        ]);
        let err = locate_sql_member(&dir.path().join("input.zip")).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("a.sql"), "{msg}");
        assert!(msg.contains("b.sql"), "{msg}");
    }

    #[test]
    fn errors_on_no_sql_member() {
        let dir = write_zip(&[("notes.txt", b"hi", zip::CompressionMethod::Stored)]);
        let err = locate_sql_member(&dir.path().join("input.zip")).unwrap_err();
        assert!(err.to_string().contains("no .sql member"));
    }

    #[test]
    fn reads_stored_and_deflated_members() {
        for method in [
            zip::CompressionMethod::Stored,
            zip::CompressionMethod::Deflated,
        ] {
            let dir = write_zip(&[("dump.sql", b"SELECT 42;", method)]);
            let mut reader = open_zip_member(&dir.path().join("input.zip"), None).unwrap();
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf).unwrap();
            assert_eq!(buf, b"SELECT 42;", "method={method:?}");
        }
    }
}

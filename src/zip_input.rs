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
//! else), then a fresh `File` is reopened and seeked to that member's data
//! offset, and the tail is streamed through an ordinary decompressor.
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

/// Ignore directory entries and common junk that archivers/OSes add: macOS
/// resource-fork entries (`__MACOSX/...`), `.DS_Store`, and other dotfiles.
fn is_junk_entry(name: &str) -> bool {
    if name.starts_with("__MACOSX/") {
        return true;
    }
    let base = name.rsplit('/').next().unwrap_or(name);
    base.is_empty() || base.starts_with('.')
}

/// Walk the central directory of the zip archive at `path` and locate the
/// sole `.sql` member (case-insensitive extension match, junk entries and
/// directories ignored).
fn locate_sql_member(path: &Path) -> anyhow::Result<ZipSqlMember> {
    let file = File::open(path).with_context(|| format!("Failed to open zip archive {path:?}"))?;
    let mut archive = zip::ZipArchive::new(file)
        .with_context(|| format!("Failed to read zip central directory in {path:?}"))?;

    let mut candidates: Vec<usize> = Vec::new();
    for i in 0..archive.len() {
        let entry = archive
            .by_index(i)
            .with_context(|| format!("Failed to read zip entry {i} in {path:?}"))?;
        if entry.is_dir() || is_junk_entry(entry.name()) {
            continue;
        }
        if entry.name().to_ascii_lowercase().ends_with(".sql") {
            candidates.push(i);
        }
    }

    match candidates.len() {
        0 => bail!("zip archive {path:?} contains no .sql member"),
        1 => {
            let entry = archive.by_index(candidates[0])?;
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
            Ok(ZipSqlMember {
                name: entry.name().to_string(),
                data_start: entry.data_start(),
                compressed_size: entry.compressed_size(),
                method,
            })
        }
        _ => {
            let mut names = Vec::with_capacity(candidates.len());
            for i in candidates {
                names.push(archive.by_index(i)?.name().to_string());
            }
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
    let member = locate_sql_member(path)?;

    let mut file =
        File::open(path).with_context(|| format!("Failed to reopen zip archive {path:?}"))?;
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
        let member = locate_sql_member(&dir.path().join("input.zip")).unwrap();
        assert_eq!(member.name, "dump.sql");
    }

    #[test]
    fn ignores_junk_entries() {
        let dir = write_zip(&[
            ("__MACOSX/dump.sql", b"junk", zip::CompressionMethod::Stored),
            (".DS_Store", b"junk", zip::CompressionMethod::Stored),
            ("dump.sql", b"SELECT 1;", zip::CompressionMethod::Stored),
        ]);
        let member = locate_sql_member(&dir.path().join("input.zip")).unwrap();
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

//! Single-file archive output for `split` (the spool-then-pack model).
//!
//! An archive (`tar.gz`, `zip`, …) is one sequential stream whose entries must
//! be written complete and (for tar) size-first — incompatible with the
//! interleaved statement stream `split` produces. So archives are built in two
//! phases: **spool** the tables to a temp directory using the normal parallel
//! writers, then **pack** each finished file into the archive in one pass.

use anyhow::Context;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

/// Single-file archive container, inferred from the output path's extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchiveFormat {
    Tar,
    TarGz,
    TarZst,
    TarBz2,
    TarXz,
    Zip,
}

impl ArchiveFormat {
    /// Detect an archive format from an output path's extension. Returns `None`
    /// for an ordinary directory output.
    pub fn from_output_path(path: &Path) -> Option<Self> {
        let name = path.file_name()?.to_str()?.to_ascii_lowercase();
        let f = if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
            Self::TarGz
        } else if name.ends_with(".tar.zst") || name.ends_with(".tzst") {
            Self::TarZst
        } else if name.ends_with(".tar.bz2") || name.ends_with(".tbz2") {
            Self::TarBz2
        } else if name.ends_with(".tar.xz") || name.ends_with(".txz") {
            Self::TarXz
        } else if name.ends_with(".tar") {
            Self::Tar
        } else if name.ends_with(".zip") {
            Self::Zip
        } else {
            return None;
        };
        Some(f)
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Tar => "tar",
            Self::TarGz => "tar.gz",
            Self::TarZst => "tar.zst",
            Self::TarBz2 => "tar.bz2",
            Self::TarXz => "tar.xz",
            Self::Zip => "zip",
        }
    }
}

/// Pack every file in `src_dir` (sorted by name for deterministic ordering)
/// into a single archive at `out_path`. Entry names are the bare file names.
pub fn pack_directory(
    src_dir: &Path,
    out_path: &Path,
    format: ArchiveFormat,
) -> anyhow::Result<()> {
    let mut entries: Vec<(PathBuf, String)> = Vec::new();
    for de in fs::read_dir(src_dir).with_context(|| format!("read spool dir {src_dir:?}"))? {
        let path = de?.path();
        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                entries.push((path.clone(), name.to_string()));
            }
        }
    }
    entries.sort_by(|a, b| a.1.cmp(&b.1));

    let file = File::create(out_path).with_context(|| format!("create archive {out_path:?}"))?;

    match format {
        ArchiveFormat::Zip => write_zip(file, &entries),
        _ => write_tar(file, &entries, format),
    }
}

/// Append each spooled file into a tar builder over `inner`, returning `inner`
/// (with the tar trailer written).
fn append_tar<W: Write>(inner: W, entries: &[(PathBuf, String)]) -> anyhow::Result<W> {
    let mut builder = tar::Builder::new(inner);
    for (path, name) in entries {
        builder
            .append_path_with_name(path, name)
            .with_context(|| format!("add {name} to archive"))?;
    }
    Ok(builder.into_inner()?)
}

fn write_tar(
    file: File,
    entries: &[(PathBuf, String)],
    format: ArchiveFormat,
) -> anyhow::Result<()> {
    let buf = BufWriter::new(file);
    // Build the tar over the (optionally compressing) writer, then finalize the
    // compressor epilogue and flush the BufWriter down to the file.
    match format {
        ArchiveFormat::Tar => {
            let mut w = append_tar(buf, entries)?;
            w.flush()?;
        }
        ArchiveFormat::TarGz => {
            let enc = flate2::write::GzEncoder::new(buf, flate2::Compression::default());
            append_tar(enc, entries)?.finish()?.flush()?;
        }
        ArchiveFormat::TarZst => {
            let enc = zstd::stream::write::Encoder::new(buf, 3)?;
            append_tar(enc, entries)?.finish()?.flush()?;
        }
        ArchiveFormat::TarBz2 => {
            let enc = bzip2::write::BzEncoder::new(buf, bzip2::Compression::default());
            append_tar(enc, entries)?.finish()?.flush()?;
        }
        ArchiveFormat::TarXz => {
            let enc = xz2::write::XzEncoder::new(buf, 6);
            append_tar(enc, entries)?.finish()?.flush()?;
        }
        ArchiveFormat::Zip => unreachable!("zip handled separately"),
    }
    Ok(())
}

fn write_zip(file: File, entries: &[(PathBuf, String)]) -> anyhow::Result<()> {
    // ZipWriter needs Write + Seek; BufWriter<File> provides both.
    let mut zip = zip::ZipWriter::new(BufWriter::new(file));
    let opts = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    for (path, name) in entries {
        zip.start_file(name.as_str(), opts)
            .with_context(|| format!("add {name} to zip"))?;
        let mut src = BufReader::new(File::open(path)?);
        io::copy(&mut src, &mut zip)?;
    }
    zip.finish()?.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn spool_with(users: &[u8], orders: &[u8]) -> tempfile::TempDir {
        let d = tempfile::TempDir::new().unwrap();
        fs::write(d.path().join("users.sql"), users).unwrap();
        fs::write(d.path().join("orders.sql"), orders).unwrap();
        d
    }

    #[test]
    fn test_format_detection() {
        let cases = [
            ("x.tar.gz", Some(ArchiveFormat::TarGz)),
            ("x.tgz", Some(ArchiveFormat::TarGz)),
            ("x.tar.zst", Some(ArchiveFormat::TarZst)),
            ("x.tar.bz2", Some(ArchiveFormat::TarBz2)),
            ("x.tar.xz", Some(ArchiveFormat::TarXz)),
            ("x.tar", Some(ArchiveFormat::Tar)),
            ("x.zip", Some(ArchiveFormat::Zip)),
            ("output", None),
            ("tables.d", None),
        ];
        for (name, want) in cases {
            assert_eq!(
                ArchiveFormat::from_output_path(Path::new(name)),
                want,
                "{name}"
            );
        }
    }

    #[test]
    fn test_pack_tar_gz_roundtrip() {
        let spool = spool_with(b"CREATE TABLE users;\n", b"CREATE TABLE orders;\n");
        let out_dir = tempfile::TempDir::new().unwrap();
        let out = out_dir.path().join("a.tar.gz");
        pack_directory(spool.path(), &out, ArchiveFormat::TarGz).unwrap();

        let dec = flate2::read::GzDecoder::new(File::open(&out).unwrap());
        let mut ar = tar::Archive::new(dec);
        let mut found = std::collections::BTreeMap::new();
        for e in ar.entries().unwrap() {
            let mut e = e.unwrap();
            let name = e.path().unwrap().to_string_lossy().into_owned();
            let mut s = String::new();
            e.read_to_string(&mut s).unwrap();
            found.insert(name, s);
        }
        assert_eq!(found.get("users.sql").unwrap(), "CREATE TABLE users;\n");
        assert_eq!(found.get("orders.sql").unwrap(), "CREATE TABLE orders;\n");
    }

    #[test]
    fn test_pack_zip_roundtrip() {
        let spool = spool_with(b"CREATE TABLE users;\n", b"CREATE TABLE orders;\n");
        let out_dir = tempfile::TempDir::new().unwrap();
        let out = out_dir.path().join("a.zip");
        pack_directory(spool.path(), &out, ArchiveFormat::Zip).unwrap();

        let mut zip = zip::ZipArchive::new(File::open(&out).unwrap()).unwrap();
        let mut names: Vec<String> = (0..zip.len())
            .map(|i| zip.by_index(i).unwrap().name().to_string())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec!["orders.sql".to_string(), "users.sql".to_string()]
        );

        let mut s = String::new();
        zip.by_name("users.sql")
            .unwrap()
            .read_to_string(&mut s)
            .unwrap();
        assert_eq!(s, "CREATE TABLE users;\n");
    }
}

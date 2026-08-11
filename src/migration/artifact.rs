use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::Serialize;
use tempfile::NamedTempFile;
use thiserror::Error;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error("artifact path has no parent: {0}")]
    MissingParent(PathBuf),
    #[error("artifact parent is not a regular directory: {0}")]
    UnsafeParent(PathBuf),
    #[error("artifact is a symlink or non-regular file: {0}")]
    UnsafeTarget(PathBuf),
    #[error("artifact parent is not owned by the current user: {0}")]
    WrongParentOwner(PathBuf),
    #[error("artifact already exists: {0}")]
    AlreadyExists(PathBuf),
    #[error("artifact I/O failed")]
    Io(#[from] std::io::Error),
    #[error("artifact JSON failed")]
    Json(#[from] serde_json::Error),
}

/// Write a new protected JSON artifact and refuse to replace an existing path.
pub fn write_json_new<T: Serialize>(
    path: impl AsRef<Path>,
    value: &T,
) -> Result<(), ArtifactError> {
    publish_json(path.as_ref(), value, false)
}

/// Atomically replace a protected regular JSON artifact.
pub fn replace_json<T: Serialize>(path: impl AsRef<Path>, value: &T) -> Result<(), ArtifactError> {
    publish_json(path.as_ref(), value, true)
}

pub fn read_json<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<T, ArtifactError> {
    let path = path.as_ref();
    validate_existing_target(path)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options.open(path)?;
    if !file.metadata()?.is_file() {
        return Err(ArtifactError::UnsafeTarget(path.to_path_buf()));
    }
    Ok(serde_json::from_reader(BufReader::new(file))?)
}

fn publish_json<T: Serialize>(path: &Path, value: &T, replace: bool) -> Result<(), ArtifactError> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    validate_parent(parent)?;
    if path.exists() || fs::symlink_metadata(path).is_ok() {
        if !replace {
            return Err(ArtifactError::AlreadyExists(path.to_path_buf()));
        }
        validate_existing_target(path)?;
    }
    let mut temp = NamedTempFile::new_in(parent)?;
    #[cfg(unix)]
    temp.as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    {
        let mut writer = BufWriter::new(temp.as_file_mut());
        serde_json::to_writer_pretty(&mut writer, value)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
    }
    temp.as_file().sync_all()?;
    if replace {
        // Rename replaces the directory entry without following it. The earlier check
        // rejects ordinary unsafe targets and limits the remaining race to replacement.
        temp.persist(path)
            .map_err(|error| ArtifactError::Io(error.error))?;
    } else {
        temp.persist_noclobber(path).map_err(|error| {
            if error.error.kind() == std::io::ErrorKind::AlreadyExists {
                ArtifactError::AlreadyExists(path.to_path_buf())
            } else {
                ArtifactError::Io(error.error)
            }
        })?;
    }
    sync_directory(parent)?;
    Ok(())
}

fn validate_parent(parent: &Path) -> Result<(), ArtifactError> {
    let metadata = fs::symlink_metadata(parent).map_err(ArtifactError::Io)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ArtifactError::UnsafeParent(parent.to_path_buf()));
    }
    #[cfg(unix)]
    {
        // SAFETY: `geteuid` has no arguments and only reads the process credentials.
        let effective_uid = unsafe { libc::geteuid() };
        if metadata.uid() != effective_uid {
            return Err(ArtifactError::WrongParentOwner(parent.to_path_buf()));
        }
    }
    Ok(())
}

fn validate_existing_target(path: &Path) -> Result<(), ArtifactError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ArtifactError::UnsafeTarget(path.to_path_buf()));
    }
    Ok(())
}

fn sync_directory(parent: &Path) -> Result<(), ArtifactError> {
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn round_trip_and_no_clobber() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plan.json");
        write_json_new(&path, &vec![1, 2]).unwrap();
        assert_eq!(read_json::<Vec<u8>>(&path).unwrap(), vec![1, 2]);
        assert!(matches!(
            write_json_new(&path, &vec![3]),
            Err(ArtifactError::AlreadyExists(_))
        ));
    }
    #[cfg(unix)]
    #[test]
    fn output_is_private() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.json");
        write_json_new(&path, &1).unwrap();
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    #[cfg(unix)]
    #[test]
    fn symlink_is_rejected() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        fs::write(&real, "1").unwrap();
        let link = dir.path().join("link");
        symlink(&real, &link).unwrap();
        assert!(matches!(
            read_json::<u8>(&link),
            Err(ArtifactError::UnsafeTarget(_))
        ));
        assert!(matches!(
            replace_json(&link, &2),
            Err(ArtifactError::UnsafeTarget(_))
        ));
    }
}

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use thiserror::Error;

#[cfg(unix)]
#[cfg(unix)]
use std::os::fd::AsRawFd;
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
    #[error("cannot roll back partially published artifact: {path}")]
    PairRollbackFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

/// Write a new protected JSON artifact and refuse to replace an existing path.
pub fn write_json_new<T: Serialize>(
    path: impl AsRef<Path>,
    value: &T,
) -> Result<(), ArtifactError> {
    publish_json(path.as_ref(), value, false)
}

/// Write a new protected UTF-8 text artifact and refuse to replace an existing path.
pub fn write_text_new(path: impl AsRef<Path>, value: &str) -> Result<(), ArtifactError> {
    publish_bytes(path.as_ref(), value.as_bytes(), false)
}

/// Stage, sync, and publish one JSON artifact and its derived text report.
///
/// Both files are staged before either path becomes visible. If the second
/// no-clobber publication fails, the helper removes only the exact first file
/// created by this invocation and syncs its parent before returning the error.
pub fn write_json_text_pair_new<T: Serialize>(
    json_path: impl AsRef<Path>,
    value: &T,
    text_path: impl AsRef<Path>,
    text: &str,
) -> Result<(), ArtifactError> {
    let mut json = serde_json::to_vec_pretty(value)?;
    json.push(b'\n');
    publish_pair_new(
        json_path.as_ref(),
        &json,
        text_path.as_ref(),
        text.as_bytes(),
        || Ok(()),
        false,
    )
}

/// Recover an interrupted pair publication, then validate both final paths.
///
/// Call this before expensive source inspection. Recovery removes only files
/// whose device and inode match the durable transaction marker.
pub fn prepare_json_text_pair_new(
    json_path: impl AsRef<Path>,
    text_path: impl AsRef<Path>,
) -> Result<(), ArtifactError> {
    let json_path = json_path.as_ref();
    let text_path = text_path.as_ref();
    if json_path == text_path {
        return Err(ArtifactError::AlreadyExists(json_path.to_path_buf()));
    }
    #[cfg(not(unix))]
    return Err(ArtifactError::UnsafeTarget(json_path.to_path_buf()));
    #[cfg(unix)]
    {
        validate_pair_parent(json_path)?;
        validate_pair_parent(text_path)?;
    }
    recover_interrupted_pair(json_path, text_path)?;
    validate_new_artifact_path(json_path)?;
    validate_new_artifact_path(text_path)
}

/// Validate the parent and require that a new protected artifact path is unused.
///
/// Publication repeats these checks and still uses no-clobber creation, so this
/// function is only an early failure check for multi-artifact workflows.
pub fn validate_new_artifact_path(path: impl AsRef<Path>) -> Result<(), ArtifactError> {
    let path = path.as_ref();
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    validate_parent(parent)?;
    if path.exists() || fs::symlink_metadata(path).is_ok() {
        return Err(ArtifactError::AlreadyExists(path.to_path_buf()));
    }
    Ok(())
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
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    publish_bytes(path, &bytes, replace)
}

fn publish_bytes(path: &Path, value: &[u8], replace: bool) -> Result<(), ArtifactError> {
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
        writer.write_all(value)?;
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

struct StagedArtifact {
    temp: NamedTempFile,
    path: PathBuf,
    parent: PathBuf,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ArtifactIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PairTransaction {
    version: u8,
    first_temp_name: String,
    first_identity: ArtifactIdentity,
    second_temp_name: String,
    second_identity: ArtifactIdentity,
}

struct LockedPairTransaction {
    transaction: PairTransaction,
    identity: ArtifactIdentity,
    _file: File,
}

fn stage_new_artifact(path: &Path, value: &[u8]) -> Result<StagedArtifact, ArtifactError> {
    validate_new_artifact_path(path)?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temp = NamedTempFile::new_in(parent)?;
    #[cfg(unix)]
    temp.as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    {
        let mut writer = BufWriter::new(temp.as_file_mut());
        writer.write_all(value)?;
        writer.flush()?;
    }
    temp.as_file().sync_all()?;
    let metadata = temp.as_file().metadata()?;
    Ok(StagedArtifact {
        temp,
        path: path.to_path_buf(),
        parent: parent.to_path_buf(),
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
    })
}

fn publish_pair_new<F>(
    first_path: &Path,
    first_value: &[u8],
    second_path: &Path,
    second_value: &[u8],
    after_staging: F,
    interrupt_after_first: bool,
) -> Result<(), ArtifactError>
where
    F: FnOnce() -> Result<(), ArtifactError>,
{
    prepare_json_text_pair_new(first_path, second_path)?;
    let first = stage_new_artifact(first_path, first_value)?;
    let second = stage_new_artifact(second_path, second_value)?;
    after_staging()?;

    let marker_path = pair_marker_path(first_path, second_path);
    let transaction = PairTransaction {
        version: 1,
        first_temp_name: staged_file_name(&first)?,
        first_identity: staged_identity(&first),
        second_temp_name: staged_file_name(&second)?,
        second_identity: staged_identity(&second),
    };
    write_json_new(&marker_path, &transaction)?;
    let marker = read_pair_transaction(&marker_path)?;
    let marker_identity = marker.identity;

    let first_parent = first.parent.clone();
    let first_identity = staged_identity(&first);
    let first_path = first.path.clone();
    first.temp.persist_noclobber(&first_path).map_err(|error| {
        if error.error.kind() == std::io::ErrorKind::AlreadyExists {
            ArtifactError::AlreadyExists(first_path.clone())
        } else {
            ArtifactError::Io(error.error)
        }
    })?;
    if interrupt_after_first {
        return Err(ArtifactError::Io(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "injected interruption after first pair member",
        )));
    }

    let second_parent = second.parent.clone();
    let second_identity = staged_identity(&second);
    let second_path = second.path.clone();
    if let Err(error) = second.temp.persist_noclobber(&second_path) {
        rollback_published_pair_member(&first_path, first_identity)?;
        sync_directory(&first_parent)?;
        remove_pair_marker(&marker_path, marker_identity)?;
        return Err(if error.error.kind() == std::io::ErrorKind::AlreadyExists {
            ArtifactError::AlreadyExists(second_path)
        } else {
            ArtifactError::Io(error.error)
        });
    }

    if let Err(error) = sync_directory(&first_parent).and_then(|()| {
        if second_parent == first_parent {
            Ok(())
        } else {
            sync_directory(&second_parent)
        }
    }) {
        rollback_published_pair_member(&second_path, second_identity)?;
        rollback_published_pair_member(&first_path, first_identity)?;
        sync_directory(&first_parent)?;
        if second_parent != first_parent {
            sync_directory(&second_parent)?;
        }
        remove_pair_marker(&marker_path, marker_identity)?;
        return Err(error);
    }
    remove_pair_marker(&marker_path, marker_identity)?;
    Ok(())
}

fn rollback_published_pair_member(
    path: &Path,
    expected_identity: ArtifactIdentity,
) -> Result<(), ArtifactError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|source| ArtifactError::PairRollbackFailed {
            path: path.to_path_buf(),
            source,
        })?;
    #[cfg(unix)]
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.dev() != expected_identity.device
        || metadata.ino() != expected_identity.inode
    {
        return Err(ArtifactError::UnsafeTarget(path.to_path_buf()));
    }
    #[cfg(not(unix))]
    if !metadata.is_file() {
        return Err(ArtifactError::UnsafeTarget(path.to_path_buf()));
    }
    fs::remove_file(path).map_err(|source| ArtifactError::PairRollbackFailed {
        path: path.to_path_buf(),
        source,
    })
}

fn staged_identity(staged: &StagedArtifact) -> ArtifactIdentity {
    ArtifactIdentity {
        #[cfg(unix)]
        device: staged.device,
        #[cfg(unix)]
        inode: staged.inode,
    }
}

fn staged_file_name(staged: &StagedArtifact) -> Result<String, ArtifactError> {
    staged
        .temp
        .path()
        .file_name()
        .and_then(|name| name.to_str())
        .map(str::to_owned)
        .ok_or_else(|| ArtifactError::UnsafeTarget(staged.temp.path().to_path_buf()))
}

fn pair_marker_path(first_path: &Path, second_path: &Path) -> PathBuf {
    let mut digest = Sha256::new();
    for path in [first_path, second_path] {
        let bytes = path.as_os_str().as_encoded_bytes();
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    let parent = first_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    parent.join(format!(
        ".sql-splitter-pair-{}.json",
        hex::encode(digest.finalize())
    ))
}

fn recover_interrupted_pair(first_path: &Path, second_path: &Path) -> Result<(), ArtifactError> {
    let marker_path = pair_marker_path(first_path, second_path);
    if fs::symlink_metadata(&marker_path).is_err() {
        return Ok(());
    }
    let marker = read_pair_transaction(&marker_path)?;
    let transaction = &marker.transaction;
    let marker_identity = marker.identity;
    if transaction.version != 1 {
        return Err(ArtifactError::UnsafeTarget(marker_path));
    }
    validate_staged_file_name(&transaction.first_temp_name, &marker_path)?;
    validate_staged_file_name(&transaction.second_temp_name, &marker_path)?;
    let first_parent = first_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let second_parent = second_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    remove_if_identity_matches(first_path, transaction.first_identity)?;
    remove_if_identity_matches(second_path, transaction.second_identity)?;
    remove_if_identity_matches(
        &first_parent.join(&transaction.first_temp_name),
        transaction.first_identity,
    )?;
    remove_if_identity_matches(
        &second_parent.join(&transaction.second_temp_name),
        transaction.second_identity,
    )?;
    remove_pair_marker(&marker_path, marker_identity)?;
    sync_directory(first_parent)?;
    if second_parent != first_parent {
        sync_directory(second_parent)?;
    }
    Ok(())
}

fn remove_if_identity_matches(
    path: &Path,
    expected_identity: ArtifactIdentity,
) -> Result<(), ArtifactError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(ArtifactError::Io(error)),
    };
    #[cfg(unix)]
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.dev() != expected_identity.device
        || metadata.ino() != expected_identity.inode
    {
        return Ok(());
    }
    #[cfg(not(unix))]
    if !metadata.is_file() {
        return Ok(());
    }
    fs::remove_file(path)?;
    Ok(())
}

fn remove_pair_marker(
    marker_path: &Path,
    marker_identity: ArtifactIdentity,
) -> Result<(), ArtifactError> {
    rollback_published_pair_member(marker_path, marker_identity)?;
    let parent = marker_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    sync_directory(parent)
}

#[cfg(unix)]
fn validate_pair_parent(path: &Path) -> Result<(), ArtifactError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    validate_parent(parent)?;
    if fs::symlink_metadata(parent)?.mode() & 0o022 != 0 {
        return Err(ArtifactError::UnsafeParent(parent.to_path_buf()));
    }
    Ok(())
}

fn validate_staged_file_name(name: &str, marker_path: &Path) -> Result<(), ArtifactError> {
    let mut components = Path::new(name).components();
    if matches!(components.next(), Some(std::path::Component::Normal(_)))
        && components.next().is_none()
    {
        Ok(())
    } else {
        Err(ArtifactError::UnsafeTarget(marker_path.to_path_buf()))
    }
}

#[cfg(unix)]
fn read_pair_transaction(marker_path: &Path) -> Result<LockedPairTransaction, ArtifactError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options.open(marker_path)?;
    // SAFETY: `flock` operates on this owned descriptor. The lock remains held
    // until `LockedPairTransaction` is dropped.
    if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::WouldBlock {
            return Err(ArtifactError::AlreadyExists(marker_path.to_path_buf()));
        }
        return Err(ArtifactError::Io(error));
    }
    let metadata = file.metadata()?;
    // SAFETY: `geteuid` has no arguments and only reads the process credentials.
    let effective_uid = unsafe { libc::geteuid() };
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != effective_uid
        || metadata.mode() & 0o777 != 0o600
    {
        return Err(ArtifactError::UnsafeTarget(marker_path.to_path_buf()));
    }
    let identity = ArtifactIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    };
    let transaction = serde_json::from_reader(BufReader::new(&file))?;
    Ok(LockedPairTransaction {
        transaction,
        identity,
        _file: file,
    })
}

#[cfg(not(unix))]
fn read_pair_transaction(marker_path: &Path) -> Result<LockedPairTransaction, ArtifactError> {
    Err(ArtifactError::UnsafeTarget(marker_path.to_path_buf()))
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

    #[test]
    fn protected_text_is_exact_and_not_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("assessment.md");
        write_text_new(&path, "# Assessment\n").unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "# Assessment\n");
        assert!(matches!(
            write_text_new(&path, "replacement"),
            Err(ArtifactError::AlreadyExists(_))
        ));
        #[cfg(unix)]
        assert_eq!(
            fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn pair_publication_rolls_back_first_file_when_second_loses_race() {
        let dir = tempfile::tempdir().unwrap();
        let json_path = dir.path().join("assessment.json");
        let report_path = dir.path().join("assessment.md");

        let error = publish_pair_new(
            &json_path,
            br#"{"version":1}"#,
            &report_path,
            b"# Assessment\n",
            || {
                write_text_new(&report_path, "competing report\n")?;
                Ok(())
            },
            false,
        )
        .unwrap_err();

        assert!(matches!(error, ArtifactError::AlreadyExists(path) if path == report_path));
        assert!(!json_path.exists());
        assert_eq!(
            fs::read_to_string(&report_path).unwrap(),
            "competing report\n"
        );

        fs::remove_file(&report_path).unwrap();
        write_json_text_pair_new(&json_path, &vec![1_u8, 2], &report_path, "retry\n").unwrap();
        assert_eq!(read_json::<Vec<u8>>(&json_path).unwrap(), vec![1, 2]);
        assert_eq!(fs::read_to_string(&report_path).unwrap(), "retry\n");
    }

    #[test]
    fn interrupted_pair_publication_is_recovered_before_retry() {
        let dir = tempfile::tempdir().unwrap();
        let json_path = dir.path().join("assessment.json");
        let report_path = dir.path().join("assessment.md");

        let error = publish_pair_new(
            &json_path,
            br#"{"version":1}"#,
            &report_path,
            b"# Assessment\n",
            || Ok(()),
            true,
        )
        .unwrap_err();
        assert!(matches!(error, ArtifactError::Io(_)));
        assert!(json_path.exists());
        assert!(!report_path.exists());
        assert!(pair_marker_path(&json_path, &report_path).exists());

        prepare_json_text_pair_new(&json_path, &report_path).unwrap();
        assert!(!json_path.exists());
        assert!(!report_path.exists());
        assert!(!pair_marker_path(&json_path, &report_path).exists());

        write_json_text_pair_new(&json_path, &vec![1_u8, 2], &report_path, "retry\n").unwrap();
        assert_eq!(read_json::<Vec<u8>>(&json_path).unwrap(), vec![1, 2]);
        assert_eq!(fs::read_to_string(&report_path).unwrap(), "retry\n");
    }

    #[cfg(unix)]
    #[test]
    fn forged_pair_marker_cannot_escape_the_output_parent() {
        let dir = tempfile::tempdir().unwrap();
        let output_dir = dir.path().join("outputs");
        fs::create_dir(&output_dir).unwrap();
        let valuable = dir.path().join("valuable");
        fs::write(&valuable, "keep").unwrap();
        let metadata = fs::metadata(&valuable).unwrap();
        let identity = ArtifactIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        let json_path = output_dir.join("assessment.json");
        let report_path = output_dir.join("assessment.md");
        let marker_path = pair_marker_path(&json_path, &report_path);
        write_json_new(
            &marker_path,
            &PairTransaction {
                version: 1,
                first_temp_name: "../valuable".into(),
                first_identity: identity,
                second_temp_name: "safe-temp".into(),
                second_identity: identity,
            },
        )
        .unwrap();

        assert!(matches!(
            prepare_json_text_pair_new(&json_path, &report_path),
            Err(ArtifactError::UnsafeTarget(path)) if path == marker_path
        ));
        assert_eq!(fs::read_to_string(valuable).unwrap(), "keep");
    }

    #[cfg(unix)]
    #[test]
    fn pair_recovery_rejects_a_non_private_marker() {
        let dir = tempfile::tempdir().unwrap();
        let json_path = dir.path().join("assessment.json");
        let report_path = dir.path().join("assessment.md");
        let marker_path = pair_marker_path(&json_path, &report_path);
        write_json_new(
            &marker_path,
            &PairTransaction {
                version: 1,
                first_temp_name: "first-temp".into(),
                first_identity: ArtifactIdentity {
                    device: 0,
                    inode: 0,
                },
                second_temp_name: "second-temp".into(),
                second_identity: ArtifactIdentity {
                    device: 0,
                    inode: 0,
                },
            },
        )
        .unwrap();
        fs::set_permissions(&marker_path, fs::Permissions::from_mode(0o644)).unwrap();

        assert!(matches!(
            prepare_json_text_pair_new(&json_path, &report_path),
            Err(ArtifactError::UnsafeTarget(path)) if path == marker_path
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

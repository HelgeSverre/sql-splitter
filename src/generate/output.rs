//! Protected family state, spooling, and atomic publication.
//!
//! Generation can produce more state than fits in memory (a family's buffered
//! child rows) and more than one output that must land together (rendered SQL
//! plus an emitted model). This module supplies the security-critical
//! primitives for both:
//!
//! * [`ProtectedSpool`] — an owner-only (`0600`) temporary file with a
//!   cryptographically unpredictable name, used to spill buffered rows. Its
//!   name is never printed at normal verbosity because the name alone can leak
//!   workflow details, and the file is removed when the spool is dropped
//!   (success, ordinary failure, or a handled interruption all run `Drop`).
//! * [`SpoolWriter`]/[`SpoolReader`]/[`SpooledRow`] — a length-prefixed, typed
//!   record format with a version byte. The reader rejects a version mismatch
//!   and an oversized length prefix *before* allocating, so a truncated or
//!   hostile spool cannot drive a huge allocation.
//! * [`AtomicOutput`] and [`PublicationSet`] — write beside the destination
//!   into a protected temp file and `rename` into place only *after* success.
//!   An existing destination is never truncated before the new content is fully
//!   written and verified; on Unix the published file follows normal
//!   output-file permissions (an existing destination's mode is preserved; a new
//!   destination follows the umask-adjusted default) while the temp stayed
//!   owner-only throughout.
//! * [`FamilyBuffer`]/[`FamilyState`]/[`FamilyBudget`] — an exact memory budget
//!   for a correlated table family. The buffer keeps rows in memory only while
//!   under budget and spills deterministically to a [`ProtectedSpool`] the
//!   moment a push would cross it, so it never retains every child row in an
//!   unbounded `Vec`.
//! * [`CancellationToken`] and [`install_interrupt_handler`] — an atomic
//!   cancellation flag that generation/verification loops check and turn into a
//!   cancellation error, backed by a best-effort process-level Ctrl-C handler.
//!
//! # Durability caveat
//!
//! Cleanup is guard-based: it depends on `Drop` running. A normal return, a
//! propagated `Err`, and a handled interruption all unwind and remove the
//! protected files. `SIGKILL`, a power loss, a machine failure, or a
//! `panic = "abort"` build can still leave a protected temp file behind — there
//! is no crash-proof cleanup, only owner-only permissions limiting exposure.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Once};

use rand::random;

use super::value::{GenerateError, GeneratedValue};

// --- Spool record framing constants -----------------------------------------

/// The spool record format version. Bumping it invalidates old spools, which is
/// safe: spools never outlive the run that wrote them.
const SPOOL_VERSION: u8 = 1;

/// The largest single spool record the reader will accept. A length prefix
/// larger than this is refused before any allocation.
const MAX_RECORD_BYTES: usize = 64 * 1024 * 1024;

/// The largest single text/bytes field the reader will accept, refused before
/// allocation.
const MAX_FIELD_BYTES: usize = 32 * 1024 * 1024;

/// The largest number of fields a single record may declare, refused before
/// allocation.
const MAX_FIELDS: u32 = 65_536;

// --- Cancellation ------------------------------------------------------------

/// Set by the process-level interrupt handler; observed by every
/// [`CancellationToken`]. A `static` (not an `Arc`) because a signal handler can
/// only touch `'static` data.
static PROCESS_INTERRUPTED: AtomicBool = AtomicBool::new(false);

/// Guards a single install of the interrupt handler.
static INSTALL_HANDLER: Once = Once::new();

/// The C signal handler: an async-signal-safe atomic store and nothing else.
#[cfg(unix)]
extern "C" fn on_sigint(_signal: libc::c_int) {
    PROCESS_INTERRUPTED.store(true, Ordering::SeqCst);
}

/// Install the process-level Ctrl-C (SIGINT) handler exactly once.
///
/// The handler only flips an atomic flag every generation/verification loop
/// already polls through a [`CancellationToken`], so the actual signal wiring
/// stays minimal. On non-Unix targets this is a no-op and cancellation must be
/// driven explicitly (e.g. [`CancellationToken::cancel`]); the atomic-flag
/// mechanism itself is fully portable and testable regardless.
pub fn install_interrupt_handler() {
    INSTALL_HANDLER.call_once(|| {
        #[cfg(unix)]
        // SAFETY: `on_sigint` performs only an async-signal-safe atomic store,
        // so installing it as a SIGINT handler is sound. The previous handler
        // returned by `signal` is intentionally discarded: install is
        // best-effort and this process owns SIGINT for the run's duration.
        unsafe {
            libc::signal(libc::SIGINT, on_sigint as *const () as libc::sighandler_t);
        }
    });
}

/// An atomic cancellation flag that generation/verification loops poll.
///
/// Each token owns its own flag (so tests never perturb one another), and every
/// token additionally observes the process-level interrupt flag set by the
/// installed Ctrl-C handler. A loop calls [`check`](Self::check) each iteration
/// and returns the resulting error so registered spool/output guards clean up
/// as the stack unwinds.
#[derive(Clone, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    /// A fresh token, not yet cancelled.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request cancellation. Used by tests and by any caller that decides to
    /// abort; the installed signal handler flips the process-level flag every
    /// token also observes.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    /// Whether this token (or the process) has been cancelled.
    ///
    /// The flag is a single-writer, single-bit latch (never un-set) used only to
    /// decide whether to stop, so a `Relaxed` load on the polling path is
    /// sufficient — no other memory is published through it. `cancel` stores with
    /// `SeqCst` for an unambiguous happens-before with any later observer.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed) || PROCESS_INTERRUPTED.load(Ordering::Relaxed)
    }

    /// `Ok(())` while running, or a `GEN-CANCELLED` error once cancelled — the
    /// value a loop returns so its guards run.
    pub fn check(&self) -> Result<(), GenerateError> {
        if self.is_cancelled() {
            Err(GenerateError::diagnostic(
                &crate::diagnostic::codes::CANCELLED,
                "runtime",
                "generation was interrupted",
            ))
        } else {
            Ok(())
        }
    }
}

// --- Temp directory + protected file creation --------------------------------

/// Where protected spool files are created.
///
/// Spools are never renamed into a destination, so by default they live in the
/// OS temp directory; a caller may pin a specific directory (e.g. a fast local
/// scratch volume). [`AtomicOutput`], by contrast, always writes beside its
/// destination because atomic `rename` requires the same filesystem.
#[derive(Debug, Clone, Default)]
pub struct TempConfig {
    dir: Option<PathBuf>,
}

impl TempConfig {
    /// Spool into `dir` instead of the OS temp directory.
    pub fn in_dir(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: Some(dir.into()),
        }
    }

    /// The directory protected spools are created in.
    fn dir(&self) -> PathBuf {
        self.dir.clone().unwrap_or_else(std::env::temp_dir)
    }
}

/// A cryptographically unpredictable 256-bit file name with the given prefix.
///
/// The name is unguessable so a concurrent local attacker cannot pre-create or
/// race the protected file, and it is deliberately opaque so nothing about the
/// workflow leaks through it.
fn random_name(prefix: &str) -> String {
    let a: u64 = random();
    let b: u64 = random();
    let c: u64 = random();
    let d: u64 = random();
    format!("{prefix}{a:016x}{b:016x}{c:016x}{d:016x}.tmp")
}

/// Create a fresh, exclusively-owned file in `dir`, returning it with its path.
///
/// Uses `create_new(true)` (`O_EXCL`) so an existing name is never reused, and
/// on Unix forces mode `0600` — both at `open` time and with an explicit
/// `set_permissions` afterward, so the file is owner-only regardless of the
/// ambient umask. Retries a handful of times if the random name collides.
fn create_protected_in(dir: &Path, prefix: &str) -> io::Result<(File, PathBuf)> {
    for _ in 0..16 {
        let path = dir.join(random_name(prefix));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => {
                #[cfg(unix)]
                {
                    // Force exactly 0600 even under a restrictive umask that
                    // would otherwise clear owner bits from the open() mode.
                    use std::os::unix::fs::PermissionsExt;
                    fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
                }
                return Ok((file, path));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not create a unique protected temporary file",
    ))
}

// --- Protected spool ---------------------------------------------------------

/// An owner-only temporary file used to spill buffered rows, removed on drop.
///
/// The backing file is created with [`create_protected_in`] (mode `0600`,
/// unpredictable name). Append rows with [`write_row`](Self::write_row), then
/// [`rewind`](Self::rewind) to read them back. Dropping the spool removes the
/// file; see the module docs for the SIGKILL/power-loss caveat.
///
/// Writes go through one persistent [`BufWriter`] and reuse a single scratch
/// buffer for the whole spool lifetime, so spilling a large family costs no
/// per-row writer allocation and does not flush per row — the buffer is flushed
/// once on [`rewind`](Self::rewind) (or explicitly via [`flush`](Self::flush)).
pub struct ProtectedSpool {
    file: BufWriter<File>,
    path: PathBuf,
    scratch: Vec<u8>,
}

impl ProtectedSpool {
    /// Create a protected spool in `temp`'s directory.
    pub fn create(temp: &TempConfig) -> io::Result<Self> {
        let (file, path) = create_protected_in(&temp.dir(), ".sqlspl-spool-")?;
        Ok(Self {
            file: BufWriter::new(file),
            path,
            scratch: Vec::new(),
        })
    }

    /// The spool's backing path. Callers must not print this at normal
    /// verbosity — the name can leak workflow details.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append one length-prefixed record, reusing the spool's persistent
    /// buffered writer and scratch buffer (no per-row allocation, no per-row
    /// flush).
    pub fn write_row(&mut self, row: &SpooledRow) -> io::Result<()> {
        write_record(&mut self.file, &mut self.scratch, row)
    }

    /// Flush buffered writes to the backing file.
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    /// Flush buffered writes, seek to the start, and return a reader over the
    /// spooled records.
    pub fn rewind(&mut self) -> io::Result<SpoolReader<&mut File>> {
        self.file.flush()?;
        let file = self.file.get_mut();
        file.rewind()?;
        Ok(SpoolReader::new(file))
    }
}

impl Drop for ProtectedSpool {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

// --- Spool records -----------------------------------------------------------

/// One spooled row: its owning table id, its row index, and its column values.
#[derive(Debug, Clone, PartialEq)]
pub struct SpooledRow {
    /// Index of the owning table within the plan's `tables`.
    pub table_id: u32,
    /// Zero-based index of this row within its table.
    pub row_index: u64,
    /// Column values, positionally aligned with the table's columns.
    pub values: Vec<GeneratedValue>,
}

impl SpooledRow {
    /// A conservative estimate of this row's in-memory footprint, used to keep a
    /// [`FamilyBuffer`] inside its byte budget.
    pub fn estimated_bytes(&self) -> u64 {
        // table_id + row_index + field count, plus each field's payload.
        let header = 16u64;
        header + self.values.iter().map(value_bytes).sum::<u64>()
    }

    /// Encode the record body (everything after the version+length frame).
    fn encode_body(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.table_id.to_le_bytes());
        out.extend_from_slice(&self.row_index.to_le_bytes());
        let count = self.values.len() as u32;
        out.extend_from_slice(&count.to_le_bytes());
        for value in &self.values {
            encode_value(value, out);
        }
    }

    /// Decode a record body, rejecting oversized field counts and lengths
    /// before allocating.
    fn decode_body(body: &[u8]) -> io::Result<Self> {
        let mut cursor = io::Cursor::new(body);
        let table_id = read_u32(&mut cursor)?;
        let row_index = read_u64(&mut cursor)?;
        let count = read_u32(&mut cursor)?;
        if count > MAX_FIELDS {
            return Err(invalid_data("spool record declares too many fields"));
        }
        let mut values = Vec::with_capacity(count as usize);
        for _ in 0..count {
            values.push(decode_value(&mut cursor)?);
        }
        Ok(Self {
            table_id,
            row_index,
            values,
        })
    }
}

/// The heap+inline footprint estimate for one value.
fn value_bytes(value: &GeneratedValue) -> u64 {
    match value {
        GeneratedValue::Null | GeneratedValue::Default | GeneratedValue::Boolean(_) => 1,
        GeneratedValue::Integer(_) => 16,
        GeneratedValue::Decimal { .. } => 20,
        GeneratedValue::Text(text)
        | GeneratedValue::DateTime(text)
        | GeneratedValue::Json(text) => text.len() as u64 + 8,
        GeneratedValue::Bytes(bytes) => bytes.len() as u64 + 8,
    }
}

/// Field tag bytes, one per [`GeneratedValue`] variant.
mod tag {
    pub const NULL: u8 = 0;
    pub const DEFAULT: u8 = 1;
    pub const BOOLEAN: u8 = 2;
    pub const INTEGER: u8 = 3;
    pub const DECIMAL: u8 = 4;
    pub const TEXT: u8 = 5;
    pub const BYTES: u8 = 6;
    pub const DATETIME: u8 = 7;
    pub const JSON: u8 = 8;
}

fn encode_value(value: &GeneratedValue, out: &mut Vec<u8>) {
    match value {
        GeneratedValue::Null => out.push(tag::NULL),
        GeneratedValue::Default => out.push(tag::DEFAULT),
        GeneratedValue::Boolean(flag) => {
            out.push(tag::BOOLEAN);
            out.push(u8::from(*flag));
        }
        GeneratedValue::Integer(value) => {
            out.push(tag::INTEGER);
            out.extend_from_slice(&value.to_le_bytes());
        }
        GeneratedValue::Decimal { minor, scale } => {
            out.push(tag::DECIMAL);
            out.extend_from_slice(&minor.to_le_bytes());
            out.extend_from_slice(&scale.to_le_bytes());
        }
        GeneratedValue::Text(text) => encode_len_prefixed(tag::TEXT, text.as_bytes(), out),
        GeneratedValue::Bytes(bytes) => encode_len_prefixed(tag::BYTES, bytes, out),
        GeneratedValue::DateTime(text) => encode_len_prefixed(tag::DATETIME, text.as_bytes(), out),
        GeneratedValue::Json(text) => encode_len_prefixed(tag::JSON, text.as_bytes(), out),
    }
}

fn encode_len_prefixed(tag: u8, bytes: &[u8], out: &mut Vec<u8>) {
    out.push(tag);
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn decode_value(cursor: &mut io::Cursor<&[u8]>) -> io::Result<GeneratedValue> {
    let tag = read_u8(cursor)?;
    let value = match tag {
        tag::NULL => GeneratedValue::Null,
        tag::DEFAULT => GeneratedValue::Default,
        tag::BOOLEAN => GeneratedValue::Boolean(read_u8(cursor)? != 0),
        tag::INTEGER => GeneratedValue::Integer(read_i128(cursor)?),
        tag::DECIMAL => GeneratedValue::Decimal {
            minor: read_i128(cursor)?,
            scale: read_u32(cursor)?,
        },
        tag::TEXT => GeneratedValue::Text(read_string(cursor)?),
        tag::BYTES => GeneratedValue::Bytes(read_bytes(cursor)?),
        tag::DATETIME => GeneratedValue::DateTime(read_string(cursor)?),
        tag::JSON => GeneratedValue::Json(read_string(cursor)?),
        other => return Err(invalid_data(&format!("unknown spool field tag {other}"))),
    };
    Ok(value)
}

/// Read a length-prefixed byte field, rejecting an oversized length before
/// allocating the buffer.
fn read_bytes(cursor: &mut io::Cursor<&[u8]>) -> io::Result<Vec<u8>> {
    let len = read_u32(cursor)? as usize;
    if len > MAX_FIELD_BYTES {
        return Err(invalid_data("spool field length exceeds the maximum"));
    }
    let mut buffer = vec![0u8; len];
    cursor.read_exact(&mut buffer)?;
    Ok(buffer)
}

fn read_string(cursor: &mut io::Cursor<&[u8]>) -> io::Result<String> {
    let bytes = read_bytes(cursor)?;
    String::from_utf8(bytes).map_err(|_| invalid_data("spool text field is not valid UTF-8"))
}

fn read_u8(cursor: &mut io::Cursor<&[u8]>) -> io::Result<u8> {
    let mut buffer = [0u8; 1];
    cursor.read_exact(&mut buffer)?;
    Ok(buffer[0])
}

fn read_u32(cursor: &mut io::Cursor<&[u8]>) -> io::Result<u32> {
    let mut buffer = [0u8; 4];
    cursor.read_exact(&mut buffer)?;
    Ok(u32::from_le_bytes(buffer))
}

fn read_u64(cursor: &mut io::Cursor<&[u8]>) -> io::Result<u64> {
    let mut buffer = [0u8; 8];
    cursor.read_exact(&mut buffer)?;
    Ok(u64::from_le_bytes(buffer))
}

fn read_i128(cursor: &mut io::Cursor<&[u8]>) -> io::Result<i128> {
    let mut buffer = [0u8; 16];
    cursor.read_exact(&mut buffer)?;
    Ok(i128::from_le_bytes(buffer))
}

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

/// Encode `row` into `scratch` and write one framed record — a version byte, a
/// `u32` body length, then the body — to `writer`. `scratch` is reused across
/// calls so a hot spooling loop never reallocates the encode buffer.
fn write_record(
    writer: &mut impl Write,
    scratch: &mut Vec<u8>,
    row: &SpooledRow,
) -> io::Result<()> {
    scratch.clear();
    row.encode_body(scratch);
    let len = u32::try_from(scratch.len())
        .map_err(|_| invalid_data("spool record exceeds the maximum size"))?;
    if scratch.len() > MAX_RECORD_BYTES {
        return Err(invalid_data("spool record exceeds the maximum size"));
    }
    writer.write_all(&[SPOOL_VERSION])?;
    writer.write_all(&len.to_le_bytes())?;
    writer.write_all(scratch)?;
    Ok(())
}

/// Writes [`SpooledRow`]s as version-tagged, length-prefixed records to any
/// [`Write`]. [`ProtectedSpool`] has its own buffered [`write_row`] for hot
/// spilling; this stand-alone writer is for one-off or caller-owned sinks.
pub struct SpoolWriter<W> {
    inner: W,
    scratch: Vec<u8>,
}

impl<W: Write> SpoolWriter<W> {
    /// Wrap a writer.
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            scratch: Vec::new(),
        }
    }

    /// Append one record: a version byte, a `u32` body length, then the body.
    pub fn write_row(&mut self, row: &SpooledRow) -> io::Result<()> {
        write_record(&mut self.inner, &mut self.scratch, row)
    }

    /// Flush the underlying writer.
    pub fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Reads [`SpooledRow`]s written by a [`SpoolWriter`], rejecting a version
/// mismatch and an oversized length prefix before allocating.
pub struct SpoolReader<R> {
    inner: R,
}

impl<R: Read> SpoolReader<R> {
    /// Wrap a reader positioned at a record boundary.
    pub fn new(inner: R) -> Self {
        Self { inner }
    }

    /// The next record, or `None` at a clean end of stream.
    pub fn read_row(&mut self) -> io::Result<Option<SpooledRow>> {
        let mut version = [0u8; 1];
        // A clean EOF at a record boundary ends the stream.
        if !read_one(&mut self.inner, &mut version)? {
            return Ok(None);
        }
        // Reject a version mismatch before reading or allocating the body.
        if version[0] != SPOOL_VERSION {
            return Err(invalid_data(&format!(
                "spool version mismatch: expected {SPOOL_VERSION}, found {}",
                version[0]
            )));
        }
        let mut len_bytes = [0u8; 4];
        self.inner.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes) as usize;
        // Reject an oversized length before allocating the record buffer.
        if len > MAX_RECORD_BYTES {
            return Err(invalid_data("spool record length exceeds the maximum"));
        }
        let mut body = vec![0u8; len];
        self.inner.read_exact(&mut body)?;
        Ok(Some(SpooledRow::decode_body(&body)?))
    }
}

/// Read exactly one byte, returning `Ok(false)` at an immediate EOF and
/// retrying on an interrupted read.
fn read_one(reader: &mut impl Read, buffer: &mut [u8; 1]) -> io::Result<bool> {
    loop {
        match reader.read(buffer) {
            Ok(0) => return Ok(false),
            Ok(_) => return Ok(true),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
}

// --- Atomic output -----------------------------------------------------------

/// Write beside a destination and `rename` into place only after success.
///
/// The content is written into a protected (`0600`, unpredictable name) temp
/// file in the *destination's directory* — the same filesystem, so the final
/// `rename` is atomic. The destination is never opened for truncation: it keeps
/// its original bytes until [`commit`](Self::commit) renames the finished temp
/// over it. On Unix the published file follows normal output-file permissions
/// (an existing destination's mode is preserved; a new destination follows the
/// umask-adjusted default), while the temp stayed owner-only throughout. If the
/// output is dropped without committing, the temp file is removed and the
/// destination is left untouched.
pub struct AtomicOutput {
    destination: PathBuf,
    temp_path: PathBuf,
    file: Option<File>,
    /// The Unix mode to apply to the temp file immediately before rename.
    preserved_mode: Option<u32>,
    committed: bool,
}

impl AtomicOutput {
    /// Stage a protected temp output beside `destination`.
    pub fn create(destination: impl AsRef<Path>) -> io::Result<Self> {
        let destination = destination.as_ref().to_path_buf();
        let dir = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
        let (file, temp_path) = create_protected_in(&dir, ".sqlspl-out-")?;
        let preserved_mode = published_mode(&destination, &dir)?;
        Ok(Self {
            destination,
            temp_path,
            file: Some(file),
            preserved_mode,
            committed: false,
        })
    }

    /// The protected temp file to write generated content into.
    pub fn writer(&mut self) -> &mut File {
        self.file
            .as_mut()
            .expect("AtomicOutput::writer after commit")
    }

    /// The protected temp path (owner-only while being written).
    pub fn temp_path(&self) -> &Path {
        &self.temp_path
    }

    /// The final destination path.
    pub fn destination(&self) -> &Path {
        &self.destination
    }

    /// Flush and fsync the temp file, apply the published mode, then atomically
    /// rename it over the destination and fsync the destination's directory so
    /// the rename itself is durable. Only after this returns `Ok` does the
    /// destination change.
    ///
    /// Atomicity note: `rename(2)` is atomic on Unix (same filesystem, which is
    /// guaranteed since the temp is a sibling of the destination). On Windows
    /// `fs::rename` over an existing file is not atomic; this module is
    /// Unix-first.
    pub fn commit(mut self) -> io::Result<()> {
        let mut file = self
            .file
            .take()
            .ok_or_else(|| invalid_data("AtomicOutput already committed"))?;
        file.flush()?;
        // Durability of the file contents before the rename that publishes them.
        file.sync_all()?;
        drop(file);
        apply_published_mode(&self.temp_path, self.preserved_mode)?;
        fs::rename(&self.temp_path, &self.destination)?;
        // Durability of the rename itself: without a directory fsync a commit
        // that returned `Ok` could still lose the published directory entry on
        // power loss right after the rename. Best-effort — some filesystems do
        // not support a directory fsync.
        sync_parent_dir(&self.destination);
        self.committed = true;
        Ok(())
    }
}

impl Drop for AtomicOutput {
    fn drop(&mut self) {
        if !self.committed {
            let _ = fs::remove_file(&self.temp_path);
        }
    }
}

/// The mode the published file should carry: an existing destination's own mode
/// (preserved), or — for a new destination — the umask-adjusted mode a normal
/// output file would get. `None` off Unix, where no mode is applied.
fn published_mode(destination: &Path, dir: &Path) -> io::Result<Option<u32>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Ok(metadata) = fs::metadata(destination) {
            return Ok(Some(metadata.mode() & 0o777));
        }
        // New destination: probe the umask-adjusted default with a normal
        // create, read its mode, then remove the probe.
        let probe = dir.join(random_name(".sqlspl-probe-"));
        let file = File::create(&probe)?;
        let mode = file.metadata()?.mode() & 0o777;
        drop(file);
        let _ = fs::remove_file(&probe);
        Ok(Some(mode))
    }
    #[cfg(not(unix))]
    {
        let _ = (destination, dir);
        Ok(None)
    }
}

/// Best-effort fsync of `path`'s parent directory, making a just-completed
/// `rename` into it durable across power loss.
///
/// Opening a directory and calling `fsync` on it is the POSIX way to persist a
/// directory entry. It is best-effort: some platforms/filesystems refuse a
/// directory `fsync`, and it is a no-op off Unix. Failures are ignored — the
/// file contents were already `sync_all`'d before the rename.
fn sync_parent_dir(path: &Path) {
    #[cfg(unix)]
    {
        let parent = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
}

/// Apply the published mode to the finished temp file before rename.
fn apply_published_mode(temp_path: &Path, mode: Option<u32>) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Some(mode) = mode {
            fs::set_permissions(temp_path, fs::Permissions::from_mode(mode))?;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (temp_path, mode);
    }
    Ok(())
}

/// A group of [`AtomicOutput`]s published together.
///
/// Every staged output writes into its own protected temp file; nothing lands
/// until [`publish`](Self::publish) renames each into place after all content
/// is written. Dropping the set without publishing removes every temp file and
/// leaves all destinations untouched. Publication is per-file atomic (each
/// `rename` is atomic); it is not a single cross-file transaction, so a failure
/// partway through can leave earlier renames applied — the individual
/// no-truncation-before-success guarantee still holds for each destination.
#[derive(Default)]
pub struct PublicationSet {
    outputs: Vec<AtomicOutput>,
}

impl PublicationSet {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stage a protected output beside `destination` and return it for writing.
    pub fn stage(&mut self, destination: impl AsRef<Path>) -> io::Result<&mut AtomicOutput> {
        self.outputs.push(AtomicOutput::create(destination)?);
        Ok(self
            .outputs
            .last_mut()
            .expect("just pushed an output onto the set"))
    }

    /// Publish every staged output by renaming its temp file into place.
    pub fn publish(self) -> io::Result<()> {
        for output in self.outputs {
            output.commit()?;
        }
        Ok(())
    }
}

/// The outcome of publishing several destinations that are *not* one atomic
/// cross-file transaction. Each destination's own no-truncation-before-success
/// guarantee still holds, but a failure partway through can leave earlier
/// destinations already published; [`PartialPublication`] reports exactly which
/// destinations landed and which one failed, so a caller never has to pretend
/// the set was pairwise atomic across filesystems.
#[derive(Debug)]
pub struct PartialPublication {
    /// Destinations that were published before the failure, in publish order.
    pub published: Vec<PathBuf>,
    /// The destination whose rename failed.
    pub failed: PathBuf,
    /// The underlying I/O error.
    pub source: io::Error,
}

impl std::fmt::Display for PartialPublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "partial publication: failed to publish `{}` ({}); already published: [{}]",
            self.failed.display(),
            self.source,
            self.published
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl std::error::Error for PartialPublication {}

/// Publish a sequence of already-written [`AtomicOutput`]s in order, renaming
/// each finished temp file over its destination. Returns [`PartialPublication`]
/// if a rename fails after one or more destinations were already published —
/// honestly reporting that the set is not atomic across files/filesystems.
pub fn publish_in_order(outputs: Vec<AtomicOutput>) -> Result<(), PartialPublication> {
    let mut published = Vec::new();
    for output in outputs {
        let destination = output.destination().to_path_buf();
        match output.commit() {
            Ok(()) => published.push(destination),
            Err(source) => {
                return Err(PartialPublication {
                    published,
                    failed: destination,
                    source,
                })
            }
        }
    }
    Ok(())
}

// --- Family state + budget ---------------------------------------------------

/// An exact memory budget (in bytes) for a correlated table family's buffered
/// rows. Once a family's buffered rows would exceed it, the buffer spills to a
/// protected spool rather than growing without bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FamilyBudget {
    /// The maximum number of buffered bytes to hold in memory.
    pub max_bytes: u64,
}

impl Default for FamilyBudget {
    fn default() -> Self {
        // A conservative default; a compiled plan supplies an exact budget.
        Self {
            max_bytes: 64 * 1024 * 1024,
        }
    }
}

/// Which kind of family rows a spool holds, for observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillKind {
    /// Buffered child rows of a family.
    Child,
    /// Buffered rows of a single table.
    Table,
}

/// Where a [`FamilyBuffer`]'s rows currently live.
pub enum FamilyState {
    /// In memory, within budget: the family's parent/aggregate rows.
    ParentState(Vec<SpooledRow>),
    /// Spilled child rows in a protected spool.
    ChildSpool(ProtectedSpool),
    /// Spilled table rows in a protected spool.
    TableSpool(ProtectedSpool),
}

/// Incremental replay of buffered family rows.
///
/// In-memory rows are moved into an owning iterator. Spilled rows are decoded
/// one record at a time from the protected spool, so replay retains at most one
/// decoded row instead of rebuilding the complete family in memory.
pub enum FamilyRowReplay<'a> {
    /// Rows that remained within the configured memory budget.
    Memory(std::vec::IntoIter<SpooledRow>),
    /// Rows decoded incrementally from a protected spool.
    Spool(SpoolReader<&'a mut File>),
}

impl Iterator for FamilyRowReplay<'_> {
    type Item = io::Result<SpooledRow>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(rows) => rows.next().map(Ok),
            Self::Spool(reader) => reader.read_row().transpose(),
        }
    }
}

/// Buffers a family's rows under an exact byte budget, spilling deterministically
/// to a [`ProtectedSpool`] the moment a push would cross the budget.
///
/// The buffer never retains every child row in an unbounded `Vec`: once spilled,
/// each further row is written straight to the protected spool. The spill
/// decision is deterministic — it depends only on the (fixed) budget and the
/// rows pushed, not on wall-clock or allocation timing — so a given plan spills
/// identically on every run.
pub struct FamilyBuffer {
    budget: u64,
    used: u64,
    table_id: u32,
    temp: TempConfig,
    spill_kind: SpillKind,
    state: FamilyState,
}

impl FamilyBuffer {
    /// A buffer that starts in memory and spills on demand.
    pub fn new(
        budget: FamilyBudget,
        table_id: u32,
        temp: TempConfig,
        spill_kind: SpillKind,
    ) -> Self {
        Self {
            budget: budget.max_bytes,
            used: 0,
            table_id,
            temp,
            spill_kind,
            state: FamilyState::ParentState(Vec::new()),
        }
    }

    /// A buffer that chooses its storage up front from an estimate: if the
    /// family is already known to exceed the budget it spills before the first
    /// row, avoiding a transient in-memory spike.
    pub fn with_estimate(
        budget: FamilyBudget,
        table_id: u32,
        temp: TempConfig,
        spill_kind: SpillKind,
        estimated_bytes: u64,
    ) -> io::Result<Self> {
        let mut buffer = Self::new(budget, table_id, temp, spill_kind);
        if estimated_bytes > buffer.budget {
            buffer.spill()?;
        }
        Ok(buffer)
    }

    /// The table id every pushed row is expected to carry.
    pub fn table_id(&self) -> u32 {
        self.table_id
    }

    /// Whether the buffer has spilled to disk.
    pub fn is_spilled(&self) -> bool {
        !matches!(self.state, FamilyState::ParentState(_))
    }

    /// The current storage state.
    pub fn state(&self) -> &FamilyState {
        &self.state
    }

    /// Buffer one row, spilling to a protected spool if it would cross the
    /// budget.
    pub fn push(&mut self, row: SpooledRow) -> io::Result<()> {
        if self.is_spilled() {
            return self.write_spilled(&row);
        }
        let size = row.estimated_bytes();
        if self.used + size <= self.budget {
            self.used += size;
            if let FamilyState::ParentState(rows) = &mut self.state {
                rows.push(row);
            }
            Ok(())
        } else {
            // Crossing the budget: move what we have to a spool, then append.
            self.spill()?;
            self.write_spilled(&row)
        }
    }

    /// Replay buffered rows in push order without materializing a spilled
    /// family in memory.
    pub fn replay_rows(&mut self) -> io::Result<FamilyRowReplay<'_>> {
        match &mut self.state {
            FamilyState::ParentState(rows) => {
                Ok(FamilyRowReplay::Memory(std::mem::take(rows).into_iter()))
            }
            FamilyState::ChildSpool(spool) | FamilyState::TableSpool(spool) => {
                Ok(FamilyRowReplay::Spool(spool.rewind()?))
            }
        }
    }

    /// Collect every buffered row in push order.
    ///
    /// Prefer [`replay_rows`](Self::replay_rows) on generation paths: this
    /// compatibility helper intentionally materializes the result for callers
    /// that need an owned collection.
    pub fn drain_rows(&mut self) -> io::Result<Vec<SpooledRow>> {
        self.replay_rows()?.collect()
    }

    /// Move any in-memory rows into a fresh protected spool and switch state.
    ///
    /// Rows go through the spool's persistent buffered writer; no per-row flush
    /// — buffered writes are flushed once when the spool is later drained.
    fn spill(&mut self) -> io::Result<()> {
        let mut spool = ProtectedSpool::create(&self.temp)?;
        if let FamilyState::ParentState(rows) = &self.state {
            for row in rows {
                spool.write_row(row)?;
            }
        }
        self.state = match self.spill_kind {
            SpillKind::Child => FamilyState::ChildSpool(spool),
            SpillKind::Table => FamilyState::TableSpool(spool),
        };
        Ok(())
    }

    /// Append a row directly to the active spool's buffered writer (no per-row
    /// flush; the buffer is flushed once at drain).
    fn write_spilled(&mut self, row: &SpooledRow) -> io::Result<()> {
        match &mut self.state {
            FamilyState::ChildSpool(spool) | FamilyState::TableSpool(spool) => spool.write_row(row),
            // `push`/`with_estimate` only reach here after `spill`, so the state
            // is always a spool; treat an in-memory state as a no-op rather than
            // panicking.
            FamilyState::ParentState(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spool_round_trip_preserves_every_value_shape() {
        let temp = TempConfig::default();
        let mut spool = ProtectedSpool::create(&temp).unwrap();
        let row = SpooledRow {
            table_id: 9,
            row_index: 3,
            values: vec![
                GeneratedValue::Null,
                GeneratedValue::Integer(-1),
                GeneratedValue::Text("x".into()),
                GeneratedValue::Bytes(vec![1, 2, 3]),
            ],
        };
        spool.write_row(&row).unwrap();
        let mut reader = spool.rewind().unwrap();
        assert_eq!(reader.read_row().unwrap(), Some(row));
        assert_eq!(reader.read_row().unwrap(), None);
    }

    #[test]
    fn cancellation_token_is_independent_per_instance() {
        let a = CancellationToken::new();
        let b = CancellationToken::new();
        a.cancel();
        assert!(a.is_cancelled());
        assert!(!b.is_cancelled());
    }
}

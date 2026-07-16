//! Adaptive I/O strategys: named writer configurations plus the shared,
//! atomically-swappable runtime values that let the controller retune the
//! write pipeline mid-run without stopping writer threads.
//!
//! Design: `docs/features/ADAPTIVE_IO_PROFILES.md`. One configuration cannot
//! serve NVMe (wants parallel writers, small buffers), spinning disks (wants
//! one writer, huge sequential writes), and cheap flash / network mounts
//! (wants the fewest write *operations*). The three named profiles capture
//! those device classes; `--io-strategy auto` opens with a probe-chosen
//! profile and lets the feedback controller own the truth afterwards.

use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const KIB: usize = 1024;
const MIB: usize = 1024 * 1024;

/// The three device-class states of the adaptive controller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileKind {
    /// NVMe/SSD: parallel writers, small buffers (today's defaults).
    Ssd,
    /// Rotational media / same-spindle read+write: one writer, big
    /// sequential writes so the head seeks as rarely as possible.
    SlowSeek,
    /// Cheap flash and network filesystems: per-operation cost dominates, so
    /// issue the fewest, largest write operations we can.
    SlowOps,
}

impl ProfileKind {
    /// Human-readable name used in transition log lines and `--io-strategy`.
    /// `ssd` (not "fast") because it names the device class the settings are
    /// tuned for — on a spinning disk the SSD settings are the *slow* choice.
    pub fn label(self) -> &'static str {
        match self {
            ProfileKind::Ssd => "ssd",
            ProfileKind::SlowSeek => "hdd",
            ProfileKind::SlowOps => "cheap",
        }
    }
}

/// The `--io-strategy` CLI choice: a pinned named profile, or `auto`
/// (probe + feedback controller).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IoStrategy {
    #[default]
    Auto,
    Ssd,
    Hdd,
    Cheap,
}

impl IoStrategy {
    /// Parse the `--io-strategy` flag value.
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "auto" | "" => Ok(IoStrategy::Auto),
            // `fast` is a compatibility alias for `ssd` (the pre-rename name).
            "ssd" => Ok(IoStrategy::Ssd),
            "hdd" => Ok(IoStrategy::Hdd),
            // The honest name. The aliases are for everyone who has ever fought
            // a no-name USB stick from a conference and knows exactly which
            // profile they need.
            "cheap" | "potato" => Ok(IoStrategy::Cheap),
            other => Err(format!(
                "Unknown I/O strategy '{other}'. Valid: auto, ssd, hdd, cheap"
            )),
        }
    }

    /// The pinned profile kind, or `None` for `auto`.
    pub fn pinned(self) -> Option<ProfileKind> {
        match self {
            IoStrategy::Auto => None,
            IoStrategy::Ssd => Some(ProfileKind::Ssd),
            IoStrategy::Hdd => Some(ProfileKind::SlowSeek),
            IoStrategy::Cheap => Some(ProfileKind::SlowOps),
        }
    }
}

/// A concrete writer configuration: everything that used to be hard-coded
/// consts (`STAGE_FLUSH`, `STAGE_TOTAL_CAP`) or env-only knobs, as one value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterProfile {
    /// Number of writer threads (target; growth is deferred in auto mode).
    pub writers: usize,
    /// Per-table staging threshold: a table's staged bytes ship to its writer
    /// once they cross this size.
    pub flush_chunk: usize,
    /// Per-file output buffer: writer threads coalesce chunks up to this size
    /// before issuing the actual file write.
    pub file_buf: usize,
    /// Cap on total bytes staged across all tables on the producer side.
    pub stage_cap: usize,
}

impl WriterProfile {
    /// The named profile table from the design doc.
    ///
    /// `compressing` matters only for `Fast`: compression is CPU-bound on
    /// fast media, so it gets all cores there. Per the 2026-07-16 amendment,
    /// the slow profiles pin writers to 1 *unconditionally* — on seek-bound
    /// media extra compression writers thrash the spindle and end up slower
    /// than plain output (measured 22.1 vs 34.5 MB/s at 100 GB scale).
    pub fn for_kind(kind: ProfileKind, cores: usize, compressing: bool) -> Self {
        match kind {
            ProfileKind::Ssd => Self {
                writers: if compressing { cores } else { cores.min(4) },
                flush_chunk: 256 * KIB,
                file_buf: 256 * KIB,
                stage_cap: 32 * MIB,
            },
            ProfileKind::SlowSeek => Self {
                writers: 1,
                flush_chunk: 8 * MIB,
                file_buf: 64 * MIB,
                stage_cap: 256 * MIB,
            },
            ProfileKind::SlowOps => Self {
                writers: 1,
                flush_chunk: 32 * MIB,
                file_buf: 64 * MIB,
                stage_cap: 512 * MIB,
            },
        }
    }

    /// Apply the expert env overrides. Precedence: env > explicit flag >
    /// auto — these two knobs predate the profiles and keep working, pinning
    /// their value across every profile the controller may switch to.
    ///
    /// - `SQL_SPLITTER_WRITERS`: writer count (also disables deferred growth).
    /// - `SQL_SPLITTER_WRITE_BUF`: per-file output buffer size in bytes.
    pub fn with_env_overrides(mut self) -> Self {
        if let Some(n) = env_writer_count() {
            self.writers = n;
        }
        if let Some(n) = env_usize("SQL_SPLITTER_WRITE_BUF").filter(|&n| n >= 4096) {
            self.file_buf = n;
        }
        self
    }
}

/// The `SQL_SPLITTER_WRITERS` env override, if set to a valid count. When
/// present it pins the writer count in every mode, which also disables the
/// controller's deferred writer growth.
pub fn env_writer_count() -> Option<usize> {
    env_usize("SQL_SPLITTER_WRITERS").filter(|&n| n >= 1)
}

/// Parse an env var as `usize`, treating absence and garbage the same.
fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

/// The runtime-swappable subset of a [`WriterProfile`], shared between the
/// producer, the writer threads, and the controller.
///
/// Writer *count* is deliberately absent: it can only grow (each table is
/// owned by one thread, and that ownership is what keeps output
/// byte-identical), so growth is a structural operation on the pool, not a
/// value swap. Everything else is a plain number read at flush points, so a
/// relaxed atomic swap retunes the pipeline mid-run with zero coordination.
#[derive(Debug)]
pub struct ProfileValues {
    flush_chunk: AtomicUsize,
    file_buf: AtomicUsize,
    stage_cap: AtomicUsize,
}

impl ProfileValues {
    /// Initialize from a profile.
    pub fn new(profile: &WriterProfile) -> Self {
        Self {
            flush_chunk: AtomicUsize::new(profile.flush_chunk),
            file_buf: AtomicUsize::new(profile.file_buf),
            stage_cap: AtomicUsize::new(profile.stage_cap),
        }
    }

    /// Swap in a new profile's values (a controller transition).
    pub fn apply(&self, profile: &WriterProfile) {
        self.flush_chunk
            .store(profile.flush_chunk, Ordering::Relaxed);
        self.file_buf.store(profile.file_buf, Ordering::Relaxed);
        self.stage_cap.store(profile.stage_cap, Ordering::Relaxed);
    }

    #[inline]
    pub fn flush_chunk(&self) -> usize {
        self.flush_chunk.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn file_buf(&self) -> usize {
        self.file_buf.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn stage_cap(&self) -> usize {
        self.stage_cap.load(Ordering::Relaxed)
    }
}

/// Bytes written (and fsynced) by the startup probe.
const PROBE_BYTES: usize = 8 * MIB;

/// Probe the output directory to choose `auto`'s *opening* profile: write and
/// fsync 8 MB, classify by the sustained rate. This is a hint only — the
/// feedback controller owns the truth once the pipeline is running. Costs
/// ~100 ms on NVMe; on slow media the cost is itself the signal.
///
/// The hidden env `SQL_SPLITTER_IO_PROBE` (`ssd` | `hdd` | `cheap`)
/// forces the verdict without touching the disk — the deterministic seam the
/// integration tests use.
///
/// Errors are swallowed into a `Fast` verdict: a probe failure (exotic
/// filesystem, permissions race) must never fail the actual split.
pub fn probe_output_dir(dir: &Path) -> ProfileKind {
    if let Ok(forced) = std::env::var("SQL_SPLITTER_IO_PROBE") {
        if let Ok(profile) = IoStrategy::parse(&forced) {
            if let Some(kind) = profile.pinned() {
                return kind;
            }
        }
    }
    match run_probe(dir) {
        Ok(mbps) => {
            if mbps > 80.0 {
                ProfileKind::Ssd
            } else if mbps >= 10.0 {
                ProfileKind::SlowSeek
            } else {
                ProfileKind::SlowOps
            }
        }
        Err(_) => ProfileKind::Ssd,
    }
}

/// Write + fsync [`PROBE_BYTES`] into `dir`, returning the observed MB/s.
fn run_probe(dir: &Path) -> std::io::Result<f64> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(".sqlsplit-io-probe");
    let result = (|| {
        let mut file = std::fs::File::create(&path)?;
        let block = vec![0u8; MIB];
        let start = Instant::now();
        for _ in 0..(PROBE_BYTES / MIB) {
            file.write_all(&block)?;
        }
        file.sync_all()?;
        let secs = start.elapsed().as_secs_f64().max(1e-9);
        Ok(PROBE_BYTES as f64 / MIB as f64 / secs)
    })();
    // Best-effort cleanup either way; the verdict matters, the file doesn't.
    let _ = std::fs::remove_file(&path);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_table_matches_design_doc() {
        let fast = WriterProfile::for_kind(ProfileKind::Ssd, 8, false);
        assert_eq!(fast.writers, 4);
        assert_eq!(fast.flush_chunk, 256 * KIB);
        assert_eq!(fast.file_buf, 256 * KIB);
        assert_eq!(fast.stage_cap, 32 * MIB);

        let seek = WriterProfile::for_kind(ProfileKind::SlowSeek, 8, false);
        assert_eq!(
            seek,
            WriterProfile {
                writers: 1,
                flush_chunk: 8 * MIB,
                file_buf: 64 * MIB,
                stage_cap: 256 * MIB,
            }
        );

        let ops = WriterProfile::for_kind(ProfileKind::SlowOps, 8, false);
        assert_eq!(
            ops,
            WriterProfile {
                writers: 1,
                flush_chunk: 32 * MIB,
                file_buf: 64 * MIB,
                stage_cap: 512 * MIB,
            }
        );
    }

    #[test]
    fn fast_profile_uses_all_cores_when_compressing() {
        assert_eq!(
            WriterProfile::for_kind(ProfileKind::Ssd, 8, true).writers,
            8
        );
        assert_eq!(
            WriterProfile::for_kind(ProfileKind::Ssd, 2, false).writers,
            2
        );
    }

    /// 2026-07-16 amendment: slow profiles pin W=1 even for compression.
    #[test]
    fn slow_profiles_cap_compression_writers_at_one() {
        assert_eq!(
            WriterProfile::for_kind(ProfileKind::SlowSeek, 16, true).writers,
            1
        );
        assert_eq!(
            WriterProfile::for_kind(ProfileKind::SlowOps, 16, true).writers,
            1
        );
    }

    #[test]
    fn io_profile_parsing() {
        assert_eq!(IoStrategy::parse("auto"), Ok(IoStrategy::Auto));
        assert_eq!(IoStrategy::parse("ssd"), Ok(IoStrategy::Ssd));
        assert_eq!(IoStrategy::parse("HDD"), Ok(IoStrategy::Hdd));
        assert_eq!(IoStrategy::parse("potato"), Ok(IoStrategy::Cheap));
        assert!(IoStrategy::parse("turbo").is_err());
    }

    #[test]
    fn profile_values_swap() {
        let fast = WriterProfile::for_kind(ProfileKind::Ssd, 4, false);
        let seek = WriterProfile::for_kind(ProfileKind::SlowSeek, 4, false);
        let values = ProfileValues::new(&fast);
        assert_eq!(values.flush_chunk(), fast.flush_chunk);
        values.apply(&seek);
        assert_eq!(values.flush_chunk(), seek.flush_chunk);
        assert_eq!(values.file_buf(), seek.file_buf);
        assert_eq!(values.stage_cap(), seek.stage_cap);
    }
}

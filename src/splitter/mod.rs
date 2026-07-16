use crate::parser::{determine_buffer_size, ContentFilter, Parser, SqlDialect, StatementType};
use crate::progress::ProgressReader;
use crate::writer::{
    env_writer_count, probe_output_dir, Clock, Controller, EpochMeasurement, IoProfile,
    ParallelWriters, ProfileKind, ProfileValues, RealClock, WriterProfile,
};
use ahash::AHashSet;
use anyhow::Context;
use serde::Serialize;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Statistics from a split operation.
#[derive(Serialize)]
pub struct Stats {
    /// Total statements processed.
    pub statements_processed: u64,
    /// Number of unique tables found.
    pub tables_found: usize,
    /// Total bytes processed from input.
    pub bytes_processed: u64,
    /// Names of all tables found.
    pub table_names: Vec<String>,
    /// Number of writer threads at the end of the run (observability for the
    /// adaptive controller's deferred writer spawn; not part of JSON output).
    #[serde(skip)]
    pub writers_used: usize,
    /// Adaptive I/O transition log lines emitted during the run, in order
    /// (observability for tests and debugging; not part of JSON output).
    #[serde(skip)]
    pub io_transitions: Vec<String>,
}

/// Configuration for the splitter.
#[derive(Default)]
pub struct SplitterConfig {
    /// SQL dialect for parsing.
    pub dialect: SqlDialect,
    /// If true, parse without writing output files.
    pub dry_run: bool,
    /// If set, only process tables in this set.
    pub table_filter: Option<AHashSet<String>>,
    /// Optional callback for progress reporting.
    pub progress_fn: Option<Box<dyn Fn(u64)>>,
    /// Filter for which statement types to include.
    pub content_filter: ContentFilter,
    /// Per-table output compression (default `None` → plain `.sql`).
    pub output_compression: Compression,
    /// I/O profile for output writing (default `Auto`).
    pub io_profile: IoProfile,
    /// Clock driving epoch measurements (None → wall clock). Test seam: a
    /// stepping mock clock makes the adaptive controller fully deterministic.
    pub io_clock: Option<Arc<dyn Clock>>,
}

/// Compression format detected from file extension
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    #[default]
    None,
    Gzip,
    Bzip2,
    Xz,
    Zstd,
    /// A zip *archive* (not a stream compressor) containing exactly one
    /// `.sql` member. Detected here so callers can branch on it, but it
    /// never goes through [`Compression::wrap_reader`] — see [`open_input`]
    /// and `crate::zip_input`.
    Zip,
}

impl Compression {
    /// Detect compression format from file extension
    pub fn from_path(path: &Path) -> Self {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_lowercase());

        match ext.as_deref() {
            Some("gz" | "gzip") => Compression::Gzip,
            Some("bz2" | "bzip2") => Compression::Bzip2,
            Some("xz" | "lzma") => Compression::Xz,
            Some("zst" | "zstd") => Compression::Zstd,
            Some("zip") => Compression::Zip,
            _ => Compression::None,
        }
    }

    /// Parse an output-compression format name (for the `--compress` flag).
    pub fn parse_output(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "none" | "" => Ok(Compression::None),
            "gzip" | "gz" => Ok(Compression::Gzip),
            "bzip2" | "bz2" => Ok(Compression::Bzip2),
            "xz" | "lzma" => Ok(Compression::Xz),
            "zstd" | "zst" => Ok(Compression::Zstd),
            other => Err(format!(
                "Unknown compression '{other}'. Valid: none, gzip, bzip2, xz, zstd"
            )),
        }
    }

    /// File-name extension for compressed output (empty for `None`).
    pub fn output_extension(&self) -> &'static str {
        match self {
            Compression::None => "",
            Compression::Gzip => ".gz",
            Compression::Bzip2 => ".bz2",
            Compression::Xz => ".xz",
            Compression::Zstd => ".zst",
            // Never selected as an output compression: `parse_output` has no
            // "zip" case, so per-table output stays rejected as documented.
            Compression::Zip => "",
        }
    }

    /// Wrap a reader with the appropriate decompressor
    pub fn wrap_reader<'a>(
        &self,
        reader: Box<dyn Read + 'a>,
    ) -> std::io::Result<Box<dyn Read + 'a>> {
        Ok(match self {
            Compression::None => reader,
            #[cfg(feature = "compression")]
            Compression::Gzip => Box::new(flate2::read::GzDecoder::new(reader)),
            #[cfg(feature = "compression")]
            Compression::Bzip2 => Box::new(bzip2::read::BzDecoder::new(reader)),
            #[cfg(feature = "compression")]
            Compression::Xz => Box::new(xz2::read::XzDecoder::new(reader)),
            #[cfg(feature = "compression")]
            Compression::Zstd => Box::new(zstd::stream::read::Decoder::new(reader)?),
            // Zip is an archive, not a stream decoder: it needs a seekable
            // `File` to parse the central directory, so it can never be
            // wrapped here. Callers must go through `open_input` instead.
            Compression::Zip => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "zip input must be opened via open_input(), not wrap_reader() (needs a seekable File)",
                ))
            }
            #[cfg(not(feature = "compression"))]
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "compressed input requires the `compression` feature",
                ))
            }
        })
    }
}

impl std::fmt::Display for Compression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Compression::None => write!(f, "none"),
            Compression::Gzip => write!(f, "gzip"),
            Compression::Bzip2 => write!(f, "bzip2"),
            Compression::Xz => write!(f, "xz"),
            Compression::Zstd => write!(f, "zstd"),
            Compression::Zip => write!(f, "zip"),
        }
    }
}

/// Open `path` as a streaming byte source, transparently handling any
/// supported compression format — including zip archives, which need a
/// different opening strategy than the stream decoders (see
/// `crate::zip_input`). This is the entry point call sites should use
/// instead of manually pairing `File::open` with [`Compression::wrap_reader`].
pub fn open_input(path: &Path) -> anyhow::Result<Box<dyn Read>> {
    open_input_impl(path, None)
}

/// Same as [`open_input`], but reports bytes read from disk (compressed
/// bytes, for compressed/zip inputs) through `progress_fn` as they're read.
pub fn open_input_with_progress(
    path: &Path,
    progress_fn: Box<dyn Fn(u64)>,
) -> anyhow::Result<Box<dyn Read>> {
    open_input_impl(path, Some(progress_fn))
}

fn open_input_impl(
    path: &Path,
    progress_fn: Option<Box<dyn Fn(u64)>>,
) -> anyhow::Result<Box<dyn Read>> {
    let compression = Compression::from_path(path);

    if compression == Compression::Zip {
        #[cfg(feature = "archive")]
        {
            return crate::zip_input::open_zip_member(path, progress_fn);
        }
        #[cfg(not(feature = "archive"))]
        {
            anyhow::bail!("zip input requires the `archive` feature");
        }
    }

    let file = File::open(path).with_context(|| format!("Failed to open input file: {path:?}"))?;
    let reader: Box<dyn Read> = match progress_fn {
        Some(cb) => Box::new(ProgressReader::new(file, cb)),
        None => Box::new(file),
    };
    compression
        .wrap_reader(reader)
        .with_context(|| format!("Failed to initialize {compression} decompression for {path:?}"))
}

pub struct Splitter {
    input_file: PathBuf,
    output_dir: PathBuf,
    config: SplitterConfig,
}

impl Splitter {
    pub fn new(input_file: PathBuf, output_dir: PathBuf) -> Self {
        Self {
            input_file,
            output_dir,
            config: SplitterConfig::default(),
        }
    }

    pub fn with_dialect(mut self, dialect: SqlDialect) -> Self {
        self.config.dialect = dialect;
        self
    }

    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.config.dry_run = dry_run;
        self
    }

    pub fn with_table_filter(mut self, tables: Vec<String>) -> Self {
        if !tables.is_empty() {
            self.config.table_filter = Some(tables.into_iter().collect());
        }
        self
    }

    pub fn with_progress<F: Fn(u64) + 'static>(mut self, f: F) -> Self {
        self.config.progress_fn = Some(Box::new(f));
        self
    }

    pub fn with_content_filter(mut self, filter: ContentFilter) -> Self {
        self.config.content_filter = filter;
        self
    }

    pub fn with_output_compression(mut self, format: Compression) -> Self {
        self.config.output_compression = format;
        self
    }

    /// Choose the I/O profile for output writing (`--io-profile`). Explicit
    /// profiles pin the writer configuration; `Auto` (the default) probes the
    /// output device and adapts at runtime.
    pub fn with_io_profile(mut self, profile: IoProfile) -> Self {
        self.config.io_profile = profile;
        self
    }

    /// Override the clock used for adaptive-I/O epoch measurements.
    /// Deterministic-test seam; production code always uses the default
    /// wall clock.
    #[doc(hidden)]
    pub fn with_io_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.config.io_clock = Some(clock);
        self
    }

    pub fn split(mut self) -> anyhow::Result<Stats> {
        let file_size = std::fs::metadata(&self.input_file)
            .with_context(|| format!("Failed to open input file: {:?}", self.input_file))?
            .len();
        let buffer_size = determine_buffer_size(file_size);
        let dialect = self.config.dialect;
        let content_filter = self.config.content_filter;

        // Open the input, transparently handling any supported compression
        // format (including zip archives, which need a two-phase open).
        let reader: Box<dyn Read> = match self.config.progress_fn.take() {
            Some(cb) => open_input_with_progress(&self.input_file, cb)?,
            None => open_input(&self.input_file)?,
        };

        let mut parser = Parser::with_dialect(reader, buffer_size, dialect);

        // Parallel, pipelined writers (skipped for dry runs), configured by
        // the selected I/O profile (see docs/features/ADAPTIVE_IO_PROFILES.md).
        // Precedence: SQL_SPLITTER_WRITERS / SQL_SPLITTER_WRITE_BUF env vars >
        // explicit --io-profile > auto.
        let out_compression = self.config.output_compression;
        let compressing = out_compression != Compression::None;
        let cores = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);

        // Epoch length for the adaptive controller: byte-based so controller
        // decisions are a pure function of (bytes, measured durations). The
        // hidden env override is the deterministic-test seam.
        let epoch_bytes = std::env::var("SQL_SPLITTER_EPOCH_BYTES")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&n| n > 0);
        // Adaptation only pays off at scale: below ~4 default epochs' worth of
        // input the run is over before the controller could act, so `auto`
        // just pins FAST (today's defaults) and skips the 8MB probe. An
        // explicit epoch override (tests) engages the full machinery.
        const DEFAULT_EPOCH_BYTES: u64 = 256 * 1024 * 1024;
        const AUTO_MIN_FILE_SIZE: u64 = 64 * 1024 * 1024;
        let adaptive = self.config.io_profile == IoProfile::Auto
            && !self.config.dry_run
            && (epoch_bytes.is_some() || file_size >= AUTO_MIN_FILE_SIZE);
        // Aim for at least 4 epochs per file so short runs still measure.
        let epoch_bytes =
            epoch_bytes.unwrap_or_else(|| DEFAULT_EPOCH_BYTES.min((file_size / 4).max(1)));

        let opening_kind = match self.config.io_profile.pinned() {
            Some(kind) => kind,
            // The fsync probe picks auto's *opening* state only; the feedback
            // controller owns the truth once the pipeline runs.
            None if adaptive => probe_output_dir(&self.output_dir).0,
            None => ProfileKind::Ssd,
        };
        let profile =
            WriterProfile::for_kind(opening_kind, cores, compressing).with_env_overrides();

        // In auto mode, open at W=1 and let the controller spawn up to FAST's
        // writer count once the device proves fast: on NVMe the W=1 opening
        // costs ~15% for one epoch; on slow media it avoids ever thrashing.
        // SQL_SPLITTER_WRITERS pins the count and disables growth entirely.
        let initial_writers = if adaptive {
            env_writer_count().unwrap_or(1)
        } else {
            profile.writers
        };
        let fast_writers = if env_writer_count().is_some() {
            initial_writers
        } else {
            WriterProfile::for_kind(ProfileKind::Ssd, cores, compressing).writers
        };

        let values = Arc::new(ProfileValues::new(&profile));
        let mut writers = if self.config.dry_run {
            None
        } else {
            // Channel depth scales with chunk size so in-flight bytes stay in
            // the same ballpark as the profile's stage cap: FAST keeps the
            // historical 64 × 256KB; the slow profiles get fewer, bigger slots.
            let capacity = (profile.stage_cap / profile.flush_chunk.max(1)).clamp(4, 64);
            Some(
                ParallelWriters::new(
                    self.output_dir.clone(),
                    initial_writers,
                    capacity,
                    out_compression,
                    Arc::clone(&values),
                )
                .with_context(|| {
                    format!("Failed to create output directory: {:?}", self.output_dir)
                })?,
            )
        };

        // Hidden debugging aid: dump per-epoch measurements to stderr (the
        // phase-0 "io stats" seam from the design doc).
        let io_debug = std::env::var("SQL_SPLITTER_IO_DEBUG").is_ok();
        let mut controller = adaptive.then(|| Controller::new(opening_kind, fast_writers));
        let clock: Arc<dyn Clock> = self
            .config
            .io_clock
            .take()
            .unwrap_or_else(|| Arc::new(RealClock::new()));
        let mut epoch_start_time = clock.now();
        let mut epoch_acked = 0u64;
        let mut epoch_stall = std::time::Duration::ZERO;
        let mut next_epoch = epoch_bytes;

        let mut tables_seen: AHashSet<String> = AHashSet::new();
        let mut stats = Stats {
            statements_processed: 0,
            tables_found: 0,
            bytes_processed: 0,
            table_names: Vec::new(),
            writers_used: initial_writers,
            io_transitions: Vec::new(),
        };

        // Track the last COPY table for PostgreSQL COPY data blocks
        let mut last_copy_table: Option<String> = None;

        while let Some(stmt) = parser.read_statement()? {
            let (stmt_type, mut table_name) =
                Parser::<&[u8]>::parse_statement_with_dialect(&stmt, dialect);

            // Track COPY statements for data association
            if stmt_type == StatementType::Copy {
                last_copy_table = Some(table_name.clone());
            }

            // Handle PostgreSQL COPY data blocks - associate with last COPY table
            let is_copy_data = if stmt_type == StatementType::Unknown && last_copy_table.is_some() {
                // Check if this looks like COPY data (ends with \.\n)
                if stmt.ends_with(b"\\.\n") || stmt.ends_with(b"\\.\r\n") {
                    // Safe: we just checked is_some() above
                    if let Some(copy_table) = last_copy_table.take() {
                        table_name = copy_table;
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            } else {
                false
            };

            if !is_copy_data && (stmt_type == StatementType::Unknown || table_name.is_empty()) {
                continue;
            }

            // Apply content filter (schema-only or data-only)
            match content_filter {
                ContentFilter::SchemaOnly => {
                    if !stmt_type.is_schema() {
                        continue;
                    }
                }
                ContentFilter::DataOnly => {
                    // For data-only, include INSERT, COPY, and COPY data blocks
                    if !stmt_type.is_data() && !is_copy_data {
                        continue;
                    }
                }
                ContentFilter::All => {}
            }

            if let Some(ref filter) = self.config.table_filter {
                if !filter.contains(&table_name) {
                    continue;
                }
            }

            if !tables_seen.contains(&table_name) {
                tables_seen.insert(table_name.clone());
                stats.tables_found += 1;
                stats.table_names.push(table_name.clone());
            }

            if let Some(w) = writers.as_mut() {
                // For MSSQL, add a semicolon when the statement doesn't already
                // end with one (GO is the batch separator, but we need `;` for
                // re-parsing). Other dialects add no suffix.
                let suffix: &[u8] = if self.config.dialect == SqlDialect::Mssql {
                    let last = stmt
                        .iter()
                        .rev()
                        .find(|&&b| b != b'\n' && b != b'\r' && b != b' ' && b != b'\t');
                    if last != Some(&b';') {
                        b";"
                    } else {
                        b""
                    }
                } else {
                    b""
                };
                w.write(&table_name, &stmt, suffix);

                // A writer thread hit an I/O error; stop parsing and surface it.
                if w.errored() {
                    break;
                }
            }

            stats.statements_processed += 1;
            stats.bytes_processed += stmt.len() as u64;

            // Adaptive-I/O epoch boundary: byte-based (see module docs), so
            // the controller's decisions are a pure function of measured
            // data, not wall-clock scheduling.
            if let Some(ctrl) = controller.as_mut() {
                if stats.bytes_processed >= next_epoch {
                    next_epoch += epoch_bytes;
                    // `writers` is always `Some` when `controller` is `Some`
                    // (adaptive requires `!dry_run`, see above).
                    if let Some(w) = writers.as_mut() {
                        let snapshot = w.stats();
                        let measurement = EpochMeasurement {
                            bytes: snapshot.bytes_acked.saturating_sub(epoch_acked),
                            duration: clock.now().saturating_sub(epoch_start_time),
                            send_stall: snapshot.send_stall.saturating_sub(epoch_stall),
                        };
                        epoch_acked = snapshot.bytes_acked;
                        epoch_stall = snapshot.send_stall;
                        epoch_start_time = clock.now();

                        if io_debug {
                            eprintln!(
                                "epoch: bytes={} dur={:?} stall={:?} tp={:.1} stall_frac={:.2}",
                                measurement.bytes,
                                measurement.duration,
                                measurement.send_stall,
                                measurement.throughput_mbps(),
                                measurement.stall_fraction()
                            );
                        }
                        let decision = ctrl.on_epoch(&measurement);
                        if let Some(kind) = decision.transition {
                            let new_profile = WriterProfile::for_kind(kind, cores, compressing)
                                .with_env_overrides();
                            values.apply(&new_profile);
                            let line = format!(
                                "output device sustaining ~{:.0} MB/s — switching to {} write profile",
                                decision.throughput_mbps,
                                kind.label()
                            );
                            eprintln!("{line}");
                            stats.io_transitions.push(line);
                        }
                        if decision.target_writers > w.writer_count() {
                            w.grow_to(decision.target_writers);
                        }
                        stats.writers_used = w.writer_count();
                    }
                }
            }
        }

        if let Some(w) = writers {
            w.finish()
                .with_context(|| format!("Failed writing output to {:?}", self.output_dir))?;
        }

        Ok(stats)
    }
}

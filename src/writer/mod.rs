//! Buffered file writers for splitting SQL statements into per-table files.

use ahash::AHashMap;
use std::collections::hash_map::Entry;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Size of the BufWriter buffer per table file. Sits at the top of the
/// sequential-write plateau (64KB–256KB); overridable via
/// `SQL_SPLITTER_WRITE_BUF` (bytes) for tuning.
pub const WRITER_BUFFER_SIZE: usize = 256 * 1024;

/// Resolve the per-file write buffer size, honoring the env override.
pub fn write_buffer_size() -> usize {
    std::env::var("SQL_SPLITTER_WRITE_BUF")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n >= 4096)
        .unwrap_or(WRITER_BUFFER_SIZE)
}

/// Buffered writer for a single table's SQL file.
pub struct TableWriter {
    writer: BufWriter<File>,
}

impl TableWriter {
    /// Create a new table writer for the given file path.
    pub fn new(filename: &Path) -> std::io::Result<Self> {
        let file = File::create(filename)?;
        let writer = BufWriter::with_capacity(write_buffer_size(), file);
        Ok(Self { writer })
    }

    /// Write a SQL statement followed by a newline. The `BufWriter` flushes
    /// itself only when its buffer fills (and on close), so writes coalesce
    /// into large syscalls instead of one per ~100 statements.
    pub fn write_statement(&mut self, stmt: &[u8]) -> std::io::Result<()> {
        self.writer.write_all(stmt)?;
        self.writer.write_all(b"\n")
    }

    /// Write a SQL statement with a custom suffix and newline.
    pub fn write_statement_with_suffix(
        &mut self,
        stmt: &[u8],
        suffix: &[u8],
    ) -> std::io::Result<()> {
        self.writer.write_all(stmt)?;
        self.writer.write_all(suffix)?;
        self.writer.write_all(b"\n")
    }

    /// Flush the internal buffer to the OS.
    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

/// Pool of per-table writers, creating files on demand.
pub struct WriterPool {
    output_dir: PathBuf,
    writers: AHashMap<String, TableWriter>,
}

impl WriterPool {
    /// Create a new writer pool targeting the given output directory.
    pub fn new(output_dir: PathBuf) -> Self {
        Self {
            output_dir,
            writers: AHashMap::new(),
        }
    }

    /// Create the output directory if it does not exist.
    pub fn ensure_output_dir(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.output_dir)
    }

    /// Get or create a writer for the given table name.
    pub fn get_writer(&mut self, table_name: &str) -> std::io::Result<&mut TableWriter> {
        use std::collections::hash_map::Entry;

        // Use entry API to avoid separate contains_key + get_mut (eliminates unwrap)
        match self.writers.entry(table_name.to_string()) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let filename = self.output_dir.join(format!("{}.sql", table_name));
                let writer = TableWriter::new(&filename)?;
                Ok(entry.insert(writer))
            }
        }
    }

    /// Write a statement to the file for the given table.
    pub fn write_statement(&mut self, table_name: &str, stmt: &[u8]) -> std::io::Result<()> {
        let writer = self.get_writer(table_name)?;
        writer.write_statement(stmt)
    }

    /// Write a statement with suffix to the file for the given table.
    pub fn write_statement_with_suffix(
        &mut self,
        table_name: &str,
        stmt: &[u8],
        suffix: &[u8],
    ) -> std::io::Result<()> {
        let writer = self.get_writer(table_name)?;
        writer.write_statement_with_suffix(stmt, suffix)
    }

    /// Flush and close all writers.
    pub fn close_all(&mut self) -> std::io::Result<()> {
        for writer in self.writers.values_mut() {
            writer.flush()?;
        }
        Ok(())
    }
}

use crate::splitter::Compression;

/// Threshold at which a table's staging buffer is shipped to its writer thread.
const STAGE_FLUSH: usize = 256 * 1024;

/// Cap on the *total* bytes staged across all tables. Individual buffers ship
/// at [`STAGE_FLUSH`], but a dump with thousands of tables could otherwise
/// stage up to `tables × STAGE_FLUSH`; crossing this cap flushes every staged
/// buffer so producer-side memory stays bounded regardless of table count.
const STAGE_TOTAL_CAP: usize = 32 * 1024 * 1024;

/// A batched write job: pre-assembled bytes for one table.
struct Chunk {
    table: Arc<str>,
    data: Vec<u8>,
}

/// Point-in-time snapshot of the writer pipeline's instrumentation counters.
///
/// These are the two signals the adaptive I/O controller samples per epoch
/// (see `docs/features/ADAPTIVE_IO_PROFILES.md`): how fast the writer threads
/// actually drain (`bytes_acked`) and how long the producer sat blocked
/// shipping chunks (`send_stall`). Near-zero stall combined with low
/// throughput means the *input* side is the bottleneck, so the write profile
/// must not react to it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriterStats {
    /// Bytes fully handed to the output sinks by writer threads.
    pub bytes_acked: u64,
    /// Cumulative time the producer spent blocked on a full writer channel.
    pub send_stall: Duration,
}

/// A per-table output stream: a plain file, or a streaming compressor over one.
/// Compressing per file keeps every output independent, so the writer pool
/// still runs fully in parallel — and the compression work parallelizes with it.
enum TableSink {
    Raw(File),
    #[cfg(feature = "compression")]
    Gzip(flate2::write::GzEncoder<File>),
    #[cfg(feature = "compression")]
    Bzip2(bzip2::write::BzEncoder<File>),
    #[cfg(feature = "compression")]
    Xz(xz2::write::XzEncoder<File>),
    #[cfg(feature = "compression")]
    Zstd(zstd::stream::write::Encoder<'static, File>),
}

impl TableSink {
    /// Create the `<table>.sql[.ext]` file and wrap it in the chosen encoder.
    fn create(dir: &Path, table: &str, format: Compression) -> std::io::Result<Self> {
        let path = dir.join(format!("{}.sql{}", table, format.output_extension()));
        let file = File::create(&path)?;
        Ok(match format {
            Compression::None => TableSink::Raw(file),
            #[cfg(feature = "compression")]
            Compression::Gzip => TableSink::Gzip(flate2::write::GzEncoder::new(
                file,
                flate2::Compression::default(),
            )),
            #[cfg(feature = "compression")]
            Compression::Bzip2 => TableSink::Bzip2(bzip2::write::BzEncoder::new(
                file,
                bzip2::Compression::default(),
            )),
            #[cfg(feature = "compression")]
            Compression::Xz => TableSink::Xz(xz2::write::XzEncoder::new(file, 6)),
            #[cfg(feature = "compression")]
            Compression::Zstd => TableSink::Zstd(zstd::stream::write::Encoder::new(file, 3)?),
            #[cfg(not(feature = "compression"))]
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "compressed output requires the `compression` feature",
                ))
            }
        })
    }

    fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self {
            TableSink::Raw(f) => f.write_all(data),
            #[cfg(feature = "compression")]
            TableSink::Gzip(e) => e.write_all(data),
            #[cfg(feature = "compression")]
            TableSink::Bzip2(e) => e.write_all(data),
            #[cfg(feature = "compression")]
            TableSink::Xz(e) => e.write_all(data),
            #[cfg(feature = "compression")]
            TableSink::Zstd(e) => e.write_all(data),
        }
    }

    /// Finalize the stream, flushing the compressor's epilogue (required for a
    /// valid archive — especially zstd, which writes a frame footer).
    fn finish(self) -> std::io::Result<()> {
        match self {
            TableSink::Raw(mut f) => f.flush(),
            #[cfg(feature = "compression")]
            TableSink::Gzip(e) => e.finish().map(|_| ()),
            #[cfg(feature = "compression")]
            TableSink::Bzip2(e) => e.finish().map(|_| ()),
            #[cfg(feature = "compression")]
            TableSink::Xz(e) => e.finish().map(|_| ()),
            #[cfg(feature = "compression")]
            TableSink::Zstd(e) => e.finish().map(|_| ()),
        }
    }
}

/// Parallel, pipelined writer pool.
///
/// The producer (parser thread) stages per-table bytes and ships ~256KB chunks
/// over bounded channels to `N` writer threads, each owning a disjoint shard of
/// tables (sharded by name hash). This overlaps parsing with writing (a
/// pipeline / double-buffer) and keeps several writes in flight so a single
/// synchronous thread doesn't bottleneck a fast SSD (queue depth > 1). Batching
/// into large chunks keeps per-statement channel overhead negligible.
///
/// Output is byte-identical to the single-threaded [`WriterPool`]: each table's
/// file is written by exactly one thread, in input order.
pub struct ParallelWriters {
    senders: Vec<SyncSender<Chunk>>,
    handles: Vec<JoinHandle<std::io::Result<()>>>,
    error_flag: Arc<AtomicBool>,
    intern: AHashMap<String, Arc<str>>,
    stage: AHashMap<Arc<str>, Vec<u8>>,
    /// Total bytes currently staged across all tables (see [`STAGE_TOTAL_CAP`]).
    staged_bytes: usize,
    /// Bytes written to sinks by the writer threads (shared, monotonically
    /// increasing). One atomic add per ~256KB chunk, so contention is nil.
    bytes_acked: Arc<AtomicU64>,
    /// Nanoseconds the producer spent blocked on full writer channels.
    /// Producer-only, so a plain counter; timing happens only on the slow
    /// path (channel already full), keeping the hot path free of clock reads.
    stall_nanos: u64,
}

impl ParallelWriters {
    /// Spawn `num_writers` writer threads targeting `output_dir`, compressing
    /// each per-table file with `format`. `capacity` is the per-shard channel
    /// depth (chunks in flight).
    pub fn new(
        output_dir: PathBuf,
        num_writers: usize,
        capacity: usize,
        format: Compression,
    ) -> std::io::Result<Self> {
        fs::create_dir_all(&output_dir)?;
        let n = num_writers.max(1);
        let error_flag = Arc::new(AtomicBool::new(false));
        let bytes_acked = Arc::new(AtomicU64::new(0));
        let mut senders = Vec::with_capacity(n);
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = sync_channel::<Chunk>(capacity.max(1));
            senders.push(tx);
            let dir = output_dir.clone();
            let ef = Arc::clone(&error_flag);
            let acked = Arc::clone(&bytes_acked);
            handles.push(std::thread::spawn(move || {
                writer_loop(rx, dir, ef, format, acked)
            }));
        }
        Ok(Self {
            senders,
            handles,
            error_flag,
            intern: AHashMap::new(),
            stage: AHashMap::new(),
            staged_bytes: 0,
            bytes_acked,
            stall_nanos: 0,
        })
    }

    /// Snapshot the pipeline's instrumentation counters (monotonic since
    /// construction). Cheap enough to call at every epoch boundary.
    pub fn stats(&self) -> WriterStats {
        WriterStats {
            bytes_acked: self.bytes_acked.load(Ordering::Relaxed),
            send_stall: Duration::from_nanos(self.stall_nanos),
        }
    }

    /// True once any writer thread has hit an I/O error, so the producer can
    /// stop early; the actual error surfaces from [`finish`](Self::finish).
    #[inline]
    pub fn errored(&self) -> bool {
        self.error_flag.load(Ordering::Relaxed)
    }

    /// Stage a statement for its table, appending `suffix` (e.g. `b";"`) and a
    /// newline, shipping a chunk once the table's buffer crosses the threshold.
    pub fn write(&mut self, table_name: &str, stmt: &[u8], suffix: &[u8]) {
        let arc = match self.intern.get(table_name) {
            Some(a) => Arc::clone(a),
            None => {
                let a: Arc<str> = Arc::from(table_name);
                self.intern.insert(table_name.to_string(), Arc::clone(&a));
                a
            }
        };
        let buf = self.stage.entry(Arc::clone(&arc)).or_default();
        buf.extend_from_slice(stmt);
        buf.extend_from_slice(suffix);
        buf.push(b'\n');
        self.staged_bytes += stmt.len() + suffix.len() + 1;
        if buf.len() >= STAGE_FLUSH {
            let data = std::mem::take(buf);
            self.staged_bytes -= data.len();
            self.ship(&arc, data);
        } else if self.staged_bytes >= STAGE_TOTAL_CAP {
            // Many tables, each under the per-table threshold: flush everything
            // so total staging memory stays bounded by the cap.
            let stage = std::mem::take(&mut self.stage);
            for (table, data) in stage {
                if !data.is_empty() {
                    self.ship(&table, data);
                }
            }
            self.staged_bytes = 0;
        }
    }

    fn ship(&mut self, table: &Arc<str>, data: Vec<u8>) {
        let shard = shard_index(table, self.senders.len());
        let chunk = Chunk {
            table: Arc::clone(table),
            data,
        };
        // Fast path: the channel has room, no clock read at all. Only when the
        // writer is backed up do we time the blocking send — that time *is*
        // the `send_stall` backpressure signal. A dead writer's receiver is
        // dropped, so sends error; the real error is surfaced by `finish`,
        // so send failures are ignored here.
        match self.senders[shard].try_send(chunk) {
            Ok(()) => {}
            Err(TrySendError::Full(chunk)) => {
                let blocked = Instant::now();
                let _ = self.senders[shard].send(chunk);
                self.stall_nanos += blocked.elapsed().as_nanos() as u64;
            }
            Err(TrySendError::Disconnected(_)) => {}
        }
    }

    /// Flush all staged data, join the writer threads, and return the first
    /// I/O error encountered (if any).
    pub fn finish(mut self) -> std::io::Result<()> {
        let stage = std::mem::take(&mut self.stage);
        for (table, data) in stage {
            if !data.is_empty() {
                self.ship(&table, data);
            }
        }
        drop(self.senders);

        let mut first_err: Option<std::io::Error> = None;
        for handle in self.handles {
            match handle.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    first_err.get_or_insert(e);
                }
                Err(_) => {
                    first_err
                        .get_or_insert_with(|| std::io::Error::other("writer thread panicked"));
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

/// FNV-1a hash of the table name → shard index. Deterministic within a run so a
/// table always maps to the same writer (preserving per-file order).
#[inline]
fn shard_index(table: &str, n: usize) -> usize {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in table.as_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (h % n as u64) as usize
}

fn writer_loop(
    rx: Receiver<Chunk>,
    output_dir: PathBuf,
    error_flag: Arc<AtomicBool>,
    format: Compression,
    bytes_acked: Arc<AtomicU64>,
) -> std::io::Result<()> {
    // Chunks are already ~256KB, so write straight to the sink (each write is a
    // large syscall / compressor input) with no extra buffering layer.
    let mut sinks: AHashMap<Arc<str>, TableSink> = AHashMap::new();
    let mut first_err: Option<std::io::Error> = None;

    for chunk in rx {
        if first_err.is_some() {
            continue; // keep draining so the producer never blocks
        }
        let sink = match sinks.entry(Arc::clone(&chunk.table)) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => match TableSink::create(&output_dir, &chunk.table, format) {
                Ok(s) => e.insert(s),
                Err(err) => {
                    error_flag.store(true, Ordering::Relaxed);
                    first_err = Some(err);
                    continue;
                }
            },
        };
        match sink.write_all(&chunk.data) {
            Ok(()) => {
                bytes_acked.fetch_add(chunk.data.len() as u64, Ordering::Relaxed);
            }
            Err(err) => {
                error_flag.store(true, Ordering::Relaxed);
                first_err = Some(err);
            }
        }
    }

    // Finalize every sink so compressor epilogues are flushed (best effort even
    // after an error, so files close cleanly).
    for (_, sink) in sinks.drain() {
        if let Err(err) = sink.finish() {
            error_flag.store(true, Ordering::Relaxed);
            if first_err.is_none() {
                first_err = Some(err);
            }
        }
    }

    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

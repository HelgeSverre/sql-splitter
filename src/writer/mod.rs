//! Buffered file writers for splitting SQL statements into per-table files.

use ahash::AHashMap;
use std::collections::hash_map::Entry;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;
use std::thread::JoinHandle;

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
}

impl ParallelWriters {
    /// Spawn `num_writers` writer threads targeting `output_dir`. `capacity` is
    /// the per-shard channel depth (chunks in flight).
    pub fn new(output_dir: PathBuf, num_writers: usize, capacity: usize) -> std::io::Result<Self> {
        fs::create_dir_all(&output_dir)?;
        let n = num_writers.max(1);
        let error_flag = Arc::new(AtomicBool::new(false));
        let mut senders = Vec::with_capacity(n);
        let mut handles = Vec::with_capacity(n);
        for _ in 0..n {
            let (tx, rx) = sync_channel::<Chunk>(capacity.max(1));
            senders.push(tx);
            let dir = output_dir.clone();
            let ef = Arc::clone(&error_flag);
            handles.push(std::thread::spawn(move || writer_loop(rx, dir, ef)));
        }
        Ok(Self {
            senders,
            handles,
            error_flag,
            intern: AHashMap::new(),
            stage: AHashMap::new(),
            staged_bytes: 0,
        })
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

    fn ship(&self, table: &Arc<str>, data: Vec<u8>) {
        let shard = shard_index(table, self.senders.len());
        // A dead writer's receiver is dropped, so `send` errors; the real error
        // is surfaced by `finish`, so ignore send failures here.
        let _ = self.senders[shard].send(Chunk {
            table: Arc::clone(table),
            data,
        });
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
) -> std::io::Result<()> {
    // Chunks are already ~256KB, so write straight to the file (each write is a
    // large syscall) with no extra buffering layer.
    let mut files: AHashMap<Arc<str>, File> = AHashMap::new();
    let mut first_err: Option<std::io::Error> = None;

    for chunk in rx {
        if first_err.is_some() {
            continue; // keep draining so the producer never blocks
        }
        let file = match files.entry(Arc::clone(&chunk.table)) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let path = output_dir.join(format!("{}.sql", chunk.table));
                match File::create(&path) {
                    Ok(f) => e.insert(f),
                    Err(err) => {
                        error_flag.store(true, Ordering::Relaxed);
                        first_err = Some(err);
                        continue;
                    }
                }
            }
        };
        if let Err(err) = file.write_all(&chunk.data) {
            error_flag.store(true, Ordering::Relaxed);
            first_err = Some(err);
        }
    }

    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

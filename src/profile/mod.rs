//! Bounded profiling: turn a SQL dump's column values into fixed-size,
//! generator-neutral *evidence*.
//!
//! The profiler scans data and accumulates bounded sketches
//! ([`ColumnSketches`]) whose memory does not grow with input size — a column
//! spanning a billion rows costs the same as one spanning a thousand. The
//! result is a tree of neutral [`evidence`] structs: observations and
//! confidence only, with no opinion about how to regenerate the data. Later
//! stages (schema inference, generation heuristics) consume that evidence
//! without depending on how it was collected.
//!
//! The three properties this module guarantees, enforced by
//! `tests/generate_profile_test.rs`:
//!
//! * **Bounded** — retained items and bytes stay within the [`ProfileBudget`].
//! * **Mergeable** — sketches over disjoint partitions merge into the same
//!   bounded evidence as a single pass (exactly, for the non-sampled parts).
//! * **Seeded** — the same seed yields the same reservoir sample.

pub mod evidence;
mod profiler;
mod sketches;

pub use evidence::{
    BooleanEvidence, CharClasses, ColumnEvidence, DumpProfile, NumericEvidence,
    RelationshipEvidence, StringShapeEvidence, TableEvidence, TimestampRangeEvidence, TopKEntry,
};
pub use profiler::{DumpProfiler, DumpProfilerBuilder};
pub use sketches::ColumnSketches;

use serde::{Deserialize, Serialize};
use std::fmt;

// --- Bounding constants -----------------------------------------------------
//
// These fix the per-column memory ceiling independently of the (unbounded)
// input. `ProfileBudget::retained_bytes_ceiling` sums them so callers can
// assert the profiler never exceeds its footprint.

/// HyperLogLog precision `p`; register count is `2^p`. `p = 12` gives 4096
/// registers (~1.6% standard error), comfortably inside the 10% tolerance the
/// distinct-count tests require, for 4 KiB of fixed overhead per column.
pub(crate) const HLL_PRECISION: u32 = 12;
/// Number of HyperLogLog registers (`2^HLL_PRECISION`).
pub(crate) const HLL_REGISTERS: usize = 1 << HLL_PRECISION;
/// Byte ceiling for a single retained reservoir sample value.
pub(crate) const MAX_SAMPLE_VALUE_BYTES: usize = 256;
/// Byte ceiling for a single retained top-k key.
pub(crate) const MAX_TOPK_KEY_BYTES: usize = 256;
/// Byte ceiling for the retained common prefix / suffix of a string column.
pub(crate) const MAX_AFFIX_BYTES: usize = 64;
/// Byte ceiling for a retained min/max timestamp string.
pub(crate) const MAX_TIMESTAMP_BYTES: usize = 64;
/// Bookkeeping bytes charged per top-k counter, on top of its key.
pub(crate) const TOPK_COUNTER_OVERHEAD: usize = 16;
/// Bytes charged per numeric-histogram bin (`f64` centroid + `u64` count).
pub(crate) const HISTOGRAM_BIN_BYTES: usize = 16;

/// Errors from building or combining profile evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProfileError {
    /// Two accumulators built with incompatible configuration were merged
    /// (e.g. different reservoir capacities or histogram bin counts).
    IncompatibleMerge {
        /// Which configuration mismatched.
        detail: &'static str,
    },
}

impl ProfileError {
    pub(crate) fn incompatible_merge(detail: &'static str) -> Self {
        ProfileError::IncompatibleMerge { detail }
    }
}

impl fmt::Display for ProfileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProfileError::IncompatibleMerge { detail } => {
                write!(f, "cannot merge profile accumulators: {detail} mismatch")
            }
        }
    }
}

impl std::error::Error for ProfileError {}

/// How deeply to profile a dump.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProfileDepth {
    /// Structure only: names, types, nullability. No data scan.
    Schema,
    /// Cheap per-column stats: counts, null rate, distinct estimate, ranges.
    Basic,
    /// Everything: quantiles, heavy hitters, string shape, samples.
    Full,
}

/// Per-column bounds on how much the profiler may retain. Every accumulator is
/// sized from these three knobs; the derived methods report the resulting
/// item and byte ceilings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileBudget {
    /// Reservoir sample capacity (retained example values per column).
    pub sample_rows: usize,
    /// Number of heavy-hitter slots (Space-Saving counters).
    pub top_k: usize,
    /// Number of numeric-histogram bins.
    pub histogram_bins: usize,
}

impl Default for ProfileBudget {
    fn default() -> Self {
        Self {
            sample_rows: 1_000,
            top_k: 32,
            histogram_bins: 32,
        }
    }
}

impl ProfileBudget {
    /// Upper bound on [`ColumnSketches::retained_items`]: the reservoir, top-k,
    /// and histogram each cap at their configured size.
    pub fn retained_items_per_column(&self) -> usize {
        self.sample_rows + self.top_k + self.histogram_bins
    }

    /// Upper bound on [`ColumnSketches::retained_bytes`]: the worst-case heap
    /// footprint of every accumulator, summed. Input size never pushes actual
    /// retention past this.
    pub fn retained_bytes_ceiling(&self) -> usize {
        self.sample_rows * MAX_SAMPLE_VALUE_BYTES
            + self.top_k * (MAX_TOPK_KEY_BYTES + TOPK_COUNTER_OVERHEAD)
            + HLL_REGISTERS
            + self.histogram_bins * HISTOGRAM_BIN_BYTES
            + 2 * MAX_AFFIX_BYTES
            + 2 * MAX_TIMESTAMP_BYTES
    }
}

/// A single observed column value, borrowed for the duration of the call.
///
/// This is the profiler's neutral input alphabet: the value families the
/// sketches know how to summarize. Text/JSON/timestamp variants borrow so a
/// scan never has to allocate to observe.
#[derive(Debug, Clone, PartialEq)]
pub enum ProfileValue<'a> {
    /// SQL `NULL`.
    Null,
    /// A boolean.
    Boolean(bool),
    /// A signed integer.
    Integer(i64),
    /// A fixed-point decimal: `minor * 10^-scale`.
    Decimal { minor: i128, scale: u8 },
    /// Free text.
    Text(&'a str),
    /// A timestamp rendering (ISO-8601 recommended; compared lexically).
    DateTime(&'a str),
    /// A JSON document rendering.
    Json(&'a str),
}

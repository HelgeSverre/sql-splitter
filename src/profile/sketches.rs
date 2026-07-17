//! Mergeable, seeded, bounded accumulators that turn an unbounded stream of
//! column values into fixed-size evidence.
//!
//! The guiding invariant: **input size grows scan time, never profile
//! memory.** Every accumulator here retains at most a budget-derived number of
//! items and a budget-derived number of bytes, regardless of how many values
//! it observes. Reservoir sampling, Space-Saving, HyperLogLog, and a streaming
//! histogram each pay a fixed, provable memory cost.
//!
//! All accumulators implement the crate-private [`EvidenceAccumulator`] trait
//! so [`ColumnSketches`] can drive them uniformly. That trait stays private on
//! purpose: a second consumer has not yet proven the abstraction, and the
//! stable surface downstream code depends on is the *evidence* structs in
//! [`crate::profile::evidence`], not these internals.

use crate::profile::evidence::{
    BooleanEvidence, CharClasses, ColumnEvidence, NumericEvidence, StringShapeEvidence,
    TimestampRangeEvidence, TopKEntry,
};
use crate::profile::{
    ProfileBudget, ProfileError, ProfileValue, HISTOGRAM_BIN_BYTES, HLL_PRECISION, HLL_REGISTERS,
    MAX_AFFIX_BYTES, MAX_SAMPLE_VALUE_BYTES, MAX_TIMESTAMP_BYTES, MAX_TOPK_KEY_BYTES,
    TOPK_COUNTER_OVERHEAD,
};
use rand::RngExt;
use rand_chacha::rand_core::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;

// Type tags mixed into the distinct-count hash so that, e.g., the integer `5`
// and the text `"5"` are not conflated across a mixed column.
const TAG_BOOL: u8 = 1;
const TAG_INT: u8 = 2;
const TAG_DECIMAL: u8 = 3;
const TAG_TEXT: u8 = 4;
const TAG_DATETIME: u8 = 5;
const TAG_JSON: u8 = 6;

/// Crate-private accumulator contract shared by every sketch.
///
/// Kept private deliberately (see module docs): only [`ColumnSketches`] drives
/// these, and freezing the trait shape into the public API before a second
/// consumer exists would be premature.
pub(crate) trait EvidenceAccumulator {
    /// The per-value input this accumulator consumes.
    type Observation<'a>;
    /// The bounded summary this accumulator finalizes into.
    type Evidence;

    /// Fold one observed value in.
    fn observe(&mut self, value: Self::Observation<'_>);
    /// Merge another accumulator of the same configuration into this one.
    fn merge(&mut self, other: Self) -> Result<(), ProfileError>
    where
        Self: Sized;
    /// Consume the accumulator, producing its bounded evidence.
    fn finish(self) -> Self::Evidence
    where
        Self: Sized;
    /// Bytes currently retained on the heap by this accumulator.
    fn retained_bytes(&self) -> usize;
}

// ---------------------------------------------------------------------------
// Hashing
// ---------------------------------------------------------------------------

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// A fixed, version-stable 64-bit hash: FNV-1a for mixing content in, then a
/// SplitMix64 finalizer for avalanche (FNV alone has weak high bits, which
/// HyperLogLog's register indexing relies on being well-distributed).
///
/// This is deliberately *not* `ahash`/`DefaultHasher`: those publish no
/// cross-version stability guarantee, and the HLL estimate must be reproducible
/// for a given input.
fn stable_hash64(tag: u8, bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET;
    h ^= tag as u64;
    h = h.wrapping_mul(FNV_PRIME);
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(FNV_PRIME);
    }
    // SplitMix64 finalizer.
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d0_49bb_1331_11eb);
    h ^= h >> 31;
    h
}

// ---------------------------------------------------------------------------
// Reservoir
// ---------------------------------------------------------------------------

/// Deterministic reservoir (Algorithm R) over retained string renderings.
///
/// Seeded from a per-column seed so the same stream yields the same sample.
/// The merge produces a uniform sample of the concatenated streams by drawing
/// a hypergeometric split and subsampling each side.
pub(crate) struct Reservoir {
    capacity: usize,
    seen: u64,
    items: Vec<String>,
    rng: ChaCha8Rng,
}

impl Reservoir {
    fn new(capacity: usize, seed: u64) -> Self {
        Self {
            capacity,
            seen: 0,
            items: Vec::new(),
            rng: ChaCha8Rng::seed_from_u64(seed),
        }
    }

    fn len(&self) -> usize {
        self.items.len()
    }

    /// Keep a uniform size-`k` subset of `items` in place (partial
    /// Fisher-Yates), truncating the rest.
    fn take_sample(items: &mut Vec<String>, k: usize, rng: &mut ChaCha8Rng) {
        let len = items.len();
        for i in 0..k.min(len) {
            let j = i + rng.random_range(0..(len - i) as u64) as usize;
            items.swap(i, j);
        }
        items.truncate(k.min(len));
    }
}

impl EvidenceAccumulator for Reservoir {
    type Observation<'a> = String;
    type Evidence = Vec<String>;

    fn observe(&mut self, value: String) {
        self.seen += 1;
        if self.items.len() < self.capacity {
            self.items.push(value);
        } else {
            let j = self.rng.random_range(0..self.seen) as usize;
            if j < self.capacity {
                self.items[j] = value;
            }
        }
    }

    fn merge(&mut self, mut other: Self) -> Result<(), ProfileError> {
        if self.capacity != other.capacity {
            return Err(ProfileError::incompatible_merge("reservoir capacity"));
        }
        let (n_a, n_b) = (self.seen, other.seen);
        if n_b == 0 {
            return Ok(());
        }
        if n_a == 0 {
            self.items = other.items;
            self.seen = other.seen;
            return Ok(());
        }
        let total = n_a + n_b;
        let m = self.capacity.min(total as usize);

        // Draw x ~ Hypergeometric(total, n_a, m) by simulating m draws without
        // replacement from an urn of n_a "A" balls and n_b "B" balls. O(m).
        let (mut ra, mut rb, mut x) = (n_a, n_b, 0usize);
        for _ in 0..m {
            if self.rng.random_range(0..(ra + rb)) < ra {
                x += 1;
                ra -= 1;
            } else {
                rb -= 1;
            }
        }

        // Subsample: a uniform subset of a uniform sample is itself uniform.
        Self::take_sample(&mut self.items, x, &mut self.rng);
        Self::take_sample(&mut other.items, m - x, &mut self.rng);
        self.items.append(&mut other.items);
        self.seen = total;
        Ok(())
    }

    fn finish(self) -> Vec<String> {
        self.items
    }

    fn retained_bytes(&self) -> usize {
        self.items.iter().map(|s| s.len()).sum()
    }
}

// ---------------------------------------------------------------------------
// Space-Saving top-k
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct Counter {
    count: u64,
    error: u64,
}

/// Space-Saving heavy hitters (Metwally et al.), bounded to `capacity`
/// counters. Uses a `BTreeMap` so eviction tie-breaks and the merge are
/// deterministic across runs.
pub(crate) struct SpaceSavingTopK {
    capacity: usize,
    counters: BTreeMap<String, Counter>,
}

impl SpaceSavingTopK {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            counters: BTreeMap::new(),
        }
    }

    fn len(&self) -> usize {
        self.counters.len()
    }

    fn min_count(&self) -> u64 {
        self.counters.values().map(|c| c.count).min().unwrap_or(0)
    }

    /// The minimum count a merge should attribute to items *absent* from this
    /// summary: an item we never tracked could have occurred up to the
    /// smallest tracked count (0 if the summary is not yet full).
    fn merge_floor(&self) -> u64 {
        if self.counters.len() >= self.capacity {
            self.min_count()
        } else {
            0
        }
    }
}

impl EvidenceAccumulator for SpaceSavingTopK {
    type Observation<'a> = &'a str;
    type Evidence = Vec<TopKEntry>;

    fn observe(&mut self, key: &str) {
        if let Some(counter) = self.counters.get_mut(key) {
            counter.count += 1;
            return;
        }
        if self.counters.len() < self.capacity {
            self.counters
                .insert(key.to_owned(), Counter { count: 1, error: 0 });
            return;
        }
        // Evict the smallest counter and reuse its slot; the newcomer inherits
        // the evicted count as its maximum overestimate.
        let victim = self
            .counters
            .iter()
            .min_by(|a, b| a.1.count.cmp(&b.1.count).then(a.0.cmp(b.0)))
            .map(|(k, c)| (k.clone(), c.count));
        if let Some((victim_key, victim_count)) = victim {
            self.counters.remove(&victim_key);
            self.counters.insert(
                key.to_owned(),
                Counter {
                    count: victim_count + 1,
                    error: victim_count,
                },
            );
        }
    }

    fn merge(&mut self, other: Self) -> Result<(), ProfileError> {
        if self.capacity != other.capacity {
            return Err(ProfileError::incompatible_merge("top-k capacity"));
        }
        let a_floor = self.merge_floor();
        let b_floor = other.merge_floor();

        let mut merged: BTreeMap<String, Counter> = BTreeMap::new();
        for (key, a) in &self.counters {
            let (cb, eb) = other
                .counters
                .get(key)
                .map(|c| (c.count, c.error))
                .unwrap_or((b_floor, b_floor));
            merged.insert(
                key.clone(),
                Counter {
                    count: a.count + cb,
                    error: a.error + eb,
                },
            );
        }
        for (key, b) in &other.counters {
            if self.counters.contains_key(key) {
                continue;
            }
            merged.insert(
                key.clone(),
                Counter {
                    count: b.count + a_floor,
                    error: b.error + a_floor,
                },
            );
        }

        if merged.len() > self.capacity {
            let mut entries: Vec<(String, Counter)> = merged.into_iter().collect();
            entries.sort_by(|a, b| b.1.count.cmp(&a.1.count).then(a.0.cmp(&b.0)));
            entries.truncate(self.capacity);
            merged = entries.into_iter().collect();
        }
        self.counters = merged;
        Ok(())
    }

    fn finish(self) -> Vec<TopKEntry> {
        let mut entries: Vec<TopKEntry> = self
            .counters
            .into_iter()
            .map(|(value, c)| TopKEntry {
                value,
                count: c.count,
                error: c.error,
            })
            .collect();
        entries.sort_by(|a, b| b.count.cmp(&a.count).then(a.value.cmp(&b.value)));
        entries
    }

    fn retained_bytes(&self) -> usize {
        self.counters
            .keys()
            .map(|k| k.len() + TOPK_COUNTER_OVERHEAD)
            .sum()
    }
}

// ---------------------------------------------------------------------------
// HyperLogLog
// ---------------------------------------------------------------------------

/// HyperLogLog distinct-count estimator over precomputed 64-bit hashes.
///
/// Fixed `HLL_REGISTERS` registers; merge is register-wise max, which makes the
/// merged estimate *exactly* equal to a single pass over the concatenated
/// stream (order- and partition-independent).
pub(crate) struct HyperLogLog {
    registers: Vec<u8>,
}

impl HyperLogLog {
    fn new() -> Self {
        Self {
            registers: vec![0; HLL_REGISTERS],
        }
    }

    fn observe_hash(&mut self, h: u64) {
        let index = (h >> (64 - HLL_PRECISION)) as usize;
        let w = h << HLL_PRECISION;
        let rho = if w == 0 {
            (64 - HLL_PRECISION) as u8 + 1
        } else {
            w.leading_zeros() as u8 + 1
        };
        if rho > self.registers[index] {
            self.registers[index] = rho;
        }
    }

    fn estimate(&self) -> f64 {
        let m = HLL_REGISTERS as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / m);
        let sum: f64 = self.registers.iter().map(|&r| 2f64.powi(-(r as i32))).sum();
        let raw = alpha * m * m / sum;
        // Small-range linear-counting correction. With 64-bit hashing the
        // large-range (2^32) correction is unnecessary.
        if raw <= 2.5 * m {
            let zeros = self.registers.iter().filter(|&&r| r == 0).count();
            if zeros != 0 {
                return m * (m / zeros as f64).ln();
            }
        }
        raw
    }
}

impl EvidenceAccumulator for HyperLogLog {
    type Observation<'a> = u64;
    type Evidence = f64;

    fn observe(&mut self, hash: u64) {
        self.observe_hash(hash);
    }

    fn merge(&mut self, other: Self) -> Result<(), ProfileError> {
        if self.registers.len() != other.registers.len() {
            return Err(ProfileError::incompatible_merge("hll precision"));
        }
        for (slot, incoming) in self.registers.iter_mut().zip(other.registers) {
            *slot = (*slot).max(incoming);
        }
        Ok(())
    }

    fn finish(self) -> f64 {
        self.estimate()
    }

    fn retained_bytes(&self) -> usize {
        self.registers.len()
    }
}

// ---------------------------------------------------------------------------
// Numeric histogram
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct Bin {
    value: f64,
    count: u64,
}

/// Ben-Haim & Tom-Tov streaming histogram: at most `max_bins` (centroid,
/// count) bins, adjacent bins merged by nearest centroid when the cap is
/// exceeded. Mergeable and supports interpolated quantiles.
pub(crate) struct NumericHistogram {
    max_bins: usize,
    bins: Vec<Bin>,
    min: f64,
    max: f64,
    total: u64,
    sum: f64,
}

impl NumericHistogram {
    fn new(max_bins: usize) -> Self {
        Self {
            max_bins: max_bins.max(1),
            bins: Vec::new(),
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            total: 0,
            sum: 0.0,
        }
    }

    fn len(&self) -> usize {
        self.bins.len()
    }

    fn insert_value(&mut self, value: f64, count: u64) {
        match self.bins.binary_search_by(|b| {
            b.value
                .partial_cmp(&value)
                .unwrap_or(std::cmp::Ordering::Equal)
        }) {
            Ok(i) => self.bins[i].count += count,
            Err(i) => self.bins.insert(i, Bin { value, count }),
        }
    }

    /// Merge the two closest-centroid adjacent bins until within `max_bins`.
    fn shrink(&mut self) {
        while self.bins.len() > self.max_bins {
            let mut best = 0usize;
            let mut best_gap = f64::INFINITY;
            for i in 0..self.bins.len() - 1 {
                let gap = self.bins[i + 1].value - self.bins[i].value;
                if gap < best_gap {
                    best_gap = gap;
                    best = i;
                }
            }
            let right = self.bins[best + 1];
            let left = self.bins[best];
            let count = left.count + right.count;
            let value =
                (left.value * left.count as f64 + right.value * right.count as f64) / count as f64;
            self.bins[best] = Bin { value, count };
            self.bins.remove(best + 1);
        }
    }

    fn quantile(&self, q: f64) -> f64 {
        if self.total == 0 {
            return f64::NAN;
        }
        if self.bins.len() == 1 {
            return self.bins[0].value;
        }
        let n = self.total as f64;
        let rank = q.clamp(0.0, 1.0) * n;

        // Piecewise-linear inverse CDF anchored at (0, min) .. (n, max) with
        // interior points at each bin's cumulative centroid.
        let mut xs = Vec::with_capacity(self.bins.len() + 2);
        let mut ys = Vec::with_capacity(self.bins.len() + 2);
        xs.push(0.0);
        ys.push(self.min);
        let mut cum = 0.0;
        for b in &self.bins {
            let c = b.count as f64;
            xs.push(cum + c / 2.0);
            ys.push(b.value);
            cum += c;
        }
        xs.push(n);
        ys.push(self.max);

        for w in 1..xs.len() {
            if rank <= xs[w] {
                let (x0, x1, y0, y1) = (xs[w - 1], xs[w], ys[w - 1], ys[w]);
                if x1 == x0 {
                    return y1;
                }
                let t = (rank - x0) / (x1 - x0);
                return y0 + t * (y1 - y0);
            }
        }
        self.max
    }
}

impl EvidenceAccumulator for NumericHistogram {
    type Observation<'a> = f64;
    type Evidence = NumericEvidence;

    fn observe(&mut self, value: f64) {
        self.total += 1;
        self.sum += value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.insert_value(value, 1);
        if self.bins.len() > self.max_bins {
            self.shrink();
        }
    }

    fn merge(&mut self, other: Self) -> Result<(), ProfileError> {
        if self.max_bins != other.max_bins {
            return Err(ProfileError::incompatible_merge("histogram bins"));
        }
        for b in other.bins {
            self.insert_value(b.value, b.count);
        }
        self.total += other.total;
        self.sum += other.sum;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.shrink();
        Ok(())
    }

    fn finish(self) -> NumericEvidence {
        let mean = if self.total == 0 {
            0.0
        } else {
            self.sum / self.total as f64
        };
        NumericEvidence {
            min: self.min,
            max: self.max,
            mean,
            p50: self.quantile(0.50),
            p90: self.quantile(0.90),
            p99: self.quantile(0.99),
        }
    }

    fn retained_bytes(&self) -> usize {
        self.bins.len() * HISTOGRAM_BIN_BYTES
    }
}

// ---------------------------------------------------------------------------
// String shape
// ---------------------------------------------------------------------------

/// Length, empty-rate, alphabet, and longest-common prefix/suffix of a text
/// column. Prefix/suffix are capped at `MAX_AFFIX_BYTES`; everything else is a
/// running scalar, so memory is fixed regardless of stream length.
pub(crate) struct StringShapeSketch {
    count: u64,
    empty: u64,
    min_len: usize,
    max_len: usize,
    total_len: u128,
    classes: CharClasses,
    prefix: Option<String>,
    suffix: Option<String>,
    truncated_affix: bool,
}

impl StringShapeSketch {
    fn new() -> Self {
        Self {
            count: 0,
            empty: 0,
            min_len: usize::MAX,
            max_len: 0,
            total_len: 0,
            classes: CharClasses::default(),
            prefix: None,
            suffix: None,
            truncated_affix: false,
        }
    }

    fn classify(classes: &mut CharClasses, s: &str) {
        for ch in s.chars() {
            if ch.is_ascii_lowercase() {
                classes.lower = true;
            } else if ch.is_ascii_uppercase() {
                classes.upper = true;
            } else if ch.is_ascii_digit() {
                classes.digit = true;
            } else if ch.is_whitespace() {
                classes.whitespace = true;
            } else if ch.is_ascii_punctuation() {
                classes.punctuation = true;
            } else if !ch.is_ascii() {
                classes.non_ascii = true;
            }
        }
    }

    /// Longest common prefix of two strings, capped at `MAX_AFFIX_BYTES` bytes
    /// (on a char boundary). Returns whether the cap was hit.
    fn common_prefix(a: &str, b: &str) -> (String, bool) {
        let mut end = 0;
        for (ca, cb) in a.char_indices().zip(b.chars()) {
            let (idx, ch) = ca;
            if ch != cb {
                break;
            }
            let next = idx + ch.len_utf8();
            if next > MAX_AFFIX_BYTES {
                return (a[..idx].to_owned(), true);
            }
            end = next;
        }
        (a[..end].to_owned(), false)
    }

    fn common_suffix(a: &str, b: &str) -> (String, bool) {
        let mut bytes = 0usize;
        let mut ai = a.chars().rev();
        let mut bi = b.chars().rev();
        loop {
            match (ai.next(), bi.next()) {
                (Some(ca), Some(cb)) if ca == cb => {
                    if bytes + ca.len_utf8() > MAX_AFFIX_BYTES {
                        return (a[a.len() - bytes..].to_owned(), true);
                    }
                    bytes += ca.len_utf8();
                }
                _ => break,
            }
        }
        (a[a.len() - bytes..].to_owned(), false)
    }
}

impl EvidenceAccumulator for StringShapeSketch {
    type Observation<'a> = &'a str;
    type Evidence = StringShapeEvidence;

    fn observe(&mut self, s: &str) {
        self.count += 1;
        if s.is_empty() {
            self.empty += 1;
        }
        let len = s.chars().count();
        self.min_len = self.min_len.min(len);
        self.max_len = self.max_len.max(len);
        self.total_len += len as u128;
        Self::classify(&mut self.classes, s);

        match &self.prefix {
            None => {
                let (p, trunc) = Self::common_prefix(s, s);
                self.prefix = Some(p);
                self.truncated_affix |= trunc;
            }
            Some(current) => {
                let (p, trunc) = Self::common_prefix(current, s);
                self.prefix = Some(p);
                self.truncated_affix |= trunc;
            }
        }
        match &self.suffix {
            None => {
                let (sfx, trunc) = Self::common_suffix(s, s);
                self.suffix = Some(sfx);
                self.truncated_affix |= trunc;
            }
            Some(current) => {
                let (sfx, trunc) = Self::common_suffix(current, s);
                self.suffix = Some(sfx);
                self.truncated_affix |= trunc;
            }
        }
    }

    fn merge(&mut self, other: Self) -> Result<(), ProfileError> {
        self.count += other.count;
        self.empty += other.empty;
        if other.count > 0 {
            self.min_len = self.min_len.min(other.min_len);
            self.max_len = self.max_len.max(other.max_len);
        }
        self.total_len += other.total_len;
        self.classes.lower |= other.classes.lower;
        self.classes.upper |= other.classes.upper;
        self.classes.digit |= other.classes.digit;
        self.classes.whitespace |= other.classes.whitespace;
        self.classes.punctuation |= other.classes.punctuation;
        self.classes.non_ascii |= other.classes.non_ascii;
        self.truncated_affix |= other.truncated_affix;

        self.prefix = match (self.prefix.take(), other.prefix) {
            (Some(a), Some(b)) => {
                let (p, trunc) = Self::common_prefix(&a, &b);
                self.truncated_affix |= trunc;
                Some(p)
            }
            (a, b) => a.or(b),
        };
        self.suffix = match (self.suffix.take(), other.suffix) {
            (Some(a), Some(b)) => {
                let (s, trunc) = Self::common_suffix(&a, &b);
                self.truncated_affix |= trunc;
                Some(s)
            }
            (a, b) => a.or(b),
        };
        Ok(())
    }

    fn finish(self) -> StringShapeEvidence {
        let mean_len = if self.count == 0 {
            0.0
        } else {
            self.total_len as f64 / self.count as f64
        };
        StringShapeEvidence {
            count: self.count,
            empty_count: self.empty,
            empty_rate: if self.count == 0 {
                0.0
            } else {
                self.empty as f64 / self.count as f64
            },
            min_len: if self.count == 0 { 0 } else { self.min_len },
            max_len: self.max_len,
            mean_len,
            classes: self.classes,
            common_prefix: self.prefix.unwrap_or_default(),
            common_suffix: self.suffix.unwrap_or_default(),
            truncated_affix: self.truncated_affix,
        }
    }

    fn retained_bytes(&self) -> usize {
        self.prefix.as_ref().map_or(0, String::len) + self.suffix.as_ref().map_or(0, String::len)
    }
}

// ---------------------------------------------------------------------------
// Column bundle
// ---------------------------------------------------------------------------

/// Truncate a string to at most `max` bytes on a char boundary, reporting
/// whether truncation occurred.
fn truncate_bytes(s: &str, max: usize) -> (&str, bool) {
    if s.len() <= max {
        return (s, false);
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    (&s[..end], true)
}

/// Render a fixed-point decimal `minor * 10^-scale` as a human-readable string.
fn format_decimal(minor: i128, scale: u8) -> String {
    if scale == 0 {
        return minor.to_string();
    }
    let scale = scale as usize;
    let neg = minor < 0;
    let digits = minor.unsigned_abs().to_string();
    let body = if digits.len() <= scale {
        format!("0.{digits:0>scale$}")
    } else {
        let point = digits.len() - scale;
        format!("{}.{}", &digits[..point], &digits[point..])
    };
    if neg {
        format!("-{body}")
    } else {
        body
    }
}

/// All per-column accumulators bundled behind one seeded, mergeable, bounded
/// facade. Feed values with [`observe`](Self::observe); combine partitions with
/// [`merge`](Self::merge); finalize to [`ColumnEvidence`] with
/// [`finish`](Self::finish).
pub struct ColumnSketches {
    total: u64,
    nulls: u64,
    bool_true: u64,
    bool_false: u64,
    saw_bool: bool,
    hll: HyperLogLog,
    topk: SpaceSavingTopK,
    reservoir: Reservoir,
    histogram: NumericHistogram,
    saw_numeric: bool,
    max_scale: Option<u8>,
    string_shape: StringShapeSketch,
    saw_string: bool,
    ts_min: Option<String>,
    ts_max: Option<String>,
    saw_ts: bool,
    json_valid: u64,
    json_total: u64,
    truncated_samples: u64,
}

impl ColumnSketches {
    /// Build a fresh set of accumulators sized to `budget` and seeded by
    /// `seed` (only the reservoir is randomized; the seed makes its sample
    /// reproducible).
    pub fn new(budget: &ProfileBudget, seed: u64) -> Self {
        Self {
            total: 0,
            nulls: 0,
            bool_true: 0,
            bool_false: 0,
            saw_bool: false,
            hll: HyperLogLog::new(),
            topk: SpaceSavingTopK::new(budget.top_k),
            reservoir: Reservoir::new(budget.sample_rows, seed),
            histogram: NumericHistogram::new(budget.histogram_bins),
            saw_numeric: false,
            max_scale: None,
            string_shape: StringShapeSketch::new(),
            saw_string: false,
            ts_min: None,
            ts_max: None,
            saw_ts: false,
            json_valid: 0,
            json_total: 0,
            truncated_samples: 0,
        }
    }

    /// Fold one column value into every relevant accumulator.
    pub fn observe(&mut self, value: ProfileValue<'_>) {
        self.total += 1;
        match value {
            ProfileValue::Null => self.nulls += 1,
            ProfileValue::Boolean(b) => {
                self.saw_bool = true;
                if b {
                    self.bool_true += 1;
                } else {
                    self.bool_false += 1;
                }
                self.hll.observe(stable_hash64(TAG_BOOL, &[b as u8]));
            }
            ProfileValue::Integer(i) => {
                self.saw_numeric = true;
                self.histogram.observe(i as f64);
                self.hll.observe(stable_hash64(TAG_INT, &i.to_le_bytes()));
                self.record_sample(&i.to_string());
            }
            ProfileValue::Decimal { minor, scale } => {
                self.saw_numeric = true;
                self.histogram
                    .observe(minor as f64 / 10f64.powi(scale as i32));
                self.max_scale = Some(self.max_scale.map_or(scale, |s| s.max(scale)));
                let rendered = format_decimal(minor, scale);
                self.hll
                    .observe(stable_hash64(TAG_DECIMAL, rendered.as_bytes()));
                self.record_sample(&rendered);
            }
            ProfileValue::Text(s) => {
                self.saw_string = true;
                self.string_shape.observe(s);
                self.hll.observe(stable_hash64(TAG_TEXT, s.as_bytes()));
                self.record_sample(s);
            }
            ProfileValue::DateTime(s) => {
                self.saw_ts = true;
                self.update_timestamp(s);
                self.hll.observe(stable_hash64(TAG_DATETIME, s.as_bytes()));
                self.record_sample(s);
            }
            ProfileValue::Json(s) => {
                self.json_total += 1;
                if serde_json::from_str::<serde_json::Value>(s).is_ok() {
                    self.json_valid += 1;
                }
                self.hll.observe(stable_hash64(TAG_JSON, s.as_bytes()));
                self.record_sample(s);
            }
        }
    }

    /// Route a rendered scalar into the byte-bounded reservoir and top-k,
    /// truncating to the sample/key ceilings and recording truncation.
    fn record_sample(&mut self, rendered: &str) {
        let (key, key_trunc) = truncate_bytes(rendered, MAX_TOPK_KEY_BYTES);
        self.topk.observe(key);
        let (sample, sample_trunc) = truncate_bytes(rendered, MAX_SAMPLE_VALUE_BYTES);
        if key_trunc || sample_trunc {
            self.truncated_samples += 1;
        }
        self.reservoir.observe(sample.to_owned());
    }

    fn update_timestamp(&mut self, s: &str) {
        let (capped, _) = truncate_bytes(s, MAX_TIMESTAMP_BYTES);
        match &self.ts_min {
            Some(current) if current.as_str() <= capped => {}
            _ => self.ts_min = Some(capped.to_owned()),
        }
        match &self.ts_max {
            Some(current) if current.as_str() >= capped => {}
            _ => self.ts_max = Some(capped.to_owned()),
        }
    }

    /// Merge another partition's sketches into this one. Fails only if the two
    /// were built with incompatible budgets.
    pub fn merge(&mut self, other: ColumnSketches) -> Result<(), ProfileError> {
        self.hll.merge(other.hll)?;
        self.topk.merge(other.topk)?;
        self.reservoir.merge(other.reservoir)?;
        self.histogram.merge(other.histogram)?;
        self.string_shape.merge(other.string_shape)?;

        self.total += other.total;
        self.nulls += other.nulls;
        self.bool_true += other.bool_true;
        self.bool_false += other.bool_false;
        self.saw_bool |= other.saw_bool;
        self.saw_numeric |= other.saw_numeric;
        self.saw_string |= other.saw_string;
        self.saw_ts |= other.saw_ts;
        self.json_valid += other.json_valid;
        self.json_total += other.json_total;
        self.truncated_samples += other.truncated_samples;
        self.max_scale = match (self.max_scale, other.max_scale) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        if let Some(min) = other.ts_min {
            self.update_timestamp(&min);
        }
        if let Some(max) = other.ts_max {
            self.update_timestamp(&max);
        }
        Ok(())
    }

    /// HyperLogLog estimate of distinct non-null values.
    pub fn distinct_estimate(&self) -> f64 {
        self.hll.estimate()
    }

    /// Count of variably-retained items (reservoir + top-k + histogram bins).
    /// Fixed-size structures (HLL registers) are excluded — they are memory,
    /// not budget-scaled "items".
    pub fn retained_items(&self) -> usize {
        self.reservoir.len() + self.topk.len() + self.histogram.len()
    }

    /// Honest count of bytes retained across every accumulator.
    pub fn retained_bytes(&self) -> usize {
        self.hll.retained_bytes()
            + self.topk.retained_bytes()
            + self.reservoir.retained_bytes()
            + self.histogram.retained_bytes()
            + self.string_shape.retained_bytes()
            + self.ts_min.as_ref().map_or(0, String::len)
            + self.ts_max.as_ref().map_or(0, String::len)
    }

    fn confidence(&self) -> f64 {
        let non_null = self.total.saturating_sub(self.nulls);
        1.0 - 1.0 / (1.0 + non_null as f64)
    }

    /// Consume the sketches into neutral, bounded [`ColumnEvidence`].
    pub fn finish(self) -> ColumnEvidence {
        let null_rate = if self.total == 0 {
            0.0
        } else {
            self.nulls as f64 / self.total as f64
        };
        let confidence = self.confidence();
        let distinct_estimate = self.hll.estimate();

        let boolean = self.saw_bool.then_some(BooleanEvidence {
            true_count: self.bool_true,
            false_count: self.bool_false,
        });
        let numeric = self.saw_numeric.then(|| self.histogram.finish());
        let string_shape = self.saw_string.then(|| self.string_shape.finish());
        let timestamp_range = if self.saw_ts {
            match (self.ts_min, self.ts_max) {
                (Some(min), Some(max)) => Some(TimestampRangeEvidence { min, max }),
                _ => None,
            }
        } else {
            None
        };
        let json_valid_rate =
            (self.json_total > 0).then(|| self.json_valid as f64 / self.json_total as f64);

        ColumnEvidence {
            name: String::new(),
            total_count: self.total,
            null_count: self.nulls,
            null_rate,
            distinct_estimate,
            sample_values: self.reservoir.finish(),
            truncated_sample_count: self.truncated_samples,
            boolean,
            numeric,
            decimal_scale: self.max_scale,
            string_shape,
            top_k: self.topk.finish(),
            timestamp_range,
            json_valid_rate,
            confidence,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hll_merge_equals_single_pass() {
        let mut whole = HyperLogLog::new();
        let mut left = HyperLogLog::new();
        let mut right = HyperLogLog::new();
        for i in 0..10_000u64 {
            let h = stable_hash64(TAG_INT, &i.to_le_bytes());
            whole.observe(h);
            if i % 2 == 0 {
                left.observe(h);
            } else {
                right.observe(h);
            }
        }
        left.merge(right).unwrap();
        assert_eq!(left.estimate(), whole.estimate());
    }

    #[test]
    fn format_decimal_places_the_point() {
        assert_eq!(format_decimal(1050, 2), "10.50");
        assert_eq!(format_decimal(5, 3), "0.005");
        assert_eq!(format_decimal(-1234, 2), "-12.34");
        assert_eq!(format_decimal(42, 0), "42");
    }

    #[test]
    fn truncate_bytes_respects_char_boundaries() {
        let (s, trunc) = truncate_bytes("héllo", 2);
        assert!(trunc);
        assert_eq!(s, "h");
    }
}

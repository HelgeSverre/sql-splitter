//! Adaptive-I/O epoch machinery for [`Splitter::split`](super::Splitter::split).
//!
//! Epochs are *byte-based* (every N input bytes) so the controller's decisions
//! are a pure function of measured data, not wall-clock scheduling. This
//! module owns the boundary bookkeeping — when to measure, the counter deltas
//! that form an [`EpochMeasurement`], and applying the controller's decision
//! to the writer pool — keeping `split()`'s statement loop down to a single
//! call. Design: `docs/features/ADAPTIVE_IO_PROFILES.md`.

use super::Stats;
use crate::writer::{
    Clock, Controller, EpochMeasurement, ParallelWriters, ProfileValues, WriterProfile, WriterStats,
};
use std::sync::Arc;
use std::time::Duration;

/// Advance the adaptive-I/O epoch boundary past `processed`, skipping every
/// boundary a single oversized statement crossed in one step. Exactly one
/// measurement fires per crossing, and the returned boundary is always
/// strictly ahead of `processed` — so the next statement can never trigger a
/// degenerate back-to-back (zero-ish duration) epoch.
#[doc(hidden)]
pub fn advance_epoch_boundary(mut next_epoch: u64, epoch_bytes: u64, processed: u64) -> u64 {
    let epoch_bytes = epoch_bytes.max(1);
    while next_epoch <= processed {
        next_epoch += epoch_bytes;
    }
    next_epoch
}

/// Drives the adaptive-I/O feedback loop from inside the split statement
/// loop: detects epoch boundaries, turns writer-counter deltas into
/// [`EpochMeasurement`]s, and applies the [`Controller`]'s decisions
/// (profile-value swaps, deferred writer growth) to the pool.
pub(crate) struct EpochDriver {
    controller: Controller,
    clock: Arc<dyn Clock>,
    /// Runtime-swappable profile values shared with the writer threads; a
    /// controller transition retunes them mid-run.
    values: Arc<ProfileValues>,
    cores: usize,
    compressing: bool,
    epoch_bytes: u64,
    /// Next byte boundary at which to measure (always ahead of the bytes
    /// processed so far).
    next_epoch: u64,
    epoch_start: Duration,
    epoch_acked: u64,
    epoch_stall: Duration,
    /// Hidden debugging aid: dump per-epoch measurements to stderr (the
    /// phase-0 "io stats" seam from the design doc).
    io_debug: bool,
}

impl EpochDriver {
    pub(crate) fn new(
        controller: Controller,
        clock: Arc<dyn Clock>,
        values: Arc<ProfileValues>,
        epoch_bytes: u64,
        cores: usize,
        compressing: bool,
    ) -> Self {
        let epoch_start = clock.now();
        Self {
            controller,
            clock,
            values,
            cores,
            compressing,
            epoch_bytes,
            next_epoch: epoch_bytes,
            epoch_start,
            epoch_acked: 0,
            epoch_stall: Duration::ZERO,
            io_debug: std::env::var("SQL_SPLITTER_IO_DEBUG").is_ok(),
        }
    }

    /// Advance past every boundary the last statement crossed and compute the
    /// epoch's measurement from the writer-counter deltas. Assumes the caller
    /// already checked that a boundary was crossed.
    fn measure(&mut self, processed: u64, snapshot: WriterStats) -> EpochMeasurement {
        // Catch up past every boundary this statement crossed: a single
        // statement larger than `epoch_bytes` would otherwise leave
        // `next_epoch` behind the processed count, firing a degenerate
        // (microsecond) epoch on the very next statement.
        self.next_epoch = advance_epoch_boundary(self.next_epoch, self.epoch_bytes, processed);
        let measurement = EpochMeasurement {
            bytes: snapshot.bytes_acked.saturating_sub(self.epoch_acked),
            duration: self.clock.now().saturating_sub(self.epoch_start),
            send_stall: snapshot.send_stall.saturating_sub(self.epoch_stall),
        };
        self.epoch_acked = snapshot.bytes_acked;
        self.epoch_stall = snapshot.send_stall;
        self.epoch_start = self.clock.now();
        measurement
    }

    /// Evaluate one loop iteration: no-op until `stats.bytes_processed`
    /// crosses the next epoch boundary, then measure the epoch, run the
    /// controller, and apply its decision to `writers` and `stats`.
    pub(crate) fn on_bytes(&mut self, writers: &mut ParallelWriters, stats: &mut Stats) {
        if stats.bytes_processed < self.next_epoch {
            return;
        }
        let measurement = self.measure(stats.bytes_processed, writers.stats());
        if self.io_debug {
            eprintln!(
                "epoch: bytes={} dur={:?} stall={:?} tp={:.1} stall_frac={:.2}",
                measurement.bytes,
                measurement.duration,
                measurement.send_stall,
                measurement.throughput_mbps(),
                measurement.stall_fraction()
            );
        }
        let decision = self.controller.on_epoch(&measurement);
        if let Some(kind) = decision.transition {
            let new_profile =
                WriterProfile::for_kind(kind, self.cores, self.compressing).with_env_overrides();
            self.values.apply(&new_profile);
            let line = format!(
                "output device sustaining ~{:.0} MB/s — switching to {} write profile",
                decision.throughput_mbps,
                kind.label()
            );
            eprintln!("{line}");
            stats.io_transitions.push(line);
        }
        if decision.target_writers > writers.writer_count() {
            writers.grow_to(decision.target_writers);
        }
        stats.writers_used = writers.writer_count();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::writer::{MockClock, ProfileKind};

    fn driver(epoch_bytes: u64, step: Duration) -> EpochDriver {
        let profile = WriterProfile::for_kind(ProfileKind::Ssd, 4, false);
        EpochDriver::new(
            Controller::new(ProfileKind::Ssd, 4),
            Arc::new(MockClock::stepping(step)),
            Arc::new(ProfileValues::new(&profile)),
            epoch_bytes,
            4,
            false,
        )
    }

    #[test]
    fn measure_reports_deltas_between_boundaries() {
        // MockClock steps 100ms per now(): construction consumes one tick,
        // each measure() consumes two (duration read + epoch restart).
        let mut d = driver(100, Duration::from_millis(100));

        let first = d.measure(
            150,
            WriterStats {
                bytes_acked: 1000,
                send_stall: Duration::from_millis(30),
            },
        );
        assert_eq!(first.bytes, 1000);
        assert_eq!(first.duration, Duration::from_millis(100));
        assert_eq!(first.send_stall, Duration::from_millis(30));

        // Second epoch sees only the deltas since the first snapshot.
        let second = d.measure(
            250,
            WriterStats {
                bytes_acked: 1600,
                send_stall: Duration::from_millis(50),
            },
        );
        assert_eq!(second.bytes, 600);
        assert_eq!(second.duration, Duration::from_millis(100));
        assert_eq!(second.send_stall, Duration::from_millis(20));
    }

    #[test]
    fn oversized_statement_skips_crossed_boundaries() {
        let mut d = driver(100, Duration::from_millis(100));
        // One statement blows past boundaries 100..=500: a single measurement
        // fires and the next boundary lands strictly ahead of the bytes seen.
        d.measure(555, WriterStats::default());
        assert_eq!(d.next_epoch, 600);
    }
}

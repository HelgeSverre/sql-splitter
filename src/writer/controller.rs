//! Feedback controller for the adaptive I/O profiles.
//!
//! The controller is a pure state machine: `(state, epoch measurement) →
//! (state, actions)`. It never touches a clock or a device itself — epochs
//! are *byte-based* (every N input bytes) and durations arrive as data, which
//! is what makes every decision reproducible in tests with a mock clock.
//! See `docs/features/ADAPTIVE_IO_PROFILES.md` for the full design.

use super::profile::ProfileKind;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Monotonic time source for epoch measurements.
///
/// Returns time elapsed since an arbitrary per-clock origin. The production
/// implementation wraps [`std::time::Instant`]; tests use [`MockClock`] so
/// controller behavior is a pure function of the scripted inputs.
pub trait Clock: Send + Sync {
    fn now(&self) -> Duration;
}

/// Wall-clock [`Clock`] backed by [`std::time::Instant`].
#[derive(Debug)]
pub struct RealClock {
    origin: std::time::Instant,
}

impl RealClock {
    pub fn new() -> Self {
        Self {
            origin: std::time::Instant::now(),
        }
    }
}

impl Default for RealClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for RealClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

/// Deterministic [`Clock`] for tests: every `now()` call advances time by a
/// fixed step, so a pipeline that samples the clock once per epoch boundary
/// sees identical epoch durations on every machine, every run.
#[derive(Debug)]
pub struct MockClock {
    nanos: AtomicU64,
    step_nanos: u64,
}

impl MockClock {
    /// A clock that advances `step` per `now()` call.
    pub fn stepping(step: Duration) -> Self {
        Self {
            nanos: AtomicU64::new(0),
            step_nanos: step.as_nanos() as u64,
        }
    }
}

impl Clock for MockClock {
    fn now(&self) -> Duration {
        let t = self.nanos.fetch_add(self.step_nanos, Ordering::Relaxed);
        Duration::from_nanos(t + self.step_nanos)
    }
}

/// What the pipeline observed during one byte-based epoch.
#[derive(Debug, Clone, Copy)]
pub struct EpochMeasurement {
    /// Bytes acked by the writer threads during the epoch.
    pub bytes: u64,
    /// Wall time the epoch spanned.
    pub duration: Duration,
    /// Time the producer spent blocked on full writer channels in the epoch.
    pub send_stall: Duration,
}

impl EpochMeasurement {
    /// Writer throughput in MB/s (MiB, matching the measurement notes).
    pub fn throughput_mbps(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs <= 0.0 {
            return f64::INFINITY;
        }
        self.bytes as f64 / (1024.0 * 1024.0) / secs
    }

    /// Fraction of the epoch the producer sat blocked on channel sends.
    pub fn stall_fraction(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs <= 0.0 {
            return 0.0;
        }
        self.send_stall.as_secs_f64() / secs
    }
}

/// What the splitter should do after an epoch boundary.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EpochDecision {
    /// `Some(kind)` when the controller changed state: swap the profile
    /// values and emit the transition log line.
    pub transition: Option<ProfileKind>,
    /// Desired writer-thread count. The pool only ever grows toward this
    /// (shrinking would reassign table ownership and break output ordering).
    pub target_writers: usize,
    /// Observed throughput for this epoch, for the transition log line.
    pub throughput_mbps: f64,
}

/// `FAST → SLOW_SEEK` fires below this throughput (with high stall).
const FAST_DOWN_MBPS: f64 = 150.0;
/// `SLOW_SEEK → SLOW_OPS` fires below this throughput (with high stall).
const SLOW_SEEK_DOWN_MBPS: f64 = 15.0;
/// Downgrades additionally require the producer blocked on channel sends
/// this fraction of the epoch — low throughput with near-zero stall means
/// the *input* is the bottleneck and the write profile must not react.
const STALL_DOWN_FRACTION: f64 = 0.30;
/// Consecutive qualifying epochs required to downgrade.
const DOWN_EPOCHS: u32 = 2;
/// Consecutive qualifying epochs required to upgrade (asymmetric hysteresis:
/// a wrong downgrade on NVMe costs ~15%; a wrong non-downgrade on an HDD
/// costs 2.5×).
const UP_EPOCHS: u32 = 3;
/// Upgrades require throughput above `factor × downgrade threshold`.
const UP_FACTOR: f64 = 2.0;

/// The adaptive-profile state machine, evaluated at epoch boundaries.
#[derive(Debug)]
pub struct Controller {
    state: ProfileKind,
    /// Writer count to grow to once the device is proven fast.
    fast_writers: usize,
    /// The first epoch is discarded as page-cache warmup.
    warmup_done: bool,
    /// Consecutive epochs meeting the current state's downgrade condition.
    down_streak: u32,
    /// Consecutive epochs meeting the current state's upgrade condition.
    up_streak: u32,
    /// Set once a measured epoch shows FAST-class throughput; gates the
    /// deferred writer spawn so slow media is never thrashed at all.
    proven_fast: bool,
}

impl Controller {
    /// `opening` is the probe-chosen starting state; `fast_writers` is the
    /// FAST profile's writer count (the growth ceiling in auto mode).
    pub fn new(opening: ProfileKind, fast_writers: usize) -> Self {
        Self {
            state: opening,
            fast_writers: fast_writers.max(1),
            warmup_done: false,
            down_streak: 0,
            up_streak: 0,
            proven_fast: false,
        }
    }

    /// Current state.
    pub fn state(&self) -> ProfileKind {
        self.state
    }

    fn decision(&self, transition: Option<ProfileKind>, mbps: f64) -> EpochDecision {
        EpochDecision {
            transition,
            target_writers: if self.state == ProfileKind::Fast && self.proven_fast {
                self.fast_writers
            } else {
                1
            },
            throughput_mbps: mbps,
        }
    }

    /// Evaluate one epoch boundary. Pure: no clocks, no I/O.
    pub fn on_epoch(&mut self, m: &EpochMeasurement) -> EpochDecision {
        let mbps = m.throughput_mbps();

        // First epoch is page-cache warmup: measure nothing from it.
        if !self.warmup_done {
            self.warmup_done = true;
            return self.decision(None, mbps);
        }

        let stalled = m.stall_fraction() > STALL_DOWN_FRACTION;
        let mut transition = None;

        match self.state {
            ProfileKind::Fast => {
                if mbps < FAST_DOWN_MBPS && stalled {
                    self.down_streak += 1;
                    if self.down_streak >= DOWN_EPOCHS {
                        transition = self.enter(ProfileKind::SlowSeek);
                    }
                } else {
                    self.down_streak = 0;
                    if mbps >= FAST_DOWN_MBPS {
                        self.proven_fast = true;
                    }
                }
            }
            ProfileKind::SlowSeek => {
                if mbps < SLOW_SEEK_DOWN_MBPS && stalled {
                    self.down_streak += 1;
                    self.up_streak = 0;
                    if self.down_streak >= DOWN_EPOCHS {
                        transition = self.enter(ProfileKind::SlowOps);
                    }
                } else if mbps > UP_FACTOR * FAST_DOWN_MBPS {
                    self.down_streak = 0;
                    self.up_streak += 1;
                    if self.up_streak >= UP_EPOCHS {
                        transition = self.enter(ProfileKind::Fast);
                        // The upgrade evidence (3 epochs > 300 MB/s) is
                        // exactly what "proven fast" means.
                        self.proven_fast = true;
                    }
                } else {
                    self.down_streak = 0;
                    self.up_streak = 0;
                }
            }
            ProfileKind::SlowOps => {
                if mbps > UP_FACTOR * SLOW_SEEK_DOWN_MBPS {
                    self.up_streak += 1;
                    if self.up_streak >= UP_EPOCHS {
                        transition = self.enter(ProfileKind::SlowSeek);
                    }
                } else {
                    self.up_streak = 0;
                }
            }
        }

        self.decision(transition, mbps)
    }

    /// Move to `next`, resetting both hysteresis streaks.
    fn enter(&mut self, next: ProfileKind) -> Option<ProfileKind> {
        self.state = next;
        self.down_streak = 0;
        self.up_streak = 0;
        Some(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a measurement from MB/s and a stall fraction, using the mock
    /// clock's fixed step as the epoch duration — the same shape the real
    /// pipeline produces.
    fn epoch(mbps: f64, stall_fraction: f64) -> EpochMeasurement {
        let duration = Duration::from_millis(100);
        EpochMeasurement {
            bytes: (mbps * 1024.0 * 1024.0 * duration.as_secs_f64()) as u64,
            duration,
            send_stall: duration.mul_f64(stall_fraction),
        }
    }

    /// Run a scripted sequence, returning the transition trace as
    /// `(epoch_index_1_based, new_state)`.
    fn trace(
        controller: &mut Controller,
        epochs: &[EpochMeasurement],
    ) -> Vec<(usize, ProfileKind)> {
        epochs
            .iter()
            .enumerate()
            .filter_map(|(i, m)| controller.on_epoch(m).transition.map(|k| (i + 1, k)))
            .collect()
    }

    #[test]
    fn steady_fast_stays_fast() {
        let mut c = Controller::new(ProfileKind::Fast, 4);
        let epochs: Vec<_> = std::iter::repeat_n(epoch(500.0, 0.05), 50).collect();
        assert_eq!(trace(&mut c, &epochs), vec![]);
        assert_eq!(c.state(), ProfileKind::Fast);
    }

    #[test]
    fn degradation_transitions_exactly_at_epoch_three() {
        // 500, 30, 28 MB/s with high stall: epoch 1 is warmup, epochs 2 and 3
        // qualify, so the FAST→SLOW_SEEK transition lands exactly at epoch 3.
        let mut c = Controller::new(ProfileKind::Fast, 4);
        let epochs = [epoch(500.0, 0.8), epoch(30.0, 0.8), epoch(28.0, 0.8)];
        assert_eq!(trace(&mut c, &epochs), vec![(3, ProfileKind::SlowSeek)]);
    }

    #[test]
    fn oscillation_around_threshold_never_flaps() {
        // 100/200 MB/s alternating straddles the 150 MB/s downgrade line but
        // never sustains it for 2 consecutive epochs — hysteresis holds.
        let mut c = Controller::new(ProfileKind::Fast, 4);
        let epochs: Vec<_> = (0..40)
            .map(|i| epoch(if i % 2 == 0 { 100.0 } else { 200.0 }, 0.8))
            .collect();
        assert_eq!(trace(&mut c, &epochs), vec![]);
        assert_eq!(c.state(), ProfileKind::Fast);
    }

    /// Regression for the 2026-07-16 field bug: `bytes_acked` used to count
    /// bytes landing in the writer's RAM coalescing buffer, so a slow device
    /// reported ~600 MB/s "throughput" alongside heavy producer stall. Per
    /// the design doc, a FAST downgrade requires *both* low throughput and
    /// high stall — high measured throughput must veto the downgrade no
    /// matter how stalled the producer is.
    #[test]
    fn high_throughput_high_stall_never_downgrades_from_fast() {
        let mut c = Controller::new(ProfileKind::Fast, 4);
        let epochs: Vec<_> = std::iter::repeat_n(epoch(612.0, 0.9), 20).collect();
        assert_eq!(trace(&mut c, &epochs), vec![]);
        assert_eq!(c.state(), ProfileKind::Fast);

        // Same for SLOW_SEEK → SLOW_OPS: 40 MB/s + heavy stall is above the
        // 15 MB/s line, so it must hold (and being above 2×15 for 3 epochs it
        // legitimately upgrades instead — anything but a downgrade).
        let mut c = Controller::new(ProfileKind::SlowSeek, 4);
        let epochs: Vec<_> = std::iter::repeat_n(epoch(40.0, 0.9), 20).collect();
        for (_, kind) in trace(&mut c, &epochs) {
            assert_ne!(kind, ProfileKind::SlowOps, "downgrade despite 40 MB/s");
        }
    }

    /// The value in the transition log line must be the exact value the
    /// state machine compared against its thresholds: a downgrade decision
    /// can therefore never carry a throughput above the downgrade line.
    #[test]
    fn logged_throughput_is_the_compared_throughput() {
        let mut c = Controller::new(ProfileKind::Fast, 4);
        let script = [
            epoch(500.0, 0.1), // warmup
            epoch(30.0, 0.8),
            epoch(28.0, 0.8), // → SLOW_SEEK
        ];
        for m in &script {
            let d = c.on_epoch(m);
            assert_eq!(
                d.throughput_mbps,
                m.throughput_mbps(),
                "decision must carry the measured value it was computed from"
            );
            if d.transition == Some(ProfileKind::SlowSeek) {
                assert!(
                    d.throughput_mbps < FAST_DOWN_MBPS,
                    "FAST downgrade fired at {} MB/s, above the {} MB/s line",
                    d.throughput_mbps,
                    FAST_DOWN_MBPS
                );
            }
        }
        assert_eq!(c.state(), ProfileKind::SlowSeek);
    }

    #[test]
    fn input_bound_low_throughput_zero_stall_never_transitions() {
        // 5 MB/s but the producer never blocks on sends: the input (slow
        // decompression, other device) is the bottleneck, not the output.
        let mut c = Controller::new(ProfileKind::Fast, 4);
        let epochs: Vec<_> = std::iter::repeat_n(epoch(5.0, 0.0), 20).collect();
        assert_eq!(trace(&mut c, &epochs), vec![]);
        assert_eq!(c.state(), ProfileKind::Fast);

        // Same holds for the SLOW_SEEK → SLOW_OPS downgrade.
        let mut c = Controller::new(ProfileKind::SlowSeek, 4);
        let epochs: Vec<_> = std::iter::repeat_n(epoch(5.0, 0.0), 20).collect();
        assert_eq!(trace(&mut c, &epochs), vec![]);
        assert_eq!(c.state(), ProfileKind::SlowSeek);
    }

    #[test]
    fn double_downgrade_fast_to_slow_seek_to_slow_ops() {
        let mut c = Controller::new(ProfileKind::Fast, 4);
        let epochs = [
            epoch(400.0, 0.1), // warmup (discarded)
            epoch(30.0, 0.8),
            epoch(30.0, 0.8), // → SLOW_SEEK
            epoch(8.0, 0.8),
            epoch(8.0, 0.8), // → SLOW_OPS
        ];
        assert_eq!(
            trace(&mut c, &epochs),
            vec![(3, ProfileKind::SlowSeek), (5, ProfileKind::SlowOps)]
        );
    }

    #[test]
    fn recovery_upgrade_after_three_qualifying_epochs() {
        // SLOW_SEEK → FAST needs 3 consecutive epochs above 2× the 150 MB/s
        // downgrade threshold; two are not enough.
        let mut c = Controller::new(ProfileKind::SlowSeek, 4);
        let epochs = [
            epoch(50.0, 0.1), // warmup
            epoch(400.0, 0.0),
            epoch(400.0, 0.0),
            epoch(250.0, 0.0), // streak broken (below 300)
            epoch(400.0, 0.0),
            epoch(400.0, 0.0),
            epoch(400.0, 0.0), // third consecutive → FAST
        ];
        assert_eq!(trace(&mut c, &epochs), vec![(7, ProfileKind::Fast)]);

        // SLOW_OPS recovers to SLOW_SEEK above 2× 15 MB/s.
        let mut c = Controller::new(ProfileKind::SlowOps, 4);
        let epochs = [
            epoch(5.0, 0.8), // warmup
            epoch(40.0, 0.0),
            epoch(40.0, 0.0),
            epoch(40.0, 0.0),
        ];
        assert_eq!(trace(&mut c, &epochs), vec![(4, ProfileKind::SlowSeek)]);
    }

    #[test]
    fn writer_growth_waits_for_proven_fast() {
        let mut c = Controller::new(ProfileKind::Fast, 4);
        // Warmup epoch: not proof, stay at 1 writer.
        assert_eq!(c.on_epoch(&epoch(500.0, 0.0)).target_writers, 1);
        // First measured fast epoch proves the device: grow to FAST's count.
        assert_eq!(c.on_epoch(&epoch(500.0, 0.0)).target_writers, 4);
        // Input-bound epochs are not proof either way; the flag is sticky.
        assert_eq!(c.on_epoch(&epoch(5.0, 0.0)).target_writers, 4);
    }

    #[test]
    fn slow_opening_never_requests_growth() {
        let mut c = Controller::new(ProfileKind::SlowSeek, 8);
        for _ in 0..10 {
            assert_eq!(c.on_epoch(&epoch(30.0, 0.5)).target_writers, 1);
        }
    }

    #[test]
    fn upgrade_to_fast_requests_growth() {
        let mut c = Controller::new(ProfileKind::SlowSeek, 4);
        c.on_epoch(&epoch(50.0, 0.1)); // warmup
        c.on_epoch(&epoch(400.0, 0.0));
        c.on_epoch(&epoch(400.0, 0.0));
        let d = c.on_epoch(&epoch(400.0, 0.0));
        assert_eq!(d.transition, Some(ProfileKind::Fast));
        assert_eq!(d.target_writers, 4);
    }

    #[test]
    fn mock_clock_steps_deterministically() {
        let clock = MockClock::stepping(Duration::from_millis(5));
        assert_eq!(clock.now(), Duration::from_millis(5));
        assert_eq!(clock.now(), Duration::from_millis(10));
        assert_eq!(clock.now(), Duration::from_millis(15));
    }

    #[test]
    fn real_clock_is_monotonic() {
        let clock = RealClock::new();
        let a = clock.now();
        let b = clock.now();
        assert!(b >= a);
    }
}

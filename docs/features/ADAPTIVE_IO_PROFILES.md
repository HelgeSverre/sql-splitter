# Adaptive I/O Profiles

**Status**: Planned (v1.16.0)
**Effort**: ~2–3 days including tests
**Motivation**: Measured 2026-07-15/16 on a Seagate Expansion 1TB USB HDD (ExFAT):
same-spindle `split` runs at 21–33 MB/s with default settings, but
`SQL_SPLITTER_WRITERS=1 SQL_SPLITTER_WRITE_BUF=67108864` reaches **54.7 MB/s
(2.52×)** — within ~10% of the drive's theoretical half-duplex ceiling
(~60 MB/s from a 119/129 MB/s sequential baseline). On a generic USB2 thumb
drive (5 MB/s writes) the effective rate under mixed I/O drops to 1.7 MB/s
regardless of byte volume, so it wants fewest write *operations*, not fewest
bytes. On NVMe, the current defaults are already right (W=1 costs ~15%).
One configuration cannot serve all three device classes.

## Design principle

**Don't identify the device — respond to it.** Static hints (rotational flag,
fsync probe) only pick the *opening* profile; a feedback loop driven by the
pipeline's own backpressure owns the truth afterwards. Static detection alone
is untrustworthy: USB bridges hide rotational flags, network mounts and cheap
flash report as non-rotational but behave worse than any HDD, SMR drives
collapse only under sustained writes, and near-full SSDs throttle.

## Architecture

### Profiles (states)

| State       | Writers          | Flush chunk | Per-file buffer | Stage cap | Intended for            |
| ----------- | ---------------- | ----------- | --------------- | --------- | ----------------------- |
| `FAST`      | min(4, cores)    | 256 KB      | 256 KB          | 32 MB     | NVMe/SSD (today's defaults) |
| `SLOW_SEEK` | 1                | 8 MB        | 64 MB           | 256 MB    | HDDs, same-spindle r+w  |
| `SLOW_OPS`  | 1                | 32 MB       | 64 MB           | 512 MB    | Cheap flash, network FS |

All numbers become runtime values on a `WriterProfile` struct (today
`STAGE_FLUSH`/`STAGE_TOTAL_CAP` are hard-coded consts in `src/writer/mod.rs`,
and only writer count + BufWriter size are env-tunable).

### Signals (already latent in the pipeline)

The parser ships chunks to writer threads over bounded channels; staging has a
byte cap. Two counters, sampled per epoch:

```
throughput   = bytes_acked_by_writers / epoch_duration
send_stall   = time_parser_blocked_on_channel_send / epoch_duration
```

`send_stall` near zero + low throughput ⇒ *input* is the bottleneck (different
device or slow decompression) ⇒ do **not** touch the write profile.
High `send_stall` ⇒ output device can't drain ⇒ step down.

### Epochs are byte-based, not time-based

An epoch ends every N bytes of input processed (default 256 MB, min 4 epochs
per file, first epoch discarded as page-cache warmup). Byte-based epochs make
controller decisions a pure function of `(bytes, measured durations)` — which
is what makes deterministic testing possible (see below).

### Controller

Small explicit state machine, evaluated at epoch boundaries:

- `FAST → SLOW_SEEK`: sustained throughput < 150 MB/s **and** send_stall > 30%
  for 2 consecutive epochs.
- `SLOW_SEEK → SLOW_OPS`: sustained throughput < 15 MB/s for 2 epochs.
- Upgrades require 3 consecutive epochs above 2× the downgrade threshold
  (asymmetric hysteresis: a wrong downgrade on NVMe costs ~15%; a wrong
  non-downgrade on an HDD costs 2.5×).
- Emits one log line per transition:
  `output device sustaining ~35 MB/s — switching to HDD write profile`.

### Mid-run reconfiguration

- Buffer/chunk/cap values: atomics read by writers at flush points — trivial.
- Writer **count**: cannot shrink safely mid-run (each table is owned by one
  thread; that ownership is what keeps output byte-identical). Dodge: **start
  at W=1 and spawn additional writers only after the first epoch proves the
  device fast.** Spawning assigns *tables not yet seen* to new shards; tables
  already written stay with their original owner, preserving per-table
  ordering by construction. Cost of the W=1 start on NVMe: ~15% for one epoch
  (milliseconds-to-seconds); benefit on slow media: never thrashed at all.

### Startup hints (choose the opening state only)

1. `fsync` probe: write + fsync 8 MB into the output dir before the pipeline
   starts. >80 MB/s ⇒ open in FAST; 10–80 ⇒ SLOW_SEEK; <10 ⇒ SLOW_OPS.
   Costs ~100 ms on NVMe, and on slow media the "cost" is itself the signal.
2. Platform rotational flag (Linux `/sys/block/*/queue/rotational`, macOS
   IOKit seek-penalty, Windows `StorageDeviceSeekPenaltyProperty`) — optional,
   ships later; the probe alone is sufficient and portable.

### CLI

```
--io-profile auto|fast|hdd|minimal-ops     (default: auto)
```

Explicit names pin the profile (no controller, no probe) — for scripting,
benchmarking, and escape hatches. Existing env vars keep working and override
everything (documented as expert knobs).

## Implementation plan

| Phase | Work | Size |
| ----- | ---- | ---- |
| 0 | Instrumentation: bytes-acked + send-stall counters in `ParallelWriters`; hidden `--io-stats` JSON dump for debugging | ~80 lines |
| 1 | `WriterProfile` struct; replace `STAGE_FLUSH`/`STAGE_TOTAL_CAP` consts with profile values; explicit `--io-profile fast\|hdd\|minimal-ops` | ~120 lines |
| 2 | Controller: `Clock` trait (real + mock), byte-based epochs, state machine, atomic profile swap, deferred writer spawn | ~200 lines |
| 3 | fsync probe + `auto` default wiring + transition log lines | ~60 lines |
| 4 | Docs (README, website performance page), benchmark script `scripts/verify-io-profiles.sh` | — |

Order matters: phases 0–1 are independently shippable (explicit profiles alone
would have captured the 2.5× on the HDD).

## Deterministic testing & verification

The problem: "adapts to a slow disk" sounds inherently nondeterministic. It
isn't, if the controller is isolated from wall-clock and real devices.

### 1. Controller unit tests (fully deterministic)

The controller is a pure function: `(state, epoch_measurements) → (state,
actions)`. With the `Clock` trait mocked, feed scripted measurement sequences
and assert exact transition traces:

- steady 500 MB/s → stays FAST forever
- 500, 30, 28 MB/s + high stall → FAST→SLOW_SEEK exactly at epoch 3
- oscillating 100/200 MB/s around the threshold → no flapping (hysteresis)
- slow throughput but zero send_stall (input-bound) → no transition
- 30 → 8 MB/s → double downgrade FAST→SLOW_SEEK→SLOW_OPS
- recovery sequence → upgrade only after 3 qualifying epochs

No I/O, no sleeps, no flakiness. This is the bulk of the coverage.

### 2. Throttled-sink integration tests (deterministic transitions)

A test-only rate-limited `Write` sink (token bucket, e.g. 10 MB/s) injected
behind the writer's file handles via a `#[cfg(test)]` (or hidden env) seam,
driving the *real* pipeline with a fixture sized for ~6 epochs (epoch size
shrunk via test config). Assert: the transition log shows FAST→SLOW_SEEK, and
it happens within epochs 2–4. The token bucket makes measured throughput
reproducible across machines because it's the *limiter*, not the machine.

### 3. The golden invariant: byte-identical output (the non-negotiable)

Every profile and every adaptation path must produce **identical output**:

```
for profile in fast hdd minimal-ops auto auto+throttle(10MB/s) auto+throttle(60MB/s):
    split fixture → sha256 every output file
assert all runs produce the same set of hashes
```

Run in CI on normal (fast) disks — profile correctness is independent of
actual device speed, so CI never needs slow hardware. This test also pins the
deferred-writer-spawn ordering guarantee (a mid-run W=1→4 ramp is forced in
the `auto+throttle` cases).

### 4. Ordering property test

Force a writer ramp at a hostile moment (epoch boundary mid-table via crafted
fixture): every table file's statement sequence must equal the input's
per-table sequence. Catches shard-reassignment bugs the hash test might mask
on symmetric fixtures.

### 5. Real-hardware acceptance (manual, per release, not CI)

`scripts/verify-io-profiles.sh <slow-mount>`: runs defaults vs `--io-profile
hdd` vs `auto` on a 4 GB fixture on the target mount. Acceptance criteria:

- On rotational media: `hdd` ≥ 1.8× `fast`; `auto` within 15% of `hdd`.
- On NVMe: `auto` within 10% of `fast` (the W=1 opening must stay cheap).
- RSS stays under the profile's stage cap + fixed overhead.

Numbers vary by drive; the *ratios* are the acceptance gates. Results appended
to BENCHMARKS.md the same way as the 2026-07-15 measurements that motivated
this feature.

## 2026-07-16 amendment: slow profiles also cap the compression path at W=1

`--compress` mode defaults the writer count to **all cores** — the right call
for CPU-bound compression on an SSD, but wrong on seek-bound media. Measured
on the same Seagate USB HDD at 100 GB scale: zstd output with the all-cores
default ran at **22.1 MB/s vs 34.5 MB/s plain** — more writer threads means
more seek thrash, and the seeks dominate the compression win. Correctly
configured (`zstd + W=1 + 16 MB buffers`) the same drive sustained
**50.4 MB/s — faster than plain**, because compression shrinks the byte
volume the spindle has to absorb.

Therefore `SLOW_SEEK` and `SLOW_OPS` cap the writer count at **1 for the
compression path too**; the all-cores compression default applies only in
`FAST`. The profile table above is unchanged — writers = 1 in the slow
profiles is unconditional, compression or not.

### What we deliberately do NOT test

Wall-clock-based assertions on real devices in CI (flaky by construction),
and absolute MB/s thresholds anywhere except the manual acceptance script.

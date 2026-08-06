# Observability and Operations Model for Production Migrations

> Builds on the logging infrastructure in §6.7, the failure-mode catalog in §6.2,
> and the review findings in §7. Defines what to measure, when to measure it, what
> to alert on, and what to do at each stage of a production migration.

---

## 1. Pre-Migration Observability

Pre-migration checks serve two purposes: they prevent the migration from starting
in a doomed state, and they establish a baseline against which migration health
is measured.

### 1.1 Standalone Health Check (`migrate check`)

A new subcommand that runs pre-flight checks without starting the migration:

```bash
sql-splitter migrate check \
  --source mysql://replica:3306/production \
  --target mysql://target:3306/production \
  --format json
```

Output is a machine-readable pass/fail/warn report:

```json
{
  "check_id": "preflight_2026-08-07T10:00:00Z",
  "source": { "dialect": "mysql", "version": "8.0.36", "host": "replica:3306" },
  "target": { "dialect": "mysql", "version": "8.4.3", "host": "target:3306" },
  "passed": 14,
  "failed": 1,
  "warned": 3,
  "checks": [
    {
      "id": "connectivity-source",
      "status": "pass",
      "latency_ms": 2.3,
      "message": "Source reachable at mysql://replica:3306/production"
    },
    {
      "id": "connectivity-target",
      "status": "pass",
      "latency_ms": 1.8,
      "message": "Target reachable at mysql://target:3306/production"
    },
    {
      "id": "disk-target",
      "status": "fail",
      "severity": "blocking",
      "message": "Target needs 4194 GB free, only 1850 GB available (including 2× binlog buffer)",
      "detail": {
        "data_size_gb": 2097,
        "binlog_overhead_gb": 2097,
        "required_gb": 4194,
        "available_gb": 1850,
        "shortfall_gb": 2344
      }
    },
    {
      "id": "disk-source",
      "status": "pass",
      "message": "Source has 500 GB free (temp files require ~2100 GB for dump path; source is live DB, not a dump)",
      "detail": { "available_gb": 500, "required_for_dump_gb": 0 }
    },
    {
      "id": "permissions-target",
      "status": "pass",
      "message": "Target credentials have CREATE, ALTER, INSERT, DROP, INDEX",
      "detail": { "missing_privileges": [] }
    },
    {
      "id": "permissions-source",
      "status": "pass",
      "message": "Source credentials have SELECT, SHOW VIEW",
      "detail": { "missing_privileges": [] }
    },
    {
      "id": "max-connections-target",
      "status": "warn",
      "severity": "advisory",
      "message": "Target has 512 max_connections, 487 active. Parallel import with 4 workers would consume 4 connections (OK), but app pool at 95% capacity",
      "detail": {
        "max_connections": 512,
        "active_connections": 487,
        "planned_workers": 4,
        "remaining_slots": 25,
        "utilization_pct": 95.1
      }
    },
    {
      "id": "tables-without-unique-key",
      "status": "pass",
      "message": "All 487 tables have at least one unique key",
      "detail": { "count": 0, "tables": [] }
    },
    {
      "id": "charset-compatibility",
      "status": "warn",
      "severity": "advisory",
      "message": "12 tables use utf8mb3 on source → target's utf8 maps to utf8mb4 in 8.4",
      "detail": {
        "count": 12,
        "tables": ["legacy_logs_2019", "..."],
        "resolution": "Type mapper will convert varchar columns to utf8mb4"
      }
    },
    {
      "id": "schema-version-compatibility",
      "status": "warn",
      "severity": "advisory",
      "message": "Source is MySQL 8.0.36, target is 8.4.3. New reserved keywords detected: MANUAL, PARALLEL, QUALIFY.",
      "detail": {
        "new_keywords": ["MANUAL", "PARALLEL", "QUALIFY"],
        "affected_columns": [],
        "affected_tables": []
      }
    },
    {
      "id": "replication-lag-source",
      "status": "pass",
      "message": "Source replica lag: 0.2s (under 5s threshold)",
      "detail": { "lag_seconds": 0.2, "threshold_seconds": 5 }
    },
    {
      "id": "ssl-configuration",
      "status": "pass",
      "message": "Both source and target connections use TLS 1.3",
      "detail": {
        "source_tls": "TLSv1.3",
        "target_tls": "TLSv1.3",
        "mode": "verify-full"
      }
    },
    {
      "id": "gtid-mode-source",
      "status": "pass",
      "message": "Source GTID mode: ON (gtid_executed captured for consistency baseline)",
      "detail": {
        "gtid_mode": "ON",
        "gtid_executed": "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-47819234"
      }
    },
    {
      "id": "long-running-queries-source",
      "status": "pass",
      "message": "No queries running > 60s on source",
      "detail": { "count": 0 }
    },
    {
      "id": "row-count-estimates",
      "status": "pass",
      "message": "Estimated 487 tables, 2.1B rows, 2.1 TB data",
      "detail": {
        "tables": 487,
        "total_rows": 2137482610,
        "total_bytes": 2251799813685,
        "largest_table": {
          "name": "pageviews",
          "rows": 892340000,
          "bytes": 943126000000
        }
      }
    },
    {
      "id": "target-empty",
      "status": "pass",
      "message": "Target database 'production' exists and is empty (0 tables). Schema will be created.",
      "detail": { "table_count": 0 }
    },
    {
      "id": "neon-scale-to-zero",
      "status": "pass",
      "message": "Not applicable: target is standard MySQL, not Neon",
      "detail": { "is_neon": false }
    },
    {
      "id": "planetscale-vtgate",
      "status": "pass",
      "message": "Not applicable: source is standard MySQL, not PlanetScale",
      "detail": { "is_planetscale": false }
    }
  ],
  "blocking_count": 1,
  "actionable": "Resolve blocking checks before running --execute. Use --force to override non-blocking warnings."
}
```

Exit codes from `migrate check`:

| Exit code | Meaning                                          |
| --------- | ------------------------------------------------ |
| 0         | All checks pass (may have warnings)              |
| 1         | One or more blocking checks failed               |
| 2         | Could not complete checks (connection error etc) |

The `check` subcommand can be run days before the migration and re-run minutes
before execution to detect changed conditions.

### 1.2 Metrics Collected Before Starting

The pre-flight check captures these metrics and records them in the migration
log as a baseline span:

| Metric                           | Source    | How Collected                                                                    | Baseline Use                                          |
| -------------------------------- | --------- | -------------------------------------------------------------------------------- | ----------------------------------------------------- |
| Source DB version                | SELECT    | `SELECT VERSION()`                                                               | Keyword compatibility, feature gate decisions         |
| Target DB version                | SELECT    | `SELECT VERSION()`                                                               | Same                                                  |
| Source row count per table       | SELECT    | `SELECT COUNT(*)` or `information_schema.TABLES.TABLE_ROWS` (approx)             | Progress % calculation, post-migration row count diff |
| Target row count per table       | SELECT    | Same                                                                             | Verify target is empty/warm                           |
| Source data size                 | I_S       | Sum `DATA_LENGTH` from `information_schema.TABLES`                               | Duration estimate, throughput target                  |
| Target free disk                 | OS/DB     | MySQL: `SHOW VARIABLES LIKE 'datadir'` + OS stat. Postgres: `pg_database_size()` | Disk-full prevention                                  |
| Source/target max_connections    | SELECT    | `SELECT @@max_connections`                                                       | Worker pool sizing                                    |
| Source/target active connections | SELECT    | `SELECT COUNT(*) FROM information_schema.PROCESSLIST`                            | Connection exhaustion prevention                      |
| Source replication lag           | SELECT    | `SHOW REPLICA STATUS` (MySQL), `pg_stat_replication` (Postgres)                  | Ensure replica is caught up                           |
| Source GTID position             | SELECT    | `SELECT @@gtid_executed`                                                         | Consistency snapshot, schema-change detection         |
| Source sql_mode                  | SELECT    | `SELECT @@sql_mode`                                                              | CF-3: detect strict mode differences                  |
| Source character set             | I_S       | Per-column collation from `information_schema.COLUMNS`                           | Charset compatibility warnings                        |
| Source reserved keyword check    | Static    | Compare column/table names against target version's reserved word list           | CF-P17: keyword collision detection                   |
| RTT source↔runner                | TCP       | TCP connect time to source host                                                  | Throughput model calibration                          |
| RTT target↔runner                | TCP       | TCP connect time to target host                                                  | Throughput model calibration                          |
| Disk IOPS (target)               | Benchmark | `--bench` 10K-row test INSERT to target                                          | Throughput model calibration (CF-4)                   |
| Neon detection                   | SELECT    | `SELECT current_setting('neon.version')`                                         | CF-5: scale-to-zero handling                          |
| PlanetScale detection            | Hostname  | Detect VTGate-specific error or hostname pattern                                 | CF-6: mandatory chunking                              |

### 1.3 Pre-Flight Report Format

The pre-flight report is a JSON file written alongside the plan. It is the
canonical baseline for automated migration tooling:

```json
{
  "report_version": "1.0",
  "migration_id": "mig_2026-08-07_production_to_84",
  "generated_at": "2026-08-07T09:58:00Z",
  "source": {
    "uri": "mysql://replica:3306/production",
    "dialect": "mysql",
    "version": "8.0.36",
    "gtid_executed": "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-47819234",
    "sql_mode": "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"
  },
  "target": {
    "uri": "mysql://target:3306/production",
    "dialect": "mysql",
    "version": "8.4.3",
    "max_connections": 512,
    "active_connections": 487
  },
  "environment": {
    "runner_hostname": "migration-runner-01",
    "runner_rtt_source_ms": 2.3,
    "runner_rtt_target_ms": 1.8,
    "parallel_workers": 4,
    "chunk_column": "site_id",
    "batch_size": 1000,
    "copy_mode": "client"
  },
  "pre_flight": {
    "passed": 14,
    "failed": 1,
    "warned": 3,
    "checks": [/* ... full check array as above ... */]
  },
  "plan_summary": {
    "tables": 487,
    "total_rows_estimate": 2137482610,
    "data_size_bytes": 2251799813685,
    "ddl_operations": {
      "create_table": 487,
      "add_constraint": 312,
      "create_index": 189
    },
    "conversions": { "mysql_to_mysql": 487, "columns_needing_conversion": 12 },
    "hazards": [],
    "destructive_operations": []
  },
  "estimate": {
    "throughput_bench_mbps": 28.4,
    "estimated_duration_hours": 22.5,
    "estimated_duration_seconds": 81000,
    "estimated_verification_hours": 0.15,
    "estimated_disk_required_gb": 4194,
    "estimated_disk_available_gb": 1850
  },
  "blocking": true,
  "blocking_reason": "disk-target: insufficient space"
}
```

CI/CD systems and migration orchestrators consume this as a gate: if
`blocking: true`, refuse to proceed.

---

## 2. During-Migration Observability

The execution phase may span hours or days. Observability must answer three
questions at any moment: **is it still running? is it healthy? when will it
finish?**

### 2.1 Minimum Metrics Set

Every span (phase, table, batch) emits these fields. The executor carries a
`MigrationMetrics` accumulator updated atomically:

```json
{
  "timestamp": "2026-08-07T14:30:01.234Z",
  "level": "info",
  "phase": "data_migration",
  "table": "users",
  "trace_id": "mig_prod_2026_08_07",
  "span_id": "data_migration_users",
  "message": "Migrated users: 49765/50000 rows (99.5%) — 19.4 MB/s",
  "fields": {
    "phase": "data_migration",
    "table": "users",
    "rows_done": 49765,
    "rows_total": 50000,
    "rows_skipped": 0,
    "bytes_done": 102005473,
    "bytes_total": 102500000,
    "duration_since_phase_start_s": 12.3,
    "elapsed_since_migration_start_s": 4234.5,
    "throughput_rows_sec": 4046,
    "throughput_mb_sec": 19.4,
    "current_batch": 49,
    "total_batches": 50,
    "batch_duration_ms": 247,
    "batch_rows": 1000,
    "error_count": 0,
    "warning_count": 0,
    "rss_mb": 1432,
    "worker_id": 2,
    "connection_status": "active"
  }
}
```

### 2.2 Progress Reporting for Long-Running Migrations (24h+)

For migrations running 24+ hours, the executor provides three complementary
progress views:

#### 2.2.1 State File (`--state-file`)

Written every 30 seconds (configurable via `--state-interval`). Atomic write
(write to `.tmp`, `fsync`, `rename`). Machine-readable snapshot:

```json
{
  "migration_id": "mig_prod_2026_08_07",
  "updated_at": "2026-08-07T18:23:45Z",
  "status": "running",
  "current_phase": "data_migration",
  "current_level": 3,
  "total_levels": 7,
  "phase_duration_s": 28193,
  "total_duration_s": 33947,
  "overall": {
    "tables_total": 487,
    "tables_done": 134,
    "tables_failed": 0,
    "tables_in_progress": 4,
    "rows_total_estimate": 2137482610,
    "rows_done": 523402891,
    "rows_skipped": 47,
    "bytes_done": 542100000000,
    "bytes_total_estimate": 2251799813685,
    "progress_pct": 24.08,
    "throughput_mb_sec": 19.2,
    "throughput_rows_sec": 18562,
    "eta_remaining_s": 89108,
    "eta_completion": "2026-08-08T19:09:53Z"
  },
  "per_table": [
    {
      "table": "users",
      "status": "completed",
      "rows_done": 50000,
      "rows_skipped": 0,
      "duration_s": 12.3,
      "throughput_rows_sec": 4065
    },
    {
      "table": "orders",
      "status": "in_progress",
      "rows_done": 34200000,
      "rows_total": 98000000,
      "rows_skipped": 0,
      "progress_pct": 34.9,
      "throughput_rows_sec": 18400,
      "worker_id": 2,
      "chunk_column": "site_id",
      "current_chunk_start": 50001,
      "current_chunk_end": 100000
    },
    {
      "table": "pageviews",
      "status": "in_progress",
      "rows_done": 452000000,
      "rows_total": 892340000,
      "rows_skipped": 3,
      "progress_pct": 50.7,
      "throughput_rows_sec": 22100,
      "worker_id": 0,
      "chunk_column": "site_id",
      "current_chunk_start": 350001,
      "current_chunk_end": 400000,
      "skew_detected": false
    },
    {
      "table": "order_items",
      "status": "pending",
      "level": 4
    }
  ],
  "workers": [
    {
      "id": 0,
      "status": "active",
      "current_table": "pageviews",
      "throughput_rows_sec": 22100
    },
    {
      "id": 1,
      "status": "active",
      "current_table": "logs_2024",
      "throughput_rows_sec": 18400
    },
    {
      "id": 2,
      "status": "active",
      "current_table": "orders",
      "throughput_rows_sec": 18400
    },
    {
      "id": 3,
      "status": "active",
      "current_table": "analytics_events",
      "throughput_rows_sec": 17800
    }
  ],
  "errors": [],
  "warnings": [
    {
      "code": "MIG-TYPE-LOSSY",
      "table": "legacy_logs_2019",
      "count": 12,
      "message": "utf8mb3 varchar widened to utf8mb4"
    }
  ],
  "resource_usage": {
    "rss_mb": 1847,
    "cpu_pct": 45.2,
    "connections_source": 4,
    "connections_target": 4
  }
}
```

#### 2.2.2 Terminal Progress Bar (`--progress bar`)

TUI progress display. On TTY output, shows:

```
╔══════════════════════════════════════════════════════════════╗
║ Phase 2/5: Data migration  ────────────────────  24.1% ETA 24h 43m ║
╠══════════════════════════════════════════════════════════════╣
║ Level 3/7: importing 4 tables in parallel                   ║
║                                                            ║
║ pageviews     ████████████████████████░────  50.7%  892M    ║
║ orders        ████████████████░─────────  34.9%   98M    ║
║ logs_2024     ██████████████████████░░░░─  40.2%  156M    ║
║ analytics     ████████░░────────────────  18.1%   42M    ║
║                                                            ║
║ Completed: 134 tables │ Failed: 0 │ Skipped rows: 47      ║
║ Throughput: 18.6K rows/s │ 19.2 MB/s (4 workers)          ║
║ Elapsed: 9h 25m │ RSS: 1.8 GB │ CPU: 45%                  ║
╚══════════════════════════════════════════════════════════════╝
```

#### 2.2.3 Log-Only Progress (`--progress log`)

Every 60 seconds (configurable), the executor emits a summary log line:

```json
{
  "timestamp": "2026-08-07T18:23:45Z",
  "level": "info",
  "phase": "data_migration",
  "operation": "progress_summary",
  "trace_id": "mig_prod_2026_08_07",
  "message": "Progress: 134/487 tables (24.1%), 523M rows, 542 GB, 19.2 MB/s, RSS 1.8 GB, ETA 24h 43m",
  "fields": {
    "elapsed_s": 33947,
    "tables_done": 134,
    "tables_total": 487,
    "rows_done": 523402891,
    "rows_skipped": 47,
    "bytes_done": 542100000000,
    "progress_pct": 24.08,
    "throughput_mb_sec": 19.2,
    "throughput_rows_sec": 18562,
    "eta_remaining_s": 89108,
    "rss_mb": 1847,
    "active_workers": 4,
    "connections_source": 4,
    "connections_target": 4
  }
}
```

### 2.3 Structured Log Queryability

The log file (`--log-file`) is JSON Lines. All diagnostic codes from §6.7 are
searchable:

```bash
# Find all errors
jq 'select(.level == "error")' migration-log.jsonl

# Find all row-level failures for a specific table
jq 'select(.table == "edge_case_types" and .operation == "insert_row" and .level == "error")' migration-log.jsonl

# Count skipped rows by reason
jq -r 'select(.fields.skipped == true) | .fields.error_code' migration-log.jsonl | sort | uniq -c

# Find type conversion warnings
jq 'select(.fields.code == "MIG-TYPE-LOSSY")' migration-log.jsonl

# Calculate average throughput per table
jq -r 'select(.operation == "insert_batch") | [.table, .fields.throughput_rows_sec] | @tsv' migration-log.jsonl \
  | awk '{sum[$1]+=$2; cnt[$1]++} END {for(t in sum) print t, sum[t]/cnt[t]}'

# Find tables with the most warnings
jq -r 'select(.level == "warn") | .table' migration-log.jsonl | sort | uniq -c | sort -rn | head

# Reconstruct per-table timeline
jq -r 'select(.operation == "insert_batch") | [.timestamp, .table, .fields.throughput_rows_sec, .fields.rows_in_batch] | @tsv' migration-log.jsonl
```

### 2.4 Alerts That Should Fire During Migration

Alerts are classified by severity. The executor emits structured events that
external monitoring systems can consume.

#### Alert Severities

| Severity   | Meaning                                                                       | Default Action                          |
| ---------- | ----------------------------------------------------------------------------- | --------------------------------------- |
| `critical` | Migration cannot continue; human intervention required                        | Abort migration, emit exit code 1       |
| `high`     | Migration will fail if condition persists; automatic recovery may be possible | Emit alert, continue with degraded perf |
| `medium`   | Something is wrong but migration is still making progress                     | Emit warning, no interruption           |
| `low`      | Advisory; no action needed during migration                                   | Log only                                |

#### Alert Catalog

| Alert ID                    | Severity | Trigger Condition                                                        | Detection Mechanism                                       | Automatic Response                                                 |
| --------------------------- | -------- | ------------------------------------------------------------------------ | --------------------------------------------------------- | ------------------------------------------------------------------ |
| `DISK-TARGET-FULL`          | critical | Available disk < 1% of estimated remaining data on target                | Periodic `SHOW VARIABLES LIKE 'datadir'` + OS stat        | Abort within 500ms of detection; flush state file                  |
| `DISK-TARGET-LOW`           | high     | Available disk < 10% of estimated remaining data on target               | Same, at 10% threshold                                    | Emit alert; reduce batch size by 50%; continue                     |
| `THROUGHPUT-DROP`           | high     | 5-minute rolling throughput < 20% of 30-minute baseline                  | Rolling window over batch-insert span durations           | Log diagnostic: check for autovacuum, lock contention, binlog lag  |
| `THROUGHPUT-DEGRADED`       | medium   | 5-minute rolling throughput < 50% of baseline                            | Same, at 50% threshold                                    | Log only; no interruption                                          |
| `CONNECTION-LOST-SOURCE`    | high     | Source connection drops (not retryable within 3 attempts)                | Read/write error on any source cursor handle              | Attempt reconnect 3× with 30s backoff; if all fail, abort level    |
| `CONNECTION-LOST-TARGET`    | high     | Target connection drops on any worker                                    | Read/write error on any target connection handle          | Attempt reconnect 3×; reprepare session settings; continue chunk   |
| `WORKER-DIED`               | high     | Worker thread panics or exits with error                                 | JoinHandle returns Err or worker exits with non-zero code | Drain remaining workers at batch boundary; report orphaned FK rows |
| `ERROR-RATE-SPIKE`          | medium   | Skipped rows / total rows in last 5 minutes exceeds 1%                   | Error counter per worker, aggregated every 60s            | Log affected tables; do not abort                                  |
| `MEMORY-PRESSURE`           | medium   | RSS exceeds 80% of system RAM (or `--max-memory` limit)                  | Periodic RSS sampling from /proc/self/status or sysinfo   | Force GC of row buffers; reduce batch size; log RSS trend          |
| `MONOTONIC-STALL`           | critical | 0 rows inserted across all workers in a 30-minute window                 | Compare `rows_done` across two state snapshots 30m apart  | Abort; the migration is not making progress                        |
| `CONNECTION-COUNT-CRITICAL` | high     | Target connection count > 95% of max_connections (including app traffic) | Periodic `SHOW PROCESSLIST` count                         | Reduce parallel workers by 1; recheck in 60s                       |
| `LATENCY-SPIKE-SOURCE`      | medium   | Source query latency > 10× baseline (80th percentile)                    | Per-batch query duration tracking                         | Check for replica lag, locks, or replica promotion                 |
| `BINLOG-LAG-TARGET`         | medium   | `SHOW MASTER STATUS` binlog position on target falling behind by > 1GB   | Periodic binlog position check                            | Warn: target binlog is filling; check sql_log_bin setting          |

#### Alert Emission Format

Alerts are written to the structured log as a `MIG-ALERT` diagnostic and can
also be delivered via callback hooks:

```json
{
  "timestamp": "2026-08-08T03:14:22Z",
  "level": "error",
  "phase": "data_migration",
  "alert_id": "DISK-TARGET-LOW",
  "severity": "high",
  "trace_id": "mig_prod_2026_08_07",
  "message": "Target disk low: 87 GB remaining (8.2%). Estimated 117 GB needed to complete. Reducing batch size to 500.",
  "fields": {
    "alert_id": "DISK-TARGET-LOW",
    "severity": "high",
    "disk_available_gb": 87,
    "disk_available_pct": 8.2,
    "disk_needed_estimate_gb": 117,
    "action_taken": "reduce_batch_size",
    "batch_size_old": 1000,
    "batch_size_new": 500
  }
}
```

### 2.5 Alert Delivery Mechanisms

| Mechanism              | Default | How                                    | When Available                                    |
| ---------------------- | ------- | -------------------------------------- | ------------------------------------------------- |
| Structured log (JSONL) | Yes     | Written to `--log-file` and stderr     | Always                                            |
| State file             | Yes     | Written to `--state-file` every 30s    | When `--state-file` is set                        |
| Exit code              | Yes     | Non-zero exit for critical/high alerts | Always                                            |
| Webhook                | Opt-in  | `--alert-webhook https://slack/api`    | POST JSON payload on alert                        |
| Slack                  | Opt-in  | `--alert-slack-webhook https://...`    | POST to Slack incoming webhook                    |
| PagerDuty              | Opt-in  | `--alert-pagerduty-key <integration>`  | Trigger PagerDuty event via Events API v2         |
| Opsgenie               | Opt-in  | `--alert-opsgenie-key <api_key>`       | Create Opsgenie alert via REST API                |
| Datadog metric         | Opt-in  | `--alert-datadog-metric`               | Emit custom metric via DogStatsD (localhost:8125) |
| CloudWatch metric      | Opt-in  | `--alert-cloudwatch-namespace`         | PutMetricData for a custom namespace              |

#### Webhook Payload

```json
{
  "migration_id": "mig_prod_2026_08_07",
  "alert_id": "DISK-TARGET-LOW",
  "severity": "high",
  "timestamp": "2026-08-08T03:14:22Z",
  "source_host": "replica:3306",
  "target_host": "target:3306",
  "runner": "migration-runner-01",
  "message": "Target disk low: 87 GB remaining (8.2%). Estimated 117 GB needed to complete.",
  "current_phase": "data_migration",
  "progress_pct": 67.4,
  "eta_completion": "2026-08-08T09:30:00Z",
  "state_file_snapshot": {/* ... same format as state file ... */}
}
```

### 2.6 Threshold Distinction: Failing vs. Slow-but-Healthy

The executor distinguishes between degradation (slow but progressing) and
failure (no progress or regressing) using monotonicity checks:

| Metric             | Healthy        | Degraded (medium alert)     | Failing (high/critical alert) |
| ------------------ | -------------- | --------------------------- | ----------------------------- |
| Rows inserted / 5m | > 0            | < 80% of 30m baseline       | 0 for 30 minutes              |
| Error rate / 5m    | < 0.01%        | 0.01% – 1%                  | > 1%                          |
| Batch duration     | < 5s           | 5s – 30s                    | > 30s (or timeout)            |
| RSS growth         | Flat or down   | Growing by > 10% / hour     | > 90% of system RAM           |
| Disk available     | > 15%          | 5% – 15%                    | < 5%                          |
| Connection drops   | 0              | 0 (but retry queue growing) | > 3 failed retry cycles       |
| Source latency     | < 10× baseline | 10× – 50× baseline          | > 50× baseline                |

Baselines are established from the first 30 minutes of data import (after the
initial ramp-up). The 30-minute baseline excludes the first 5 minutes to avoid
cold-cache effects.

---

## 3. Post-Migration Observability

### 3.1 Final Migration Report (`--report`)

The final report is a structured JSON document for audit, compliance, and
operational handoff:

```json
{
  "report_version": "1.0",
  "migration_id": "mig_prod_2026_08_07",
  "status": "success",
  "exit_code": 0,
  "started_at": "2026-08-07T12:00:00Z",
  "completed_at": "2026-08-08T10:30:00Z",
  "duration_s": 81000,
  "duration_human": "22h 30m",
  "source": {
    "uri": "mysql://replica:3306/production",
    "dialect": "mysql",
    "version": "8.0.36"
  },
  "target": {
    "uri": "mysql://target:3306/production",
    "dialect": "mysql",
    "version": "8.4.3"
  },
  "phases": {
    "pre_flight": {
      "status": "pass",
      "duration_s": 23,
      "checks_passed": 14,
      "checks_failed": 0,
      "checks_warned": 3,
      "blocking_override": "disk_warning_accepted"
    },
    "schema_extraction": {
      "status": "success",
      "duration_s": 47,
      "tables_extracted": 487,
      "warning": "Source schema stable during extraction (GTID unchanged)"
    },
    "plan_generation": {
      "status": "success",
      "duration_s": 2,
      "operations_total": 988,
      "hazards_detected": 0,
      "destructive_operations": 0
    },
    "ddl_pre_data": {
      "status": "success",
      "duration_s": 34,
      "operations_executed": 487,
      "operations_failed": 0,
      "staging_strategy_used": "direct"
    },
    "data_migration": {
      "status": "success",
      "duration_s": 80640,
      "tables_migrated": 487,
      "rows_migrated": 2137482563,
      "rows_skipped": 47,
      "bytes_migrated": 2251799813685,
      "throughput_avg_mb_sec": 27.9,
      "throughput_peak_mb_sec": 34.2,
      "throughput_min_mb_sec": 18.5,
      "parallel_workers_used": 4,
      "batch_count": 2137483,
      "batch_size_avg": 1000,
      "alerts_fired": 2,
      "alert_details": [
        {
          "id": "DISK-TARGET-LOW",
          "severity": "high",
          "timestamp": "2026-08-08T03:14:22Z",
          "resolved": true,
          "action": "batch_size_reduced_to_500"
        },
        {
          "id": "THROUGHPUT-DEGRADED",
          "severity": "medium",
          "timestamp": "2026-08-08T06:00:00Z",
          "resolved": true,
          "cause": "target_autovacuum_kicked_in"
        }
      ]
    },
    "ddl_post_data": {
      "status": "success",
      "duration_s": 180,
      "constraints_added": 312,
      "indexes_created": 189,
      "indexes_created_concurrently": 45,
      "not_valid_used": true
    },
    "verification": {
      "status": "success",
      "mode": "rowcount",
      "duration_s": 294,
      "tables_verified": 487,
      "mismatches": 0,
      "row_count_delta": 0,
      "fk_orphans_found": 0
    }
  },
  "warnings": [
    {
      "code": "MIG-CHARSET-WIDEN",
      "count": 12,
      "tables": ["legacy_logs_2019"],
      "message": "utf8mb3 columns widened to utf8mb4"
    },
    {
      "code": "MIG-RESERVED-KEYWORD",
      "count": 0,
      "message": "No reserved keyword collisions detected"
    },
    {
      "code": "MIG-ROW-SKIPPED",
      "count": 47,
      "tables": {
        "edge_case_types": 15,
        "zero_date_table": 8,
        "invalid_json": 24
      }
    }
  ],
  "errors": [],
  "resource_usage": {
    "peak_rss_mb": 2193,
    "avg_cpu_pct": 42.1,
    "peak_connections_source": 4,
    "peak_connections_target": 4,
    "io_read_mb": 2252000,
    "io_write_mb": 2252000
  },
  "verification": {
    "checksums_matched": true,
    "per_table": [
      {
        "table": "users",
        "source_rows": 50000,
        "target_rows": 50000,
        "delta": 0,
        "status": "match"
      },
      {
        "table": "orders",
        "source_rows": 98000000,
        "target_rows": 98000000,
        "delta": 0,
        "status": "match"
      },
      {
        "table": "edge_case_types",
        "source_rows": 20000,
        "target_rows": 19985,
        "delta": 15,
        "status": "mismatch",
        "reason": "15 rows skipped: NOT NULL violation (col_not_null)"
      }
    ]
  },
  "audit": {
    "runner_hostname": "migration-runner-01",
    "runner_user": "migration-bot",
    "sql_splitter_version": "0.15.0",
    "command": "migrate --source mysql://replica:3306/production --target mysql://target:3306/production --execute --verify rowcount --parallel 4 --chunk-column site_id --log-file /var/log/migration.jsonl --state-file /var/log/migration-state.json --report /var/log/migration-report.json",
    "git_sha": "abc123def",
    "environment": { "arch": "x86_64", "os": "linux", "rustc": "1.82.0" }
  },
  "recommendations": {
    "immediate": [
      "Run VACUUM ANALYZE on target to update query planner statistics",
      "Verify application connection strings point to new target",
      "Run application smoke tests against target"
    ],
    "before_cutover": [
      "Run --verify checksum for deep verification if --verify rowcount was used",
      "Check FK integrity with --verify fk",
      "Compare auto-increment sequence values between source and target"
    ],
    "ongoing": [
      "Monitor binlog disk usage for 7 days (2TB import → 2TB binlog generated)",
      "Set binlog_expire_logs_seconds to purge old binlogs",
      "Monitor application query performance; update indexes based on execution plans",
      "Keep old staging tables (__sql_splitter_old_*) for 72h before running --cleanup-staging"
    ]
  }
}
```

### 3.2 Verification Results Structure

Verification results are a standalone JSON document, also available via
`--verify ... --format json`:

```json
{
  "verification_id": "verify_2026-08-08T10:15:00Z",
  "migration_id": "mig_prod_2026_08_07",
  "mode": "checksum",
  "algorithm": "sha256",
  "status": "mismatch",
  "summary": {
    "tables_checked": 487,
    "tables_matched": 484,
    "tables_mismatched": 3,
    "total_delta_rows": 24
  },
  "mismatches": [
    {
      "table": "edge_case_types",
      "source_rows": 20000,
      "target_rows": 19985,
      "delta": 15,
      "source_checksum": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
      "target_checksum": "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a",
      "cause": "15 rows skipped: NOT NULL violation (col_not_null)",
      "chunk_mismatches": [
        {
          "chunk_id": "edge_case_types_004",
          "source_rows": 1000,
          "target_rows": 985,
          "delta": 15
        }
      ]
    },
    {
      "table": "zero_date_table",
      "source_rows": 5000,
      "target_rows": 4992,
      "delta": 8,
      "cause": "8 rows skipped: zero date values converted to NULL on target"
    },
    {
      "table": "invalid_json_logs",
      "source_rows": 10000,
      "target_rows": 9999,
      "delta": 1,
      "cause": "1 row skipped: invalid JSON in source column"
    }
  ],
  "fk_integrity": {
    "checked": true,
    "orphans_found": 0,
    "orphan_details": []
  },
  "sequence_check": {
    "checked": true,
    "mismatches": [
      {
        "table": "orders",
        "source_max_id": 98000123,
        "target_max_id": 98000123,
        "status": "match"
      },
      {
        "table": "users",
        "source_max_id": 50893,
        "target_sequence": 50000,
        "delta": -893,
        "action": "ALTER TABLE users AUTO_INCREMENT = 50894"
      }
    ]
  },
  "duration_s": 14400,
  "recommendation": "3 tables have row deltas. Review mismatches above. All deltas are from intentionally skipped rows (NOT NULL violations, zero dates, invalid JSON). No data corruption detected."
}
```

### 3.3 Ongoing Monitoring Recommendations

The final report includes a `recommendations.ongoing` section. Additional
structured recommendations:

```json
{
  "post_migration_monitoring": [
    {
      "id": "binlog-disk",
      "priority": "high",
      "metric": "Target binlog disk usage",
      "query": "SHOW BINARY LOGS",
      "threshold": "Automatically purge logs older than 24h after migration completes",
      "duration": "7 days"
    },
    {
      "id": "query-performance",
      "priority": "high",
      "metric": "Slow query log volume on target",
      "query": "SHOW GLOBAL STATUS LIKE 'Slow_queries'",
      "threshold": "Compare to source baseline; if 2×, investigate missing indexes",
      "duration": "30 days"
    },
    {
      "id": "connection-pool",
      "priority": "medium",
      "metric": "Application connection pool errors",
      "query": "Application metric: db_connection_errors_total",
      "threshold": "Any connection refused errors after cutover indicate incorrect connection string",
      "duration": "7 days"
    },
    {
      "id": "auto-increment-gap",
      "priority": "medium",
      "metric": "Auto-increment gap between source and target",
      "query": "SELECT MAX(id) FROM <table> on both source and target",
      "threshold": "If source > target, update AUTO_INCREMENT on target before writes begin",
      "duration": "Before application cutover"
    },
    {
      "id": "application-errors",
      "priority": "high",
      "metric": "Application error rate after cutover",
      "query": "Application metric: http_errors_total, db_errors_total",
      "threshold": "Any increase correlated with cutover time",
      "duration": "24 hours after cutover"
    },
    {
      "id": "replication-lag",
      "priority": "low",
      "metric": "Target replication lag (if target is a replica)",
      "query": "SHOW REPLICA STATUS",
      "threshold": "> 5 seconds",
      "duration": "Ongoing if target serves as replica"
    },
    {
      "id": "disk-growth",
      "priority": "low",
      "metric": "Target disk usage trend",
      "query": "Database size from information_schema",
      "threshold": "Unbounded growth may indicate uncleaned binlogs or staging tables",
      "duration": "30 days"
    }
  ]
}
```

---

## 4. Operational Runbook

### 4.1 Before Migration — Engineer Checklist

```
□ 1.  Run pre-flight checks 72h before migration window:
      sql-splitter migrate check --source <source> --target <target> --format json \
        --report preflight-72h.json

□ 2.  Resolve all blocking (status: "fail") checks from the report.

□ 3.  Estimate migration duration:
      sql-splitter migrate --source <source> --target <target> --bench --parallel 4

□ 4.  Provision migration runner:
      - CPU: 4+ cores (for parallel workers)
      - RAM: 4 GB minimum, 8 GB recommended for 2TB+ migrations
      - Disk: 200 GB for temp files, log files, state files (NOT the database data — that's on the target)
      - Network: <5ms RTT to both source and target
      - Run in screen/tmux or as a systemd service

□ 5.  Schedule migration window:
      - Block application writes to source tables being migrated (or read from replica)
      - Provision target storage at 2× data size (MySQL: data + binlog)
      - For managed MySQL: storage can only increase, never decrease, 1× per 6h

□ 6.  Capture source GTID position or LSN for consistency baseline:
      mysql -h source -e "SELECT @@gtid_executed"
      psql -h source -c "SELECT pg_current_wal_lsn()"

□ 7.  Prepare rollback connection strings to source (keep source running).

□ 8.  Notify stakeholders: migration window start, expected duration, rollback plan.

□ 9.  Run final pre-flight check 5 minutes before migration:
      sql-splitter migrate check --source <source> --target <target> --format json \
        --report preflight-final.json
      diff preflight-72h.json preflight-final.json  # Detect changed conditions

□ 10. Start continuous monitoring of source and target during migration:
      - Source replication lag
      - Target disk usage
      - Target connection count
      - Migration runner health (ps aux, free -m)
```

### 4.2 During Migration — Phase-Specific Recovery

#### Phase 0 (Schema Extraction)

| Failure                                     | Detection                        | Action                                                                                                                          |
| ------------------------------------------- | -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Source `information_schema` query times out | `MIG-SCHEMA-QUERY-TIMEOUT` error | Narrow scope with `--tables` filter to exclude unused schemas/tables. Increase `--query-timeout` from 30s default.              |
| Source schema changed during extraction     | `MIG-SCHEMA-CHANGED` warning     | Re-extract. If persists, lock tables for extraction duration (`FLUSH TABLES WITH READ LOCK`) or extract from a stopped replica. |
| Unsupported charset                         | `MIG-UNSUPPORTED-CHARSET` error  | Convert tables to compatible charset on source before migration, or accept charset widening with type conversion warnings.      |
| Reserved keyword collision                  | `MIG-RESERVED-KEYWORD` warning   | Rename columns on source before migration, or accept quoted identifiers (application code must handle quoted names).            |

#### Phase 1 (Pre-Data DDL)

| Failure                                       | Detection                        | Action                                                                                                                        |
| --------------------------------------------- | -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| CREATE TABLE fails (table exists)             | Idempotency check finds no error | Re-run migration. IF NOT EXISTS is idempotent.                                                                                |
| DROP CONSTRAINT fails on MySQL (non-existent) | MySQL error                      | Check constraint exists first (idempotency check). This is a bug if it occurs.                                                |
| MySQL DDL interrupted mid-ALTER               | `MIG-DDL-PARTIAL` error          | MySQL DDL is NOT transactional. Assess partial table state manually. If uncertain, drop and let idempotent CREATE restore it. |
| Postgres DDL interrupted                      | Transaction rolled back          | Postgres DDL is transactional. Re-run migration; the failed phase is entirely rolled back. Clean state.                       |

#### Phase 2 (Data Import)

| Failure                                       | Detection                             | Action                                                                                                                                                   |
| --------------------------------------------- | ------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Single table import fails with INSERT errors  | `MIG-ROW-SKIPPED`, `MIG-FK-ORPHAN`    | Review skipped rows in final report. If tolerable count, accept. If large count, investigate source data quality.                                        |
| INSERT fails due to target table missing      | FK dependency bug at wrong topo level | This is a bug in `SchemaGraph::topo_levels()`. The level grouping should prevent this. If it occurs, file a bug; the FK graph is incorrect.              |
| Target runs out of disk space                 | `DISK-TARGET-FULL` critical alert     | Migrate to a larger target. Increase storage if managed DB. Re-run migration (idempotent INSERT skips imported rows).                                    |
| Binlog fills target disk                      | `BINLOG-LAG-TARGET` medium alert      | If SUPER available: `SET SESSION sql_log_bin = 0` and restart import. If managed MySQL (no SUPER): increase storage, wait 6h, retry.                     |
| Connection dropped mid-import                 | `CONNECTION-LOST-TARGET` high alert   | Worker reconnects, reapplies session settings, resumes from last committed chunk (per PK chunk boundary).                                                |
| Parallel worker dies with no orphan detection | `WORKER-DIED` high alert              | Drain other workers at batch boundary. Run FK orphan check on tables that had in-progress workers. If orphans found, user must decide to delete or stop. |
| Self-referential FK row ordering fails        | INSERT fails on FK self-reference     | Self-ref ordering query (`ORDER BY CASE WHEN fk_col IS NULL...`) may have a bug. Verify row ordering query.                                              |
| Neon compute suspends mid-transaction         | Connection reset by peer              | Worker reconnects after wake (CF-5), reapplies session settings per transaction, continues from last check-pointed chunk.                                |
| PlanetScale VTGate kills SELECT               | `MIG-VTGATE-QUERY-TIMEOUT` error      | This should not occur if `--chunk-column` is mandatory and chunks are sized < 5 minutes of query time. Increase chunk granularity.                       |

#### Phase 3b (Post-Data DDL)

| Failure                                   | Detection                   | Action                                                                                                                                           |
| ----------------------------------------- | --------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| ADD CONSTRAINT fails (FK column mismatch) | MySQL error 1215, PG 42830  | Verify schema diff. Column types in source/target FK pairs must match exactly. Review plan output.                                               |
| CREATE INDEX times out on large table     | MySQL timeout, PG lock wait | Use `CREATE INDEX CONCURRENTLY` on PG. On MySQL, accept the index rebuild time (included in plan estimate).                                      |
| ADD FK NOT VALID fails on PG              | PG error                    | If `NOT VALID` syntax fails, fall back to standard ADD CONSTRAINT. This is an edge case for very old PG versions (< 9.4 — unlikely in practice). |

#### Phase 4 (Verification)

| Failure                            | Detection                 | Action                                                                                                                  |
| ---------------------------------- | ------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| Checksum mismatch                  | `MIG-VERIFY-MISMATCH`     | Run chunk-level diff for the mismatched table. Identify which rows differ. Determine if skipped rows are acceptable.    |
| Row count mismatch                 | `MIG-ROW-COUNT-DELTA`     | Cross-reference with skipped-row log. If all deltas are explained by intentional skips, accept.                         |
| FK orphans found                   | `MIG-FK-ORPHAN`           | FK orphaned rows were skipped during import (parent row didn't exist). Report as data quality issue, not migration bug. |
| Verification connection drops      | `CONNECTION-LOST-SOURCE`  | Verification is chunk-aware (HF-4). Restart verification — it resumes from last completed chunk, not from row 1.        |
| Verification takes too long (22h+) | Duration exceeds expected | Switch to `--verify-mode rowcount` for fast verification (COUNT(*) per table, seconds per table). Run checksum later.   |

### 4.3 After Successful Migration

```
□ 1.  Review final report:
      cat migration-report.json | jq '.recommendations.immediate'

□ 2.  Run deep verification if only rowcount was checked:
      sql-splitter migrate --source <src> --target <tgt> --verify checksum --format json

□ 3.  Fix auto-increment sequences:
      jq '.verification.sequence_check.mismatches' verify.json
      # ALTER TABLE <table> AUTO_INCREMENT = <correct_value>;

□ 4.  Update application connection strings to point to target.

□ 5.  Run application smoke tests against target:
      - Read queries: verify data is accessible
      - Write queries: verify constraints and triggers are intact
      - Full integration test suite

□ 6.  Monitor target for 24h:
      - Slow query log volume
      - Application error rate
      - Connection pool saturation
      - Disk usage trend

□ 7.  Schedule binlog cleanup:
      SET GLOBAL binlog_expire_logs_seconds = 86400;  # 1 day

□ 8.  Keep staging tables for 72h as rollback insurance:
      # Tables named __sql_splitter_old_<name> are preserved
      # After 72h verification period:
      sql-splitter cleanup-staging --target <target>

□ 9.  Archive migration artifacts:
      - preflight-final.json
      - migration-report.json
      - migration-log.jsonl
      - verify-results.json
      → Store in migration artifact bucket (S3/GCS) for 1 year (compliance)

□ 10. Schedule ongoing monitoring per §3.3 recommendations.
```

---

## 5. Alerting Architecture

### 5.1 Alert Lifecycle

```
┌──────────────────────────┐
│  Metric crosses          │
│  threshold               │
└───────────┬──────────────┘
            ▼
┌──────────────────────────┐    ┌───────────────────────────┐
│  Alert generated         │───▶│  Structured log entry      │
│  (MIG-ALERT diagnostic)  │    │  (JSONL, always)           │
└───────────┬──────────────┘    └───────────────────────────┘
            │
            ▼
┌──────────────────────────┐    ┌───────────────────────────┐
│  Severity check          │    │  Exit code update          │
│  (critical/high/medium)  │───▶│  (only for critical/high)  │
└───────────┬──────────────┘    └───────────────────────────┘
            │
            ▼
┌──────────────────────────┐
│  Optional delivery       │
│  (webhook, Slack, PD,    │
│   Opsgenie, Datadog, CW) │
└──────────────────────────┘
```

### 5.2 Alert Suppression and Deduplication

- **Deduplication window:** 10 minutes. Same alert ID (e.g., `THROUGHPUT-DROP`)
  from the same phase does not re-fire within the window.
- **Escalation:** A `medium` alert that persists for 60 minutes escalates to
  `high`. A `high` alert that persists for 30 minutes escalates to `critical`
  and triggers abort.
- **Auto-resolution:** When a metric returns to healthy range for 5 consecutive
  samples (5 minutes for per-minute samples), the alert is marked resolved
  and written to the log.
- **Silence on known conditions:** Post-data DDL phase inherently has zero
  throughput (no data movement). Zero-throughput alerts are suppressed during
  DDL phases.
- **First-5-minute suppression:** No alerts fire in the first 5 minutes of
  data migration (cold-cache effects, initial ramp-up). Alert suppression
  window is logged.

### 5.3 CLI Flags for Alerting

```
Alerting:
  --alert-threshold <PRESET>        strict|normal|relaxed (default: normal)
                                    strict:  tighter thresholds, fire sooner
                                    relaxed: wider thresholds, fewer false positives
  --alert-webhook <URL>             POST alert JSON to this URL
  --alert-slack-webhook <URL>       POST alert to Slack incoming webhook
  --alert-pagerduty-key <KEY>       PagerDuty Events API v2 integration key
  --alert-opsgenie-key <KEY>        Opsgenie REST API key
  --alert-datadog-metric <NAME>     Emit custom metric to DogStatsD at localhost:8125
  --alert-cloudwatch-namespace <NS> Emit PutMetricData to CloudWatch namespace
  --alert-repeat <SECS>             Minimum seconds between repeated alerts (default: 600)
  --alert-on <LIST>                 Comma-separated alert IDs to enable (default: all)
  --alert-suppress <LIST>           Comma-separated alert IDs to suppress
  --alert-verbose                   Include full state snapshot in webhook payloads
```

### 5.4 Alert Threshold Presets

| Preset    | `THROUGHPUT-DROP` | `DISK-TARGET-LOW` | `ERROR-RATE-SPIKE` | `MEMORY-PRESSURE` | `MONOTONIC-STALL` |
| --------- | ----------------- | ----------------- | ------------------ | ----------------- | ----------------- |
| `strict`  | < 30% of baseline | < 15% remaining   | > 0.5% skipped     | > 70% RAM         | 15m no progress   |
| `normal`  | < 20% of baseline | < 10% remaining   | > 1% skipped       | > 80% RAM         | 30m no progress   |
| `relaxed` | < 10% of baseline | < 5% remaining    | > 5% skipped       | > 90% RAM         | 60m no progress   |

---

## 6. Extension Points

### 6.1 Custom Alert Callbacks (Future)

For Phase 4+, a plugin/callback system:

```rust
pub trait AlertHandler: Send + Sync {
    fn on_alert(&self, alert: &Alert) -> Result<()>;
    fn on_resolve(&self, alert_id: &str) -> Result<()>;
    fn on_phase_change(&self, from: Phase, to: Phase) -> Result<()>;
    fn on_complete(&self, report: &MigrationReport) -> Result<()>;
}
```

### 6.2 OpenTelemetry Integration (Future)

For Phase 5+, optional OTLP export of spans and metrics:

```bash
sql-splitter migrate ... \
  --otel-endpoint https://api.honeycomb.io/v1/traces \
  --otel-headers "x-honeycomb-team=${HONEYCOMB_KEY}"
```

The span hierarchy from §6.7 maps directly to OpenTelemetry trace spans. The
structured log JSON Lines format is compatible with existing OTEL log adapters.

### 6.3 Metrics Export Without Alerting (Future)

```bash
sql-splitter migrate ... \
  --metrics-statsd localhost:8125 \
  --metrics-prometheus localhost:9090
```

Exports the same metrics described in §2.1 as gauges/counters for integration
with existing monitoring stacks (Grafana, Datadog, CloudWatch dashboards)
without requiring the full alerting subsystem.

---

## 7. Metrics Summary

### 7.1 All Metrics in One Table

| Category        | Metric                  | Description                                     | Emitted in Phase | Persisted in Report |
| --------------- | ----------------------- | ----------------------------------------------- | ---------------- | ------------------- |
| **Progress**    | `rows_done`             | Rows migrated per table and overall             | Data migration   | Yes                 |
|                 | `rows_total`            | Estimated total rows to migrate                 | Plan generation  | Yes                 |
|                 | `rows_skipped`          | Rows skipped due to errors                      | Data migration   | Yes                 |
|                 | `bytes_done`            | Bytes transferred per table and overall         | Data migration   | Yes                 |
|                 | `tables_done`           | Tables fully migrated                           | Data migration   | Yes                 |
|                 | `tables_total`          | Total tables in migration plan                  | Plan generation  | Yes                 |
|                 | `progress_pct`          | Overall completion percentage                   | Data migration   | Yes                 |
|                 | `current_level`         | Current topo level being processed              | Data migration   | Yes                 |
| **Throughput**  | `throughput_rows_sec`   | Rows per second                                 | Data migration   | Yes                 |
|                 | `throughput_mb_sec`     | Megabytes per second                            | Data migration   | Yes                 |
|                 | `batch_duration_ms`     | Duration of the last INSERT batch               | Data migration   | No                  |
|                 | `batch_size`            | Rows in the last batch                          | Data migration   | No                  |
| **Latency**     | `source_query_ms`       | Duration of SELECT query per batch              | Data migration   | No                  |
|                 | `target_insert_ms`      | Duration of INSERT execution per batch          | Data migration   | No                  |
|                 | `ddl_execution_ms`      | Duration of DDL statement execution             | DDL phases       | No                  |
| **Errors**      | `error_count`           | Cumulative error count                          | All              | Yes                 |
|                 | `warning_count`         | Cumulative warning count                        | All              | Yes                 |
|                 | `skipped_by_reason`     | Rows skipped grouped by error code              | Data migration   | Yes                 |
|                 | `connection_drops`      | Number of connection drops during migration     | All              | Yes                 |
| **Resources**   | `rss_mb`                | Resident set size (memory usage)                | All              | Yes                 |
|                 | `cpu_pct`               | CPU usage percentage                            | All              | Yes                 |
|                 | `connections_source`    | Number of connections open to source            | All              | Yes                 |
|                 | `connections_target`    | Number of connections open to target            | All              | Yes                 |
|                 | `io_read_mb`            | Total I/O read (cumulative)                     | Data migration   | Yes                 |
|                 | `io_write_mb`           | Total I/O write (cumulative)                    | Data migration   | Yes                 |
| **Timing**      | `duration_s`            | Duration of current phase / total migration     | All              | Yes                 |
|                 | `eta_remaining_s`       | Estimated seconds until completion              | Data migration   | Yes                 |
|                 | `eta_completion`        | Estimated wall-clock completion time (ISO 8601) | Data migration   | Yes                 |
| **Environment** | `disk_available_gb`     | Available disk on target                        | Pre-flight       | Yes                 |
|                 | `disk_required_gb`      | Estimated disk needed on target                 | Pre-flight       | Yes                 |
|                 | `rtt_source_ms`         | RTT from runner to source                       | Pre-flight       | Yes                 |
|                 | `rtt_target_ms`         | RTT from runner to target                       | Pre-flight       | Yes                 |
|                 | `throughput_bench_mbps` | Measured INSERT throughput from `--bench`       | Pre-flight       | Yes                 |

### 7.2 The Four Golden Signals of Migration Health

For the operator watching a 24-hour migration, these four metrics tell the
entire story:

1.  **Progress %** — Is data still moving? A flat line means stalled.
2.  **Throughput (rows/s)** — Is throughput stable, degrading, or improving?
    Trends matter more than absolute numbers.
3.  **Error rate (% skipped)** — Is data quality good? Spikes warrant
    investigation.
4.  **Disk remaining (GB)** — Will we run out of space? The one metric that
    can kill the migration.

Everything else (RSS, CPU, latency, connection count, chunk skew) is diagnostic
detail needed only when one of the four golden signals goes bad.

---

## 8. Implementation Sequencing

The observability and operations work is distributed across phases:

| Phase       | Observability Work                                                                                                                                                                                                                                  |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Phase 0** | Pre-flight check framework (`migrate check` subcommand). 18 checks. JSON output format. Exit codes. Baseline metric collection (version, RTT, disk).                                                                                                |
| **Phase 1** | Plan generation report (plan_summary + estimate in JSON).                                                                                                                                                                                           |
| **Phase 2** | Structured logging with span hierarchy (§6.7). Diagnostic codes (`MIG-*`). Row-level error collection (`MIG-ROW-SKIPPED`). State file writer. `--log-file`, `--state-file`. `MigrationMetrics` accumulator (thread-safe).                           |
| **Phase 3** | Progress reporting (`--progress bar` / `log` / `none`). Alert detection engine (threshold checks). Alert emission (`MIG-ALERT`). `--alert-webhook`, `--alert-slack-webhook`. Disk-full check. Connection-drop recovery alerts.                      |
| **Phase 4** | Final report generation (`--report`). Verification results structure. Chunk-aware verification resume. `--verify-mode rowcount`. Alert dedup + escalation. Alert threshold presets (`strict`/`normal`/`relaxed`). `--bench` throughput measurement. |
| **Phase 5** | Optional alert backends (PagerDuty, Opsgenie, Datadog, CloudWatch). OpenTelemetry integration. StatsD/Prometheus metrics export. Custom alert callback trait. E2E observability integration tests.                                                  |

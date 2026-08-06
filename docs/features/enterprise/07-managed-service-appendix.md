# Future: Managed Cloud Service Architecture

> **STATUS: DEFERRED — Not in current implementation plan.**
> This appendix captures speculative architecture for a future managed
> cloud migration service. The primary CLI with live database connections
> is the implementation focus. This appendix exists to preserve the
> design work for future reference.

## §5.3.1 Why a Managed Service?

The CLI model has hard limits for enterprise migration work:

| CLI Limit         | Problem                                                         |
| ----------------- | --------------------------------------------------------------- |
| Laptop disk space | 500 GB dump needs 500 GB free on a laptop                       |
| Laptop network    | WAN transfer of 500 GB over hotel WiFi is days                  |
| Laptop uptime     | Long-running migration (hours) can't survive laptop sleep       |
| Laptop auth       | Customer credentials on a migration engineer's personal machine |
| Collaboration     | Only one person can run the CLI at a time                       |
| Audit trail       | No record of who ran what migration with which parameters       |

A managed service (cloud-hosted, web UI, multi-tenant) addresses all of these.
But it changes the architecture from a single binary to a distributed system.

## §5.3.2 Service Components

```
┌─────────────────────────────────────────────────────────────────┐
│                        Web UI (Dashboard)                        │
│  • Select source type (PlanetScale / MySQL / PostgreSQL)         │
│  • Enter connection details (encrypted at rest)                  │
│  • Select target (MySQL / Serverless Postgres / RDS)             │
│  • Run assessment → review plan → execute migration              │
│  • Download runbook, view verification report                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │ HTTPS + WebSocket (progress)
┌──────────────────────────▼──────────────────────────────────────┐
│                        API Server (Rust)                         │
│  • POST /migrations — create a new migration job                 │
│  • GET  /migrations/:id — status, progress, results              │
│  • POST /migrations/:id/execute — approve and run                │
│  • WebSocket /migrations/:id/progress — real-time log stream     │
│  • Auth via SSO (or standalone API keys)                         │
└──────────────────────────┬──────────────────────────────────────┘
                           │ Job queue (PostgreSQL or Redis)
┌──────────────────────────▼──────────────────────────────────────┐
│                      Migration Worker (Rust)                     │
│  • Runs sql-splitter commands as a library, not subprocess       │
│  • Connects to source database (read-only)                       │
│  • Runs pscale database dump (if PlanetScale source)              │
│  • Streams through parser → analysis → plan generation           │
│  • Optionally executes migration on target (with approval)       │
│  • Writes results to object storage (S3-compatible)              │
│  • Reports progress via job queue → WebSocket → UI               │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│              Ephemeral Compute (AWS EC2 / Fargate)               │
│  • Per-migration worker instance                                │
│  • Scoped to customer's region (data locality)                   │
│  • Sized by dump size (small: 2 vCPU/4 GB, large: 16 vCPU/64 GB)│
│  • Ephemeral disk (gp3, sized to dump estimate)                  │
│  • Terminated after migration completes (or fails)               │
│  • Credentials injected via KMS/Secrets Manager, never logged     │
└──────────────────────────────────────────────────────────────────┘
```

## §5.3.3 Key Architectural Decisions

### Credential Management

Credentials for source and target databases are the most sensitive data in
the system. The architecture must ensure:

1. **Credentials never touch the web UI server.** The API server receives
   an encrypted blob, decrypts it in the worker, and the worker connects
   directly to the database. At no point does the API server process
   plaintext credentials.

2. **KMS-backed encryption.** Each migration job generates a data key.
   Credentials are encrypted with that key. The key is stored in AWS KMS
   (or equivalent). Only the worker instance (with IAM role access to KMS)
   can decrypt the credentials.

3. **Short-lived connections.** The worker connects, extracts, disconnects.
   No persistent connection pools holding credentials in memory.

4. **Audit log.** Every connection attempt (success or failure) is logged
   with timestamp, source IP, target host:port (NOT username/password).

```
Client (Web UI)
  │
  │ 1. User enters DB credentials
  │ 2. Browser encrypts with session-scoped key
  │
  ▼
API Server
  │
  │ 3. Receives encrypted credentials
  │ 4. Stores in job record (encrypted, never decrypted)
  │ 5. Enqueues migration job
  │
  ▼
Worker
  │
  │ 6. Receives job with encrypted credentials
  │ 7. Decrypts via KMS
  │ 8. Connects to source database
  │ 9. Runs migration
  │ 10. Destroys decrypted credentials from memory
  │
  ▼
Done — credentials never persisted in plaintext
```

### Ephemeral Compute Model

Each migration runs on a dedicated, short-lived compute instance:

| Migration Size         | vCPU | RAM   | Disk          | Max Duration | Cost (AWS on-demand) |
| ---------------------- | ---- | ----- | ------------- | ------------ | -------------------- |
| Small (< 10 GB)        | 2    | 4 GB  | 50 GB gp3     | 2 hours      | ~$0.50               |
| Medium (10–100 GB)     | 4    | 16 GB | 250 GB gp3    | 6 hours      | ~$4.00               |
| Large (100–500 GB)     | 8    | 32 GB | 1 TB gp3      | 12 hours     | ~$18.00              |
| XL (500 GB–1 TB)       | 16   | 64 GB | 2 TB gp3      | 24 hours     | ~$50.00              |
| PlanetScale (any size) | 2    | 4 GB  | Sized to dump | N/A          | ~$0.50 + dump time   |

The compute instance runs in the same AWS region as the target database.
This minimizes latency and data transfer costs.

### PlanetScale → AWS Data Transfer

For PlanetScale → cloud migrations, data must travel from
PlanetScale's infrastructure to AWS. Two paths:

1. **Internet path**: `pscale database dump` downloads to the worker
   instance over the public internet. PlanetScale throttles production
   dumps to avoid production impact. This is the only path for standard
   PlanetScale databases.

2. **Private path (Enterprise)**: PlanetScale Enterprise supports AWS
   PrivateLink. Data can travel over private networking. This requires
   coordination between PlanetScale and the cloud platform's AWS account.

The service should default to the internet path and offer the private
path configuration for Enterprise customers.

## §5.3.4 Security Model

| Concern                        | Mitigation                                                                                                                                    |
| ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Credential exposure in transit | TLS 1.3 for all connections; browser-to-API, API-to-worker, worker-to-DB                                                                      |
| Credential exposure at rest    | KMS-backed encryption per job; credentials never stored in plaintext                                                                          |
| Credential exposure in logs    | Structured logging with automatic PII redaction (host:port only, never user/pass)                                                             |
| Worker compromise              | IAM role with minimal permissions (KMS decrypt for one key, S3 write to one bucket); no outbound internet except to the specific DB endpoints |
| Multi-tenant isolation         | One worker per migration job; no shared compute; workers are firewalled from each other                                                       |
| Dump file exposure             | Ephemeral disk encrypted at rest (EBS default); shredded on instance termination                                                              |
| Replay attacks                 | One-time use data keys; job status prevents re-execution                                                                                      |
| SQL injection in table names   | Table names are validated against information_schema before inclusion in queries; never interpolated directly                                 |

## §5.3.5 Network Topology for Private Cloud

For private cloud customers (dedicated AWS account + VPC), the
managed migration service must operate within customer networking constraints:

```
┌──────────────────────────────────────────────────────────────┐
│                    Customer VPC                               │
│  ┌─────────┐  ┌───────────┐  ┌───────────────────────────┐   │
│  │ App     │  │ AWS RDS   │  │ Migration Worker           │   │
│  │ Compute │  │ (in VPC)  │  │ (VPC Endpoint Service)     │   │
│  └─────────┘  └───────────┘  └───────────────────────────┘   │
│                                                               │
│  VPC Peering / PrivateLink to the migration service's VPC     │
└──────────────────────────────────────────────────────────────┘
```

The migration worker connects to the customer's RDS instance via VPC
Peering or PrivateLink. No data traverses the public internet. Credentials
are still KMS-encrypted, but the network path is private.

## §5.3.6 PlanetScale-Specific: Subprocess in the Cloud

Running `pscale database dump` in a managed service requires:

1. **Pre-built AMI/container image** with the PlanetScale CLI installed
   and kept up to date.

2. **Service token injection**: The worker receives a PlanetScale service
   token (encrypted, KMS-backed) and authenticates with `pscale auth
login --service-token $TOKEN` before running the dump.

3. **Dump progress monitoring**: `pscale database dump` writes progress
   to stdout. The worker parses this and reports progress via the job
   queue → WebSocket → UI.

4. **Timeout and retry**: If the dump fails (network, PlanetScale outage,
   rate limiting), the worker retries with exponential backoff. The job
   status shows "Retrying (attempt 2/3)" to the user.

5. **Data transfer billing**: PlanetScale charges for outbound data
   transfer. The migration service must track this cost and either absorb
   it or pass it through to the customer.

## §5.3.7 API Design

```
POST /api/v1/migrations
  Body: {
    source_type: "planetscale" | "mysql" | "postgresql",
    source_config: { ... encrypted credentials ... },
    target_type: "mysql" | "serverless-postgres" | "rds-mysql" | "rds-postgres",
    target_config: { ... encrypted credentials ... },
    options: {
      tables: ["users", "orders"],  // optional filter
      skip_verify: false,
      dry_run: false,               // plan only, no execute
    }
  }
  Response: { migration_id: "mig_abc123", status: "pending_approval" }

POST /api/v1/migrations/mig_abc123/approve
  Response: { status: "running", estimated_duration: "45 minutes" }

GET /api/v1/migrations/mig_abc123
  Response: {
    status: "running",
    phase: "extracting_schema",
    progress: { tables_processed: 12, tables_total: 47, bytes_read: 1234567 },
    estimated_completion: "2026-08-06T14:30:00Z",
    logs: [...],
  }

WebSocket /api/v1/migrations/mig_abc123/progress
  → Real-time JSON stream of phase transitions, errors, and row counts

GET /api/v1/migrations/mig_abc123/runbook
  Response: { format: "markdown", content: "# Migration Runbook\n\n..." }

GET /api/v1/migrations/mig_abc123/verification
  Response: { tables_matched: 47, tables_mismatched: 0, row_count_delta: 0 }
```

## §5.3.8 Comparison: CLI vs. Managed Service

| Dimension              | CLI (Local)                                                     | Managed Service                                                                  |
| ---------------------- | --------------------------------------------------------------- | -------------------------------------------------------------------------------- |
| **Deployment**         | Single binary, `scp` to jump host                               | Cloud infrastructure (API, workers, job queue, KMS, S3)                          |
| **Auth model**         | Env vars, config file, CLI args                                 | KMS-backed encryption, service tokens, SSO                                       |
| **Disk space**         | Migration engineer's laptop                                     | Ephemeral EBS, sized to dump, shredded after                                     |
| **Network**            | Engineer's internet connection                                  | AWS backbone, same-region placement                                              |
| **Uptime**             | Laptop must stay on and connected                               | Cloud-managed, survives instance failures                                        |
| **Multi-tenancy**      | One engineer at a time                                          | Multiple concurrent migrations, isolated per job                                 |
| **Audit**              | Manual (engineer writes notes)                                  | Structured logs, event stream, immutable job records                             |
| **Collaboration**      | Screen sharing                                                  | Shared dashboard, status visible to team                                         |
| **Security posture**   | Credentials on personal machine                                 | KMS, IAM roles, ephemeral instances, no persistent credential storage            |
| **Development effort** | ~2-4 weeks for DB connection trait + MySQL + PG implementations | ~6-12 months for full service (API, workers, UI, auth, KMS, monitoring, billing) |
| **Operational burden** | None (CLI is stateless)                                         | 24/7 on-call, instance lifecycle, credential rotation, compliance                |
| **Target customer**    | Migration engineer (internal)                                   | Sales, CS, and self-serve customers                                              |

The CLI is the right starting point. A managed service is the right
aspiration — but only after the CLI proves the migration patterns work at
scale with real customers. Building the managed service first risks building
the wrong abstraction for workflows that haven't been validated.

### Managed Service Gaps (from Migration Execution Design)

Several enterprise readiness requirements are explicitly scoped to the
managed service phase only, not the CLI:

| Gap                                                | Why                                                    | When                 |
| -------------------------------------------------- | ------------------------------------------------------ | -------------------- |
| Multi-tenant isolation in managed service          | Separate compute, KMS, network isolation               | Managed service only |
| Regulatory compliance reporting (GDPR audit trail) | Immutable audit log, customer-facing compliance report | Managed service only |

These are architectural requirements that make little sense in a single-tenant
CLI tool. They belong entirely to the managed service's operational surface:
multi-tenant workloads need hardware isolation; regulatory compliance needs an
auditable system boundary that a CLI running on a laptop cannot provide.

## §5.3.9 Incremental Path: CLI → Managed Service

```
Phase 1 (now):        CLI with live DB connections
                      → Migration engineer runs locally

Phase 2 (3-6 months):  CLI with --output json + CI/CD scripts
                      → Migration engineer scripts repeatable paths

Phase 3 (6-12 months): Headless worker mode
                      → Same binary, run as a service with --worker flag
                      → Reads jobs from a queue, writes results to S3

Phase 4 (12+ months):  Web UI + API server
                      → Full managed service
```

Each phase builds on the previous without discarding work. The same
`DbSource` trait used in the CLI is called by the worker. The same
migration plan JSON format produced by the CLI is consumed by the API.

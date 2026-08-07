# Deferred Managed Service Appendix

This document is explicitly outside the MVP and production beta. No managed
service component, provider API, hosted credential store, webhook, or provider
subprocess is implied by the core CLI plan.

## Prerequisites

A service proposal can start only after stable, versioned CLI and library
contracts exist for plans, catalogs, canonical values, snapshots, execution
journals, verification reports, cancellation, and diagnostic codes. Both
same-dialect paths must pass the acceptance gates in
[08](./08-implementation-prerequisites.md).

## Possible later architecture

A later service could have an API/control plane, isolated ephemeral workers,
encrypted artifact storage, KMS-backed per-tenant credentials, private network
connectivity, immutable audit events, and provider-specific adapters. The
security design needs threat modeling, tenant isolation tests, credential
rotation and revocation, data retention controls, and regional processing rules.

The service may make a tightly scoped exception to invoke a provider CLI such as
`pscale` inside an isolated worker. That exception remains here only. It needs a
pinned executable, verified artifact, non-shell argument construction, bounded
resources, protected service-token delivery, redacted output, manifest
validation, and secure cleanup. It does not add `pscale://`, token flags, live
VTGate copy, or subprocess handling to the core CLI.

Provider APIs and external monitoring can expose information that SQL cannot,
such as volume free capacity or autosuspend settings. Such data must retain a
`provider_api` provenance and an unknown state when unavailable.

## Deferred online migration

Hosted execution does not make migration online. Online migration still needs a
CDC source, durable offsets, initial-snapshot coordination, ordered apply,
schema-change handling, lag monitoring, cutover fencing, and rollback policy.
These are separate future requirements.

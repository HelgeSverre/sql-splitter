# Synthetic Data Sidebar Design

## Goal

Separate the `generate` CLI command from the broader synthetic-data reference.
The command appears as an ordinary link alongside the other commands, while
the detailed model, operator, inference, privacy, diagnostics, and library
documentation lives in a separate top-level `Synthetic data` sidebar section.

## Navigation

```text
Commands
├─ Overview
├─ analyze
├─ …
├─ generate
├─ graph
└─ …

Synthetic data
├─ Model reference
├─ Generator reference
├─ Modifiers
├─ Planners
├─ Profiling and inference
├─ Privacy and verification
├─ Diagnostics
└─ Rust API
```

`Synthetic data` is the section label because it is concise, describes the
user-facing concept, and does not collide with the `generate` command or with
individual generators. `Generate pipeline` is too implementation-oriented;
`Generator` is ambiguous; `Synthetic Data Generation` is unnecessarily long
for the available sidebar width.

## Command page

`/commands/generate/` becomes a compact command reference consistent with the
other pages under Commands. It explains what the command does, shows the main
invocation patterns, documents its options and output/exit behavior, states
the synthetic-not-anonymized warning, and links to the detailed pages.

The command page does not duplicate the model language, generator catalog,
planner reference, inference details, diagnostic catalog, or Rust API.

## Detailed pages and URLs

The detailed pages keep their existing `/commands/generate/...` URLs. Sidebar
grouping is independent of URL structure in Starlight, so changing navigation
does not require breaking canonical diagnostic links, adding redirects, or
changing URLs embedded in the Rust diagnostic catalog.

Generator category pages remain absent from the primary sidebar. They stay
searchable, routable, and linked from Generator reference.

## Verification

The built-output regression test will assert that:

- `/commands/generate/` is an ordinary `generate` link under Commands;
- `Synthetic data` is a separate top-level group;
- the group contains the eight agreed detailed links in order;
- no nested bold `Generate` group exists;
- generator category pages do not appear in the primary sidebar.

Website lint, build, link validation, and the complete repository test suite
remain required before committing the implementation.

## Non-goals

- Moving or renaming detailed-page URLs.
- Changing synthetic-generation behavior or diagnostic URLs.
- Adding custom sidebar components or CSS.
- Reorganizing unrelated website sections.

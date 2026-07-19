# Synthetic Data Sidebar Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `generate` an ordinary command link and move its detailed reference pages into a separate top-level `Synthetic data` sidebar section.

**Architecture:** Keep every existing `/commands/generate/...` route, but configure Starlight navigation manually so URL layout and sidebar taxonomy are independent. Replace the mixed-purpose Generate landing page with a compact CLI reference that links to the detailed pages instead of duplicating them.

**Tech Stack:** Astro 7, Starlight 0.41, MDX, Bun test

## Global Constraints

- Preserve every existing documentation URL and canonical diagnostic URL.
- Use `Synthetic data` as the exact top-level sidebar label.
- Keep `/commands/generate/` as the ordinary lowercase `generate` command link.
- Keep generator category pages routable, searchable, and absent from the primary sidebar.
- Do not add custom Starlight components, CSS, redirects, or unrelated navigation changes.
- Keep the concise command page exactly as specified and preserve complete-model
  example validation by marking the existing model under the model reference's
  Imports section.
- Preserve the unrelated unstaged `website/package.json` change.

---

### Task 1: Separate the command and reference navigation roles

**Files:**

- Modify: `website/src/buildOutput.test.ts`
- Modify: `website/astro.config.mjs`
- Modify: `website/src/content/docs/commands/generate/index.mdx`
- Modify: `website/src/content/docs/commands/generate/model-reference.mdx`

**Interfaces:**

- Consumes: Starlight's manual `sidebar` configuration, built `dist/commands/generate/index.html`, and the generate-docs complete-model validation marker
- Produces: an ordinary `generate` Commands link, a top-level `Synthetic data` group, a concise command reference page, and a validated complete model in the model reference
- Preserves: the eight detailed reference slugs, five generator-category routes, and user-facing complete-model validation

- [ ] **Step 1: Replace the old sidebar test with the failing desired hierarchy**

Replace `the Generate sidebar exposes one shallow reference section` in
`website/src/buildOutput.test.ts` with:

```ts
test("generate is a command and Synthetic data owns its reference pages", () => {
  const html = readFileSync(
    join(dist, "commands", "generate", "index.html"),
    "utf8",
  );
  const sidebarStart = html.indexOf('<ul class="top-level');
  const sidebarEnd = html.indexOf("</nav>", sidebarStart);

  expect(sidebarStart).toBeGreaterThan(-1);
  expect(sidebarEnd).toBeGreaterThan(sidebarStart);

  const sidebar = html.slice(sidebarStart, sidebarEnd);
  const commandsStart = sidebar.indexOf(">Commands</span>");
  const syntheticDataStart = sidebar.indexOf(">Synthetic data</span>");
  const cookbookStart = sidebar.indexOf(">Cookbook</span>");

  expect(commandsStart).toBeGreaterThan(-1);
  expect(syntheticDataStart).toBeGreaterThan(commandsStart);
  expect(cookbookStart).toBeGreaterThan(syntheticDataStart);

  const commands = sidebar.slice(commandsStart, syntheticDataStart);
  const syntheticData = sidebar.slice(syntheticDataStart, cookbookStart);
  const referenceLinks = Array.from(
    syntheticData.matchAll(
      /<a href="(\/commands\/generate\/[^"#]*)"[^>]*><span[^>]*>([^<]+)<\/span><\/a>/g,
    ),
    ([, href, label]) => [href, label],
  );

  expect(commands).toMatch(
    /<a href="\/commands\/generate\/"[^>]*><span[^>]*>generate<\/span><\/a>/,
  );
  expect(commands).not.toMatch(/<summary[^>]*>[\s\S]*?>Generate<\/span>/);
  expect(referenceLinks).toEqual([
    ["/commands/generate/model-reference/", "Model reference"],
    ["/commands/generate/generators/", "Generator reference"],
    ["/commands/generate/modifiers/", "Modifiers"],
    ["/commands/generate/planners/", "Planners"],
    ["/commands/generate/inference/", "Profiling and inference"],
    ["/commands/generate/privacy-verification/", "Privacy and verification"],
    ["/commands/generate/diagnostics/", "Diagnostics"],
    ["/commands/generate/library-api/", "Rust API"],
  ]);
});
```

- [ ] **Step 2: Run the built-output test and verify the old hierarchy fails**

Run: `cd website && bun test src/buildOutput.test.ts`

Expected: FAIL because `Synthetic data` is absent and `generate` is currently a nested bold group.

- [ ] **Step 3: Configure the two sidebar roles**

In `website/astro.config.mjs`, replace the nested `Generate` item inside
Commands with the ordinary slug `"commands/generate"`. Immediately after the
Commands group, add:

```js
{
  label: "Synthetic data",
  items: [
    "commands/generate/model-reference",
    {
      slug: "commands/generate/generators",
      label: "Generator reference",
    },
    "commands/generate/modifiers",
    "commands/generate/planners",
    "commands/generate/inference",
    "commands/generate/privacy-verification",
    {
      slug: "commands/generate/diagnostics",
      label: "Diagnostics",
    },
    {
      slug: "commands/generate/library-api",
      label: "Rust API",
    },
  ],
},
```

The Commands order remains Overview, analyze, completions, convert, diff,
generate, graph, merge, order, query, redact, sample, shard, split, validate.

- [ ] **Step 4: Replace the command page with a focused CLI reference**

Replace `website/src/content/docs/commands/generate/index.mdx` with:

````mdx
---
title: generate
description: Generate relational synthetic SQL from a model, a source dump, or both
sidebar:
  order: 1
---

`generate` creates relational synthetic SQL from a hand-authored YAML model,
a model inferred from a SQL dump, or an inferred model plus explicit overrides.
Its alias is `gen`.

:::caution[Synthetic is not anonymized]
Generated output can replay literal choices, samples, defaults, and constraint
values. `GEN-SOURCE-VALUES` identifies rules that can replay literals without
printing the values. Use [`redact`](/commands/redact/) when you need irreversible
anonymization.
:::

## Usage

```bash
sql-splitter generate [OPTIONS] [INPUT]
```

Provide a source dump, a complete `--config` model, or both. An overrides
document requires a source dump because it patches the inferred base model.

## Common workflows

Generate directly from a dump:

```bash
sql-splitter generate production.sql -o synthetic.sql
```

Generate repeatable fixtures from a reviewed model:

```bash
sql-splitter generate --config model.yaml --seed 42 -o synthetic.sql
```

Infer and save an editable model without generating SQL:

```bash
sql-splitter generate production.sql --emit-config model.yaml
```

Apply overrides while saving the resolved model and generated SQL:

```bash
sql-splitter generate production.sql \
  --config overrides.yaml \
  --emit-config resolved.yaml \
  --scale 0.1 \
  -o synthetic.sql
```

## Preflight and reporting

```bash
# Parse and compile a complete model; write nothing
sql-splitter generate --config model.yaml --check

# Resolve counts and dependencies without generating rows
sql-splitter generate --config model.yaml --dry-run

# Explain inferred column rules without printing observed values
sql-splitter generate production.sql --dry-run --explain

# Generate, audit exact checks, then publish atomically
sql-splitter generate --config model.yaml --verify -o synthetic.sql
```

`--strict` promotes warning-severity diagnostics to failures. Informational
diagnostics and advisories remain non-fatal. `--quiet` suppresses ordinary human
reporting, but safety advisories still reach stderr.

## Options

| Input and model             | Meaning                            | Default     |
| --------------------------- | ---------------------------------- | ----------- |
| `[INPUT]`                   | Source SQL dump to profile         | none        |
| `-c, --config <PATH>`       | Complete model or overrides YAML   | none        |
| `--emit-config <PATH>`      | Write the resolved complete model  | none        |
| `-o, --output <PATH>`       | Generated SQL; `-` means stdout    | stdout      |
| `--profile-depth <MODE>`    | Profiling depth: `basic` or `full` | `basic`     |
| `--profile-sample <COUNT>`  | Retained profiling sample capacity | `1000`      |
| `--input-dialect <DIALECT>` | Force the source parsing dialect   | auto-detect |

| Rendering                  | Meaning                                    | Default                   |
| -------------------------- | ------------------------------------------ | ------------------------- |
| `--dialect <DIALECT>`      | Output SQL dialect                         | model, source, then MySQL |
| `--schema-only`            | Emit DDL only                              | false                     |
| `--data-only`              | Emit row data only                         | false                     |
| `--batch-size <COUNT>`     | Rows per `INSERT` or `COPY` batch          | `1000`                    |
| `--no-copy`                | Use PostgreSQL `INSERT` instead of `COPY`  | false                     |
| `--mssql-production-style` | Add production-style MSSQL DDL conventions | false                     |
| `--mssql-go <COUNT>`       | Emit `GO` every N MSSQL insert batches     | every batch               |
| `--compress <FORMAT>`      | Reserved; currently unavailable            | none                      |

| Volume and selection           | Meaning                                           | Default |
| ------------------------------ | ------------------------------------------------- | ------- |
| `--scale <FACTOR>`             | Multiply every resolved row count                 | none    |
| `--rows <COUNT>`               | Set absolute root-table counts                    | none    |
| `--table-rows <TABLE=COUNT>`   | Per-table absolute count; repeatable              | none    |
| `--table-scale <TABLE=FACTOR>` | Per-table scale; repeatable                       | none    |
| `--max-rows <COUNT>`           | Cap every table after other count controls        | none    |
| `--tables <GLOBS>`             | Include matching tables and required dependencies | all     |
| `--exclude <GLOBS>`            | Exclude matching tables                           | none    |

| Randomness     | Meaning                               | Default                |
| -------------- | ------------------------------------- | ---------------------- |
| `--seed <U64>` | Stable root seed                      | model or fresh entropy |
| `--randomize`  | Ignore configured seed; use a new one | false                  |

| Preflight and reporting | Meaning                                   | Default |
| ----------------------- | ----------------------------------------- | ------- |
| `--check`               | Compile a complete model only             | false   |
| `--dry-run`             | Compile and report resolved counts        | false   |
| `--verify`              | Generate, audit, and publish atomically   | false   |
| `--explain`             | Include inference decisions               | false   |
| `--strict`              | Fail on warning-severity diagnostics      | false   |
| `--json`                | Print a machine-readable report on stdout | false   |
| `--quiet`               | Suppress ordinary human reporting         | false   |
| `--progress`            | Accepted but not wired to generation      | false   |

## Important constraints

- `--check`, `--dry-run`, and `--verify` are mutually exclusive.
- `--schema-only` and `--data-only` are mutually exclusive.
- `--seed` and `--randomize` are mutually exclusive.
- `--scale` and `--rows` are mutually exclusive.
- A table cannot receive both `--table-rows` and `--table-scale`.
- `--json`, `--output -`, and `--emit-config -` each own stdout; select only one.
- `--check` requires a complete model and cannot profile `[INPUT]`.
- `--verify` requires a real output path and cannot use compression.
- MSSQL-specific flags require an explicit `--dialect mssql`.

## Detailed documentation

| Topic                      | Reference                                                            |
| -------------------------- | -------------------------------------------------------------------- |
| YAML model language        | [Model reference](/commands/generate/model-reference/)               |
| Column value producers     | [Generator reference](/commands/generate/generators/)                |
| Post-generation transforms | [Modifiers](/commands/generate/modifiers/)                           |
| Coordinated invariants     | [Planners](/commands/generate/planners/)                             |
| Dump profiling and choices | [Profiling and inference](/commands/generate/inference/)             |
| Safety and exact checks    | [Privacy and verification](/commands/generate/privacy-verification/) |
| Stable `GEN-*` codes       | [Diagnostics](/commands/generate/diagnostics/)                       |
| Embedding in Rust          | [Rust API](/commands/generate/library-api/)                          |

## Exit codes

| Code | Meaning                                                            |
| ---- | ------------------------------------------------------------------ |
| `0`  | Generation or the requested preflight mode succeeded               |
| `1`  | Model, warning-under-strict, runtime, I/O, or verification failure |
| `2`  | Invalid CLI usage or conflicting flags                             |
````

- [ ] **Step 4a: Preserve complete-model example validation in the model reference**

Add `{/* validate-generate-model */}` immediately before the existing complete
`kind: model` YAML example under **Imports** in
`website/src/content/docs/commands/generate/model-reference.mdx`. Keep the
concise command page exactly as specified above.

Run:

```bash
cargo nextest run --test generate_docs_test marked_complete_model_examples_compile
```

Expected: the existing user-facing model parses and compiles, and the focused
documentation test passes.

- [ ] **Step 5: Format, build, and run the focused regression**

Run:

```bash
cd website
bunx prettier astro.config.mjs src/buildOutput.test.ts src/content/docs/commands/generate/index.mdx src/content/docs/commands/generate/model-reference.mdx --write
bun run build
bun test src/buildOutput.test.ts
```

Expected: the build succeeds with all internal links valid and all four
`buildOutput.test.ts` tests pass.

- [ ] **Step 6: Run project verification and review the diff**

Run:

```bash
just website-lint
just website-build
just test
cargo clippy --all-targets -- -D warnings
git diff --check
```

Expected: Astro reports zero diagnostics, the website builds every page with
valid links, all Rust tests pass, Clippy reports no warnings, and the diff has
no whitespace errors.

- [ ] **Step 7: Commit only the implementation files**

Run:

```bash
git add website/astro.config.mjs \
  website/src/buildOutput.test.ts \
  website/src/content/docs/commands/generate/index.mdx \
  website/src/content/docs/commands/generate/model-reference.mdx \
  docs/superpowers/plans/2026-07-19-synthetic-data-sidebar.md
git diff --cached --check
git commit -m "docs: separate generate command from synthetic data reference"
```

Expected: the implementation is committed on `design/synthetic-data-generation`;
the unrelated `website/package.json` change remains unstaged.

# Website Documentation Review

**Reviewed**: 2026-01-21
**Current Version**: 1.12.6

## Status

- [x] Inaccuracies identified
- [x] Inaccuracies fixed (2026-01-21)
- [x] Command aliases documented (2026-01-21)
- [x] Missing flags documented (2026-01-21)
- [x] New pages created (2026-01-21)

---

## Inaccuracies (Fixed)

| Page                    | Issue                                                                     | Status   |
| ----------------------- | ------------------------------------------------------------------------- | -------- |
| `commands/index.mdx`    | "Universal Flags" claims all commands support flags that aren't universal | ✅ Fixed |
| `commands/query.mdx`    | `--tables` shown with `-t` short flag (doesn't exist)                     | ✅ Fixed |
| `commands/query.mdx`    | `--progress` shown with `-p` short flag (doesn't exist)                   | ✅ Fixed |
| `commands/redact.mdx`   | `--exclude` shown with `-e` short flag (should be `-x`)                   | ✅ Fixed |
| `commands/diff.mdx`     | `--format` missing `-f` short flag                                        | ✅ Fixed |
| `commands/order.mdx`    | Claims cycles get "deferred FK constraints" (not implemented)             | ✅ Fixed |
| `commands/validate.mdx` | JSON output example doesn't match actual schema                           | ✅ Fixed |
| `advanced/piping.mdx`   | Shows `-o -` for stdout (not supported, should omit `-o`)                 | ✅ Fixed |

---

## Missing Documentation (Completed)

### Command Aliases ✅

All command pages now document their alias (e.g., "**Alias:** `sp`").

### New Pages Created ✅

- [x] `commands/completions.mdx` - Shell completion installation
- [x] `reference/json-output.mdx` - JSON output structures for all commands
- [x] `advanced/glob-patterns.mdx` - Multi-file mode and glob pattern behavior
- [x] `reference/compression.mdx` - Supported compression formats (.gz, .bz2, .xz, .zst)

### Missing Flags Added ✅

- [x] `split`: added `-v, --verbose`
- [x] `query`: `--timing` already documented in options table

---

## Additional Items Completed (2026-01-21)

- [x] Analytics script added (Ahrefs)
- [x] Required flags clarified on `sample` and `shard` pages
- [x] Dynamic version (removed hardcoded version from installation.mdx)
- [x] Docker usage guide created (`guides/docker-usage.mdx`)
- [x] Benchmarking page created (`contributing/benchmarking.mdx`)
- [x] Command Builder interactive tool created (`tools/command-builder.mdx`)
- [x] Troubleshooting guide created (`guides/troubleshooting.mdx`)
- [x] Command Builder moved to top nav (via SocialIcons override)
- [x] Command Builder visual design improved (Oracle consultation)

---

## Documentation Complete

All planned documentation has been created and verified. The website now includes:

- 12 command pages with aliases
- 8 guide pages (including Docker, troubleshooting)
- 5 reference pages (including compression, JSON output)
- 4 advanced pages (including glob patterns)
- 3 contributing pages (including benchmarking)
- 1 interactive tool (Command Builder)
- Analytics tracking (Ahrefs)

# Website Build Pipeline Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace custom website artifact generation with Astro and Starlight integrations, remove dead website code, and make schema vendoring reproducible.

**Architecture:** Astro reads the informational release version from website package metadata through its typed environment schema. Starlight and the official Astro sitemap integration generate llms and sitemap files, while `just schemas` remains the explicit bridge between authoritative root schemas and the Vercel-ready vendored copy.

**Tech Stack:** Astro 7, Starlight 0.41, Bun 1.3, `starlight-llms-txt`, `@astrojs/sitemap`, Bun test, Just

## Global Constraints

- Preserve unrelated worktree changes and the existing website dependency upgrades.
- Keep generation deterministic and fail verification when derived files drift.
- Do not add a slug or path dependency when Starlight IDs and platform URL APIs suffice.
- Keep `schemas/` authoritative and `website/public/schemas/` committed for website-only Vercel builds.
- Leave implementation changes uncommitted because `website/package.json` contains pre-existing user edits.

---

### Task 1: Share OG route normalization

**Files:**

- Create: `website/src/ogPath.test.ts`
- Create: `website/src/ogPath.ts`
- Modify: `website/src/routeData.ts`
- Modify: `website/src/pages/og/[...slug].png.ts`

**Interfaces:**

- Produces: `getOgImageSlug(contentId: string): string`
- Consumes: Starlight's normalized content entry and route IDs

- [ ] **Step 1: Write the failing unit test**

```ts
import { describe, expect, test } from "bun:test";
import { getOgImageSlug } from "./ogPath";

describe("getOgImageSlug", () => {
  test.each([
    ["index", "index"],
    ["getting-started/index", "getting-started"],
    ["commands/split", "commands/split"],
  ])("maps %s to %s", (contentId, expected) => {
    expect(getOgImageSlug(contentId)).toBe(expected);
  });
});
```

- [ ] **Step 2: Run the test and confirm the missing helper fails**

Run: `cd website && bun test src/ogPath.test.ts`

Expected: FAIL because `src/ogPath.ts` does not exist.

- [ ] **Step 3: Implement and use the helper**

```ts
export function getOgImageSlug(contentId: string): string {
  const segments = contentId.split("/").filter(Boolean);
  if (segments.at(-1) === "index") segments.pop();
  return segments.join("/") || "index";
}
```

Use the helper in both OG path consumers. Construct the absolute meta URL with `new URL()` and `context.site ?? context.url`.

- [ ] **Step 4: Run the focused test**

Run: `cd website && bun test src/ogPath.test.ts`

Expected: 3 PASS, 0 FAIL.

### Task 2: Delegate derived outputs to Astro and Starlight

**Files:**

- Create: `website/src/buildOutput.test.ts`
- Modify: `website/package.json`
- Modify: `website/astro.config.mjs`
- Modify: `website/tsconfig.json`
- Modify: `website/src/components/SiteTitle.astro`
- Delete: `website/scripts/update-version.ts`
- Delete: `website/src/version.ts`
- Delete: `website/src/pages/sitemap.xml.ts`
- Delete: `website/llms.txt`
- Delete: `website/public/llms.txt`

**Interfaces:**

- Produces: `SQL_SPLITTER_VERSION` from `astro:env/server`
- Produces: `dist/llms.txt`, `dist/llms-full.txt`, `dist/llms-small.txt`, `dist/sitemap-index.xml`

- [ ] **Step 1: Write the failing build-output test**

```ts
import { expect, test } from "bun:test";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const dist = join(root, "dist");

test("framework integrations own llms and sitemap outputs", () => {
  expect(existsSync(join(dist, "llms.txt"))).toBe(true);
  expect(existsSync(join(dist, "llms-full.txt"))).toBe(true);
  expect(existsSync(join(dist, "llms-small.txt"))).toBe(true);
  expect(existsSync(join(dist, "sitemap-index.xml"))).toBe(true);
  expect(existsSync(join(dist, "sitemap.xml"))).toBe(false);
});

test("the Starlight header shows the package version", () => {
  const html = readFileSync(
    join(dist, "getting-started", "index.html"),
    "utf8",
  );
  expect(html).toContain("v1.15.0");
});
```

- [ ] **Step 2: Run the test against the baseline build**

Run: `cd website && bun test src/buildOutput.test.ts`

Expected: FAIL because `llms-full.txt` and `llms-small.txt` are absent and the handwritten `sitemap.xml` exists.

- [ ] **Step 3: Update dependencies and package scripts**

Run:

```bash
cd website
bun remove @astrojs/react @resvg/resvg-js @types/react @types/react-dom react react-dom tsx
bun add starlight-llms-txt
bun add --dev @astrojs/check typescript
bun pm pkg set version=1.15.0 scripts.og="bun scripts/generate-og-image.ts" scripts.test:unit="bun test src/ogPath.test.ts" scripts.test:build="bun run build && bun test src/buildOutput.test.ts"
```

Remove `prebuild` from `package.json`.

- [ ] **Step 4: Configure typed version and llms generation**

Import `envField`, website package metadata, and `starlight-llms-txt` in `astro.config.mjs`. Define a server/public `SQL_SPLITTER_VERSION` string whose default is the package version, add the llms plugin to Starlight, and remove the React integration and Vite `define` block.

Import `SQL_SPLITTER_VERSION` from `astro:env/server` in `SiteTitle.astro`.

- [ ] **Step 5: Remove replaced files and use Astro's base TypeScript preset**

Delete the updater, injected version module, handwritten sitemap route, and static llms copies. Change `tsconfig.json` to extend `astro/tsconfigs/base`.

- [ ] **Step 6: Build and run the artifact test**

Run:

```bash
cd website
bun run build
bun test src/buildOutput.test.ts
```

Expected: build succeeds and both tests pass.

### Task 3: Remove dead website files and stale documentation

**Files:**

- Modify: `website/src/pages/schemas/index.astro`
- Modify: `website/src/content/docs/contributing/website-og-images.mdx`
- Delete: `website/index.html`
- Delete: `website/style.css`
- Delete: `website/apple-touch-icon.png`
- Delete: `website/favicon-192.png`
- Delete: `website/favicon.ico`
- Delete: `website/src/styles/global.css`

**Interfaces:**

- Consumes: Astro's `public/` directory as the only static asset source

- [ ] **Step 1: Remove files with no active importer or build consumer**

Delete the legacy homepage, stylesheet, duplicated favicon files, and unused global stylesheet. Keep `og-image.html`, which is consumed by `scripts/generate-og-image.ts`.

- [ ] **Step 2: Remove stale imports and dependency references**

Remove the unused `glob` import from the schema index page. Update the OG contributor page to name Sharp, which the active renderer uses, instead of the unused `@resvg/resvg-js` package.

- [ ] **Step 3: Verify no removed dependency or file remains referenced**

Run:

```bash
cd website
rg -n "@astrojs/react|@resvg/resvg-js|src/version|update-version|styles/global.css|src/pages/sitemap.xml" . -g '!node_modules' -g '!dist' || true
```

Expected: no matches.

### Task 4: Make schema vendoring and release version sync explicit

**Files:**

- Modify: `justfile`
- Modify: `AGENTS.md`
- Modify: `website/src/buildOutput.test.ts`

**Interfaces:**

- Consumes: authoritative `schemas/*.schema.json`
- Produces: byte-identical `website/public/schemas/*.schema.json`

- [ ] **Step 1: Extend the artifact test with schema equality**

Read sorted `.schema.json` filenames from both directories, assert equal file lists, and assert equal bytes for each pair.

- [ ] **Step 2: Run the schema assertion**

Run: `cd website && bun test src/buildOutput.test.ts`

Expected: PASS for the current synchronized checkout.

- [ ] **Step 3: Harden `just schemas` and `just bump`**

Before copying schemas, delete only `website/public/schemas/*.schema.json`; copy the root set; then run `diff -qr schemas website/public/schemas`. Add `cd website && bun pm pkg set version={{ new_version }}` to `just bump`. Delete `website-update-version` and remove it from `release-prepare`.

- [ ] **Step 4: Update repository guidance**

Document that the Starlight plugin generates llms files at build time and that `just schemas` owns the committed website schema mirror.

- [ ] **Step 5: Verify schema and version commands**

Run:

```bash
diff -qr schemas website/public/schemas
just --list
```

Expected: no schema differences and successful recipe listing.

### Task 5: Full verification and review

**Files:**

- Verify all modified website and repository tooling files

- [ ] **Step 1: Run unit and generated-artifact tests**

Run: `cd website && bun run test:unit && bun run test:build`

Expected: all tests pass.

- [ ] **Step 2: Run website diagnostics and formatting**

Run:

```bash
just website-lint
just website-validate-schemas
```

Expected: both commands pass without prompts, diagnostics, or formatting drift.

- [ ] **Step 3: Run a clean production build and recheck artifacts**

Run:

```bash
just website-build
cd website && bun test src/buildOutput.test.ts
```

Expected: build and artifact tests pass; only the official sitemap index/shard files exist.

- [ ] **Step 4: Review the complete diff**

Run: `git diff --check && git status --short && git diff --stat`

Expected: no whitespace errors; unrelated pre-existing work remains intact.

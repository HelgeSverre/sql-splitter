# Website build pipeline cleanup

**Status:** Approved

## Goal

Let Astro and Starlight generate derived website artifacts, remove custom
version and sitemap machinery, and make the remaining generated-file ownership
explicit.

## Build architecture

The website commits source content and framework configuration. Astro and
Starlight generate deployment artifacts during `bun run build`:

- `starlight-llms-txt` generates `llms.txt`, `llms-full.txt`, and
  `llms-small.txt` from the docs collection.
- `@astrojs/sitemap` is the only sitemap generator and emits
  `sitemap-index.xml` plus its sitemap shards.
- Astro's typed environment schema exposes the informational sql-splitter
  version to the server-rendered site title.

The website package version mirrors the Cargo package version. `just bump`
updates both package manifests through their package-management tools. The
website build does not parse or rewrite `Cargo.toml`, `llms.txt`, or generated
HTML.

## Routes and path handling

Starlight content IDs are already URL-friendly slugs. A focused shared helper
maps an ID ending in an `index` segment to the route represented by its parent
and maps the root index to `index` for the OG image route. Both the Starlight
route middleware and OG static-path generator use this helper.

The helper uses string segments and URL construction. No slug or path utility
dependency is needed.

## JSON schema ownership

Root `schemas/` remains the authoritative checked-in output because Rust
integration tests consume those files and the website directory is excluded
from packaged crates. `website/public/schemas/` remains a committed vendored
copy because Vercel builds with `website/` as its project root.

`just schemas` owns the complete pipeline: generate and format root schemas,
validate them, remove stale vendored schema files, copy the authoritative set,
and verify both directories are identical. Website schema validation operates
on the vendored copy.

## TypeScript and dependencies

The website extends `astro/tsconfigs/base`. `@astrojs/check` and `typescript`
are explicit development dependencies so repository lint commands are
non-interactive and reproducible.

Unused React integration packages, `@resvg/resvg-js`, and `tsx` are removed.
Satori, Sharp, Fontsource, and Playwright remain because the active OG image
pipelines use them.

## Dead files

Remove the handwritten sitemap route, version module and updater, duplicate
static llms files, unused global stylesheet, and legacy root-level static
homepage/favicon copies. Keep `og-image.html` because the homepage OG image
generator consumes it.

## Failure behavior

- A missing or invalid website version fails Astro environment validation.
- Invalid documentation fails the Starlight build or link validator.
- Schema generation fails if Rust validation fails or if the copied website
  schemas differ from the authoritative directory.
- Sitemap and llms outputs are checked after a production build.

## Verification

Verification covers the shared OG slug helper, Astro diagnostics, formatting,
schema validation and synchronization, production build output, generated
llms files, the official sitemap index, and absence of the removed custom
`sitemap.xml` route.

import { expect, test } from "bun:test";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL("..", import.meta.url));
const dist = join(root, "dist");
const schemas = fileURLToPath(new URL("../../schemas/", import.meta.url));
const vendoredSchemas = join(root, "public", "schemas");
const cargoToml = readFileSync(
  new URL("../../Cargo.toml", import.meta.url),
  "utf8",
);
const cargoVersion = cargoToml.match(/^version\s*=\s*"([^"]+)"/m)?.[1];

if (!cargoVersion) {
  throw new Error("Could not find the package version in Cargo.toml");
}

test("framework integrations own llms and sitemap outputs", () => {
  expect(existsSync(join(dist, "llms.txt"))).toBe(true);
  expect(existsSync(join(dist, "llms-full.txt"))).toBe(true);
  expect(existsSync(join(dist, "llms-small.txt"))).toBe(true);
  expect(existsSync(join(dist, "sitemap-index.xml"))).toBe(true);
  expect(existsSync(join(dist, "sitemap.xml"))).toBe(false);
});

test("the Starlight header shows the Cargo package version", () => {
  const html = readFileSync(
    join(dist, "getting-started", "index.html"),
    "utf8",
  );

  expect(html).toContain(`v${cargoVersion}`);
});

test("the Generate sidebar exposes one shallow reference section", () => {
  const html = readFileSync(
    join(dist, "commands", "generate", "index.html"),
    "utf8",
  );
  const sidebarStart = html.indexOf('<ul class="top-level');
  const sidebarEnd = html.indexOf("</nav>", sidebarStart);

  expect(sidebarStart).toBeGreaterThan(-1);
  expect(sidebarEnd).toBeGreaterThan(sidebarStart);

  const sidebar = html.slice(sidebarStart, sidebarEnd);
  const generateLinks = Array.from(
    sidebar.matchAll(
      /<a href="(\/commands\/generate\/[^"#]*)"[^>]*><span[^>]*>([^<]+)<\/span><\/a>/g,
    ),
    ([, href, label]) => [href, label],
  );

  expect(sidebar).toMatch(
    /<summary[^>]*><span[^>]*><span[^>]*>Generate<\/span>/,
  );
  expect(generateLinks).toEqual([
    ["/commands/generate/", "Overview"],
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

test("the website schema mirror matches the authoritative schemas", () => {
  const schemaFiles = readdirSync(schemas)
    .filter((file) => file.endsWith(".schema.json"))
    .sort();
  const vendoredFiles = readdirSync(vendoredSchemas)
    .filter((file) => file.endsWith(".schema.json"))
    .sort();

  expect(vendoredFiles).toEqual(schemaFiles);

  for (const file of schemaFiles) {
    expect(readFileSync(join(vendoredSchemas, file))).toEqual(
      readFileSync(join(schemas, file)),
    );
  }
});

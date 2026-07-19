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

test("the Starlight header links to Context7", () => {
  const html = readFileSync(
    join(dist, "getting-started", "index.html"),
    "utf8",
  );

  expect(html).toContain("https://context7.com/helgesverre/sql-splitter");
  expect(html).toContain("Context7");
  expect(html).toContain("data-context7-icon");
  expect(html).not.toContain("context7-icon-green.svg");
});

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
      /<a href="(\/commands\/generate\/(?!generators\/)[^"#]*)"[^>]*><span[^>]*>([^<]+)<\/span><\/a>/g,
    ),
    ([, href, label]) => [href, label],
  );

  expect(commands).toMatch(
    /<a href="\/commands\/generate\/"[^>]*><span[^>]*>generate<\/span><\/a>/,
  );
  expect(commands).not.toMatch(/<summary[^>]*>[\s\S]*?>Generate<\/span>/);
  expect(syntheticData).toMatch(
    /<summary[^>]*>[\s\S]*?<span[^>]*>Generators<\/span>[\s\S]*?<svg[^>]*class="caret/,
  );
  expect(syntheticData).not.toContain(">Generator reference</span>");
  expect(referenceLinks).toEqual([
    ["/commands/generate/model-reference/", "Model reference"],
    ["/commands/generate/modifiers/", "Modifiers"],
    ["/commands/generate/planners/", "Planners"],
    ["/commands/generate/inference/", "Profiling and inference"],
    ["/commands/generate/privacy-verification/", "Privacy and verification"],
    ["/commands/generate/diagnostics/", "Diagnostics"],
    ["/commands/generate/library-api/", "Rust API"],
  ]);

  const generatorSubitems = syntheticData.matchAll(
    /<a href="\/commands\/generate\/generators\/([^"#]*)"[^>]*><span[^>]*>([^<]+)<\/span><\/a>/g,
  );

  expect(
    Array.from(generatorSubitems, ([, href, label]) => [href, label]),
  ).toEqual([
    ["", "Overview"],
    ["core/", "Core generators"],
    ["semantic/", "Semantic generators"],
    ["credentials/", "Credential generators"],
    ["observed-statistical/", "Observed and statistical generators"],
    ["relationships/", "Relationship generators"],
  ]);
});

test("the Generators group expands only for generator pages", () => {
  const generateHtml = readFileSync(
    join(dist, "commands", "generate", "index.html"),
    "utf8",
  );
  const generatorHtml = readFileSync(
    join(dist, "commands", "generate", "generators", "core", "index.html"),
    "utf8",
  );

  const groupTag = (html: string) => {
    const labelIndex = html.indexOf(">Generators</span>");
    const detailsStart = html.lastIndexOf("<details", labelIndex);
    const detailsEnd = html.indexOf(">", detailsStart);

    expect(labelIndex).toBeGreaterThan(-1);
    expect(detailsStart).toBeGreaterThan(-1);
    expect(detailsEnd).toBeGreaterThan(detailsStart);

    return html.slice(detailsStart, detailsEnd + 1);
  };

  expect(groupTag(generateHtml)).not.toContain(" open");
  expect(groupTag(generatorHtml)).toContain(" open");
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

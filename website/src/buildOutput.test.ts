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

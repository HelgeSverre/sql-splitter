import { expect, test } from "bun:test";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import packageJson from "../package.json" with { type: "json" };

const root = fileURLToPath(new URL("..", import.meta.url));
const dist = join(root, "dist");
const schemas = fileURLToPath(new URL("../../schemas/", import.meta.url));
const vendoredSchemas = join(root, "public", "schemas");

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

  expect(html).toContain(`v${packageJson.version}`);
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

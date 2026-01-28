#!/usr/bin/env node
/**
 * Updates version references in static files before build.
 * Run as part of the build process.
 *
 * On Vercel (or other CI without Cargo.toml), this script is a no-op
 * since the version should already be updated in the committed files.
 */

import { readFileSync, writeFileSync, existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));

// Get version from Cargo.toml
const cargoPath = resolve(__dirname, "../../Cargo.toml");

// On Vercel/CI, Cargo.toml won't exist - skip gracefully
if (!existsSync(cargoPath)) {
  console.log("Cargo.toml not found (CI/Vercel deploy) - skipping version update");
  console.log("Version should already be updated in committed files.");
  process.exit(0);
}

const cargoContent = readFileSync(cargoPath, "utf-8");
const versionMatch = cargoContent.match(/^version\s*=\s*"([^"]+)"/m);
if (!versionMatch) {
  console.error("Could not find version in Cargo.toml");
  process.exit(1);
}
const version = versionMatch[1];

console.log(`Updating version references to ${version}`);

// Update llms.txt
const llmsPath = resolve(__dirname, "../llms.txt");
let llmsContent = readFileSync(llmsPath, "utf-8");
llmsContent = llmsContent.replace(
  /# sql-splitter \d+\.\d+\.\d+/,
  `# sql-splitter ${version}`
);
writeFileSync(llmsPath, llmsContent);
console.log(`  ✓ Updated llms.txt`);

console.log("Done!");

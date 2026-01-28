import { readFileSync, existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Reads the version from the root Cargo.toml file.
 * This ensures the website always displays the current version
 * without manual updates.
 */
export function getVersion(): string {
  // Try multiple possible locations for Cargo.toml
  const possiblePaths = [
    // Development: from src/version.ts -> ../../Cargo.toml
    resolve(dirname(fileURLToPath(import.meta.url)), "../../Cargo.toml"),
    // Build time: from website root
    resolve(process.cwd(), "../Cargo.toml"),
    // Fallback: absolute path from workspace
    resolve(process.cwd(), "../../Cargo.toml"),
  ];

  for (const cargoPath of possiblePaths) {
    if (existsSync(cargoPath)) {
      const content = readFileSync(cargoPath, "utf-8");
      const match = content.match(/^version\s*=\s*"([^"]+)"/m);
      if (match) {
        return match[1];
      }
    }
  }

  // Fallback: extract version from llms.txt which is updated by prebuild script
  const llmsPaths = [
    resolve(dirname(fileURLToPath(import.meta.url)), "../llms.txt"),
    resolve(process.cwd(), "llms.txt"),
    resolve(process.cwd(), "../llms.txt"),
  ];

  for (const llmsPath of llmsPaths) {
    if (existsSync(llmsPath)) {
      const llmsContent = readFileSync(llmsPath, "utf-8");
      const llmsMatch = llmsContent.match(/# sql-splitter (\d+\.\d+\.\d+)/);
      if (llmsMatch) {
        return llmsMatch[1];
      }
    }
  }

  console.warn("Could not determine version from Cargo.toml or llms.txt");
  return "0.0.0";
}

export const VERSION = getVersion();

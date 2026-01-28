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

  // Fallback: return a placeholder that the prebuild script will have updated
  console.warn("Could not find Cargo.toml, using fallback version detection");
  return "0.0.0";
}

export const VERSION = getVersion();

#!/usr/bin/env node
/**
 * Renders og-image.html to public/og-image.png (1200x630).
 *
 * The homepage BaseLayout references a static image at /og-image.png, which
 * Astro serves from public/. Generating straight into public/ keeps the
 * committed image and its HTML source from drifting apart.
 *
 * Usage: bun run og   (or: npx tsx scripts/generate-og-image.ts)
 */

import { resolve, dirname } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { chromium } from "playwright";

const __dirname = dirname(fileURLToPath(import.meta.url));
const source = resolve(__dirname, "../og-image.html");
const output = resolve(__dirname, "../public/og-image.png");

const browser = await chromium.launch();
const page = await browser.newPage({
  viewport: { width: 1200, height: 630 },
  deviceScaleFactor: 1,
});
await page.goto(pathToFileURL(source).href);
// Wait for the webfont to load so text renders with the intended face.
await page.evaluate(() => document.fonts.ready);
await page.screenshot({
  path: output,
  clip: { x: 0, y: 0, width: 1200, height: 630 },
});
await browser.close();

console.log(`Rendered ${output}`);

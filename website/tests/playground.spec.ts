import { expect, test } from "@playwright/test";

// End-to-end proof that the committed WASM artifact actually runs in a
// browser: load an example dump, see the real profiler's analysis, generate
// synthetic SQL from it.
test("playground analyzes an example dump and generates synthetic SQL", async ({
  page,
}) => {
  await page.goto("/playground/");

  await page.getByRole("button", { name: /examples/i }).click();
  await page.getByRole("button", { name: /mysql — web shop/i }).click();

  // The real profiler found the fixture's tables
  const sidebar = page.locator(".pg-sidebar");
  await expect(sidebar.getByRole("button", { name: /^users/ })).toBeVisible({
    timeout: 20000,
  });
  await expect(sidebar.getByRole("button", { name: /^orders/ })).toBeVisible();

  // The inference assigned a semantic generator to users.email
  await expect(page.locator(".pg-columns")).toContainText("email");

  await page.locator(".pg-btn-primary").click();
  await expect(page.locator(".pg-sql pre")).toContainText("INSERT INTO", {
    timeout: 20000,
  });

  // Determinism marketing claim: seed is in the UI and defaults to 42
  await expect(page.locator('input[x-model\\.number="seed"]')).toHaveValue(
    "42",
  );
});

test("playground rejects compressed uploads with a helpful message", async ({
  page,
}) => {
  await page.goto("/playground/");

  const gzipHeader = Buffer.from([0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00]);
  await page.locator('input[type="file"]').setInputFiles({
    name: "dump.sql.gz",
    mimeType: "application/gzip",
    buffer: gzipHeader,
  });

  await expect(page.locator(".pg-error")).toContainText("gzip");
});

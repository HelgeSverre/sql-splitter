import { expect, test } from "@playwright/test";

// End-to-end proof that the committed WASM artifact actually runs in a
// browser: load an example dump, see the real profiler's analysis, generate
// synthetic SQL from it.
test("playground analyzes an example dump and generates synthetic SQL", async ({
  page,
}) => {
  await page.goto("/playground/");

  await page.getByRole("button", { name: /examples/i }).click();
  await page.getByRole("button", { name: /^SaaS · MySQL/i }).click();

  // The real profiler found the fixture's tables
  const sidebar = page.locator(".pg-sidebar");
  await expect(sidebar.getByRole("button", { name: /^users/ })).toBeVisible({
    timeout: 20000,
  });
  await expect(sidebar.getByRole("button", { name: /^orders/ })).toBeVisible();

  // The inference assigned a generator to a users column
  await sidebar.getByRole("button", { name: /^users/ }).click();
  await expect(page.locator(".pg-columns")).toContainText("email");

  await page.locator(".pg-btn-primary").click();
  await expect(page.locator(".pg-sql pre")).toContainText("INSERT INTO", {
    timeout: 30000,
  });

  // Determinism marketing claim: seed is in the UI and defaults to 42
  await expect(page.locator('input[x-model\\.number="seed"]')).toHaveValue(
    "42",
  );
});

test("model tab, cross-dialect output, and warnings tab work", async ({
  page,
}) => {
  await page.goto("/playground/");

  await page.getByRole("button", { name: /examples/i }).click();
  await page.getByRole("button", { name: /^SaaS · MySQL/i }).click();
  const sidebar = page.locator(".pg-sidebar");
  await expect(sidebar.getByRole("button", { name: /^users/ })).toBeVisible({
    timeout: 20000,
  });

  // Model tab defaults to the tree explorer, open to depth 2: the tables
  // node is expanded and table names are visible
  await page.getByRole("button", { name: "Model" }).click();
  await expect(page.locator(".pg-tree .pg-t-toggle").first()).toBeVisible({
    timeout: 30000,
  });
  const tablesRow = page.locator('.pg-t-toggle[data-path="$.tables"]');
  await expect(tablesRow).toBeVisible();
  await expect(
    page.locator('.pg-t-toggle[data-path="$.tables.users"]'),
  ).toBeVisible();
  // Collapsing the tables node hides its children and shows a count badge
  await tablesRow.click();
  await expect(
    page.locator('.pg-t-toggle[data-path="$.tables.users"]'),
  ).toBeHidden();
  await expect(tablesRow.locator(".pg-t-badge")).toContainText("{");
  // And expanding again brings them back
  await tablesRow.click();
  await expect(
    page.locator('.pg-t-toggle[data-path="$.tables.users"]'),
  ).toBeVisible();
  // Raw view still shows the exact YAML document
  await page.getByRole("button", { name: "Raw", exact: true }).click();
  await expect(page.locator(".pg-model")).toContainText("kind: model");

  // Cross-dialect: mysql source rendered as mssql emits GO batches
  await page.locator('select[x-model="outDialect"]').selectOption("mssql");
  await page.locator(".pg-btn-primary").click();
  await expect(page.locator(".pg-sql")).toContainText("GO", {
    timeout: 30000,
  });

  // Warnings tab renders structured entries
  await page.getByRole("button", { name: /warnings/i }).click();
  await expect(page.locator(".pg-warnings")).toBeVisible();
});

test("every example loads and analyzes through the real pipeline", async ({
  page,
}) => {
  const examples = [
    { label: /^SaaS · Postgres/i, chip: /postgres/ },
    { label: /^Dealership · MySQL/i, chip: /mysql/ },
    { label: /^Ledger · MSSQL/i, chip: /mssql/ },
    { label: /^CMS · SQLite/i, chip: /sqlite/ },
  ];
  await page.goto("/playground/");
  for (const example of examples) {
    await page.getByRole("button", { name: /examples/i }).click();
    await page.getByRole("button", { name: example.label }).click();
    await expect(
      page.locator(".pg-sidebar .pg-table-item").first(),
    ).toBeVisible({ timeout: 30000 });
    await expect(page.locator(".pg-chip")).toContainText(example.chip, {
      timeout: 5000,
    });
    expect(
      await page.locator(".pg-sidebar .pg-table-item").count(),
    ).toBeGreaterThan(3);
  }
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

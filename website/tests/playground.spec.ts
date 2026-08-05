import { expect, test } from "@playwright/test";

// End-to-end proof that the committed WASM artifact actually runs in a
// browser: load an example dump, see the real profiler's analysis, generate
// synthetic SQL from it.
test("playground analyzes an example dump and generates synthetic SQL", async ({
  page,
}) => {
  await page.goto("/playground/");

  await page.getByTestId("examples-menu").click();
  await page.getByTestId("example-saas-mysql").click();

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

  await page.getByTestId("examples-menu").click();
  await page.getByTestId("example-saas-mysql").click();
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
  await page.getByTestId("out-dialect-menu").click();
  await page.getByTestId("out-dialect-mssql").click();
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
    { testId: "example-saas-postgres", chip: /postgres/ },
    { testId: "example-dealership-mysql", chip: /mysql/ },
    { testId: "example-ledger-mssql", chip: /mssql/ },
    { testId: "example-cms-sqlite", chip: /sqlite/ },
  ];
  await page.goto("/playground/");
  for (const example of examples) {
    await page.getByTestId("examples-menu").click();
    await page.getByTestId(example.testId).click();
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

test("sidebar resizes by dragging and persists across reloads", async ({
  page,
}) => {
  await page.goto("/playground/");
  await page.getByRole("button", { name: /examples/i }).click();
  await page.getByRole("button", { name: /^SaaS · MySQL/i }).click();
  const sidebar = page.locator(".pg-sidebar");
  await expect(sidebar).toBeVisible({ timeout: 20000 });

  const before = (await sidebar.boundingBox())!.width;
  expect(Math.round(before)).toBe(240);

  const handle = (await page.locator(".pg-resizer").boundingBox())!;
  await page.mouse.move(handle.x + 2, handle.y + handle.height / 2);
  await page.mouse.down();
  await page.mouse.move(handle.x + 122, handle.y + handle.height / 2, {
    steps: 5,
  });
  await page.mouse.up();

  const widened = (await sidebar.boundingBox())!.width;
  expect(widened).toBeGreaterThan(340);
  expect(widened).toBeLessThan(400);

  // Persists across a reload
  await page.reload();
  await page.getByRole("button", { name: /examples/i }).click();
  await page.getByRole("button", { name: /^SaaS · MySQL/i }).click();
  await expect(sidebar).toBeVisible({ timeout: 20000 });
  expect((await sidebar.boundingBox())!.width).toBeCloseTo(widened, 0);

  // Double-click resets
  await page.locator(".pg-resizer").dblclick();
  expect(Math.round((await sidebar.boundingBox())!.width)).toBe(240);
});

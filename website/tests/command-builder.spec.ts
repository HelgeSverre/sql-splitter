import { test, expect } from '@playwright/test';

test.describe('Command Builder', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/tools/command-builder/');
    // Wait for React hydration by waiting for astro-island to be present and JS to load
    await page.waitForSelector('astro-island[client="load"]', { timeout: 10000 });
    // Give React time to hydrate
    await page.waitForTimeout(500);
  });

  test('renders with default split command', async ({ page }) => {
    const output = page.locator('#generated-command code');
    await expect(output).toContainText('sql-splitter split dump.sql -o output/');
  });

  test('updates command when input file changes', async ({ page }) => {
    const input = page.locator('#input-file');
    await input.fill('mydata.sql');

    const output = page.locator('#generated-command code');
    await expect(output).toContainText('sql-splitter split mydata.sql');
  });

  test('updates command when output changes', async ({ page }) => {
    const output = page.locator('#output');
    await output.fill('tables/');

    const commandOutput = page.locator('#generated-command code');
    await expect(commandOutput).toContainText('-o tables/');
  });

  test('switches command when clicking analyze', async ({ page }) => {
    await page.click('[data-command="analyze"]');

    const output = page.locator('#generated-command code');
    await expect(output).toContainText('sql-splitter analyze');
    // Analyze should not have -o flag
    await expect(output).not.toContainText('-o');
  });

  test('shows second input for diff command', async ({ page }) => {
    // Second input should be hidden initially
    await expect(page.locator('#input-file2-group')).toBeHidden();

    await page.click('[data-command="diff"]');

    // Second input should be visible for diff
    await expect(page.locator('#input-file2-group')).toBeVisible();

    const output = page.locator('#generated-command code');
    await expect(output).toContainText('sql-splitter diff dump.sql new.sql');
  });

  test('shows convert dialect options', async ({ page }) => {
    await page.click('[data-command="convert"]');

    // Target dialect should be visible
    await expect(page.locator('#to-dialect-group')).toBeVisible();

    const output = page.locator('#generated-command code');
    await expect(output).toContainText('--to postgres');
  });

  test('sample command shows percent option', async ({ page }) => {
    await page.click('[data-command="sample"]');

    await expect(page.locator('#percent-group')).toBeVisible();

    const output = page.locator('#generated-command code');
    await expect(output).toContainText('--percent 10');
  });

  test('redact command shows hash/fake options', async ({ page }) => {
    await page.click('[data-command="redact"]');

    await expect(page.locator('#hash-group')).toBeVisible();
    await expect(page.locator('#fake-group')).toBeVisible();

    await page.fill('#hash-patterns', '*.email');
    await page.fill('#fake-patterns', '*.name');

    const output = page.locator('#generated-command code');
    await expect(output).toContainText('--hash "*.email"');
    await expect(output).toContainText('--fake "*.name"');
  });

  test('flags update command output', async ({ page }) => {
    await page.check('#progress');
    await page.check('#dry-run');

    const output = page.locator('#generated-command code');
    await expect(output).toContainText('--progress');
    await expect(output).toContainText('--dry-run');
  });

  test('dialect selection updates command', async ({ page }) => {
    await page.selectOption('#dialect', 'postgres');

    const output = page.locator('#generated-command code');
    await expect(output).toContainText('-d postgres');
  });

  test('copy button exists and is clickable', async ({ page }) => {
    const copyBtn = page.locator('#copy-command');
    await expect(copyBtn).toBeVisible();
    await expect(copyBtn).toContainText('Copy');
  });

  test('query command shows interactive flag', async ({ page }) => {
    await page.click('[data-command="query"]');

    await expect(page.locator('#interactive-opt')).toBeVisible();
    await expect(page.locator('#query-group')).toBeVisible();
  });

  test('shard command shows tenant options', async ({ page }) => {
    await page.click('[data-command="shard"]');

    await expect(page.locator('#tenant-column-group')).toBeVisible();
    await expect(page.locator('#tenant-value-group')).toBeVisible();

    await page.fill('#tenant-value', '42');

    const output = page.locator('#generated-command code');
    await expect(output).toContainText('--tenant-column tenant_id');
    await expect(output).toContainText('--tenant-value 42');
  });
});

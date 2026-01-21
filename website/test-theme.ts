import { chromium } from "playwright";

async function captureScreenshots() {
  const browser = await chromium.launch();
  const context = await browser.newContext({
    viewport: { width: 1280, height: 900 },
  });
  const page = await context.newPage();

  // Docs page - Default (auto/system)
  await page.goto("http://localhost:4321/getting-started/");
  await page.waitForLoadState("networkidle");
  await page.screenshot({
    path: "screenshots/docs-default.png",
    fullPage: false,
  });
  console.log("Captured: docs-default.png");

  // Docs page - Force light mode via Starlight dropdown
  await page.click("starlight-theme-select");
  await page.waitForTimeout(300);
  await page.screenshot({
    path: "screenshots/docs-dropdown-open.png",
    fullPage: false,
  });
  console.log("Captured: docs-dropdown-open.png");

  // Select "Light"
  await page.click("starlight-theme-select select");
  await page.selectOption("starlight-theme-select select", "light");
  await page.waitForTimeout(500);
  await page.screenshot({
    path: "screenshots/docs-light-forced.png",
    fullPage: false,
  });
  console.log("Captured: docs-light-forced.png");

  // Focus on sidebar
  await page.screenshot({
    path: "screenshots/docs-sidebar-light-forced.png",
    clip: { x: 0, y: 0, width: 350, height: 900 },
  });
  console.log("Captured: docs-sidebar-light-forced.png");

  // Select "Dark"
  await page.selectOption("starlight-theme-select select", "dark");
  await page.waitForTimeout(500);
  await page.screenshot({
    path: "screenshots/docs-dark-forced.png",
    fullPage: false,
  });
  console.log("Captured: docs-dark-forced.png");

  // Focus on sidebar in dark mode
  await page.screenshot({
    path: "screenshots/docs-sidebar-dark-forced.png",
    clip: { x: 0, y: 0, width: 350, height: 900 },
  });
  console.log("Captured: docs-sidebar-dark-forced.png");

  // Now navigate to homepage and check if theme persists
  await page.goto("http://localhost:4321/");
  await page.waitForLoadState("networkidle");
  await page.screenshot({
    path: "screenshots/homepage-after-docs-dark.png",
    fullPage: false,
  });
  console.log("Captured: homepage-after-docs-dark.png");

  await browser.close();
  console.log("Done!");
}

captureScreenshots().catch(console.error);

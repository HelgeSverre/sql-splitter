import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: "list",
  use: {
    baseURL: "http://localhost:4399",
    trace: "on-first-retry",
  },
  webServer: {
    command: "npm run preview -- --port 4399",
    url: "http://localhost:4399",
    reuseExistingServer: !process.env.CI,
    timeout: 30000,
  },
});

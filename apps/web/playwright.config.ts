import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  timeout: 45_000,
  workers: 1,
  use: {
    baseURL: process.env.NETWORKFORGE_WEB_BASE_URL ?? "http://localhost:3000",
    browserName: "chromium",
    channel: process.env.CI ? undefined : "msedge",
    viewport: { width: 1360, height: 900 },
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
  },
});

import { test, expect } from "@playwright/test";
import { readFile } from "node:fs/promises";

test("template validates and runs through background jobs", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: /^Read-through cache/ }).click();
  await expect(page.getByText(/3 components.*2 connections/)).toBeVisible();
  await expect(page.getByText("Model ready for simulation.")).toBeVisible();
  await page.getByRole("button", { name: "Run simulation" }).click();
  await expect(page.getByText("RUN RESULTS")).toBeVisible();
  await expect(page.getByText("SAMPLED REQUEST TRACE")).toBeVisible();
});

test("exported architecture can be imported", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: /^Async workers/ }).click();
  const downloadPromise = page.waitForEvent("download");
  await page.getByRole("button", { name: "Export JSON" }).click();
  const download = await downloadPromise;
  const path = await download.path();
  if (!path) throw new Error("Download path unavailable");
  const exported = JSON.parse(await readFile(path, "utf8"));
  expect(exported.format).toBe("networkforge.architecture.v1");
  expect(exported.architecture.nodes.some((node: { kind: string }) => node.kind === "queue")).toBe(true);
  await page.getByRole("button", { name: /^API baseline/ }).click();
  await page.getByLabel("Import architecture JSON").setInputFiles(path);
  await expect(page.getByText(/4 components.*3 connections/)).toBeVisible();
  await expect(page.getByText("Model ready for simulation.")).toBeVisible();
});

test("experiment reconnects after page reload", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("spinbutton", { name: "Requests / second", exact: true }).fill("500");
  await page.getByLabel("Duration / seconds").fill("10");
  await page.getByRole("combobox", { name: /^Runs/ }).selectOption("100");
  await page.getByRole("button", { name: "Run experiment" }).click();
  await expect.poll(() => page.evaluate(() => localStorage.getItem("networkforge-active-job"))).not.toBeNull();
  await page.reload();
  await expect(page.getByText("MEAN THROUGHPUT")).toBeVisible({ timeout: 30_000 });
  await expect.poll(() => page.evaluate(() => localStorage.getItem("networkforge-active-job"))).toBeNull();
});

test("measured data calibration reports a fitted scenario", async ({ page }) => {
  await page.goto("/");
  await page.getByRole("button", { name: "Compare with measurements" }).click();
  await expect(page.getByLabel("Calibration report")).toContainText("Best service-time scale:");
  await expect(page.getByLabel("Calibration report")).toContainText("fitted score:");
});

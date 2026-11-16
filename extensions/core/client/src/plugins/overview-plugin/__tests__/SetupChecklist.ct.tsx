import { expect, test } from "@playwright/experimental-ct-react";
import type { Page } from "@playwright/test";

import SetupChecklistHarness from "./SetupChecklistHarness";

async function seedProgress(page: Page, entries: Record<string, boolean>) {
  await page.evaluate((progress) => {
    return new Promise<void>((resolve, reject) => {
      const req = indexedDB.open("ocm-fleet-setup-progress", 1);
      req.onupgradeneeded = () => {
        const db = req.result;
        if (!db.objectStoreNames.contains("steps")) {
          db.createObjectStore("steps");
        }
      };
      req.onsuccess = () => {
        const db = req.result;
        const tx = db.transaction("steps", "readwrite");
        const store = tx.objectStore("steps");
        for (const [key, value] of Object.entries(progress)) {
          store.put(value, key);
        }
        tx.oncomplete = () => {
          db.close();
          resolve();
        };
        tx.onerror = () => reject(tx.error);
      };
      req.onerror = () => reject(req.error);
    });
  }, entries);
}

test("a rejected live check leaves the stored progress to decide", async ({
  mount,
  page,
}) => {
  let searches = 0;
  await page.route(/queryResources/, (route) => {
    searches += 1;
    return route.fulfill({
      status: 500,
      contentType: "application/json",
      body: JSON.stringify({ message: "the inventory is unavailable" }),
    });
  });
  await seedProgress(page, { registry: true });

  await mount(<SetupChecklistHarness />);

  // The checklist paints even though both live reads failed; the stored
  // progress marks the one step done and leaves the other one actionable.
  await expect(page.getByText("Getting started")).toBeVisible();
  await expect(page.getByText("1 of 2 complete")).toBeVisible();
  await expect(
    page.getByText("Connect a registry", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Create a provider", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: "Configure Create a provider" }),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: "Configure Connect a registry" }),
  ).toHaveCount(0);
  expect(searches).toBe(2);
});

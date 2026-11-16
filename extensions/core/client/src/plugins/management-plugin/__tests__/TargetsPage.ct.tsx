import { expect, test } from "@playwright/experimental-ct-react";
import { ScalprumContext } from "@scalprum/react-core";
import { MemoryRouter } from "react-router-dom";

import TargetsPage from "../TargetsPage";

const scalprum = {
  initialized: true,
  config: {},
  api: { fleet: { getPluginPagePath: () => "/plugins/clusters" } },
  pluginStore: {},
} as never;

function mountPage() {
  return (
    <ScalprumContext.Provider value={scalprum}>
      <MemoryRouter>
        <TargetsPage />
      </MemoryRouter>
    </ScalprumContext.Provider>
  );
}

function target(id: string) {
  return {
    id,
    name: id,
    type: "kubernetes",
    state: "ready",
    labels: {},
    acceptedManifestTypes: ["manifest.kubernetes.io/v1"],
    origin: "provisioned",
    cluster: `clusters/${id}`,
  };
}

interface DeliveryStatus {
  targetId: string;
  state: string;
  operation?: string;
  attempts?: number;
  message?: string;
}

function deployment(name: string, status: DeliveryStatus) {
  return {
    name,
    uid: name,
    manifestStrategy: { type: "TYPE_INLINE" },
    placementStrategy: { type: "TYPE_STATIC" },
    resolvedTargetIds: [status.targetId],
    state: "STATE_ACTIVE",
    reconciling: false,
    createTime: "2026-01-01T00:00:00Z",
    updateTime: "2026-01-01T00:00:00Z",
    etag: "etag",
    targetStatuses: [status],
  };
}

test("reports a failed first read without a count and refetches on Retry", async ({
  mount,
  page,
}) => {
  let targetReads = 0;
  await page.route(
    "**/apis/open-cluster-management.io/v1alpha/targets",
    (route) => {
      targetReads += 1;
      if (targetReads === 1) {
        return route.fulfill({
          status: 503,
          contentType: "application/json",
          body: JSON.stringify({
            message: "the target inventory is unavailable",
          }),
        });
      }
      return route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ targets: [target("k8s-hub-a")] }),
      });
    },
  );
  await page.route("**/v1alpha/deployments*", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ deployments: [], nextPageToken: "" }),
    }),
  );

  await mount(mountPage());

  // A read that failed has no count to report: the error stands alone.
  await expect(page.getByText("The targets did not load")).toBeVisible();
  await expect(
    page.getByText("the target inventory is unavailable"),
  ).toBeVisible();
  await expect(page.getByText("0 registered")).toHaveCount(0);

  await page.getByRole("button", { name: "Retry" }).click();

  await expect(page.getByRole("row", { name: /k8s-hub-a/ })).toBeVisible();
  await expect(page.getByText("1 registered")).toBeVisible();
  expect(targetReads).toBe(2);
});

test("labels each delivery by state and counts only active work", async ({
  mount,
  page,
}) => {
  await page.route(
    "**/apis/open-cluster-management.io/v1alpha/targets",
    (route) =>
      route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ targets: [target("k8s-hub-a")] }),
      }),
  );
  const deployments = [
    deployment("deployments/applied", {
      targetId: "k8s-hub-a",
      state: "STATE_DELIVERED",
      operation: "OPERATION_DELIVER",
    }),
    deployment("deployments/broken", {
      targetId: "k8s-hub-a",
      state: "STATE_FAILED",
      operation: "OPERATION_DELIVER",
      attempts: 3,
      message: "apply manifest 1: conflict",
    }),
    deployment("deployments/retired", {
      targetId: "k8s-hub-a",
      state: "STATE_DELIVERED",
      operation: "OPERATION_REMOVE",
    }),
  ];
  await page.route("**/v1alpha/deployments*", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ deployments, nextPageToken: "" }),
    }),
  );

  await mount(mountPage());

  const row = page.getByRole("row", { name: /k8s-hub-a/ });
  // A delivered delivery is the deployment's name alone; every other
  // state says what it is, and the color follows the state.
  await expect(
    row.locator(".pf-v6-c-label", { hasText: "applied" }),
  ).toHaveClass(/pf-m-green/);
  await expect(
    row.locator(".pf-v6-c-label", { hasText: "broken · failed" }),
  ).toHaveClass(/pf-m-red/);
  // Grey is PatternFly's default label color and adds no color modifier:
  // a neutral label is one without any of the status colors.
  await expect(
    row.locator(".pf-v6-c-label", { hasText: "retired · removed" }),
  ).not.toHaveClass(/pf-m-(green|red|blue|orange|purple|teal|yellow)/);

  await row.getByRole("button").click();

  const applied = page.getByTestId("delivery-applied");
  await expect(
    applied.locator(".pf-v6-c-label", { hasText: "delivered" }),
  ).toHaveClass(/pf-m-green/);

  const broken = page.getByTestId("delivery-broken");
  await expect(
    broken.locator(".pf-v6-c-label", { hasText: "failed" }),
  ).toHaveClass(/pf-m-red/);
  await expect(broken).toContainText("3 failed attempts");
  await expect(broken).toContainText("apply manifest 1: conflict");

  const retired = page.getByTestId("delivery-retired");
  await expect(
    retired.locator(".pf-v6-c-label", { hasText: "removed" }),
  ).not.toHaveClass(/pf-m-(green|red|blue|orange|purple|teal|yellow)/);

  // The completed removal is not active work, so it does not count.
  await expect(
    page
      .locator("dt", { hasText: "Active Deployments" })
      .locator("xpath=following-sibling::dd"),
  ).toHaveText("2");
});

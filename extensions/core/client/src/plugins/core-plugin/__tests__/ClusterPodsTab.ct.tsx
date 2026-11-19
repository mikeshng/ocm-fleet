import { expect, test } from "@playwright/experimental-ct-react";
import { ScalprumContext } from "@scalprum/react-core";
import { MemoryRouter } from "react-router-dom";

import ClusterPodsTab from "../ClusterPodsTab";

const scalprum = {
  initialized: true,
  config: {},
  api: { fleet: { getPluginPagePath: () => "/plugins/pods" } },
  pluginStore: {},
} as never;

function pod(name: string, namespace: string) {
  return {
    name: `//kubernetes.open-cluster-management.io/clusters/demo/namespaces/${namespace}/apiResources/v1/objects/${name}`,
    resourceType: "kubernetes.open-cluster-management.io/Object",
    labels: {},
    resource: {
      observation: {
        kind: "Pod",
        metadata: {
          uid: `uid-${name}`,
          name,
          namespace,
          creationTimestamp: "2026-11-15T00:00:00Z",
        },
        extracted: { status: "Running", nodeName: "node-1" },
      },
    },
  };
}

function mountTab() {
  return (
    <ScalprumContext.Provider value={scalprum}>
      <MemoryRouter>
        <ClusterPodsTab clusterId="demo" />
      </MemoryRouter>
    </ScalprumContext.Provider>
  );
}

test("blames the filters when a filter excludes every indexed pod", async ({
  mount,
  page,
}) => {
  await page.route(/queryResources/, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        resources: [pod("web-1", "default"), pod("web-2", "api")],
        nextPageToken: "",
      }),
    }),
  );

  await mount(mountTab());

  await expect(page.getByText("web-1")).toBeVisible();
  await page.getByPlaceholder("Filter by name").fill("no-such-pod");

  await expect(
    page.getByText("No pods match the current filter criteria."),
  ).toBeVisible();
  await page
    .getByLabel("Pods table")
    .getByRole("button", { name: "Clear filters" })
    .click();

  await expect(page.getByText("web-1")).toBeVisible();
});

test("says nothing is indexed when the cluster has no pods", async ({
  mount,
  page,
}) => {
  await page.route(/queryResources/, (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ resources: [], nextPageToken: "" }),
    }),
  );

  await mount(mountTab());

  await expect(
    page.getByText("No pods are currently indexed for this cluster."),
  ).toBeVisible();
  await expect(
    page
      .getByLabel("Pods table")
      .getByRole("button", { name: "Clear filters" }),
  ).toHaveCount(0);
});

import { expect, test } from "@playwright/experimental-ct-react";

import EnrollManifestModal from "../EnrollManifestModal";

const SERVICE = "kubernetes.open-cluster-management.io";

test("shows the manifest once and revokes the bundle", async ({
  mount,
  page,
}) => {
  const posts: unknown[] = [];
  const deletes: string[] = [];
  await page.route("**/apis/**", async (route) => {
    const request = route.request();
    if (request.method() === "DELETE") {
      deletes.push(new URL(request.url()).pathname);
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({}),
      });
      return;
    }
    posts.push(request.postDataJSON());
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        name: "enrollmentBundles/eb-1",
        bundle: { id: "eb-1", profile: "workloads" },
        manifest: "apiVersion: v1\nkind: List\nitems: []\n",
        instructions: [
          "Apply the manifest to the cluster as its administrator: kubectl apply -f <file>",
        ],
        expiresAt: "2026-11-16T09:30:00Z",
      }),
    });
  });

  await mount(<EnrollManifestModal isOpen onClose={() => {}} />);

  await page.getByRole("textbox", { name: "Cluster ID" }).fill(" hub1 ");
  await page
    .getByRole("textbox", { name: "API server" })
    .fill("https://hub1.example.com");
  await page.getByRole("button", { name: "Generate manifest" }).click();

  await expect(
    page.getByText("Shown once: the manifest carries an enrollment secret"),
  ).toBeVisible();
  await expect(
    page.getByRole("textbox", { name: "Enrollment manifest text" }),
  ).toHaveValue(/apiVersion: v1 kind: List/);
  await expect(
    page.getByText("Apply the manifest to the cluster as its administrator"),
  ).toBeVisible();

  expect(posts).toEqual([
    {
      clusterId: "hub1",
      apiServer: "https://hub1.example.com",
      profile: "workloads",
    },
  ]);

  await page.getByRole("button", { name: "Revoke bundle" }).click();

  // The bundle's manifest is the only copy of its secret; revoking it
  // asks once more, naming the bundle.
  await expect(page.getByText('Revoke bundle "eb-1"?')).toBeVisible();
  await page.getByRole("button", { name: "Revoke bundle" }).last().click();

  await expect(page.getByText("Bundle revoked")).toBeVisible();
  expect(deletes).toEqual([`/apis/${SERVICE}/v1alpha/enrollmentBundles/eb-1`]);
});

test("stays open while the manifest is being generated", async ({
  mount,
  page,
}) => {
  let release: () => void = () => {};
  const answered = new Promise<void>((resolve) => {
    release = resolve;
  });
  await page.route("**/apis/**", async (route) => {
    await answered;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        name: "enrollmentBundles/eb-1",
        bundle: { id: "eb-1", profile: "workloads" },
        manifest: "apiVersion: v1\nkind: List\nitems: []\n",
        instructions: [
          "Apply the manifest to the cluster as its administrator: kubectl apply -f <file>",
        ],
        expiresAt: "2026-11-16T09:30:00Z",
      }),
    });
  });

  const closes: string[] = [];
  await mount(
    <EnrollManifestModal isOpen onClose={() => closes.push("closed")} />,
  );
  await page.getByRole("textbox", { name: "Cluster ID" }).fill("hub1");
  await page
    .getByRole("textbox", { name: "API server" })
    .fill("https://hub1.example.com");
  await page.getByRole("button", { name: "Generate manifest" }).click();

  // The request is in flight: neither Escape, the modal's own close button,
  // nor the footer Cancel may discard the one-time manifest.
  await expect(page.getByRole("button", { name: "Cancel" })).toBeDisabled();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("button", { name: "Close" })).toHaveCount(0);
  expect(closes).toEqual([]);

  release();
  await expect(
    page.getByText("Shown once: the manifest carries an enrollment secret"),
  ).toBeVisible();
  expect(closes).toEqual([]);
});

test("a revocation in flight cannot be dismissed before its answer lands", async ({
  mount,
  page,
}) => {
  let releaseDelete: () => void = () => {};
  const heldDelete = new Promise<void>((resolve) => {
    releaseDelete = resolve;
  });
  const deletes: string[] = [];
  await page.route("**/apis/**", async (route) => {
    const request = route.request();
    if (request.method() === "DELETE") {
      deletes.push(new URL(request.url()).pathname);
      await heldDelete;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({}),
      });
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        name: "enrollmentBundles/eb-1",
        bundle: { id: "eb-1", profile: "workloads" },
        manifest: "apiVersion: v1\nkind: List\nitems: []\n",
        instructions: [
          "Apply the manifest to the cluster as its administrator: kubectl apply -f <file>",
        ],
        expiresAt: "2026-11-16T09:30:00Z",
      }),
    });
  });

  const closes: string[] = [];
  await mount(
    <EnrollManifestModal isOpen onClose={() => closes.push("closed")} />,
  );
  await page.getByRole("textbox", { name: "Cluster ID" }).fill("hub1");
  await page
    .getByRole("textbox", { name: "API server" })
    .fill("https://hub1.example.com");
  await page.getByRole("button", { name: "Generate manifest" }).click();
  await expect(
    page.getByText("Shown once: the manifest carries an enrollment secret"),
  ).toBeVisible();

  await page.getByRole("button", { name: "Revoke bundle" }).click();
  await expect(page.getByText('Revoke bundle "eb-1"?')).toBeVisible();
  await page.getByRole("button", { name: "Revoke bundle" }).last().click();

  // The DELETE is out: the confirmation's Cancel is disabled and Escape
  // must not dismiss the one-time manifest's only revoke path.
  await expect(
    page.getByRole("button", { name: "Cancel" }).last(),
  ).toBeDisabled();
  await page.keyboard.press("Escape");
  await expect(page.getByText('Revoke bundle "eb-1"?')).toBeVisible();
  await expect(page.getByRole("button", { name: "Close" })).toHaveCount(0);
  expect(closes).toEqual([]);
  await expect.poll(() => deletes.length).toBe(1);
  expect(deletes).toEqual([`/apis/${SERVICE}/v1alpha/enrollmentBundles/eb-1`]);

  releaseDelete();
  await expect(page.getByText("Bundle revoked")).toBeVisible();
  expect(closes).toEqual([]);
});

test("shows the server's sentence for a bundle that was not made", async ({
  mount,
  page,
}) => {
  await page.route("**/apis/**", (route) =>
    route.fulfill({
      status: 400,
      contentType: "application/json",
      body: JSON.stringify({
        error:
          "invalid argument: cluster id \"Hub1\" must consist of lowercase letters, digits, '-' and '.', and start and end with a letter or digit",
        reason: "invalid-argument",
      }),
    }),
  );

  await mount(<EnrollManifestModal isOpen onClose={() => {}} />);

  await page.getByRole("textbox", { name: "Cluster ID" }).fill("Hub1");
  await page
    .getByRole("textbox", { name: "API server" })
    .fill("https://hub1.example.com");
  await page.getByRole("button", { name: "Generate manifest" }).click();

  await expect(
    page.getByText(/must consist of lowercase letters/),
  ).toBeVisible();
  await expect(page.getByText("Bundle revoked")).toHaveCount(0);
});

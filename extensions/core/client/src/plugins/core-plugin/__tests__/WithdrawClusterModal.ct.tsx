import { expect, test } from "@playwright/experimental-ct-react";

import WithdrawClusterModal from "../WithdrawClusterModal";

const SERVICE = "kubernetes.open-cluster-management.io";

test("names the deployments that block withdrawal and retries leaving them", async ({
  mount,
  page,
}) => {
  const bodies: unknown[] = [];
  const withdrawn: string[] = [];
  await page.route("**/apis/**", async (route) => {
    bodies.push(route.request().postDataJSON());
    if (bodies.length === 1) {
      await route.fulfill({
        status: 409,
        contentType: "application/json",
        body: JSON.stringify({
          error:
            '2 deployment(s) still resolve to target "targets/x"; delete them first, or withdraw with leaveDeployments to leave on the cluster what they delivered',
          reason: "deployments-resolve-to-target",
          details: {
            deployments: ["deployments/a", "deployments/b"],
            target: "targets/x",
          },
        }),
      });
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ cluster: { id: "hub1", name: "clusters/hub1" } }),
    });
  });

  await mount(
    <WithdrawClusterModal
      isOpen
      clusterId="hub1"
      service={SERVICE}
      onClose={() => {}}
      onWithdrawn={() => withdrawn.push("withdrawn")}
    />,
  );

  await page
    .getByRole("textbox", { name: "Reason for withdrawing the cluster" })
    .fill("retired");
  await page.getByRole("button", { name: "Withdraw", exact: true }).click();

  await expect(
    page.getByText("Deployments still resolve to this cluster"),
  ).toBeVisible();
  await expect(page.getByText("deployments/a")).toBeVisible();
  await expect(page.getByText("deployments/b")).toBeVisible();
  await expect(page.getByText("targets/x")).toBeVisible();

  await page.getByRole("button", { name: "Withdraw and leave them" }).click();

  await expect.poll(() => bodies.length).toBe(2);
  expect(bodies[0]).toEqual({ reason: "retired" });
  expect(bodies[1]).toEqual({ reason: "retired", leaveDeployments: true });
  // The request landing is not the outcome: the success path must also report
  // the withdrawal, which is what closes the modal and refetches the record.
  await expect.poll(() => withdrawn.length).toBe(1);
});

test("a withdrawal in flight cannot be dismissed before its answer lands", async ({
  mount,
  page,
}) => {
  let release: () => void = () => {};
  const held = new Promise<void>((resolve) => {
    release = resolve;
  });
  await page.route("**/apis/**", async (route) => {
    await held;
    await route.fulfill({
      status: 409,
      contentType: "application/json",
      body: JSON.stringify({
        error: '1 deployment(s) still resolve to target "targets/x"',
        reason: "deployments-resolve-to-target",
        details: { deployments: ["deployments/a"], target: "targets/x" },
      }),
    });
  });

  const closes: string[] = [];
  await mount(
    <WithdrawClusterModal
      isOpen
      clusterId="hub1"
      service={SERVICE}
      onClose={() => closes.push("closed")}
      onWithdrawn={() => {}}
    />,
  );

  await page.getByRole("button", { name: "Withdraw", exact: true }).click();
  // The footer Cancel guard reports the in-flight state: while it is disabled
  // the request is out, so a dismissal control that still worked would have
  // called onClose by now.
  await expect(page.getByRole("button", { name: "Cancel" })).toBeDisabled();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("button", { name: "Close" })).toHaveCount(0);
  expect(closes).toEqual([]);
  await expect(
    page.getByText("Withdraw cluster", { exact: true }),
  ).toBeVisible();
  release();

  await expect(
    page.getByText("Deployments still resolve to this cluster"),
  ).toBeVisible();
  await expect(page.getByText("deployments/a")).toBeVisible();
});

test("shows the server's sentence for a withdrawal that failed otherwise", async ({
  mount,
  page,
}) => {
  await page.route("**/apis/**", (route) =>
    route.fulfill({
      status: 404,
      contentType: "application/json",
      body: JSON.stringify({
        error: 'cluster "hub1" is not enrolled',
        reason: "not-found",
      }),
    }),
  );

  await mount(
    <WithdrawClusterModal
      isOpen
      clusterId="hub1"
      service={SERVICE}
      onClose={() => {}}
      onWithdrawn={() => {}}
    />,
  );

  await page.getByRole("button", { name: "Withdraw", exact: true }).click();

  await expect(page.getByText('cluster "hub1" is not enrolled')).toBeVisible();
  await expect(
    page.getByRole("button", { name: "Withdraw and leave them" }),
  ).toHaveCount(0);
});

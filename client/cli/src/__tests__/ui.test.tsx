import { cleanup, render } from "ink-testing-library";
import { afterEach, describe, expect, it } from "vitest";

import {
  deliverySummary,
  DeploymentListView,
} from "../commands/deployment/list";
import { Output } from "../ui";

afterEach(cleanup);

describe("Ink output", () => {
  it("renders command output in terminal frame", () => {
    const instance = render(<Output data="fleetctl ready" />);
    expect(instance.lastFrame()).toContain("fleetctl ready");
  });

  it("renders deployment data with human defaults and a JSON hint", () => {
    const instance = render(
      <DeploymentListView data={[{ name: "example" }]} />,
    );

    expect(instance.lastFrame()).toContain("example");
    expect(instance.lastFrame()).toContain("UNKNOWN");
    expect(instance.lastFrame()).toContain("NONE");
    expect(instance.lastFrame()).toContain("Pause reason");
    expect(instance.lastFrame()).toContain(
      "Use --output json or yaml for full details.",
    );
  });

  it("renders the empty deployment list", () => {
    const instance = render(<DeploymentListView data={[]} />);

    expect(instance.lastFrame()).toContain(
      "Use --output json or yaml for full details.",
    );
  });

  it("shows delivery failures with their reason", () => {
    const instance = render(
      <DeploymentListView
        data={[
          {
            name: "clash",
            state: "STATE_CREATING",
            targetStatuses: [
              {
                targetId: "k8s-hub1.ext",
                state: "STATE_FAILED",
                attempts: 2,
                message: 'apply manifest 1: conflict with "work-agent"',
              },
              { targetId: "k8s-hub2.ext", state: "STATE_DELIVERED" },
            ],
          },
        ]}
      />,
    );
    const frame = instance.lastFrame() ?? "";

    expect(frame).toContain("1/2 delivered");
    expect(frame).toContain("Deliveries needing attention");
    // Assert the attention row carries the target, state, attempts, and
    // reason together; a bare "2" elsewhere in the frame would prove nothing.
    const attention = frame
      .split("\n")
      .find((line) => line.includes("k8s-hub1.ext"));
    expect(attention).toBeDefined();
    expect(attention).toContain("failed");
    expect(attention).toContain("2");
    expect(attention).toContain('conflict with "work-agent"');
  });

  it("reads an auth failure as words, not the proto constant", () => {
    const instance = render(
      <DeploymentListView
        data={[
          {
            name: "locked",
            targetStatuses: [
              {
                targetId: "k8s-hub1.ext",
                state: "STATE_AUTH_FAILED",
                attempts: 1,
                message: "401 Unauthorized",
              },
            ],
          },
        ]}
      />,
    );
    const frame = instance.lastFrame() ?? "";

    expect(frame).toContain("Deliveries needing attention");
    const attention = frame
      .split("\n")
      .find((line) => line.includes("k8s-hub1.ext"));
    expect(attention).toBeDefined();
    expect(attention).toContain("auth failed");
    expect(attention).toContain("401 Unauthorized");
  });

  it("keeps a retained reason visible while a retry is pending", () => {
    const instance = render(
      <DeploymentListView
        data={[
          {
            name: "clash",
            targetStatuses: [
              {
                targetId: "k8s-hub1.ext",
                state: "STATE_PENDING",
                attempts: 3,
                message: "apply manifest 1: conflict",
              },
            ],
          },
        ]}
      />,
    );

    expect(instance.lastFrame()).toContain("Deliveries needing attention");
    expect(instance.lastFrame()).toContain("apply manifest 1: conflict");
  });

  it("omits the attention table when every target is delivered", () => {
    const instance = render(
      <DeploymentListView
        data={[
          {
            name: "clean",
            targetStatuses: [
              { targetId: "k8s-hub1.ext", state: "STATE_DELIVERED" },
            ],
          },
        ]}
      />,
    );

    expect(instance.lastFrame()).toContain("1/1 delivered");
    expect(instance.lastFrame()).not.toContain("Deliveries needing attention");
  });

  it("reports completed removals as removed, not delivered", () => {
    const instance = render(
      <DeploymentListView
        data={[
          {
            name: "retired",
            state: "STATE_DELETING",
            targetStatuses: [
              {
                targetId: "k8s-hub1.ext",
                state: "STATE_DELIVERED",
                operation: "OPERATION_REMOVE",
              },
              {
                // The id carries no digit so the "2" assertion below can only
                // be satisfied by the rendered attempt count.
                targetId: "k8s-hub-b.ext",
                state: "STATE_FAILED",
                operation: "OPERATION_REMOVE",
                attempts: 2,
                message: "remove: forbidden",
              },
            ],
          },
        ]}
      />,
    );
    const frame = instance.lastFrame() ?? "";

    expect(frame).not.toContain("delivered");
    expect(frame).toContain("Deliveries needing attention");
    // The attention row names the removal state, so "remove" is not
    // satisfied by the "1/2 removed" summary alone.
    const attention = frame
      .split("\n")
      .find((line) => line.includes("k8s-hub-b.ext"));
    expect(attention).toBeDefined();
    expect(attention).toContain("remove failed");
    expect(attention).toContain("2");
    expect(attention).toContain("forbidden");
  });

  it("summarizes delivery and removal work side by side", () => {
    const instance = render(
      <DeploymentListView
        data={[
          {
            name: "shrinking",
            targetStatuses: [
              {
                targetId: "k8s-hub1.ext",
                state: "STATE_DELIVERED",
                operation: "OPERATION_DELIVER",
              },
              {
                targetId: "k8s-hub2.ext",
                state: "STATE_DELIVERED",
                operation: "OPERATION_REMOVE",
              },
            ],
          },
        ]}
      />,
    );

    expect(instance.lastFrame()).toContain("delivered");
    expect(instance.lastFrame()).toContain("removed");
  });
});

describe("deliverySummary", () => {
  it("counts delivery and removal work separately", () => {
    expect(
      deliverySummary([
        {
          targetId: "t1",
          state: "STATE_DELIVERED",
          operation: "OPERATION_DELIVER",
        },
        {
          targetId: "t2",
          state: "STATE_DELIVERED",
          operation: "OPERATION_REMOVE",
        },
      ]),
    ).toBe("1/1 delivered, 1/1 removed");
  });

  it("treats a status without an operation as a delivery", () => {
    expect(
      deliverySummary([{ targetId: "t1", state: "STATE_DELIVERED" }]),
    ).toBe("1/1 delivered");
  });

  it("does not count a completed removal as delivered", () => {
    expect(
      deliverySummary([
        {
          targetId: "t1",
          state: "STATE_DELIVERED",
          operation: "OPERATION_REMOVE",
        },
      ]),
    ).toBe("1/1 removed");
  });
});

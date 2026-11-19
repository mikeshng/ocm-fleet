import { describe, expect, it } from "vitest";

import type { MgmtDeployment, TargetDeliveryStatus } from "../api";
import {
  deliveryColor,
  deliveryStateLabel,
  deploymentsByTarget,
  isRemoval,
  isRemovalCompleted,
} from "../targetDeliveries";

function deployment(
  name: string,
  overrides: Partial<MgmtDeployment> = {},
): MgmtDeployment {
  return {
    name,
    uid: name,
    manifestStrategy: { type: "TYPE_INLINE" },
    placementStrategy: { type: "TYPE_STATIC" },
    resolvedTargetIds: [],
    state: "STATE_ACTIVE",
    reconciling: false,
    createTime: "2026-01-01T00:00:00Z",
    updateTime: "2026-01-01T00:00:00Z",
    etag: "etag",
    ...overrides,
  };
}

const failed: TargetDeliveryStatus = {
  targetId: "clusters/b",
  state: "STATE_FAILED",
  attempts: 2,
  message: 'apply manifest 1: conflict with "work-agent"',
};

describe("deploymentsByTarget", () => {
  it("groups by placement and orders the deployment names", () => {
    const grouped = deploymentsByTarget([
      deployment("deployments/zeta", {
        resolvedTargetIds: ["clusters/b", "clusters/a"],
      }),
      deployment("deployments/alpha", {
        resolvedTargetIds: ["clusters/b"],
      }),
    ]);

    expect(grouped.get("clusters/a")?.map((d) => d.name)).toEqual([
      "deployments/zeta",
    ]);
    expect(grouped.get("clusters/b")?.map((d) => d.name)).toEqual([
      "deployments/alpha",
      "deployments/zeta",
    ]);
  });

  it("carries the delivery status of a deployment on its target", () => {
    const grouped = deploymentsByTarget([
      deployment("deployments/zeta", {
        resolvedTargetIds: ["clusters/b"],
        targetStatuses: [failed],
      }),
    ]);

    expect(grouped.get("clusters/b")).toEqual([
      { name: "deployments/zeta", status: failed },
    ]);
  });

  it("shows a status even when placement no longer names its target", () => {
    const grouped = deploymentsByTarget([
      deployment("deployments/zeta", { targetStatuses: [failed] }),
    ]);

    expect(grouped.get("clusters/b")?.map((d) => d.name)).toEqual([
      "deployments/zeta",
    ]);
  });

  it("reports placement without a delivery record as having no status", () => {
    const grouped = deploymentsByTarget([
      deployment("deployments/zeta", { resolvedTargetIds: ["clusters/b"] }),
    ]);

    expect(grouped.get("clusters/b")).toEqual([{ name: "deployments/zeta" }]);
  });
});

describe("deliveryColor", () => {
  it("maps delivery states to label colors", () => {
    expect(deliveryColor("STATE_DELIVERED")).toBe("green");
    expect(deliveryColor("STATE_FAILED")).toBe("red");
    expect(deliveryColor("STATE_PARTIAL")).toBe("red");
    expect(deliveryColor("STATE_AUTH_FAILED")).toBe("red");
    expect(deliveryColor("STATE_ACCEPTED")).toBe("blue");
    expect(deliveryColor("STATE_PROGRESSING")).toBe("blue");
    expect(deliveryColor("STATE_PENDING")).toBe("grey");
    expect(deliveryColor()).toBe("grey");
  });

  it("does not color a completed removal as a live delivery", () => {
    expect(deliveryColor("STATE_DELIVERED", "OPERATION_REMOVE")).toBe("grey");
    expect(deliveryColor("STATE_DELIVERED", "OPERATION_DELIVER")).toBe("green");
  });
});

describe("deliveryStateLabel", () => {
  it("names a missing or default state as pending", () => {
    expect(deliveryStateLabel()).toBe("pending");
    expect(deliveryStateLabel("STATE_UNSPECIFIED")).toBe("pending");
  });

  it("reads states as words", () => {
    expect(deliveryStateLabel("STATE_AUTH_FAILED")).toBe("auth failed");
    expect(deliveryStateLabel("STATE_DELIVERED")).toBe("delivered");
  });

  it("words removal work as removal, not delivery", () => {
    expect(deliveryStateLabel("STATE_DELIVERED", "OPERATION_REMOVE")).toBe(
      "removed",
    );
    expect(deliveryStateLabel("STATE_FAILED", "OPERATION_REMOVE")).toBe(
      "removal failed",
    );
    expect(deliveryStateLabel("STATE_PROGRESSING", "OPERATION_REMOVE")).toBe(
      "removal progressing",
    );
    expect(deliveryStateLabel(undefined, "OPERATION_REMOVE")).toBe(
      "removal pending",
    );
  });
});

describe("removal helpers", () => {
  const deliveredRemoval: TargetDeliveryStatus = {
    targetId: "clusters/b",
    state: "STATE_DELIVERED",
    operation: "OPERATION_REMOVE",
  };

  it("treats only an explicit remove operation as a removal", () => {
    expect(isRemoval(deliveredRemoval)).toBe(true);
    expect(isRemoval(failed)).toBe(false);
    expect(isRemoval()).toBe(false);
  });

  it("treats only a delivered removal as completed", () => {
    expect(isRemovalCompleted(deliveredRemoval)).toBe(true);
    expect(
      isRemovalCompleted({
        targetId: "clusters/b",
        state: "STATE_FAILED",
        operation: "OPERATION_REMOVE",
      }),
    ).toBe(false);
    expect(isRemovalCompleted(failed)).toBe(false);
  });
});

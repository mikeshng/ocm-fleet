import type { InventoryResource } from "@ocm-fleet/common";
import { describe, expect, it } from "vitest";

import { resolveObservedCluster } from "../ObservedClusterSearchResult";

function record(
  hub: string,
  name: string,
  enrollment: Record<string, string> = { state: "observed" },
): InventoryResource {
  return {
    name: `//ocm.open-cluster-management.io/hubs/${hub}/managedClusters/${name}`,
    resourceType: "ocm.open-cluster-management.io/ManagedCluster",
    resource: {
      name: `hubs/${hub}/managedClusters/${name}`,
      observation: {
        hub,
        name,
        accepted: true,
        available: "True",
        enrollment,
      },
    },
  } as unknown as InventoryResource;
}

describe("resolveObservedCluster", () => {
  it("tells two hubs' clusters of one name apart by their titles", () => {
    const one = resolveObservedCluster(record("hub1", "local-cluster"));
    const two = resolveObservedCluster(record("hub2", "local-cluster"));

    expect(one.title).toBe("hub1.local-cluster");
    expect(two.title).toBe("hub2.local-cluster");
    expect(one.to).toBe("hub1.local-cluster");
    expect(two.to).toBe("hub2.local-cluster");
  });

  it("says where an observed cluster stands", () => {
    expect(resolveObservedCluster(record("hub1", "ext2")).description).toBe(
      "Hub hub1 · observed · Available",
    );
  });

  it("does not repeat the id of a cluster enrolled through its hub", () => {
    const result = resolveObservedCluster(
      record("hub1", "ext", {
        state: "enrolled",
        cluster: "clusters/hub1.ext",
      }),
    );

    expect(result.title).toBe("hub1.ext");
    expect(result.to).toBe("hub1.ext");
    expect(result.description).toBe("Hub hub1 · enrolled · Available");
  });

  it("names the cluster a hub's record leads to when its id differs", () => {
    const result = resolveObservedCluster(
      record("hub2", "site-b-7", {
        state: "enrolled",
        cluster: "clusters/byhand",
      }),
    );

    expect(result.title).toBe("hub2.site-b-7");
    expect(result.to).toBe("byhand");
    expect(result.description).toBe(
      "Hub hub2 · enrolled as byhand · Available",
    );
  });
});

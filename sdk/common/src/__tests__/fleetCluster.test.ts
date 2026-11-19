import { describe, expect, it } from "vitest";

import {
  type ClusterRecord,
  clusterRecordFilter,
  ENROLLED_CLUSTER_TYPE,
  enrolledClusterStatus,
  fleetClusterTypeFilter,
  foldLinkedClusterResults,
  hubReportedProblem,
  KIND_CLUSTER_TYPE,
  mergeFleetClusters,
  OBSERVED_CLUSTER_TYPE,
  observedClusterRouteId,
  observedClusterStatus,
  observedRecordFilter,
} from "../objects/fleetCluster.js";

function created(id: string): ClusterRecord {
  return {
    resourceType: KIND_CLUSTER_TYPE,
    resource: { name: `clusters/${id}` },
  };
}

function enrolled(
  id: string,
  state = "enrolled",
  ready = "True",
): ClusterRecord {
  return {
    resourceType: ENROLLED_CLUSTER_TYPE,
    resource: {
      name: `clusters/${id}`,
      observation: { state },
      conditions: { Ready: { status: ready } },
    },
  };
}

function observed(
  hub: string,
  name: string,
  enrollment: Record<string, string> = { state: "observed" },
  extra: Record<string, unknown> = {},
): ClusterRecord {
  return {
    resourceType: OBSERVED_CLUSTER_TYPE,
    resource: {
      name: `hubs/${hub}/managedClusters/${name}`,
      observation: {
        hub,
        name,
        accepted: true,
        available: "True",
        enrollment,
        ...extra,
      },
    },
  };
}

describe("fleetClusterTypeFilter", () => {
  it("matches the records of every origin", () => {
    const filter = fleetClusterTypeFilter();
    expect(filter).toContain(`resourceType == "${KIND_CLUSTER_TYPE}"`);
    expect(filter).toContain(`resourceType == "${ENROLLED_CLUSTER_TYPE}"`);
    expect(filter).toContain(`resourceType == "${OBSERVED_CLUSTER_TYPE}"`);
  });
});

describe("mergeFleetClusters", () => {
  it("shows a created cluster once, with the hub that reports it", () => {
    const rows = mergeFleetClusters([
      observed("hub1", "spoke", {
        state: "created",
        cluster: "clusters/spoke",
      }),
      created("spoke"),
    ]);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      id: "spoke",
      origin: "created",
      hub: "hub1",
      indexed: true,
    });
    expect(rows[0].observed?.resourceType).toBe(OBSERVED_CLUSTER_TYPE);
  });

  it("links a created cluster whose name on the hub differs", () => {
    const rows = mergeFleetClusters([
      created("hub"),
      observed("hub1", "local-cluster", {
        state: "created",
        cluster: "clusters/hub",
      }),
    ]);
    expect(rows.map((r) => r.id)).toEqual(["hub"]);
    expect(rows[0].hub).toBe("hub1");
  });

  it("shows an enrolled cluster once, with the hub that reports it", () => {
    const rows = mergeFleetClusters([
      enrolled("hub1.ext"),
      observed("hub1", "ext", {
        state: "enrolled",
        cluster: "clusters/hub1.ext",
      }),
    ]);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      id: "hub1.ext",
      origin: "enrolled",
      hub: "hub1",
      indexed: true,
    });
  });

  it("gives a cluster that only a hub knows a row of its own", () => {
    const rows = mergeFleetClusters([observed("hub1", "ext2")]);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      id: "hub1.ext2",
      origin: "observed",
      hub: "hub1",
      indexed: false,
    });
    expect(rows[0].result).toBe(rows[0].observed);
  });

  it("folds a hub record into a withdrawn cluster of the same id", () => {
    const rows = mergeFleetClusters([
      observed("hub1", "ext", { state: "withdrawn" }),
      enrolled("hub1.ext", "withdrawn"),
    ]);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      id: "hub1.ext",
      origin: "enrolled",
      indexed: false,
    });
  });

  it("keeps an enrolled cluster that no hub reports", () => {
    const rows = mergeFleetClusters([enrolled("by-hand")]);
    expect(rows[0]).toMatchObject({ id: "by-hand", origin: "enrolled" });
    expect(rows[0].hub).toBeUndefined();
    expect(rows[0].observed).toBeUndefined();
  });

  it("prefers the created record when both kinds share an id", () => {
    const rows = mergeFleetClusters([enrolled("spoke"), created("spoke")]);
    expect(rows).toHaveLength(1);
    expect(rows[0].origin).toBe("created");
  });

  it("orders rows by id", () => {
    const rows = mergeFleetClusters([
      observed("hub1", "ext2"),
      created("spoke"),
      enrolled("alpha"),
    ]);
    expect(rows.map((r) => r.id)).toEqual(["alpha", "hub1.ext2", "spoke"]);
  });

  it("keeps two hubs' clusters of one name apart", () => {
    const rows = mergeFleetClusters([
      observed("hub2", "local-cluster"),
      observed("hub1", "local-cluster"),
    ]);
    expect(rows.map((r) => [r.id, r.hub])).toEqual([
      ["hub1.local-cluster", "hub1"],
      ["hub2.local-cluster", "hub2"],
    ]);
  });

  describe("a cluster that two hubs report", () => {
    const link = { state: "enrolled", cluster: "clusters/byhand" };
    const fromHub1 = observed("hub1", "edge-7", link);
    const fromHub2 = observed("hub2", "site-b-7", link);
    function byHand(homeHub?: string): ClusterRecord {
      const record = enrolled("byhand");
      if (homeHub) {
        record.resource.localLabels = {
          "ocm.open-cluster-management.io/hub": homeHub,
        };
      }
      return record;
    }

    it("appears once", () => {
      const rows = mergeFleetClusters([byHand("hub1"), fromHub1, fromHub2]);
      expect(rows).toHaveLength(1);
      expect(rows[0].id).toBe("byhand");
    });

    it("shows the hub whose facts it carries, whatever the order", () => {
      for (const records of [
        [byHand("hub2"), fromHub1, fromHub2],
        [byHand("hub2"), fromHub2, fromHub1],
        [fromHub1, fromHub2, byHand("hub2")],
      ]) {
        const [row] = mergeFleetClusters(records);
        expect(row.hub).toBe("hub2");
        expect(row.observed).toBe(fromHub2);
        expect(row.hubs).toEqual(["hub2", "hub1"]);
      }
    });

    it("shows the first hub by id when it carries no hub's facts", () => {
      for (const records of [
        [byHand(), fromHub2, fromHub1],
        [byHand(), fromHub1, fromHub2],
      ]) {
        const [row] = mergeFleetClusters(records);
        expect(row.hub).toBe("hub1");
        expect(row.observed).toBe(fromHub1);
        expect(row.hubs).toEqual(["hub1", "hub2"]);
      }
    });

    it("names a hub once when it has two records of the cluster", () => {
      const [row] = mergeFleetClusters([
        byHand("hub1"),
        fromHub1,
        observed("hub1", "edge-7-again", link),
      ]);
      expect(row.hubs).toEqual(["hub1"]);
    });
  });
});

describe("observedClusterRouteId", () => {
  it("leads to the linked cluster when there is one", () => {
    expect(
      observedClusterRouteId(
        observed("hub1", "local-cluster", {
          state: "created",
          cluster: "clusters/hub",
        }),
      ),
    ).toBe("hub");
  });

  it("leads to the hub's own view otherwise", () => {
    expect(observedClusterRouteId(observed("hub1", "ext2"))).toBe("hub1.ext2");
  });
});

describe("foldLinkedClusterResults", () => {
  it("drops the cluster record that a hub record stands for", () => {
    const hubRecord = observed("hub1", "ext", {
      state: "enrolled",
      cluster: "clusters/hub1.ext",
    });
    const other = enrolled("by-hand");
    expect(
      foldLinkedClusterResults([enrolled("hub1.ext"), hubRecord, other]),
    ).toEqual([hubRecord, other]);
  });

  it("returns the list unchanged when nothing is linked", () => {
    const results = [created("spoke"), observed("hub1", "ext2")];
    expect(foldLinkedClusterResults(results)).toBe(results);
  });
});

describe("lookup filters", () => {
  it("finds a cluster's own record by id", () => {
    expect(clusterRecordFilter("spoke")).toBe(
      `(resourceType == "${KIND_CLUSTER_TYPE}" || resourceType == "${ENROLLED_CLUSTER_TYPE}") && resource.name == "clusters/spoke"`,
    );
  });

  it("finds the hub record linked to an id without a hub part", () => {
    expect(observedRecordFilter("spoke")).toBe(
      `resourceType == "${OBSERVED_CLUSTER_TYPE}" && (resource.observation.enrollment.cluster == "clusters/spoke")`,
    );
  });

  it("also finds the hub record named by an id with a hub part", () => {
    expect(observedRecordFilter("hub1.ext.example")).toBe(
      `resourceType == "${OBSERVED_CLUSTER_TYPE}" && (resource.observation.enrollment.cluster == "clusters/hub1.ext.example" || resource.name == "hubs/hub1/managedClusters/ext.example")`,
    );
  });

  it("escapes what goes into a filter", () => {
    expect(clusterRecordFilter('a"b')).toContain('"clusters/a\\"b"');
    expect(observedRecordFilter('h.a"b')).toContain('managedClusters/a\\"b"');
  });

  it("escapes a backslash, and a backslash before a quote first", () => {
    expect(clusterRecordFilter(String.raw`a\b`)).toContain(
      String.raw`"clusters/a\\b"`,
    );
    expect(observedRecordFilter(String.raw`h.a\b`)).toContain(
      String.raw`managedClusters/a\\b"`,
    );
    expect(clusterRecordFilter(String.raw`a\"b`)).toContain(
      String.raw`"clusters/a\\\"b"`,
    );
  });
});

describe("enrolledClusterStatus", () => {
  it.each([
    ["enrolled", "True", "Ready", true, false],
    ["enrolled", "False", "Not ready", false, true],
    ["enrolled", "Unknown", "Unknown", false, false],
    ["needs-enrollment", "False", "Needs enrollment", false, true],
    ["withdrawn", "False", "Withdrawn", false, false],
  ])("%s with Ready=%s is %s", (state, ready, label, healthy, attention) => {
    expect(enrolledClusterStatus(enrolled("x", state, ready))).toMatchObject({
      text: label,
      healthy,
      attention,
    });
  });
});

describe("observedClusterStatus", () => {
  it("reports what the hub says", () => {
    expect(observedClusterStatus(observed("h", "c")).text).toBe("Available");
    expect(
      observedClusterStatus(
        observed("h", "c", undefined, { available: "False" }),
      ),
    ).toMatchObject({ text: "Unavailable", attention: true });
    expect(
      observedClusterStatus(
        observed("h", "c", undefined, { available: "Unknown" }),
      ).text,
    ).toBe("Unknown");
    expect(
      observedClusterStatus(observed("h", "c", undefined, { accepted: false }))
        .text,
    ).toBe("Not accepted");
  });

  it("says when the hub has not been heard from", () => {
    const record = observed("h", "c");
    record.resource.conditions = { Stale: { status: "True" } };
    expect(observedClusterStatus(record)).toMatchObject({
      text: "Stale",
      healthy: false,
      attention: true,
    });
  });

  it("asks for attention when the hub itself says Unknown", () => {
    const silent = observed("h", "c", undefined, { available: "Unknown" });
    expect(observedClusterStatus(silent)).toMatchObject({
      text: "Unknown",
      color: "grey",
      attention: false,
    });

    const lost = observed("h", "c", undefined, { available: "Unknown" });
    lost.resource.conditions = {
      ManagedClusterConditionAvailable: { status: "Unknown" },
    };
    expect(observedClusterStatus(lost)).toMatchObject({
      text: "Unknown",
      color: "orange",
      healthy: false,
      attention: true,
    });
  });
});

describe("hubReportedProblem", () => {
  const lost = () => {
    const record = observed(
      "hub1",
      "c",
      { state: "enrolled", cluster: "clusters/hub1.c" },
      { available: "Unknown" },
    );
    record.resource.conditions = {
      ManagedClusterConditionAvailable: { status: "Unknown" },
    };
    return record;
  };

  it("is silent while the hub reports the cluster available", () => {
    const [row] = mergeFleetClusters([
      enrolled("hub1.c"),
      observed("hub1", "c", { state: "enrolled", cluster: "clusters/hub1.c" }),
    ]);
    expect(hubReportedProblem(row)).toBeNull();
  });

  it("gives the hub's word for a cluster that is known here as well", () => {
    const [row] = mergeFleetClusters([enrolled("hub1.c"), lost()]);
    expect(row.origin).toBe("enrolled");
    expect(hubReportedProblem(row)).toMatchObject({
      text: "Unknown",
      attention: true,
    });
  });

  it("is silent for a row that is only the hub's record", () => {
    const [row] = mergeFleetClusters([lost()]);
    expect(row.origin).toBe("observed");
    expect(hubReportedProblem(row)).toBeNull();
  });

  it("is silent for a cluster no hub reports", () => {
    const [row] = mergeFleetClusters([enrolled("alone")]);
    expect(hubReportedProblem(row)).toBeNull();
  });
});

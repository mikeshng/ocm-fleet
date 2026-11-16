import {
  ClusterResource,
  extractClusterId,
  OBSERVED_CLUSTER_TYPE,
} from "@ocm-fleet/common";
import { describe, expect, it } from "vitest";

import {
  buildAddonBasePath,
  type ClusterRow,
  clusterRowStatus,
  deriveClusterState,
  extractService,
  hubColumn,
  hubFactsNotice,
  hubViewNotice,
  isFailureState,
  isHealthyState,
  isTransientState,
  matchesClusterFilters,
  serviceLabel,
  stateLabel,
} from "../clusterTypes";

function resource(overrides: Partial<ClusterResource> = {}): ClusterResource {
  return { name: "clusters/test", uid: "u1", ...overrides };
}

function row(id: string, hub?: string, hubs?: string[]): ClusterRow {
  return {
    id,
    origin: "enrolled",
    indexed: true,
    hub,
    hubs,
    result: {
      name: `//kubernetes.open-cluster-management.io/clusters/${id}`,
      resourceType: "kubernetes.open-cluster-management.io/Cluster",
      resource: resource({ name: `clusters/${id}` }),
    },
  } as ClusterRow;
}

describe("matchesClusterFilters", () => {
  const rows = [
    row("hub1.local-cluster", "hub1", ["hub1"]),
    row("hub2.local-cluster", "hub2", ["hub2"]),
    row("hub1.ext", "hub1", ["hub1"]),
    row("byhand", "hub1", ["hub1", "hub2"]),
    row("alone"),
  ];
  const ids = (filters: { name?: string; hub?: string }) =>
    rows.filter((r) => matchesClusterFilters(r, filters)).map((r) => r.id);

  it("passes every row when nothing is asked", () => {
    expect(ids({})).toHaveLength(rows.length);
    expect(ids({ name: "", hub: " " })).toHaveLength(rows.length);
  });

  it("narrows by a part of the id, whatever the case", () => {
    expect(ids({ name: "LOCAL" })).toEqual([
      "hub1.local-cluster",
      "hub2.local-cluster",
    ]);
  });

  it("narrows to one hub's clusters", () => {
    expect(ids({ hub: "hub2" })).toEqual(["hub2.local-cluster", "byhand"]);
  });

  it("finds a cluster two hubs report under each of them", () => {
    expect(ids({ hub: "hub1" })).toContain("byhand");
    expect(ids({ hub: "hub2" })).toContain("byhand");
  });

  it("leaves out clusters no hub reports when a hub is asked for", () => {
    expect(ids({ hub: "hub" })).not.toContain("alone");
  });

  it("applies both filters together", () => {
    expect(ids({ name: "local", hub: "hub2" })).toEqual(["hub2.local-cluster"]);
  });
});

describe("hubColumn", () => {
  it("shows the hub", () => {
    expect(hubColumn(row("hub1.ext", "hub1", ["hub1"]))).toBe("hub1");
  });

  it("says when other hubs report the cluster as well", () => {
    expect(hubColumn(row("byhand", "hub1", ["hub1", "hub2"]))).toBe(
      "hub1 (also hub2)",
    );
  });

  it("shows a dash for a cluster no hub reports", () => {
    expect(hubColumn(row("alone"))).toBe("\u2014");
  });
});

describe("hubFactsNotice", () => {
  it("is silent while the hub's labels are current", () => {
    expect(
      hubFactsNotice({
        HubFactsCurrent: { status: "True", reason: "Current" },
      }),
    ).toBeNull();
  });

  it("is silent for a cluster that carries no hub's labels", () => {
    expect(hubFactsNotice({ Ready: { status: "True" } })).toBeNull();
    expect(hubFactsNotice(undefined)).toBeNull();
  });

  it("says why the labels are old and that they still select the cluster", () => {
    const notice = hubFactsNotice({
      HubFactsCurrent: {
        status: "False",
        reason: "HubUnreachable",
        message:
          'hub "hub1" does not answer; the labels it supplied are the last ones seen',
      },
    });
    expect(notice?.title).toBe("The labels from its hub are not current");
    expect(notice?.body).toBe(
      'hub "hub1" does not answer; the labels it supplied are the last ones seen. Deployments still select this cluster by them.',
    );
  });

  it("still speaks when the record gives no message", () => {
    expect(hubFactsNotice({ HubFactsCurrent: { status: "False" } })?.body).toBe(
      "Deployments still select this cluster by them.",
    );
  });
});

/** An enrolled cluster of hub1, with the hub's record of it when given. */
function enrolledRow(
  ready: string,
  onHub?: { available: string; message?: string; stale?: boolean },
): ClusterRow {
  const enrolled = row("hub1.c", onHub && "hub1", onHub && ["hub1"]);
  enrolled.result.resource.observation = { state: "enrolled" };
  enrolled.result.resource.conditions = { Ready: { status: ready } };
  if (onHub) {
    enrolled.observed = {
      name: "//ocm.open-cluster-management.io/hubs/hub1/managedClusters/c",
      resourceType: OBSERVED_CLUSTER_TYPE,
      resource: resource({
        name: "hubs/hub1/managedClusters/c",
        observation: {
          hub: "hub1",
          name: "c",
          accepted: true,
          available: onHub.available,
        },
        conditions: {
          ManagedClusterConditionAvailable: {
            status: onHub.available,
            message: onHub.message,
          },
          ...(onHub.stale ? { Stale: { status: "True" } } : {}),
        },
      }),
    } as ClusterRow["observed"];
  }
  return enrolled;
}

const LEASE_STOPPED = "Registration agent stopped updating its lease.";

describe("clusterRowStatus", () => {
  it("is the cluster's own state while its hub reports it available", () => {
    const status = clusterRowStatus(enrolledRow("True", { available: "True" }));
    expect(status).toMatchObject({ text: "Ready", healthy: true });
    expect(status.onHub).toBeUndefined();
  });

  it("is the cluster's own state when no hub reports it", () => {
    const status = clusterRowStatus(enrolledRow("True"));
    expect(status).toMatchObject({ text: "Ready", healthy: true });
    expect(status.onHub).toBeUndefined();
  });

  it("adds the hub's word and asks for attention when the hub lost the cluster", () => {
    const status = clusterRowStatus(
      enrolledRow("True", { available: "Unknown", message: LEASE_STOPPED }),
    );
    expect(status).toMatchObject({
      text: "Ready",
      color: "green",
      healthy: false,
      attention: true,
    });
    expect(status.onHub).toMatchObject({ text: "Unknown", color: "orange" });
  });
});

describe("hubViewNotice", () => {
  it("is silent while the hub reports the cluster available", () => {
    expect(
      hubViewNotice(enrolledRow("True", { available: "True" })),
    ).toBeNull();
    expect(hubViewNotice(enrolledRow("True"))).toBeNull();
  });

  it("quotes the hub and says that the cluster still answers here", () => {
    const notice = hubViewNotice(
      enrolledRow("True", { available: "Unknown", message: LEASE_STOPPED }),
    );
    expect(notice?.title).toBe("Hub hub1 has lost contact with this cluster");
    expect(notice?.body).toBe(
      "The hub's reason: Registration agent stopped updating its lease. The cluster still answers here: it is indexed and deployments are delivered to it.",
    );
  });

  it("does not say that a cluster answers here when it does not", () => {
    const notice = hubViewNotice(
      enrolledRow("False", { available: "False", message: "no heartbeat" }),
    );
    expect(notice?.title).toBe("Hub hub1 reports this cluster as unavailable");
    expect(notice?.body).toBe("The hub's reason: no heartbeat.");
  });

  it("leaves a hub that does not answer to the notice about its labels", () => {
    expect(
      hubViewNotice(enrolledRow("True", { available: "True", stale: true })),
    ).toBeNull();
  });
});

describe("deriveClusterState", () => {
  it("returns PAUSED_AUTH when pauseReason is set", () => {
    expect(
      deriveClusterState(
        resource({ state: "CREATING", pauseReason: "auth failed" }),
      ),
    ).toBe("PAUSED_AUTH");
  });

  it("returns PAUSED_AUTH even when state is ACTIVE", () => {
    expect(
      deriveClusterState(
        resource({ state: "ACTIVE", pauseReason: "token expired" }),
      ),
    ).toBe("PAUSED_AUTH");
  });

  it("pauseReason takes precedence over conditions", () => {
    expect(
      deriveClusterState(
        resource({
          pauseReason: "cred issue",
          conditions: { Ready: { status: "True" } },
        }),
      ),
    ).toBe("PAUSED_AUTH");
  });

  it("returns resource.state when no pauseReason", () => {
    expect(deriveClusterState(resource({ state: "CREATING" }))).toBe(
      "CREATING",
    );
    expect(deriveClusterState(resource({ state: "ACTIVE" }))).toBe("ACTIVE");
    expect(deriveClusterState(resource({ state: "DELETING" }))).toBe(
      "DELETING",
    );
  });

  it("falls back to RUNNING when Ready condition is True", () => {
    expect(
      deriveClusterState(
        resource({ conditions: { Ready: { status: "True" } } }),
      ),
    ).toBe("RUNNING");
  });

  it("falls back to ERROR when Ready condition is False", () => {
    expect(
      deriveClusterState(
        resource({ conditions: { Ready: { status: "False" } } }),
      ),
    ).toBe("ERROR");
  });

  it("returns undefined when no state, no pauseReason, no conditions", () => {
    expect(deriveClusterState(resource())).toBeUndefined();
  });

  it("ignores empty string pauseReason", () => {
    expect(
      deriveClusterState(resource({ state: "ACTIVE", pauseReason: "" })),
    ).toBe("ACTIVE");
  });
});

describe("stateLabel", () => {
  it("returns label for known states", () => {
    expect(stateLabel("PAUSED_AUTH")).toEqual({
      text: "Paused",
      color: "orange",
    });
    expect(stateLabel("ACTIVE")).toEqual({ text: "Active", color: "green" });
  });

  it("composes underlying state with pause suffix", () => {
    expect(stateLabel("PAUSED_AUTH", "CREATING")).toEqual({
      text: "Creating (Paused)",
      color: "orange",
    });
    expect(stateLabel("PAUSED_AUTH", "ACTIVE")).toEqual({
      text: "Active (Paused)",
      color: "orange",
    });
  });

  it("falls back to plain Paused when underlying state is unknown", () => {
    expect(stateLabel("PAUSED_AUTH", "BOGUS")).toEqual({
      text: "Paused",
      color: "orange",
    });
  });

  it("returns Unknown for undefined or unrecognized state", () => {
    expect(stateLabel(undefined)).toEqual({ text: "Unknown", color: "grey" });
    expect(stateLabel("BOGUS")).toEqual({ text: "Unknown", color: "grey" });
  });
});

describe("state category helpers", () => {
  it("isHealthyState", () => {
    expect(isHealthyState("ACTIVE")).toBe(true);
    expect(isHealthyState("RUNNING")).toBe(true);
    expect(isHealthyState("PAUSED_AUTH")).toBe(false);
  });

  it("isFailureState", () => {
    expect(isFailureState("FAILED")).toBe(true);
    expect(isFailureState("ERROR")).toBe(true);
    expect(isFailureState("PAUSED_AUTH")).toBe(false);
  });

  it("isTransientState", () => {
    expect(isTransientState("CREATING")).toBe(true);
    expect(isTransientState("PAUSED_AUTH")).toBe(false);
  });
});

describe("extractClusterId", () => {
  it("strips clusters/ prefix", () => {
    expect(extractClusterId("clusters/my-cluster")).toBe("my-cluster");
  });

  it("returns unchanged if no prefix", () => {
    expect(extractClusterId("my-cluster")).toBe("my-cluster");
  });
});

describe("extractService", () => {
  it("extracts service from canonical name", () => {
    expect(
      extractService("//kind.open-cluster-management.io/clusters/c1"),
    ).toBe("kind.open-cluster-management.io");
  });

  it("returns empty for malformed name", () => {
    expect(extractService("no-match")).toBe("");
  });
});

describe("serviceLabel", () => {
  it("maps known services", () => {
    expect(serviceLabel("kubernetes.open-cluster-management.io")).toBe(
      "Kubernetes",
    );
    expect(serviceLabel("kind.open-cluster-management.io")).toBe("Kind");
  });

  it("falls back to raw service string", () => {
    expect(serviceLabel("custom.io")).toBe("custom.io");
  });
});

describe("buildAddonBasePath", () => {
  it("rejects an unregistered addon service", () => {
    expect(() =>
      buildAddonBasePath("unregistered-addon.open-cluster-management.io"),
    ).toThrow("Unsupported cluster service");
  });

  it("builds path for known services", () => {
    expect(buildAddonBasePath("kind.open-cluster-management.io")).toBe(
      "/apis/kind.open-cluster-management.io/v1alpha",
    );
  });

  it("throws for unknown service", () => {
    expect(() => buildAddonBasePath("unknown.io")).toThrow(
      "Unsupported cluster service",
    );
  });
});

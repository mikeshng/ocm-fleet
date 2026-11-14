import { mergeFleetClusters } from "@ocm-fleet/common";
import { describe, expect, it } from "vitest";

import { rowToDashboardCluster, toDashboardCluster } from "../useFleetData";

describe("retained cluster dashboard", () => {
  it("identifies Kind and leaves unreported versions unknown", () => {
    const cluster = toDashboardCluster(
      {
        name: "//kind.open-cluster-management.io/clusters/local",
        resourceType: "kind.open-cluster-management.io/Cluster",
        resource: {
          name: "clusters/local",
          uid: "cluster-1",
          state: "RUNNING",
          labels: {},
          createTime: "",
          updateTime: "",
          etag: "1",
        },
      },
      {
        region: "demo-region",
        lat: 0,
        lng: 0,
        environment: "Development",
        cpuPercent: 40,
        memoryPercent: 50,
      },
    );
    expect(cluster.id).toBe("local");
    expect(cluster.provider).toBe("kind");
    expect(cluster.version).toBe("unknown");
    expect(cluster.status).toBe("healthy");
  });
});

describe("clusters that were not created here", () => {
  const enrichment = {
    region: "demo-region",
    lat: 0,
    lng: 0,
    environment: "Development",
    cpuPercent: 40,
    memoryPercent: 50,
  };
  const common = {
    uid: "u",
    labels: {},
    createTime: "",
    updateTime: "",
    etag: "1",
  };

  it("shows an enrolled cluster with its version and state", () => {
    const [row] = mergeFleetClusters([
      {
        name: "//kubernetes.open-cluster-management.io/clusters/hub1.ext",
        resourceType: "kubernetes.open-cluster-management.io/Cluster",
        resource: {
          ...common,
          name: "clusters/hub1.ext",
          observation: { state: "enrolled", kubernetesVersion: "v1.37.0" },
          conditions: { Ready: { status: "True" } },
        },
      },
    ]);
    expect(rowToDashboardCluster(row, enrichment)).toMatchObject({
      id: "hub1.ext",
      provider: "enrolled",
      version: "v1.37.0",
      status: "healthy",
      state: "Ready",
    });
  });

  it("shows a cluster that only a hub reports", () => {
    const [row] = mergeFleetClusters([
      {
        name: "//ocm.open-cluster-management.io/hubs/hub1/managedClusters/ext2",
        resourceType: "ocm.open-cluster-management.io/ManagedCluster",
        resource: {
          ...common,
          name: "hubs/hub1/managedClusters/ext2",
          observation: {
            hub: "hub1",
            name: "ext2",
            accepted: true,
            available: "False",
            enrollment: { state: "observed" },
          },
        },
      },
    ]);
    expect(rowToDashboardCluster(row, enrichment)).toMatchObject({
      id: "hub1.ext2",
      provider: "observed",
      version: "unknown",
      status: "critical",
      state: "Unavailable",
    });
  });
});

import { describe, expect, it } from "vitest";

import { inventorySearchFilters } from "../inventoryFilters";

describe("inventorySearchFilters", () => {
  it("builds all four filters, each restricted to its resource type", () => {
    expect(inventorySearchFilters("prod-1")).toEqual({
      clusters:
        '(resourceType == "kind.open-cluster-management.io/Cluster") && (resource.name.startsWith("prod-1") || resource.name.startsWith("clusters/prod-1"))',
      enrolled:
        '(resourceType == "kubernetes.open-cluster-management.io/Cluster") && (resource.name.startsWith("clusters/prod-1") || "prod-1" in resource.observation.searchTerms)',
      observed:
        '(resourceType == "ocm.open-cluster-management.io/ManagedCluster") && (resource.observation.name.startsWith("prod-1") || "prod-1" in resource.observation.searchTerms)',
      kubernetes:
        'resourceType == "kubernetes.open-cluster-management.io/Object" && (resource.observation.kind == "Pod" || resource.observation.kind == "Namespace" || resource.observation.kind == "Node") && resource.observation.metadata.name.startsWith("prod-1")',
    });
  });

  it("escapes a quote and a backslash at every interpolation site", () => {
    const escaped = String.raw`a\\\"b`;
    expect(inventorySearchFilters(String.raw`a\"b`)).toEqual({
      clusters: `(resourceType == "kind.open-cluster-management.io/Cluster") && (resource.name.startsWith("${escaped}") || resource.name.startsWith("clusters/${escaped}"))`,
      enrolled: `(resourceType == "kubernetes.open-cluster-management.io/Cluster") && (resource.name.startsWith("clusters/${escaped}") || "${escaped}" in resource.observation.searchTerms)`,
      observed: `(resourceType == "ocm.open-cluster-management.io/ManagedCluster") && (resource.observation.name.startsWith("${escaped}") || "${escaped}" in resource.observation.searchTerms)`,
      kubernetes: `resourceType == "kubernetes.open-cluster-management.io/Object" && (resource.observation.kind == "Pod" || resource.observation.kind == "Namespace" || resource.observation.kind == "Node") && resource.observation.metadata.name.startsWith("${escaped}")`,
    });
  });
});

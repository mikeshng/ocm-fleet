import type { InventoryResource } from "@ocm-fleet/common";
import { describe, expect, it } from "vitest";

import { resolveK8sObject } from "../K8sObjectSearchResult";

function indexedObject(
  cluster: string,
  kind: string,
  name: string,
  namespace?: string,
): InventoryResource {
  const path = namespace
    ? `clusters/${cluster}/namespaces/${namespace}/apiResources/things/objects/uid-1`
    : `clusters/${cluster}/apiResources/things/objects/uid-1`;
  return {
    name: `//kubernetes.open-cluster-management.io/${path}`,
    resourceType: "kubernetes.open-cluster-management.io/Object",
    resource: {
      name: path,
      uid: "uid-1",
      observation: { kind, metadata: { name, namespace, uid: "uid-1" } },
    },
  } as unknown as InventoryResource;
}

describe("resolveK8sObject", () => {
  // A search runs across every cluster, and the same name is common on
  // many of them: a result has to say which cluster it is on.
  it("says which cluster a pod is on, then its namespace", () => {
    const result = resolveK8sObject(
      indexedObject("hub1.ext", "Pod", "web-7955-hb2d2", "web"),
    );
    expect(result.title).toBe("web-7955-hb2d2");
    expect(result.description).toBe("hub1.ext · web · Pod");
    expect(result.to).toBe("hub1.ext/uid-1");
  });

  it("says which cluster a namespace or a node is on", () => {
    expect(
      resolveK8sObject(indexedObject("hub1.ext2", "Namespace", "web"))
        .description,
    ).toBe("hub1.ext2 · Namespace");
    expect(
      resolveK8sObject(indexedObject("spoke", "Node", "node-1")).description,
    ).toBe("spoke · Node");
  });

  it("says which cluster an object of another kind is on", () => {
    const result = resolveK8sObject(
      indexedObject("hub1.ext", "ConfigMap", "settings", "web"),
    );
    expect(result.description).toBe("hub1.ext · web · ConfigMap");
    expect(result.to).toBe("hub1.ext");
  });

  it("leaves the cluster out when the record's name does not carry one", () => {
    const resource = indexedObject("hub1.ext", "Pod", "web-1", "web");
    const result = resolveK8sObject({
      ...resource,
      name: "//elsewhere/objects/uid-1",
    } as InventoryResource);
    expect(result.description).toBe("web · Pod");
  });
});

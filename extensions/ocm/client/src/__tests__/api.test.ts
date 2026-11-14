import { afterEach, describe, expect, it, vi } from "vitest";

import {
  connectHub,
  createEnrollmentAddon,
  findObservedCluster,
  getHubAccessManifest,
} from "../api";

function mockFetch(body: unknown, status = 200) {
  return vi.spyOn(globalThis, "fetch").mockResolvedValue({
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 200 ? "OK" : "Error",
    json: () => Promise.resolve(body),
  } as Response);
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("hub API", () => {
  it("reads the access manifest before anything is connected", async () => {
    const spy = mockFetch({ manifest: "kind: Namespace", instructions: ["a"] });

    const access = await getHubAccessManifest();

    const [url, init] = spy.mock.calls[0];
    expect(url).toBe(
      "/apis/ocm.open-cluster-management.io/v1alpha/hubAccessManifest",
    );
    expect(init?.method).toBe("GET");
    expect(access.manifest).toBe("kind: Namespace");
  });

  it("connects a hub with the details it is given", async () => {
    const spy = mockFetch({ hub: { id: "hub1" } });

    await connectHub({
      hubId: "hub1",
      apiServer: "https://hub:6443",
      caCert: "CA",
      token: "T",
    });

    const [url, init] = spy.mock.calls[0];
    expect(url).toBe(
      "/apis/ocm.open-cluster-management.io/v1alpha/hubs:connect",
    );
    expect(init?.method).toBe("POST");
    expect(JSON.parse(init?.body as string)).toEqual({
      hubId: "hub1",
      apiServer: "https://hub:6443",
      caCert: "CA",
      token: "T",
    });
  });

  it("reports the reason the API gives for a refusal", async () => {
    mockFetch({ error: "the credential can read no cluster" }, 400);

    await expect(connectHub({ hubId: "hub1" })).rejects.toThrow(
      "the credential can read no cluster",
    );
  });

  it("falls back to the status when the API gives no reason", async () => {
    mockFetch(null, 502);

    await expect(connectHub({ hubId: "hub1" })).rejects.toThrow(
      "Request failed (502)",
    );
  });

  it("asks for the enrollment add-on of one hub", async () => {
    const spy = mockFetch({
      bundle: { id: "b1" },
      manifest: "m",
      enableExample: "e",
      instructions: [],
    });

    const addon = await createEnrollmentAddon("hub1", {
      profile: "read-only",
      removeAccessWithAddon: true,
    });

    const [url, init] = spy.mock.calls[0];
    expect(url).toBe(
      "/apis/ocm.open-cluster-management.io/v1alpha/hubs/hub1:enrollmentAddon",
    );
    expect(JSON.parse(init?.body as string)).toEqual({
      profile: "read-only",
      removeAccessWithAddon: true,
    });
    expect(addon.manifest).toBe("m");
  });
});

describe("findObservedCluster", () => {
  function record(name: string, cluster?: string) {
    return {
      name: `//ocm.open-cluster-management.io/hubs/hub1/managedClusters/${name}`,
      resourceType: "ocm.open-cluster-management.io/ManagedCluster",
      resource: {
        name: `hubs/hub1/managedClusters/${name}`,
        observation: {
          hub: "hub1",
          name,
          enrollment: cluster
            ? { state: "enrolled", cluster }
            : { state: "observed" },
        },
      },
    };
  }

  it("prefers the record that is linked to the cluster", async () => {
    mockFetch({
      resources: [record("ext"), record("other", "clusters/hub1.ext")],
      nextPageToken: "",
    });

    const found = await findObservedCluster("hub1.ext");

    expect(found?.resource.observation?.name).toBe("other");
  });

  it("returns nothing when no hub reports the cluster", async () => {
    mockFetch({ resources: [], nextPageToken: "" });

    expect(await findObservedCluster("spoke")).toBeUndefined();
  });
});

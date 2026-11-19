import { afterEach, describe, expect, it, vi } from "vitest";

import {
  connectHub,
  createEnrollmentAddon,
  findObservedClusters,
  getHubAccessManifest,
  type HubObservation,
  type HubResult,
  listHubClusters,
  listHubs,
  mergeLiveHubTimes,
  unavailableClusterCount,
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
      "/apis/ocm.open-cluster-management.io/v1alpha/hubs/hub1:generateEnrollmentAddon",
    );
    expect(JSON.parse(init?.body as string)).toEqual({
      profile: "read-only",
      removeAccessWithAddon: true,
    });
    expect(addon.manifest).toBe("m");
  });

  it("says which clusters of the hub the add-on is for", async () => {
    const spy = mockFetch({
      bundle: { id: "b1" },
      manifest: "m",
      enableExample: "",
      rollout: { mode: "every-cluster" },
      instructions: [],
    });

    const addon = await createEnrollmentAddon("hub2", {
      rollout: "every-cluster",
    });

    const [, init] = spy.mock.calls[0];
    expect(JSON.parse(init?.body as string)).toEqual({
      rollout: "every-cluster",
    });
    expect(addon.rollout?.mode).toBe("every-cluster");
    expect(addon.enableExample).toBe("");
  });
});

describe("findObservedClusters", () => {
  function record(name: string, cluster?: string, hub = "hub1") {
    return {
      name: `//ocm.open-cluster-management.io/hubs/${hub}/managedClusters/${name}`,
      resourceType: "ocm.open-cluster-management.io/ManagedCluster",
      resource: {
        name: `hubs/${hub}/managedClusters/${name}`,
        observation: {
          hub,
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

    const found = await findObservedClusters("hub1.ext");

    expect(found.map((r) => r.resource.observation?.name)).toEqual(["other"]);
  });

  it("shows the record that shares the name when none is linked", async () => {
    mockFetch({ resources: [record("ext")], nextPageToken: "" });

    const found = await findObservedClusters("hub1.ext");

    expect(found.map((r) => r.resource.observation?.name)).toEqual(["ext"]);
  });

  it("returns every hub's record of a cluster that several hubs report", async () => {
    mockFetch({
      resources: [
        record("site-b-7", "clusters/byhand", "hub2"),
        record("edge-7", "clusters/byhand", "hub1"),
      ],
      nextPageToken: "",
    });

    // By hub id when no hub is preferred.
    expect(
      (await findObservedClusters("byhand")).map(
        (r) => r.resource.observation?.hub,
      ),
    ).toEqual(["hub1", "hub2"]);
    // The hub the page shows for the cluster comes first.
    expect(
      (await findObservedClusters("byhand", "hub2")).map(
        (r) => r.resource.observation?.hub,
      ),
    ).toEqual(["hub2", "hub1"]);
  });

  it("returns nothing when no hub reports the cluster", async () => {
    mockFetch({ resources: [], nextPageToken: "" });

    expect(await findObservedClusters("spoke")).toEqual([]);
  });
});

describe("listHubClusters", () => {
  it("escapes a hub id that goes into a filter", async () => {
    const spy = mockFetch({ resources: [], nextPageToken: "" });

    await listHubClusters(String.raw`h\"ub`);

    const [url] = spy.mock.calls[0];
    const filter = new URLSearchParams(String(url).split("?")[1]).get("filter");
    expect(filter).toContain(String.raw`resource.observation.hub == "h\\\"ub"`);
  });
});

describe("listHubs", () => {
  function hub(name: string, lastSeenAt: string): HubResult {
    return {
      name: `//ocm.open-cluster-management.io/hubs/${name}`,
      resourceType: "ocm.open-cluster-management.io/Hub",
      resource: {
        name: `hubs/${name}`,
        uid: name,
        labels: {},
        createTime: "2026-11-18T09:00:00Z",
        updateTime: "2026-11-18T10:00:00Z",
        etag: `"${name}-etag"`,
        observation: {
          id: name,
          apiServer: `https://${name}:6443`,
          lastSeenAt,
        },
      },
    };
  }

  it("puts the live last-heard time of each hub on its record", async () => {
    const stored = "2026-11-18T10:00:00Z";
    const live = "2026-11-18T12:00:00Z";
    const asked: string[] = [];
    vi.spyOn(globalThis, "fetch").mockImplementation((input) => {
      const url = String(input);
      asked.push(url);
      if (url.includes(":queryResources")) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () =>
            Promise.resolve({
              resources: [hub("hub1", stored), hub("hub2", stored)],
              nextPageToken: "",
            }),
        } as Response);
      }
      if (url.endsWith("/hubs/hub1/observation")) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: () =>
            Promise.resolve({
              id: "hub1",
              apiServer: "https://hub1:6443",
              lastSeenAt: live,
            }),
        } as Response);
      }
      // hub2's observation cannot be read: its record's time must stand.
      return Promise.resolve({
        ok: false,
        status: 404,
        json: () => Promise.resolve({ error: "not found" }),
      } as Response);
    });

    const hubs = await listHubs();

    expect(hubs.map((h) => h.resource.observation?.lastSeenAt)).toEqual([
      live,
      stored,
    ]);
    expect(
      asked
        .filter((url) => url.endsWith("/observation"))
        .map((url) => url.split("/hubs/")[1]),
    ).toEqual(["hub1/observation", "hub2/observation"]);
  });
});

describe("mergeLiveHubTimes", () => {
  function hub(name: string, lastSeenAt: string, withId = true): HubResult {
    return {
      name: `//ocm.open-cluster-management.io/hubs/${name}`,
      resourceType: "ocm.open-cluster-management.io/Hub",
      resource: {
        name: `hubs/${name}`,
        uid: name,
        labels: {},
        createTime: "2026-11-18T09:00:00Z",
        updateTime: "2026-11-18T10:00:00Z",
        etag: `"${name}-etag"`,
        observation: withId
          ? { id: name, apiServer: `https://${name}:6443`, lastSeenAt }
          : ({
              apiServer: `https://${name}:6443`,
              lastSeenAt,
            } as HubObservation),
      },
    };
  }

  it("replaces only the time, and only where a live time is known", () => {
    const hubs = [
      hub("hub1", "2026-11-18T10:00:00Z"),
      hub("hub2", "2026-11-18T11:00:00Z"),
    ];

    const merged = mergeLiveHubTimes(
      hubs,
      new Map([["hub1", "2026-11-18T12:00:00Z"]]),
    );

    expect(merged[0].resource.observation).toEqual({
      id: "hub1",
      apiServer: "https://hub1:6443",
      lastSeenAt: "2026-11-18T12:00:00Z",
    });
    expect(merged[1]).toBe(hubs[1]);
  });

  it("finds a hub by its record name when the observation has no id", () => {
    const hubs = [hub("hub9", "2026-11-18T10:00:00Z", false)];

    const merged = mergeLiveHubTimes(
      hubs,
      new Map([["hub9", "2026-11-18T12:00:00Z"]]),
    );

    expect(merged[0].resource.observation?.lastSeenAt).toBe(
      "2026-11-18T12:00:00Z",
    );
  });
});

describe("unavailableClusterCount", () => {
  const base = { id: "hub1", apiServer: "https://hub1:6443" };

  it("counts the clusters the hub does not report as available", () => {
    expect(
      unavailableClusterCount({ ...base, clusterCount: 5, availableCount: 3 }),
    ).toBe(2);
  });

  it("counts none when every cluster is available", () => {
    expect(
      unavailableClusterCount({ ...base, clusterCount: 2, availableCount: 2 }),
    ).toBe(0);
  });

  it("counts none for a record without counts, or without an observation", () => {
    expect(unavailableClusterCount(base)).toBe(0);
    expect(unavailableClusterCount(undefined)).toBe(0);
  });

  it("is never negative", () => {
    expect(
      unavailableClusterCount({ ...base, clusterCount: 1, availableCount: 2 }),
    ).toBe(0);
  });
});

import { afterEach, describe, expect, it, vi } from "vitest";

import { listDeployments, type MgmtDeployment } from "../api";

function deployment(name: string): MgmtDeployment {
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
  };
}

function page(from: number, to: number): MgmtDeployment[] {
  return Array.from({ length: to - from }, (_, i) =>
    deployment(`deployments/d-${String(from + i).padStart(3, "0")}`),
  );
}

afterEach(() => vi.unstubAllGlobals());

describe("listDeployments", () => {
  it("follows nextPageToken and merges every page", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(
        Response.json({ deployments: page(0, 50), nextPageToken: "t1" }),
      )
      .mockResolvedValueOnce(
        Response.json({ deployments: page(50, 100), nextPageToken: "t2" }),
      )
      .mockResolvedValueOnce(
        Response.json({ deployments: page(100, 120), nextPageToken: "" }),
      );
    vi.stubGlobal("fetch", fetch);

    const response = await listDeployments();

    expect(fetch.mock.calls.map((call) => call[0])).toEqual([
      "/v1alpha/deployments",
      "/v1alpha/deployments?pageToken=t1",
      "/v1alpha/deployments?pageToken=t2",
    ]);
    expect(response.deployments).toHaveLength(120);
    expect(response.deployments[0].name).toBe("deployments/d-000");
    expect(response.deployments[119].name).toBe("deployments/d-119");
    expect(response.nextPageToken).toBe("");
  });

  it("reports a server that repeats the token it was given", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(
        Response.json({ deployments: page(0, 50), nextPageToken: "t1" }),
      )
      .mockResolvedValueOnce(
        Response.json({ deployments: page(50, 60), nextPageToken: "t1" }),
      );
    vi.stubGlobal("fetch", fetch);

    await expect(listDeployments()).rejects.toThrow(
      'Cyclic pagination detected: repeated nextPageToken "t1"',
    );
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it("detects a token cycle longer than one step", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValueOnce(
        Response.json({ deployments: page(0, 2), nextPageToken: "t1" }),
      )
      .mockResolvedValueOnce(
        Response.json({ deployments: page(2, 4), nextPageToken: "t2" }),
      )
      .mockResolvedValueOnce(
        Response.json({ deployments: page(4, 6), nextPageToken: "t1" }),
      );
    vi.stubGlobal("fetch", fetch);

    await expect(listDeployments()).rejects.toThrow(
      'Cyclic pagination detected: repeated nextPageToken "t1"',
    );
    expect(fetch).toHaveBeenCalledTimes(3);
  });

  it("bounds a server that keeps minting new tokens", async () => {
    let pageNumber = 0;
    const fetch = vi.fn().mockImplementation(() => {
      pageNumber += 1;
      return Promise.resolve(
        Response.json({
          deployments: page(0, 1),
          nextPageToken: `t${pageNumber}`,
        }),
      );
    });
    vi.stubGlobal("fetch", fetch);

    await expect(listDeployments()).rejects.toThrow(
      "The deployment list did not end after 10000 pages",
    );
    expect(fetch).toHaveBeenCalledTimes(10_000);
  });
});

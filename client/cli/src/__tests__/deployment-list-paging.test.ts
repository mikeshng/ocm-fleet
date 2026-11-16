import { beforeEach, describe, expect, it, vi } from "vitest";

import { parseArgs } from "../argv";

const { request } = vi.hoisted(() => ({ request: vi.fn() }));

vi.mock("../commands/context", () => ({
  clientForArgs: async () => ({ request }),
}));

import { listCommand } from "../commands/deployment/list";
import type { DeploymentListItem } from "../commands/deployment/types";

function names(from: number, to: number): DeploymentListItem[] {
  return Array.from({ length: to - from }, (_, i) => ({
    name: `deployments/d-${String(from + i).padStart(3, "0")}`,
  }));
}

describe("deployment list paging", () => {
  beforeEach(() => request.mockReset());

  it("follows nextPageToken and returns every deployment", async () => {
    request
      .mockResolvedValueOnce({ deployments: names(0, 50), nextPageToken: "t1" })
      .mockResolvedValueOnce({
        deployments: names(50, 100),
        nextPageToken: "t2",
      })
      .mockResolvedValueOnce({
        deployments: names(100, 120),
        nextPageToken: "",
      });

    const result = await listCommand.run({
      args: parseArgs(["deployment", "list"]),
    });

    expect(request.mock.calls.map((call) => call[0])).toEqual([
      "/v1alpha/deployments",
      "/v1alpha/deployments?pageToken=t1",
      "/v1alpha/deployments?pageToken=t2",
    ]);
    expect(result).toHaveLength(120);
    expect(result[0].name).toBe("deployments/d-000");
    expect(result[119].name).toBe("deployments/d-119");
  });

  it("reports a server that repeats the token it was given", async () => {
    request
      .mockResolvedValueOnce({ deployments: names(0, 50), nextPageToken: "t1" })
      .mockResolvedValueOnce({
        deployments: names(50, 60),
        nextPageToken: "t1",
      });

    await expect(
      listCommand.run({ args: parseArgs(["deployment", "list"]) }),
    ).rejects.toThrow(
      'Cyclic pagination detected: repeated nextPageToken "t1"',
    );
    expect(request).toHaveBeenCalledTimes(2);
  });

  it("detects a token cycle longer than one step", async () => {
    request
      .mockResolvedValueOnce({ deployments: names(0, 2), nextPageToken: "t1" })
      .mockResolvedValueOnce({ deployments: names(2, 4), nextPageToken: "t2" })
      .mockResolvedValueOnce({ deployments: names(4, 6), nextPageToken: "t1" });

    await expect(
      listCommand.run({ args: parseArgs(["deployment", "list"]) }),
    ).rejects.toThrow(
      'Cyclic pagination detected: repeated nextPageToken "t1"',
    );
    expect(request).toHaveBeenCalledTimes(3);
  });

  it("bounds a server that keeps minting new tokens", async () => {
    let page = 0;
    request.mockImplementation(() => {
      page += 1;
      return Promise.resolve({
        deployments: names(0, 1),
        nextPageToken: `t${page}`,
      });
    });

    await expect(
      listCommand.run({ args: parseArgs(["deployment", "list"]) }),
    ).rejects.toThrow("The deployment list did not end after 10000 pages");
    expect(request).toHaveBeenCalledTimes(10_000);
  });

  it("passes --page-size with the paging token", async () => {
    request
      .mockResolvedValueOnce({ deployments: names(0, 2), nextPageToken: "t1" })
      .mockResolvedValueOnce({ deployments: names(2, 3), nextPageToken: "" });

    await listCommand.run({
      args: parseArgs(["deployment", "list", "--page-size", "2"]),
    });

    expect(request.mock.calls.map((call) => call[0])).toEqual([
      "/v1alpha/deployments?pageSize=2",
      "/v1alpha/deployments?pageSize=2&pageToken=t1",
    ]);
  });
});

import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

import { executeCommand } from "../app";
import { parseArgs } from "../argv";
import { accessManifestCommand } from "../commands/hub/access-manifest";
import { connectCommand } from "../commands/hub/connect";
import { disconnectCommand } from "../commands/hub/disconnect";
import { enrollmentAddonCommand } from "../commands/hub/enrollment-addon";
import { listCommand } from "../commands/hub/list";
import { findCommand } from "../commands/registry";

const api = "/apis/ocm.open-cluster-management.io/v1alpha";

interface Call {
  method: string;
  path: string;
  body?: unknown;
}

/** Answers the CLI's requests from a table of "METHOD path" and records them. */
function serve(answers: Record<string, unknown>): Call[] {
  const calls: Call[] = [];
  vi.stubGlobal("fetch", async (url: URL, init: RequestInit = {}) => {
    const method = init.method ?? "GET";
    const path = url.pathname + url.search;
    calls.push({
      method,
      path,
      ...(init.body ? { body: JSON.parse(String(init.body)) } : {}),
    });
    const answer = answers[`${method} ${path}`];
    return answer === undefined
      ? new Response("no such hub", { status: 404, statusText: "Not Found" })
      : Response.json(answer);
  });
  return calls;
}

async function workDirectory(): Promise<string> {
  return mkdtemp(join(tmpdir(), "fleetctl-hub-"));
}

async function argsFor(...argv: string[]) {
  return parseArgs([
    ...argv,
    "--config-dir",
    await workDirectory(),
    "--server",
    "https://fleet.example",
  ]);
}

const connected = {
  name: "hubs/hub2",
  conditions: { Ready: { status: "True" } },
  observation: { id: "hub2", clusterCount: 2, availableCount: 2 },
};

afterEach(() => {
  vi.unstubAllGlobals();
});

describe("hub commands", () => {
  it("are found under hub and its alias", () => {
    expect(findCommand(["hubs", "connect"])?.path).toBe("hub connect");
    expect(findCommand(["hub", "ls"])?.path).toBe("hub list");
    expect(findCommand(["hub", "enrollment-addon"])?.path).toBe(
      "hub enrollment-addon",
    );
  });

  it("prints the access manifest as it is", async () => {
    const calls = serve({
      [`GET ${api}/hubAccessManifest`]: { manifest: "kind: Namespace\n" },
    });
    const text = await executeCommand(accessManifestCommand, {
      args: await argsFor("hub", "access-manifest"),
    });
    expect(text).toBe("kind: Namespace");
    expect(calls).toEqual([
      { method: "GET", path: `${api}/hubAccessManifest` },
    ]);
  });

  it("connects a hub with its address, CA, and token, and waits until it is read", async () => {
    const directory = await workDirectory();
    await writeFile(join(directory, "ca.crt"), "CERTIFICATE");
    await writeFile(join(directory, "token"), "secret-token\n");
    const calls = serve({
      [`POST ${api}/hubs:connect`]: {},
      [`GET ${api}/hubs/hub2`]: connected,
    });
    const text = await executeCommand(connectCommand, {
      args: await argsFor(
        "hub",
        "connect",
        "--id",
        "hub2",
        "--name",
        "Team B",
        "--api-server",
        "https://hub2:6443",
        "--ca-file",
        join(directory, "ca.crt"),
        "--token-file",
        join(directory, "token"),
      ),
    });
    expect(text).toBe("hub hub2 is connected, read-only: 2 clusters");
    expect(calls[0]).toEqual({
      method: "POST",
      path: `${api}/hubs:connect`,
      body: {
        hubId: "hub2",
        apiServer: "https://hub2:6443",
        token: "secret-token",
        caCert: "CERTIFICATE",
        displayName: "Team B",
      },
    });
  });

  it("says what a hub is waiting for when it does not connect in time", async () => {
    serve({
      [`POST ${api}/hubs:connect`]: {},
      [`GET ${api}/hubs/hub2`]: {
        name: "hubs/hub2",
        conditions: { Synced: { status: "False" } },
      },
    });
    await expect(
      connectCommand.run({
        args: await argsFor(
          "hub",
          "connect",
          "hub2",
          "--api-server",
          "https://hub2:6443",
          "--token",
          "t",
          "--timeout",
          "0",
        ),
      }),
    ).rejects.toThrow("hub hub2 is not connected after 0 s: Reading");
  });

  it("refuses a timeout that is not a number of seconds, before it asks anything", async () => {
    const calls = serve({});
    await expect(
      connectCommand.run({
        args: await argsFor(
          "hub",
          "connect",
          "hub2",
          "--api-server",
          "https://hub2:6443",
          "--token",
          "t",
          "--timeout",
          "soon",
        ),
      }),
    ).rejects.toThrow("--timeout takes a number of seconds");
    expect(calls).toEqual([]);
  });

  it("asks for the enrollment add-on with the rollout and the grant that were chosen", async () => {
    const calls = serve({
      [`POST ${api}/hubs/hub1:generateEnrollmentAddon`]: {
        manifest: "kind: AddOnTemplate\n",
      },
    });
    const plain = await executeCommand(enrollmentAddonCommand, {
      args: await argsFor("hub", "enrollment-addon", "--id", "hub1"),
    });
    await enrollmentAddonCommand.run({
      args: await argsFor(
        "hub",
        "enrollment-addon",
        "--id",
        "hub1",
        "--rollout",
        "every-cluster",
        "--grant-platform-delivery",
        "--profile",
        "read-only",
      ),
    });
    await enrollmentAddonCommand.run({
      args: await argsFor(
        "hub",
        "enrollment-addon",
        "--id",
        "hub1",
        "--placement",
        "fleet/prod",
      ),
    });
    expect(plain).toBe("kind: AddOnTemplate");
    expect(calls.map((call) => call.body)).toEqual([
      { grantPlatformDelivery: false },
      {
        grantPlatformDelivery: true,
        profile: "read-only",
        rollout: "every-cluster",
      },
      {
        grantPlatformDelivery: false,
        rollout: "placement",
        placementNamespace: "fleet",
        placementName: "prod",
      },
    ]);
  });

  it("refuses a rollout it does not know", async () => {
    await expect(
      enrollmentAddonCommand.run({
        args: await argsFor(
          "hub",
          "enrollment-addon",
          "--id",
          "hub1",
          "--rollout",
          "all",
        ),
      }),
    ).rejects.toThrow("--rollout takes one of: manual, every-cluster");
  });

  it("refuses what it cannot ask the server with, before it asks anything", async () => {
    const calls = serve({});
    await expect(
      connectCommand.run({ args: await argsFor("hub", "connect", "hub2") }),
    ).rejects.toThrow("--api-server is required");
    await expect(
      connectCommand.run({
        args: await argsFor("hub", "connect", "--api-server", "https://hub2"),
      }),
    ).rejects.toThrow("--id is required");
    await expect(
      enrollmentAddonCommand.run({
        args: await argsFor(
          "hub",
          "enrollment-addon",
          "--id",
          "hub1",
          "--placement",
          "prod",
        ),
      }),
    ).rejects.toThrow("--placement takes NAMESPACE/NAME");
    expect(calls).toEqual([]);
  });

  it("lists the hubs with what each reports", async () => {
    serve({
      [`GET ${api}/hubs?pageSize=100`]: {
        hubs: [
          connected,
          {
            name: "hubs/hub1",
            conditions: { Connected: { status: "False" } },
            observation: { clusterCount: 2, enrolledCount: 1 },
          },
        ],
      },
    });
    const text = await executeCommand(listCommand, {
      args: await argsFor("hub", "list"),
    });
    expect(String(text).split("\n")).toEqual([
      "HUB    STATUS          CLUSTERS   AVAILABLE   ENROLLED   ACCESS",
      "hub2   Connected       2          2           0          read-only",
      "hub1   Not reachable   2          0           1          read-only",
    ]);
  });

  it("says so when no hub is connected", async () => {
    serve({ [`GET ${api}/hubs?pageSize=100`]: {} });
    expect(
      await executeCommand(listCommand, { args: await argsFor("hub", "list") }),
    ).toBe("No hub is connected.");
  });

  it("disconnects a hub", async () => {
    const calls = serve({ [`POST ${api}/hubs/hub2:disconnect`]: {} });
    const text = await executeCommand(disconnectCommand, {
      args: await argsFor("hub", "disconnect", "hub2"),
    });
    expect(text).toBe("hub hub2 is disconnected");
    expect(calls).toEqual([
      { method: "POST", path: `${api}/hubs/hub2:disconnect`, body: {} },
    ]);
  });
});

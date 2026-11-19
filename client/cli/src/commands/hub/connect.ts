import { readFile } from "node:fs/promises";

import { flagString, type ParsedArgs } from "../../argv";
import { clientForArgs } from "../context";
import type { CommandSpec } from "../types";
import {
  hubAccess,
  hubApi,
  hubIdOf,
  hubPath,
  type HubRecord,
  hubStatus,
  requiredHubId,
} from "./helpers";

async function hubToken(args: ParsedArgs): Promise<string> {
  const file = flagString(args, "token-file");
  const token = file ? await readFile(file, "utf8") : flagString(args, "token");
  if (!token.trim()) throw new Error("--token or --token-file is required");
  return token.trim();
}

export const connectCommand = {
  path: "hub connect",
  description: "Connect an OCM hub with a token of its read-only account",
  implemented: true,
  present: (hub) => {
    const count = hub.observation?.clusterCount ?? 0;
    return `hub ${hubIdOf(hub)} is connected, ${hubAccess(hub)}: ${count} cluster${count === 1 ? "" : "s"}`;
  },
  run: async ({ args }) => {
    const id = requiredHubId(args);
    const apiServer = flagString(args, "api-server");
    if (!apiServer) throw new Error("--api-server is required");
    const caFile = flagString(args, "ca-file");
    const displayName = flagString(args, "name");
    const seconds = Number(flagString(args, "timeout", "120"));
    if (!Number.isFinite(seconds) || seconds < 0) {
      throw new Error("--timeout takes a number of seconds");
    }
    const client = await clientForArgs(args);
    await client.request(`${hubApi}/hubs:connect`, {
      method: "POST",
      body: JSON.stringify({
        hubId: id,
        apiServer,
        token: await hubToken(args),
        ...(caFile ? { caCert: await readFile(caFile, "utf8") } : {}),
        ...(displayName ? { displayName } : {}),
      }),
    });
    // The connection is made at once; the hub's clusters are read a moment later.
    const deadline = Date.now() + seconds * 1000;
    for (;;) {
      const hub = await client.request<HubRecord>(hubPath(id));
      if (hubStatus(hub) === "Connected") return hub;
      if (Date.now() >= deadline) {
        throw new Error(
          `hub ${id} is not connected after ${seconds} s: ${hubStatus(hub)}`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  },
} satisfies CommandSpec<HubRecord>;

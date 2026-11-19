import { clientForArgs } from "../context";
import type { CommandSpec } from "../types";
import {
  hubAccess,
  hubApi,
  hubIdOf,
  type HubRecord,
  hubStatus,
} from "./helpers";

const columns = [
  "HUB",
  "STATUS",
  "CLUSTERS",
  "AVAILABLE",
  "ENROLLED",
  "ACCESS",
];

function table(rows: string[][]): string {
  const widths = columns.map((column, index) =>
    Math.max(column.length, ...rows.map((row) => row[index].length)),
  );
  return [columns, ...rows]
    .map((row) =>
      row
        .map((cell, index) => cell.padEnd(widths[index]))
        .join("   ")
        .trimEnd(),
    )
    .join("\n");
}

export const listCommand = {
  path: "hub list",
  aliases: ["ls"],
  description: "List connected OCM hubs",
  implemented: true,
  present: (hubs) =>
    hubs.length === 0
      ? "No hub is connected."
      : table(
          hubs.map((hub) => [
            hubIdOf(hub),
            hubStatus(hub),
            String(hub.observation?.clusterCount ?? 0),
            String(hub.observation?.availableCount ?? 0),
            String(hub.observation?.enrolledCount ?? 0),
            hubAccess(hub),
          ]),
        ),
  run: async ({ args }) => {
    const answer = await (
      await clientForArgs(args)
    ).request<{ hubs?: HubRecord[] }>(`${hubApi}/hubs?pageSize=100`);
    return answer.hubs ?? [];
  },
} satisfies CommandSpec<HubRecord[]>;

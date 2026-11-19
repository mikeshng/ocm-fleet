import { clientForArgs } from "../context";
import type { CommandSpec } from "../types";
import { hubPath, requiredHubId } from "./helpers";

export const disconnectCommand = {
  path: "hub disconnect",
  description: "Disconnect an OCM hub; its enrolled clusters stay enrolled",
  implemented: true,
  present: (id) => `hub ${id} is disconnected`,
  run: async ({ args }) => {
    const id = requiredHubId(args);
    await (
      await clientForArgs(args)
    ).request(`${hubPath(id)}:disconnect`, { method: "POST", body: "{}" });
    return id;
  },
} satisfies CommandSpec<string>;

import { clientForArgs } from "../context";
import type { CommandSpec } from "../types";
import { hubApi, withoutFinalNewline } from "./helpers";

export const accessManifestCommand = {
  path: "hub access-manifest",
  description: "Print the read-only account a hub's administrator applies",
  implemented: true,
  present: (manifest) => manifest,
  run: async ({ args }) => {
    const answer = await (
      await clientForArgs(args)
    ).request<{ manifest: string }>(`${hubApi}/hubAccessManifest`);
    return withoutFinalNewline(answer.manifest);
  },
} satisfies CommandSpec<string>;

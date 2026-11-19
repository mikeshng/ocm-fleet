import { flagString, hasFlag } from "../../argv";
import { clientForArgs } from "../context";
import type { CommandSpec } from "../types";
import { hubPath, requiredHubId, withoutFinalNewline } from "./helpers";

const rollouts = ["manual", "every-cluster"];

export const enrollmentAddonCommand = {
  path: "hub enrollment-addon",
  description:
    "Print the add-on that enrolls a hub's clusters, for the hub's administrator to apply",
  implemented: true,
  present: (manifest) => manifest,
  run: async ({ args }) => {
    const id = requiredHubId(args);
    const profile = flagString(args, "profile");
    const rollout = flagString(args, "rollout");
    const placement = flagString(args, "placement");
    const [placementNamespace, placementName] = placement.split("/");
    if (placement && (!placementNamespace || !placementName)) {
      throw new Error(
        "--placement takes NAMESPACE/NAME, a Placement on the hub",
      );
    }
    if (rollout && !rollouts.includes(rollout)) {
      throw new Error(`--rollout takes one of: ${rollouts.join(", ")}`);
    }
    const answer = await (
      await clientForArgs(args)
    ).request<{ manifest: string }>(`${hubPath(id)}:generateEnrollmentAddon`, {
      method: "POST",
      body: JSON.stringify({
        grantPlatformDelivery: hasFlag(args, "grant-platform-delivery"),
        ...(profile ? { profile } : {}),
        ...(hasFlag(args, "remove-access-with-addon")
          ? { removeAccessWithAddon: true }
          : {}),
        ...(placement
          ? { rollout: "placement", placementNamespace, placementName }
          : rollout
            ? { rollout }
            : {}),
      }),
    });
    return withoutFinalNewline(answer.manifest);
  },
} satisfies CommandSpec<string>;

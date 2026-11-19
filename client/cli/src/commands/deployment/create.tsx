import { flagString } from "../../argv";
import { configDirectory } from "../../config";
import {
  buildDeploymentEnvelope,
  signDeploymentEnvelope,
} from "../../crypto/signing";
import type { CommandSpec } from "../types";
import {
  deploymentBody,
  deploymentClient,
  parsePlacement,
  readManifest,
} from "./helpers";

/** The created deployment as the server returns it; only its name is presented. */
interface CreatedDeployment {
  name: string;
}

export const createCommand = {
  path: "deployment create",
  description: "Create a deployment",
  implemented: true,
  present: (deployment) => `Created ${deployment.name}.`,
  run: async ({ args }) => {
    const id = flagString(args, "id");
    const manifestFile = flagString(args, "manifest-file");
    const resourceType = flagString(args, "resource-type");
    if (!id || !manifestFile || !resourceType) {
      throw new Error(
        "--id, --manifest-file, and --resource-type are required",
      );
    }
    const manifest = await readManifest(manifestFile);
    const body = deploymentBody(args, manifest.raws);
    if (args.flags.get("sign") === true) {
      if (manifest.contents.length !== 1) {
        throw new Error("--sign takes a file with one manifest");
      }
      const validUntil = new Date(Date.now() + 24 * 60 * 60 * 1000);
      const envelope = buildDeploymentEnvelope({
        deploymentID: id,
        manifestType: resourceType,
        manifest: manifest.contents[0],
        placement: parsePlacement(args),
        validUntil,
      });
      body.userSignature = await signDeploymentEnvelope(
        configDirectory(flagString(args, "config-dir") || undefined),
        envelope,
      );
      body.validUntil = validUntil.toISOString();
    }
    const response = await (
      await deploymentClient(args)
    ).request<CreatedDeployment>(
      `/v1alpha/deployments?deploymentId=${encodeURIComponent(id)}`,
      {
        method: "POST",
        body: JSON.stringify(body),
      },
    );
    return response;
  },
} satisfies CommandSpec<CreatedDeployment>;

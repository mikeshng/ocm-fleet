import { readFile } from "node:fs/promises";

import { loadAll } from "js-yaml";

import { flagString, type ParsedArgs } from "../../argv";
import type { CliClient } from "../../client";
import { clientForArgs } from "../context";

export function deploymentName(value: string): string {
  return value.startsWith("deployments/") ? value : `deployments/${value}`;
}

export function deploymentClient(args: ParsedArgs): Promise<CliClient> {
  return clientForArgs(args);
}

/** The manifests of one file: a JSON file is one, a YAML file one per document. */
export interface ManifestInput {
  contents: unknown[];
  raws: string[];
}

export interface SigningPlacement {
  type: string;
  targets?: string[];
  match_labels?: Record<string, string>;
}

export function parsePlacement(args: ParsedArgs): SigningPlacement {
  const type = flagString(args, "placement-type", "all").toLowerCase();
  if (type === "static") {
    const targets = flagString(args, "target-ids")
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean);
    if (targets.length === 0) {
      throw new Error("--target-ids is required for static placement");
    }
    return { type, targets };
  }
  if (type === "selector") {
    const selector = flagString(args, "target-selector");
    if (!selector) {
      throw new Error("--target-selector is required for selector placement");
    }
    return {
      type,
      match_labels: Object.fromEntries(
        selector.split(",").map((entry) => {
          const [key, ...values] = entry.split("=");
          if (!key || values.length === 0) {
            throw new Error(`invalid target selector ${entry}`);
          }
          return [key.trim(), values.join("=").trim()];
        }),
      ),
    };
  }
  return { type };
}

export async function readManifest(path: string): Promise<ManifestInput> {
  const text = path === "-" ? await readStdin() : await readFile(path, "utf8");
  try {
    return {
      contents: [JSON.parse(text)],
      raws: [Buffer.from(text).toString("base64")],
    };
  } catch {
    const contents = loadAll(text).filter((document) => document != null);
    if (contents.length === 0) throw new Error(`${path} holds no manifest`);
    return {
      contents,
      raws: contents.map((content) =>
        Buffer.from(JSON.stringify(content)).toString("base64"),
      ),
    };
  }
}

async function readStdin(): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf8");
}

export function deploymentBody(
  args: ParsedArgs,
  rawManifests: string[],
): Record<string, unknown> {
  const placement = parsePlacement(args);
  const placementStrategy: Record<string, unknown> = {
    type: `TYPE_${placement.type.toUpperCase()}`,
  };
  if (placement.targets) placementStrategy.targetIds = placement.targets;
  if (placement.match_labels)
    placementStrategy.targetSelector = { matchLabels: placement.match_labels };
  const rolloutType = flagString(
    args,
    "rollout-type",
    "immediate",
  ).toLowerCase();
  if (rolloutType !== "immediate") {
    throw new Error(`unsupported rollout type ${rolloutType}`);
  }
  return {
    manifestStrategy: {
      type: "TYPE_INLINE",
      manifests: rawManifests.map((raw) => ({
        manifestType: flagString(args, "resource-type"),
        raw,
      })),
    },
    placementStrategy,
    rolloutStrategy: { type: "TYPE_IMMEDIATE" },
  };
}

import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { parseArgs } from "../argv";
import { createCommand } from "../commands/deployment/create";
import { deploymentBody, readManifest } from "../commands/deployment/helpers";

async function fileWith(name: string, text: string): Promise<string> {
  const path = join(await mkdtemp(join(tmpdir(), "fleetctl-manifest-")), name);
  await writeFile(path, text);
  return path;
}

function decoded(raw: string): unknown {
  return JSON.parse(Buffer.from(raw, "base64").toString());
}

describe("deployment create", () => {
  it("confirms with the name of the created deployment", () => {
    expect(createCommand.present({ name: "deployments/hello" })).toBe(
      "Created deployments/hello.",
    );
  });

  it("sends a label selector as matchLabels and every manifest of the file", () => {
    const body = deploymentBody(
      parseArgs([
        "deployment",
        "create",
        "--resource-type",
        "kubernetes",
        "--placement-type",
        "selector",
        "--target-selector",
        "env=prod,tier=web",
      ]),
      ["b25l", "dHdv"],
    );
    expect(body.placementStrategy).toEqual({
      type: "TYPE_SELECTOR",
      targetSelector: { matchLabels: { env: "prod", tier: "web" } },
    });
    expect(body.manifestStrategy).toEqual({
      type: "TYPE_INLINE",
      manifests: [
        { manifestType: "kubernetes", raw: "b25l" },
        { manifestType: "kubernetes", raw: "dHdv" },
      ],
    });
  });

  it("reads a JSON file as one manifest, byte for byte", async () => {
    const text = '{ "kind": "ConfigMap" }\n';
    const manifest = await readManifest(await fileWith("one.json", text));
    expect(manifest.contents).toEqual([{ kind: "ConfigMap" }]);
    expect(manifest.raws).toEqual([Buffer.from(text).toString("base64")]);
  });

  it("reads a YAML file as one manifest for each document", async () => {
    const manifest = await readManifest(
      await fileWith(
        "two.yaml",
        "kind: Namespace\nmetadata:\n  name: web\n---\n\n---\nkind: Deployment\n",
      ),
    );
    expect(manifest.contents).toEqual([
      { kind: "Namespace", metadata: { name: "web" } },
      { kind: "Deployment" },
    ]);
    expect(manifest.raws.map(decoded)).toEqual(manifest.contents);
  });

  it("refuses a file that holds no manifest", async () => {
    await expect(
      readManifest(await fileWith("none.yaml", "# nothing\n")),
    ).rejects.toThrow("holds no manifest");
  });

  it("signs one manifest only", async () => {
    await expect(
      createCommand.run({
        args: parseArgs([
          "deployment",
          "create",
          "--id",
          "two",
          "--resource-type",
          "kubernetes",
          "--manifest-file",
          await fileWith(
            "two.yaml",
            "kind: Namespace\n---\nkind: Deployment\n",
          ),
          "--sign",
        ]),
      }),
    ).rejects.toThrow("--sign takes a file with one manifest");
  });
});

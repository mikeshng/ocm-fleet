import { expect, test } from "../fixtures";
import { uniqueId } from "../steps/deployments";

const ANSI = /\u001B\[[0-9;]*m/g;

/** Wrapped table rows rejoin once layout whitespace is removed. */
function compact(text: string): string {
  return text.replace(ANSI, "").replace(/\s+/g, "");
}

function occurrences(text: string, value: string): number {
  return text.split(value).length - 1;
}

test(
  "deployment list follows every page and reports a failed delivery",
  { kindClusters: [{ access: "modifiable", spec: "any", state: "any" }] },
  async ({ cli, kindClusters: [cluster] }) => {
    const { deployments } = cli;
    const deliveredId = uniqueId("paging-ok");
    const rejectedId = uniqueId("paging-bad");
    const configMapName = uniqueId("paging-cm");
    const targetId = `k8s-${cluster.id}`;

    await test.step("create a deployment that delivers", async () => {
      await deployments.createManifest({
        id: deliveredId,
        manifest: {
          apiVersion: "v1",
          data: { from: "ocm-fleet-e2e-cli" },
          kind: "ConfigMap",
          metadata: { name: configMapName, namespace: "default" },
        },
        targets: [cluster.id],
      });
      await deployments.waitUntilActive(deliveredId);
    });

    await test.step("create a deployment Kubernetes rejects and wait for the recorded failure", async () => {
      await deployments.createManifest({
        id: rejectedId,
        manifest: {
          apiVersion: "v1",
          immutable: "not-a-boolean",
          kind: "ConfigMap",
          metadata: { name: `${configMapName}-bad`, namespace: "default" },
        },
        targets: [cluster.id],
      });
      await deployments.waitUntilDeliveryAttemptFails(
        rejectedId,
        targetId,
        "immutable",
      );
    });

    await test.step("list one deployment per page and find each exactly once", async () => {
      const { names, table } = await deployments.listPage(1);
      expect(
        names.filter((name) => name === `deployments/${deliveredId}`),
      ).toEqual([`deployments/${deliveredId}`]);
      expect(
        names.filter((name) => name === `deployments/${rejectedId}`),
      ).toEqual([`deployments/${rejectedId}`]);

      // The attention banner appears once; what precedes it is the main
      // table, whose rows must each name a deployment exactly once.
      const parts = table.split("Deliveries needing attention");
      expect(parts).toHaveLength(2);
      const main = compact(parts[0]);
      const attention = compact(parts[1]);
      expect(occurrences(main, `deployments/${deliveredId}`)).toBe(1);
      expect(occurrences(main, `deployments/${rejectedId}`)).toBe(1);
      expect(main).toContain("1/1delivered");
      expect(main).toContain("0/1delivered");

      // The failed delivery is reported from the gateway's own payload:
      // its target, its attempt count, and the API server's reason.
      expect(attention).toContain(`deployments/${rejectedId}`);
      expect(attention).not.toContain(`deployments/${deliveredId}`);
      expect(attention).toContain(targetId);
      expect(attention).toMatch(/[1-9]\d*failedattempts?/);
      expect(attention).toContain("immutable");
    });
  },
);

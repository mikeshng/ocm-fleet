import { ResourceApiError } from "@ocm-fleet/common";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { ClusterRow } from "../clusterTypes";
import {
  createManifestBundle,
  credentialExpiresAt,
  enrollmentActions,
  type EnrollmentBundleAnswer,
  type EnrollmentClusterRecord,
  forgetCluster,
  manifestEnrollmentBody,
  renewClusterCredential,
  revokeEnrollmentBundle,
  withdrawCluster,
  withdrawConflict,
} from "../enrollmentApi";

const SERVICE = "kubernetes.open-cluster-management.io";

function clusterRecord(): EnrollmentClusterRecord {
  return {
    id: "hub1",
    name: "clusters/hub1",
    labels: { region: "eu" },
    observation: { state: "enrolled", apiServer: "https://hub1.example.com" },
  };
}

function row(
  origin: ClusterRow["origin"],
  state?: string,
  overrides: Partial<ClusterRow> = {},
): ClusterRow {
  return {
    id: "hub1",
    origin,
    indexed: true,
    result: {
      name: `//${SERVICE}/clusters/hub1`,
      resourceType: `${SERVICE}/Cluster`,
      resource: {
        name: `//${SERVICE}/clusters/hub1`,
        uid: "u1",
        observation: state === undefined ? undefined : { state },
      },
    },
    ...overrides,
  } as ClusterRow;
}

function conflictResponse(
  body: Record<string, unknown>,
  status = 409,
): Response {
  return Response.json(body, { status });
}

afterEach(() => vi.unstubAllGlobals());

describe("renewClusterCredential", () => {
  it("posts the renew verb and returns the stored record", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValue(Response.json({ cluster: clusterRecord() }));
    vi.stubGlobal("fetch", fetch);

    const cluster = await renewClusterCredential(SERVICE, "hub1");

    expect(fetch).toHaveBeenCalledExactlyOnceWith(
      `/apis/${SERVICE}/v1alpha/clusters/hub1:renewCredential`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      },
    );
    expect(cluster).toEqual(clusterRecord());
  });
});

describe("credentialExpiresAt", () => {
  it("returns the expiry the renewal named", () => {
    expect(
      credentialExpiresAt({
        ...clusterRecord(),
        observation: { credentialExpiresAt: "2026-11-20T09:30:00Z" },
      }),
    ).toBe("2026-11-20T09:30:00Z");
  });

  it("returns nothing when the record names no expiry", () => {
    expect(credentialExpiresAt(clusterRecord())).toBe("");
    expect(credentialExpiresAt({ ...clusterRecord(), observation: {} })).toBe(
      "",
    );
    expect(
      credentialExpiresAt({
        ...clusterRecord(),
        observation: { credentialExpiresAt: 7 },
      }),
    ).toBe("");
  });
});

describe("withdrawCluster", () => {
  it("posts an empty body when no reason is given", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValue(Response.json({ cluster: clusterRecord() }));
    vi.stubGlobal("fetch", fetch);

    await withdrawCluster(SERVICE, "hub1");

    expect(fetch).toHaveBeenCalledExactlyOnceWith(
      `/apis/${SERVICE}/v1alpha/clusters/hub1:withdraw`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}",
      },
    );
  });

  it("trims the reason and omits a blank one", async () => {
    const fetch = vi
      .fn()
      .mockImplementation(async () =>
        Response.json({ cluster: clusterRecord() }),
      );
    vi.stubGlobal("fetch", fetch);

    await withdrawCluster(SERVICE, "hub1", { reason: "  retired  " });
    await withdrawCluster(SERVICE, "hub1", { reason: "   " });

    expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual({
      reason: "retired",
    });
    expect(JSON.parse(fetch.mock.calls[1][1].body)).toEqual({});
  });

  it("asks to leave deployments only when told to", async () => {
    const fetch = vi
      .fn()
      .mockImplementation(async () =>
        Response.json({ cluster: clusterRecord() }),
      );
    vi.stubGlobal("fetch", fetch);

    await withdrawCluster(SERVICE, "hub1", { leaveDeployments: true });
    await withdrawCluster(SERVICE, "hub1", { leaveDeployments: false });

    expect(JSON.parse(fetch.mock.calls[0][1].body)).toEqual({
      leaveDeployments: true,
    });
    expect(JSON.parse(fetch.mock.calls[1][1].body)).toEqual({});
  });

  it("surfaces the 409 deployments envelope as a ResourceApiError", async () => {
    const message =
      '2 deployment(s) still resolve to target "targets/x"; delete them first';
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        conflictResponse({
          error: message,
          reason: "deployments-resolve-to-target",
          details: {
            deployments: ["deployments/a", "deployments/b"],
            target: "targets/x",
          },
        }),
      ),
    );

    const error = await withdrawCluster(SERVICE, "hub1").catch((e) => e);

    expect(error).toBeInstanceOf(ResourceApiError);
    expect(error.status).toBe(409);
    expect(error.reason).toBe("deployments-resolve-to-target");
    expect(error.message).toBe(message);
    expect(withdrawConflict(error)).toEqual({
      deployments: ["deployments/a", "deployments/b"],
      target: "targets/x",
    });
  });
});

describe("forgetCluster", () => {
  it("deletes the record and resolves without a body", async () => {
    const fetch = vi
      .fn()
      .mockResolvedValue(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetch);

    await expect(forgetCluster(SERVICE, "hub1")).resolves.toBeUndefined();

    expect(fetch).toHaveBeenCalledExactlyOnceWith(
      `/apis/${SERVICE}/v1alpha/clusters/hub1`,
      { method: "DELETE" },
    );
  });
});

describe("manifestEnrollmentBody", () => {
  it("trims the id, address, and profile", () => {
    expect(
      manifestEnrollmentBody({
        clusterId: "  hub1 ",
        apiServer: " https://hub1.example.com ",
        profile: " read-only ",
      }),
    ).toEqual({
      clusterId: "hub1",
      apiServer: "https://hub1.example.com",
      profile: "read-only",
    });
  });

  it("leaves blank fields for the server to name as missing", () => {
    expect(
      manifestEnrollmentBody({ clusterId: " ", apiServer: "", profile: "" }),
    ).toEqual({});
  });

  it("asks for platform delivery only when it is granted", () => {
    const input = {
      clusterId: "hub1",
      apiServer: "https://hub1.example.com",
      profile: "workloads",
    };
    expect(
      manifestEnrollmentBody({ ...input, grantPlatformDelivery: true }),
    ).toEqual({ ...input, grantPlatformDelivery: true });
    expect(
      manifestEnrollmentBody({ ...input, grantPlatformDelivery: false }),
    ).not.toHaveProperty("grantPlatformDelivery");
  });
});

describe("createManifestBundle", () => {
  it("posts the bundle request and returns the manifest answer", async () => {
    const answer: EnrollmentBundleAnswer = {
      name: "enrollmentBundles/eb-1",
      bundle: { id: "eb-1", profile: "read-only", tokenLifetimeSeconds: 600 },
      manifest: "apiVersion: v1\nkind: List\n",
      instructions: ["Apply the manifest to the cluster"],
      expiresAt: "2026-11-16T09:30:00Z",
    };
    const fetch = vi.fn().mockResolvedValue(Response.json(answer));
    vi.stubGlobal("fetch", fetch);

    const created = await createManifestBundle(SERVICE, {
      clusterId: "hub1",
      apiServer: "https://hub1.example.com",
      profile: "read-only",
      grantPlatformDelivery: true,
    });

    expect(created).toEqual(answer);
    expect(fetch).toHaveBeenCalledExactlyOnceWith(
      `/apis/${SERVICE}/v1alpha/enrollmentBundles`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          clusterId: "hub1",
          apiServer: "https://hub1.example.com",
          profile: "read-only",
          grantPlatformDelivery: true,
        }),
      },
    );
  });
});

describe("revokeEnrollmentBundle", () => {
  it("deletes the bundle", async () => {
    const fetch = vi.fn().mockResolvedValue(Response.json({}));
    vi.stubGlobal("fetch", fetch);

    await expect(
      revokeEnrollmentBundle(SERVICE, "eb-1"),
    ).resolves.toBeUndefined();

    expect(fetch).toHaveBeenCalledExactlyOnceWith(
      `/apis/${SERVICE}/v1alpha/enrollmentBundles/eb-1`,
      { method: "DELETE" },
    );
  });
});

describe("withdrawConflict", () => {
  function apiError(body: unknown, status = 409): ResourceApiError {
    return new ResourceApiError(status, null, body as Record<string, unknown>);
  }

  it("returns the deployments and target of the conflict", () => {
    expect(
      withdrawConflict(
        apiError({
          reason: "deployments-resolve-to-target",
          details: { deployments: ["deployments/a"], target: "targets/x" },
        }),
      ),
    ).toEqual({ deployments: ["deployments/a"], target: "targets/x" });
  });

  it("ignores a plain error, another status, or another reason", () => {
    expect(withdrawConflict(new Error("boom"))).toBeNull();
    expect(
      withdrawConflict(
        apiError(
          {
            reason: "deployments-resolve-to-target",
            details: { deployments: ["deployments/a"], target: "targets/x" },
          },
          422,
        ),
      ),
    ).toBeNull();
    expect(
      withdrawConflict(
        apiError({
          reason: "not-enrolled",
          details: { deployments: ["deployments/a"], target: "targets/x" },
        }),
      ),
    ).toBeNull();
  });

  it("ignores details that do not list deployments and a target", () => {
    const reason = "deployments-resolve-to-target";
    expect(withdrawConflict(apiError({ reason }))).toBeNull();
    expect(
      withdrawConflict(apiError({ reason, details: { deployments: [] } })),
    ).toBeNull();
    expect(
      withdrawConflict(
        apiError({ reason, details: { deployments: [1], target: "x" } }),
      ),
    ).toBeNull();
    expect(
      withdrawConflict(
        apiError({ reason, details: { deployments: ["d"], target: 7 } }),
      ),
    ).toBeNull();
  });
});

describe("enrollmentActions", () => {
  it("offers renew and withdraw only for an enrolled cluster", () => {
    expect(enrollmentActions(row("enrolled", "enrolled"))).toEqual({
      renew: true,
      withdraw: true,
      forget: false,
    });
  });

  it("offers only forget for a withdrawn record", () => {
    expect(enrollmentActions(row("enrolled", "withdrawn"))).toEqual({
      renew: false,
      withdraw: false,
      forget: true,
    });
  });

  it("offers withdraw but not renew while a lapsed credential needs enrollment", () => {
    expect(enrollmentActions(row("enrolled", "needs-enrollment"))).toEqual({
      renew: false,
      withdraw: true,
      forget: false,
    });
  });

  it("offers nothing for a created or an observed cluster", () => {
    for (const origin of ["created", "observed"] as const) {
      expect(enrollmentActions(row(origin, "enrolled"))).toEqual({
        renew: false,
        withdraw: false,
        forget: false,
      });
    }
  });

  it("offers nothing when the record carries no state", () => {
    expect(enrollmentActions(row("enrolled"))).toEqual({
      renew: false,
      withdraw: false,
      forget: false,
    });
  });
});

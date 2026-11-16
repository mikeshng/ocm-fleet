import { describe, expect, it } from "vitest";

import {
  connectHubRequest,
  DISPLAY_NAME_MAX_BYTES,
  displayNameError,
  hubIdError,
} from "../connectHubRequest";

describe("connectHubRequest", () => {
  it("sends the address, CA certificate, and token in the fields mode", () => {
    const request = connectHubRequest({
      hubId: "  hub1 ",
      mode: "fields",
      apiServer: " https://hub:6443 ",
      caCert: "CA",
      token: " T ",
    });

    expect(request).toEqual({
      hubId: "hub1",
      apiServer: "https://hub:6443",
      caCert: "CA",
      token: "T",
    });
    expect("displayName" in request).toBe(false);
  });

  it("sends the kubeconfig in the kubeconfig mode", () => {
    const request = connectHubRequest({
      hubId: "hub1",
      mode: "kubeconfig",
      kubeconfig: "apiVersion: v1\n",
    });

    expect(request).toEqual({ hubId: "hub1", kubeconfig: "apiVersion: v1\n" });
  });

  it("includes a display name, trimmed, in both modes", () => {
    const fields = connectHubRequest({
      hubId: "hub1",
      displayName: "  Test hub  ",
      mode: "fields",
      apiServer: "https://hub:6443",
      caCert: "CA",
      token: "T",
    });
    const kubeconfig = connectHubRequest({
      hubId: "hub1",
      displayName: "Test hub",
      mode: "kubeconfig",
      kubeconfig: "apiVersion: v1\n",
    });

    expect(fields.displayName).toBe("Test hub");
    expect(kubeconfig.displayName).toBe("Test hub");
  });

  it("omits a whitespace-only display name", () => {
    const request = connectHubRequest({
      hubId: "hub1",
      displayName: "   ",
      mode: "kubeconfig",
      kubeconfig: "apiVersion: v1\n",
    });

    expect("displayName" in request).toBe(false);
  });
});

describe("displayNameError", () => {
  it("accepts a name at the byte limit", () => {
    expect(displayNameError("a".repeat(DISPLAY_NAME_MAX_BYTES))).toBeNull();
  });

  it("refuses a name over the byte limit", () => {
    expect(
      displayNameError("a".repeat(DISPLAY_NAME_MAX_BYTES + 1)),
    ).not.toBeNull();
  });

  it("counts bytes, not characters", () => {
    // 61 characters, 122 bytes: the server measures bytes.
    const name = "é".repeat(61);
    expect(name.length).toBeLessThan(DISPLAY_NAME_MAX_BYTES);
    expect(displayNameError(name)).not.toBeNull();
  });

  it("ignores surrounding whitespace when measuring", () => {
    expect(
      displayNameError(`  ${"a".repeat(DISPLAY_NAME_MAX_BYTES)}  `),
    ).toBeNull();
  });
});

describe("hubIdError", () => {
  it("accepts the ids the server takes", () => {
    expect(hubIdError("hub1")).toBeNull();
    expect(hubIdError("a")).toBeNull();
    expect(hubIdError("my-hub-2")).toBeNull();
    expect(hubIdError("a".repeat(40))).toBeNull();
    expect(hubIdError("  hub1  ")).toBeNull();
  });

  it("refuses the ids the server rejects", () => {
    expect(hubIdError("")).not.toBeNull();
    expect(hubIdError("   ")).not.toBeNull();
    expect(hubIdError("-hub")).not.toBeNull();
    expect(hubIdError("hub-")).not.toBeNull();
    expect(hubIdError("Hub1")).not.toBeNull();
    expect(hubIdError("hub_1")).not.toBeNull();
    expect(hubIdError("a".repeat(41))).not.toBeNull();
  });
});

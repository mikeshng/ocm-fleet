import { describe, expect, it } from "vitest";

import {
  enrollmentState,
  initialRolloutMode,
  NO_VALID_ADDON,
  ROLLOUT_CHOICES,
  rolloutProblem,
  rolloutRequest,
  rolloutSummary,
} from "../enrollmentRollout";

describe("rollout choices", () => {
  it("offers one cluster at a time first, then every cluster, then a Placement", () => {
    expect(ROLLOUT_CHOICES.map((choice) => choice.value)).toEqual([
      "manual",
      "every-cluster",
      "placement",
    ]);
  });

  it("starts with the hub's last choice", () => {
    expect(initialRolloutMode({ mode: "every-cluster" })).toBe("every-cluster");
    expect(initialRolloutMode({ mode: "placement" })).toBe("placement");
  });

  it("starts with one cluster at a time when nothing was made before", () => {
    expect(initialRolloutMode(undefined)).toBe("manual");
    expect(initialRolloutMode({ mode: "something-new" })).toBe("manual");
  });
});

describe("rolloutProblem", () => {
  it("asks nothing more for one cluster at a time or every cluster", () => {
    expect(rolloutProblem("manual", "", "")).toBeNull();
    expect(rolloutProblem("every-cluster", "", "")).toBeNull();
  });

  it("needs both names of a Placement", () => {
    expect(rolloutProblem("placement", "team", "")).toMatch(/namespace and/);
    expect(rolloutProblem("placement", " ", "prod")).toMatch(/namespace and/);
    expect(rolloutProblem("placement", "team", "prod")).toBeNull();
  });

  it("refuses names a hub would refuse", () => {
    expect(rolloutProblem("placement", "Team_A", "prod")).toMatch(/lowercase/);
    expect(rolloutProblem("placement", "team", "prod clusters")).toMatch(
      /lowercase/,
    );
    expect(rolloutProblem("placement", "team.a", "prod")).toMatch(/lowercase/);
  });
});

describe("rolloutRequest", () => {
  it("names a Placement only for the Placement choice", () => {
    expect(rolloutRequest("every-cluster", "team", "prod")).toEqual({
      rollout: "every-cluster",
    });
    expect(rolloutRequest("manual", "", "")).toEqual({ rollout: "manual" });
    expect(rolloutRequest("placement", " team ", " prod ")).toEqual({
      rollout: "placement",
      placementNamespace: "team",
      placementName: "prod",
    });
  });
});

describe("rolloutSummary", () => {
  it("says which clusters the add-on reaches", () => {
    expect(rolloutSummary({ mode: "every-cluster" })).toBe(
      "Every cluster of this hub",
    );
    expect(rolloutSummary({ mode: "manual" })).toMatch(/one at a time/);
    expect(
      rolloutSummary({
        mode: "placement",
        placementNamespace: "team",
        placementName: "prod",
      }),
    ).toBe("The clusters that Placement team/prod selects");
  });

  it("says when no add-on was made", () => {
    expect(rolloutSummary(undefined)).toBe("None made for this connection");
  });
});

describe("enrollmentState", () => {
  const hub = { id: "hub2", apiServer: "https://hub2:6443" };

  it("is quiet for a hub nothing was enrolled through", () => {
    expect(
      enrollmentState({ ...hub, enrollmentSecrets: 0 }).warning,
    ).toBeNull();
    expect(enrollmentState(undefined).warning).toBeNull();
  });

  it("is quiet while a secret is valid", () => {
    const state = enrollmentState({
      ...hub,
      enrolledCount: 3,
      enrolledThroughHubCount: 3,
      enrollmentSecrets: 1,
      enrollmentRollout: { mode: "every-cluster" },
    });
    expect(state.warning).toBeNull();
    expect(state.summary).toBe("Every cluster of this hub");
  });

  it("warns when clusters were enrolled through the hub and no secret is valid", () => {
    // A hub that was disconnected and connected again: its clusters are
    // still enrolled, and the add-on on the hub carries a dead secret.
    const state = enrollmentState({
      ...hub,
      enrolledCount: 3,
      enrolledThroughHubCount: 3,
      enrollmentSecrets: 0,
    });
    expect(state.warning).toMatch(/made again/);
    expect(state.warning).toMatch(/The 3 clusters that were enrolled/);
    expect(state.summary).toBe("None made for this connection");
    expect(
      enrollmentState({
        ...hub,
        enrolledThroughHubCount: 1,
        enrollmentSecrets: 0,
      }).warning,
    ).toMatch(
      /The cluster that was enrolled through its add-on cannot repair its/,
    );
  });

  it("does not repeat the last add-on's reach once its secret is gone", () => {
    // Every secret of the hub was revoked: the add-on on the hub, if it
    // is still there, enrolls nothing.
    const state = enrollmentState({
      ...hub,
      enrollmentSecrets: 0,
      enrollmentRollout: { mode: "every-cluster" },
    });
    expect(state.summary).toBe(NO_VALID_ADDON);
    expect(state.warning).toBeNull();
    const dependent = enrollmentState({
      ...hub,
      enrolledThroughHubCount: 2,
      enrollmentSecrets: 0,
      enrollmentRollout: { mode: "every-cluster" },
    });
    expect(dependent.summary).toBe(NO_VALID_ADDON);
    expect(dependent.warning).toMatch(/made again/);
  });

  it("reads a secret without a recorded choice as one cluster at a time", () => {
    const state = enrollmentState({ ...hub, enrollmentSecrets: 1 });
    expect(state.summary).toBe(rolloutSummary({ mode: "manual" }));
    expect(state.warning).toBeNull();
  });

  it("is quiet about clusters that were enrolled another way", () => {
    // Enrolled by hand, each with a secret of its own: the hub's secret
    // has nothing to do with them.
    const state = enrollmentState({
      ...hub,
      enrolledCount: 2,
      enrolledThroughHubCount: 0,
      enrollmentSecrets: 0,
    });
    expect(state.warning).toBeNull();
  });

  it("is quiet when a secret was revoked before any cluster used it", () => {
    // No warning: nothing depends on the secret. The summary still says
    // that the add-on reaches no cluster now.
    const state = enrollmentState({
      ...hub,
      enrolledCount: 0,
      enrolledThroughHubCount: 0,
      enrollmentSecrets: 0,
      enrollmentRollout: { mode: "manual" },
    });
    expect(state.warning).toBeNull();
    expect(state.summary).toBe(NO_VALID_ADDON);
  });
});

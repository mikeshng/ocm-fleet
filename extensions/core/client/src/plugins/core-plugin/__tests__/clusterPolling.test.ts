import { describe, expect, it } from "vitest";

import {
  FAILED_POLL_LIMIT,
  pollingPaused,
  shouldPoll,
} from "../clusterPolling";

describe("cluster polling", () => {
  it("polls while a cluster is transient and the reads succeed", () => {
    expect(shouldPoll(true, 0)).toBe(true);
    expect(shouldPoll(true, FAILED_POLL_LIMIT - 1)).toBe(true);
    expect(pollingPaused(true, 0)).toBe(false);
  });

  it("stops after three failed silent reads and says it is paused", () => {
    expect(shouldPoll(true, FAILED_POLL_LIMIT)).toBe(false);
    expect(pollingPaused(true, FAILED_POLL_LIMIT)).toBe(true);
    expect(pollingPaused(true, FAILED_POLL_LIMIT + 1)).toBe(true);
  });

  it("does not poll, or claim to be paused, when no cluster is transient", () => {
    expect(shouldPoll(false, 0)).toBe(false);
    expect(shouldPoll(false, FAILED_POLL_LIMIT)).toBe(false);
    expect(pollingPaused(false, FAILED_POLL_LIMIT)).toBe(false);
  });
});

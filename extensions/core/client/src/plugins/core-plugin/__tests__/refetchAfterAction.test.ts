import { describe, expect, it, vi } from "vitest";

import { refetchAfterAction } from "../refetchAfterAction";

describe("refetchAfterAction", () => {
  it("says nothing when the record is read again", async () => {
    const refresh = vi.fn<() => Promise<boolean>>().mockResolvedValue(true);
    const report = vi.fn();
    await refetchAfterAction(refresh, report, "the read failed");
    expect(refresh).toHaveBeenCalledTimes(1);
    expect(report).not.toHaveBeenCalled();
  });

  it("reports the sentence when the read fails, since the action did happen", async () => {
    // A read that failed before reaching the server, and one the server refused, both reach
    // the page as a false answer; either way the row is out of date.
    const refresh = vi.fn<() => Promise<boolean>>().mockResolvedValue(false);
    const report = vi.fn();
    await refetchAfterAction(refresh, report, "the read failed");
    expect(report).toHaveBeenCalledTimes(1);
    expect(report).toHaveBeenCalledWith("the read failed");
  });

  it("leaves a thrown read to the caller's own failure sentence", async () => {
    const thrown = new Error("request failed");
    const refresh = vi.fn<() => Promise<boolean>>().mockRejectedValue(thrown);
    const report = vi.fn();
    await expect(
      refetchAfterAction(refresh, report, "the read failed"),
    ).rejects.toBe(thrown);
    expect(report).not.toHaveBeenCalled();
  });
});

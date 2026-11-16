import { describe, expect, it, vi } from "vitest";

import { withNetworkRetry } from "../networkRetry";

const networkFailure = () => new TypeError("Failed to fetch");

describe("withNetworkRetry", () => {
  it("returns the first result without another attempt", async () => {
    const run = vi.fn<() => Promise<string>>().mockResolvedValue("answer");
    await expect(withNetworkRetry(run, 0)).resolves.toBe("answer");
    expect(run).toHaveBeenCalledTimes(1);
  });

  it("tries a read again when the request never reached the server", async () => {
    const run = vi
      .fn<() => Promise<string>>()
      .mockRejectedValueOnce(networkFailure())
      .mockResolvedValueOnce("answer");
    await expect(withNetworkRetry(run, 0)).resolves.toBe("answer");
    expect(run).toHaveBeenCalledTimes(2);
  });

  it("gives up after the second network failure", async () => {
    const run = vi
      .fn<() => Promise<string>>()
      .mockRejectedValue(networkFailure());
    await expect(withNetworkRetry(run, 0)).rejects.toThrow("Failed to fetch");
    expect(run).toHaveBeenCalledTimes(2);
  });

  it("returns an answer from the server at once, even a refusal", async () => {
    const refusal = new Error("Query API error 401");
    const run = vi.fn<() => Promise<string>>().mockRejectedValue(refusal);
    await expect(withNetworkRetry(run, 0)).rejects.toBe(refusal);
    expect(run).toHaveBeenCalledTimes(1);
  });
});

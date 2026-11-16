import { describe, expect, it } from "vitest";

import { deleteConfirmationMatches } from "../deleteConfirmation";

describe("the delete confirmation", () => {
  it("accepts the cluster's name, typed exactly", () => {
    expect(deleteConfirmationMatches("hub", "hub")).toBe(true);
    expect(deleteConfirmationMatches("hub1.ext", "hub1.ext")).toBe(true);
  });

  it("accepts a name pasted with surrounding whitespace", () => {
    expect(deleteConfirmationMatches("  hub1.ext\n", "hub1.ext")).toBe(true);
  });

  it("refuses a prefix, a different name, and a different case", () => {
    expect(deleteConfirmationMatches("hu", "hub")).toBe(false);
    expect(deleteConfirmationMatches("hub1", "hub1.ext")).toBe(false);
    expect(deleteConfirmationMatches("spoke", "hub")).toBe(false);
    expect(deleteConfirmationMatches("Hub", "hub")).toBe(false);
  });

  it("refuses an empty field", () => {
    expect(deleteConfirmationMatches("", "hub")).toBe(false);
    expect(deleteConfirmationMatches("   ", "hub")).toBe(false);
  });
});

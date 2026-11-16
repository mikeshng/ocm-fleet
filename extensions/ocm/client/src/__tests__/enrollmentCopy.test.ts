import { describe, expect, it } from "vitest";

import { COPIED, copyLine } from "../enrollmentCopy";

describe("copyLine", () => {
  it("names what each copy icon copies", () => {
    expect(copyLine("bundle", null)).toContain("bundle's id");
    expect(copyLine("manifest", null)).toContain("add-on manifest");
    expect(copyLine("example", null)).toContain("example for one cluster");
  });

  it("confirms only the box that was just copied", () => {
    expect(copyLine("manifest", "manifest")).toBe(COPIED);
    expect(copyLine("bundle", "manifest")).not.toBe(COPIED);
    expect(copyLine("example", "manifest")).not.toBe(COPIED);
  });
});

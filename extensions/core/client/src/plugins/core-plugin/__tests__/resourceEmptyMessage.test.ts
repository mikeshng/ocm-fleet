import { describe, expect, it } from "vitest";

import { resourceEmptyMessage } from "../useClusterResources";

describe("resourceEmptyMessage", () => {
  it("says nothing is indexed for this cluster when one is in view", () => {
    expect(resourceEmptyMessage(0, "demo", "pods")).toBe(
      "No pods are currently indexed for this cluster.",
    );
  });

  it("says nothing is indexed on any cluster when none is in view", () => {
    expect(resourceEmptyMessage(0, undefined, "namespaces")).toBe(
      "No namespaces are currently indexed on any cluster.",
    );
  });

  it("blames the filters when resources exist but none match", () => {
    expect(resourceEmptyMessage(3, "demo", "pods")).toBe(
      "No pods match the current filter criteria.",
    );
    expect(resourceEmptyMessage(3, undefined, "namespaces")).toBe(
      "No namespaces match the current filter criteria.",
    );
  });
});

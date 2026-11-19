import { describe, expect, it } from "vitest";

import {
  celResourceTypeFilter,
  completionOf,
  resourceTypesToCheck,
} from "../liveCompletion";

describe("completionOf", () => {
  it("is complete from stored progress alone", () => {
    expect(
      completionOf({ id: "ocm-hub-connect" }, { "ocm-hub-connect": true }, {}),
    ).toBe(true);
  });

  it("is complete when the declared resource exists", () => {
    expect(
      completionOf(
        {
          id: "ocm-hub-connect",
          completeWhenResourceExists: "ocm.open-cluster-management.io/Hub",
        },
        {},
        { "ocm.open-cluster-management.io/Hub": true },
      ),
    ).toBe(true);
  });

  it("is not complete when neither progress nor the resource says so", () => {
    expect(
      completionOf(
        {
          id: "ocm-hub-connect",
          completeWhenResourceExists: "ocm.open-cluster-management.io/Hub",
        },
        { "ocm-hub-connect": false },
        { "ocm.open-cluster-management.io/Hub": false },
      ),
    ).toBe(false);
  });

  it("does not read a resource for an action that declares none", () => {
    expect(
      completionOf(
        { id: "kind-connect" },
        {},
        { "kind.open-cluster-management.io/Cluster": true },
      ),
    ).toBe(false);
  });
});

describe("resourceTypesToCheck", () => {
  it("names each declared type once, in order", () => {
    expect(
      resourceTypesToCheck([
        { completeWhenResourceExists: "b.example.io/Kind" },
        {},
        { completeWhenResourceExists: "a.example.io/Kind" },
        { completeWhenResourceExists: "b.example.io/Kind" },
      ]),
    ).toEqual(["b.example.io/Kind", "a.example.io/Kind"]);
  });
});

describe("celResourceTypeFilter", () => {
  it("quotes the type as a CEL string literal", () => {
    expect(celResourceTypeFilter("ocm.open-cluster-management.io/Hub")).toBe(
      'resourceType == "ocm.open-cluster-management.io/Hub"',
    );
  });

  it("escapes a quote rather than ending the literal", () => {
    expect(celResourceTypeFilter('a.example.io/Kind" || true')).toBe(
      'resourceType == "a.example.io/Kind\\" || true"',
    );
  });
});

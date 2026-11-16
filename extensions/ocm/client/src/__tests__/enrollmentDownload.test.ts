import { describe, expect, it } from "vitest";

import { DOWNLOAD_FILENAMES, enrollmentDownload } from "../enrollmentDownload";

const MANIFEST =
  "apiVersion: addon.open-cluster-management.io/v1alpha1\nkind: AddOnTemplate\n";
const EXAMPLE =
  "apiVersion: addon.open-cluster-management.io/v1alpha1\nkind: ManagedClusterAddOn\n";

describe("the enrollment dialog's downloads", () => {
  it("saves the manifest under its own name, with a final newline", () => {
    expect(enrollmentDownload("manifest", MANIFEST, EXAMPLE)).toEqual({
      filename: "ocm-fleet-enrollment-addon.yaml",
      text: MANIFEST,
    });
    expect(
      enrollmentDownload("manifest", "kind: AddOnTemplate", EXAMPLE),
    ).toEqual({
      filename: "ocm-fleet-enrollment-addon.yaml",
      text: "kind: AddOnTemplate\n",
    });
  });

  it("saves the example under a different name", () => {
    expect(enrollmentDownload("example", MANIFEST, EXAMPLE)).toEqual({
      filename: "ocm-fleet-enrollment-enable.yaml",
      text: EXAMPLE,
    });
    expect(enrollmentDownload("example", MANIFEST, EXAMPLE)?.text).not.toBe(
      enrollmentDownload("manifest", MANIFEST, EXAMPLE)?.text,
    );
  });

  it("has no file for the bundle's id", () => {
    expect(enrollmentDownload("bundle", MANIFEST, EXAMPLE)).toBeNull();
    expect(DOWNLOAD_FILENAMES.bundle).toBeNull();
  });

  it("has no file when the dialog holds no text yet", () => {
    expect(enrollmentDownload("manifest", "", EXAMPLE)).toBeNull();
    expect(enrollmentDownload("example", MANIFEST, "")).toBeNull();
  });
});

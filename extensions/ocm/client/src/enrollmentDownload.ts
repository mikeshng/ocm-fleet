import type { EnrollmentCopyBox } from "./enrollmentCopy";

/**
 * The two boxes of the enrollment dialog that hold a file the hub's administrator applies,
 * and the names they are saved under. The bundle id is not a file.
 */
export const DOWNLOAD_FILENAMES: Record<EnrollmentCopyBox, string | null> = {
  bundle: null,
  manifest: "ocm-fleet-enrollment-addon.yaml",
  example: "ocm-fleet-enrollment-enable.yaml",
};

export interface EnrollmentDownload {
  filename: string;
  text: string;
}

/**
 * What the Download link under a box saves: the same text its copy icon copies, under the
 * name above, ending in a newline so an editor opens it as a file and not as a fragment.
 */
export function enrollmentDownload(
  box: EnrollmentCopyBox,
  manifest: string,
  example: string,
): EnrollmentDownload | null {
  const filename = DOWNLOAD_FILENAMES[box];
  if (!filename) return null;
  const text = box === "manifest" ? manifest : example;
  if (!text) return null;
  return { filename, text: text.endsWith("\n") ? text : text + "\n" };
}

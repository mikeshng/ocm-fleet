/**
 * What each copy control of the enrollment dialog copies, and the confirmation shown after a
 * copy. The dialog shows one line of text under each copy box, so that the copy icons are told
 * apart by words and a copy is confirmed on the screen.
 */
export type EnrollmentCopyBox = "bundle" | "manifest" | "example";

const HINTS: Record<EnrollmentCopyBox, string> = {
  bundle: "The copy icon copies the bundle's id.",
  manifest: "The copy icon copies the add-on manifest.",
  example: "The copy icon copies the example for one cluster.",
};

export const COPIED = "Copied.";

/** The line under a copy box: its hint, or the confirmation when it was the box just copied. */
export function copyLine(
  box: EnrollmentCopyBox,
  copied: EnrollmentCopyBox | null,
): string {
  return copied === box ? COPIED : HINTS[box];
}

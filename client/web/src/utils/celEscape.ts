/**
 * escapeCelString escapes a value before it is put inside a CEL string
 * literal: the backslash first, so that the escapes added for quotes are
 * not themselves doubled, then the double quote.
 */
export function escapeCelString(value: string): string {
  return value.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

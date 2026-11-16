import { describe, expect, it } from "vitest";

import { escapeCelString } from "../celEscape";

describe("escapeCelString", () => {
  it("leaves plain text as it is", () => {
    expect(escapeCelString("clusters/prod-1")).toBe("clusters/prod-1");
  });

  it("escapes a double quote", () => {
    expect(escapeCelString('a"b')).toBe('a\\"b');
  });

  it("escapes a backslash", () => {
    expect(escapeCelString(String.raw`a\b`)).toBe(String.raw`a\\b`);
  });

  it("escapes a backslash before a quote first, then the quote", () => {
    expect(escapeCelString(String.raw`a\"b`)).toBe(String.raw`a\\\"b`);
  });
});

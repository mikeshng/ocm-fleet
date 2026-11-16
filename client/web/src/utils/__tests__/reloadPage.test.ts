import { describe, expect, it, vi } from "vitest";

import { reloadPage } from "../reloadPage";

describe("reloadPage", () => {
  it("reloads the location of the window it is given", () => {
    const reload = vi.fn();
    reloadPage({ location: { reload } });
    expect(reload).toHaveBeenCalledTimes(1);
  });
});

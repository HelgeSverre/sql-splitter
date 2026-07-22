import { describe, expect, test } from "bun:test";
import { getOgImageSlug } from "./ogPath";

describe("getOgImageSlug", () => {
  test.each([
    ["index", "index"],
    ["getting-started/index", "getting-started"],
    ["commands/split", "commands/split"],
  ])("maps %s to %s", (contentId, expected) => {
    expect(getOgImageSlug(contentId)).toBe(expected);
  });
});

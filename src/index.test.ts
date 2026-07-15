import { fileURLToPath } from "node:url";

import { discoverAndLoadExtensions } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "@effect/vitest";
import { Effect } from "effect";

it.describe("root extension", () => {
  it.effect("registers the subagent tool", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", undefined);
    vi.stubEnv("CMUX_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111");
    vi.stubEnv("CMUX_SURFACE_ID", "22222222-2222-4222-8222-222222222222");

    return Effect.gen(function* () {
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );

      expect(result.errors).toEqual([]);
      expect(result.extensions).toHaveLength(1);
      expect(result.extensions[0]?.tools.has("subagent")).toBe(true);
    }).pipe(Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())));
  });

  it.effect("does not register inside a subagent Pi process", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_review-api");

    return Effect.gen(function* () {
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );

      expect(result.errors).toEqual([]);
      expect(result.extensions).toHaveLength(1);
      expect(result.extensions[0]?.tools.size).toBe(0);
      expect(result.extensions[0]?.handlers.size).toBe(0);
    }).pipe(Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())));
  });
});

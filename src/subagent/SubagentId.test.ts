import { expect, it } from "@effect/vitest";
import { Effect, Random } from "effect";

import { decodeSubagentId, generateSubagentId } from "./SubagentId.ts";

it.describe("generateSubagentId", () => {
  it.effect("generates a branded subagent ID from a title", () =>
    Effect.gen(function* () {
      const subagentId = yield* generateSubagentId("  Café / STORE API! ").pipe(
        Random.withSeed("subagent-id"),
      );

      expect(subagentId).toMatch(/^sa_[a-z0-9]{8}_cafe-store-api$/);
      expect(yield* decodeSubagentId(subagentId)).toBe(subagentId);
    }),
  );

  it.effect("uses a deterministic random segment when seeded", () =>
    Effect.gen(function* () {
      const first = yield* generateSubagentId("Review").pipe(Random.withSeed("subagent-id"));
      const second = yield* generateSubagentId("Review").pipe(Random.withSeed("subagent-id"));

      expect(first).toBe(second);
    }),
  );

  it.effect("falls back when a title has no ASCII slug", () =>
    Effect.gen(function* () {
      const subagentId = yield* generateSubagentId("Σμιθ").pipe(Random.withSeed("subagent-id"));

      expect(subagentId).toMatch(/^sa_[a-z0-9]{8}_subagent$/);
    }),
  );

  it.effect("limits the slug to 48 characters", () =>
    Effect.gen(function* () {
      const subagentId = yield* generateSubagentId("a".repeat(80)).pipe(
        Random.withSeed("subagent-id"),
      );

      expect(subagentId.split("_")[2]).toHaveLength(48);
    }),
  );

  it.effect("rejects subagent IDs longer than the generated format", () =>
    Effect.gen(function* () {
      const error = yield* decodeSubagentId(`sa_12345678_${"a".repeat(49)}`).pipe(Effect.flip);

      expect(error).toBeDefined();
    }),
  );
});

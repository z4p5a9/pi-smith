import { it } from "@effect/vitest";
import { Effect } from "effect";

import { decodeSubagentId } from "./SubagentId.ts";
import { SubagentPool } from "./SubagentPool.ts";

it.describe("SubagentPool", () => {
  it.effect("submits a subagent spec", () =>
    Effect.gen(function* () {
      const pool = yield* SubagentPool;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* pool.submit(subagentId, { title: "Review API" });
    }).pipe(Effect.provide(SubagentPool.layer)),
  );
});

import { expect, it } from "@effect/vitest";
import { Effect } from "effect";

import { SubagentAlreadyExistsError, SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import { decodeSubagentId } from "./SubagentId.ts";

it.describe("SubagentCheckpoint", () => {
  it.effect("puts a new subagent record", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* checkpoint.put({ subagentId, status: "queued", title: "Review API" });
    }).pipe(Effect.provide(SubagentCheckpoint.layer)),
  );

  it.effect("rejects an existing subagent ID", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const record = { subagentId, status: "queued" as const, title: "Review API" };

      yield* checkpoint.put(record);
      const error = yield* checkpoint.put(record).pipe(Effect.flip);

      expect(error).toBeInstanceOf(SubagentAlreadyExistsError);
      expect(error.subagentId).toBe(subagentId);
    }).pipe(Effect.provide(SubagentCheckpoint.layer)),
  );
});

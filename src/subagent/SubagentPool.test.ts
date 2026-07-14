import { expect, it } from "@effect/vitest";
import { Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";

import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import { decodeSubagentId } from "./SubagentId.ts";
import { SubagentPool } from "./SubagentPool.ts";

it.describe("SubagentPool", () => {
  it.effect("submits a subagent spec", () =>
    Effect.gen(function* () {
      const pool = yield* SubagentPool;
      const checkpoint = yield* SubagentCheckpoint;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* checkpoint.put({ subagentId, status: "queued", title: "Review API" });
      yield* pool.submit(subagentId, { title: "Review API" });
    }).pipe(Effect.provide(SubagentPool.layer)),
  );

  it.effect("continues consuming after a checkpoint update failure", () =>
    Effect.gen(function* () {
      const pool = yield* SubagentPool;
      const checkpoint = yield* SubagentCheckpoint;
      const missingSubagentId = yield* decodeSubagentId("sa_12345678_missing");
      const probeSubagentId = yield* decodeSubagentId("sa_87654321_probe");

      for (let index = 0; index < 10; index++) {
        yield* pool.submit(missingSubagentId, { title: "Missing" });
      }

      yield* checkpoint.put({
        subagentId: probeSubagentId,
        status: "queued",
        title: "Probe",
      });
      yield* pool.submit(probeSubagentId, { title: "Probe" });

      const assertion = yield* Effect.gen(function* () {
        let record = yield* checkpoint.get(probeSubagentId);

        while (record.status !== "starting") {
          yield* Effect.sleep("1 millis");
          record = yield* checkpoint.get(probeSubagentId);
        }

        return record;
      }).pipe(Effect.timeout("1 second"), Effect.forkChild);

      yield* TestClock.adjust("1 second");

      const record = yield* Fiber.join(assertion);
      expect(record.status).toBe("starting");
    }).pipe(Effect.provide(SubagentPool.layer)),
  );
});

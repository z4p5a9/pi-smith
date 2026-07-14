import { expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";

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

      const record = yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((currentRecord) => currentRecord.status === "running"),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );
      expect(record.status).toBe("running");
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

      const record = yield* checkpoint.changes(probeSubagentId).pipe(
        Stream.filter((currentRecord) => currentRecord.status === "running"),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );
      expect(record.status).toBe("running");
    }).pipe(Effect.provide(SubagentPool.layer)),
  );

  it.effect("continues consuming after a duplicate supervisor start", () =>
    Effect.gen(function* () {
      const pool = yield* SubagentPool;
      const checkpoint = yield* SubagentCheckpoint;
      const duplicateSubagentId = yield* decodeSubagentId("sa_12345678_duplicate");
      const probeSubagentId = yield* decodeSubagentId("sa_87654321_probe");

      yield* checkpoint.put({
        subagentId: duplicateSubagentId,
        status: "queued",
        title: "Duplicate",
      });
      yield* pool.submit(duplicateSubagentId, { title: "Duplicate" });

      yield* checkpoint.changes(duplicateSubagentId).pipe(
        Stream.filter((record) => record.status === "running"),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
        Effect.asVoid,
      );

      for (let index = 0; index < 10; index++) {
        yield* pool.submit(duplicateSubagentId, { title: "Duplicate" });
      }

      yield* checkpoint.put({
        subagentId: probeSubagentId,
        status: "queued",
        title: "Probe",
      });
      yield* pool.submit(probeSubagentId, { title: "Probe" });

      const record = yield* checkpoint.changes(probeSubagentId).pipe(
        Stream.filter((currentRecord) => currentRecord.status === "running"),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );
      expect(record.status).toBe("running");
    }).pipe(Effect.provide(SubagentPool.layer)),
  );
});

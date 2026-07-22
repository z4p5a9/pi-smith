import { expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";

import {
  SubagentAlreadyExistsError,
  SubagentCheckpoint,
  SubagentNotFoundError,
} from "./SubagentCheckpoint.ts";
import { decodeSubagentId } from "./SubagentId.ts";

it.describe("SubagentCheckpoint", () => {
  it.effect("puts a new subagent record", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* checkpoint.put({
        subagentId,
        status: "queued",
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral" as const,
      });
    }).pipe(Effect.provide(SubagentCheckpoint.layer)),
  );

  it.effect("rejects an existing subagent ID", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const record = {
        subagentId,
        status: "queued" as const,
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral" as const,
      };

      yield* checkpoint.put(record);
      const error = yield* checkpoint.put(record).pipe(Effect.flip);

      expect(error).toBeInstanceOf(SubagentAlreadyExistsError);
      expect(error.subagentId).toBe(subagentId);
    }).pipe(Effect.provide(SubagentCheckpoint.layer)),
  );

  it.effect("updates an existing subagent record with its terminal event", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* checkpoint.put({
        subagentId,
        status: "queued",
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral" as const,
      });
      yield* checkpoint.update(subagentId, {
        status: "exited",
        latestEvent: { kind: "message", content: "Task complete." },
      });

      expect(yield* checkpoint.get(subagentId)).toMatchObject({
        status: "exited",
        latestEvent: { kind: "message", content: "Task complete." },
      });
    }).pipe(Effect.provide(SubagentCheckpoint.layer)),
  );

  it.effect("rejects an update for an unknown subagent ID", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const error = yield* checkpoint.update(subagentId, { status: "starting" }).pipe(Effect.flip);

      expect(error).toBeInstanceOf(SubagentNotFoundError);
      expect(error.subagentId).toBe(subagentId);
    }).pipe(Effect.provide(SubagentCheckpoint.layer)),
  );

  it.effect("gets an existing subagent record", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const record = {
        subagentId,
        status: "queued" as const,
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral" as const,
      };

      yield* checkpoint.put(record);

      expect(yield* checkpoint.get(subagentId)).toEqual(record);
    }).pipe(Effect.provide(SubagentCheckpoint.layer)),
  );

  it.effect("rejects a get for an unknown subagent ID", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const error = yield* checkpoint.get(subagentId).pipe(Effect.flip);

      expect(error).toBeInstanceOf(SubagentNotFoundError);
      expect(error.subagentId).toBe(subagentId);
    }).pipe(Effect.provide(SubagentCheckpoint.layer)),
  );

  it.effect("checks whether a subagent record exists", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      expect(yield* checkpoint.has(subagentId)).toBe(false);

      yield* checkpoint.put({
        subagentId,
        status: "queued",
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral" as const,
      });

      expect(yield* checkpoint.has(subagentId)).toBe(true);
    }).pipe(Effect.provide(SubagentCheckpoint.layer)),
  );

  it.effect("emits the current subagent record", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const record = {
        subagentId,
        status: "queued" as const,
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral" as const,
      };

      yield* checkpoint.put(record);

      const emitted = yield* checkpoint
        .changes(subagentId)
        .pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(emitted).toEqual(record);
    }).pipe(Effect.provide(SubagentCheckpoint.layer)),
  );

  it.effect("rejects changes for an unknown subagent ID", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const error = yield* checkpoint.changes(subagentId).pipe(Stream.runHead, Effect.flip);

      expect(error).toBeInstanceOf(SubagentNotFoundError);
      expect(error.subagentId).toBe(subagentId);
    }).pipe(Effect.provide(SubagentCheckpoint.layer)),
  );
});

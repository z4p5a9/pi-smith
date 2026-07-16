import { expect, it } from "@effect/vitest";
import { Effect, Fiber, Layer, Schema } from "effect";
import { TestClock } from "effect/testing";

import { SubagentBridgeDisconnectedError } from "./SubagentBridge.ts";
import { decodeSubagentId } from "./SubagentId.ts";
import { spawnSubagentProcess, SubagentProcessStartTimeoutError } from "./SubagentProcess.ts";
import { TestSubagentBridge } from "../testing/TestSubagentBridge.ts";
import { TestSubagentHost } from "../testing/TestSubagentHost.ts";

it.describe("spawnSubagentProcess", () => {
  it.effect("starts the host after listening and returns after readiness is acknowledged", () =>
    Effect.gen(function* () {
      const testBridge = yield* TestSubagentBridge;
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* testHost.stub([{ hostId: "test-host" }]);

      const process = yield* spawnSubagentProcess(subagentId, {
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      expect(yield* process.status).toBe("running");
      expect(yield* testBridge.calls).toEqual([
        { operation: "listen", subagentId },
        { operation: "connect", subagentId },
      ]);

      yield* testBridge.disconnect(subagentId);

      const error = yield* process.await.pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeDisconnectedError)(error)).toBe(true);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(TestSubagentHost.layer.pipe(Layer.provideMerge(TestSubagentBridge.layer))),
    ),
  );

  it.effect("times out and releases acquired resources", () =>
    Effect.gen(function* () {
      const testBridge = yield* TestSubagentBridge;
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* testBridge.block(subagentId);
      yield* testHost.stub([{ hostId: "test-host" }]);

      const result = yield* spawnSubagentProcess(subagentId, {
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
      }).pipe(Effect.scoped, Effect.flip, Effect.forkChild({ startImmediately: true }));

      yield* TestClock.adjust("30 seconds");

      const error = yield* Fiber.join(result);

      expect(Schema.is(SubagentProcessStartTimeoutError)(error)).toBe(true);
      expect(yield* testHost.active).toEqual([]);
      expect(yield* testBridge.isListening(subagentId)).toBe(false);
      expect(yield* testBridge.isConnected(subagentId)).toBe(false);
      yield* testHost.verify;
    }).pipe(
      Effect.provide(TestSubagentHost.layer.pipe(Layer.provideMerge(TestSubagentBridge.layer))),
    ),
  );
});

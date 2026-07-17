import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Layer, Schema, Scope } from "effect";
import { TestClock } from "effect/testing";

import * as SubagentBridge from "./SubagentBridge.ts";
import { SubagentBridgeDisconnectedError } from "./SubagentBridge.ts";
import { decodeSubagentId } from "./SubagentId.ts";
import { spawnSubagentProcess, SubagentProcessStartTimeoutError } from "./SubagentProcess.ts";
import { TestSubagentHost } from "../testing/TestSubagentHost.ts";
import { layer as unixSocketSubagentBridgeTransportLayer } from "./UnixSocketSubagentBridgeTransport.ts";

it.describe("spawnSubagentProcess", () => {
  it.effect("starts the host after listening and returns after hello is acknowledged", () =>
    Effect.gen(function* () {
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_process-start");
      const parentScope = yield* Scope.Scope;

      yield* testHost.stub([{ hostId: "test-host" }]);

      const started = yield* spawnSubagentProcess(subagentId, {
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
      }).pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);

      const childScope = yield* Scope.fork(parentScope);
      yield* SubagentBridge.connect(subagentId).pipe(Scope.provide(childScope));
      const process = yield* Fiber.join(started);

      expect(yield* process.status).toBe("running");

      yield* Scope.close(childScope, Exit.void);

      const error = yield* process.await.pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeDisconnectedError)(error)).toBe(true);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.merge(
          TestSubagentHost.layer,
          unixSocketSubagentBridgeTransportLayer.pipe(Layer.provide(NodeFileSystem.layer)),
        ),
      ),
    ),
  );

  it.effect("times out and releases acquired resources", () =>
    Effect.gen(function* () {
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_process-timeout");

      yield* testHost.stub([{ hostId: "test-host" }]);

      const result = yield* spawnSubagentProcess(subagentId, {
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
      }).pipe(Effect.scoped, Effect.flip, Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);
      yield* TestClock.adjust("30 seconds");

      const error = yield* Fiber.join(result);

      expect(Schema.is(SubagentProcessStartTimeoutError)(error)).toBe(true);
      expect(yield* testHost.active).toEqual([]);
      yield* SubagentBridge.listen(subagentId).pipe(Effect.scoped);
      yield* testHost.verify;
    }).pipe(
      Effect.provide(
        Layer.merge(
          TestSubagentHost.layer,
          unixSocketSubagentBridgeTransportLayer.pipe(Layer.provide(NodeFileSystem.layer)),
        ),
      ),
    ),
  );
});

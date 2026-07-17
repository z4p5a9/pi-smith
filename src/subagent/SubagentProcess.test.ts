import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Layer, Schema, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";

import { SubagentBridge, SubagentBridgeDisconnectedError } from "./SubagentBridge.ts";
import { decodeSubagentId } from "./SubagentId.ts";
import { spawnSubagentProcess, SubagentProcessStartTimeoutError } from "./SubagentProcess.ts";
import { TestSubagentHost } from "../testing/TestSubagentHost.ts";
import { layer as unixSocketSubagentBridgeTransportLayer } from "./UnixSocketSubagentBridgeTransport.ts";

it.describe("spawnSubagentProcess", () => {
  it.effect("starts the host after listening and returns after readiness is acknowledged", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
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
      const child = yield* bridge.connect(subagentId).pipe(Scope.provide(childScope));
      const sentReady = yield* child
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const process = yield* Fiber.join(started);
      const ready = yield* process.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(yield* process.status).toBe("running");
      expect(ready.event).toEqual({ kind: "ready" });

      yield* ready.acknowledge;
      yield* Fiber.join(sentReady);

      yield* Scope.close(childScope, Exit.void);

      const error = yield* process.await.pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeDisconnectedError)(error)).toBe(true);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.merge(
          TestSubagentHost.layer,
          SubagentBridge.layer.pipe(
            Layer.provide(unixSocketSubagentBridgeTransportLayer),
            Layer.provide(NodeFileSystem.layer),
          ),
        ),
      ),
    ),
  );

  it.effect("times out and releases acquired resources", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
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
      yield* bridge.listen(subagentId).pipe(Effect.scoped);
      yield* testHost.verify;
    }).pipe(
      Effect.provide(
        Layer.merge(
          TestSubagentHost.layer,
          SubagentBridge.layer.pipe(
            Layer.provide(unixSocketSubagentBridgeTransportLayer),
            Layer.provide(NodeFileSystem.layer),
          ),
        ),
      ),
    ),
  );
});

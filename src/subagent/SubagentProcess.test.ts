import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Schema, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";

import { TestHost } from "../testing/TestHost.ts";
import { SubagentBridge } from "../bridge/Bridge.ts";
import * as UnixSocketBridgeTransport from "../bridge/unix/UnixSocketBridgeTransport.ts";
import { SubagentHarness } from "../harness/Harness.ts";
import { decodeSubagentId } from "./SubagentId.ts";
import { spawnSubagentProcess, SubagentProcessStartTimeoutError } from "./SubagentProcess.ts";

it.describe("spawnSubagentProcess", () => {
  it.effect("exposes Bridge event deliveries separately from the session lifetime", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const testHost = yield* TestHost;
      const parentScope = yield* Scope.Scope;
      const processScope = yield* Scope.fork(parentScope);
      const delivered = yield* Deferred.make<void>();
      const subagentId = yield* decodeSubagentId("sa_12345678_process-start");

      yield* testHost.stub([null]);

      const spawning = yield* spawnSubagentProcess(subagentId, {
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
      }).pipe(Scope.provide(processScope), Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* bridge.connect(subagentId);
      const process = yield* Fiber.join(spawning);
      const sending = yield* child
        .sendEvent({ kind: "completed", report: "Task complete." })
        .pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* Deferred.isDone(delivered)).toBe(false);

      const awaiting = yield* Effect.all(
        [
          process.await,
          process.events.pipe(
            Stream.runForEach((delivery) =>
              Deferred.succeed(delivered, undefined).pipe(
                Effect.andThen(delivery.acknowledge),
                Effect.asVoid,
              ),
            ),
          ),
        ],
        { concurrency: "unbounded", discard: true },
      ).pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(delivered);

      yield* Fiber.join(sending);
      yield* child.close;
      yield* Fiber.join(awaiting);
      yield* Scope.close(processScope, Exit.void);

      expect(yield* testHost.active).toEqual([]);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.merge(TestHost.layer, SubagentBridge.layer).pipe(
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () =>
                  Effect.succeed({
                    executable: "pi",
                    args: ["--name", "Review API", "Complete the task."],
                    cwd: "/worktree",
                  }),
              }),
            ),
          ),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("times out and releases acquired resources", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const testHost = yield* TestHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_process-timeout");

      yield* testHost.stub([null]);

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
        Layer.merge(TestHost.layer, SubagentBridge.layer).pipe(
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () =>
                  Effect.succeed({
                    executable: "pi",
                    args: ["--name", "Review API", "Complete the task."],
                    cwd: "/worktree",
                  }),
              }),
            ),
          ),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );
});

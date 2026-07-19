import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Layer, Scope, Stream } from "effect";

import { TestHost } from "../testing/TestHost.ts";
import { SubagentBridge } from "../host/bridge/Bridge.ts";
import * as UnixSocketBridgeTransport from "../host/bridge/unix/UnixSocketBridgeTransport.ts";
import { SubagentHarness } from "../harness/Harness.ts";
import { SubagentHostStartError } from "../host/Host.ts";
import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import { decodeSubagentId } from "./SubagentId.ts";
import { makeSubagentProcess } from "./SubagentProcess.ts";

it.describe("SubagentProcess", () => {
  it.effect("accepts the first message, emits it, and resolves exited", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_process-message");

      yield* testHost.stub([null]);
      yield* checkpoint.put({
        subagentId,
        status: "queued",
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      const process = yield* makeSubagentProcess(subagentId, {
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });
      const running = yield* process.run.pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* bridge.connect(subagentId);

      yield* child.sendEvent({ kind: "message", content: "Task complete." });

      expect(yield* process.await).toEqual({ kind: "exited" });
      expect(yield* process.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption))).toEqual(
        { kind: "message", content: "Task complete." },
      );
      expect(yield* checkpoint.get(subagentId)).toMatchObject({
        status: "completed",
        latestEvent: { kind: "message", content: "Task complete." },
      });

      yield* Fiber.join(running);

      expect(yield* testHost.active).toEqual([]);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        TestHost.layer.pipe(
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("resolves killed when interrupted while running", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_process-killed");

      yield* testHost.stub([null]);
      yield* checkpoint.put({
        subagentId,
        status: "queued",
        title: "Interrupted",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      const process = yield* makeSubagentProcess(subagentId, {
        title: "Interrupted",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });
      const running = yield* process.run.pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);

      yield* Fiber.interrupt(running);

      expect(yield* process.await).toEqual({ kind: "killed" });
      expect(yield* process.events.pipe(Stream.runCollect)).toEqual([]);
      expect(yield* testHost.active).toEqual([]);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        TestHost.layer.pipe(
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("converts a Host failure into a failure event and a failed result", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_process-host-fail");

      yield* testHost.stub([
        SubagentHostStartError.make({
          subagentId,
          host: "test",
          reason: "Pane creation failed",
        }),
      ]);
      yield* checkpoint.put({
        subagentId,
        status: "queued",
        title: "Host failure",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      const process = yield* makeSubagentProcess(subagentId, {
        title: "Host failure",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      yield* process.run;

      const resolved = yield* process.await;

      expect(resolved.kind).toBe("failed");

      const failure = yield* process.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(failure.kind).toBe("failure");
      expect(yield* checkpoint.get(subagentId)).toMatchObject({
        status: "failed",
        latestEvent: { kind: "failure" },
      });
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        TestHost.layer.pipe(
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("resolves failed when the child disconnects before reporting", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_process-disconnect");
      const parentScope = yield* Scope.Scope;
      const childScope = yield* Scope.fork(parentScope);

      yield* testHost.stub([null]);
      yield* checkpoint.put({
        subagentId,
        status: "queued",
        title: "Disconnect",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      const process = yield* makeSubagentProcess(subagentId, {
        title: "Disconnect",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });
      const running = yield* process.run.pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);

      yield* bridge.connect(subagentId).pipe(Scope.provide(childScope));
      yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((record) => record.status === "running"),
        Stream.runHead,
      );
      yield* Scope.close(childScope, Exit.void);

      expect(yield* process.await).toEqual({
        kind: "failed",
        reason: "Subagent disconnected before reporting",
      });
      expect(yield* process.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption))).toEqual(
        {
          kind: "failure",
          reason: "Subagent disconnected before reporting",
        },
      );
      expect((yield* checkpoint.get(subagentId)).status).toBe("failed");

      yield* Fiber.join(running);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        TestHost.layer.pipe(
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
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

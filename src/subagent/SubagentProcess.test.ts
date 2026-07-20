import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Layer, Scope, Stream } from "effect";

import { TestHost } from "../testing/TestHost.ts";
import * as Protocol from "../host/Protocol.ts";
import * as UnixSocketTransport from "../host/link/unix/UnixSocketTransport.ts";
import { SubagentHarness } from "../harness/Harness.ts";
import { SubagentHostStartError } from "../host/Host.ts";
import { SubagentCapacity } from "./SubagentCapacity.ts";
import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import { decodeSubagentId } from "./SubagentId.ts";
import { makeSubagentProcess } from "./SubagentProcess.ts";

it.describe("SubagentProcess", () => {
  it.effect("accepts the first message, emits it, and resolves exited", () =>
    Effect.gen(function* () {
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
        mode: "ephemeral",
      });

      const process = yield* makeSubagentProcess(subagentId, {
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });
      const running = yield* process.run.pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* Protocol.connect(subagentId);

      yield* child.send({ kind: "message", content: "Task complete." });

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
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentCapacity.layer(1)),
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("runs persistent turns and idles between them", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_process-persistent");

      yield* testHost.stub([null]);
      yield* checkpoint.put({
        subagentId,
        status: "queued",
        title: "Assistant",
        prompt: "Stand by.",
        cwd: "/worktree",
        mode: "persistent",
      });

      const process = yield* makeSubagentProcess(subagentId, {
        title: "Assistant",
        prompt: "Stand by.",
        cwd: "/worktree",
        mode: "persistent",
      });
      const running = yield* process.run.pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* Protocol.connect(subagentId);

      yield* child.send({ kind: "message", content: "Ready." });
      yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((record) => record.status === "idle"),
        Stream.runHead,
      );

      yield* process.send("Review the diff.");

      expect(yield* child.inbox.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption))).toEqual({
        kind: "message",
        content: "Review the diff.",
      });

      yield* child.send({ kind: "message", content: "Reviewed." });

      const accepted = yield* process.events.pipe(Stream.take(2), Stream.runCollect);

      expect(accepted).toEqual([
        { kind: "message", content: "Ready." },
        { kind: "message", content: "Reviewed." },
      ]);
      yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((record) => record.status === "idle"),
        Stream.runHead,
      );

      yield* Fiber.interrupt(running);

      expect(yield* process.await).toEqual({ kind: "killed" });
      expect(yield* testHost.active).toEqual([]);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        TestHost.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentCapacity.layer(1)),
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("fails when the child disconnects while idle", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_process-idle-drop");
      const parentScope = yield* Scope.Scope;
      const childScope = yield* Scope.fork(parentScope);

      yield* testHost.stub([null]);
      yield* checkpoint.put({
        subagentId,
        status: "queued",
        title: "Assistant",
        prompt: "Stand by.",
        cwd: "/worktree",
        mode: "persistent",
      });

      const process = yield* makeSubagentProcess(subagentId, {
        title: "Assistant",
        prompt: "Stand by.",
        cwd: "/worktree",
        mode: "persistent",
      });
      const running = yield* process.run.pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* Protocol.connect(subagentId).pipe(Scope.provide(childScope));

      yield* child.send({ kind: "message", content: "Ready." });
      yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((record) => record.status === "idle"),
        Stream.runHead,
      );
      yield* Scope.close(childScope, Exit.void);

      expect(yield* process.await).toEqual({
        kind: "failed",
        reason: "Link disconnected",
      });
      expect((yield* checkpoint.get(subagentId)).status).toBe("failed");

      yield* Fiber.join(running);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        TestHost.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentCapacity.layer(1)),
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provideMerge(UnixSocketTransport.layer),
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
        mode: "ephemeral",
      });

      const process = yield* makeSubagentProcess(subagentId, {
        title: "Interrupted",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
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
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentCapacity.layer(1)),
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provideMerge(UnixSocketTransport.layer),
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
        mode: "ephemeral",
      });

      const process = yield* makeSubagentProcess(subagentId, {
        title: "Host failure",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      yield* process.run;

      expect(yield* process.await).toEqual({ kind: "failed", reason: "Pane creation failed" });
      expect(yield* process.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption))).toEqual(
        {
          kind: "failure",
          reason: "Pane creation failed",
        },
      );
      expect(yield* checkpoint.get(subagentId)).toMatchObject({
        status: "failed",
        latestEvent: { kind: "failure" },
      });
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        TestHost.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentCapacity.layer(1)),
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("resolves failed when the child disconnects before reporting", () =>
    Effect.gen(function* () {
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
        mode: "ephemeral",
      });

      const process = yield* makeSubagentProcess(subagentId, {
        title: "Disconnect",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });
      const running = yield* process.run.pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);

      yield* Protocol.connect(subagentId).pipe(Scope.provide(childScope));
      yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((record) => record.status === "running"),
        Stream.runHead,
      );
      yield* Scope.close(childScope, Exit.void);

      expect(yield* process.await).toEqual({
        kind: "failed",
        reason: "Link disconnected",
      });
      expect(yield* process.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption))).toEqual(
        {
          kind: "failure",
          reason: "Link disconnected",
        },
      );
      expect((yield* checkpoint.get(subagentId)).status).toBe("failed");

      yield* Fiber.join(running);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        TestHost.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentCapacity.layer(1)),
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );
});

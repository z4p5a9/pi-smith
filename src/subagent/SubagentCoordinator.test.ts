import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Layer, ManagedRuntime, Option, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";

import { TestHost } from "../testing/TestHost.ts";
import { SubagentBridge } from "../host/bridge/Bridge.ts";
import * as UnixSocketBridgeTransport from "../host/bridge/unix/UnixSocketBridgeTransport.ts";
import { SubagentHarness } from "../harness/Harness.ts";
import { SubagentHostUnavailableError } from "../host/Host.ts";
import { SubagentCapacity } from "./SubagentCapacity.ts";
import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import {
  SubagentCoordinator,
  SubagentInactiveError,
  SubagentUnknownError,
} from "./SubagentCoordinator.ts";
import { decodeSubagentId, type SubagentId } from "./SubagentId.ts";

it.describe("SubagentCoordinator", () => {
  it.effect("projects and retains one completed message before notification consumption", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const checkpoint = yield* SubagentCheckpoint;
      const coordinator = yield* SubagentCoordinator;
      const testHost = yield* TestHost;

      yield* testHost.stub([null]);

      const subagentId = yield* coordinator.create({
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);
      expect((yield* checkpoint.get(subagentId)).status).toBe("starting");

      const child = yield* bridge.connect(subagentId);

      yield* child.sendEvent({ kind: "message", content: "Task complete." });

      expect(
        yield* coordinator.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
      ).toEqual({
        subagentId,
        event: { kind: "message", content: "Task complete." },
      });
      expect(yield* checkpoint.get(subagentId)).toEqual({
        subagentId,
        status: "completed",
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
        latestEvent: { kind: "message", content: "Task complete." },
      });

      const observed = yield* checkpoint
        .changes(subagentId)
        .pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(observed.status).toBe("completed");
      yield* Effect.suspend(() =>
        testHost.active.pipe(
          Effect.flatMap((active) =>
            active.length === 0 ? Effect.void : Effect.fail("Child is still active"),
          ),
        ),
      ).pipe(Effect.eventually);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentCoordinator.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("projects Host acquisition failure as a failure event", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const coordinator = yield* SubagentCoordinator;
      const testHost = yield* TestHost;
      const errorSubagentId = yield* decodeSubagentId("sa_12345678_unavailable");

      yield* testHost.stub([
        SubagentHostUnavailableError.make({
          subagentId: errorSubagentId,
          host: "test",
          reason: "Host unavailable",
        }),
      ]);

      yield* coordinator.create({
        title: "Unavailable",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      const { event, subagentId } = yield* coordinator.events.pipe(
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );

      expect(event.kind).toBe("failure");
      expect(yield* checkpoint.get(subagentId)).toMatchObject({
        status: "failed",
        latestEvent: { kind: "failure" },
      });
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentCoordinator.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("projects a child-reported failure", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const checkpoint = yield* SubagentCheckpoint;
      const coordinator = yield* SubagentCoordinator;
      const testHost = yield* TestHost;

      yield* testHost.stub([null]);

      const subagentId = yield* coordinator.create({
        title: "Failed task",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* bridge.connect(subagentId);

      yield* child.sendEvent({ kind: "failure", reason: "Model request failed" });

      expect(
        yield* coordinator.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
      ).toEqual({
        subagentId,
        event: { kind: "failure", reason: "Model request failed" },
      });
      expect(yield* checkpoint.get(subagentId)).toMatchObject({
        status: "failed",
        latestEvent: { kind: "failure", reason: "Model request failed" },
      });
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentCoordinator.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("projects a disconnected running child as failed", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const checkpoint = yield* SubagentCheckpoint;
      const coordinator = yield* SubagentCoordinator;
      const testHost = yield* TestHost;
      const parentScope = yield* Scope.Scope;
      const childScope = yield* Scope.fork(parentScope);

      yield* testHost.stub([null]);

      const subagentId = yield* coordinator.create({
        title: "Disconnect",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);
      yield* bridge.connect(subagentId).pipe(Scope.provide(childScope));
      yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((record) => record.status === "running"),
        Stream.runHead,
      );
      yield* Scope.close(childScope, Exit.void);

      const notification = yield* coordinator.events.pipe(
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );

      expect(notification.subagentId).toBe(subagentId);
      expect(notification.event.kind).toBe("failure");
      expect((yield* checkpoint.get(subagentId)).status).toBe("failed");
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentCoordinator.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("runs at most ten children and preserves FIFO admission", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const checkpoint = yield* SubagentCheckpoint;
      const coordinator = yield* SubagentCoordinator;
      const testHost = yield* TestHost;

      yield* testHost.stub(Array.from({ length: 11 }, () => null));

      const subagentIds = yield* Effect.forEach(
        Array.from({ length: 12 }, (_, index) => index),
        (index) =>
          coordinator.create({
            title: `Worker ${String(index + 1)}`,
            prompt: "Complete the task.",
            cwd: "/worktree",
            mode: "ephemeral",
          }),
      );
      const started: Array<SubagentId> = [];

      for (let index = 0; index < 10; index++) {
        started.push(yield* testHost.takeStart);
      }

      expect(new Set(started)).toEqual(new Set(subagentIds.slice(0, 10)));
      const eleventhSubagentId = yield* Effect.fromNullishOr(subagentIds[10]);
      const twelfthSubagentId = yield* Effect.fromNullishOr(subagentIds[11]);
      const firstStartedSubagentId = yield* Effect.fromNullishOr(started[0]);

      expect((yield* checkpoint.get(eleventhSubagentId)).status).toBe("queued");
      expect((yield* checkpoint.get(twelfthSubagentId)).status).toBe("queued");

      const child = yield* bridge.connect(firstStartedSubagentId);

      yield* child.sendEvent({ kind: "message", content: "Done." });
      yield* coordinator.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(yield* testHost.takeStart).toBe(eleventhSubagentId);
      expect((yield* checkpoint.get(twelfthSubagentId)).status).toBe("queued");
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentCoordinator.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("publishes one event when delivery races with disconnect", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const coordinator = yield* SubagentCoordinator;
      const testHost = yield* TestHost;
      const parentScope = yield* Scope.Scope;
      const childScope = yield* Scope.fork(parentScope);

      yield* testHost.stub([null]);

      const subagentId = yield* coordinator.create({
        title: "Race",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* bridge.connect(subagentId).pipe(Scope.provide(childScope));

      yield* child
        .sendEvent({ kind: "message", content: "Task complete." })
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
      yield* Scope.close(childScope, Exit.void);

      expect(
        (yield* coordinator.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)))
          .subagentId,
      ).toBe(subagentId);

      const second = yield* coordinator.events.pipe(
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
        Effect.timeoutOption("1 millis"),
        Effect.forkChild({ startImmediately: true }),
      );

      yield* TestClock.adjust("1 millis");

      expect(Option.isNone(yield* Fiber.join(second))).toBe(true);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentCoordinator.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("routes sends to a persistent subagent and aggregates its turns", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const checkpoint = yield* SubagentCheckpoint;
      const coordinator = yield* SubagentCoordinator;
      const testHost = yield* TestHost;

      yield* testHost.stub([null]);

      const subagentId = yield* coordinator.create({
        title: "Assistant",
        prompt: "Stand by.",
        cwd: "/worktree",
        mode: "persistent",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* bridge.connect(subagentId);

      yield* child.sendEvent({ kind: "message", content: "Ready." });

      expect(
        yield* coordinator.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
      ).toEqual({
        subagentId,
        event: { kind: "message", content: "Ready." },
      });

      yield* coordinator.send(subagentId, "Review the diff.");

      expect(yield* child.messages.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption))).toBe(
        "Review the diff.",
      );

      yield* child.sendEvent({ kind: "message", content: "Reviewed." });

      expect(
        yield* coordinator.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
      ).toEqual({
        subagentId,
        event: { kind: "message", content: "Reviewed." },
      });
      expect((yield* checkpoint.get(subagentId)).status).toBe("idle");

      yield* coordinator.kill(subagentId);
      yield* Effect.suspend(() =>
        testHost.active.pipe(
          Effect.flatMap((active) =>
            active.length === 0 ? Effect.void : Effect.fail("Child is still active"),
          ),
        ),
      ).pipe(Effect.eventually);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentCoordinator.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("fails sending to an unknown subagent", () =>
    Effect.gen(function* () {
      const coordinator = yield* SubagentCoordinator;
      const subagentId = yield* decodeSubagentId("sa_12345678_unknown");
      const error = yield* coordinator.send(subagentId, "Hello.").pipe(Effect.flip);

      expect(error).toBeInstanceOf(SubagentUnknownError);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentCoordinator.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("fails sending to a finished subagent", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const coordinator = yield* SubagentCoordinator;
      const testHost = yield* TestHost;

      yield* testHost.stub([null]);

      const subagentId = yield* coordinator.create({
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* bridge.connect(subagentId);

      yield* child.sendEvent({ kind: "message", content: "Task complete." });
      yield* coordinator.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      const error = yield* Effect.suspend(() =>
        coordinator.send(subagentId, "Too late.").pipe(Effect.flip),
      ).pipe(Effect.eventually);

      expect(error).toBeInstanceOf(SubagentInactiveError);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentCoordinator.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("does not project Coordinator shutdown as failure", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestHost;

      yield* testHost.stub([null]);

      const subagentId = yield* Effect.scoped(
        Effect.gen(function* () {
          const coordinator = yield* SubagentCoordinator;
          const admittedSubagentId = yield* coordinator.create({
            title: "Interrupted",
            prompt: "Complete the task.",
            cwd: "/worktree",
            mode: "ephemeral",
          });

          expect(yield* testHost.takeStart).toBe(admittedSubagentId);
          return admittedSubagentId;
        }).pipe(Effect.provide(SubagentCoordinator.layer)),
      );

      expect((yield* checkpoint.get(subagentId)).status).toBe("starting");
      expect(yield* testHost.active).toEqual([]);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.mergeAll(
          SubagentCheckpoint.layer,
          TestHost.layer,
          Layer.succeed(
            SubagentHarness,
            SubagentHarness.of({
              makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
            }),
          ),
        ).pipe(
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provideMerge(SubagentCapacity.layer(10)),
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("releases live children when its ManagedRuntime is disposed", () => {
    const runtime = ManagedRuntime.make(
      SubagentCoordinator.layer.pipe(
        Layer.provideMerge(SubagentCheckpoint.layer),
        Layer.provideMerge(TestHost.layer),
        Layer.provideMerge(SubagentBridge.layer),
        Layer.provide(
          Layer.succeed(
            SubagentHarness,
            SubagentHarness.of({
              makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
            }),
          ),
        ),
        Layer.provide(SubagentCapacity.layer(10)),
        Layer.provide(UnixSocketBridgeTransport.layer),
        Layer.provide(NodeFileSystem.layer),
      ),
    );

    return Effect.gen(function* () {
      runtime.runFork(
        SubagentCoordinator.use((coordinator) => coordinator.events.pipe(Stream.runDrain)),
      );

      const parentScope = yield* Scope.Scope;
      const childScope = yield* Scope.fork(parentScope);
      const { bridge, checkpoint, subagentId, testHost } = yield* Effect.promise(() =>
        runtime.runPromise(
          Effect.gen(function* () {
            const runtimeBridge = yield* SubagentBridge;
            const runtimeCheckpoint = yield* SubagentCheckpoint;
            const coordinator = yield* SubagentCoordinator;
            const runtimeTestHost = yield* TestHost;

            yield* runtimeTestHost.stub([null]);

            const admittedSubagentId = yield* coordinator.create({
              title: "Interrupted",
              prompt: "Complete the task.",
              cwd: "/worktree",
              mode: "ephemeral",
            });

            expect(yield* runtimeTestHost.takeStart).toBe(admittedSubagentId);
            return {
              bridge: runtimeBridge,
              checkpoint: runtimeCheckpoint,
              subagentId: admittedSubagentId,
              testHost: runtimeTestHost,
            };
          }),
        ),
      );

      yield* bridge.connect(subagentId).pipe(Scope.provide(childScope));
      yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((record) => record.status === "running"),
        Stream.runHead,
      );
      expect(yield* testHost.active).toEqual([subagentId]);
      yield* Effect.promise(() => runtime.dispose());
      expect(yield* testHost.active).toEqual([]);
      expect((yield* checkpoint.get(subagentId)).status).toBe("running");
      yield* testHost.verify;
    }).pipe(Effect.ensuring(Effect.promise(() => runtime.dispose())));
  });
});

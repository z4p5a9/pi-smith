import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Layer, Option, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";

import { TestSubagentHost } from "../testing/TestSubagentHost.ts";
import { SubagentBridge } from "./SubagentBridge.ts";
import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import { SubagentCoordinator } from "./SubagentCoordinator.ts";
import { SubagentHarness } from "./SubagentHarness.ts";
import { SubagentHostUnavailableError } from "./SubagentHost.ts";
import { decodeSubagentId, type SubagentId } from "./SubagentId.ts";
import { layer as unixSocketSubagentBridgeTransportLayer } from "./UnixSocketSubagentBridgeTransport.ts";

it.describe("SubagentCoordinator", () => {
  it.effect("projects and retains one completed event before notification consumption", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const checkpoint = yield* SubagentCheckpoint;
      const coordinator = yield* SubagentCoordinator;
      const testHost = yield* TestSubagentHost;

      yield* testHost.stub([null]);

      const subagentId = yield* coordinator.create({
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);
      expect((yield* checkpoint.get(subagentId)).status).toBe("starting");

      const child = yield* bridge.connect(subagentId);

      yield* child.sendEvent({ kind: "completed", report: "Task complete." });
      yield* child.close;

      expect(
        yield* coordinator.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
      ).toEqual({
        subagentId,
        event: { kind: "completed", report: "Task complete." },
      });
      expect(yield* checkpoint.get(subagentId)).toEqual({
        subagentId,
        status: "completed",
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        latestEvent: { kind: "completed", report: "Task complete." },
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
          Layer.provideMerge(TestSubagentHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("projects Host acquisition failure through the terminal router", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const coordinator = yield* SubagentCoordinator;
      const testHost = yield* TestSubagentHost;
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
      });

      const { event, subagentId } = yield* coordinator.events.pipe(
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );

      expect(event.kind).toBe("failed");
      expect(yield* checkpoint.get(subagentId)).toMatchObject({
        status: "failed",
        latestEvent: { kind: "failed" },
      });
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentCoordinator.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(TestSubagentHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentBridge.layer),
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
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
      const testHost = yield* TestSubagentHost;

      yield* testHost.stub([null]);

      const subagentId = yield* coordinator.create({
        title: "Failed task",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* bridge.connect(subagentId);

      yield* child.sendEvent({ kind: "failed", reason: "Model request failed" });
      yield* child.close;

      expect(
        yield* coordinator.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
      ).toEqual({
        subagentId,
        event: { kind: "failed", reason: "Model request failed" },
      });
      expect(yield* checkpoint.get(subagentId)).toMatchObject({
        status: "failed",
        latestEvent: { kind: "failed", reason: "Model request failed" },
      });
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentCoordinator.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(TestSubagentHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
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
      const testHost = yield* TestSubagentHost;
      const parentScope = yield* Scope.Scope;
      const childScope = yield* Scope.fork(parentScope);

      yield* testHost.stub([null]);

      const subagentId = yield* coordinator.create({
        title: "Disconnect",
        prompt: "Complete the task.",
        cwd: "/worktree",
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
      expect(notification.event.kind).toBe("failed");
      expect((yield* checkpoint.get(subagentId)).status).toBe("failed");
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentCoordinator.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(TestSubagentHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("runs at most ten children and preserves FIFO while cleanup holds the slot", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const checkpoint = yield* SubagentCheckpoint;
      const coordinator = yield* SubagentCoordinator;
      const testHost = yield* TestSubagentHost;

      yield* testHost.stub(Array.from({ length: 11 }, () => null));

      const subagentIds = yield* Effect.forEach(
        Array.from({ length: 12 }, (_, index) => index),
        (index) =>
          coordinator.create({
            title: `Worker ${String(index + 1)}`,
            prompt: "Complete the task.",
            cwd: "/worktree",
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

      yield* child.sendEvent({ kind: "completed", report: "Done." });
      expect((yield* checkpoint.get(eleventhSubagentId)).status).toBe("queued");
      yield* child.close;
      yield* coordinator.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(yield* testHost.takeStart).toBe(eleventhSubagentId);
      expect((yield* checkpoint.get(twelfthSubagentId)).status).toBe("queued");
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentCoordinator.layer.pipe(
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(TestSubagentHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("accepts one terminal event when delivery races with disconnect", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const coordinator = yield* SubagentCoordinator;
      const testHost = yield* TestSubagentHost;
      const parentScope = yield* Scope.Scope;
      const childScope = yield* Scope.fork(parentScope);

      yield* testHost.stub([null]);

      const subagentId = yield* coordinator.create({
        title: "Race",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* bridge.connect(subagentId).pipe(Scope.provide(childScope));

      yield* child
        .sendEvent({ kind: "completed", report: "Task complete." })
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
          Layer.provideMerge(TestSubagentHost.layer),
          Layer.provideMerge(SubagentBridge.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("does not project Coordinator shutdown as failure", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestSubagentHost;

      yield* testHost.stub([null]);

      const subagentId = yield* Effect.scoped(
        Effect.gen(function* () {
          const coordinator = yield* SubagentCoordinator;
          const admittedSubagentId = yield* coordinator.create({
            title: "Interrupted",
            prompt: "Complete the task.",
            cwd: "/worktree",
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
          TestSubagentHost.layer,
          SubagentBridge.layer,
          Layer.succeed(
            SubagentHarness,
            SubagentHarness.of({
              makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
            }),
          ),
        ).pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );
});

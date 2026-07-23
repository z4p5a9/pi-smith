import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  ManagedRuntime,
  Option,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";

import { TestHost } from "../testing/TestHost.ts";
import * as Link from "../host/link/Link.ts";
import { SubagentLinkTransport } from "../host/link/Transport.ts";
import * as UnixSocketTransport from "../host/link/unix/UnixSocketTransport.ts";
import * as Protocol from "../host/Protocol.ts";
import { SubagentHarness } from "../harness/Harness.ts";
import { SubagentHostUnavailableError } from "../host/Host.ts";
import { SubagentCapacity } from "./SubagentCapacity.ts";
import { SubagentCheckpoint, type SubagentRecord } from "./SubagentCheckpoint.ts";
import {
  RootSupervisor,
  SubagentKillInactiveError,
  SubagentKillUnknownError,
} from "./RootSupervisor.ts";
import type { SubagentEventEnvelope } from "./SubagentEvent.ts";
import { SubagentEventOutbox } from "./SubagentEventOutbox.ts";
import { decodeSubagentId, type SubagentId } from "./SubagentId.ts";
import { SubagentRegistry } from "./SubagentRegistry.ts";

it.describe("RootSupervisor", () => {
  it.effect("shares registry, checkpoint, and outbox through its root layer", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const rootSupervisor = yield* RootSupervisor;
      const eventOutbox = yield* SubagentEventOutbox;
      const registry = yield* SubagentRegistry;
      const testHost = yield* TestHost;

      yield* testHost.stub([null]);

      const subagentId = yield* rootSupervisor.create({
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      const ref = yield* Effect.fromNullishOr(yield* registry.lookup(subagentId));

      expect(yield* ref.send("Ignored.")).toBeUndefined();
      expect(yield* testHost.takeStart).toBe(subagentId);
      expect((yield* checkpoint.get(subagentId)).status).toBe("starting");

      const child = yield* Protocol.connect(subagentId);

      yield* child.send({ kind: "message", content: "Task complete." });

      expect(
        yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
      ).toEqual({
        subagentId,
        event: { kind: "message", content: "Task complete." },
      });
      expect(yield* checkpoint.get(subagentId)).toEqual({
        subagentId,
        status: "exited",
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
        latestEvent: { kind: "message", content: "Task complete." },
      });

      const observed = yield* checkpoint
        .changes(subagentId)
        .pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(observed.status).toBe("exited");
      yield* Effect.suspend(() =>
        testHost.active.pipe(
          Effect.flatMap((active) =>
            active.length === 0 ? Effect.void : Effect.fail("Child is still active"),
          ),
        ),
      ).pipe(Effect.eventually);
      yield* Effect.gen(function* () {
        if ((yield* registry.lookup(subagentId)) !== undefined) {
          return yield* Effect.fail("Subagent reference is still registered");
        }

        return yield* Effect.void;
      }).pipe(Effect.eventually);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        RootSupervisor.layer.pipe(
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("projects Host acquisition failure as a failure event", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const rootSupervisor = yield* RootSupervisor;
      const eventOutbox = yield* SubagentEventOutbox;
      const testHost = yield* TestHost;
      const errorSubagentId = yield* decodeSubagentId("sa_12345678_unavailable");

      yield* testHost.stub([
        SubagentHostUnavailableError.make({
          subagentId: errorSubagentId,
          host: "test",
          reason: "Host unavailable",
        }),
      ]);

      yield* rootSupervisor.create({
        title: "Unavailable",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      const { event, subagentId } = yield* eventOutbox.events.pipe(
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
        RootSupervisor.layerNoDeps.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("projects a child-reported failure", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const rootSupervisor = yield* RootSupervisor;
      const eventOutbox = yield* SubagentEventOutbox;
      const testHost = yield* TestHost;

      yield* testHost.stub([null]);

      const subagentId = yield* rootSupervisor.create({
        title: "Failed task",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* Protocol.connect(subagentId);

      yield* child.send({ kind: "failure", reason: "Model request failed" });

      expect(
        yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
      ).toEqual({
        subagentId,
        event: { kind: "failure", reason: "Model request failed" },
      });
      expect(yield* checkpoint.get(subagentId)).toMatchObject({
        status: "exited",
        latestEvent: { kind: "failure", reason: "Model request failed" },
      });
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        RootSupervisor.layerNoDeps.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("projects a disconnected running child as failed", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const rootSupervisor = yield* RootSupervisor;
      const eventOutbox = yield* SubagentEventOutbox;
      const testHost = yield* TestHost;
      const parentScope = yield* Scope.Scope;
      const childScope = yield* Scope.fork(parentScope);

      yield* testHost.stub([null]);

      const subagentId = yield* rootSupervisor.create({
        title: "Disconnect",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);
      yield* Protocol.connect(subagentId).pipe(Scope.provide(childScope));
      yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((record) => record.status === "running"),
        Stream.runHead,
      );
      yield* Scope.close(childScope, Exit.void);

      const notification = yield* eventOutbox.events.pipe(
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
        RootSupervisor.layerNoDeps.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("runs at most ten children and preserves FIFO admission", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const rootSupervisor = yield* RootSupervisor;
      const eventOutbox = yield* SubagentEventOutbox;
      const testHost = yield* TestHost;

      yield* testHost.stub(Array.from({ length: 11 }, () => null));

      const subagentIds = yield* Effect.forEach(
        Array.from({ length: 12 }, (_, index) => index),
        (index) =>
          rootSupervisor.create({
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

      const child = yield* Protocol.connect(firstStartedSubagentId);

      yield* child.send({ kind: "message", content: "Done." });
      yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(yield* testHost.takeStart).toBe(eleventhSubagentId);
      expect((yield* checkpoint.get(twelfthSubagentId)).status).toBe("queued");
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        RootSupervisor.layerNoDeps.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("admits a persistent subagent before host capacity is available", () =>
    Effect.gen(function* () {
      const rootSupervisor = yield* RootSupervisor;
      const capacity = yield* SubagentCapacity;
      const registry = yield* SubagentRegistry;
      const testHost = yield* TestHost;
      const capacityAcquired = yield* Deferred.make<void>();
      const releaseCapacity = yield* Deferred.make<void>();
      const capacityHolder = yield* capacity
        .withPermit(
          Deferred.succeed(capacityAcquired, undefined).pipe(
            Effect.andThen(Deferred.await(releaseCapacity)),
          ),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(capacityAcquired);

      const subagentId = yield* rootSupervisor.create({
        title: "Queued assistant",
        prompt: "Stand by.",
        cwd: "/worktree",
        mode: "persistent",
      });
      const ref = yield* Effect.fromNullishOr(yield* registry.lookup(subagentId));
      const messageId = yield* ref.send("Review the diff.");

      expect(messageId).toMatch(/^msg_[a-z0-9]{24}$/);
      expect(yield* testHost.calls).toEqual([]);

      yield* rootSupervisor.kill(subagentId);
      yield* Deferred.succeed(releaseCapacity, undefined);
      yield* Fiber.join(capacityHolder);
      expect(yield* testHost.calls).toEqual([]);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        RootSupervisor.layerNoDeps.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provideMerge(SubagentCapacity.layer(1)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("does not start a subagent killed before host capacity is available", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const rootSupervisor = yield* RootSupervisor;
      const capacity = yield* SubagentCapacity;
      const testHost = yield* TestHost;
      const capacityAcquired = yield* Deferred.make<void>();
      const releaseCapacity = yield* Deferred.make<void>();
      const capacityHolder = yield* capacity
        .withPermit(
          Deferred.succeed(capacityAcquired, undefined).pipe(
            Effect.andThen(Deferred.await(releaseCapacity)),
          ),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(capacityAcquired);

      const killedSubagentId = yield* rootSupervisor.create({
        title: "Cancelled assistant",
        prompt: "Stand by.",
        cwd: "/worktree",
        mode: "persistent",
      });

      expect((yield* checkpoint.get(killedSubagentId)).status).toBe("queued");
      yield* rootSupervisor.kill(killedSubagentId);
      expect((yield* checkpoint.get(killedSubagentId)).status).toBe("killed");
      yield* testHost.stub([null]);

      const sentinelSubagentId = yield* rootSupervisor.create({
        title: "Sentinel",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      yield* Deferred.succeed(releaseCapacity, undefined);
      yield* Fiber.join(capacityHolder);

      expect(yield* testHost.takeStart).toBe(sentinelSubagentId);
      expect(yield* testHost.calls).toEqual([
        {
          subagentId: sentinelSubagentId,
          command: { executable: "pi", args: [] },
        },
      ]);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        RootSupervisor.layerNoDeps.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provideMerge(SubagentCapacity.layer(1)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("projects killing a running subagent as killed", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const rootSupervisor = yield* RootSupervisor;
      const testHost = yield* TestHost;

      yield* testHost.stub([null]);

      const subagentId = yield* rootSupervisor.create({
        title: "Running assistant",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);
      yield* Protocol.connect(subagentId);
      yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((record) => record.status === "running"),
        Stream.runHead,
      );

      yield* rootSupervisor.kill(subagentId);

      expect((yield* checkpoint.get(subagentId)).status).toBe("killed");
      expect(yield* testHost.active).toEqual([]);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        RootSupervisor.layerNoDeps.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("retains an event published before kill", () =>
    Effect.gen(function* () {
      const eventOutbox = yield* SubagentEventOutbox.make();
      const published = yield* Deferred.make<SubagentEventEnvelope>();
      const observedEventOutbox = SubagentEventOutbox.of({
        ...eventOutbox,
        publish: Effect.fn("ObservedSubagentEventOutbox.publish")(function* (
          envelope: SubagentEventEnvelope,
        ) {
          yield* eventOutbox.publish(envelope);
          yield* Deferred.succeed(published, envelope);
        }),
      });

      yield* Effect.gen(function* () {
        const checkpoint = yield* SubagentCheckpoint;
        const rootSupervisor = yield* RootSupervisor;
        const testHost = yield* TestHost;

        yield* testHost.stub([null]);

        const subagentId = yield* rootSupervisor.create({
          title: "Published assistant",
          prompt: "Complete the task.",
          cwd: "/worktree",
          mode: "persistent",
        });

        expect(yield* testHost.takeStart).toBe(subagentId);

        const child = yield* Protocol.connect(subagentId);

        yield* child.send({ kind: "message", content: "Published." });
        yield* Deferred.await(published);
        yield* rootSupervisor.kill(subagentId);

        expect(
          yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
        ).toEqual({
          subagentId,
          event: { kind: "message", content: "Published." },
        });
        expect((yield* checkpoint.get(subagentId)).status).toBe("killed");
        yield* testHost.verify;
      }).pipe(
        Effect.scoped,
        Effect.provide(
          RootSupervisor.layerNoDeps.pipe(
            Layer.provideMerge(Layer.succeed(SubagentEventOutbox, observedEventOutbox)),
            Layer.provideMerge(SubagentCheckpoint.layer),
            Layer.provideMerge(SubagentRegistry.layer),
            Layer.provideMerge(TestHost.layer),
            Layer.provide(
              Layer.succeed(
                SubagentHarness,
                SubagentHarness.of({
                  makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
                }),
              ),
            ),
            Layer.provide(SubagentCapacity.layer(10)),
            Layer.provideMerge(UnixSocketTransport.layer),
            Layer.provide(NodeFileSystem.layer),
          ),
        ),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("drops an event whose publication is interrupted by kill", () =>
    Effect.gen(function* () {
      const eventOutbox = yield* SubagentEventOutbox.make();
      const publishing = yield* Deferred.make<SubagentEventEnvelope>();
      const blockedEventOutbox = SubagentEventOutbox.of({
        ...eventOutbox,
        publish: Effect.fn("BlockedSubagentEventOutbox.publish")(function* (
          envelope: SubagentEventEnvelope,
        ) {
          yield* Deferred.succeed(publishing, envelope);
          return yield* Effect.never;
        }),
      });

      yield* Effect.gen(function* () {
        const checkpoint = yield* SubagentCheckpoint;
        const rootSupervisor = yield* RootSupervisor;
        const testHost = yield* TestHost;

        yield* testHost.stub([null]);

        const subagentId = yield* rootSupervisor.create({
          title: "Interrupted publication",
          prompt: "Complete the task.",
          cwd: "/worktree",
          mode: "persistent",
        });

        expect(yield* testHost.takeStart).toBe(subagentId);

        const child = yield* Protocol.connect(subagentId);

        yield* child.send({ kind: "message", content: "In flight." });
        yield* Deferred.await(publishing);
        yield* rootSupervisor.kill(subagentId);

        const event = yield* eventOutbox.events.pipe(
          Stream.runHead,
          Effect.flatMap(Effect.fromOption),
          Effect.timeoutOption("1 millis"),
          Effect.forkChild({ startImmediately: true }),
        );

        yield* TestClock.adjust("1 millis");

        expect(Option.isNone(yield* Fiber.join(event))).toBe(true);
        expect((yield* checkpoint.get(subagentId)).status).toBe("killed");
        yield* testHost.verify;
      }).pipe(
        Effect.scoped,
        Effect.provide(
          RootSupervisor.layerNoDeps.pipe(
            Layer.provideMerge(Layer.succeed(SubagentEventOutbox, blockedEventOutbox)),
            Layer.provideMerge(SubagentCheckpoint.layer),
            Layer.provideMerge(SubagentRegistry.layer),
            Layer.provideMerge(TestHost.layer),
            Layer.provide(
              Layer.succeed(
                SubagentHarness,
                SubagentHarness.of({
                  makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
                }),
              ),
            ),
            Layer.provide(SubagentCapacity.layer(10)),
            Layer.provideMerge(UnixSocketTransport.layer),
            Layer.provide(NodeFileSystem.layer),
          ),
        ),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("publishes one event when delivery races with disconnect", () =>
    Effect.gen(function* () {
      const rootSupervisor = yield* RootSupervisor;
      const eventOutbox = yield* SubagentEventOutbox;
      const testHost = yield* TestHost;
      const parentScope = yield* Scope.Scope;
      const childScope = yield* Scope.fork(parentScope);

      yield* testHost.stub([null]);

      const subagentId = yield* rootSupervisor.create({
        title: "Race",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* Protocol.connect(subagentId).pipe(Scope.provide(childScope));

      yield* child
        .send({ kind: "message", content: "Task complete." })
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));
      yield* Scope.close(childScope, Exit.void);

      expect(
        (yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)))
          .subagentId,
      ).toBe(subagentId);

      const second = yield* eventOutbox.events.pipe(
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
        RootSupervisor.layerNoDeps.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("routes sends to a persistent subagent and aggregates its turns", () =>
    Effect.gen(function* () {
      const observeRegistryMiss = yield* Deferred.make<void>();
      const registryMiss = yield* Deferred.make<void>();
      const checkpoint = yield* SubagentCheckpoint.make;
      const observedCheckpoint = SubagentCheckpoint.of({
        ...checkpoint,
        has: Effect.fn("ObservedSubagentCheckpoint.has")(function* (subagentId: SubagentId) {
          if (yield* Deferred.isDone(observeRegistryMiss)) {
            yield* Deferred.succeed(registryMiss, undefined);
          }

          return yield* checkpoint.has(subagentId);
        }),
      });

      yield* Effect.gen(function* () {
        const rootSupervisor = yield* RootSupervisor;
        const eventOutbox = yield* SubagentEventOutbox;
        const capacity = yield* SubagentCapacity;
        const registry = yield* SubagentRegistry;
        const testHost = yield* TestHost;

        yield* testHost.stub([null]);

        const subagentId = yield* rootSupervisor.create({
          title: "Assistant",
          prompt: "Stand by.",
          cwd: "/worktree",
          mode: "persistent",
        });
        const ref = yield* Effect.fromNullishOr(yield* registry.lookup(subagentId));

        expect(yield* testHost.takeStart).toBe(subagentId);

        const child = yield* Protocol.connect(subagentId);

        yield* child.send({ kind: "message", content: "Ready." });

        expect(
          yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
        ).toEqual({
          subagentId,
          event: { kind: "message", content: "Ready." },
        });

        const capacityAcquired = yield* Deferred.make<void>();
        const releaseCapacity = yield* Deferred.make<void>();
        const capacityHolder = yield* capacity
          .withPermit(
            Deferred.succeed(capacityAcquired, undefined).pipe(
              Effect.andThen(Deferred.await(releaseCapacity)),
            ),
          )
          .pipe(Effect.forkChild({ startImmediately: true }));
        yield* Deferred.await(capacityAcquired);

        const deliveryReceived = yield* Deferred.make<void>();
        const delivery = yield* child.inbox.pipe(
          Stream.runHead,
          Effect.tap(() => Deferred.succeed(deliveryReceived, undefined)),
          Effect.forkChild({ startImmediately: true }),
        );
        const oversizedContent = "x".repeat(Link.maxLinkFrameBytes);
        const oversizedMessageId = yield* ref.send(oversizedContent);

        expect(oversizedMessageId).toMatch(/^msg_[a-z0-9]{24}$/);
        yield* checkpoint.changes(subagentId).pipe(
          Stream.filter((record) => record.status === "queued"),
          Stream.runHead,
        );
        expect(yield* Deferred.isDone(deliveryReceived)).toBe(false);

        yield* Deferred.succeed(releaseCapacity, undefined);

        const actualBytes =
          Link.maxLinkFrameBytes +
          new TextEncoder().encode(
            `{"v":1,"subagentId":"${subagentId}","seq":0,"data":{"kind":"message","content":""}}`,
          ).byteLength;

        expect(
          yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
        ).toEqual({
          subagentId,
          event: {
            kind: "message-rejected",
            messageId: oversizedMessageId,
            reason: "frame-too-large",
            actualBytes,
            maxBytes: Link.maxLinkFrameBytes,
          },
        });
        expect(yield* Deferred.isDone(deliveryReceived)).toBe(false);
        yield* checkpoint.changes(subagentId).pipe(
          Stream.filter((record) => record.status === "idle"),
          Stream.runHead,
        );
        expect(yield* testHost.active).toEqual([subagentId]);

        const smallMessageId = yield* ref.send("Review the diff.");

        expect(smallMessageId).toMatch(/^msg_[a-z0-9]{24}$/);
        expect(smallMessageId).not.toBe(oversizedMessageId);

        expect(yield* Fiber.join(delivery)).toEqual(
          Option.some({
            kind: "message",
            content: "Review the diff.",
          }),
        );

        yield* child.send({ kind: "message", content: "Reviewed." });

        expect(
          yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
        ).toEqual({
          subagentId,
          event: { kind: "message", content: "Reviewed." },
        });
        expect((yield* checkpoint.get(subagentId)).status).toBe("idle");
        yield* Fiber.join(capacityHolder);

        yield* rootSupervisor.kill(subagentId);
        expect((yield* checkpoint.get(subagentId)).status).toBe("killed");
        expect(yield* testHost.active).toEqual([]);
        yield* Deferred.succeed(observeRegistryMiss, undefined);

        const error = yield* Effect.gen(function* () {
          const inactiveError = yield* rootSupervisor.kill(subagentId).pipe(Effect.flip);

          if (!(yield* Deferred.isDone(registryMiss))) {
            return yield* Effect.fail("Process is still retained");
          }

          return inactiveError;
        }).pipe(Effect.eventually);

        expect(error).toBeInstanceOf(SubagentKillInactiveError);
        expect((yield* checkpoint.get(subagentId)).status).toBe("killed");
        yield* testHost.verify;
      }).pipe(
        Effect.scoped,
        Effect.provide(
          RootSupervisor.layerNoDeps.pipe(
            Layer.provideMerge(SubagentEventOutbox.layer),
            Layer.provide(Layer.succeed(SubagentCheckpoint, observedCheckpoint)),
            Layer.provideMerge(SubagentRegistry.layer),
            Layer.provideMerge(TestHost.layer),
            Layer.provide(
              Layer.succeed(
                SubagentHarness,
                SubagentHarness.of({
                  makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
                }),
              ),
            ),
            Layer.provideMerge(SubagentCapacity.layer(1)),
            Layer.provideMerge(UnixSocketTransport.layer),
            Layer.provide(NodeFileSystem.layer),
          ),
        ),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("projects killing a queued persistent subagent as killed", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const rootSupervisor = yield* RootSupervisor;
      const eventOutbox = yield* SubagentEventOutbox;
      const capacity = yield* SubagentCapacity;
      const registry = yield* SubagentRegistry;
      const testHost = yield* TestHost;

      yield* testHost.stub([null]);

      const subagentId = yield* rootSupervisor.create({
        title: "Queued assistant",
        prompt: "Stand by.",
        cwd: "/worktree",
        mode: "persistent",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* Protocol.connect(subagentId);

      yield* child.send({ kind: "message", content: "Ready." });
      yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));
      yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((record) => record.status === "idle"),
        Stream.runHead,
      );

      const ref = yield* Effect.fromNullishOr(yield* registry.lookup(subagentId));
      const capacityAcquired = yield* Deferred.make<void>();
      const releaseCapacity = yield* Deferred.make<void>();
      const capacityHolder = yield* capacity
        .withPermit(
          Deferred.succeed(capacityAcquired, undefined).pipe(
            Effect.andThen(Deferred.await(releaseCapacity)),
          ),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(capacityAcquired);
      yield* ref.send("Review the diff.");
      yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((record) => record.status === "queued"),
        Stream.runHead,
      );

      yield* rootSupervisor.kill(subagentId);

      expect((yield* checkpoint.get(subagentId)).status).toBe("killed");
      expect(yield* testHost.active).toEqual([]);
      yield* Deferred.succeed(releaseCapacity, undefined);
      yield* Fiber.join(capacityHolder);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        RootSupervisor.layerNoDeps.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provideMerge(SubagentCapacity.layer(1)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("preserves exited when exit races with kill", () =>
    Effect.gen(function* () {
      const exitProjected = yield* Deferred.make<void>();
      const releaseProjection = yield* Deferred.make<void>();
      const checkpoint = yield* SubagentCheckpoint.make;
      const controlledCheckpoint = SubagentCheckpoint.of({
        ...checkpoint,
        update: Effect.fn("ControlledSubagentCheckpoint.update")(function* (
          subagentId: SubagentId,
          fields: Partial<Omit<SubagentRecord, "subagentId">>,
        ) {
          yield* checkpoint.update(subagentId, fields);

          if (fields.status === "exited") {
            yield* Deferred.succeed(exitProjected, undefined);
            yield* Deferred.await(releaseProjection);
          }
        }),
      });

      yield* Effect.gen(function* () {
        const rootSupervisor = yield* RootSupervisor;
        const testHost = yield* TestHost;

        yield* testHost.stub([null]);

        const subagentId = yield* rootSupervisor.create({
          title: "Completing assistant",
          prompt: "Complete the task.",
          cwd: "/worktree",
          mode: "ephemeral",
        });

        expect(yield* testHost.takeStart).toBe(subagentId);

        const child = yield* Protocol.connect(subagentId);

        yield* child.send({ kind: "message", content: "Task complete." });
        yield* Deferred.await(exitProjected);

        expect(yield* checkpoint.get(subagentId)).toMatchObject({
          status: "exited",
          latestEvent: { kind: "message", content: "Task complete." },
        });

        const kill = yield* rootSupervisor
          .kill(subagentId)
          .pipe(Effect.flip, Effect.forkChild({ startImmediately: true }));

        expect(kill.pollUnsafe()).toBeUndefined();
        yield* Deferred.succeed(releaseProjection, undefined);

        const error = yield* Fiber.join(kill);

        expect(error).toBeInstanceOf(SubagentKillInactiveError);
        expect(yield* checkpoint.get(subagentId)).toMatchObject({
          status: "exited",
          latestEvent: { kind: "message", content: "Task complete." },
        });
        yield* testHost.verify;
      }).pipe(
        Effect.scoped,
        Effect.provide(
          RootSupervisor.layerNoDeps.pipe(
            Layer.provideMerge(SubagentEventOutbox.layer),
            Layer.provide(Layer.succeed(SubagentCheckpoint, controlledCheckpoint)),
            Layer.provideMerge(SubagentRegistry.layer),
            Layer.provideMerge(TestHost.layer),
            Layer.provide(
              Layer.succeed(
                SubagentHarness,
                SubagentHarness.of({
                  makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
                }),
              ),
            ),
            Layer.provide(SubagentCapacity.layer(10)),
            Layer.provideMerge(UnixSocketTransport.layer),
            Layer.provide(NodeFileSystem.layer),
          ),
        ),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("fails killing an unknown subagent", () =>
    Effect.gen(function* () {
      const rootSupervisor = yield* RootSupervisor;
      const subagentId = yield* decodeSubagentId("sa_12345678_unknown");
      const error = yield* rootSupervisor.kill(subagentId).pipe(Effect.flip);

      expect(error).toBeInstanceOf(SubagentKillUnknownError);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        RootSupervisor.layerNoDeps.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("removes a naturally exited subagent from the registry", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const rootSupervisor = yield* RootSupervisor;
      const eventOutbox = yield* SubagentEventOutbox;
      const registry = yield* SubagentRegistry;
      const testHost = yield* TestHost;

      yield* testHost.stub([null]);

      const subagentId = yield* rootSupervisor.create({
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      expect(yield* registry.lookup(subagentId)).toBeDefined();
      expect(yield* testHost.takeStart).toBe(subagentId);

      const child = yield* Protocol.connect(subagentId);

      yield* child.send({ kind: "message", content: "Task complete." });
      yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect((yield* checkpoint.get(subagentId)).status).toBe("exited");
      yield* Effect.gen(function* () {
        if ((yield* registry.lookup(subagentId)) !== undefined) {
          return yield* Effect.fail("Subagent reference is still registered");
        }

        return yield* Effect.void;
      }).pipe(Effect.eventually);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        RootSupervisor.layerNoDeps.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("removes a naturally exited subagent before a late kill", () =>
    Effect.gen(function* () {
      const observeRegistryMiss = yield* Deferred.make<void>();
      const registryMiss = yield* Deferred.make<void>();
      const checkpoint = yield* SubagentCheckpoint.make;
      const observedCheckpoint = SubagentCheckpoint.of({
        ...checkpoint,
        has: Effect.fn("ObservedSubagentCheckpoint.has")(function* (subagentId: SubagentId) {
          if (yield* Deferred.isDone(observeRegistryMiss)) {
            yield* Deferred.succeed(registryMiss, undefined);
          }

          return yield* checkpoint.has(subagentId);
        }),
      });

      yield* Effect.gen(function* () {
        const rootSupervisor = yield* RootSupervisor;
        const eventOutbox = yield* SubagentEventOutbox;
        const testHost = yield* TestHost;

        yield* testHost.stub([null]);

        const subagentId = yield* rootSupervisor.create({
          title: "Review API",
          prompt: "Complete the task.",
          cwd: "/worktree",
          mode: "ephemeral",
        });

        expect(yield* testHost.takeStart).toBe(subagentId);

        const child = yield* Protocol.connect(subagentId);

        yield* child.send({ kind: "message", content: "Task complete." });
        yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

        expect((yield* checkpoint.get(subagentId)).status).toBe("exited");
        yield* Deferred.succeed(observeRegistryMiss, undefined);

        const error = yield* Effect.gen(function* () {
          const inactiveError = yield* rootSupervisor.kill(subagentId).pipe(Effect.flip);

          if (!(yield* Deferred.isDone(registryMiss))) {
            return yield* Effect.fail("Process is still retained");
          }

          return inactiveError;
        }).pipe(Effect.eventually);

        expect(error).toBeInstanceOf(SubagentKillInactiveError);
        expect((yield* checkpoint.get(subagentId)).status).toBe("exited");
        yield* testHost.verify;
      }).pipe(
        Effect.scoped,
        Effect.provide(
          RootSupervisor.layerNoDeps.pipe(
            Layer.provideMerge(SubagentEventOutbox.layer),
            Layer.provide(Layer.succeed(SubagentCheckpoint, observedCheckpoint)),
            Layer.provideMerge(SubagentRegistry.layer),
            Layer.provideMerge(TestHost.layer),
            Layer.provide(
              Layer.succeed(
                SubagentHarness,
                SubagentHarness.of({
                  makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
                }),
              ),
            ),
            Layer.provide(SubagentCapacity.layer(10)),
            Layer.provideMerge(UnixSocketTransport.layer),
            Layer.provide(NodeFileSystem.layer),
          ),
        ),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("removes a failed subagent from the registry", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const rootSupervisor = yield* RootSupervisor;
      const eventOutbox = yield* SubagentEventOutbox;
      const registry = yield* SubagentRegistry;
      const testHost = yield* TestHost;
      const errorSubagentId = yield* decodeSubagentId("sa_12345678_unavailable");

      yield* testHost.stub([
        SubagentHostUnavailableError.make({
          subagentId: errorSubagentId,
          host: "test",
          reason: "Host unavailable",
        }),
      ]);

      const subagentId = yield* rootSupervisor.create({
        title: "Failed task",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      expect(
        yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
      ).toEqual({
        subagentId,
        event: { kind: "failure", reason: "Host unavailable" },
      });
      expect(yield* checkpoint.get(subagentId)).toMatchObject({
        status: "failed",
        latestEvent: { kind: "failure", reason: "Host unavailable" },
      });
      yield* Effect.gen(function* () {
        if ((yield* registry.lookup(subagentId)) !== undefined) {
          return yield* Effect.fail("Subagent reference is still registered");
        }

        return yield* Effect.void;
      }).pipe(Effect.eventually);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        RootSupervisor.layerNoDeps.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("does not project RootSupervisor shutdown as killed", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestHost;

      yield* testHost.stub([null]);

      const subagentId = yield* Effect.scoped(
        Effect.gen(function* () {
          const rootSupervisor = yield* RootSupervisor;
          const admittedSubagentId = yield* rootSupervisor.create({
            title: "Interrupted",
            prompt: "Complete the task.",
            cwd: "/worktree",
            mode: "ephemeral",
          });

          expect(yield* testHost.takeStart).toBe(admittedSubagentId);
          return admittedSubagentId;
        }).pipe(
          Effect.provide(
            RootSupervisor.layerNoDeps.pipe(
              Layer.provideMerge(SubagentEventOutbox.layer),
              Layer.provideMerge(SubagentRegistry.layer),
            ),
          ),
        ),
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
          Layer.provideMerge(SubagentCapacity.layer(10)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("interrupts in-flight admission before completing root shutdown", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint.make;
      const registry = yield* SubagentRegistry.make();
      const registering = yield* Deferred.make<SubagentId>();
      const releaseRegistration = yield* Deferred.make<void>();
      const controlledRegistry = SubagentRegistry.of({
        ...registry,
        register: Effect.fn("ControlledSubagentRegistry.register")(function* (subagentId, ref) {
          yield* Deferred.succeed(registering, subagentId);
          yield* Deferred.await(releaseRegistration);
          yield* registry.register(subagentId, ref);
        }),
      });
      const runtime = ManagedRuntime.make(
        RootSupervisor.layerNoDeps.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provide(Layer.succeed(SubagentCheckpoint, checkpoint)),
          Layer.provide(Layer.succeed(SubagentRegistry, controlledRegistry)),
          Layer.provideMerge(TestHost.layer),
          Layer.provide(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.never,
              }),
            ),
          ),
          Layer.provide(SubagentCapacity.layer(10)),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      );

      yield* Effect.gen(function* () {
        const creating = runtime.runFork(
          RootSupervisor.use((rootSupervisor) =>
            rootSupervisor.create({
              title: "Interrupted admission",
              prompt: "Wait.",
              cwd: "/worktree",
              mode: "persistent",
            }),
          ),
        );
        const subagentId = yield* Deferred.await(registering);
        const disposing = yield* Effect.promise(() => runtime.dispose()).pipe(
          Effect.forkChild({ startImmediately: true }),
        );

        yield* Effect.yieldNow;
        expect(disposing.pollUnsafe()).toBeUndefined();

        yield* Deferred.succeed(releaseRegistration, undefined);
        yield* Fiber.join(disposing);

        const creationExit = yield* Fiber.await(creating);

        expect(Exit.isFailure(creationExit) && Cause.hasInterruptsOnly(creationExit.cause)).toBe(
          true,
        );
        expect(yield* registry.lookup(subagentId)).toBeUndefined();
        expect(yield* checkpoint.get(subagentId)).toMatchObject({
          status: "queued",
          title: "Interrupted admission",
        });
      }).pipe(Effect.ensuring(Effect.promise(() => runtime.dispose())));
    }),
  );

  it.effect("releases live children when its ManagedRuntime is disposed", () => {
    const runtime = ManagedRuntime.make(
      RootSupervisor.layerNoDeps.pipe(
        Layer.provideMerge(SubagentEventOutbox.layer),
        Layer.provideMerge(SubagentCheckpoint.layer),
        Layer.provideMerge(SubagentRegistry.layer),
        Layer.provideMerge(TestHost.layer),
        Layer.provide(
          Layer.succeed(
            SubagentHarness,
            SubagentHarness.of({
              makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
            }),
          ),
        ),
        Layer.provide(SubagentCapacity.layer(10)),
        Layer.provideMerge(UnixSocketTransport.layer),
        Layer.provide(NodeFileSystem.layer),
      ),
    );

    return Effect.gen(function* () {
      runtime.runFork(
        SubagentEventOutbox.use((eventOutbox) => eventOutbox.events.pipe(Stream.runDrain)),
      );

      const parentScope = yield* Scope.Scope;
      const childScope = yield* Scope.fork(parentScope);
      const { checkpoint, subagentId, testHost, transport } = yield* Effect.promise(() =>
        runtime.runPromise(
          Effect.gen(function* () {
            const runtimeTransport = yield* SubagentLinkTransport;
            const runtimeCheckpoint = yield* SubagentCheckpoint;
            const rootSupervisor = yield* RootSupervisor;
            const runtimeTestHost = yield* TestHost;

            yield* runtimeTestHost.stub([null]);

            const admittedSubagentId = yield* rootSupervisor.create({
              title: "Interrupted",
              prompt: "Complete the task.",
              cwd: "/worktree",
              mode: "ephemeral",
            });

            expect(yield* runtimeTestHost.takeStart).toBe(admittedSubagentId);
            return {
              checkpoint: runtimeCheckpoint,
              subagentId: admittedSubagentId,
              testHost: runtimeTestHost,
              transport: runtimeTransport,
            };
          }),
        ),
      );

      yield* Protocol.connect(subagentId).pipe(
        Effect.provideService(SubagentLinkTransport, transport),
        Scope.provide(childScope),
      );
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

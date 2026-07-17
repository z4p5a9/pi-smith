import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Option, Schema, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";

import { TestSubagentHost } from "../testing/TestSubagentHost.ts";
import { SubagentBridge, SubagentBridgeDisconnectedError } from "./SubagentBridge.ts";
import { decodeSubagentId } from "./SubagentId.ts";
import { SubagentNotRegisteredError, SubagentRegistry } from "./SubagentRegistry.ts";
import { SubagentAlreadyStartedError, SubagentSupervisor } from "./SubagentSupervisor.ts";
import { layer as unixSocketSubagentBridgeTransportLayer } from "./UnixSocketSubagentBridgeTransport.ts";

it.describe("SubagentSupervisor", () => {
  it.effect("starts a subagent child", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const supervisor = yield* SubagentSupervisor;
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_supervisor-start");

      yield* testHost.stub([{ hostId: "test-host" }]);

      const started = yield* supervisor
        .start(subagentId, {
          title: "Review API",
          prompt: "Complete the task.",
          cwd: "/worktree",
        })
        .pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);

      const connection = yield* bridge.connect(subagentId);
      const sentReady = yield* connection
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const child = yield* Fiber.join(started);
      yield* Fiber.join(sentReady);

      const result = yield* child.await.pipe(Effect.timeoutOption("1 millis"), Effect.forkChild);

      yield* TestClock.adjust("1 millis");

      expect(Option.isNone(yield* Fiber.join(result))).toBe(true);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.merge(
          SubagentSupervisor.layer,
          TestSubagentHost.layer.pipe(
            Layer.provideMerge(
              SubagentBridge.layer.pipe(
                Layer.provide(unixSocketSubagentBridgeTransportLayer),
                Layer.provide(NodeFileSystem.layer),
              ),
            ),
          ),
        ),
      ),
    ),
  );

  it.effect("rejects one of two concurrent starts for the same subagent ID", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const supervisor = yield* SubagentSupervisor;
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_supervisor-duplicate");

      yield* testHost.stub([{ hostId: "test-host" }]);

      const start = supervisor
        .start(subagentId, { title: "Review API", prompt: "Complete the task.", cwd: "/worktree" })
        .pipe(
          Effect.as("started" as const),
          Effect.catchTag("SubagentAlreadyStartedError", Effect.succeed),
        );
      const pendingResults = yield* Effect.all([start, start], { concurrency: "unbounded" }).pipe(
        Effect.forkChild({ startImmediately: true }),
      );

      expect(yield* testHost.takeStart).toBe(subagentId);

      const connection = yield* bridge.connect(subagentId);
      const sentReady = yield* connection
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const results = yield* Fiber.join(pendingResults);
      yield* Fiber.join(sentReady);

      expect(results.filter((result) => result === "started")).toHaveLength(1);
      expect(results.filter(Schema.is(SubagentAlreadyStartedError))).toHaveLength(1);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.merge(
          SubagentSupervisor.layer,
          TestSubagentHost.layer.pipe(
            Layer.provideMerge(
              SubagentBridge.layer.pipe(
                Layer.provide(unixSocketSubagentBridgeTransportLayer),
                Layer.provide(NodeFileSystem.layer),
              ),
            ),
          ),
        ),
      ),
    ),
  );

  it.effect("registers a running child", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const supervisor = yield* SubagentSupervisor;
      const registry = yield* SubagentRegistry;
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_supervisor-registry");

      yield* testHost.stub([{ hostId: "test-host" }]);

      const started = yield* supervisor
        .start(subagentId, {
          title: "Review API",
          prompt: "Complete the task.",
          cwd: "/worktree",
        })
        .pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);

      const connection = yield* bridge.connect(subagentId);
      const sentReady = yield* connection
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* Fiber.join(started);
      yield* Fiber.join(sentReady);

      const process = yield* registry.get(subagentId);

      expect(yield* process.status).toBe("running");
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.merge(
          SubagentSupervisor.layer,
          TestSubagentHost.layer.pipe(
            Layer.provideMerge(
              SubagentBridge.layer.pipe(
                Layer.provide(unixSocketSubagentBridgeTransportLayer),
                Layer.provide(NodeFileSystem.layer),
              ),
            ),
          ),
        ),
      ),
    ),
  );

  it.effect("forwards acknowledged events independently from the child scope", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const supervisor = yield* SubagentSupervisor;
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_supervisor-events");
      const parentScope = yield* Scope.Scope;
      const processMessage = yield* Deferred.make<void>();
      const received = yield* supervisor.events.pipe(
        Stream.take(2),
        Stream.mapEffect((event) =>
          event.event.kind === "message"
            ? Deferred.await(processMessage).pipe(Effect.as(event))
            : Effect.succeed(event),
        ),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );

      yield* testHost.stub([{ hostId: "test-host" }]);

      const started = yield* supervisor
        .start(subagentId, {
          title: "Review API",
          prompt: "Complete the task.",
          cwd: "/worktree",
        })
        .pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);

      const childScope = yield* Scope.fork(parentScope);
      const connection = yield* bridge.connect(subagentId).pipe(Scope.provide(childScope));
      const sentReady = yield* connection
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const child = yield* Fiber.join(started);
      yield* Fiber.join(sentReady);

      yield* connection.sendEvent({ kind: "message", content: "Task complete." });
      yield* Scope.close(childScope, Exit.void);
      yield* Deferred.succeed(processMessage, undefined);

      expect(Array.from(yield* Fiber.join(received))).toEqual([
        { subagentId, event: { kind: "ready" } },
        { subagentId, event: { kind: "message", content: "Task complete." } },
      ]);
      expect(Schema.is(SubagentBridgeDisconnectedError)(yield* child.await.pipe(Effect.flip))).toBe(
        true,
      );
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.merge(
          SubagentSupervisor.layer,
          TestSubagentHost.layer.pipe(
            Layer.provideMerge(
              SubagentBridge.layer.pipe(
                Layer.provide(unixSocketSubagentBridgeTransportLayer),
                Layer.provide(NodeFileSystem.layer),
              ),
            ),
          ),
        ),
      ),
    ),
  );

  it.effect("propagates child failure after cleanup", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const supervisor = yield* SubagentSupervisor;
      const registry = yield* SubagentRegistry;
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_supervisor-cleanup");
      const parentScope = yield* Scope.Scope;

      yield* testHost.stub([{ hostId: "test-host" }]);

      const started = yield* supervisor
        .start(subagentId, {
          title: "Review API",
          prompt: "Complete the task.",
          cwd: "/worktree",
        })
        .pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);

      const childScope = yield* Scope.fork(parentScope);
      const connection = yield* bridge.connect(subagentId).pipe(Scope.provide(childScope));
      const sentReady = yield* connection
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const child = yield* Fiber.join(started);
      yield* Fiber.join(sentReady);

      yield* Scope.close(childScope, Exit.void);

      const error = yield* child.await.pipe(Effect.flip);
      const registryError = yield* registry.get(subagentId).pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeDisconnectedError)(error)).toBe(true);
      expect(Schema.is(SubagentNotRegisteredError)(registryError)).toBe(true);
      expect(yield* testHost.active).toEqual([]);
      yield* bridge.listen(subagentId).pipe(Effect.scoped);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        Layer.merge(
          SubagentSupervisor.layer,
          TestSubagentHost.layer.pipe(
            Layer.provideMerge(
              SubagentBridge.layer.pipe(
                Layer.provide(unixSocketSubagentBridgeTransportLayer),
                Layer.provide(NodeFileSystem.layer),
              ),
            ),
          ),
        ),
      ),
    ),
  );
});

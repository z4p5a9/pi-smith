import { expect, it } from "@effect/vitest";
import { Effect, Fiber, Layer, Option, Schema } from "effect";
import { TestClock } from "effect/testing";

import { TestSubagentBridge } from "../testing/TestSubagentBridge.ts";
import { TestSubagentHost } from "../testing/TestSubagentHost.ts";
import { SubagentBridgeDisconnectedError } from "./SubagentBridge.ts";
import { decodeSubagentId } from "./SubagentId.ts";
import { SubagentNotRegisteredError, SubagentRegistry } from "./SubagentRegistry.ts";
import { SubagentAlreadyStartedError, SubagentSupervisor } from "./SubagentSupervisor.ts";

it.describe("SubagentSupervisor", () => {
  it.effect("starts a subagent child", () =>
    Effect.gen(function* () {
      const supervisor = yield* SubagentSupervisor;
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* testHost.stub([{ hostId: "test-host" }]);

      const child = yield* supervisor.start(subagentId, {
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });
      const result = yield* child.await.pipe(Effect.timeoutOption("1 millis"), Effect.forkChild);

      yield* TestClock.adjust("1 millis");

      expect(Option.isNone(yield* Fiber.join(result))).toBe(true);
      yield* testHost.verify;
    }).pipe(
      Effect.provide(
        Layer.merge(
          SubagentSupervisor.layer,
          TestSubagentHost.layer.pipe(Layer.provideMerge(TestSubagentBridge.layer)),
        ),
      ),
    ),
  );

  it.effect("rejects one of two concurrent starts for the same subagent ID", () =>
    Effect.gen(function* () {
      const supervisor = yield* SubagentSupervisor;
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* testHost.stub([{ hostId: "test-host" }]);

      const start = supervisor
        .start(subagentId, { title: "Review API", prompt: "Complete the task.", cwd: "/worktree" })
        .pipe(
          Effect.as("started" as const),
          Effect.catchTag("SubagentAlreadyStartedError", Effect.succeed),
        );
      const results = yield* Effect.all([start, start], { concurrency: "unbounded" });

      expect(results.filter((result) => result === "started")).toHaveLength(1);
      expect(results.filter(Schema.is(SubagentAlreadyStartedError))).toHaveLength(1);
      yield* testHost.verify;
    }).pipe(
      Effect.provide(
        Layer.merge(
          SubagentSupervisor.layer,
          TestSubagentHost.layer.pipe(Layer.provideMerge(TestSubagentBridge.layer)),
        ),
      ),
    ),
  );

  it.effect("registers a running child", () =>
    Effect.gen(function* () {
      const supervisor = yield* SubagentSupervisor;
      const registry = yield* SubagentRegistry;
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* testHost.stub([{ hostId: "test-host" }]);
      yield* supervisor.start(subagentId, {
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });
      const process = yield* registry.get(subagentId);

      expect(yield* process.status).toBe("running");
      yield* testHost.verify;
    }).pipe(
      Effect.provide(
        Layer.merge(
          SubagentSupervisor.layer,
          TestSubagentHost.layer.pipe(Layer.provideMerge(TestSubagentBridge.layer)),
        ),
      ),
    ),
  );

  it.effect("propagates child failure after cleanup", () =>
    Effect.gen(function* () {
      const supervisor = yield* SubagentSupervisor;
      const registry = yield* SubagentRegistry;
      const testBridge = yield* TestSubagentBridge;
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* testHost.stub([{ hostId: "test-host" }]);

      const child = yield* supervisor.start(subagentId, {
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      yield* testBridge.disconnect(subagentId);

      const error = yield* child.await.pipe(Effect.flip);
      const registryError = yield* registry.get(subagentId).pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeDisconnectedError)(error)).toBe(true);
      expect(Schema.is(SubagentNotRegisteredError)(registryError)).toBe(true);
      expect(yield* testHost.active).toEqual([]);
      expect(yield* testBridge.isListening(subagentId)).toBe(false);
      expect(yield* testBridge.isConnected(subagentId)).toBe(false);
      yield* testHost.verify;
    }).pipe(
      Effect.provide(
        Layer.merge(
          SubagentSupervisor.layer,
          TestSubagentHost.layer.pipe(Layer.provideMerge(TestSubagentBridge.layer)),
        ),
      ),
    ),
  );
});

import { expect, it } from "@effect/vitest";
import { Effect, Fiber, Option, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";

import { SubagentBridge, SubagentBridgeDisconnectedError } from "../subagent/SubagentBridge.ts";
import { decodeSubagentId } from "../subagent/SubagentId.ts";
import { TestSubagentBridge } from "./TestSubagentBridge.ts";

it.describe("TestSubagentBridge", () => {
  it.effect("connects paired sessions and disconnects them", () =>
    Effect.gen(function* () {
      const testBridge = yield* TestSubagentBridge;
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const listener = yield* bridge.listen(subagentId);
      const childSession = yield* bridge.connect(subagentId);

      yield* childSession.sendEvent({ kind: "ready" });

      const rootSession = yield* listener.accept;

      expect(yield* testBridge.isListening(subagentId)).toBe(true);
      expect(yield* testBridge.isConnected(subagentId)).toBe(true);
      expect(yield* testBridge.calls).toEqual([
        { operation: "listen", subagentId },
        { operation: "connect", subagentId },
      ]);

      yield* testBridge.disconnect(subagentId);

      const [rootError, childError] = yield* Effect.all([
        rootSession.await.pipe(Effect.flip),
        childSession.await.pipe(Effect.flip),
      ]);

      expect(Schema.is(SubagentBridgeDisconnectedError)(rootError)).toBe(true);
      expect(Schema.is(SubagentBridgeDisconnectedError)(childError)).toBe(true);
      expect(yield* testBridge.isConnected(subagentId)).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(TestSubagentBridge.layer)),
  );

  it.effect("blocks and unblocks a connection deterministically", () =>
    Effect.gen(function* () {
      const testBridge = yield* TestSubagentBridge;
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* testBridge.block(subagentId);

      const listener = yield* bridge.listen(subagentId);
      const connection = yield* bridge
        .connect(subagentId)
        .pipe(Effect.forkScoped({ startImmediately: true }));
      const acceptance = yield* listener.accept.pipe(
        Effect.timeoutOption("1 millis"),
        Effect.forkChild,
      );

      yield* TestClock.adjust("1 millis");

      expect(Option.isNone(yield* Fiber.join(acceptance))).toBe(true);
      expect(yield* testBridge.isConnected(subagentId)).toBe(false);

      yield* testBridge.unblock(subagentId);
      const childSession = yield* Fiber.join(connection);

      yield* childSession.sendEvent({ kind: "ready" });
      yield* listener.accept;

      expect(yield* testBridge.isConnected(subagentId)).toBe(true);
    }).pipe(Effect.scoped, Effect.provide(TestSubagentBridge.layer)),
  );

  it.effect("delivers subagent events", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const listener = yield* bridge.listen(subagentId);
      const childSession = yield* bridge.connect(subagentId);

      yield* childSession.sendEvent({ kind: "ready" });

      const rootSession = yield* listener.accept;
      const received = yield* rootSession.events.pipe(
        Stream.take(2),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );

      yield* childSession.sendEvent({ kind: "message", content: "Task complete." });

      expect(Array.from(yield* Fiber.join(received))).toEqual([
        { kind: "ready" },
        { kind: "message", content: "Task complete." },
      ]);
    }).pipe(Effect.scoped, Effect.provide(TestSubagentBridge.layer)),
  );

  it.effect("releases listening and connection state with the scope", () =>
    Effect.gen(function* () {
      const testBridge = yield* TestSubagentBridge;
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* Effect.scoped(
        Effect.gen(function* () {
          const listener = yield* bridge.listen(subagentId);

          const childSession = yield* bridge.connect(subagentId);

          yield* childSession.sendEvent({ kind: "ready" });
          yield* listener.accept;

          expect(yield* testBridge.isListening(subagentId)).toBe(true);
          expect(yield* testBridge.isConnected(subagentId)).toBe(true);
        }),
      );

      expect(yield* testBridge.isListening(subagentId)).toBe(false);
      expect(yield* testBridge.isConnected(subagentId)).toBe(false);
    }).pipe(Effect.provide(TestSubagentBridge.layer)),
  );
});

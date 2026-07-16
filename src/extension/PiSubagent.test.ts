import { fileURLToPath } from "node:url";

import { NodeFileSystem } from "@effect/platform-node";
import { discoverAndLoadExtensions, SessionManager } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "@effect/vitest";
import { Effect, Fiber, Layer, Schema, Stream } from "effect";

import { SubagentBridge, SubagentBridgeDisconnectedError } from "../subagent/SubagentBridge.ts";
import { decodeSubagentId } from "../subagent/SubagentId.ts";
import { layer as unixSocketSubagentBridgeTransportLayer } from "../subagent/UnixSocketSubagentBridgeTransport.ts";

it.describe("Pi subagent extension", () => {
  it.effect("reports settled Pi outcomes and closes the bridge session", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_review-api");

    return Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const listener = yield* bridge.listen(subagentId);
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./PiSubagent.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );
      const loaded = yield* Effect.fromNullishOr(result.extensions[0]);
      const start = yield* Effect.fromNullishOr(loaded.handlers.get("session_start")?.[0]);

      const starting = yield* Effect.promise(() => start()).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const session = yield* listener.accept;
      const ready = yield* session.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      yield* ready.acknowledge;
      yield* Fiber.join(starting);

      expect(result.errors).toEqual([]);
      expect(loaded.tools.size).toBe(0);

      const sessionManager = SessionManager.inMemory("/tmp/smith-extension-test");
      const settle = yield* Effect.fromNullishOr(loaded.handlers.get("agent_settled")?.[0]);
      const settlingWithoutResponse = yield* Effect.promise(() =>
        settle({ type: "agent_settled" }, { sessionManager }),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      const missingResponse = yield* session.events.pipe(
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );

      expect(missingResponse.event).toEqual({
        kind: "failure",
        reason: "Pi settled without an assistant response",
      });

      yield* missingResponse.acknowledge;
      yield* Fiber.join(settlingWithoutResponse);

      sessionManager.appendMessage({
        role: "assistant",
        content: [
          { type: "text", text: "Task complete." },
          { type: "thinking", thinking: "Done." },
          { type: "text", text: "Files updated." },
        ],
        api: "anthropic-messages",
        provider: "anthropic",
        model: "test-model",
        usage: {
          input: 0,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "stop",
        timestamp: 0,
      });
      const settling = yield* Effect.promise(() =>
        settle({ type: "agent_settled" }, { sessionManager }),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      const report = yield* session.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(report.event).toEqual({
        kind: "message",
        content: "Task complete.\nFiles updated.",
      });

      yield* report.acknowledge;
      yield* Fiber.join(settling);

      sessionManager.appendMessage({
        role: "assistant",
        content: [],
        api: "anthropic-messages",
        provider: "anthropic",
        model: "test-model",
        usage: {
          input: 0,
          output: 0,
          cacheRead: 0,
          cacheWrite: 0,
          totalTokens: 0,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
        },
        stopReason: "error",
        errorMessage: "Model request failed",
        timestamp: 1,
      });
      const settlingFailure = yield* Effect.promise(() =>
        settle({ type: "agent_settled" }, { sessionManager }),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      const failure = yield* session.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(failure.event).toEqual({ kind: "failure", reason: "Model request failed" });

      yield* failure.acknowledge;
      yield* Fiber.join(settlingFailure);

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);

      yield* Effect.promise(() => shutdown());
      const error = yield* session.await.pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeDisconnectedError)(error)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
      Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())),
    );
  });

  it.effect("rejects a missing subagent ID", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", undefined);

    return Effect.gen(function* () {
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./PiSubagent.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );

      expect(result.extensions).toEqual([]);
      expect(result.errors).toHaveLength(1);
    }).pipe(Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())));
  });

  it.effect("rejects an invalid subagent ID", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "invalid");

    return Effect.gen(function* () {
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./PiSubagent.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );

      expect(result.extensions).toEqual([]);
      expect(result.errors).toHaveLength(1);
    }).pipe(Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())));
  });

  it.effect("rejects an empty subagent ID", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "");

    return Effect.gen(function* () {
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./PiSubagent.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );

      expect(result.extensions).toEqual([]);
      expect(result.errors).toHaveLength(1);
    }).pipe(Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())));
  });
});

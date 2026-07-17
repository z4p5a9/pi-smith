import { fileURLToPath } from "node:url";

import { NodeFileSystem } from "@effect/platform-node";
import { discoverAndLoadExtensions, SessionManager } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "@effect/vitest";
import { Effect, Fiber, Layer, Stream } from "effect";

import { SubagentBridge } from "../subagent/SubagentBridge.ts";
import { decodeSubagentId } from "../subagent/SubagentId.ts";
import { layer as unixSocketSubagentBridgeTransportLayer } from "../subagent/UnixSocketSubagentBridgeTransport.ts";

it.describe("Pi subagent extension", () => {
  it.effect("reports a settled assistant response and shuts down after acknowledgement", () => {
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
      let shutdownRequested = false;
      const settle = yield* Effect.fromNullishOr(loaded.handlers.get("agent_settled")?.[0]);
      const settling = yield* Effect.promise(() =>
        settle(
          { type: "agent_settled" },
          {
            sessionManager,
            shutdown: () => {
              shutdownRequested = true;
            },
          },
        ),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      const report = yield* session.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(report.event).toEqual({
        kind: "message",
        content: "Task complete.\nFiles updated.",
      });
      expect(shutdownRequested).toBe(false);

      yield* report.acknowledge;
      yield* Fiber.join(settling);
      expect(shutdownRequested).toBe(true);

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);

      yield* Effect.promise(() => shutdown());
      yield* session.await;
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

  it.effect("reports a missing assistant response", () => {
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

      const sessionManager = SessionManager.inMemory("/tmp/smith-extension-test");
      let shutdownRequested = false;
      const settle = yield* Effect.fromNullishOr(loaded.handlers.get("agent_settled")?.[0]);
      const settling = yield* Effect.promise(() =>
        settle(
          { type: "agent_settled" },
          {
            sessionManager,
            shutdown: () => {
              shutdownRequested = true;
            },
          },
        ),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      const report = yield* session.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(report.event).toEqual({
        kind: "failure",
        reason: "Pi settled without an assistant response",
      });
      expect(shutdownRequested).toBe(false);

      yield* report.acknowledge;
      yield* Fiber.join(settling);
      expect(shutdownRequested).toBe(true);

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);

      yield* Effect.promise(() => shutdown());
      yield* session.await;
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

  it.effect("reports a failed assistant response", () => {
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

      const sessionManager = SessionManager.inMemory("/tmp/smith-extension-test");
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
        timestamp: 0,
      });
      let shutdownRequested = false;
      const settle = yield* Effect.fromNullishOr(loaded.handlers.get("agent_settled")?.[0]);
      const settling = yield* Effect.promise(() =>
        settle(
          { type: "agent_settled" },
          {
            sessionManager,
            shutdown: () => {
              shutdownRequested = true;
            },
          },
        ),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      const report = yield* session.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(report.event).toEqual({ kind: "failure", reason: "Model request failed" });
      expect(shutdownRequested).toBe(false);

      yield* report.acknowledge;
      yield* Fiber.join(settling);
      expect(shutdownRequested).toBe(true);

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);

      yield* Effect.promise(() => shutdown());
      yield* session.await;
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

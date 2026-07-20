import { fileURLToPath } from "node:url";

import { NodeFileSystem } from "@effect/platform-node";
import { discoverAndLoadExtensions, SessionManager } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "@effect/vitest";
import { Effect, Exit, Fiber, Layer, Scope } from "effect";

import * as Protocol from "../host/Protocol.ts";
import * as UnixSocketTransport from "../host/link/unix/UnixSocketTransport.ts";
import { decodeSubagentId } from "../subagent/SubagentId.ts";

it.describe("Pi subagent extension", () => {
  it.effect("reports a settled assistant response without shutting itself down", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_review-api");

    return Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const listener = yield* Protocol.listen(subagentId);
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./pi-subagent.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );
      const loaded = yield* Effect.fromNullishOr(result.extensions[0]);
      const start = yield* Effect.fromNullishOr(loaded.handlers.get("session_start")?.[0]);

      const starting = yield* Effect.promise(() =>
        start({ type: "session_start" }, { shutdown: () => undefined }),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      const session = yield* listener.accept;

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

      const report = yield* session.take;

      yield* Fiber.join(settling);

      expect(report).toEqual({
        kind: "message",
        content: "Task complete.\nFiles updated.",
      });
      expect(shutdownRequested).toBe(false);

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);

      yield* Effect.promise(() => shutdown());
      yield* session.await.pipe(Effect.exit);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
      Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())),
    );
  });

  it.effect("starts queued root messages FIFO, one per acknowledged settlement", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_review-api");

    return Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const listener = yield* Protocol.listen(subagentId);
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./pi-subagent.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );
      const loaded = yield* Effect.fromNullishOr(result.extensions[0]);
      const sent = vi.fn();
      result.runtime.sendMessage = (message, options) => {
        sent(message, options);
      };
      const start = yield* Effect.fromNullishOr(loaded.handlers.get("session_start")?.[0]);
      const starting = yield* Effect.promise(() =>
        start({ type: "session_start" }, { shutdown: () => undefined }),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      const session = yield* listener.accept;

      yield* Fiber.join(starting);
      yield* session.send("A");
      yield* session.send("B");
      yield* session.send("C");
      yield* Effect.yieldNow;

      expect(sent).not.toHaveBeenCalled();

      const sessionManager = SessionManager.inMemory("/tmp/smith-extension-test");
      sessionManager.appendMessage({
        role: "assistant",
        content: [{ type: "text", text: "Complete." }],
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
      const settle = yield* Effect.fromNullishOr(loaded.handlers.get("agent_settled")?.[0]);
      const initialSettlement = yield* Effect.promise(() =>
        settle({ type: "agent_settled" }, { sessionManager }),
      ).pipe(Effect.forkChild({ startImmediately: true }));

      yield* Effect.yieldNow;
      expect(sent).not.toHaveBeenCalled();

      expect(yield* session.take).toEqual({ kind: "message", content: "Complete." });
      yield* Fiber.join(initialSettlement);
      yield* Effect.suspend(() =>
        sent.mock.calls.length >= 1 ? Effect.void : Effect.fail("A was not started"),
      ).pipe(Effect.eventually);

      expect(sent).toHaveBeenCalledTimes(1);
      expect(sent).toHaveBeenLastCalledWith(
        { customType: "smith-root-message", content: "A", display: true },
        { triggerTurn: true },
      );

      const aSettlement = yield* Effect.promise(() =>
        settle({ type: "agent_settled" }, { sessionManager }),
      ).pipe(Effect.forkChild({ startImmediately: true }));

      yield* Effect.yieldNow;
      expect(sent).toHaveBeenCalledTimes(1);

      expect(yield* session.take).toEqual({ kind: "message", content: "Complete." });
      yield* Fiber.join(aSettlement);
      yield* Effect.suspend(() =>
        sent.mock.calls.length >= 2 ? Effect.void : Effect.fail("B was not started"),
      ).pipe(Effect.eventually);

      expect(sent).toHaveBeenCalledTimes(2);
      expect(sent).toHaveBeenLastCalledWith(
        { customType: "smith-root-message", content: "B", display: true },
        { triggerTurn: true },
      );

      const bSettlement = yield* Effect.promise(() =>
        settle({ type: "agent_settled" }, { sessionManager }),
      ).pipe(Effect.forkChild({ startImmediately: true }));

      yield* Effect.yieldNow;
      expect(sent).toHaveBeenCalledTimes(2);

      expect(yield* session.take).toEqual({ kind: "message", content: "Complete." });
      yield* Fiber.join(bSettlement);
      yield* Effect.suspend(() =>
        sent.mock.calls.length >= 3 ? Effect.void : Effect.fail("C was not started"),
      ).pipe(Effect.eventually);

      expect(sent).toHaveBeenCalledTimes(3);
      expect(sent).toHaveBeenLastCalledWith(
        { customType: "smith-root-message", content: "C", display: true },
        { triggerTurn: true },
      );

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);

      yield* Effect.promise(() => shutdown());
      yield* session.await.pipe(Effect.exit);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
      Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())),
    );
  });

  it.effect("does not start queued work when settlement reporting fails", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_review-api");

    return Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const listenerScope = yield* Scope.make();
      const listener = yield* Protocol.listen(subagentId).pipe(Scope.provide(listenerScope));
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./pi-subagent.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );
      const loaded = yield* Effect.fromNullishOr(result.extensions[0]);
      const sent = vi.fn();
      result.runtime.sendMessage = (message, options) => {
        sent(message, options);
      };
      const start = yield* Effect.fromNullishOr(loaded.handlers.get("session_start")?.[0]);
      const starting = yield* Effect.promise(() =>
        start({ type: "session_start" }, { shutdown: () => undefined }),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      const session = yield* listener.accept;

      yield* Fiber.join(starting);
      yield* session.send("A");
      yield* Scope.close(listenerScope, Exit.void);
      yield* session.await.pipe(Effect.exit);

      const sessionManager = SessionManager.inMemory("/tmp/smith-extension-test");
      const settle = yield* Effect.fromNullishOr(loaded.handlers.get("agent_settled")?.[0]);
      const settlement = yield* Effect.promise(() =>
        settle({ type: "agent_settled" }, { sessionManager }),
      ).pipe(Effect.exit);

      expect(Exit.isFailure(settlement)).toBe(true);
      expect(sent).not.toHaveBeenCalled();

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);

      yield* Effect.promise(() => shutdown());
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
      Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())),
    );
  });

  it.effect("reports a missing assistant response", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_review-api");

    return Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const listener = yield* Protocol.listen(subagentId);
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./pi-subagent.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );
      const loaded = yield* Effect.fromNullishOr(result.extensions[0]);
      const start = yield* Effect.fromNullishOr(loaded.handlers.get("session_start")?.[0]);
      const starting = yield* Effect.promise(() =>
        start({ type: "session_start" }, { shutdown: () => undefined }),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      const session = yield* listener.accept;

      yield* Fiber.join(starting);

      const sessionManager = SessionManager.inMemory("/tmp/smith-extension-test");
      const settle = yield* Effect.fromNullishOr(loaded.handlers.get("agent_settled")?.[0]);

      const settling = yield* Effect.promise(() =>
        settle({ type: "agent_settled" }, { sessionManager }),
      ).pipe(Effect.forkChild({ startImmediately: true }));

      const report = yield* session.take;

      yield* Fiber.join(settling);

      expect(report).toEqual({
        kind: "failure",
        reason: "Pi settled without an assistant response",
      });

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);

      yield* Effect.promise(() => shutdown());
      yield* session.await.pipe(Effect.exit);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
      Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())),
    );
  });

  it.effect("reports a failed assistant response", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_review-api");

    return Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const listener = yield* Protocol.listen(subagentId);
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./pi-subagent.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );
      const loaded = yield* Effect.fromNullishOr(result.extensions[0]);
      const start = yield* Effect.fromNullishOr(loaded.handlers.get("session_start")?.[0]);
      const starting = yield* Effect.promise(() =>
        start({ type: "session_start" }, { shutdown: () => undefined }),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      const session = yield* listener.accept;

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
      const settle = yield* Effect.fromNullishOr(loaded.handlers.get("agent_settled")?.[0]);

      const settling = yield* Effect.promise(() =>
        settle({ type: "agent_settled" }, { sessionManager }),
      ).pipe(Effect.forkChild({ startImmediately: true }));

      const report = yield* session.take;

      yield* Fiber.join(settling);

      expect(report).toEqual({ kind: "failure", reason: "Model request failed" });

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);

      yield* Effect.promise(() => shutdown());
      yield* session.await.pipe(Effect.exit);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
      Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())),
    );
  });

  it.effect("reports an aborted assistant response", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_review-api");

    return Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const listener = yield* Protocol.listen(subagentId);
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./pi-subagent.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );
      const loaded = yield* Effect.fromNullishOr(result.extensions[0]);
      const start = yield* Effect.fromNullishOr(loaded.handlers.get("session_start")?.[0]);
      const starting = yield* Effect.promise(() =>
        start({ type: "session_start" }, { shutdown: () => undefined }),
      ).pipe(Effect.forkChild({ startImmediately: true }));
      const session = yield* listener.accept;

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
        stopReason: "aborted",
        timestamp: 0,
      });
      const settle = yield* Effect.fromNullishOr(loaded.handlers.get("agent_settled")?.[0]);

      const settling = yield* Effect.promise(() =>
        settle({ type: "agent_settled" }, { sessionManager }),
      ).pipe(Effect.forkChild({ startImmediately: true }));

      const report = yield* session.take;

      yield* Fiber.join(settling);

      expect(report).toEqual({ kind: "failure", reason: "Request aborted" });

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);

      yield* Effect.promise(() => shutdown());
      yield* session.await.pipe(Effect.exit);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
      Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())),
    );
  });

  it.effect("requests shutdown when the link connection ends", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_review-api");

    return Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const listenerScope = yield* Scope.make();
      const listener = yield* Protocol.listen(subagentId).pipe(Scope.provide(listenerScope));
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./pi-subagent.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );
      const loaded = yield* Effect.fromNullishOr(result.extensions[0]);
      const start = yield* Effect.fromNullishOr(loaded.handlers.get("session_start")?.[0]);
      let shutdownRequested = false;

      const starting = yield* Effect.promise(() =>
        start(
          { type: "session_start" },
          {
            shutdown: () => {
              shutdownRequested = true;
            },
          },
        ),
      ).pipe(Effect.forkChild({ startImmediately: true }));

      yield* listener.accept;
      yield* Fiber.join(starting);

      expect(shutdownRequested).toBe(false);

      yield* Scope.close(listenerScope, Exit.void);
      yield* Effect.suspend(() =>
        shutdownRequested ? Effect.void : Effect.fail("Shutdown not requested"),
      ).pipe(Effect.eventually);

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);

      yield* Effect.promise(() => shutdown());
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
      Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())),
    );
  });

  it.effect("rejects a missing subagent ID", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", undefined);

    return Effect.gen(function* () {
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./pi-subagent.ts", import.meta.url))],
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
          [fileURLToPath(new URL("./pi-subagent.ts", import.meta.url))],
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
          [fileURLToPath(new URL("./pi-subagent.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );

      expect(result.extensions).toEqual([]);
      expect(result.errors).toHaveLength(1);
    }).pipe(Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())));
  });
});

import { fileURLToPath } from "node:url";

import { NodeFileSystem } from "@effect/platform-node";
import { discoverAndLoadExtensions, SessionManager } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Schema, Scope } from "effect";

import * as Protocol from "../../../host/Protocol.ts";
import { LinkDisconnectedError, LinkProtocolError } from "../../../host/link/Link.ts";
import { SubagentLinkTransport } from "../../../host/link/Transport.ts";
import * as UnixSocketTransport from "../../../host/link/unix/UnixSocketTransport.ts";
import { decodeSubagentId } from "../../../subagent/SubagentId.ts";

const encodeJson = Schema.encodeEffect(Schema.UnknownFromJsonString);

it.describe("Pi subagent extension", () => {
  it.effect("reports a settled assistant response without shutting itself down", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_review-api");

    return Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const listener = yield* Protocol.listen(subagentId);
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
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
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
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
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
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
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
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
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
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
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
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

  it.effect("logs a root protocol failure once before requesting shutdown once", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_extension-version");

    return Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_extension-version");
      const transport = yield* SubagentLinkTransport;
      const server = yield* transport.listen(subagentId);
      const sendInvalidFrame = yield* Deferred.make<void>();
      const acknowledgement = yield* encodeJson({ v: 1, subagentId, ack: 0 }).pipe(Effect.orDie);
      const invalidFrame = yield* encodeJson({
        v: 2,
        subagentId,
        seq: 0,
        data: { kind: "message", content: "Wrong version." },
      }).pipe(Effect.orDie);

      yield* server
        .run((socket) =>
          Effect.gen(function* () {
            const write = yield* socket.writer;
            let acknowledged = false;

            yield* socket.runString(() => {
              if (acknowledged) {
                return Effect.void;
              }

              acknowledged = true;

              return write(`${acknowledgement}\n`).pipe(
                Effect.andThen(Deferred.await(sendInvalidFrame)),
                Effect.andThen(write(`${invalidFrame}\n`)),
                Effect.orDie,
              );
            });
          }).pipe(Effect.scoped),
        )
        .pipe(Effect.forkScoped);

      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );
      const loaded = yield* Effect.fromNullishOr(result.extensions[0]);
      const start = yield* Effect.fromNullishOr(loaded.handlers.get("session_start")?.[0]);
      const shutdownObserved = yield* Deferred.make<void>();
      const observations: Array<ReadonlyArray<unknown>> = [];

      yield* Effect.acquireUseRelease(
        Effect.sync(() => {
          const log = console.log;
          console.log = (...args: Array<unknown>) => {
            observations.push(args.slice(1));
          };
          return log;
        }),
        () =>
          Effect.gen(function* () {
            const starting = yield* Effect.promise(() =>
              start(
                { type: "session_start" },
                {
                  shutdown: () => {
                    observations.push(["shutdown"]);
                    queueMicrotask(() => {
                      Deferred.doneUnsafe(shutdownObserved, Effect.void);
                    });
                  },
                },
              ),
            ).pipe(Effect.forkChild({ startImmediately: true }));

            yield* Fiber.join(starting);
            yield* Deferred.succeed(sendInvalidFrame, undefined);
            yield* Deferred.await(shutdownObserved);
          }),
        (log) =>
          Effect.sync(() => {
            console.log = log;
          }),
      );

      expect(observations).toHaveLength(2);
      expect(observations[0]?.[0]).toBe("Subagent link failed");
      expect(Schema.is(LinkProtocolError)(observations[0]?.[1])).toBe(true);
      expect(observations[0]?.[2]).toEqual({ subagentId });
      expect(observations[1]).toEqual(["shutdown"]);

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);
      yield* Effect.promise(() => shutdown());
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
      Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())),
    );
  });

  it.effect("logs a startup connection failure once before requesting shutdown once", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_extension-connect-failure");

    return Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_extension-connect-failure");
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );
      const loaded = yield* Effect.fromNullishOr(result.extensions[0]);
      const start = yield* Effect.fromNullishOr(loaded.handlers.get("session_start")?.[0]);
      const shutdownObserved = yield* Deferred.make<void>();
      const observations: Array<ReadonlyArray<unknown>> = [];
      let startupError: unknown;

      yield* Effect.acquireUseRelease(
        Effect.sync(() => {
          const log = console.log;
          console.log = (...args: Array<unknown>) => {
            observations.push(args.slice(1));
          };
          return log;
        }),
        () =>
          Effect.gen(function* () {
            startupError = yield* Effect.promise(() =>
              start(
                { type: "session_start" },
                {
                  shutdown: () => {
                    observations.push(["shutdown"]);
                    queueMicrotask(() => {
                      Deferred.doneUnsafe(shutdownObserved, Effect.void);
                    });
                  },
                },
              ).then(
                () => undefined,
                (error: unknown) => error,
              ),
            );

            yield* Deferred.await(shutdownObserved);
          }),
        (log) =>
          Effect.sync(() => {
            console.log = log;
          }),
      );

      expect(startupError).toBeInstanceOf(Error);
      expect(observations).toHaveLength(2);
      expect(observations[0]?.[0]).toBe("Subagent link failed");
      expect(Schema.is(LinkDisconnectedError)(observations[0]?.[1])).toBe(true);
      expect(observations[0]?.[2]).toEqual({ subagentId });
      expect(observations[1]).toEqual(["shutdown"]);

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);
      yield* Effect.promise(() => shutdown());
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
      Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())),
    );
  });

  it.effect("requests shutdown once without logging when the root closes", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_extension-close");

    return Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_extension-close");
      const listenerScope = yield* Scope.make();
      const listener = yield* Protocol.listen(subagentId).pipe(Scope.provide(listenerScope));
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );
      const loaded = yield* Effect.fromNullishOr(result.extensions[0]);
      const start = yield* Effect.fromNullishOr(loaded.handlers.get("session_start")?.[0]);
      const shutdownObserved = yield* Deferred.make<void>();
      const observations: Array<ReadonlyArray<unknown>> = [];

      yield* Effect.acquireUseRelease(
        Effect.sync(() => {
          const log = console.log;
          console.log = (...args: Array<unknown>) => {
            observations.push(args.slice(1));
          };
          return log;
        }),
        () =>
          Effect.gen(function* () {
            const starting = yield* Effect.promise(() =>
              start(
                { type: "session_start" },
                {
                  shutdown: () => {
                    observations.push(["shutdown"]);
                    queueMicrotask(() => {
                      Deferred.doneUnsafe(shutdownObserved, Effect.void);
                    });
                  },
                },
              ),
            ).pipe(Effect.forkChild({ startImmediately: true }));

            yield* listener.accept;
            yield* Fiber.join(starting);
            yield* Scope.close(listenerScope, Exit.void);
            yield* Deferred.await(shutdownObserved);
          }),
        (log) =>
          Effect.sync(() => {
            console.log = log;
          }),
      );

      expect(observations).toEqual([["shutdown"]]);

      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);
      yield* Effect.promise(() => shutdown());
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
      Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())),
    );
  });

  it.effect("logs a Pi delivery defect once before requesting shutdown once", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_extension-delivery");

    return Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_extension-delivery");
      const listener = yield* Protocol.listen(subagentId);
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );
      const loaded = yield* Effect.fromNullishOr(result.extensions[0]);
      const deliveryError = new Error("Pi rejected the root message");
      result.runtime.sendMessage = () => {
        throw deliveryError;
      };
      const start = yield* Effect.fromNullishOr(loaded.handlers.get("session_start")?.[0]);
      const shutdownObserved = yield* Deferred.make<void>();
      const observations: Array<ReadonlyArray<unknown>> = [];

      yield* Effect.acquireUseRelease(
        Effect.sync(() => {
          const log = console.log;
          console.log = (...args: Array<unknown>) => {
            observations.push(args.slice(1));
          };
          return log;
        }),
        () =>
          Effect.gen(function* () {
            const starting = yield* Effect.promise(() =>
              start(
                { type: "session_start" },
                {
                  shutdown: () => {
                    observations.push(["shutdown"]);
                    queueMicrotask(() => {
                      Deferred.doneUnsafe(shutdownObserved, Effect.void);
                    });
                  },
                },
              ),
            ).pipe(Effect.forkChild({ startImmediately: true }));
            const session = yield* listener.accept;

            yield* Fiber.join(starting);
            yield* session.send("Review the diff.");

            const sessionManager = SessionManager.inMemory("/tmp/smith-extension-test");
            const settle = yield* Effect.fromNullishOr(loaded.handlers.get("agent_settled")?.[0]);
            const settling = yield* Effect.promise(() =>
              settle({ type: "agent_settled" }, { sessionManager }),
            ).pipe(Effect.forkChild({ startImmediately: true }));

            expect(yield* session.take).toEqual({
              kind: "failure",
              reason: "Pi settled without an assistant response",
            });
            yield* Fiber.join(settling);
            yield* Deferred.await(shutdownObserved);
          }),
        (log) =>
          Effect.sync(() => {
            console.log = log;
          }),
      );

      expect(observations).toEqual([
        ["Failed to deliver root message to Pi", deliveryError, { subagentId }],
        ["shutdown"],
      ]);

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
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
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
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
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
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );

      expect(result.extensions).toEqual([]);
      expect(result.errors).toHaveLength(1);
    }).pipe(Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())));
  });
});

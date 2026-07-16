import { fileURLToPath } from "node:url";

import { NodeFileSystem } from "@effect/platform-node";
import { discoverAndLoadExtensions } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "@effect/vitest";
import { Effect, Fiber, Layer, Schema, Stream } from "effect";

import { SubagentBridge, SubagentBridgeDisconnectedError } from "../subagent/SubagentBridge.ts";
import { decodeSubagentId } from "../subagent/SubagentId.ts";
import { layer as unixSocketSubagentBridgeTransportLayer } from "../subagent/UnixSocketSubagentBridgeTransport.ts";

it.describe("Pi subagent extension", () => {
  it.effect("connects and closes a subagent bridge session", () => {
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

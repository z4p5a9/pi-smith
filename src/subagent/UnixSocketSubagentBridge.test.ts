import { NodeFileSystem, NodeSocket } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Layer, Schema, Scope } from "effect";

import {
  SubagentBridge,
  SubagentBridgeConnectError,
  SubagentBridgeDisconnectedError,
} from "./SubagentBridge.ts";
import { maxSubagentBridgeHandshakeBytes } from "./SubagentBridgeProtocol.ts";
import { decodeSubagentId } from "./SubagentId.ts";
import { layer as unixSocketSubagentBridgeLayer } from "./UnixSocketSubagentBridge.ts";

const encodeJson = Schema.encodeEffect(Schema.UnknownFromJsonString);

it.describe("UnixSocketSubagentBridge", () => {
  it.effect("connects a subagent bridge session", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-connect");
      const listener = yield* bridge.listen(subagentId);
      const child = yield* bridge.connect(subagentId);
      const root = yield* listener.accept;

      expect(child).toHaveProperty("await");
      expect(root).toHaveProperty("await");
    }).pipe(
      Effect.scoped,
      Effect.provide(unixSocketSubagentBridgeLayer.pipe(Layer.provideMerge(NodeFileSystem.layer))),
    ),
  );

  it.effect("accepts a valid connection after rejecting a malformed handshake", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-handshake");
      const listener = yield* bridge.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const handshake = yield* encodeJson({ version: 2, subagentId }).pipe(Effect.orDie);

      const invalidConnection = yield* socket
        .run(() => undefined, {
          onOpen: write(`${handshake}\n`).pipe(Effect.orDie),
        })
        .pipe(Effect.forkScoped);
      yield* Fiber.await(invalidConnection);

      const child = yield* bridge.connect(subagentId);
      const root = yield* listener.accept;

      expect(child).toHaveProperty("await");
      expect(root).toHaveProperty("await");
    }).pipe(
      Effect.scoped,
      Effect.provide(unixSocketSubagentBridgeLayer.pipe(Layer.provideMerge(NodeFileSystem.layer))),
    ),
  );

  it.effect("accepts a valid connection after rejecting another subagent", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-handshake");
      const otherSubagentId = yield* decodeSubagentId("sa_87654321_other-subagent");
      const listener = yield* bridge.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const handshake = yield* encodeJson({ version: 1, subagentId: otherSubagentId }).pipe(
        Effect.orDie,
      );

      const invalidConnection = yield* socket
        .run(() => undefined, {
          onOpen: write(`${handshake}\n`).pipe(Effect.orDie),
        })
        .pipe(Effect.forkScoped);
      yield* Fiber.await(invalidConnection);

      const child = yield* bridge.connect(subagentId);
      const root = yield* listener.accept;

      expect(child).toHaveProperty("await");
      expect(root).toHaveProperty("await");
    }).pipe(
      Effect.scoped,
      Effect.provide(unixSocketSubagentBridgeLayer.pipe(Layer.provideMerge(NodeFileSystem.layer))),
    ),
  );

  it.effect("accepts a valid connection after rejecting an oversized handshake", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-oversized");
      const listener = yield* bridge.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;

      const invalidConnection = yield* socket
        .run(() => undefined, {
          onOpen: write("x".repeat(maxSubagentBridgeHandshakeBytes + 1)).pipe(Effect.orDie),
        })
        .pipe(Effect.forkScoped);
      yield* Fiber.await(invalidConnection);

      const child = yield* bridge.connect(subagentId);
      const root = yield* listener.accept;

      expect(child).toHaveProperty("await");
      expect(root).toHaveProperty("await");
    }).pipe(
      Effect.scoped,
      Effect.provide(unixSocketSubagentBridgeLayer.pipe(Layer.provideMerge(NodeFileSystem.layer))),
    ),
  );

  it.effect("observes a disconnected subagent", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-disconnect");
      const listener = yield* bridge.listen(subagentId);
      const childScope = yield* Scope.make();

      yield* bridge.connect(subagentId).pipe(Scope.provide(childScope));
      const root = yield* listener.accept;
      yield* Scope.close(childScope, Exit.void);
      const error = yield* root.await.pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeDisconnectedError)(error)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(unixSocketSubagentBridgeLayer.pipe(Layer.provideMerge(NodeFileSystem.layer))),
    ),
  );

  it.effect("removes the socket when its scope closes", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-cleanup");

      yield* bridge.listen(subagentId).pipe(Effect.scoped);
      yield* bridge.listen(subagentId).pipe(Effect.scoped);
    }).pipe(
      Effect.provide(unixSocketSubagentBridgeLayer.pipe(Layer.provideMerge(NodeFileSystem.layer))),
    ),
  );

  it.effect("reports a missing listener", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-missing");
      const error = yield* bridge.connect(subagentId).pipe(Effect.scoped, Effect.flip);

      expect(Schema.is(SubagentBridgeConnectError)(error)).toBe(true);
    }).pipe(
      Effect.provide(unixSocketSubagentBridgeLayer.pipe(Layer.provideMerge(NodeFileSystem.layer))),
    ),
  );
});

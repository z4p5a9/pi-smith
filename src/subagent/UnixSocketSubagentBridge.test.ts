import { NodeFileSystem, NodeSocket } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Exit, Layer, Schema, Scope } from "effect";

import {
  SubagentBridge,
  SubagentBridgeConnectError,
  SubagentBridgeDisconnectedError,
  SubagentBridgeHandshakeError,
} from "./SubagentBridge.ts";
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

  it.effect("rejects an invalid handshake", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-handshake");
      const listener = yield* bridge.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const handshake = yield* encodeJson({ version: 2, subagentId }).pipe(Effect.orDie);

      yield* socket
        .run(() => undefined, {
          onOpen: write(`${handshake}\n`).pipe(Effect.orDie),
        })
        .pipe(Effect.forkScoped);

      const error = yield* listener.accept.pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeHandshakeError)(error)).toBe(true);
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

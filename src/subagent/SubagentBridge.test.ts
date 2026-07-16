import { NodeFileSystem, NodeSocket, NodeSocketServer } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Option, Schema, Scope, Stream } from "effect";
import * as Socket from "effect/unstable/socket/Socket";

import {
  SubagentBridge,
  SubagentBridgeDisconnectedError,
  SubagentBridgeProtocolError,
  SubagentBridgeSendEventError,
} from "./SubagentBridge.ts";
import { maxSubagentBridgeEventBytes } from "./SubagentBridgeProtocol.ts";
import { decodeSubagentId } from "./SubagentId.ts";
import { layer as unixSocketSubagentBridgeTransportLayer } from "./UnixSocketSubagentBridgeTransport.ts";

const encodeJson = Schema.encodeEffect(Schema.UnknownFromJsonString);

it.describe("SubagentBridge", () => {
  it.effect("establishes a session with the first event", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-connect");
      const listener = yield* bridge.listen(subagentId);
      const child = yield* bridge.connect(subagentId);
      const delivery = yield* child
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const root = yield* listener.accept;

      expect(yield* root.events.pipe(Stream.runHead)).toEqual(Option.some({ kind: "ready" }));
      yield* Fiber.join(delivery);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("delivers every event through the same acknowledged path", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-events");
      const listener = yield* bridge.listen(subagentId);
      const child = yield* bridge.connect(subagentId);
      const ready = yield* child
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const root = yield* listener.accept;
      const received = yield* root.events.pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Fiber.join(ready);
      yield* child.sendEvent({ kind: "message", content: "Task complete." });
      yield* child.sendEvent({ kind: "failure", reason: "Task failed." });

      expect(Array.from(yield* Fiber.join(received))).toEqual([
        { kind: "ready" },
        { kind: "message", content: "Task complete." },
        { kind: "failure", reason: "Task failed." },
      ]);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("accepts messages and duplicate ready events in any order", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-order");
      const listener = yield* bridge.listen(subagentId);
      const child = yield* bridge.connect(subagentId);
      const message = yield* child
        .sendEvent({ kind: "message", content: "Already working." })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const root = yield* listener.accept;
      const received = yield* root.events.pipe(
        Stream.take(3),
        Stream.runCollect,
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Fiber.join(message);
      yield* child.sendEvent({ kind: "ready" });
      yield* child.sendEvent({ kind: "ready" });

      expect(Array.from(yield* Fiber.join(received))).toEqual([
        { kind: "message", content: "Already working." },
        { kind: "ready" },
        { kind: "ready" },
      ]);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("accepts a valid connection after rejecting a malformed event", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-invalid");
      const listener = yield* bridge.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const event = yield* encodeJson({ version: 2, subagentId, event: { kind: "ready" } }).pipe(
        Effect.orDie,
      );

      const invalidConnection = yield* socket
        .run(() => undefined, {
          onOpen: write(`${event}\n`).pipe(Effect.orDie),
        })
        .pipe(Effect.forkScoped);
      yield* Fiber.await(invalidConnection);

      const child = yield* bridge.connect(subagentId);
      const delivery = yield* child
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* listener.accept;
      yield* Fiber.join(delivery);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("accepts a valid connection after rejecting another subagent", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-identity");
      const otherSubagentId = yield* decodeSubagentId("sa_87654321_other-subagent");
      const listener = yield* bridge.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const event = yield* encodeJson({
        version: 1,
        subagentId: otherSubagentId,
        event: { kind: "ready" },
      }).pipe(Effect.orDie);

      const invalidConnection = yield* socket
        .run(() => undefined, {
          onOpen: write(`${event}\n`).pipe(Effect.orDie),
        })
        .pipe(Effect.forkScoped);
      yield* Fiber.await(invalidConnection);

      const child = yield* bridge.connect(subagentId);
      const delivery = yield* child
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* listener.accept;
      yield* Fiber.join(delivery);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("accepts a valid connection after rejecting an oversized event", () =>
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
          onOpen: write("x".repeat(maxSubagentBridgeEventBytes + 1)).pipe(Effect.orDie),
        })
        .pipe(Effect.forkScoped);
      yield* Fiber.await(invalidConnection);

      const child = yield* bridge.connect(subagentId);
      const delivery = yield* child
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* listener.accept;
      yield* Fiber.join(delivery);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("rejects malformed wire data after accepting a connection", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-malformed");
      const listener = yield* bridge.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const first = yield* encodeJson({
        version: 1,
        subagentId,
        event: { kind: "ready" },
      }).pipe(Effect.orDie);
      const malformed = yield* encodeJson({
        version: 1,
        subagentId,
        event: { kind: "message" },
      }).pipe(Effect.orDie);

      yield* socket
        .run(() => undefined, {
          onOpen: write(`${first}\n`).pipe(Effect.orDie),
        })
        .pipe(Effect.forkScoped);

      const root = yield* listener.accept;
      yield* write(`${malformed}\n`);
      const error = yield* root.await.pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeProtocolError)(error)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("rejects an oversized event before sending it", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-large-send");
      const listener = yield* bridge.listen(subagentId);
      const child = yield* bridge.connect(subagentId);
      const ready = yield* child
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* listener.accept;
      yield* Fiber.join(ready);

      const error = yield* child
        .sendEvent({ kind: "message", content: "x".repeat(maxSubagentBridgeEventBytes + 1) })
        .pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeSendEventError)(error)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("fails event delivery when disconnected before acknowledgement", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-no-ack");
      const serverScope = yield* Scope.make();
      const received = yield* Deferred.make<void>();
      const server = yield* NodeSocketServer.make({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      }).pipe(Scope.provide(serverScope));

      yield* server
        .run((socket) => {
          let frameReceived = false;

          return Stream.pipeThroughChannel(Stream.never, Socket.toChannel(socket)).pipe(
            Stream.runForEach((chunk) => {
              for (const byte of chunk) {
                if (byte === 0x0a) {
                  frameReceived = true;
                }
              }

              return frameReceived ? Deferred.succeed(received, undefined) : Effect.void;
            }),
          );
        })
        .pipe(Effect.forkScoped, Scope.provide(serverScope));

      const child = yield* bridge.connect(subagentId);
      const delivery = yield* child
        .sendEvent({ kind: "message", content: "Task complete." })
        .pipe(Effect.forkScoped({ startImmediately: true }));

      yield* Deferred.await(received);
      yield* Scope.close(serverScope, Exit.void);

      const error = yield* Fiber.join(delivery).pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeSendEventError)(error)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("observes a disconnected subagent", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-disconnect");
      const listener = yield* bridge.listen(subagentId);
      const childScope = yield* Scope.make();
      const child = yield* bridge.connect(subagentId).pipe(Scope.provide(childScope));
      const ready = yield* child
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const root = yield* listener.accept;

      yield* Fiber.join(ready);
      yield* Scope.close(childScope, Exit.void);

      const error = yield* root.await.pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeDisconnectedError)(error)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("removes the socket when its scope closes", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-cleanup");

      yield* bridge.listen(subagentId).pipe(Effect.scoped);
      yield* bridge.listen(subagentId).pipe(Effect.scoped);
    }).pipe(
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("reports a missing listener when sending the first event", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-missing");
      const child = yield* bridge.connect(subagentId);
      const error = yield* child.sendEvent({ kind: "ready" }).pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeSendEventError)(error)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );
});

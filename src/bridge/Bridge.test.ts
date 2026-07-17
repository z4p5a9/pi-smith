import { NodeFileSystem, NodeSocket } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Schema, Scope, Stream } from "effect";

import {
  SubagentBridge,
  SubagentBridgeDisconnectedError,
  SubagentBridgeProtocolError,
  SubagentBridgeSendEventError,
} from "./Bridge.ts";
import { maxSubagentBridgeChildFrameBytes } from "./BridgeProtocol.ts";
import { decodeSubagentId } from "../subagent/SubagentId.ts";
import * as UnixSocketBridgeTransport from "./unix/UnixSocketBridgeTransport.ts";

const encodeJson = Schema.encodeEffect(Schema.UnknownFromJsonString);

it.describe("SubagentBridge", () => {
  it.effect("accepts hello before delivering one acknowledged event and closing", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-connect");
      const listener = yield* bridge.listen(subagentId);
      const connecting = yield* bridge
        .connect(subagentId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      const root = yield* listener.accept;
      const child = yield* Fiber.join(connecting);
      const sent = yield* Deferred.make<void>();
      const sending = yield* child
        .sendEvent({ kind: "completed", report: "Task complete." })
        .pipe(
          Effect.andThen(Deferred.succeed(sent, undefined)),
          Effect.forkChild({ startImmediately: true }),
        );
      const delivery = yield* root.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      expect(delivery.event).toEqual({ kind: "completed", report: "Task complete." });
      expect(yield* Deferred.isDone(sent)).toBe(false);

      yield* delivery.acknowledge;
      yield* Fiber.join(sending);
      yield* child.close;
      yield* Effect.all([root.await, child.await]);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("rejects an event before hello without poisoning the listener", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-event-first");
      const listener = yield* bridge.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const event = yield* encodeJson({
        kind: "event",
        version: 1,
        subagentId,
        event: { kind: "completed", report: "Task complete." },
      }).pipe(Effect.orDie);
      const invalidConnection = yield* socket
        .run(() => undefined, { onOpen: write(`${event}\n`).pipe(Effect.orDie) })
        .pipe(Effect.forkScoped);

      yield* Fiber.await(invalidConnection);

      const connecting = yield* bridge
        .connect(subagentId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* listener.accept;
      yield* Fiber.join(connecting);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("rejects a wrong protocol version without poisoning the listener", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-version");
      const listener = yield* bridge.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const hello = yield* encodeJson({ kind: "hello", version: 2, subagentId }).pipe(Effect.orDie);
      const invalidConnection = yield* socket
        .run(() => undefined, { onOpen: write(`${hello}\n`).pipe(Effect.orDie) })
        .pipe(Effect.forkScoped);

      yield* Fiber.await(invalidConnection);

      const connecting = yield* bridge
        .connect(subagentId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* listener.accept;
      yield* Fiber.join(connecting);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("rejects a wrong subagent ID without poisoning the listener", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-identity");
      const otherSubagentId = yield* decodeSubagentId("sa_87654321_other-subagent");
      const listener = yield* bridge.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const hello = yield* encodeJson({
        kind: "hello",
        version: 1,
        subagentId: otherSubagentId,
      }).pipe(Effect.orDie);
      const invalidConnection = yield* socket
        .run(() => undefined, { onOpen: write(`${hello}\n`).pipe(Effect.orDie) })
        .pipe(Effect.forkScoped);

      yield* Fiber.await(invalidConnection);

      const connecting = yield* bridge
        .connect(subagentId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* listener.accept;
      yield* Fiber.join(connecting);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("rejects malformed data without poisoning the listener", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-malformed");
      const listener = yield* bridge.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const invalidConnection = yield* socket
        .run(() => undefined, { onOpen: write('{"kind":"event"}\n').pipe(Effect.orDie) })
        .pipe(Effect.forkScoped);

      yield* Fiber.await(invalidConnection);

      const connecting = yield* bridge
        .connect(subagentId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* listener.accept;
      yield* Fiber.join(connecting);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("rejects an oversized frame without poisoning the listener", () =>
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
          onOpen: write("x".repeat(maxSubagentBridgeChildFrameBytes + 1)).pipe(Effect.orDie),
        })
        .pipe(Effect.forkScoped);

      yield* Fiber.await(invalidConnection);

      const connecting = yield* bridge
        .connect(subagentId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      yield* listener.accept;
      yield* Fiber.join(connecting);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("fails the session on a second event", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-second-event");
      const listener = yield* bridge.listen(subagentId);
      const connecting = yield* bridge
        .connect(subagentId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      const root = yield* listener.accept;
      const child = yield* Fiber.join(connecting);
      const first = yield* child
        .sendEvent({ kind: "completed", report: "Task complete." })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const delivery = yield* root.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption));

      yield* delivery.acknowledge;
      yield* Fiber.join(first);

      const sendError = yield* child
        .sendEvent({ kind: "failed", reason: "Duplicate." })
        .pipe(Effect.flip);
      const rootError = yield* root.await.pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeSendEventError)(sendError)).toBe(true);
      expect(Schema.is(SubagentBridgeProtocolError)(rootError)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(UnixSocketBridgeTransport.layer),
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
      const connecting = yield* bridge
        .connect(subagentId)
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* listener.accept;

      const child = yield* Fiber.join(connecting);
      const error = yield* child
        .sendEvent({ kind: "completed", report: "x".repeat(maxSubagentBridgeChildFrameBytes + 1) })
        .pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeSendEventError)(error)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("distinguishes unexpected EOF from graceful close", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const disconnectedId = yield* decodeSubagentId("sa_12345678_bridge-disconnect");
      const disconnectedListener = yield* bridge.listen(disconnectedId);
      const childScope = yield* Scope.make();
      const connecting = yield* bridge
        .connect(disconnectedId)
        .pipe(Scope.provide(childScope), Effect.forkChild({ startImmediately: true }));
      const disconnectedRoot = yield* disconnectedListener.accept;

      yield* Fiber.join(connecting);
      yield* Scope.close(childScope, Exit.void);

      expect(
        Schema.is(SubagentBridgeDisconnectedError)(yield* disconnectedRoot.await.pipe(Effect.flip)),
      ).toBe(true);

      const closedId = yield* decodeSubagentId("sa_87654321_bridge-close");
      const closedListener = yield* bridge.listen(closedId);
      const closedConnecting = yield* bridge
        .connect(closedId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      const closedRoot = yield* closedListener.accept;
      const closedChild = yield* Fiber.join(closedConnecting);

      yield* closedChild.close;
      yield* Effect.all([closedRoot.await, closedChild.await]);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(UnixSocketBridgeTransport.layer),
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
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("reports a missing listener while connecting", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-missing");
      const error = yield* bridge.connect(subagentId).pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeDisconnectedError)(error)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentBridge.layer.pipe(
          Layer.provide(UnixSocketBridgeTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );
});

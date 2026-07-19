import { NodeFileSystem, NodeSocket } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Layer, Schema, Scope, Stream } from "effect";

import {
  SubagentBridge,
  SubagentBridgeDisconnectedError,
  SubagentBridgeSendEventError,
} from "./Bridge.ts";
import { maxSubagentBridgeFrameBytes } from "./BridgeProtocol.ts";
import { decodeSubagentId } from "../../subagent/SubagentId.ts";
import * as UnixSocketBridgeTransport from "./unix/UnixSocketBridgeTransport.ts";

const encodeJson = Schema.encodeEffect(Schema.UnknownFromJsonString);

it.describe("SubagentBridge", () => {
  it.effect("acknowledges an event on receipt and delivers it", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-connect");
      const listener = yield* bridge.listen(subagentId);
      const connecting = yield* bridge
        .connect(subagentId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      const root = yield* listener.accept;
      const child = yield* Fiber.join(connecting);

      yield* child.sendEvent({ kind: "message", content: "Task complete." });

      const event = yield* root.take;

      expect(event).toEqual({ kind: "message", content: "Task complete." });
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

  it.effect("delivers multiple events in order", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-multiple");
      const listener = yield* bridge.listen(subagentId);
      const connecting = yield* bridge
        .connect(subagentId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      const root = yield* listener.accept;
      const child = yield* Fiber.join(connecting);

      yield* child.sendEvent({ kind: "message", content: "First." });
      yield* child.sendEvent({ kind: "failure", reason: "Second." });

      expect(yield* root.take).toEqual({ kind: "message", content: "First." });
      expect(yield* root.take).toEqual({ kind: "failure", reason: "Second." });
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

  it.effect("delivers root messages to the child and acknowledges them", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-send");
      const listener = yield* bridge.listen(subagentId);
      const connecting = yield* bridge
        .connect(subagentId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      const root = yield* listener.accept;
      const child = yield* Fiber.join(connecting);

      yield* root.send("Review the diff.");
      yield* root.send("Then report back.");

      const messages = yield* child.messages.pipe(Stream.take(2), Stream.runCollect);

      expect(messages).toEqual(["Review the diff.", "Then report back."]);

      yield* child.sendEvent({ kind: "message", content: "Done." });

      expect(yield* root.take).toEqual({ kind: "message", content: "Done." });
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

  it.effect("surfaces the session on an event-first connection", () =>
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
        event: { kind: "message", content: "Task complete." },
      }).pipe(Effect.orDie);

      yield* socket
        .run(() => undefined, { onOpen: write(`${event}\n`).pipe(Effect.orDie) })
        .pipe(Effect.forkScoped);

      const root = yield* listener.accept;
      const delivered = yield* root.take;

      expect(delivered).toEqual({ kind: "message", content: "Task complete." });
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

  it.effect("fails a second concurrent connection without the established session", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-second-conn");
      const listener = yield* bridge.listen(subagentId);
      const connecting = yield* bridge
        .connect(subagentId)
        .pipe(Effect.forkChild({ startImmediately: true }));
      const root = yield* listener.accept;
      const child = yield* Fiber.join(connecting);

      const secondError = yield* bridge.connect(subagentId).pipe(Effect.flip, Effect.scoped);

      expect(Schema.is(SubagentBridgeDisconnectedError)(secondError)).toBe(true);

      yield* child.sendEvent({ kind: "message", content: "Still alive." });

      const event = yield* root.take;

      expect(event).toEqual({ kind: "message", content: "Still alive." });
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
          onOpen: write("x".repeat(maxSubagentBridgeFrameBytes + 1)).pipe(Effect.orDie),
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
        .sendEvent({
          kind: "message",
          content: "x".repeat(maxSubagentBridgeFrameBytes + 1),
        })
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

  it.effect("settles both sessions when the child connection closes", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const subagentId = yield* decodeSubagentId("sa_12345678_bridge-disconnect");
      const listener = yield* bridge.listen(subagentId);
      const childScope = yield* Scope.make();
      const connecting = yield* bridge
        .connect(subagentId)
        .pipe(Scope.provide(childScope), Effect.forkChild({ startImmediately: true }));
      const root = yield* listener.accept;
      const child = yield* Fiber.join(connecting);

      yield* Scope.close(childScope, Exit.void);
      yield* root.await.pipe(Effect.exit);
      yield* child.await.pipe(Effect.exit);

      const sendError = yield* child
        .sendEvent({ kind: "message", content: "Too late." })
        .pipe(Effect.flip);

      expect(Schema.is(SubagentBridgeSendEventError)(sendError)).toBe(true);
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

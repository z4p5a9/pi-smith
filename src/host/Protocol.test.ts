import { NodeFileSystem, NodeSocket } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Schema, Scope, Stream } from "effect";

import { LinkDisconnectedError, LinkProtocolError, maxLinkFrameBytes } from "./link/Link.ts";
import * as UnixSocketTransport from "./link/unix/UnixSocketTransport.ts";
import * as Protocol from "./Protocol.ts";
import { decodeSubagentId } from "../subagent/SubagentId.ts";

const encodeJson = Schema.encodeEffect(Schema.UnknownFromJsonString);

it.describe("SubagentProtocol", () => {
  it.effect("establishes on the hello and delivers events to the root", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-connect");
      const listener = yield* Protocol.listen(subagentId);
      const connecting = yield* Protocol.connect(subagentId).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const root = yield* listener.accept;
      const child = yield* Fiber.join(connecting);

      const sending = yield* child
        .send({ kind: "message", content: "Task complete." })
        .pipe(Effect.forkChild({ startImmediately: true }));

      const event = yield* root.take;

      expect(event).toEqual({ kind: "message", content: "Task complete." });
      yield* Fiber.join(sending);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );

  it.effect("delivers multiple events in order", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-multiple");
      const listener = yield* Protocol.listen(subagentId);
      const connecting = yield* Protocol.connect(subagentId).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const root = yield* listener.accept;
      const child = yield* Fiber.join(connecting);

      const sendingFirst = yield* child
        .send({ kind: "message", content: "First." })
        .pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* root.take).toEqual({ kind: "message", content: "First." });
      yield* Fiber.join(sendingFirst);

      const sendingSecond = yield* child
        .send({ kind: "failure", reason: "Second." })
        .pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* root.take).toEqual({ kind: "failure", reason: "Second." });
      yield* Fiber.join(sendingSecond);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );

  it.effect("resolves a child event only when the root takes it", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-take-ack");
      const listener = yield* Protocol.listen(subagentId);
      const connecting = yield* Protocol.connect(subagentId).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const root = yield* listener.accept;
      const child = yield* Fiber.join(connecting);

      const sending = yield* child
        .send({ kind: "message", content: "Waiting on the root." })
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      expect(sending.pollUnsafe()).toBe(undefined);

      expect(yield* root.take).toEqual({ kind: "message", content: "Waiting on the root." });
      yield* Fiber.join(sending);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );

  it.effect("delivers root messages to the child as they are consumed", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-send");
      const listener = yield* Protocol.listen(subagentId);
      const connecting = yield* Protocol.connect(subagentId).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const root = yield* listener.accept;
      const child = yield* Fiber.join(connecting);

      const sending = yield* root
        .send("Review the diff.")
        .pipe(
          Effect.andThen(root.send("Then report back.")),
          Effect.forkChild({ startImmediately: true }),
        );

      const inbox = yield* child.inbox.pipe(Stream.take(2), Stream.runCollect);

      expect(inbox).toEqual([
        { kind: "message", content: "Review the diff." },
        { kind: "message", content: "Then report back." },
      ]);

      yield* Fiber.join(sending);
      yield* child
        .send({ kind: "message", content: "Done." })
        .pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* root.take).toEqual({ kind: "message", content: "Done." });
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );

  it.effect("requires hello before surfacing a session", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-event-first");
      const listener = yield* Protocol.listen(subagentId);
      const accepting = yield* listener.accept.pipe(Effect.forkChild({ startImmediately: true }));

      expect(accepting.pollUnsafe()).toBeUndefined();

      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const eventWritten = yield* Deferred.make<void>();
      const event = yield* encodeJson({
        v: 1,
        subagentId,
        seq: 0,
        data: { kind: "message", content: "Task complete." },
      }).pipe(Effect.orDie);

      const invalidConnection = yield* socket
        .run(() => undefined, {
          onOpen: write(`${event}\n`).pipe(
            Effect.andThen(Deferred.succeed(eventWritten, undefined)),
            Effect.orDie,
          ),
        })
        .pipe(Effect.forkScoped);

      yield* Deferred.await(eventWritten);
      yield* Fiber.await(invalidConnection);

      expect(accepting.pollUnsafe()).toBeUndefined();

      const connecting = yield* Protocol.connect(subagentId).pipe(
        Effect.forkChild({ startImmediately: true }),
      );

      yield* Fiber.join(accepting);
      yield* Fiber.join(connecting);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );

  it.effect("drops a duplicate hello and keeps taking events", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-dup-hello");
      const listener = yield* Protocol.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const firstAcknowledged = yield* Deferred.make<void>();
      const secondAcknowledged = yield* Deferred.make<void>();
      let output = "";
      const frames = yield* Effect.forEach(
        [
          { v: 1, subagentId, seq: 0, data: { kind: "hello" } },
          { v: 1, subagentId, seq: 1, data: { kind: "hello" } },
          { v: 1, subagentId, seq: 2, data: { kind: "message", content: "After the hellos." } },
        ],
        (frame) => encodeJson(frame).pipe(Effect.orDie),
      );

      yield* socket
        .runString(
          (chunk) => {
            output += chunk;

            return Effect.gen(function* () {
              if (output.includes('"ack":0')) {
                yield* Deferred.succeed(firstAcknowledged, undefined);
              }

              if (output.includes('"ack":1')) {
                yield* Deferred.succeed(secondAcknowledged, undefined);
              }
            });
          },
          {
            onOpen: write(`${frames[0]}\n`).pipe(Effect.orDie),
          },
        )
        .pipe(Effect.forkScoped);

      const root = yield* listener.accept;
      const taking = yield* root.take.pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(firstAcknowledged);
      yield* write(`${frames[1]}\n`).pipe(Effect.orDie);
      yield* Deferred.await(secondAcknowledged);
      yield* write(`${frames[2]}\n`).pipe(Effect.orDie);

      expect(yield* Fiber.join(taking)).toEqual({
        kind: "message",
        content: "After the hellos.",
      });
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );

  it.effect("fails a second concurrent connection without the established session", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-second-conn");
      const listener = yield* Protocol.listen(subagentId);
      const connecting = yield* Protocol.connect(subagentId).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      const root = yield* listener.accept;
      const child = yield* Fiber.join(connecting);

      const secondError = yield* Protocol.connect(subagentId).pipe(Effect.flip, Effect.scoped);

      expect(Schema.is(LinkDisconnectedError)(secondError)).toBe(true);

      yield* child
        .send({ kind: "message", content: "Still alive." })
        .pipe(Effect.forkChild({ startImmediately: true }));

      const event = yield* root.take;

      expect(event).toEqual({ kind: "message", content: "Still alive." });
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );

  it.effect("rejects a wrong envelope version without poisoning the listener", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-version");
      const listener = yield* Protocol.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const hello = yield* encodeJson({
        v: 2,
        subagentId,
        seq: 0,
        data: { kind: "hello" },
      }).pipe(Effect.orDie);
      const invalidConnection = yield* socket
        .run(() => undefined, { onOpen: write(`${hello}\n`).pipe(Effect.orDie) })
        .pipe(Effect.forkScoped);

      yield* Fiber.await(invalidConnection);

      const connecting = yield* Protocol.connect(subagentId).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* listener.accept;
      yield* Fiber.join(connecting);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );

  it.effect("rejects a wrong subagent ID without poisoning the listener", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-identity");
      const otherSubagentId = yield* decodeSubagentId("sa_87654321_other-subagent");
      const listener = yield* Protocol.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const hello = yield* encodeJson({
        v: 1,
        subagentId: otherSubagentId,
        seq: 0,
        data: { kind: "hello" },
      }).pipe(Effect.orDie);
      const invalidConnection = yield* socket
        .run(() => undefined, { onOpen: write(`${hello}\n`).pipe(Effect.orDie) })
        .pipe(Effect.forkScoped);

      yield* Fiber.await(invalidConnection);

      const connecting = yield* Protocol.connect(subagentId).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* listener.accept;
      yield* Fiber.join(connecting);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );

  it.effect("rejects a wrong subagent ID after establishing the session", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-established-identity");
      const otherSubagentId = yield* decodeSubagentId("sa_87654321_other-established");
      const listener = yield* Protocol.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const helloAcknowledged = yield* Deferred.make<void>();
      let output = "";
      const hello = yield* encodeJson({
        v: 1,
        subagentId,
        seq: 0,
        data: { kind: "hello" },
      }).pipe(Effect.orDie);
      const event = yield* encodeJson({
        v: 1,
        subagentId: otherSubagentId,
        seq: 1,
        data: { kind: "message", content: "Wrong identity." },
      }).pipe(Effect.orDie);

      const connection = yield* socket
        .runString(
          (chunk) => {
            output += chunk;

            return output.includes('"ack":0')
              ? Deferred.succeed(helloAcknowledged, undefined)
              : Effect.void;
          },
          { onOpen: write(`${hello}\n`).pipe(Effect.orDie) },
        )
        .pipe(Effect.forkScoped);

      const root = yield* listener.accept;
      yield* Deferred.await(helloAcknowledged);
      yield* write(`${event}\n`).pipe(Effect.orDie);
      yield* Fiber.await(connection);

      const error = yield* root.await.pipe(Effect.flip);

      expect(Schema.is(LinkProtocolError)(error)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );

  it.effect("rejects malformed data without poisoning the listener", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-malformed");
      const listener = yield* Protocol.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const invalidConnection = yield* socket
        .run(() => undefined, { onOpen: write('{"kind":"event"}\n').pipe(Effect.orDie) })
        .pipe(Effect.forkScoped);

      yield* Fiber.await(invalidConnection);

      const connecting = yield* Protocol.connect(subagentId).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* listener.accept;
      yield* Fiber.join(connecting);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );

  it.effect("rejects an oversized frame without poisoning the listener", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-oversized");
      const listener = yield* Protocol.listen(subagentId);
      const socket = yield* NodeSocket.makeNet({
        path: `/tmp/smith-${process.getuid?.() ?? 0}/${subagentId}.sock`,
      });
      const write = yield* socket.writer;
      const invalidConnection = yield* socket
        .run(() => undefined, {
          onOpen: write("x".repeat(maxLinkFrameBytes + 1)).pipe(Effect.orDie),
        })
        .pipe(Effect.forkScoped);

      yield* Fiber.await(invalidConnection);

      const connecting = yield* Protocol.connect(subagentId).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      yield* listener.accept;
      yield* Fiber.join(connecting);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );

  it.effect("settles both sessions when the child connection closes", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-disconnect");
      const listener = yield* Protocol.listen(subagentId);
      const childScope = yield* Scope.make();
      const connecting = yield* Protocol.connect(subagentId).pipe(
        Scope.provide(childScope),
        Effect.forkChild({ startImmediately: true }),
      );
      const root = yield* listener.accept;
      const child = yield* Fiber.join(connecting);

      yield* Scope.close(childScope, Exit.void);
      yield* root.await.pipe(Effect.exit);
      yield* child.await.pipe(Effect.exit);

      const sendError = yield* child
        .send({ kind: "message", content: "Too late." })
        .pipe(Effect.flip);

      expect(Schema.is(LinkDisconnectedError)(sendError)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );

  it.effect("removes the socket when its scope closes", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-cleanup");

      yield* Protocol.listen(subagentId).pipe(Effect.scoped);
      yield* Protocol.listen(subagentId).pipe(Effect.scoped);
    }).pipe(Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer)))),
  );

  it.effect("reports a missing listener while connecting", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_protocol-missing");
      const error = yield* Protocol.connect(subagentId).pipe(Effect.flip);

      expect(Schema.is(LinkDisconnectedError)(error)).toBe(true);
    }).pipe(
      Effect.scoped,
      Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
    ),
  );
});

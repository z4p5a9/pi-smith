import { NodeSocket, NodeSocketServer } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Schema, Scope } from "effect";
import type * as Socket from "effect/unstable/socket/Socket";

import { decodeSubagentId, type SubagentId } from "../../subagent/SubagentId.ts";
import * as Link from "./Link.ts";

const socketPath = (name: string) => `/tmp/smith-link-test-${process.pid}-${name}.sock`;
const encodeJson = Schema.encodeEffect(Schema.UnknownFromJsonString);

const listen = Effect.fn("listen")(function* (name: string, subagentId: SubagentId) {
  const server = yield* NodeSocketServer.make({ path: socketPath(name) });
  const accepted = yield* Deferred.make<Link.Link>();

  yield* server
    .run((socket: Socket.Socket) =>
      Link.make(socket, subagentId).pipe(
        Effect.flatMap((link) =>
          Deferred.succeed(accepted, link).pipe(Effect.andThen(link.closed)),
        ),
        Effect.exit,
        Effect.scoped,
      ),
    )
    .pipe(Effect.forkScoped);

  return { accept: Deferred.await(accepted) };
});

const connect = Effect.fn("connect")(function* (name: string, subagentId: SubagentId) {
  const socket = yield* NodeSocket.makeNet({ path: socketPath(name) });

  return yield* Link.make(socket, subagentId);
});

it.describe("Link", () => {
  it.effect("delivers one datagram in both directions simultaneously", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-duplex");
      const listener = yield* listen("duplex", subagentId);
      const client = yield* connect("duplex", subagentId);
      const server = yield* listener.accept;

      const clientSending = yield* client
        .send({ kind: "message", content: "from client" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const serverSending = yield* server
        .send({ kind: "failure", reason: "from server" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      const inbound = yield* server.recv;
      const outbound = yield* client.recv;

      expect(inbound.data).toEqual({ kind: "message", content: "from client" });
      expect(outbound.data).toEqual({ kind: "failure", reason: "from server" });
      yield* inbound.ack;
      yield* outbound.ack;
      yield* Fiber.join(clientSending);
      yield* Fiber.join(serverSending);
    }).pipe(Effect.scoped),
  );

  it.effect("resolves a send only when the consumer fulfills the acknowledgement", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-ack-gated");
      const listener = yield* listen("ack-gated", subagentId);
      const client = yield* connect("ack-gated", subagentId);

      const sending = yield* client
        .send({ kind: "message", content: "needs ack" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const server = yield* listener.accept;
      const inbound = yield* server.recv;

      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      expect(sending.pollUnsafe()).toBe(undefined);

      yield* inbound.ack;
      yield* Fiber.join(sending);
    }).pipe(Effect.scoped),
  );

  it.effect("serializes concurrent sends until the active frame is acknowledged", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-concurrent");
      const listener = yield* listen("concurrent", subagentId);
      const client = yield* connect("concurrent", subagentId);

      const first = yield* client
        .send({ kind: "message", content: "first" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const server = yield* listener.accept;
      const one = yield* server.recv;

      const second = yield* client
        .send({ kind: "message", content: "second" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const receivingSecond = yield* server.recv.pipe(Effect.forkChild({ startImmediately: true }));

      yield* Effect.yieldNow;
      yield* Effect.yieldNow;

      expect(one.data).toEqual({ kind: "message", content: "first" });
      expect(receivingSecond.pollUnsafe()).toBe(undefined);
      expect(second.pollUnsafe()).toBe(undefined);

      yield* one.ack;
      yield* Fiber.join(first);

      const two = yield* Fiber.join(receivingSecond);

      expect(two.data).toEqual({ kind: "message", content: "second" });
      yield* two.ack;
      yield* Fiber.join(second);
    }).pipe(Effect.scoped),
  );

  it.effect("fails an active and waiting send when the peer goes away", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-peer-loss");
      const listener = yield* listen("peer-loss", subagentId);
      const clientScope = yield* Scope.make();
      const client = yield* connect("peer-loss", subagentId).pipe(Scope.provide(clientScope));
      const server = yield* listener.accept;

      const sending = yield* server
        .send({ kind: "message", content: "never acked" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      const inbound = yield* client.recv;

      expect(inbound.data).toEqual({ kind: "message", content: "never acked" });

      const waiting = yield* server
        .send({ kind: "message", content: "waiting for the window" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Scope.close(clientScope, Exit.void);

      const inFlight = yield* Fiber.join(sending).pipe(Effect.flip);
      const queued = yield* Fiber.join(waiting).pipe(Effect.flip);

      expect(Schema.is(Link.LinkDisconnectedError)(inFlight)).toBe(true);
      expect(Schema.is(Link.LinkDisconnectedError)(queued)).toBe(true);

      yield* server.closed.pipe(Effect.exit);

      const late = yield* server.send({ kind: "message", content: "too late" }).pipe(Effect.flip);

      expect(Schema.is(Link.LinkDisconnectedError)(late)).toBe(true);

      const drained = yield* server.recv.pipe(Effect.flip);

      expect(Schema.is(Link.LinkDisconnectedError)(drained)).toBe(true);
    }).pipe(Effect.scoped),
  );

  it.effect("ignores interruption of a waiting send", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-send-interrupt");
      const listener = yield* listen("send-interrupt", subagentId);
      const client = yield* connect("send-interrupt", subagentId);
      const server = yield* listener.accept;

      const first = yield* client
        .send({ kind: "message", content: "first" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const one = yield* server.recv;
      const interrupted = yield* client
        .send({ kind: "message", content: "interrupted" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Fiber.interrupt(interrupted);
      yield* one.ack;
      yield* Fiber.join(first);

      const third = yield* client
        .send({ kind: "message", content: "third" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const received = yield* server.recv;

      expect(received.data).toEqual({ kind: "message", content: "third" });
      yield* received.ack;
      yield* Fiber.join(third);
    }).pipe(Effect.scoped),
  );

  it.effect("terminates the link when the active send is interrupted", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-active-interrupt");
      const listener = yield* listen("active-interrupt", subagentId);
      const socket = yield* NodeSocket.makeNet({ path: socketPath("active-interrupt") });
      const opened = yield* Deferred.make<void>();
      const firstObserved = yield* Deferred.make<void>();
      let output = "";
      let dataFrames = 0;

      yield* socket
        .runString(
          (chunk) => {
            output += chunk;
            const lines = output.split("\n");
            output = lines.pop() ?? "";

            return Effect.gen(function* () {
              for (const line of lines) {
                if (line.includes('"data":')) {
                  dataFrames += 1;

                  if (dataFrames === 1) {
                    yield* Deferred.succeed(firstObserved, undefined);
                  }
                }
              }
            });
          },
          { onOpen: Deferred.succeed(opened, undefined).pipe(Effect.asVoid) },
        )
        .pipe(Effect.forkScoped);

      yield* Deferred.await(opened);

      const server = yield* listener.accept;
      const first = yield* server
        .send({ kind: "message", content: "first" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(firstObserved);
      const waiting = yield* server
        .send({ kind: "message", content: "second" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Fiber.interrupt(first);

      const active = yield* Fiber.await(first);
      const closed = yield* server.closed.pipe(Effect.flip);
      const queued = yield* Fiber.join(waiting).pipe(Effect.flip);
      const late = yield* server.send({ kind: "message", content: "third" }).pipe(Effect.flip);

      expect(Exit.isFailure(active) && Cause.hasInterruptsOnly(active.cause)).toBe(true);
      expect(Schema.is(Link.LinkDisconnectedError)(closed)).toBe(true);
      expect(Schema.is(Link.LinkDisconnectedError)(queued)).toBe(true);
      expect(Schema.is(Link.LinkDisconnectedError)(late)).toBe(true);
      expect(dataFrames).toBe(1);
    }).pipe(Effect.scoped),
  );

  it.effect("fails when a second inbound frame arrives before the first is received", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-window-queued");
      const listener = yield* listen("window-queued", subagentId);
      const socket = yield* NodeSocket.makeNet({ path: socketPath("window-queued") });
      const write = yield* socket.writer;
      const frames = yield* Effect.forEach(
        [
          {
            v: 1,
            subagentId,
            seq: 0,
            data: { kind: "message", content: "first" },
          },
          {
            v: 1,
            subagentId,
            seq: 1,
            data: { kind: "message", content: "second" },
          },
        ],
        (frame) => encodeJson(frame).pipe(Effect.orDie),
      );

      yield* socket
        .run(() => undefined, {
          onOpen: write(`${frames[0]}\n${frames[1]}\n`).pipe(Effect.orDie),
        })
        .pipe(Effect.forkScoped);

      const server = yield* listener.accept;
      const error = yield* server.closed.pipe(Effect.flip, Effect.timeout("1 second"));

      expect(Schema.is(Link.LinkProtocolError)(error)).toBe(true);
    }).pipe(Effect.scoped),
  );

  it.effect("fails when a second inbound frame arrives after recv but before ack", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-window-held");
      const listener = yield* listen("window-held", subagentId);
      const socket = yield* NodeSocket.makeNet({ path: socketPath("window-held") });
      const write = yield* socket.writer;
      const opened = yield* Deferred.make<void>();
      const firstFrame = yield* encodeJson({
        v: 1,
        subagentId,
        seq: 0,
        data: { kind: "message", content: "first" },
      }).pipe(Effect.orDie);
      const secondFrame = yield* encodeJson({
        v: 1,
        subagentId,
        seq: 1,
        data: { kind: "message", content: "second" },
      }).pipe(Effect.orDie);

      yield* socket
        .run(() => undefined, {
          onOpen: write(`${firstFrame}\n`).pipe(
            Effect.orDie,
            Effect.andThen(Deferred.succeed(opened, undefined)),
            Effect.asVoid,
          ),
        })
        .pipe(Effect.forkScoped);

      yield* Deferred.await(opened);

      const server = yield* listener.accept;
      const first = yield* server.recv;

      expect(first.data).toEqual({ kind: "message", content: "first" });

      yield* write(`${secondFrame}\n`).pipe(Effect.orDie);

      const error = yield* server.closed.pipe(Effect.flip, Effect.timeout("1 second"));

      expect(Schema.is(Link.LinkProtocolError)(error)).toBe(true);
    }).pipe(Effect.scoped),
  );

  it.effect("treats duplicate acknowledgement actions as harmless", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-duplicate-ack");
      const listener = yield* listen("duplicate-ack", subagentId);
      const client = yield* connect("duplicate-ack", subagentId);
      const server = yield* listener.accept;

      const first = yield* client
        .send({ kind: "message", content: "first" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const one = yield* server.recv;

      yield* one.ack;
      yield* one.ack;
      yield* Fiber.join(first);

      const second = yield* client
        .send({ kind: "message", content: "second" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const two = yield* server.recv;

      expect(two.data).toEqual({ kind: "message", content: "second" });
      yield* two.ack;
      yield* Fiber.join(second);
    }).pipe(Effect.scoped),
  );

  it.effect("does not let an old acknowledgement claim a reused sequence number", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-reused-sequence");
      const listener = yield* listen("reused-sequence", subagentId);
      const socket = yield* NodeSocket.makeNet({ path: socketPath("reused-sequence") });
      const write = yield* socket.writer;
      const opened = yield* Deferred.make<void>();
      const firstAcknowledged = yield* Deferred.make<void>();
      const secondAcknowledged = yield* Deferred.make<void>();
      const acknowledgementsAtBarrier = yield* Deferred.make<number>();
      const firstFrame = yield* encodeJson({
        v: 1,
        subagentId,
        seq: 7,
        data: { kind: "message", content: "first" },
      }).pipe(Effect.orDie);
      const secondFrame = yield* encodeJson({
        v: 1,
        subagentId,
        seq: 7,
        data: { kind: "message", content: "second" },
      }).pipe(Effect.orDie);
      const acknowledgeBarrier = yield* encodeJson({ v: 1, subagentId, ack: 0 }).pipe(Effect.orDie);
      let output = "";
      let acknowledgements = 0;

      yield* socket
        .runString(
          (chunk) => {
            output += chunk;
            const lines = output.split("\n");
            output = lines.pop() ?? "";

            return Effect.gen(function* () {
              for (const line of lines) {
                if (line.includes('"ack":7')) {
                  acknowledgements += 1;

                  if (acknowledgements === 1) {
                    yield* Deferred.succeed(firstAcknowledged, undefined);
                  }

                  if (acknowledgements === 2) {
                    yield* Deferred.succeed(secondAcknowledged, undefined);
                  }
                }

                if (line.includes('"content":"barrier"')) {
                  yield* Deferred.succeed(acknowledgementsAtBarrier, acknowledgements);
                  yield* write(`${acknowledgeBarrier}\n`).pipe(Effect.orDie);
                }
              }
            });
          },
          {
            onOpen: write(`${firstFrame}\n`).pipe(
              Effect.orDie,
              Effect.andThen(Deferred.succeed(opened, undefined)),
              Effect.asVoid,
            ),
          },
        )
        .pipe(Effect.forkScoped);

      yield* Deferred.await(opened);

      const server = yield* listener.accept;
      const first = yield* server.recv;

      expect(first.data).toEqual({ kind: "message", content: "first" });
      yield* first.ack;
      yield* Deferred.await(firstAcknowledged);

      yield* write(`${secondFrame}\n`).pipe(Effect.orDie);

      const second = yield* server.recv;

      expect(second.data).toEqual({ kind: "message", content: "second" });
      yield* first.ack;

      const barrier = yield* server
        .send({ kind: "message", content: "barrier" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* Deferred.await(acknowledgementsAtBarrier)).toBe(1);
      yield* Fiber.join(barrier);

      yield* second.ack;
      yield* Deferred.await(secondAcknowledged);
    }).pipe(Effect.scoped),
  );

  it.effect("fails the link on a wrong subagent identity", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-identity");
      const otherSubagentId = yield* decodeSubagentId("sa_87654321_other-subagent");
      const listener = yield* listen("identity", subagentId);
      const client = yield* connect("identity", otherSubagentId);

      yield* client
        .send({ kind: "hello" })
        .pipe(Effect.exit, Effect.forkChild({ startImmediately: true }));

      const server = yield* listener.accept;
      const error = yield* server.closed.pipe(Effect.flip);

      expect(Schema.is(Link.LinkProtocolError)(error)).toBe(true);
    }).pipe(Effect.scoped),
  );

  it.effect("fails the link on a malformed frame", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-malformed");
      const listener = yield* listen("malformed", subagentId);
      const socket = yield* NodeSocket.makeNet({ path: socketPath("malformed") });
      const write = yield* socket.writer;

      yield* socket
        .run(() => undefined, { onOpen: write('{"v":1}\n').pipe(Effect.orDie) })
        .pipe(Effect.forkScoped);

      const server = yield* listener.accept;
      const error = yield* server.closed.pipe(Effect.flip);

      expect(Schema.is(Link.LinkProtocolError)(error)).toBe(true);
    }).pipe(Effect.scoped),
  );

  it.effect("fails the link on an oversized frame", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-oversized");
      const listener = yield* listen("oversized", subagentId);
      const socket = yield* NodeSocket.makeNet({ path: socketPath("oversized") });
      const write = yield* socket.writer;

      yield* socket
        .run(() => undefined, {
          onOpen: write("x".repeat(Link.maxLinkFrameBytes + 1)).pipe(Effect.orDie),
        })
        .pipe(Effect.forkScoped);

      const server = yield* listener.accept;
      const error = yield* server.closed.pipe(Effect.flip);

      expect(Schema.is(Link.LinkProtocolError)(error)).toBe(true);
    }).pipe(Effect.scoped),
  );
});

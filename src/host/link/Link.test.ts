import { NodeSocket, NodeSocketServer } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Schema, Scope } from "effect";
import type * as Socket from "effect/unstable/socket/Socket";

import { decodeSubagentId, type SubagentId } from "../../subagent/SubagentId.ts";
import * as Link from "./Link.ts";

const socketPath = (name: string) => `/tmp/smith-link-test-${process.pid}-${name}.sock`;

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
  it.effect("delivers datagrams both directions through one frame shape", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-duplex");
      const listener = yield* listen("duplex", subagentId);
      const client = yield* connect("duplex", subagentId);

      const clientSending = yield* client
        .send({ kind: "message", content: "from client" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const server = yield* listener.accept;

      const inbound = yield* server.recv;

      expect(inbound.data).toEqual({ kind: "message", content: "from client" });
      yield* inbound.ack;
      yield* Fiber.join(clientSending);

      const serverSending = yield* server
        .send({ kind: "failure", reason: "from server" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const outbound = yield* client.recv;

      expect(outbound.data).toEqual({ kind: "failure", reason: "from server" });
      yield* outbound.ack;
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

  it.effect("correlates concurrent sends through their sequence numbers", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_link-concurrent");
      const listener = yield* listen("concurrent", subagentId);
      const client = yield* connect("concurrent", subagentId);

      const first = yield* client
        .send({ kind: "message", content: "first" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const second = yield* client
        .send({ kind: "message", content: "second" })
        .pipe(Effect.forkChild({ startImmediately: true }));
      const server = yield* listener.accept;

      const one = yield* server.recv;
      const two = yield* server.recv;

      expect([one.data, two.data]).toEqual([
        { kind: "message", content: "first" },
        { kind: "message", content: "second" },
      ]);

      // Acknowledge out of order: each send resolves on its own ack.
      yield* two.ack;
      yield* Fiber.join(second);

      expect(first.pollUnsafe()).toBe(undefined);

      yield* one.ack;
      yield* Fiber.join(first);
    }).pipe(Effect.scoped),
  );

  it.effect("fails in-flight and later sends when the peer goes away", () =>
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

      yield* Scope.close(clientScope, Exit.void);

      const inFlight = yield* Fiber.join(sending).pipe(Effect.flip);

      expect(Schema.is(Link.LinkDisconnectedError)(inFlight)).toBe(true);

      yield* server.closed.pipe(Effect.exit);

      const late = yield* server.send({ kind: "message", content: "too late" }).pipe(Effect.flip);

      expect(Schema.is(Link.LinkDisconnectedError)(late)).toBe(true);

      const drained = yield* server.recv.pipe(Effect.flip);

      expect(Schema.is(Link.LinkDisconnectedError)(drained)).toBe(true);
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

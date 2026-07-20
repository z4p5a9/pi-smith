import {
  Cause,
  Deferred,
  Effect,
  Exit,
  HashMap,
  Option,
  Queue,
  Ref,
  Schema,
  type Scope,
  Stream,
} from "effect";
import * as Ndjson from "effect/unstable/encoding/Ndjson";
import * as Socket from "effect/unstable/socket/Socket";

import { SubagentEvent } from "../../subagent/SubagentEvent.ts";
import { SubagentId } from "../../subagent/SubagentId.ts";

export const maxLinkFrameBytes = 1024 * 1024;

export class LinkDisconnectedError extends Schema.TaggedErrorClass<LinkDisconnectedError>()(
  "LinkDisconnectedError",
  {
    reason: Schema.String,
  },
) {}

export class LinkProtocolError extends Schema.TaggedErrorClass<LinkProtocolError>()(
  "LinkProtocolError",
  {
    reason: Schema.String,
  },
) {}

export const Datagram = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("hello") }),
  ...SubagentEvent.members,
]);

export type Datagram = typeof Datagram.Type;

const Frame = Schema.Union([
  Schema.Struct({
    v: Schema.Literal(1),
    subagentId: SubagentId,
    seq: Schema.Finite,
    data: Datagram,
  }),
  Schema.Struct({
    v: Schema.Literal(1),
    subagentId: SubagentId,
    ack: Schema.Finite,
  }),
]);

export interface Link {
  readonly send: (data: Datagram) => Effect.Effect<void, LinkDisconnectedError>;
  readonly recv: Effect.Effect<
    { readonly data: Datagram; readonly ack: Effect.Effect<void> },
    LinkDisconnectedError | LinkProtocolError
  >;
  readonly closed: Effect.Effect<void, LinkDisconnectedError | LinkProtocolError>;
}

const encodeFrame = Schema.encodeEffect(Schema.fromJsonString(Frame));

export const make = Effect.fn("Link.make")(function* (
  socket: Socket.Socket,
  subagentId: SubagentId,
): Effect.fn.Return<Link, never, Scope.Scope> {
  const encoder = new TextEncoder();
  const write = yield* socket.writer;
  const inbound = yield* Queue.unbounded<
    { readonly data: Datagram; readonly ack: Effect.Effect<void> },
    Cause.Done
  >();
  const pending = yield* Ref.make(
    HashMap.empty<number, Deferred.Deferred<void, LinkDisconnectedError>>(),
  );
  const lifetime = yield* Deferred.make<void, LinkDisconnectedError | LinkProtocolError>();
  const sequence = yield* Ref.make(0);
  const inboundByteCount = yield* Ref.make(0);

  const writeFrame = Effect.fn("Link.writeFrame")(function* (frame: typeof Frame.Type) {
    const json = yield* encodeFrame(frame).pipe(Effect.orDie);

    return yield* write(encoder.encode(`${json}\n`)).pipe(
      Effect.mapError((error) => LinkDisconnectedError.make({ reason: error.message })),
    );
  });

  const handleFrame = Effect.fn("Link.handleFrame")(function* (frame: typeof Frame.Type) {
    if (frame.subagentId !== subagentId) {
      return yield* LinkProtocolError.make({
        reason: `Expected subagent ID ${subagentId}, received ${frame.subagentId}`,
      });
    }

    if ("ack" in frame) {
      const acked = yield* Ref.modify(
        pending,
        (entries) => [HashMap.get(entries, frame.ack), HashMap.remove(entries, frame.ack)] as const,
      );

      if (Option.isNone(acked)) {
        return yield* Effect.logDebug("Dropped unmatched link ack").pipe(
          Effect.annotateLogs({ ack: frame.ack }),
        );
      }

      return yield* Deferred.succeed(acked.value, undefined).pipe(Effect.asVoid);
    }

    return yield* Queue.offer(inbound, {
      data: frame.data,
      // The consumer fulfills the acknowledgement. Writes park in the socket
      // send queue forever once the connection is gone, so the ack races the
      // lifetime and degrades to a no-op on a disconnected link.
      ack: Effect.raceFirst(
        writeFrame({ v: 1, subagentId, ack: frame.seq }),
        Deferred.await(lifetime).pipe(Effect.exit, Effect.asVoid),
      ).pipe(Effect.ignore),
    }).pipe(Effect.asVoid);
  });

  // The write side goes through `write` directly; the channel is read-only and
  // `Stream.never` keeps its input open so the socket is never half-closed.
  yield* Stream.never.pipe(
    Stream.pipeThroughChannel(Socket.toChannel(socket)),
    Stream.mapEffect((chunk) =>
      Effect.gen(function* () {
        let count = yield* Ref.get(inboundByteCount);

        for (const byte of chunk) {
          if (byte === 0x0a) {
            count = 0;
            continue;
          }

          count += 1;

          if (count > maxLinkFrameBytes) {
            return yield* LinkProtocolError.make({
              reason: `Link frame exceeds ${maxLinkFrameBytes} bytes`,
            });
          }
        }

        yield* Ref.set(inboundByteCount, count);
        return chunk;
      }),
    ),
    Stream.pipeThroughChannel(Ndjson.decodeSchema(Frame)()),
    Stream.runForEach(handleFrame),
    Effect.catchReason("SocketError", "SocketCloseError", (reason, error) =>
      reason.code === 1000 ? Effect.void : Effect.fail(error),
    ),
    Effect.catchTag("SocketError", (error) =>
      LinkDisconnectedError.make({
        reason: error.message,
      }),
    ),
    Effect.catchTag(["NdjsonError", "SchemaError"], (error) =>
      LinkProtocolError.make({
        reason: String(error),
      }),
    ),
    Effect.onExit((exit) =>
      Effect.gen(function* () {
        yield* Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)
          ? Deferred.failCause(lifetime, exit.cause)
          : Deferred.succeed(lifetime, undefined);
        yield* Queue.end(inbound);

        const remaining = yield* Ref.getAndSet(pending, HashMap.empty());

        for (const acked of HashMap.values(remaining)) {
          yield* Deferred.fail(acked, LinkDisconnectedError.make({ reason: "Link disconnected" }));
        }
      }),
    ),
    Effect.ignore,
    Effect.forkScoped,
  );

  const disconnected = Deferred.await(lifetime).pipe(
    Effect.andThen(LinkDisconnectedError.make({ reason: "Link disconnected" })),
  );

  // For a sender, a link that died on a protocol error is simply disconnected.
  const sendDisconnected = disconnected.pipe(
    Effect.catchTag("LinkProtocolError", (error) =>
      LinkDisconnectedError.make({ reason: error.reason }),
    ),
  );

  const send = Effect.fn("Link.send")(function* (data: Datagram) {
    const seq = yield* Ref.getAndUpdate(sequence, (next) => next + 1);
    const acked = yield* Deferred.make<void, LinkDisconnectedError>();

    yield* Ref.update(pending, HashMap.set(seq, acked));

    return yield* Effect.raceFirst(
      writeFrame({ v: 1, subagentId, seq, data }),
      sendDisconnected,
    ).pipe(
      Effect.andThen(Deferred.await(acked)),
      Effect.onInterrupt(() => Ref.update(pending, HashMap.remove(seq))),
      Effect.onError(() => Ref.update(pending, HashMap.remove(seq))),
    );
  });

  return {
    send,
    recv: Queue.take(inbound).pipe(Effect.catch(() => disconnected)),
    closed: Deferred.await(lifetime),
  } satisfies Link;
});

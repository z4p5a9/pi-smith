import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Queue,
  Ref,
  Schema,
  Semaphore,
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
  const inbound = yield* Queue.dropping<
    { readonly data: Datagram; readonly ack: Effect.Effect<void> },
    Cause.Done
  >(1);
  const sendWindow = yield* Semaphore.make(1);
  const inboundDeliveryLease = yield* Ref.make<
    | { readonly state: "available" }
    | { readonly state: "held"; readonly seq: number; readonly identity: object }
    | { readonly state: "acknowledging"; readonly seq: number; readonly identity: object }
  >({ state: "available" });
  const outbound = yield* Ref.make<{
    readonly nextSequence: number;
    readonly active?: {
      readonly seq: number;
      readonly acked: Deferred.Deferred<void, LinkDisconnectedError>;
    };
  }>({ nextSequence: 0 });
  const lifetime = yield* Deferred.make<void, LinkDisconnectedError | LinkProtocolError>();
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
      const active = (yield* Ref.get(outbound)).active;

      if (active === undefined || active.seq !== frame.ack) {
        return yield* Effect.logDebug("Dropped unmatched link ack").pipe(
          Effect.annotateLogs({ ack: frame.ack }),
        );
      }

      return yield* Deferred.succeed(active.acked, undefined).pipe(Effect.asVoid);
    }

    const identity = {};
    const previous = yield* Ref.modify(
      inboundDeliveryLease,
      (current) =>
        [
          current,
          current.state === "available"
            ? { state: "held" as const, seq: frame.seq, identity }
            : current,
        ] as const,
    );

    if (previous.state !== "available") {
      return yield* LinkProtocolError.make({
        reason: `Received data frame ${frame.seq} while frame ${previous.seq} remains unacknowledged`,
      });
    }

    const ack = Effect.uninterruptible(
      Effect.gen(function* () {
        const claimed = yield* Ref.modify(inboundDeliveryLease, (current) =>
          current.state === "held" && current.identity === identity
            ? ([true, { state: "acknowledging" as const, seq: frame.seq, identity }] as const)
            : ([false, current] as const),
        );

        if (!claimed) {
          return;
        }

        const written = yield* Effect.raceFirst(
          writeFrame({ v: 1, subagentId, ack: frame.seq }).pipe(Effect.as(true as const)),
          Deferred.await(lifetime).pipe(Effect.exit, Effect.as(false as const)),
        ).pipe(
          Effect.catch((error) => Deferred.fail(lifetime, error).pipe(Effect.as(false as const))),
        );

        if (written) {
          yield* Ref.update(inboundDeliveryLease, (current) =>
            current.state === "acknowledging" && current.identity === identity
              ? { state: "available" as const }
              : current,
          );
        }
      }),
    );

    const offered = yield* Queue.offer(inbound, {
      data: frame.data,
      ack,
    });

    if (!offered) {
      return yield* LinkProtocolError.make({
        reason: `Inbound data window rejected frame ${frame.seq}`,
      });
    }

    return yield* Effect.void;
  });

  // The write side goes through `write` directly; the channel is read-only and
  // `Stream.never` keeps its input open so the socket is never half-closed.
  const reader = Stream.never.pipe(
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
  );

  yield* Effect.raceFirst(reader, Deferred.await(lifetime).pipe(Effect.exit, Effect.asVoid)).pipe(
    Effect.onExit((exit) =>
      Effect.gen(function* () {
        yield* Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)
          ? Deferred.failCause(lifetime, exit.cause)
          : Deferred.succeed(lifetime, undefined);
        yield* Queue.end(inbound);
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
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const permits = yield* restore(Effect.raceFirst(sendDisconnected, sendWindow.take(1)));
        const acked = yield* Deferred.make<void, LinkDisconnectedError>();
        const seq = yield* Ref.modify(outbound, (current) => [
          current.nextSequence,
          {
            nextSequence: current.nextSequence + 1,
            active: { seq: current.nextSequence, acked },
          },
        ]);

        return yield* restore(
          Effect.raceFirst(
            sendDisconnected,
            writeFrame({ v: 1, subagentId, seq, data }).pipe(
              Effect.catch((error) =>
                Deferred.fail(lifetime, error).pipe(Effect.andThen(Effect.fail(error))),
              ),
              Effect.andThen(Deferred.await(acked)),
            ),
          ),
        ).pipe(
          Effect.onInterrupt(() =>
            Deferred.fail(
              lifetime,
              LinkDisconnectedError.make({ reason: "Link send interrupted" }),
            ).pipe(Effect.asVoid),
          ),
          Effect.ensuring(
            Ref.update(outbound, (current) => ({
              nextSequence: current.nextSequence,
            })).pipe(Effect.andThen(sendWindow.release(permits))),
          ),
        );
      }),
    );
  });

  return {
    send,
    recv: Queue.take(inbound).pipe(Effect.catch(() => disconnected)),
    closed: Deferred.await(lifetime),
  } satisfies Link;
});

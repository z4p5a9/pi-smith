import { Context, Deferred, Effect, Exit, Layer, Queue, Schema, Semaphore, Stream } from "effect";
import * as Ndjson from "effect/unstable/encoding/Ndjson";
import * as Socket from "effect/unstable/socket/Socket";

import {
  encodeSubagentBridgeAcknowledgementFrame,
  encodeSubagentBridgeEventFrame,
  maxSubagentBridgeAcknowledgementBytes,
  maxSubagentBridgeEventBytes,
  SubagentBridgeAcknowledgementFrame,
  SubagentBridgeEventFrame,
} from "./SubagentBridgeProtocol.ts";
import { SubagentBridgeTransport } from "./SubagentBridgeTransport.ts";
import type { SubagentEvent } from "./SubagentEvent.ts";
import { SubagentId } from "./SubagentId.ts";

export interface SubagentBridgeRootSession {
  readonly events: Stream.Stream<SubagentEventDelivery>;
  readonly await: Effect.Effect<
    void,
    SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
  >;
}

export interface SubagentEventDelivery {
  readonly event: SubagentEvent;
  readonly acknowledge: Effect.Effect<void>;
}

export interface SubagentBridgeChildSession {
  readonly sendEvent: (event: SubagentEvent) => Effect.Effect<void, SubagentBridgeSendEventError>;
  readonly await: Effect.Effect<
    void,
    SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
  >;
}

export interface SubagentBridgeListener {
  readonly accept: Effect.Effect<SubagentBridgeRootSession>;
}

export class SubagentBridgeListenError extends Schema.TaggedErrorClass<SubagentBridgeListenError>()(
  "SubagentBridgeListenError",
  {
    subagentId: SubagentId,
    reason: Schema.String,
  },
) {}

export class SubagentBridgeConnectError extends Schema.TaggedErrorClass<SubagentBridgeConnectError>()(
  "SubagentBridgeConnectError",
  {
    subagentId: SubagentId,
    reason: Schema.String,
  },
) {}

export class SubagentBridgeProtocolError extends Schema.TaggedErrorClass<SubagentBridgeProtocolError>()(
  "SubagentBridgeProtocolError",
  {
    subagentId: SubagentId,
    reason: Schema.String,
  },
) {}

export class SubagentBridgeSendEventError extends Schema.TaggedErrorClass<SubagentBridgeSendEventError>()(
  "SubagentBridgeSendEventError",
  {
    subagentId: SubagentId,
    reason: Schema.String,
  },
) {}

export class SubagentBridgeDisconnectedError extends Schema.TaggedErrorClass<SubagentBridgeDisconnectedError>()(
  "SubagentBridgeDisconnectedError",
  {
    subagentId: SubagentId,
    reason: Schema.String,
  },
) {}

const make = Effect.gen(function* () {
  const transport = yield* SubagentBridgeTransport;

  const listen = Effect.fn("SubagentBridge.listen")(function* (subagentId: SubagentId) {
    yield* Effect.annotateCurrentSpan({ subagentId });

    const server = yield* transport.listen(subagentId);
    const accepted = yield* Deferred.make<SubagentBridgeRootSession>();

    const runConnection = Effect.fn("SubagentBridge.runConnection")(function* (
      socket: Socket.Socket,
    ) {
      const events = yield* Queue.bounded<SubagentEventDelivery>(1);
      const outgoingBytes = yield* Queue.bounded<Uint8Array>(0);
      const lifetime = yield* Deferred.make<
        void,
        SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
      >();
      const encoder = new TextEncoder();
      let frameByteCount = 0;
      const connection = { accepted: false };

      const session = {
        events: Stream.fromQueue(events),
        await: Deferred.await(lifetime),
      } satisfies SubagentBridgeRootSession;

      const frames = Stream.fromQueue(outgoingBytes).pipe(
        Stream.pipeThroughChannel(Socket.toChannel(socket)),
        Stream.mapEffect((chunk): Effect.Effect<Uint8Array, SubagentBridgeProtocolError> => {
          for (const byte of chunk) {
            if (byte === 0x0a) {
              frameByteCount = 0;
              continue;
            }

            frameByteCount += 1;

            if (frameByteCount > maxSubagentBridgeEventBytes) {
              return SubagentBridgeProtocolError.make({
                subagentId,
                reason: `Bridge event frame exceeds ${maxSubagentBridgeEventBytes} bytes`,
              });
            }
          }

          return Effect.succeed(chunk);
        }),
        Stream.pipeThroughChannel(Ndjson.decodeSchema(SubagentBridgeEventFrame)()),
      );

      const exit = yield* frames.pipe(
        Stream.runForEach((frame) =>
          Effect.gen(function* () {
            if (frame.subagentId !== subagentId) {
              return yield* SubagentBridgeProtocolError.make({
                subagentId,
                reason: `Expected subagent ID ${subagentId}, received ${frame.subagentId}`,
              });
            }

            if (!connection.accepted) {
              if (!(yield* Deferred.succeed(accepted, session))) {
                return yield* SubagentBridgeProtocolError.make({
                  subagentId,
                  reason: "Another bridge connection is already active",
                });
              }

              connection.accepted = true;
            }

            const acknowledged = yield* Deferred.make<void>();

            yield* Queue.offer(events, {
              event: frame.event,
              acknowledge: Deferred.succeed(acknowledged, undefined).pipe(Effect.asVoid),
            });
            yield* Deferred.await(acknowledged);

            const acknowledgement = yield* encodeSubagentBridgeAcknowledgementFrame({
              kind: "ack",
              version: 1,
              subagentId,
            }).pipe(Effect.orDie);

            yield* Queue.offer(outgoingBytes, encoder.encode(`${acknowledgement}\n`));
            return undefined;
          }),
        ),
        Effect.catchTag("SocketError", (error) =>
          SubagentBridgeDisconnectedError.make({
            subagentId,
            reason: error.message,
          }),
        ),
        Effect.catchTag(["NdjsonError", "SchemaError"], (error) =>
          SubagentBridgeProtocolError.make({
            subagentId,
            reason: String(error),
          }),
        ),
        Effect.andThen(
          SubagentBridgeDisconnectedError.make({
            subagentId,
            reason: "Bridge connection closed",
          }),
        ),
        Effect.onExit((connectionExit) =>
          Exit.isFailure(connectionExit)
            ? Deferred.failCause(lifetime, connectionExit.cause)
            : Effect.void,
        ),
        Effect.exit,
      );

      yield* Exit.match(exit, {
        onFailure: (cause) =>
          connection.accepted
            ? Effect.void
            : Effect.logWarning("Rejected subagent bridge connection", cause).pipe(
                Effect.annotateLogs({ subagentId }),
              ),
        onSuccess: () => Effect.void,
      });
    });

    yield* server.run(runConnection).pipe(Effect.forkScoped);

    return { accept: Deferred.await(accepted) } satisfies SubagentBridgeListener;
  });

  const connect = Effect.fn("SubagentBridge.connect")(function* (subagentId: SubagentId) {
    yield* Effect.annotateCurrentSpan({ subagentId });

    const socket = yield* transport.connect(subagentId);
    const acknowledgements = yield* Queue.bounded<void>(0);
    const outgoingBytes = yield* Queue.bounded<Uint8Array>(0);
    const lifetime = yield* Deferred.make<
      void,
      SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
    >();
    const sendLock = yield* Semaphore.make(1);
    const encoder = new TextEncoder();
    let frameByteCount = 0;

    const frames = Stream.fromQueue(outgoingBytes).pipe(
      Stream.pipeThroughChannel(Socket.toChannel(socket)),
      Stream.mapEffect((chunk): Effect.Effect<Uint8Array, SubagentBridgeProtocolError> => {
        for (const byte of chunk) {
          if (byte === 0x0a) {
            frameByteCount = 0;
            continue;
          }

          frameByteCount += 1;

          if (frameByteCount > maxSubagentBridgeAcknowledgementBytes) {
            return SubagentBridgeProtocolError.make({
              subagentId,
              reason: `Bridge acknowledgement exceeds ${maxSubagentBridgeAcknowledgementBytes} bytes`,
            });
          }
        }

        return Effect.succeed(chunk);
      }),
      Stream.pipeThroughChannel(Ndjson.decodeSchema(SubagentBridgeAcknowledgementFrame)()),
    );

    yield* frames.pipe(
      Stream.runForEach((frame) => {
        if (frame.subagentId !== subagentId) {
          return SubagentBridgeProtocolError.make({
            subagentId,
            reason: `Expected subagent ID ${subagentId}, received ${frame.subagentId}`,
          });
        }

        return Queue.offer(acknowledgements, undefined);
      }),
      Effect.catchTag("SocketError", (error) =>
        SubagentBridgeDisconnectedError.make({
          subagentId,
          reason: error.message,
        }),
      ),
      Effect.catchTag(["NdjsonError", "SchemaError"], (error) =>
        SubagentBridgeProtocolError.make({
          subagentId,
          reason: String(error),
        }),
      ),
      Effect.andThen(
        SubagentBridgeDisconnectedError.make({
          subagentId,
          reason: "Bridge connection closed",
        }),
      ),
      Effect.onExit((exit) =>
        Exit.isFailure(exit) ? Deferred.failCause(lifetime, exit.cause) : Effect.void,
      ),
      Effect.forkScoped,
    );

    const sendEvent = Effect.fn("SubagentBridge.sendEvent")(
      function* (event: SubagentEvent) {
        const frame = yield* encodeSubagentBridgeEventFrame({
          version: 1,
          subagentId,
          event,
        }).pipe(Effect.orDie);
        const bytes = encoder.encode(`${frame}\n`);

        if (bytes.byteLength - 1 > maxSubagentBridgeEventBytes) {
          return yield* SubagentBridgeSendEventError.make({
            subagentId,
            reason: `Bridge event frame exceeds ${maxSubagentBridgeEventBytes} bytes`,
          });
        }

        yield* Effect.raceFirst(Queue.offer(outgoingBytes, bytes), Deferred.await(lifetime));
        yield* Effect.raceFirst(Queue.take(acknowledgements), Deferred.await(lifetime));
        return undefined;
      },
      (effect) => sendLock.withPermit(effect),
      Effect.mapError((error) =>
        SubagentBridgeSendEventError.make({
          subagentId,
          reason: String(error),
        }),
      ),
    );

    return {
      sendEvent,
      await: Deferred.await(lifetime),
    } satisfies SubagentBridgeChildSession;
  });

  return { listen, connect };
});

export class SubagentBridge extends Context.Service<SubagentBridge>()(
  "@smith/subagent/SubagentBridge",
  { make },
) {
  static readonly layer = Layer.effect(SubagentBridge, SubagentBridge.make);
}

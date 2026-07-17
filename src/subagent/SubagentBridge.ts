import {
  type Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Layer,
  Queue,
  Schema,
  Semaphore,
  Stream,
} from "effect";
import * as Ndjson from "effect/unstable/encoding/Ndjson";
import * as Socket from "effect/unstable/socket/Socket";

import {
  encodeSubagentBridgeAcknowledgementFrame,
  encodeSubagentBridgeCloseFrame,
  encodeSubagentBridgeEventFrame,
  encodeSubagentBridgeHelloFrame,
  maxSubagentBridgeAcknowledgementBytes,
  maxSubagentBridgeChildFrameBytes,
  SubagentBridgeAcknowledgementFrame,
  SubagentBridgeChildFrame,
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
  readonly close: Effect.Effect<void, SubagentBridgeCloseError>;
  readonly await: Effect.Effect<
    void,
    SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
  >;
}

export interface SubagentBridgeListener {
  readonly accept: Effect.Effect<SubagentBridgeRootSession>;
}

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

export class SubagentBridgeCloseError extends Schema.TaggedErrorClass<SubagentBridgeCloseError>()(
  "SubagentBridgeCloseError",
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
      const events = yield* Queue.bounded<SubagentEventDelivery, Cause.Done>(1);
      const outgoingBytes = yield* Queue.bounded<Uint8Array>(0);
      const lifetime = yield* Deferred.make<
        void,
        SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
      >();
      const encoder = new TextEncoder();
      let frameByteCount = 0;
      const connection: { state: "pending" | "open" | "terminal" | "closing" } = {
        state: "pending",
      };

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

            if (frameByteCount > maxSubagentBridgeChildFrameBytes) {
              return SubagentBridgeProtocolError.make({
                subagentId,
                reason: `Bridge child frame exceeds ${maxSubagentBridgeChildFrameBytes} bytes`,
              });
            }
          }

          return Effect.succeed(chunk);
        }),
        Stream.pipeThroughChannel(Ndjson.decodeSchema(SubagentBridgeChildFrame)()),
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

            if (frame.kind === "hello") {
              if (connection.state !== "pending") {
                return yield* SubagentBridgeProtocolError.make({
                  subagentId,
                  reason: "Bridge hello has already been received",
                });
              }

              if (!(yield* Deferred.succeed(accepted, session))) {
                return yield* SubagentBridgeProtocolError.make({
                  subagentId,
                  reason: "Another bridge connection is already active",
                });
              }

              connection.state = "open";
            } else if (frame.kind === "event") {
              // oxlint-disable-next-line no-warning-comments -- tracks the planned protocol evolution
              // TODO: Permit multiple events when persistent subagents define their protocol semantics.
              if (connection.state === "terminal") {
                return yield* SubagentBridgeProtocolError.make({
                  subagentId,
                  reason: "Bridge protocol version 1 permits one event",
                });
              }

              if (connection.state !== "open") {
                return yield* SubagentBridgeProtocolError.make({
                  subagentId,
                  reason: "Bridge connection is not open",
                });
              }

              connection.state = "terminal";
              const acknowledged = yield* Deferred.make<void>();

              yield* Queue.offer(events, {
                event: frame.event,
                acknowledge: Deferred.succeed(acknowledged, undefined).pipe(Effect.asVoid),
              });
              yield* Deferred.await(acknowledged);
            } else {
              if (connection.state !== "open" && connection.state !== "terminal") {
                return yield* SubagentBridgeProtocolError.make({
                  subagentId,
                  reason: "Bridge connection is not open",
                });
              }

              connection.state = "closing";
            }

            const acknowledgement = yield* encodeSubagentBridgeAcknowledgementFrame({
              kind: "ack",
              version: 1,
              subagentId,
            }).pipe(Effect.orDie);

            yield* Queue.offer(outgoingBytes, encoder.encode(`${acknowledgement}\n`));
            return yield* Effect.void;
          }),
        ),
        Effect.catchReason("SocketError", "SocketCloseError", (reason, error) =>
          connection.state === "closing" && reason.code === 1000 ? Effect.void : Effect.fail(error),
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
          Effect.suspend(() =>
            connection.state === "closing"
              ? Effect.void
              : SubagentBridgeDisconnectedError.make({
                  subagentId,
                  reason: "Bridge connection closed",
                }),
          ),
        ),
        Effect.ensuring(Queue.end(events)),
        Effect.onExit((connectionExit) =>
          Exit.isFailure(connectionExit)
            ? Deferred.failCause(lifetime, connectionExit.cause)
            : Deferred.succeed(lifetime, undefined).pipe(Effect.asVoid),
        ),
        Effect.exit,
      );

      yield* Exit.match(exit, {
        onFailure: (cause) =>
          connection.state === "pending"
            ? Effect.logWarning("Rejected subagent bridge connection", cause).pipe(
                Effect.annotateLogs({ subagentId }),
              )
            : Effect.void,
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
    const outgoingBytes = yield* Queue.bounded<Uint8Array, Cause.Done>(0);
    const lifetime = yield* Deferred.make<
      void,
      SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
    >();
    const sendLock = yield* Semaphore.make(1);
    const encoder = new TextEncoder();
    let frameByteCount = 0;
    const connection: { state: "connecting" | "open" | "closing" | "closed" } = {
      state: "connecting",
    };

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
      Effect.catchReason("SocketError", "SocketCloseError", (reason, error) =>
        connection.state === "closing" && reason.code === 1000 ? Effect.void : Effect.fail(error),
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
        Effect.suspend(() =>
          connection.state === "closing"
            ? Effect.void
            : SubagentBridgeDisconnectedError.make({
                subagentId,
                reason: "Bridge connection closed",
              }),
        ),
      ),
      Effect.onExit((exit) => {
        if (Exit.isFailure(exit)) {
          return Deferred.failCause(lifetime, exit.cause);
        }

        connection.state = "closed";
        return Deferred.succeed(lifetime, undefined).pipe(Effect.asVoid);
      }),
      Effect.forkScoped,
    );

    const hello = yield* encodeSubagentBridgeHelloFrame({
      kind: "hello",
      version: 1,
      subagentId,
    }).pipe(Effect.orDie);

    yield* Effect.raceFirst(
      Queue.offer(outgoingBytes, encoder.encode(`${hello}\n`)),
      Deferred.await(lifetime),
    );
    yield* Effect.raceFirst(Queue.take(acknowledgements), Deferred.await(lifetime));
    connection.state = "open";

    const sendEvent = Effect.fn("SubagentBridge.sendEvent")(
      function* (event: SubagentEvent) {
        if (connection.state !== "open") {
          return yield* SubagentBridgeSendEventError.make({
            subagentId,
            reason: "Bridge connection is not open",
          });
        }

        const frame = yield* encodeSubagentBridgeEventFrame({
          kind: "event",
          version: 1,
          subagentId,
          event,
        }).pipe(Effect.orDie);
        const bytes = encoder.encode(`${frame}\n`);

        if (bytes.byteLength - 1 > maxSubagentBridgeChildFrameBytes) {
          return yield* SubagentBridgeSendEventError.make({
            subagentId,
            reason: `Bridge event frame exceeds ${maxSubagentBridgeChildFrameBytes} bytes`,
          });
        }

        yield* Effect.raceFirst(Queue.offer(outgoingBytes, bytes), Deferred.await(lifetime));
        yield* Effect.raceFirst(Queue.take(acknowledgements), Deferred.await(lifetime));
        return yield* Effect.void;
      },
      (effect) => sendLock.withPermit(effect),
      Effect.mapError((error) =>
        SubagentBridgeSendEventError.make({
          subagentId,
          reason: String(error),
        }),
      ),
    );

    const close = Effect.gen(function* () {
      if (connection.state === "closed") {
        return;
      }

      if (connection.state === "closing") {
        yield* Deferred.await(lifetime);
        return;
      }

      connection.state = "closing";
      const frame = yield* encodeSubagentBridgeCloseFrame({
        kind: "close",
        version: 1,
        subagentId,
      }).pipe(Effect.orDie);

      yield* Effect.raceFirst(
        Queue.offer(outgoingBytes, encoder.encode(`${frame}\n`)),
        Deferred.await(lifetime),
      );
      yield* Effect.raceFirst(Queue.take(acknowledgements), Deferred.await(lifetime));
      yield* Queue.end(outgoingBytes);
      yield* Deferred.await(lifetime);
      return;
    }).pipe(
      (effect) => sendLock.withPermit(effect),
      Effect.mapError((error) =>
        SubagentBridgeCloseError.make({
          subagentId,
          reason: String(error),
        }),
      ),
      Effect.withSpan("SubagentBridge.close"),
    );

    return {
      sendEvent,
      close,
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

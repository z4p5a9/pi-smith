import {
  Cause,
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
  encodeSubagentBridgeEventFrame,
  encodeSubagentBridgeHelloFrame,
  encodeSubagentBridgeMessageFrame,
  maxSubagentBridgeFrameBytes,
  SubagentBridgeChildFrame,
  SubagentBridgeRootFrame,
} from "./BridgeProtocol.ts";
import { SubagentBridgeTransport } from "./BridgeTransport.ts";
import type { SubagentEvent } from "../../subagent/SubagentEvent.ts";
import { SubagentId } from "../../subagent/SubagentId.ts";

export interface SubagentBridgeRootSession {
  readonly take: Effect.Effect<
    SubagentEvent,
    SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
  >;
  readonly send: (content: string) => Effect.Effect<void, SubagentBridgeSendMessageError>;
  readonly await: Effect.Effect<
    void,
    SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
  >;
}

export interface SubagentBridgeChildSession {
  readonly sendEvent: (event: SubagentEvent) => Effect.Effect<void, SubagentBridgeSendEventError>;
  readonly messages: Stream.Stream<string>;
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

export class SubagentBridgeSendMessageError extends Schema.TaggedErrorClass<SubagentBridgeSendMessageError>()(
  "SubagentBridgeSendMessageError",
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
  const encoder = new TextEncoder();

  const listen = Effect.fn("SubagentBridge.listen")(function* (subagentId: SubagentId) {
    yield* Effect.annotateCurrentSpan({ subagentId });

    const server = yield* transport.listen(subagentId);
    const accepted = yield* Deferred.make<SubagentBridgeRootSession>();

    const serveConnection = Effect.fn("SubagentBridge.serveConnection")(function* (
      socket: Socket.Socket,
    ) {
      const write = yield* socket.writer;
      const events = yield* Queue.unbounded<SubagentEvent, Cause.Done>();
      const acknowledgements = yield* Queue.unbounded<void, Cause.Done>();
      const lifetime = yield* Deferred.make<
        void,
        SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
      >();
      const sendLock = yield* Semaphore.make(1);
      const acknowledgement = yield* encodeSubagentBridgeAcknowledgementFrame({
        kind: "ack",
        version: 1,
        subagentId,
      }).pipe(Effect.orDie);
      let frameByteCount = 0;
      let established = false;

      const disconnected = Deferred.await(lifetime).pipe(
        Effect.andThen(
          SubagentBridgeDisconnectedError.make({
            subagentId,
            reason: "Bridge connection closed",
          }),
        ),
      );

      const send = Effect.fn("SubagentBridge.send")(
        function* (content: string) {
          const frame = yield* encodeSubagentBridgeMessageFrame({
            kind: "message",
            version: 1,
            subagentId,
            content,
          }).pipe(Effect.orDie);
          const bytes = encoder.encode(`${frame}\n`);

          if (bytes.byteLength - 1 > maxSubagentBridgeFrameBytes) {
            return yield* SubagentBridgeSendMessageError.make({
              subagentId,
              reason: `Bridge message frame exceeds ${maxSubagentBridgeFrameBytes} bytes`,
            });
          }

          yield* Effect.raceFirst(write(bytes), disconnected);
          yield* Queue.take(acknowledgements).pipe(Effect.catch(() => disconnected));
          return yield* Effect.void;
        },
        (effect) => sendLock.withPermit(effect),
        Effect.mapError((error) =>
          SubagentBridgeSendMessageError.make({
            subagentId,
            reason: String(error),
          }),
        ),
      );

      const session = {
        take: Queue.take(events).pipe(Effect.catch(() => disconnected)),
        send,
        await: Deferred.await(lifetime),
      } satisfies SubagentBridgeRootSession;

      const handleFrame = (frame: typeof SubagentBridgeChildFrame.Type) =>
        Effect.gen(function* () {
          if (frame.subagentId !== subagentId) {
            return yield* SubagentBridgeProtocolError.make({
              subagentId,
              reason: `Expected subagent ID ${subagentId}, received ${frame.subagentId}`,
            });
          }

          if (!established) {
            if (!(yield* Deferred.succeed(accepted, session))) {
              return yield* SubagentBridgeProtocolError.make({
                subagentId,
                reason: "Another bridge connection is already active",
              });
            }

            established = true;
          }

          if (frame.kind === "ack") {
            yield* Queue.offer(acknowledgements, undefined);
            return yield* Effect.void;
          }

          yield* write(`${acknowledgement}\n`).pipe(
            Effect.mapError((error) =>
              SubagentBridgeProtocolError.make({
                subagentId,
                reason: error.message,
              }),
            ),
          );

          if (frame.kind === "event") {
            yield* Queue.offer(events, frame.event);
          }

          return yield* Effect.void;
        });

      const exit = yield* Stream.never.pipe(
        Stream.pipeThroughChannel(Socket.toChannel(socket)),
        Stream.mapEffect((chunk): Effect.Effect<Uint8Array, SubagentBridgeProtocolError> => {
          for (const byte of chunk) {
            if (byte === 0x0a) {
              frameByteCount = 0;
              continue;
            }

            frameByteCount += 1;

            if (frameByteCount > maxSubagentBridgeFrameBytes) {
              return SubagentBridgeProtocolError.make({
                subagentId,
                reason: `Bridge child frame exceeds ${maxSubagentBridgeFrameBytes} bytes`,
              });
            }
          }

          return Effect.succeed(chunk);
        }),
        Stream.pipeThroughChannel(Ndjson.decodeSchema(SubagentBridgeChildFrame)()),
        Stream.runForEach(handleFrame),
        Effect.catchReason("SocketError", "SocketCloseError", (reason, error) =>
          reason.code === 1000 ? Effect.void : Effect.fail(error),
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
        Effect.ensuring(Queue.end(events).pipe(Effect.andThen(Queue.end(acknowledgements)))),
        Effect.onExit((connectionExit) =>
          Exit.isFailure(connectionExit) && !Cause.hasInterruptsOnly(connectionExit.cause)
            ? Deferred.failCause(lifetime, connectionExit.cause)
            : Deferred.succeed(lifetime, undefined).pipe(Effect.asVoid),
        ),
        Effect.exit,
      );

      yield* Exit.match(exit, {
        onFailure: (cause) =>
          established
            ? Effect.void
            : Effect.logWarning("Rejected subagent bridge connection", cause).pipe(
                Effect.annotateLogs({ subagentId }),
              ),
        onSuccess: () => Effect.void,
      });
    }, Effect.scoped);

    yield* server.run(serveConnection).pipe(Effect.forkScoped);

    return { accept: Deferred.await(accepted) } satisfies SubagentBridgeListener;
  });

  const connect = Effect.fn("SubagentBridge.connect")(function* (subagentId: SubagentId) {
    yield* Effect.annotateCurrentSpan({ subagentId });

    const socket = yield* transport.connect(subagentId);
    const write = yield* socket.writer;
    const acknowledgements = yield* Queue.unbounded<void, Cause.Done>();
    const messages = yield* Queue.unbounded<string, Cause.Done>();
    const lifetime = yield* Deferred.make<
      void,
      SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
    >();
    const sendLock = yield* Semaphore.make(1);
    const acknowledgement = yield* encodeSubagentBridgeAcknowledgementFrame({
      kind: "ack",
      version: 1,
      subagentId,
    }).pipe(Effect.orDie);
    let frameByteCount = 0;

    const disconnected = Deferred.await(lifetime).pipe(
      Effect.andThen(
        SubagentBridgeDisconnectedError.make({
          subagentId,
          reason: "Bridge connection closed",
        }),
      ),
    );

    const handleFrame = (frame: typeof SubagentBridgeRootFrame.Type) =>
      Effect.gen(function* () {
        if (frame.subagentId !== subagentId) {
          return yield* SubagentBridgeProtocolError.make({
            subagentId,
            reason: `Expected subagent ID ${subagentId}, received ${frame.subagentId}`,
          });
        }

        if (frame.kind === "ack") {
          yield* Queue.offer(acknowledgements, undefined);
          return yield* Effect.void;
        }

        yield* write(`${acknowledgement}\n`).pipe(
          Effect.mapError((error) =>
            SubagentBridgeProtocolError.make({
              subagentId,
              reason: error.message,
            }),
          ),
        );
        yield* Queue.offer(messages, frame.content);
        return yield* Effect.void;
      });

    yield* Stream.never.pipe(
      Stream.pipeThroughChannel(Socket.toChannel(socket)),
      Stream.mapEffect((chunk): Effect.Effect<Uint8Array, SubagentBridgeProtocolError> => {
        for (const byte of chunk) {
          if (byte === 0x0a) {
            frameByteCount = 0;
            continue;
          }

          frameByteCount += 1;

          if (frameByteCount > maxSubagentBridgeFrameBytes) {
            return SubagentBridgeProtocolError.make({
              subagentId,
              reason: `Bridge root frame exceeds ${maxSubagentBridgeFrameBytes} bytes`,
            });
          }
        }

        return Effect.succeed(chunk);
      }),
      Stream.pipeThroughChannel(Ndjson.decodeSchema(SubagentBridgeRootFrame)()),
      Stream.runForEach(handleFrame),
      Effect.catchReason("SocketError", "SocketCloseError", (reason, error) =>
        reason.code === 1000 ? Effect.void : Effect.fail(error),
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
      Effect.ensuring(Queue.end(acknowledgements).pipe(Effect.andThen(Queue.end(messages)))),
      Effect.onExit((exit) =>
        Exit.isFailure(exit) && !Cause.hasInterruptsOnly(exit.cause)
          ? Deferred.failCause(lifetime, exit.cause)
          : Deferred.succeed(lifetime, undefined).pipe(Effect.asVoid),
      ),
      Effect.forkScoped,
    );

    const hello = yield* encodeSubagentBridgeHelloFrame({
      kind: "hello",
      version: 1,
      subagentId,
    }).pipe(Effect.orDie);

    yield* Effect.raceFirst(
      write(`${hello}\n`).pipe(
        Effect.catchTag("SocketError", (error) =>
          SubagentBridgeDisconnectedError.make({
            subagentId,
            reason: error.message,
          }),
        ),
      ),
      disconnected,
    );
    yield* Queue.take(acknowledgements).pipe(Effect.catch(() => disconnected));

    const sendEvent = Effect.fn("SubagentBridge.sendEvent")(
      function* (event: SubagentEvent) {
        const frame = yield* encodeSubagentBridgeEventFrame({
          kind: "event",
          version: 1,
          subagentId,
          event,
        }).pipe(Effect.orDie);
        const bytes = encoder.encode(`${frame}\n`);

        if (bytes.byteLength - 1 > maxSubagentBridgeFrameBytes) {
          return yield* SubagentBridgeSendEventError.make({
            subagentId,
            reason: `Bridge event frame exceeds ${maxSubagentBridgeFrameBytes} bytes`,
          });
        }

        yield* Effect.raceFirst(write(bytes), disconnected);
        yield* Queue.take(acknowledgements).pipe(Effect.catch(() => disconnected));
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

    return {
      sendEvent,
      messages: Stream.fromQueue(messages),
      await: Deferred.await(lifetime),
    } satisfies SubagentBridgeChildSession;
  });

  return { listen, connect };
});

export class SubagentBridge extends Context.Service<SubagentBridge>()("@smith/host/bridge/Bridge", {
  make,
}) {
  static readonly layer = Layer.effect(SubagentBridge, SubagentBridge.make);
}

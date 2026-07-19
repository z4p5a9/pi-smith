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
  maxSubagentBridgeAcknowledgementBytes,
  maxSubagentBridgeChildFrameBytes,
  SubagentBridgeAcknowledgementFrame,
  SubagentBridgeChildFrame,
} from "./BridgeProtocol.ts";
import { SubagentBridgeTransport } from "./BridgeTransport.ts";
import type { SubagentEvent } from "../../subagent/SubagentEvent.ts";
import { SubagentId } from "../../subagent/SubagentId.ts";

export interface SubagentBridgeRootSession {
  readonly events: Stream.Stream<SubagentEvent>;
  readonly await: Effect.Effect<
    void,
    SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
  >;
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
      const lifetime = yield* Deferred.make<
        void,
        SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
      >();
      const acknowledgement = yield* encodeSubagentBridgeAcknowledgementFrame({
        kind: "ack",
        version: 1,
        subagentId,
      }).pipe(Effect.orDie);
      let frameByteCount = 0;
      let established = false;

      const session = {
        events: Stream.fromQueue(events),
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
        Effect.ensuring(Queue.end(events)),
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
    const lifetime = yield* Deferred.make<
      void,
      SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
    >();
    const sendLock = yield* Semaphore.make(1);
    let frameByteCount = 0;

    const disconnected = Deferred.await(lifetime).pipe(
      Effect.andThen(
        SubagentBridgeDisconnectedError.make({
          subagentId,
          reason: "Bridge connection closed",
        }),
      ),
    );

    const handleAcknowledgement = (frame: SubagentBridgeAcknowledgementFrame) =>
      frame.subagentId === subagentId
        ? Queue.offer(acknowledgements, undefined).pipe(Effect.asVoid)
        : SubagentBridgeProtocolError.make({
            subagentId,
            reason: `Expected subagent ID ${subagentId}, received ${frame.subagentId}`,
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
      Stream.runForEach(handleAcknowledgement),
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
      Effect.ensuring(Queue.end(acknowledgements)),
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

        if (bytes.byteLength - 1 > maxSubagentBridgeChildFrameBytes) {
          return yield* SubagentBridgeSendEventError.make({
            subagentId,
            reason: `Bridge event frame exceeds ${maxSubagentBridgeChildFrameBytes} bytes`,
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

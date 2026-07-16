import { Context, Deferred, Effect, Exit, Fiber, Layer, Result, Schema, Stream } from "effect";
import * as Ndjson from "effect/unstable/encoding/Ndjson";
import * as Socket from "effect/unstable/socket/Socket";

import {
  encodeSubagentBridgeHandshake,
  maxSubagentBridgeHandshakeBytes,
  SubagentBridgeHandshake,
} from "./SubagentBridgeProtocol.ts";
import { SubagentBridgeTransport } from "./SubagentBridgeTransport.ts";
import { SubagentId } from "./SubagentId.ts";

export interface SubagentBridgeSession {
  readonly await: Effect.Effect<void, SubagentBridgeDisconnectedError>;
}

export interface SubagentBridgeListener {
  readonly accept: Effect.Effect<SubagentBridgeSession>;
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

export class SubagentBridgeHandshakeError extends Schema.TaggedErrorClass<SubagentBridgeHandshakeError>()(
  "SubagentBridgeHandshakeError",
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
    const accepted = yield* Deferred.make<SubagentBridgeSession>();

    const runConnection = Effect.fn("SubagentBridge.runConnection")(function* (
      socket: Socket.Socket,
    ) {
      const handshake = yield* Deferred.make<
        SubagentBridgeHandshake,
        SubagentBridgeHandshakeError
      >();
      const lineFeedByte = 0x0a;
      let handshakeByteCount = 0;

      const incomingBytes = Stream.pipeThroughChannel(Stream.never, Socket.toChannel(socket));
      const handshakes = incomingBytes.pipe(
        Stream.mapEffect((chunk) => {
          for (const byte of chunk) {
            if (byte === lineFeedByte) {
              handshakeByteCount = 0;
              continue;
            }

            handshakeByteCount += 1;

            if (handshakeByteCount > maxSubagentBridgeHandshakeBytes) {
              return SubagentBridgeHandshakeError.make({
                subagentId,
                reason: `Bridge handshake exceeds ${maxSubagentBridgeHandshakeBytes} bytes`,
              });
            }
          }

          return Effect.succeed(chunk);
        }),
        Stream.pipeThroughChannel(Ndjson.decodeSchema(SubagentBridgeHandshake)()),
      );

      const connectionFiber = yield* handshakes.pipe(
        Stream.runForEach((value) => Deferred.succeed(handshake, value).pipe(Effect.asVoid)),
        Effect.catchTag(["SocketError", "NdjsonError", "SchemaError"], (error) =>
          SubagentBridgeHandshakeError.make({
            subagentId,
            reason: String(error),
          }),
        ),
        Effect.onExit((exit) =>
          Exit.isFailure(exit)
            ? Deferred.failCause(handshake, exit.cause)
            : Deferred.fail(
                handshake,
                SubagentBridgeHandshakeError.make({
                  subagentId,
                  reason: "Connection closed before the bridge handshake completed",
                }),
              ),
        ),
        Effect.forkChild,
      );

      const handshakeResult = yield* Deferred.await(handshake).pipe(Effect.result);

      if (Result.isFailure(handshakeResult)) {
        yield* Effect.logWarning(
          "Rejected subagent bridge connection",
          handshakeResult.failure,
        ).pipe(Effect.annotateLogs({ subagentId }));
        return;
      }

      if (handshakeResult.success.subagentId !== subagentId) {
        yield* Effect.logWarning(
          "Rejected subagent bridge connection",
          SubagentBridgeHandshakeError.make({
            subagentId,
            reason: `Expected subagent ID ${subagentId}, received ${handshakeResult.success.subagentId}`,
          }),
        ).pipe(Effect.annotateLogs({ subagentId }));
        return;
      }

      const session = {
        await: Effect.gen(function* () {
          yield* Fiber.await(connectionFiber);

          return yield* SubagentBridgeDisconnectedError.make({
            subagentId,
            reason: "Bridge connection closed",
          });
        }),
      } satisfies SubagentBridgeSession;

      if (!(yield* Deferred.succeed(accepted, session))) {
        return;
      }

      yield* Fiber.await(connectionFiber);
    });

    yield* server.run(runConnection).pipe(Effect.forkScoped);

    return { accept: Deferred.await(accepted) } satisfies SubagentBridgeListener;
  });

  const connect = Effect.fn("SubagentBridge.connect")(function* (subagentId: SubagentId) {
    yield* Effect.annotateCurrentSpan({ subagentId });

    const socket = yield* transport.connect(subagentId);
    const write = yield* socket.writer;
    const connection = yield* socket.run(() => undefined).pipe(Effect.forkScoped);
    const handshake = yield* encodeSubagentBridgeHandshake({
      version: 1,
      subagentId,
    }).pipe(Effect.orDie);

    yield* Effect.raceFirst(
      write(`${handshake}\n`).pipe(
        Effect.mapError((error) =>
          SubagentBridgeConnectError.make({
            subagentId,
            reason: error.message,
          }),
        ),
      ),
      Fiber.await(connection).pipe(
        Effect.flatMap(() =>
          SubagentBridgeConnectError.make({
            subagentId,
            reason: "Bridge connection closed before the handshake was sent",
          }),
        ),
      ),
    );

    return {
      await: Effect.gen(function* () {
        yield* Fiber.await(connection);

        return yield* SubagentBridgeDisconnectedError.make({
          subagentId,
          reason: "Bridge connection closed",
        });
      }),
    } satisfies SubagentBridgeSession;
  });

  return { listen, connect };
});

export class SubagentBridge extends Context.Service<SubagentBridge>()(
  "@smith/subagent/SubagentBridge",
  { make },
) {
  static readonly layer = Layer.effect(SubagentBridge, SubagentBridge.make);
}

import { NodeSocket, NodeSocketServer } from "@effect/platform-node";
import { Deferred, Effect, Exit, Fiber, FileSystem, Layer, Option, Result, Stream } from "effect";
import * as Ndjson from "effect/unstable/encoding/Ndjson";
import * as Socket from "effect/unstable/socket/Socket";

import {
  SubagentBridge,
  SubagentBridgeConnectError,
  SubagentBridgeDisconnectedError,
  SubagentBridgeHandshakeError,
  SubagentBridgeListenError,
  type SubagentBridgeSession,
} from "./SubagentBridge.ts";
import {
  encodeSubagentBridgeHandshake,
  maxSubagentBridgeHandshakeBytes,
  SubagentBridgeHandshake,
} from "./SubagentBridgeProtocol.ts";
import type { SubagentId } from "./SubagentId.ts";

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const uid = yield* Effect.sync(() => process.getuid?.());
  const runtimeDirectory = `/tmp/smith-${uid ?? "unsupported"}`;
  const socketPath = (subagentId: SubagentId) => `${runtimeDirectory}/${subagentId}.sock`;

  const listen = Effect.fn("SubagentBridge.listen")(function* (subagentId: SubagentId) {
    yield* Effect.annotateCurrentSpan({
      subagentId,
      transport: "unix-socket",
    });

    if (uid === undefined) {
      return yield* SubagentBridgeListenError.make({
        subagentId,
        reason: "Unix user IDs are unavailable on this platform",
      });
    }

    yield* fs.makeDirectory(runtimeDirectory, { recursive: true, mode: 0o700 }).pipe(
      Effect.mapError((error) =>
        SubagentBridgeListenError.make({
          subagentId,
          reason: error.message,
        }),
      ),
    );

    const [temporaryDirectory, resolvedRuntimeDirectory, info] = yield* Effect.all([
      fs.realPath("/tmp"),
      fs.realPath(runtimeDirectory),
      fs.stat(runtimeDirectory),
    ]).pipe(
      Effect.mapError((error) =>
        SubagentBridgeListenError.make({
          subagentId,
          reason: error.message,
        }),
      ),
    );

    if (resolvedRuntimeDirectory !== `${temporaryDirectory}/smith-${uid}`) {
      return yield* SubagentBridgeListenError.make({
        subagentId,
        reason: `Bridge runtime directory must not be a symbolic link: ${runtimeDirectory}`,
      });
    }

    if (info.type !== "Directory") {
      return yield* SubagentBridgeListenError.make({
        subagentId,
        reason: `Bridge runtime path is not a directory: ${runtimeDirectory}`,
      });
    }

    if (Option.getOrUndefined(info.uid) !== uid) {
      return yield* SubagentBridgeListenError.make({
        subagentId,
        reason: `Bridge runtime directory is owned by another user: ${runtimeDirectory}`,
      });
    }

    if ((info.mode & 0o077) !== 0) {
      return yield* SubagentBridgeListenError.make({
        subagentId,
        reason: `Bridge runtime directory grants group or other permissions: ${runtimeDirectory}`,
      });
    }

    const server = yield* NodeSocketServer.make({
      path: socketPath(subagentId),
    }).pipe(
      Effect.mapError((error) =>
        SubagentBridgeListenError.make({
          subagentId,
          reason: error.message,
        }),
      ),
    );
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
        ).pipe(
          Effect.annotateLogs({
            subagentId,
            transport: "unix-socket",
          }),
        );
        return;
      }

      if (handshakeResult.success.subagentId !== subagentId) {
        yield* Effect.logWarning(
          "Rejected subagent bridge connection",
          SubagentBridgeHandshakeError.make({
            subagentId,
            reason: `Expected subagent ID ${subagentId}, received ${handshakeResult.success.subagentId}`,
          }),
        ).pipe(
          Effect.annotateLogs({
            subagentId,
            transport: "unix-socket",
          }),
        );
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

    return { accept: Deferred.await(accepted) };
  });

  const connect = Effect.fn("SubagentBridge.connect")(function* (subagentId: SubagentId) {
    yield* Effect.annotateCurrentSpan({
      subagentId,
      transport: "unix-socket",
    });

    if (uid === undefined) {
      return yield* SubagentBridgeConnectError.make({
        subagentId,
        reason: "Unix user IDs are unavailable on this platform",
      });
    }

    const socket = yield* NodeSocket.makeNet({ path: socketPath(subagentId) });
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

export const layer = Layer.effect(SubagentBridge, make);

import { NodeSocket, NodeSocketServer } from "@effect/platform-node";
import { Deferred, Effect, Fiber, FileSystem, Layer, Option, Ref, Result, Schema } from "effect";

import {
  SubagentBridge,
  SubagentBridgeConnectError,
  SubagentBridgeDisconnectedError,
  SubagentBridgeHandshakeError,
  SubagentBridgeListenError,
  type SubagentBridgeSession,
} from "./SubagentBridge.ts";
import { SubagentId } from "./SubagentId.ts";

const SubagentBridgeHandshake = Schema.Struct({
  version: Schema.Literal(1),
  subagentId: SubagentId,
});

const encodeSubagentBridgeHandshake = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeHandshake),
);

const decodeSubagentBridgeHandshake = Schema.decodeUnknownEffect(
  Schema.fromJsonString(SubagentBridgeHandshake),
);

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const uid = yield* Effect.sync(() => process.getuid?.());
  const runtimeDirectory = `/tmp/smith-${uid ?? "unsupported"}`;
  const socketPath = (subagentId: SubagentId) => `${runtimeDirectory}/${subagentId}.sock`;

  const listen = Effect.fn("SubagentBridge.listen")(function* (subagentId: SubagentId) {
    yield* Effect.annotateCurrentSpan({ subagentId, transport: "unix-socket" });

    if (uid === undefined) {
      return yield* SubagentBridgeListenError.make({
        subagentId,
        reason: "Unix user IDs are unavailable on this platform",
      });
    }

    yield* fs
      .makeDirectory(runtimeDirectory, { recursive: true, mode: 0o700 })
      .pipe(
        Effect.mapError((error) =>
          SubagentBridgeListenError.make({ subagentId, reason: error.message }),
        ),
      );

    const [temporaryDirectory, resolvedRuntimeDirectory, info] = yield* Effect.all([
      fs.realPath("/tmp"),
      fs.realPath(runtimeDirectory),
      fs.stat(runtimeDirectory),
    ]).pipe(
      Effect.mapError((error) =>
        SubagentBridgeListenError.make({ subagentId, reason: error.message }),
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

    const server = yield* NodeSocketServer.make({ path: socketPath(subagentId) }).pipe(
      Effect.mapError((error) =>
        SubagentBridgeListenError.make({ subagentId, reason: error.message }),
      ),
    );
    const accepted = yield* Deferred.make<SubagentBridgeSession, SubagentBridgeHandshakeError>();

    yield* server
      .run((socket) =>
        Effect.gen(function* () {
          const handshake = yield* Deferred.make<
            typeof SubagentBridgeHandshake.Type,
            SubagentBridgeHandshakeError
          >();
          const state = yield* Ref.make({ buffer: "", complete: false });
          const decoder = new TextDecoder();

          const connection = yield* socket
            .run((chunk) =>
              Effect.gen(function* () {
                const frame = yield* Ref.modify(state, (prev) => {
                  if (prev.complete) {
                    return [undefined, prev] as const;
                  }

                  const buffer = prev.buffer + decoder.decode(chunk);
                  const delimiter = buffer.indexOf("\n");

                  if (delimiter === -1) {
                    const next = { buffer, complete: false };
                    return [undefined, next] as const;
                  }

                  const next = { buffer: "", complete: true };
                  return [buffer.slice(0, delimiter), next] as const;
                });

                if (frame === undefined) {
                  return;
                }

                yield* Deferred.complete(
                  handshake,
                  decodeSubagentBridgeHandshake(frame).pipe(
                    Effect.mapError((error) =>
                      SubagentBridgeHandshakeError.make({
                        subagentId,
                        reason: String(error),
                      }),
                    ),
                  ),
                );
              }),
            )
            .pipe(
              Effect.onExit(() =>
                Deferred.fail(
                  handshake,
                  SubagentBridgeHandshakeError.make({
                    subagentId,
                    reason: "Connection closed before the bridge handshake completed",
                  }),
                ),
              ),
              Effect.forkChild,
            );

          const result = yield* Deferred.await(handshake).pipe(Effect.result);

          if (Result.isFailure(result)) {
            yield* Deferred.fail(accepted, result.failure);
            return;
          }

          if (result.success.subagentId !== subagentId) {
            yield* Deferred.fail(
              accepted,
              SubagentBridgeHandshakeError.make({
                subagentId,
                reason: `Expected subagent ID ${subagentId}, received ${result.success.subagentId}`,
              }),
            );
            return;
          }

          const session = {
            await: Effect.gen(function* () {
              yield* Fiber.await(connection);

              return yield* SubagentBridgeDisconnectedError.make({
                subagentId,
                reason: "Bridge connection closed",
              });
            }),
          } satisfies SubagentBridgeSession;

          if (!(yield* Deferred.succeed(accepted, session))) {
            return;
          }

          yield* Fiber.await(connection);
        }),
      )
      .pipe(Effect.forkScoped);

    return { accept: Deferred.await(accepted) };
  });

  const connect = Effect.fn("SubagentBridge.connect")(function* (subagentId: SubagentId) {
    yield* Effect.annotateCurrentSpan({ subagentId, transport: "unix-socket" });

    if (uid === undefined) {
      return yield* SubagentBridgeConnectError.make({
        subagentId,
        reason: "Unix user IDs are unavailable on this platform",
      });
    }

    const socket = yield* NodeSocket.makeNet({ path: socketPath(subagentId) });
    const write = yield* socket.writer;
    const connection = yield* socket.run(() => undefined).pipe(Effect.forkScoped);
    const handshake = yield* encodeSubagentBridgeHandshake({ version: 1, subagentId }).pipe(
      Effect.orDie,
    );

    yield* Effect.raceFirst(
      write(`${handshake}\n`).pipe(
        Effect.mapError((error) =>
          SubagentBridgeConnectError.make({ subagentId, reason: error.message }),
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

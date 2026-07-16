import { NodeSocket, NodeSocketServer } from "@effect/platform-node";
import { Effect, FileSystem, Layer, Option } from "effect";

import { SubagentBridgeConnectError, SubagentBridgeListenError } from "./SubagentBridge.ts";
import { SubagentBridgeTransport } from "./SubagentBridgeTransport.ts";
import type { SubagentId } from "./SubagentId.ts";

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const uid = yield* Effect.sync(() => process.getuid?.());
  const runtimeDirectory = `/tmp/smith-${uid ?? "unsupported"}`;
  const socketPath = (subagentId: SubagentId) => `${runtimeDirectory}/${subagentId}.sock`;

  const listen = Effect.fn("SubagentBridgeTransport.listen")(function* (subagentId: SubagentId) {
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

    return yield* NodeSocketServer.make({
      path: socketPath(subagentId),
    }).pipe(
      Effect.mapError((error) =>
        SubagentBridgeListenError.make({
          subagentId,
          reason: error.message,
        }),
      ),
    );
  });

  const connect = Effect.fn("SubagentBridgeTransport.connect")(function* (subagentId: SubagentId) {
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

    return yield* NodeSocket.makeNet({ path: socketPath(subagentId) });
  });

  return { listen, connect };
});

export const layer = Layer.effect(SubagentBridgeTransport, make);

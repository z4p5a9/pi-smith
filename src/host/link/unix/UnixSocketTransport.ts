import { NodeSocket, NodeSocketServer } from "@effect/platform-node";
import { Effect, FileSystem, Layer, Option } from "effect";

import {
  SubagentLinkConnectError,
  SubagentLinkListenError,
  SubagentLinkTransport,
} from "../Transport.ts";
import type { SubagentId } from "../../../subagent/SubagentId.ts";

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const uid = yield* Effect.sync(() => process.getuid?.());
  const runtimeDirectory = `/tmp/smith-${uid ?? "unsupported"}`;
  const socketPath = (subagentId: SubagentId) => `${runtimeDirectory}/${subagentId}.sock`;

  const listen = Effect.fn("UnixSocketTransport.listen")(function* (subagentId: SubagentId) {
    yield* Effect.annotateCurrentSpan({
      subagentId,
      transport: "unix-socket",
    });

    if (uid === undefined) {
      return yield* SubagentLinkListenError.make({
        subagentId,
        reason: "Unix user IDs are unavailable on this platform",
      });
    }

    yield* fs.makeDirectory(runtimeDirectory, { recursive: true, mode: 0o700 }).pipe(
      Effect.mapError((error) =>
        SubagentLinkListenError.make({
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
        SubagentLinkListenError.make({
          subagentId,
          reason: error.message,
        }),
      ),
    );

    if (resolvedRuntimeDirectory !== `${temporaryDirectory}/smith-${uid}`) {
      return yield* SubagentLinkListenError.make({
        subagentId,
        reason: `Link runtime directory must not be a symbolic link: ${runtimeDirectory}`,
      });
    }

    if (info.type !== "Directory") {
      return yield* SubagentLinkListenError.make({
        subagentId,
        reason: `Link runtime path is not a directory: ${runtimeDirectory}`,
      });
    }

    if (Option.getOrUndefined(info.uid) !== uid) {
      return yield* SubagentLinkListenError.make({
        subagentId,
        reason: `Link runtime directory is owned by another user: ${runtimeDirectory}`,
      });
    }

    if ((info.mode & 0o077) !== 0) {
      return yield* SubagentLinkListenError.make({
        subagentId,
        reason: `Link runtime directory grants group or other permissions: ${runtimeDirectory}`,
      });
    }

    return yield* NodeSocketServer.make({
      path: socketPath(subagentId),
    }).pipe(
      Effect.mapError((error) =>
        SubagentLinkListenError.make({
          subagentId,
          reason: error.message,
        }),
      ),
    );
  });

  const connect = Effect.fn("UnixSocketTransport.connect")(function* (subagentId: SubagentId) {
    yield* Effect.annotateCurrentSpan({
      subagentId,
      transport: "unix-socket",
    });

    if (uid === undefined) {
      return yield* SubagentLinkConnectError.make({
        subagentId,
        reason: "Unix user IDs are unavailable on this platform",
      });
    }

    return yield* NodeSocket.makeNet({ path: socketPath(subagentId) });
  });

  return { listen, connect };
});

export const layer = Layer.effect(SubagentLinkTransport, make);

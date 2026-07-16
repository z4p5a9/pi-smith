import { NodeFileSystem } from "@effect/platform-node";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Config, ConfigProvider, Effect, Layer, ManagedRuntime } from "effect";

import { SubagentBridge } from "../subagent/SubagentBridge.ts";
import { SubagentId } from "../subagent/SubagentId.ts";
import { layer as unixSocketSubagentBridgeTransportLayer } from "../subagent/UnixSocketSubagentBridgeTransport.ts";

export default function extension(pi: ExtensionAPI): void {
  const subagentId = Effect.runSync(
    Config.schema(SubagentId, "SMITH_SUBAGENT_ID").pipe(
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        ConfigProvider.fromEnv({ preserveEmptyStrings: true }),
      ),
    ),
  );

  const runtime = ManagedRuntime.make(
    Layer.effectDiscard(
      Effect.gen(function* () {
        const bridge = yield* SubagentBridge;
        const session = yield* bridge.connect(subagentId);

        yield* session.sendEvent({ kind: "ready" });
        yield* session.await.pipe(
          Effect.catchTag(
            ["SubagentBridgeProtocolError", "SubagentBridgeDisconnectedError"],
            (error) =>
              Effect.logWarning("Subagent bridge disconnected", error).pipe(
                Effect.annotateLogs({ subagentId }),
              ),
          ),
          Effect.forkScoped,
        );
      }),
    ).pipe(
      Layer.provide(SubagentBridge.layer),
      Layer.provide(unixSocketSubagentBridgeTransportLayer),
      Layer.provide(NodeFileSystem.layer),
    ),
  );

  pi.on("session_start", () => runtime.runPromise(Effect.void));
  pi.on("session_shutdown", () => runtime.dispose());
}

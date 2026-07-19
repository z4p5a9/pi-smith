import { NodeFileSystem } from "@effect/platform-node";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Config, ConfigProvider, Effect, Layer, ManagedRuntime } from "effect";

import { ChildSession } from "../harness/pi/ChildSession.ts";
import { SubagentBridge } from "../host/bridge/Bridge.ts";
import * as UnixSocketBridgeTransport from "../host/bridge/unix/UnixSocketBridgeTransport.ts";
import { SubagentId } from "../subagent/SubagentId.ts";

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
    ChildSession.layer(subagentId).pipe(
      Layer.provide(SubagentBridge.layer),
      Layer.provide(UnixSocketBridgeTransport.layer),
      Layer.provide(NodeFileSystem.layer),
    ),
  );

  let shuttingDown = false;

  pi.on("session_start", (_event, ctx) => {
    const starting = runtime.runPromise(ChildSession.use((session) => session.start));

    // The bridge connection ending means the root released this subagent.
    void starting
      .then(() =>
        runtime.runPromise(ChildSession.use((session) => session.await.pipe(Effect.ignore))),
      )
      .then(
        () => {
          if (!shuttingDown) {
            ctx.shutdown();
          }
        },
        () => undefined,
      );

    return starting;
  });

  pi.on("agent_settled", (_event, ctx) =>
    runtime.runPromise(ChildSession.use((session) => session.sendSettled(ctx.sessionManager))),
  );

  pi.on("session_shutdown", () => {
    shuttingDown = true;
    return runtime.dispose();
  });
}

import { NodeFileSystem } from "@effect/platform-node";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Config, ConfigProvider, Effect, Layer, ManagedRuntime } from "effect";

import { ChildSession } from "../harness/pi/ChildSession.ts";
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
    ChildSession.layer(subagentId).pipe(
      Layer.provide(SubagentBridge.layer),
      Layer.provide(unixSocketSubagentBridgeTransportLayer),
      Layer.provide(NodeFileSystem.layer),
    ),
  );

  pi.on("session_start", () => runtime.runPromise(ChildSession.use((session) => session.start)));

  pi.on("agent_settled", (_event, ctx) =>
    runtime.runPromise(
      Effect.gen(function* () {
        const session = yield* ChildSession;

        yield* session.sendSettled(ctx.sessionManager);
        ctx.shutdown();
      }),
    ),
  );

  pi.on("session_shutdown", () =>
    Effect.runPromise(
      Effect.promise(() =>
        runtime.runPromiseExit(ChildSession.use((session) => session.close)),
      ).pipe(Effect.ensuring(Effect.promise(() => runtime.dispose())), Effect.flatten),
    ),
  );
}

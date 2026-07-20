import { NodeFileSystem } from "@effect/platform-node";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Config, ConfigProvider, Effect, Layer, ManagedRuntime, Stream } from "effect";

import { ChildSession } from "../harness/pi/ChildSession.ts";
import * as UnixSocketTransport from "../host/link/unix/UnixSocketTransport.ts";
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
      Layer.provide(UnixSocketTransport.layer),
      Layer.provide(NodeFileSystem.layer),
    ),
  );

  let shuttingDown = false;

  pi.on("session_start", (_event, ctx) => {
    const starting = runtime.runPromise(ChildSession.use((session) => session.start));

    // Root messages become follow-up prompts in the child Pi session; other
    // root datagrams have no defined child-side meaning yet and are dropped.
    void starting
      .then(() =>
        runtime.runPromise(
          ChildSession.use((session) =>
            session.inbox.pipe(
              Stream.runForEach((datagram) =>
                datagram.kind === "message"
                  ? Effect.sync(() => {
                      pi.sendMessage(
                        {
                          customType: "smith-root-message",
                          content: datagram.content,
                          display: true,
                        },
                        { deliverAs: "followUp", triggerTurn: true },
                      );
                    })
                  : Effect.logDebug("Dropped root failure datagram"),
              ),
            ),
          ),
        ),
      )
      .catch(() => undefined);

    // The link connection ending means the root released this subagent.
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

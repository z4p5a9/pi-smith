import { NodeFileSystem } from "@effect/platform-node";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Config, ConfigProvider, Effect, Layer, ManagedRuntime, Queue, Stream } from "effect";

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
  const commands = Effect.runSync(Queue.unbounded<string>());
  const ready = Effect.runSync(Queue.unbounded<void>());

  let shuttingDown = false;

  pi.on("session_start", (_event, ctx) => {
    const starting = runtime.runPromise(
      ChildSession.use((session) => session.start).pipe(
        Effect.tapError((error) =>
          Effect.logWarning("Subagent link failed", error).pipe(
            Effect.annotateLogs({ subagentId }),
          ),
        ),
      ),
    );

    // Root messages wait for the preceding Pi run to settle and report before
    // they start a new run; other root datagrams have no child-side meaning.
    void starting
      .then(() =>
        runtime.runPromise(
          Effect.raceFirst(
            ChildSession.use((session) =>
              session.inbox.pipe(
                Stream.runForEach((datagram) =>
                  datagram.kind === "message"
                    ? Queue.offer(commands, datagram.content).pipe(Effect.asVoid)
                    : Effect.logDebug("Dropped root failure datagram"),
                ),
              ),
            ),
            Effect.forever(
              Effect.gen(function* () {
                yield* Queue.take(ready);
                const content = yield* Queue.take(commands);

                yield* Effect.sync(() => {
                  pi.sendMessage(
                    {
                      customType: "smith-root-message",
                      content,
                      display: true,
                    },
                    { triggerTurn: true },
                  );
                }).pipe(
                  Effect.tapDefect((error) =>
                    Effect.logError("Failed to deliver root message to Pi", error).pipe(
                      Effect.annotateLogs({ subagentId }),
                    ),
                  ),
                );
              }),
            ),
          ).pipe(
            Effect.catch((error) =>
              Effect.logWarning("Subagent link failed", error).pipe(
                Effect.annotateLogs({ subagentId }),
              ),
            ),
          ),
        ),
      )
      .catch(() => undefined)
      .then(() => {
        if (!shuttingDown) {
          shuttingDown = true;
          ctx.shutdown();
        }
      })
      .catch(() => undefined);

    return starting;
  });

  pi.on("agent_settled", (_event, ctx) =>
    runtime.runPromise(
      ChildSession.use((session) => session.sendSettled(ctx.sessionManager)).pipe(
        Effect.andThen(Queue.offer(ready, undefined)),
        Effect.asVoid,
      ),
    ),
  );

  pi.on("session_shutdown", () => {
    shuttingDown = true;
    return runtime.dispose();
  });
}

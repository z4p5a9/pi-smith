import { NodeFileSystem } from "@effect/platform-node";
import type { ExtensionAPI, SessionEntry } from "@earendil-works/pi-coding-agent";
import { Config, ConfigProvider, Effect, Layer, ManagedRuntime } from "effect";

import { SubagentId } from "../subagent/SubagentId.ts";
import { PiSubagentHarness } from "../subagent/PiSubagentHarness.ts";
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
    PiSubagentHarness.layer(subagentId).pipe(
      Layer.provide(unixSocketSubagentBridgeTransportLayer),
      Layer.provide(NodeFileSystem.layer),
    ),
  );

  pi.on("session_start", () => runtime.runPromise(Effect.void));
  pi.on("agent_settled", (_event, ctx) =>
    runtime.runPromise(
      Effect.gen(function* () {
        const harness = yield* PiSubagentHarness;
        const branch = ctx.sessionManager.getBranch();
        let entry: SessionEntry | undefined;

        for (let index = branch.length - 1; index >= 0; index--) {
          const currentEntry = branch[index];

          if (currentEntry?.type === "message" && currentEntry.message.role === "assistant") {
            entry = currentEntry;
            break;
          }
        }

        if (entry === undefined || entry.type !== "message" || entry.message.role !== "assistant") {
          yield* harness.sendEvent({
            kind: "failure",
            reason: "Pi settled without an assistant response",
          });
        } else if (entry.message.stopReason === "error" || entry.message.stopReason === "aborted") {
          yield* harness.sendEvent({
            kind: "failure",
            reason: entry.message.errorMessage ?? `Request ${entry.message.stopReason}`,
          });
        } else {
          const content: Array<string> = [];

          for (const block of entry.message.content) {
            if (block.type === "text") {
              content.push(block.text);
            }
          }

          yield* harness.sendEvent({ kind: "message", content: content.join("\n") });
        }
      }).pipe(
        Effect.andThen(
          Effect.sync(() => {
            ctx.shutdown();
          }),
        ),
      ),
    ),
  );
  pi.on("session_shutdown", () =>
    Effect.runPromise(
      Effect.promise(() =>
        runtime.runPromiseExit(
          Effect.gen(function* () {
            const harness = yield* PiSubagentHarness;

            yield* harness.close;
          }),
        ),
      ).pipe(Effect.ensuring(Effect.promise(() => runtime.dispose())), Effect.flatten),
    ),
  );
}

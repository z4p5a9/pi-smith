import { NodeChildProcessSpawner, NodeFileSystem, NodePath } from "@effect/platform-node";
import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import {
  Config,
  ConfigProvider,
  Effect,
  Layer,
  ManagedRuntime,
  Option,
  Schema,
  Stream,
} from "effect";
import { Type } from "typebox";

import { layer as cmuxPaneSubagentHostLayer } from "./subagent/CmuxPaneSubagentHost.ts";
import { SubagentCheckpoint } from "./subagent/SubagentCheckpoint.ts";
import { generateSubagentId } from "./subagent/SubagentId.ts";
import { SubagentPool } from "./subagent/SubagentPool.ts";
import { SubagentSupervisor } from "./subagent/SubagentSupervisor.ts";
import { layer as unixSocketSubagentBridgeTransportLayer } from "./subagent/UnixSocketSubagentBridgeTransport.ts";

export default function extension(pi: ExtensionAPI): void {
  const childMarker = Effect.runSync(
    Config.option(Config.string("SMITH_SUBAGENT_ID")).pipe(
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        ConfigProvider.fromEnv({ preserveEmptyStrings: true }),
      ),
    ),
  );

  if (Option.isSome(childMarker)) {
    return;
  }

  const [workspaceId, surfaceId] = Effect.runSync(
    Effect.all([
      Config.schema(Schema.String.check(Schema.isUUID()), "CMUX_WORKSPACE_ID"),
      Config.schema(Schema.String.check(Schema.isUUID()), "CMUX_SURFACE_ID"),
    ]).pipe(
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        ConfigProvider.fromEnv({ preserveEmptyStrings: true }),
      ),
    ),
  );

  const runtime = ManagedRuntime.make(
    SubagentPool.layer.pipe(
      Layer.provide(cmuxPaneSubagentHostLayer({ workspaceId, surfaceId })),
      Layer.provide(unixSocketSubagentBridgeTransportLayer),
      Layer.provide(NodeChildProcessSpawner.layer),
      Layer.provide(NodeFileSystem.layer),
      Layer.provide(NodePath.layer),
    ),
  );

  pi.on("session_start", (_event, ctx) => {
    runtime.runFork(
      Effect.gen(function* () {
        const supervisor = yield* SubagentSupervisor;

        yield* supervisor.events.pipe(
          Stream.runForEach(({ event, subagentId }) => {
            return Effect.try(() => {
              pi.sendMessage(
                {
                  customType: "smith-subagent",
                  content:
                    event.kind === "completed"
                      ? `Subagent ${subagentId} completed:\n\n${event.report}`
                      : `Subagent ${subagentId} failed:\n\n${event.reason}`,
                  display: false,
                  details: { subagentId, event },
                },
                {
                  deliverAs: "followUp",
                  triggerTurn: true,
                },
              );

              if (ctx.hasUI) {
                ctx.ui.notify(
                  event.kind === "completed"
                    ? `Subagent ${subagentId} completed`
                    : `Subagent ${subagentId} failed`,
                  event.kind === "completed" ? "info" : "error",
                );
              }
            }).pipe(
              Effect.catch((error) =>
                Effect.logError("Failed to deliver subagent event to root Pi", error).pipe(
                  Effect.annotateLogs({ subagentId }),
                ),
              ),
            );
          }),
        );
      }),
    );
  });

  pi.on("session_shutdown", () => {
    return runtime.dispose();
  });

  pi.registerTool({
    name: "subagent",
    label: "Subagent",
    description: "Create and start a subagent.",
    parameters: Type.Object(
      {
        title: Type.String({
          description: "Human-readable title for the subagent.",
          minLength: 1,
          pattern: "\\S",
        }),
        prompt: Type.String({
          description: "Initial task prompt for the subagent.",
          minLength: 1,
          pattern: "\\S",
        }),
      },
      { additionalProperties: false },
    ),
    execute(toolCallId, { prompt, title }, _signal, _onUpdate, ctx) {
      return runtime.runPromise(
        Effect.gen(function* () {
          const subagentId = yield* generateSubagentId(title);
          const checkpoint = yield* SubagentCheckpoint;
          const pool = yield* SubagentPool;
          const spec = { title, prompt, cwd: ctx.cwd };

          yield* checkpoint.put({ subagentId, status: "queued", ...spec });
          yield* pool.submit(subagentId, spec);

          return {
            content: [{ type: "text" as const, text: subagentId }],
            details: { subagentId },
          };
        }).pipe(
          Effect.withSpan("subagent", {
            attributes: { toolCallId },
          }),
        ),
      );
    },
  });
}

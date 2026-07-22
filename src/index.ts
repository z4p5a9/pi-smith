import { NodeChildProcessSpawner, NodeFileSystem, NodePath } from "@effect/platform-node";
import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
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

import * as PiSubagentHarness from "./harness/pi/PiSubagentHarness.ts";
import * as CmuxPaneHost from "./host/cmux/CmuxPaneHost.ts";
import * as UnixSocketTransport from "./host/link/unix/UnixSocketTransport.ts";
import { SubagentCapacity } from "./subagent/SubagentCapacity.ts";
import { SubagentCheckpoint, SubagentRecord } from "./subagent/SubagentCheckpoint.ts";
import { SubagentCoordinator } from "./subagent/SubagentCoordinator.ts";
import { SubagentEventOutbox } from "./subagent/SubagentEventOutbox.ts";
import { decodeSubagentId } from "./subagent/SubagentId.ts";

const encodeSubagentRecord = Schema.encodeEffect(Schema.fromJsonString(SubagentRecord));

export const deliverSubagentEvents = Effect.fn("deliverSubagentEvents")(function* (
  pi: Pick<ExtensionAPI, "sendMessage">,
  ctx: {
    readonly hasUI: boolean;
    readonly ui: Pick<ExtensionContext["ui"], "notify">;
  },
) {
  const eventOutbox = yield* SubagentEventOutbox;

  yield* eventOutbox.events.pipe(
    Stream.runForEach(({ event, subagentId }) =>
      Effect.try(() => {
        if (event.kind === "message-rejected") {
          pi.sendMessage(
            {
              customType: "smith-subagent",
              content:
                `Message ${event.messageId} to subagent ${subagentId} was rejected before ` +
                `delivery: ${event.actualBytes} bytes exceeds the ${event.maxBytes}-byte limit.`,
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
              `Message ${event.messageId} to subagent ${subagentId} was rejected`,
              "error",
            );
          }

          return;
        }

        pi.sendMessage(
          {
            customType: "smith-subagent",
            content:
              event.kind === "message"
                ? `Subagent ${subagentId} reported:\n\n${event.content}`
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
            event.kind === "message"
              ? `Subagent ${subagentId} reported`
              : `Subagent ${subagentId} failed`,
            event.kind === "message" ? "info" : "error",
          );
        }
      }).pipe(
        Effect.catch((error) =>
          Effect.logError("Failed to deliver subagent event to root Pi", error).pipe(
            Effect.annotateLogs({ subagentId }),
          ),
        ),
      ),
    ),
  );
});

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
    SubagentCoordinator.layer.pipe(
      Layer.provideMerge(SubagentEventOutbox.layer),
      Layer.provide(SubagentCapacity.layer(10)),
      Layer.provide(SubagentCheckpoint.layer),
      Layer.provide(PiSubagentHarness.layer),
      Layer.provide(CmuxPaneHost.layer({ workspaceId, surfaceId })),
      Layer.provide(UnixSocketTransport.layer),
      Layer.provide(NodeChildProcessSpawner.layer),
      Layer.provide(NodeFileSystem.layer),
      Layer.provide(NodePath.layer),
    ),
  );

  pi.on("session_start", (_event, ctx) => {
    runtime.runFork(deliverSubagentEvents(pi, ctx));
  });

  pi.on("session_shutdown", () => {
    return runtime.dispose();
  });

  pi.registerTool({
    name: "subagent",
    label: "Subagent",
    description:
      "Create and start a subagent. Ephemeral subagents complete one task and report " +
      "once; persistent subagents stay alive after reporting and accept follow-up " +
      "messages through the subagent_send tool. Reports arrive asynchronously as " +
      "follow-up messages.",
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
        mode: Type.Optional(
          Type.Union([Type.Literal("ephemeral"), Type.Literal("persistent")], {
            description: "Lifetime mode; defaults to ephemeral.",
          }),
        ),
      },
      { additionalProperties: false },
    ),
    execute(toolCallId, { mode, prompt, title }, _signal, _onUpdate, ctx) {
      return runtime.runPromise(
        Effect.gen(function* () {
          const coordinator = yield* SubagentCoordinator;
          const spec = { title, prompt, cwd: ctx.cwd, mode: mode ?? ("ephemeral" as const) };
          const subagentId = yield* coordinator.create(spec);

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

  pi.registerTool({
    name: "subagent_send",
    label: "Send to subagent",
    description:
      "Queue a follow-up message for a subagent. An idle persistent subagent starts a " +
      "new turn; a working subagent receives it as steering. The response arrives " +
      "asynchronously as a follow-up message.",
    parameters: Type.Object(
      {
        subagentId: Type.String({
          description: "The subagent ID returned by the subagent tool.",
          minLength: 1,
        }),
        message: Type.String({
          description: "The message to deliver to the subagent.",
          minLength: 1,
          pattern: "\\S",
        }),
      },
      { additionalProperties: false },
    ),
    execute(toolCallId, { message, subagentId }) {
      return runtime.runPromise(
        Effect.gen(function* () {
          const coordinator = yield* SubagentCoordinator;
          const id = yield* decodeSubagentId(subagentId);

          const messageId = yield* coordinator.send(id, message);

          return {
            content: [
              {
                type: "text" as const,
                text: `Queued message ${messageId} for subagent ${id}.`,
              },
            ],
            details: { subagentId: id, messageId },
          };
        }).pipe(
          Effect.catchTags({
            SchemaError: () =>
              Effect.succeed({
                content: [{ type: "text" as const, text: `Invalid subagent ID: ${subagentId}` }],
                details: { subagentId },
              }),
            SubagentUnknownError: () =>
              Effect.succeed({
                content: [{ type: "text" as const, text: `Unknown subagent: ${subagentId}` }],
                details: { subagentId },
              }),
            SubagentInactiveError: () =>
              Effect.succeed({
                content: [
                  { type: "text" as const, text: `Subagent ${subagentId} is no longer active.` },
                ],
                details: { subagentId },
              }),
          }),
          Effect.withSpan("subagent_send", {
            attributes: { toolCallId },
          }),
        ),
      );
    },
  });

  pi.registerTool({
    name: "subagent_kill",
    label: "Kill subagent",
    description: "Kill a subagent, releasing its pane and capacity.",
    parameters: Type.Object(
      {
        subagentId: Type.String({
          description: "The subagent ID returned by the subagent tool.",
          minLength: 1,
        }),
      },
      { additionalProperties: false },
    ),
    execute(toolCallId, { subagentId }) {
      return runtime.runPromise(
        Effect.gen(function* () {
          const coordinator = yield* SubagentCoordinator;
          const id = yield* decodeSubagentId(subagentId);

          yield* coordinator.kill(id);

          return {
            content: [{ type: "text" as const, text: `Killed subagent ${id}.` }],
            details: { subagentId },
          };
        }).pipe(
          Effect.catchTags({
            SchemaError: () =>
              Effect.succeed({
                content: [{ type: "text" as const, text: `Invalid subagent ID: ${subagentId}` }],
                details: { subagentId },
              }),
            SubagentUnknownError: () =>
              Effect.succeed({
                content: [{ type: "text" as const, text: `Unknown subagent: ${subagentId}` }],
                details: { subagentId },
              }),
            SubagentInactiveError: () =>
              Effect.succeed({
                content: [
                  { type: "text" as const, text: `Subagent ${subagentId} is no longer active.` },
                ],
                details: { subagentId },
              }),
          }),
          Effect.withSpan("subagent_kill", {
            attributes: { toolCallId },
          }),
        ),
      );
    },
  });

  pi.registerTool({
    name: "subagent_status",
    label: "Subagent status",
    description: "Read the current status of a subagent.",
    parameters: Type.Object(
      {
        subagentId: Type.String({
          description: "The subagent ID returned by the subagent tool.",
          minLength: 1,
        }),
      },
      { additionalProperties: false },
    ),
    execute(toolCallId, { subagentId }) {
      return runtime.runPromise(
        Effect.gen(function* () {
          const coordinator = yield* SubagentCoordinator;
          const id = yield* decodeSubagentId(subagentId);
          const record = yield* coordinator.status(id);
          const text = yield* encodeSubagentRecord(record).pipe(Effect.orDie);

          return {
            content: [{ type: "text" as const, text }],
            details: { subagentId },
          };
        }).pipe(
          Effect.catchTags({
            SchemaError: () =>
              Effect.succeed({
                content: [{ type: "text" as const, text: `Invalid subagent ID: ${subagentId}` }],
                details: { subagentId },
              }),
            SubagentUnknownError: () =>
              Effect.succeed({
                content: [{ type: "text" as const, text: `Unknown subagent: ${subagentId}` }],
                details: { subagentId },
              }),
          }),
          Effect.withSpan("subagent_status", {
            attributes: { toolCallId },
          }),
        ),
      );
    },
  });
}

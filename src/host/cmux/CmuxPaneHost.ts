import { Config, Duration, Effect, Layer, Schema, Semaphore, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { isPositiveFiniteDuration } from "../../lib/schema.ts";
import type { SubagentId } from "../../subagent/SubagentId.ts";
import { SubagentLinkTransport } from "../link/Transport.ts";
import * as Protocol from "../Protocol.ts";
import {
  SubagentHost,
  SubagentHostResponseError,
  SubagentHostStartError,
  SubagentHostUnavailableError,
  type SubagentCommand,
} from "../Host.ts";

const encodeCmuxRpcParams = Schema.encodeEffect(Schema.UnknownFromJsonString);

const CmuxPaneCreateResponse = Schema.Struct({
  surface_id: Schema.String.check(Schema.isUUID()),
});

const decodeCmuxPaneCreateResponse = Schema.decodeUnknownEffect(
  Schema.fromJsonString(CmuxPaneCreateResponse),
);

const CmuxPaneListResponse = Schema.Struct({
  panes: Schema.Array(
    Schema.Struct({
      surface_ids: Schema.Array(Schema.String.check(Schema.isUUID())),
      pixel_frame: Schema.Struct({
        x: Schema.Finite,
        y: Schema.Finite,
        width: Schema.Finite.check(Schema.isGreaterThan(0)),
        height: Schema.Finite.check(Schema.isGreaterThan(0)),
      }),
    }),
  ),
});

const decodeCmuxPaneListResponse = Schema.decodeUnknownEffect(
  Schema.fromJsonString(CmuxPaneListResponse),
);

const config = Config.schema(
  Schema.DurationFromString.check(isPositiveFiniteDuration()),
  "SMITH_CMUX_PANE_CLOSE_TIMEOUT",
).pipe(Config.withDefault(Duration.seconds(20)));

const make = (root: { readonly workspaceId: string; readonly surfaceId: string }) =>
  Effect.gen(function* () {
    const transport = yield* SubagentLinkTransport;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const closeTimeout = yield* config;
    const layout = yield* Semaphore.make(1);
    const subagentSurfaceIds = new Set<string>();

    const rpc = Effect.fn("CmuxPaneHost.rpc")(function* (
      method: string,
      params: Readonly<Record<string, unknown>>,
    ) {
      const encodedParams = yield* encodeCmuxRpcParams(params).pipe(Effect.orDie);
      const process = yield* spawner.spawn(
        ChildProcess.make("cmux", ["rpc", method, encodedParams], {
          detached: false,
          stdin: "ignore",
          stdout: "pipe",
          stderr: "pipe",
        }),
      );
      const [exitCode, stdout, stderr] = yield* Effect.all(
        [
          process.exitCode,
          process.stdout.pipe(Stream.decodeText, Stream.mkString),
          process.stderr.pipe(Stream.decodeText, Stream.mkString),
        ],
        { concurrency: "unbounded" },
      );

      return { exitCode, stdout, stderr };
    });

    const listPanes = Effect.fn("CmuxPaneHost.listPanes")(function* (subagentId: SubagentId) {
      const result = yield* rpc("pane.list", { workspace_id: root.workspaceId }).pipe(
        Effect.scoped,
        Effect.mapError((error) =>
          SubagentHostUnavailableError.make({
            subagentId,
            host: "cmux-pane",
            reason: error.message,
          }),
        ),
      );

      if (result.exitCode !== 0) {
        return yield* SubagentHostStartError.make({
          subagentId,
          host: "cmux-pane",
          reason: result.stderr.trim(),
        });
      }

      return yield* decodeCmuxPaneListResponse(result.stdout).pipe(
        Effect.mapError((error) =>
          SubagentHostResponseError.make({
            subagentId,
            host: "cmux-pane",
            reason: String(error),
          }),
        ),
      );
    });

    const selectPaneSplit = Effect.fn("CmuxPaneHost.selectPaneSplit")(function* (
      subagentId: SubagentId,
      response: typeof CmuxPaneListResponse.Type,
    ) {
      const rootPane = response.panes.find((pane) => pane.surface_ids.includes(root.surfaceId));

      if (rootPane === undefined) {
        return yield* SubagentHostStartError.make({
          subagentId,
          host: "cmux-pane",
          reason: "CMUX root surface is not in the configured workspace",
        });
      }

      let sourceSurfaceId: string;
      let direction: "right" | "down";

      if (subagentSurfaceIds.size === 0) {
        if (response.panes.length !== 1) {
          return yield* SubagentHostStartError.make({
            subagentId,
            host: "cmux-pane",
            reason: "CMUX workspace contains panes not owned by Smith",
          });
        }

        sourceSurfaceId = root.surfaceId;
        direction = "right";
      } else {
        let target:
          | {
              readonly surfaceId: string;
              readonly x: number;
              readonly y: number;
              readonly width: number;
              readonly height: number;
            }
          | undefined;

        for (const pane of response.panes) {
          const surfaceId = pane.surface_ids.find((current) => subagentSurfaceIds.has(current));

          if (surfaceId === undefined) {
            continue;
          }

          if (
            target === undefined ||
            pane.pixel_frame.width * pane.pixel_frame.height > target.width * target.height ||
            (pane.pixel_frame.width * pane.pixel_frame.height === target.width * target.height &&
              (pane.pixel_frame.y < target.y ||
                (pane.pixel_frame.y === target.y && pane.pixel_frame.x < target.x)))
          ) {
            target = { ...pane.pixel_frame, surfaceId };
          }
        }

        if (target === undefined) {
          return yield* SubagentHostStartError.make({
            subagentId,
            host: "cmux-pane",
            reason: "CMUX subagent panes are not in the configured workspace",
          });
        }

        sourceSurfaceId = target.surfaceId;
        direction = target.width >= target.height ? "right" : "down";
      }

      return { sourceSurfaceId, direction };
    });

    const createPane = Effect.fn("CmuxPaneHost.createPane")(function* (
      subagentId: SubagentId,
      command: SubagentCommand,
      split: {
        readonly sourceSurfaceId: string;
        readonly direction: "right" | "down";
      },
    ) {
      const initialCommand = [command.executable, ...command.args]
        .map((argument) => `'${argument.replaceAll("'", `'"'"'`)}'`)
        .join(" ");
      const result = yield* rpc("pane.create", {
        workspace_id: root.workspaceId,
        surface_id: split.sourceSurfaceId,
        direction: split.direction,
        initial_divider_position: 0.5,
        type: "terminal",
        focus: false,
        initial_command: initialCommand,
        working_directory: command.cwd,
        startup_environment: command.env,
      }).pipe(
        Effect.scoped,
        Effect.mapError((error) =>
          SubagentHostUnavailableError.make({
            subagentId,
            host: "cmux-pane",
            reason: error.message,
          }),
        ),
      );

      if (result.exitCode !== 0) {
        return yield* SubagentHostStartError.make({
          subagentId,
          host: "cmux-pane",
          reason: result.stderr.trim(),
        });
      }

      return yield* decodeCmuxPaneCreateResponse(result.stdout).pipe(
        Effect.mapError((error) =>
          SubagentHostResponseError.make({
            subagentId,
            host: "cmux-pane",
            reason: String(error),
          }),
        ),
      );
    });

    const closeSurface = Effect.fn("CmuxPaneHost.closeSurface")(function* (surfaceId: string) {
      const result = yield* rpc("surface.close", {
        workspace_id: root.workspaceId,
        surface_id: surfaceId,
      }).pipe(Effect.scoped);

      if (result.exitCode !== 0) {
        return yield* Effect.fail(result.stderr.trim());
      }

      return yield* Effect.void;
    });

    const create = Effect.fn("CmuxPaneHost.create")(function* (
      subagentId: SubagentId,
      command: SubagentCommand,
    ) {
      return yield* layout.withPermit(
        Effect.gen(function* () {
          const panes = yield* listPanes(subagentId);
          const liveSurfaceIds = new Set(panes.panes.flatMap((pane) => pane.surface_ids));

          for (const surfaceId of subagentSurfaceIds) {
            if (!liveSurfaceIds.has(surfaceId)) {
              subagentSurfaceIds.delete(surfaceId);
            }
          }

          const split = yield* selectPaneSplit(subagentId, panes);
          const pane = yield* createPane(subagentId, command, split);
          subagentSurfaceIds.add(pane.surface_id);

          return pane;
        }),
      );
    });

    const close = Effect.fn("CmuxPaneHost.close")(function* (surfaceId: string) {
      return yield* layout.withPermit(
        Effect.gen(function* () {
          yield* closeSurface(surfaceId);
          subagentSurfaceIds.delete(surfaceId);
        }),
      );
    });

    const start = Effect.fn("SubagentHost.start")(
      function* (subagentId: SubagentId, command: SubagentCommand) {
        yield* Effect.annotateCurrentSpan({ subagentId, host: "cmux-pane" });

        const listener = yield* Protocol.listen(subagentId).pipe(
          Effect.provideService(SubagentLinkTransport, transport),
          Effect.mapError((error) =>
            SubagentHostStartError.make({
              subagentId,
              host: "cmux-pane",
              reason: error.reason,
            }),
          ),
        );

        yield* Effect.acquireRelease(create(subagentId, command), (pane) =>
          close(pane.surface_id).pipe(
            Effect.timeout(closeTimeout),
            Effect.catch((error) =>
              Effect.logWarning("Failed to close CMUX subagent pane", error).pipe(
                Effect.annotateLogs({ subagentId, host: "cmux-pane" }),
              ),
            ),
          ),
        );

        return yield* listener.accept;
      },
      (effect, subagentId) =>
        effect.pipe(
          Effect.timeoutOrElse({
            duration: "30 seconds",
            orElse: () =>
              SubagentHostStartError.make({
                subagentId,
                host: "cmux-pane",
                reason: "Subagent did not establish a link connection within 30 seconds",
              }),
          }),
        ),
    );

    return { start };
  });

export const layer = (root: { readonly workspaceId: string; readonly surfaceId: string }) =>
  Layer.effect(SubagentHost, make(root));

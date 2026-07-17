import { Config, Duration, Effect, Layer, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { isPositiveFiniteDuration } from "../../lib/schema.ts";
import type { SubagentId } from "../../subagent/SubagentId.ts";
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

const config = Config.schema(
  Schema.DurationFromString.check(isPositiveFiniteDuration()),
  "SMITH_CMUX_PANE_CLOSE_TIMEOUT",
).pipe(Config.withDefault(Duration.seconds(20)));

const make = (root: { readonly workspaceId: string; readonly surfaceId: string }) =>
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const closeTimeout = yield* config;

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

    const create = Effect.fn("CmuxPaneHost.create")(function* (
      subagentId: SubagentId,
      command: SubagentCommand,
      workspaceId: string,
      surfaceId: string,
    ) {
      const initialCommand = [command.executable, ...command.args]
        .map((argument) => `'${argument.replaceAll("'", `'"'"'`)}'`)
        .join(" ");
      const result = yield* rpc("pane.create", {
        workspace_id: workspaceId,
        surface_id: surfaceId,
        direction: "right",
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

      const response = yield* decodeCmuxPaneCreateResponse(result.stdout).pipe(
        Effect.mapError((error) =>
          SubagentHostResponseError.make({
            subagentId,
            host: "cmux-pane",
            reason: String(error),
          }),
        ),
      );

      return response;
    });

    const close = Effect.fn("CmuxPaneHost.close")(function* (
      workspaceId: string,
      surfaceId: string,
    ) {
      const result = yield* rpc("surface.close", {
        workspace_id: workspaceId,
        surface_id: surfaceId,
      }).pipe(Effect.scoped);

      if (result.exitCode !== 0) {
        return yield* Effect.fail(result.stderr.trim());
      }

      return yield* Effect.void;
    });

    const start = Effect.fn("SubagentHost.start")(function* (
      subagentId: SubagentId,
      command: SubagentCommand,
    ) {
      yield* Effect.annotateCurrentSpan({ subagentId, host: "cmux-pane" });

      yield* Effect.acquireRelease(
        create(subagentId, command, root.workspaceId, root.surfaceId),
        (pane) =>
          close(root.workspaceId, pane.surface_id).pipe(
            Effect.timeout(closeTimeout),
            Effect.catch((error) =>
              Effect.logWarning("Failed to close CMUX subagent pane", error).pipe(
                Effect.annotateLogs({ subagentId, host: "cmux-pane" }),
              ),
            ),
          ),
      );

      return yield* Effect.void;
    });

    return { start };
  });

export const layer = (root: { readonly workspaceId: string; readonly surfaceId: string }) =>
  Layer.effect(SubagentHost, make(root));

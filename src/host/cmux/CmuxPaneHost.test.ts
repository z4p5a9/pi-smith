import { expect, it } from "@effect/vitest";
import { Effect, Fiber, Layer, PlatformError, Schema } from "effect";
import { TestClock } from "effect/testing";

import { TestChildProcessSpawner } from "../../testing/TestChildProcessSpawner.ts";
import { decodeSubagentId } from "../../subagent/SubagentId.ts";
import * as CmuxPaneHost from "./CmuxPaneHost.ts";
import {
  SubagentHost,
  SubagentHostResponseError,
  SubagentHostStartError,
  SubagentHostUnavailableError,
} from "../Host.ts";

const encodeJson = Schema.encodeSync(Schema.UnknownFromJsonString);

it.describe("CmuxPaneHost", () => {
  it.effect("creates and closes an exact CMUX pane", () => {
    const workspaceId = "11111111-1111-4111-8111-111111111111";
    const rootSurfaceId = "22222222-2222-4222-8222-222222222222";
    const childSurfaceId = "44444444-4444-4444-8444-444444444444";

    return Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* childProcesses.stub([
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            surface_id: childSurfaceId,
          }),
        },
        { exitCode: Effect.succeed(0) },
      ]);

      yield* host
        .start(subagentId, {
          executable: "/opt/pi",
          args: ["--title", "Review's API", ""],
          cwd: "/worktree",
          env: { SMITH_CHILD_CONFIG: "/tmp/smith.json" },
        })
        .pipe(Effect.scoped);

      expect(yield* childProcesses.calls).toMatchObject([
        {
          command: "cmux",
          args: [
            "rpc",
            "pane.create",
            encodeJson({
              workspace_id: workspaceId,
              surface_id: rootSurfaceId,
              direction: "right",
              type: "terminal",
              focus: false,
              initial_command: `'/opt/pi' '--title' 'Review'"'"'s API' ''`,
              working_directory: "/worktree",
              startup_environment: { SMITH_CHILD_CONFIG: "/tmp/smith.json" },
            }),
          ],
        },
        {
          command: "cmux",
          args: [
            "rpc",
            "surface.close",
            encodeJson({
              workspace_id: workspaceId,
              surface_id: childSurfaceId,
            }),
          ],
        },
      ]);
      yield* childProcesses.verify;
    }).pipe(
      Effect.provide(
        CmuxPaneHost.layer({ workspaceId, surfaceId: rootSurfaceId }).pipe(
          Layer.provideMerge(TestChildProcessSpawner.layer),
        ),
      ),
    );
  });

  it.effect("reports an unavailable CMUX process", () =>
    Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* childProcesses.stub([
        {
          error: PlatformError.systemError({
            _tag: "NotFound",
            module: "ChildProcess",
            method: "spawn",
            pathOrDescriptor: "cmux",
          }),
        },
      ]);

      const error = yield* host
        .start(subagentId, { executable: "pi", args: [] })
        .pipe(Effect.scoped, Effect.flip);

      expect(Schema.is(SubagentHostUnavailableError)(error)).toBe(true);
      yield* childProcesses.verify;
    }).pipe(
      Effect.provide(
        CmuxPaneHost.layer({
          workspaceId: "11111111-1111-4111-8111-111111111111",
          surfaceId: "22222222-2222-4222-8222-222222222222",
        }).pipe(Layer.provideMerge(TestChildProcessSpawner.layer)),
      ),
    ),
  );

  it.effect("reports a failed pane creation", () =>
    Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* childProcesses.stub([
        {
          exitCode: Effect.succeed(1),
          stderr: "method_not_found: Unknown method",
        },
      ]);

      const error = yield* host
        .start(subagentId, { executable: "pi", args: [] })
        .pipe(Effect.scoped, Effect.flip);

      expect(Schema.is(SubagentHostStartError)(error)).toBe(true);
      expect(error).toMatchObject({
        reason: "method_not_found: Unknown method",
      });
      yield* childProcesses.verify;
    }).pipe(
      Effect.provide(
        CmuxPaneHost.layer({
          workspaceId: "11111111-1111-4111-8111-111111111111",
          surfaceId: "22222222-2222-4222-8222-222222222222",
        }).pipe(Layer.provideMerge(TestChildProcessSpawner.layer)),
      ),
    ),
  );

  it.effect("reports a malformed pane response", () =>
    Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* childProcesses.stub([{ exitCode: Effect.succeed(0), stdout: "{}" }]);

      const error = yield* host
        .start(subagentId, { executable: "pi", args: [] })
        .pipe(Effect.scoped, Effect.flip);

      expect(Schema.is(SubagentHostResponseError)(error)).toBe(true);
      yield* childProcesses.verify;
    }).pipe(
      Effect.provide(
        CmuxPaneHost.layer({
          workspaceId: "11111111-1111-4111-8111-111111111111",
          surfaceId: "22222222-2222-4222-8222-222222222222",
        }).pipe(Layer.provideMerge(TestChildProcessSpawner.layer)),
      ),
    ),
  );

  it.effect("does not fail when pane cleanup fails", () => {
    const workspaceId = "11111111-1111-4111-8111-111111111111";
    const childSurfaceId = "44444444-4444-4444-8444-444444444444";

    return Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* childProcesses.stub([
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            surface_id: childSurfaceId,
          }),
        },
        { exitCode: Effect.succeed(1), stderr: "surface not found" },
      ]);

      yield* host.start(subagentId, { executable: "pi", args: [] }).pipe(Effect.scoped);

      yield* childProcesses.verify;
    }).pipe(
      Effect.provide(
        CmuxPaneHost.layer({
          workspaceId,
          surfaceId: "22222222-2222-4222-8222-222222222222",
        }).pipe(Layer.provideMerge(TestChildProcessSpawner.layer)),
      ),
    );
  });

  it.effect("stops waiting when pane cleanup times out", () => {
    const workspaceId = "11111111-1111-4111-8111-111111111111";
    const childSurfaceId = "44444444-4444-4444-8444-444444444444";

    return Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* childProcesses.stub([
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({ surface_id: childSurfaceId }),
        },
        { exitCode: Effect.never },
      ]);

      const start = yield* host
        .start(subagentId, { executable: "pi", args: [] })
        .pipe(Effect.scoped, Effect.forkChild({ startImmediately: true }));

      yield* TestClock.adjust("20 seconds");

      yield* Fiber.join(start);
      yield* childProcesses.verify;
    }).pipe(
      Effect.provide(
        CmuxPaneHost.layer({
          workspaceId,
          surfaceId: "22222222-2222-4222-8222-222222222222",
        }).pipe(Layer.provideMerge(TestChildProcessSpawner.layer)),
      ),
    );
  });
});

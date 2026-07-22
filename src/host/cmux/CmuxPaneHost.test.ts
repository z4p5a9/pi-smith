import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, PlatformError, Schema, Scope } from "effect";
import { TestClock } from "effect/testing";

import { TestChildProcessSpawner } from "../../testing/TestChildProcessSpawner.ts";
import { decodeSubagentId } from "../../subagent/SubagentId.ts";
import * as CmuxPaneHost from "./CmuxPaneHost.ts";
import * as Protocol from "../Protocol.ts";
import * as UnixSocketTransport from "../link/unix/UnixSocketTransport.ts";
import {
  SubagentHost,
  SubagentHostResponseError,
  SubagentHostStartError,
  SubagentHostUnavailableError,
} from "../Host.ts";

const encodeJson = Schema.encodeSync(Schema.UnknownFromJsonString);
const decodeJson = Schema.decodeSync(Schema.UnknownFromJsonString);

it.describe("CmuxPaneHost", () => {
  it.effect("creates an exact CMUX pane, delivers child events, and closes it", () => {
    const workspaceId = "11111111-1111-4111-8111-111111111111";
    const rootSurfaceId = "22222222-2222-4222-8222-222222222222";
    const childSurfaceId = "44444444-4444-4444-8444-444444444444";

    return Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_cmux-create");
      const parentScope = yield* Scope.Scope;
      const hostScope = yield* Scope.fork(parentScope);

      yield* childProcesses.stub([
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            panes: [
              {
                surface_ids: [rootSurfaceId],
                pixel_frame: { x: 0, y: 0, width: 2000, height: 1200 },
              },
            ],
          }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            surface_id: childSurfaceId,
          }),
        },
        { exitCode: Effect.succeed(0) },
      ]);

      const starting = yield* host
        .start(subagentId, {
          executable: "/opt/pi",
          args: ["--title", "Review's API", ""],
          cwd: "/worktree",
          env: { SMITH_CHILD_CONFIG: "/tmp/smith.json" },
        })
        .pipe(Scope.provide(hostScope), Effect.forkChild({ startImmediately: true }));

      yield* Effect.suspend(() =>
        childProcesses.calls.pipe(
          Effect.flatMap((calls) => (calls.length > 0 ? Effect.void : Effect.fail("No pane yet"))),
        ),
      ).pipe(Effect.eventually);

      const child = yield* Protocol.connect(subagentId);
      const session = yield* Fiber.join(starting);

      const sending = yield* child
        .send({ kind: "message", content: "Task complete." })
        .pipe(Effect.forkChild({ startImmediately: true }));

      const event = yield* session.take;

      yield* Fiber.join(sending);

      expect(event).toEqual({ kind: "message", content: "Task complete." });

      yield* Scope.close(hostScope, Exit.void);

      expect(yield* childProcesses.calls).toMatchObject([
        {
          command: "cmux",
          args: ["rpc", "pane.list", encodeJson({ workspace_id: workspaceId })],
        },
        {
          command: "cmux",
          args: [
            "rpc",
            "pane.create",
            encodeJson({
              workspace_id: workspaceId,
              surface_id: rootSurfaceId,
              direction: "right",
              initial_divider_position: 0.5,
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
      Effect.scoped,
      Effect.provide(
        CmuxPaneHost.layer({ workspaceId, surfaceId: rootSurfaceId }).pipe(
          Layer.provideMerge(TestChildProcessSpawner.layer),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    );
  });

  it.effect("splits the largest subagent pane along its longest dimension", () => {
    const workspaceId = "11111111-1111-4111-8111-111111111111";
    const rootSurfaceId = "22222222-2222-4222-8222-222222222222";
    const childSurfaceIds = [
      "33333333-3333-4333-8333-333333333333",
      "44444444-4444-4444-8444-444444444444",
      "55555555-5555-4555-8555-555555555555",
      "66666666-6666-4666-8666-666666666666",
    ];

    return Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const parentScope = yield* Scope.Scope;
      const hostScopes: Array<Scope.Closeable> = [];
      const subagentIds = yield* Effect.forEach(["first", "second", "third", "fourth"], (name) =>
        decodeSubagentId(`sa_12345678_cmux-${name}`),
      );

      yield* childProcesses.stub([
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            panes: [
              {
                surface_ids: [rootSurfaceId],
                pixel_frame: { x: 0, y: 0, width: 2000, height: 1200 },
              },
            ],
          }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({ surface_id: childSurfaceIds[0] }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            panes: [
              {
                surface_ids: [rootSurfaceId],
                pixel_frame: { x: 0, y: 0, width: 1000, height: 1200 },
              },
              {
                surface_ids: [childSurfaceIds[0]],
                pixel_frame: { x: 1000, y: 0, width: 1000, height: 1200 },
              },
            ],
          }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({ surface_id: childSurfaceIds[1] }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            panes: [
              {
                surface_ids: [rootSurfaceId],
                pixel_frame: { x: 0, y: 0, width: 1000, height: 1200 },
              },
              {
                surface_ids: [childSurfaceIds[0]],
                pixel_frame: { x: 1000, y: 0, width: 1000, height: 600 },
              },
              {
                surface_ids: [childSurfaceIds[1]],
                pixel_frame: { x: 1000, y: 600, width: 1000, height: 600 },
              },
            ],
          }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({ surface_id: childSurfaceIds[2] }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            panes: [
              {
                surface_ids: [rootSurfaceId],
                pixel_frame: { x: 0, y: 0, width: 1000, height: 1200 },
              },
              {
                surface_ids: [childSurfaceIds[0]],
                pixel_frame: { x: 1000, y: 0, width: 500, height: 600 },
              },
              {
                surface_ids: [childSurfaceIds[2]],
                pixel_frame: { x: 1500, y: 0, width: 500, height: 600 },
              },
              {
                surface_ids: [childSurfaceIds[1]],
                pixel_frame: { x: 1000, y: 600, width: 1000, height: 600 },
              },
            ],
          }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({ surface_id: childSurfaceIds[3] }),
        },
        { exitCode: Effect.succeed(0) },
        { exitCode: Effect.succeed(0) },
        { exitCode: Effect.succeed(0) },
        { exitCode: Effect.succeed(0) },
      ]);

      for (let index = 0; index < subagentIds.length; index++) {
        const subagentId = subagentIds[index];

        if (subagentId === undefined) {
          return yield* Effect.die("Missing test subagent ID");
        }

        const hostScope = yield* Scope.fork(parentScope);
        hostScopes.push(hostScope);
        const starting = yield* host
          .start(subagentId, { executable: "pi", args: [] })
          .pipe(Scope.provide(hostScope), Effect.forkChild({ startImmediately: true }));

        yield* Effect.suspend(() =>
          childProcesses.calls.pipe(
            Effect.flatMap((calls) =>
              calls.length >= (index + 1) * 2 ? Effect.void : Effect.fail("No pane yet"),
            ),
          ),
        ).pipe(Effect.eventually);

        yield* Protocol.connect(subagentId);
        yield* Fiber.join(starting);
      }

      const calls = yield* childProcesses.calls;
      const creations: Array<unknown> = [];

      for (const call of calls) {
        if (!("args" in call) || call.args[1] !== "pane.create") {
          continue;
        }

        creations.push(decodeJson(call.args[2] ?? ""));
      }

      expect(creations).toMatchObject([
        {
          surface_id: rootSurfaceId,
          direction: "right",
          initial_divider_position: 0.5,
        },
        {
          surface_id: childSurfaceIds[0],
          direction: "down",
          initial_divider_position: 0.5,
        },
        {
          surface_id: childSurfaceIds[0],
          direction: "right",
          initial_divider_position: 0.5,
        },
        {
          surface_id: childSurfaceIds[1],
          direction: "right",
          initial_divider_position: 0.5,
        },
      ]);

      for (const hostScope of hostScopes) {
        yield* Scope.close(hostScope, Exit.void);
      }

      yield* childProcesses.verify;
      return yield* Effect.void;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        CmuxPaneHost.layer({ workspaceId, surfaceId: rootSurfaceId }).pipe(
          Layer.provideMerge(TestChildProcessSpawner.layer),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    );
  });

  it.effect("rejects a workspace containing panes not owned by Smith", () => {
    const workspaceId = "11111111-1111-4111-8111-111111111111";
    const rootSurfaceId = "22222222-2222-4222-8222-222222222222";

    return Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_cmux-foreign-pane");

      yield* childProcesses.stub([
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            panes: [
              {
                surface_ids: [rootSurfaceId],
                pixel_frame: { x: 0, y: 0, width: 1000, height: 1200 },
              },
              {
                surface_ids: ["77777777-7777-4777-8777-777777777777"],
                pixel_frame: { x: 1000, y: 0, width: 1000, height: 1200 },
              },
            ],
          }),
        },
      ]);

      const error = yield* host
        .start(subagentId, { executable: "pi", args: [] })
        .pipe(Effect.scoped, Effect.flip);

      expect(error).toMatchObject({
        reason: "CMUX workspace contains panes not owned by Smith",
      });
      yield* childProcesses.verify;
    }).pipe(
      Effect.provide(
        CmuxPaneHost.layer({ workspaceId, surfaceId: rootSurfaceId }).pipe(
          Layer.provideMerge(TestChildProcessSpawner.layer),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    );
  });

  it.effect("serializes concurrent pane creation", () => {
    const workspaceId = "11111111-1111-4111-8111-111111111111";
    const rootSurfaceId = "22222222-2222-4222-8222-222222222222";
    const firstSurfaceId = "33333333-3333-4333-8333-333333333333";
    const secondSurfaceId = "44444444-4444-4444-8444-444444444444";

    return Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const parentScope = yield* Scope.Scope;
      const firstScope = yield* Scope.fork(parentScope);
      const secondScope = yield* Scope.fork(parentScope);
      const firstListExitCode = yield* Deferred.make<number>();
      const firstSubagentId = yield* decodeSubagentId("sa_12345678_cmux-concurrent-first");
      const secondSubagentId = yield* decodeSubagentId("sa_12345678_cmux-concurrent-second");

      yield* childProcesses.stub([
        {
          exitCode: Deferred.await(firstListExitCode),
          stdout: encodeJson({
            panes: [
              {
                surface_ids: [rootSurfaceId],
                pixel_frame: { x: 0, y: 0, width: 2000, height: 1200 },
              },
            ],
          }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({ surface_id: firstSurfaceId }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            panes: [
              {
                surface_ids: [rootSurfaceId],
                pixel_frame: { x: 0, y: 0, width: 1000, height: 1200 },
              },
              {
                surface_ids: [firstSurfaceId],
                pixel_frame: { x: 1000, y: 0, width: 1000, height: 1200 },
              },
            ],
          }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({ surface_id: secondSurfaceId }),
        },
        { exitCode: Effect.succeed(0) },
        { exitCode: Effect.succeed(0) },
      ]);

      const first = yield* host
        .start(firstSubagentId, { executable: "pi", args: [] })
        .pipe(Scope.provide(firstScope), Effect.forkChild({ startImmediately: true }));

      yield* Effect.suspend(() =>
        childProcesses.calls.pipe(
          Effect.flatMap((calls) =>
            calls.length === 1 ? Effect.void : Effect.fail("No list yet"),
          ),
        ),
      ).pipe(Effect.eventually);

      const second = yield* host
        .start(secondSubagentId, { executable: "pi", args: [] })
        .pipe(Scope.provide(secondScope), Effect.forkChild({ startImmediately: true }));

      expect(yield* childProcesses.calls).toHaveLength(1);
      yield* Deferred.succeed(firstListExitCode, 0);

      yield* Effect.suspend(() =>
        childProcesses.calls.pipe(
          Effect.flatMap((calls) =>
            calls.length === 4 ? Effect.void : Effect.fail("Panes not created"),
          ),
        ),
      ).pipe(Effect.eventually);

      yield* Protocol.connect(firstSubagentId);
      yield* Protocol.connect(secondSubagentId);
      yield* Fiber.join(first);
      yield* Fiber.join(second);

      const methods: Array<string | undefined> = [];

      for (const call of yield* childProcesses.calls) {
        if ("args" in call) {
          methods.push(call.args[1]);
        }
      }

      expect(methods).toEqual(["pane.list", "pane.create", "pane.list", "pane.create"]);

      yield* Scope.close(firstScope, Exit.void);
      yield* Scope.close(secondScope, Exit.void);
      yield* childProcesses.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        CmuxPaneHost.layer({ workspaceId, surfaceId: rootSurfaceId }).pipe(
          Layer.provideMerge(TestChildProcessSpawner.layer),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    );
  });

  it.effect("reports an unavailable CMUX process", () =>
    Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_cmux-unavailable");

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
        }).pipe(
          Layer.provideMerge(TestChildProcessSpawner.layer),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("reports a failed pane creation", () =>
    Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_cmux-failed");

      yield* childProcesses.stub([
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            panes: [
              {
                surface_ids: ["22222222-2222-4222-8222-222222222222"],
                pixel_frame: { x: 0, y: 0, width: 2000, height: 1200 },
              },
            ],
          }),
        },
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
        }).pipe(
          Layer.provideMerge(TestChildProcessSpawner.layer),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("reports a malformed pane response", () =>
    Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_cmux-malformed");

      yield* childProcesses.stub([
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            panes: [
              {
                surface_ids: ["22222222-2222-4222-8222-222222222222"],
                pixel_frame: { x: 0, y: 0, width: 2000, height: 1200 },
              },
            ],
          }),
        },
        { exitCode: Effect.succeed(0), stdout: "{}" },
      ]);

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
        }).pipe(
          Layer.provideMerge(TestChildProcessSpawner.layer),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("times out when no subagent connects and closes the pane", () => {
    const workspaceId = "11111111-1111-4111-8111-111111111111";
    const childSurfaceId = "44444444-4444-4444-8444-444444444444";

    return Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_cmux-start-timeout");

      yield* childProcesses.stub([
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            panes: [
              {
                surface_ids: ["22222222-2222-4222-8222-222222222222"],
                pixel_frame: { x: 0, y: 0, width: 2000, height: 1200 },
              },
            ],
          }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({ surface_id: childSurfaceId }),
        },
        { exitCode: Effect.succeed(0) },
      ]);

      const starting = yield* host
        .start(subagentId, { executable: "pi", args: [] })
        .pipe(Effect.scoped, Effect.flip, Effect.forkChild({ startImmediately: true }));

      yield* Effect.suspend(() =>
        childProcesses.calls.pipe(
          Effect.flatMap((calls) => (calls.length > 0 ? Effect.void : Effect.fail("No pane yet"))),
        ),
      ).pipe(Effect.eventually);
      yield* TestClock.adjust("30 seconds");

      const error = yield* Fiber.join(starting);

      expect(Schema.is(SubagentHostStartError)(error)).toBe(true);
      expect(error).toMatchObject({
        reason: "Subagent did not establish a link connection within 30 seconds",
      });
      yield* childProcesses.verify;
    }).pipe(
      Effect.provide(
        CmuxPaneHost.layer({
          workspaceId,
          surfaceId: "22222222-2222-4222-8222-222222222222",
        }).pipe(
          Layer.provideMerge(TestChildProcessSpawner.layer),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    );
  });

  it.effect("does not fail when pane cleanup fails", () => {
    const workspaceId = "11111111-1111-4111-8111-111111111111";
    const childSurfaceId = "44444444-4444-4444-8444-444444444444";

    return Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_cmux-cleanup");
      const parentScope = yield* Scope.Scope;
      const hostScope = yield* Scope.fork(parentScope);

      yield* childProcesses.stub([
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            panes: [
              {
                surface_ids: ["22222222-2222-4222-8222-222222222222"],
                pixel_frame: { x: 0, y: 0, width: 2000, height: 1200 },
              },
            ],
          }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            surface_id: childSurfaceId,
          }),
        },
        { exitCode: Effect.succeed(1), stderr: "surface not found" },
      ]);

      const starting = yield* host
        .start(subagentId, { executable: "pi", args: [] })
        .pipe(Scope.provide(hostScope), Effect.forkChild({ startImmediately: true }));

      yield* Effect.suspend(() =>
        childProcesses.calls.pipe(
          Effect.flatMap((calls) => (calls.length > 0 ? Effect.void : Effect.fail("No pane yet"))),
        ),
      ).pipe(Effect.eventually);

      yield* Protocol.connect(subagentId);
      yield* Fiber.join(starting);
      yield* Scope.close(hostScope, Exit.void);

      yield* childProcesses.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        CmuxPaneHost.layer({
          workspaceId,
          surfaceId: "22222222-2222-4222-8222-222222222222",
        }).pipe(
          Layer.provideMerge(TestChildProcessSpawner.layer),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    );
  });

  it.effect("stops waiting when pane cleanup times out", () => {
    const workspaceId = "11111111-1111-4111-8111-111111111111";
    const childSurfaceId = "44444444-4444-4444-8444-444444444444";

    return Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_cmux-close-timeout");
      const parentScope = yield* Scope.Scope;
      const hostScope = yield* Scope.fork(parentScope);

      yield* childProcesses.stub([
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({
            panes: [
              {
                surface_ids: ["22222222-2222-4222-8222-222222222222"],
                pixel_frame: { x: 0, y: 0, width: 2000, height: 1200 },
              },
            ],
          }),
        },
        {
          exitCode: Effect.succeed(0),
          stdout: encodeJson({ surface_id: childSurfaceId }),
        },
        { exitCode: Effect.never },
      ]);

      const starting = yield* host
        .start(subagentId, { executable: "pi", args: [] })
        .pipe(Scope.provide(hostScope), Effect.forkChild({ startImmediately: true }));

      yield* Effect.suspend(() =>
        childProcesses.calls.pipe(
          Effect.flatMap((calls) => (calls.length > 0 ? Effect.void : Effect.fail("No pane yet"))),
        ),
      ).pipe(Effect.eventually);

      yield* Protocol.connect(subagentId);
      yield* Fiber.join(starting);

      const closing = yield* Scope.close(hostScope, Exit.void).pipe(
        Effect.forkChild({ startImmediately: true }),
      );

      yield* TestClock.adjust("20 seconds");
      yield* Fiber.join(closing);
      yield* childProcesses.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        CmuxPaneHost.layer({
          workspaceId,
          surfaceId: "22222222-2222-4222-8222-222222222222",
        }).pipe(
          Layer.provideMerge(TestChildProcessSpawner.layer),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    );
  });
});

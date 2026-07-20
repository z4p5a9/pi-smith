import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Layer, Scope } from "effect";

import * as Protocol from "../host/Protocol.ts";
import * as UnixSocketTransport from "../host/link/unix/UnixSocketTransport.ts";
import { SubagentHost, SubagentHostUnavailableError } from "../host/Host.ts";
import { decodeSubagentId } from "../subagent/SubagentId.ts";
import { TestHost } from "./TestHost.ts";

it.describe("TestHost", () => {
  it.effect("stubs and records a scoped host start", () =>
    Effect.gen(function* () {
      const testHost = yield* TestHost;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_host-start");
      const parentScope = yield* Scope.Scope;
      const hostScope = yield* Scope.fork(parentScope);

      yield* testHost.stub([null]);

      const starting = yield* host
        .start(subagentId, {
          executable: "pi",
          args: ["--name", "Review API"],
          cwd: "/worktree",
          env: { SMITH_SUBAGENT_ID: subagentId },
        })
        .pipe(Scope.provide(hostScope), Effect.forkChild({ startImmediately: true }));

      expect(yield* testHost.takeStart).toBe(subagentId);
      expect(yield* testHost.active).toEqual([subagentId]);

      yield* Protocol.connect(subagentId);
      yield* Fiber.join(starting);
      yield* Scope.close(hostScope, Exit.void);

      expect(yield* testHost.calls).toEqual([
        {
          subagentId,
          command: {
            executable: "pi",
            args: ["--name", "Review API"],
            cwd: "/worktree",
            env: { SMITH_SUBAGENT_ID: subagentId },
          },
        },
      ]);
      expect(yield* testHost.active).toEqual([]);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        TestHost.layer.pipe(
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("stubs a host start failure without connecting", () =>
    Effect.gen(function* () {
      const testHost = yield* TestHost;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_host-failure");

      yield* testHost.stub([
        SubagentHostUnavailableError.make({
          subagentId,
          host: "test",
          reason: "Host unavailable",
        }),
      ]);

      const error = yield* host
        .start(subagentId, { executable: "pi", args: [] })
        .pipe(Effect.scoped, Effect.flip);

      expect(error).toBeInstanceOf(SubagentHostUnavailableError);
      expect(yield* testHost.active).toEqual([]);
      yield* testHost.verify;
    }).pipe(
      Effect.provide(
        TestHost.layer.pipe(
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );
});

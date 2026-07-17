import { expect, it } from "@effect/vitest";
import { Effect } from "effect";

import { SubagentHost, SubagentHostUnavailableError } from "../host/Host.ts";
import { decodeSubagentId } from "../subagent/SubagentId.ts";
import { TestHost } from "./TestHost.ts";

it.describe("TestHost", () => {
  it.effect("stubs and records a scoped host start", () =>
    Effect.gen(function* () {
      const testHost = yield* TestHost;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_host-start");

      yield* testHost.stub([null]);

      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* host.start(subagentId, {
            executable: "pi",
            args: ["--name", "Review API"],
            cwd: "/worktree",
            env: { SMITH_SUBAGENT_ID: subagentId },
          });

          expect(yield* testHost.active).toEqual([subagentId]);
          expect(yield* testHost.takeStart).toBe(subagentId);
        }),
      );

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
    }).pipe(Effect.provide(TestHost.layer)),
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
    }).pipe(Effect.provide(TestHost.layer)),
  );
});

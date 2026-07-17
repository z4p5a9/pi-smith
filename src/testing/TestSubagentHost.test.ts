import { expect, it } from "@effect/vitest";
import { Effect } from "effect";

import { SubagentHost, SubagentHostUnavailableError } from "../subagent/SubagentHost.ts";
import { decodeSubagentId } from "../subagent/SubagentId.ts";
import { TestSubagentHost } from "./TestSubagentHost.ts";

it.describe("TestSubagentHost", () => {
  it.effect("stubs and records a scoped host start", () =>
    Effect.gen(function* () {
      const testHost = yield* TestSubagentHost;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_host-start");

      yield* testHost.stub([{ hostId: "test-host" }]);

      yield* Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* host.start(
            subagentId,
            { title: "Review API", prompt: "Complete the task.", cwd: "/worktree" },
            {
              executable: "pi",
              args: ["--name", "Review API"],
              cwd: "/worktree",
              env: { SMITH_SUBAGENT_ID: subagentId },
            },
          );

          expect(handle).toEqual({ hostId: "test-host" });
          expect(yield* testHost.active).toEqual([{ hostId: "test-host" }]);
          expect(yield* testHost.takeStart).toBe(subagentId);
        }),
      );

      expect(yield* testHost.calls).toEqual([
        {
          subagentId,
          spec: { title: "Review API", prompt: "Complete the task.", cwd: "/worktree" },
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
    }).pipe(Effect.provide(TestSubagentHost.layer)),
  );

  it.effect("stubs a host start failure without connecting", () =>
    Effect.gen(function* () {
      const testHost = yield* TestSubagentHost;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_host-failure");

      yield* testHost.stub([
        {
          error: SubagentHostUnavailableError.make({
            subagentId,
            host: "cmux-pane",
            reason: "CMUX unavailable",
          }),
        },
      ]);

      const error = yield* host
        .start(
          subagentId,
          { title: "Review API", prompt: "Complete the task.", cwd: "/worktree" },
          { executable: "pi", args: [] },
        )
        .pipe(Effect.scoped, Effect.flip);

      expect(error).toBeInstanceOf(SubagentHostUnavailableError);
      expect(yield* testHost.active).toEqual([]);
      yield* testHost.verify;
    }).pipe(Effect.provide(TestSubagentHost.layer)),
  );
});

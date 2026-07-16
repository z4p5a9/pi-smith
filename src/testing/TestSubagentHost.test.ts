import { expect, it } from "@effect/vitest";
import { Effect, Layer, Stream } from "effect";

import { SubagentBridge } from "../subagent/SubagentBridge.ts";
import { SubagentHost, SubagentHostUnavailableError } from "../subagent/SubagentHost.ts";
import { decodeSubagentId } from "../subagent/SubagentId.ts";
import { TestSubagentBridge } from "./TestSubagentBridge.ts";
import { TestSubagentHost } from "./TestSubagentHost.ts";

it.describe("TestSubagentHost", () => {
  it.effect("stubs and records a scoped host start", () =>
    Effect.gen(function* () {
      const testHost = yield* TestSubagentHost;
      const testBridge = yield* TestSubagentBridge;
      const bridge = yield* SubagentBridge;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* testHost.stub([{ hostId: "test-host" }]);

      yield* Effect.scoped(
        Effect.gen(function* () {
          const listener = yield* bridge.listen(subagentId);
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

          const session = yield* listener.accept;
          const delivery = yield* session.events.pipe(
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          );

          yield* delivery.acknowledge;

          expect(handle).toEqual({ hostId: "test-host" });
          expect(yield* testHost.active).toEqual([{ hostId: "test-host" }]);
          expect(yield* testBridge.isConnected(subagentId)).toBe(true);
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
      expect(yield* testBridge.isConnected(subagentId)).toBe(false);
      yield* testHost.verify;
    }).pipe(
      Effect.provide(TestSubagentHost.layer.pipe(Layer.provideMerge(TestSubagentBridge.layer))),
    ),
  );

  it.effect("stubs a host start failure without connecting", () =>
    Effect.gen(function* () {
      const testHost = yield* TestSubagentHost;
      const testBridge = yield* TestSubagentBridge;
      const host = yield* SubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

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
      expect(yield* testBridge.calls).toEqual([]);
      expect(yield* testHost.active).toEqual([]);
      yield* testHost.verify;
    }).pipe(
      Effect.provide(TestSubagentHost.layer.pipe(Layer.provideMerge(TestSubagentBridge.layer))),
    ),
  );
});

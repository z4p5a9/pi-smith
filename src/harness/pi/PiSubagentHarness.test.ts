import { fileURLToPath } from "node:url";

import { expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

import { SubagentHarness, SubagentHarnessCommandError } from "../Harness.ts";
import { decodeSubagentId } from "../../subagent/SubagentId.ts";
import * as PiSubagentHarness from "./PiSubagentHarness.ts";

it.describe("PiSubagentHarness", () => {
  it.effect("constructs a Pi child command", () =>
    Effect.gen(function* () {
      const harness = yield* SubagentHarness;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const command = yield* harness.makeCommand(subagentId, {
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      expect(command).toEqual({
        executable: process.execPath,
        args: [
          process.argv[1],
          "--extension",
          fileURLToPath(new URL("../../extension/pi-subagent.ts", import.meta.url)),
          "--name",
          "Review API",
          "Complete the task.",
        ],
        cwd: "/worktree",
        env: { SMITH_SUBAGENT_ID: subagentId },
      });
    }).pipe(Effect.provide(PiSubagentHarness.layer)),
  );

  it.effect("rejects a missing Pi entrypoint", () => {
    const piEntrypoint = process.argv.splice(1, 1)[0];

    return Effect.gen(function* () {
      const harness = yield* SubagentHarness;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const error = yield* harness
        .makeCommand(subagentId, {
          title: "Review API",
          prompt: "Complete the task.",
          cwd: "/worktree",
          mode: "ephemeral",
        })
        .pipe(Effect.flip);

      expect(Schema.is(SubagentHarnessCommandError)(error)).toBe(true);
    }).pipe(
      Effect.ensuring(
        Effect.sync(() => {
          if (piEntrypoint !== undefined) {
            process.argv.splice(1, 0, piEntrypoint);
          }
        }),
      ),
      Effect.provide(PiSubagentHarness.layer),
    );
  });
});

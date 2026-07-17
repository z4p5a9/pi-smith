import { fileURLToPath } from "node:url";

import { expect, it } from "@effect/vitest";
import { Effect } from "effect";

import { decodeSubagentId } from "../../subagent/SubagentId.ts";
import { makeCommand, PiSubagentEntrypointUnavailableError } from "./command.ts";

it.describe("makeCommand", () => {
  it.effect("constructs a Pi child command", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const command = makeCommand(subagentId, {
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
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
    }),
  );

  it("rejects a missing Pi entrypoint", () => {
    const piEntrypoint = process.argv.splice(1, 1)[0];
    const subagentId = Effect.runSync(decodeSubagentId("sa_12345678_review-api"));

    try {
      expect(() =>
        makeCommand(subagentId, {
          title: "Review API",
          prompt: "Complete the task.",
          cwd: "/worktree",
        }),
      ).toThrow(PiSubagentEntrypointUnavailableError);
    } finally {
      if (piEntrypoint !== undefined) {
        process.argv.splice(1, 0, piEntrypoint);
      }
    }
  });
});

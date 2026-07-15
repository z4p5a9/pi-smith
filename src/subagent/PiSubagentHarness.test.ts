import { fileURLToPath } from "node:url";

import { expect, it } from "@effect/vitest";
import { Effect } from "effect";

import {
  makePiSubagentCommand,
  PiSubagentEntrypointUnavailableError,
} from "./PiSubagentHarness.ts";
import { decodeSubagentId } from "./SubagentId.ts";

it.describe("makePiSubagentCommand", () => {
  it.effect("constructs a Pi child command", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const command = yield* makePiSubagentCommand(subagentId, {
        title: "Review API",
        cwd: "/worktree",
      });

      expect(command).toEqual({
        executable: process.execPath,
        args: [
          process.argv[1],
          "--extension",
          fileURLToPath(new URL("../extension/PiSubagent.ts", import.meta.url)),
          "--name",
          "Review API",
        ],
        cwd: "/worktree",
        env: { SMITH_SUBAGENT_ID: subagentId },
      });
    }),
  );

  it.effect("rejects a missing Pi entrypoint", () =>
    Effect.gen(function* () {
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const error = yield* Effect.acquireUseRelease(
        Effect.sync(() => process.argv.splice(1, 1)[0]),
        () =>
          makePiSubagentCommand(subagentId, {
            title: "Review API",
            cwd: "/worktree",
          }).pipe(Effect.flip),
        (piEntrypoint) =>
          Effect.sync(() => {
            if (piEntrypoint !== undefined) {
              process.argv.splice(1, 0, piEntrypoint);
            }
          }),
      );

      expect(error).toBeInstanceOf(PiSubagentEntrypointUnavailableError);
    }),
  );
});

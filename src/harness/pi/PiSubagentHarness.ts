import { fileURLToPath } from "node:url";

import { Effect, Layer } from "effect";

import { SubagentHarness, SubagentHarnessCommandError } from "../Harness.ts";
import type { SubagentId } from "../../subagent/SubagentId.ts";
import type { SubagentSpec } from "../../subagent/SubagentSpec.ts";

const makeCommand = Effect.fn("PiSubagentHarness.makeCommand")(function* (
  subagentId: SubagentId,
  spec: SubagentSpec,
) {
  const piEntrypoint = process.argv[1];

  if (piEntrypoint === undefined) {
    return yield* SubagentHarnessCommandError.make({
      subagentId,
      harness: "pi",
      reason: "Pi entrypoint is unavailable",
    });
  }

  return {
    executable: process.execPath,
    args: [
      piEntrypoint,
      "--extension",
      fileURLToPath(new URL("./extension/index.ts", import.meta.url)),
      "--name",
      spec.title,
      spec.prompt,
    ],
    cwd: spec.cwd,
    env: { SMITH_SUBAGENT_ID: subagentId },
  };
});

export const layer = Layer.succeed(SubagentHarness, SubagentHarness.of({ makeCommand }));

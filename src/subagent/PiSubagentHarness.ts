import { fileURLToPath } from "node:url";

import { Effect, Schema } from "effect";

import type { SubagentCommand } from "./SubagentHost.ts";
import type { SubagentId } from "./SubagentId.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

export class PiSubagentEntrypointUnavailableError extends Schema.TaggedErrorClass<PiSubagentEntrypointUnavailableError>()(
  "PiSubagentEntrypointUnavailableError",
  {},
) {}

export const makePiSubagentCommand = Effect.fn("makePiSubagentCommand")(function* (
  subagentId: SubagentId,
  spec: SubagentSpec,
): Effect.fn.Return<SubagentCommand, PiSubagentEntrypointUnavailableError> {
  const piEntrypoint = process.argv[1];

  if (piEntrypoint === undefined) {
    return yield* PiSubagentEntrypointUnavailableError.make({});
  }

  return {
    executable: process.execPath,
    args: [
      piEntrypoint,
      "--extension",
      fileURLToPath(new URL("../extension/PiSubagent.ts", import.meta.url)),
      "--name",
      spec.title,
      spec.prompt,
    ],
    cwd: spec.cwd,
    env: { SMITH_SUBAGENT_ID: subagentId },
  };
});

import { fileURLToPath } from "node:url";

import { Schema } from "effect";

import type { SubagentCommand } from "../../subagent/SubagentHost.ts";
import type { SubagentId } from "../../subagent/SubagentId.ts";
import type { SubagentSpec } from "../../subagent/SubagentSpec.ts";

export class PiSubagentEntrypointUnavailableError extends Schema.TaggedErrorClass<PiSubagentEntrypointUnavailableError>()(
  "PiSubagentEntrypointUnavailableError",
  {},
) {}

export const makeCommand = (subagentId: SubagentId, spec: SubagentSpec): SubagentCommand => {
  const piEntrypoint = process.argv[1];

  if (piEntrypoint === undefined) {
    throw PiSubagentEntrypointUnavailableError.make({});
  }

  return {
    executable: process.execPath,
    args: [
      piEntrypoint,
      "--extension",
      fileURLToPath(new URL("../../extension/pi-subagent.ts", import.meta.url)),
      "--name",
      spec.title,
      spec.prompt,
    ],
    cwd: spec.cwd,
    env: { SMITH_SUBAGENT_ID: subagentId },
  };
};

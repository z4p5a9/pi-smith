import { Context, type Effect, Schema } from "effect";

import type { SubagentCommand } from "../host/Host.ts";
import { SubagentId } from "../subagent/SubagentId.ts";
import type { SubagentSpec } from "../subagent/SubagentSpec.ts";

export class SubagentHarnessCommandError extends Schema.TaggedErrorClass<SubagentHarnessCommandError>()(
  "SubagentHarnessCommandError",
  {
    subagentId: SubagentId,
    harness: Schema.String,
    reason: Schema.String,
  },
) {}

export class SubagentHarness extends Context.Service<
  SubagentHarness,
  {
    readonly makeCommand: (
      subagentId: SubagentId,
      spec: SubagentSpec,
    ) => Effect.Effect<SubagentCommand, SubagentHarnessCommandError>;
  }
>()("@smith/harness/Harness") {}

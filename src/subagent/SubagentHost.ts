import { Context, Schema, type Effect, type Scope } from "effect";

import { SubagentId } from "./SubagentId.ts";

export interface SubagentCommand {
  readonly executable: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd?: string;
  readonly env?: Readonly<Record<string, string>>;
}

export class SubagentHostUnavailableError extends Schema.TaggedErrorClass<SubagentHostUnavailableError>()(
  "SubagentHostUnavailableError",
  {
    subagentId: SubagentId,
    host: Schema.String,
    reason: Schema.String,
  },
) {}

export class SubagentHostStartError extends Schema.TaggedErrorClass<SubagentHostStartError>()(
  "SubagentHostStartError",
  {
    subagentId: SubagentId,
    host: Schema.String,
    reason: Schema.String,
  },
) {}

export class SubagentHostResponseError extends Schema.TaggedErrorClass<SubagentHostResponseError>()(
  "SubagentHostResponseError",
  {
    subagentId: SubagentId,
    host: Schema.String,
    reason: Schema.String,
  },
) {}

export class SubagentHost extends Context.Service<
  SubagentHost,
  {
    readonly start: (
      subagentId: SubagentId,
      command: SubagentCommand,
    ) => Effect.Effect<
      void,
      SubagentHostUnavailableError | SubagentHostStartError | SubagentHostResponseError,
      Scope.Scope
    >;
  }
>()("@smith/subagent/SubagentHost") {}

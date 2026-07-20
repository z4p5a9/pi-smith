import { Context, Schema, type Effect, type Scope } from "effect";

import type { LinkDisconnectedError, LinkProtocolError } from "./link/Link.ts";
import type { SubagentEvent } from "../subagent/SubagentEvent.ts";
import { SubagentId } from "../subagent/SubagentId.ts";

export interface SubagentCommand {
  readonly executable: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd?: string;
  readonly env?: Readonly<Record<string, string>>;
}

export interface SubagentHostSession {
  readonly take: Effect.Effect<SubagentEvent, LinkDisconnectedError | LinkProtocolError>;
  readonly send: (content: string) => Effect.Effect<void, LinkDisconnectedError>;
  readonly await: Effect.Effect<void, LinkDisconnectedError | LinkProtocolError>;
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
      SubagentHostSession,
      SubagentHostUnavailableError | SubagentHostStartError | SubagentHostResponseError,
      Scope.Scope
    >;
  }
>()("@smith/host/Host") {}

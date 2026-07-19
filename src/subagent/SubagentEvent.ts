import { Schema } from "effect";

import type { SubagentId } from "./SubagentId.ts";

export const SubagentEvent = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("message"),
    content: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("failure"),
    reason: Schema.String,
  }),
]);

export type SubagentEvent = typeof SubagentEvent.Type;

export interface SubagentEventEnvelope {
  readonly subagentId: SubagentId;
  readonly event: SubagentEvent;
}

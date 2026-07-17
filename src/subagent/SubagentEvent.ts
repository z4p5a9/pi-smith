import { Schema } from "effect";

export const SubagentEvent = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("completed"),
    report: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("failed"),
    reason: Schema.String,
  }),
]);

export type SubagentEvent = typeof SubagentEvent.Type;

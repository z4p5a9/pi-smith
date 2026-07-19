import { Schema } from "effect";

export const SubagentProcessResult = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("exited"),
  }),
  Schema.Struct({
    kind: Schema.Literal("failed"),
    reason: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("killed"),
  }),
]);

export type SubagentProcessResult = typeof SubagentProcessResult.Type;

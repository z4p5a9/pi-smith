import { Schema } from "effect";

export const SubagentEvent = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("ready"),
  }),
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

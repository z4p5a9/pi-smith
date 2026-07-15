import { Schema } from "effect";

export const SubagentSpec = Schema.Struct({
  title: Schema.String.check(Schema.isPattern(/\S/)),
  cwd: Schema.String,
});

export type SubagentSpec = typeof SubagentSpec.Type;

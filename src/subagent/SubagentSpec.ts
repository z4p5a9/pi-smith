import { Schema } from "effect";

export const SubagentSpec = Schema.Struct({
  title: Schema.String.check(Schema.isPattern(/\S/)),
});

export type SubagentSpec = typeof SubagentSpec.Type;

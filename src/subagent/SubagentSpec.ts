import { Schema } from "effect";

export const SubagentSpec = Schema.Struct({
  title: Schema.String.check(Schema.isPattern(/\S/)),
  prompt: Schema.String.check(Schema.isPattern(/\S/)),
  cwd: Schema.String,
  mode: Schema.Literals(["ephemeral", "persistent"]),
});

export type SubagentSpec = typeof SubagentSpec.Type;

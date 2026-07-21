import { Effect, Random, Schema } from "effect";

export const SubagentMessageId = Schema.String.check(Schema.isPattern(/^msg_[a-z0-9]{24}$/)).pipe(
  Schema.brand("SubagentMessageId"),
);

export type SubagentMessageId = typeof SubagentMessageId.Type;

export const generateSubagentMessageId = Effect.fn("generateSubagentMessageId")(function* () {
  const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz";
  let random = "";

  for (let index = 0; index < 24; index++) {
    const randomIndex = yield* Random.nextIntBetween(0, alphabet.length, { halfOpen: true });
    random += alphabet.charAt(randomIndex);
  }

  return SubagentMessageId.make(`msg_${random}`);
});

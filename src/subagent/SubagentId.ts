import { Effect, Random, Schema } from "effect";

export const SubagentId = Schema.String.check(
  Schema.isPattern(/^sa_[a-z0-9]{8}_[a-z0-9]+(?:-[a-z0-9]+)*$/),
).pipe(Schema.brand("SubagentId"));

export type SubagentId = typeof SubagentId.Type;

export const decodeSubagentId = Effect.fn("decodeSubagentId")(function* (value: unknown) {
  return yield* Schema.decodeUnknownEffect(SubagentId)(value);
});

export const generateSubagentId = Effect.fn("generateSubagentId")(function* (title: string) {
  const alphabet = "0123456789abcdefghijklmnopqrstuvwxyz";
  let random = "";

  for (let index = 0; index < 8; index++) {
    const randomIndex = yield* Random.nextIntBetween(0, alphabet.length, { halfOpen: true });
    random += alphabet.charAt(randomIndex);
  }

  const slug =
    title
      .normalize("NFKD")
      .toLowerCase()
      .replace(/\p{Mark}/gu, "")
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 48)
      .replace(/-+$/g, "") || "subagent";

  return SubagentId.make(`sa_${random}_${slug}`);
});

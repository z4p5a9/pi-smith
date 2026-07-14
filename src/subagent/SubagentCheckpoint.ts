import { Context, Effect, Layer, Ref, Schema } from "effect";

import { SubagentId } from "./SubagentId.ts";
import { SubagentSpec } from "./SubagentSpec.ts";

export const SubagentRecord = Schema.Struct({
  subagentId: SubagentId,
  status: Schema.Literal("queued"),
  ...SubagentSpec.fields,
});

export type SubagentRecord = typeof SubagentRecord.Type;

export class SubagentAlreadyExistsError extends Schema.TaggedErrorClass<SubagentAlreadyExistsError>()(
  "SubagentAlreadyExistsError",
  {
    subagentId: SubagentId,
  },
) {}

const make = Effect.gen(function* () {
  const records = yield* Ref.make(new Map<SubagentId, SubagentRecord>());

  const put = Effect.fn("SubagentCheckpoint.put")(function* (record: SubagentRecord) {
    const inserted = yield* Ref.modify(records, (currentRecords) => {
      if (currentRecords.has(record.subagentId)) {
        return [false, currentRecords];
      }

      const updatedRecords = new Map(currentRecords);
      updatedRecords.set(record.subagentId, record);

      return [true, updatedRecords];
    });

    if (!inserted) {
      return yield* SubagentAlreadyExistsError.make({ subagentId: record.subagentId });
    }

    return yield* Effect.void;
  });

  return { put };
});

export class SubagentCheckpoint extends Context.Service<SubagentCheckpoint>()(
  "@smith/subagent/SubagentCheckpoint",
  { make },
) {
  static readonly layer = Layer.effect(SubagentCheckpoint, SubagentCheckpoint.make);
}

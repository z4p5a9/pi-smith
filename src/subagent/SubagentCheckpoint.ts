import { Context, Effect, Layer, Ref, Schema } from "effect";

import { SubagentId } from "./SubagentId.ts";
import { SubagentSpec } from "./SubagentSpec.ts";

export const SubagentRecord = Schema.Struct({
  subagentId: SubagentId,
  status: Schema.Literals(["queued", "starting"]),
  ...SubagentSpec.fields,
});

export type SubagentRecord = typeof SubagentRecord.Type;

export class SubagentAlreadyExistsError extends Schema.TaggedErrorClass<SubagentAlreadyExistsError>()(
  "SubagentAlreadyExistsError",
  {
    subagentId: SubagentId,
  },
) {}

export class SubagentNotFoundError extends Schema.TaggedErrorClass<SubagentNotFoundError>()(
  "SubagentNotFoundError",
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

  const update = Effect.fn("SubagentCheckpoint.update")(function* (
    subagentId: SubagentId,
    fields: Partial<Omit<SubagentRecord, "subagentId">>,
  ) {
    const updated = yield* Ref.modify(records, (currentRecords) => {
      const record = currentRecords.get(subagentId);

      if (record === undefined) {
        return [false, currentRecords];
      }

      const updatedRecords = new Map(currentRecords);
      updatedRecords.set(subagentId, { ...record, ...fields });

      return [true, updatedRecords];
    });

    if (!updated) {
      return yield* SubagentNotFoundError.make({ subagentId });
    }

    return yield* Effect.void;
  });

  const get = Effect.fn("SubagentCheckpoint.get")(function* (subagentId: SubagentId) {
    const currentRecords = yield* Ref.get(records);
    const record = currentRecords.get(subagentId);

    if (record === undefined) {
      return yield* SubagentNotFoundError.make({ subagentId });
    }

    return record;
  });

  return { put, update, get };
});

export class SubagentCheckpoint extends Context.Service<SubagentCheckpoint>()(
  "@smith/subagent/SubagentCheckpoint",
  { make },
) {
  static readonly layer = Layer.effect(SubagentCheckpoint, SubagentCheckpoint.make);
}

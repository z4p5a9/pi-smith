import { Context, Effect, Layer, Schema, Stream, SubscriptionRef } from "effect";

import { SubagentEvent } from "./SubagentEvent.ts";
import { SubagentId } from "./SubagentId.ts";
import { SubagentSpec } from "./SubagentSpec.ts";

export const SubagentRecord = Schema.Struct({
  subagentId: SubagentId,
  status: Schema.Literals(["queued", "starting", "running", "completed", "failed"]),
  latestEvent: Schema.optional(SubagentEvent),
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
  const records = yield* SubscriptionRef.make(new Map<SubagentId, SubagentRecord>());

  const put = Effect.fn("SubagentCheckpoint.put")(function* (record: SubagentRecord) {
    yield* SubscriptionRef.modifyEffect(records, (prev) => {
      if (prev.has(record.subagentId)) {
        return SubagentAlreadyExistsError.make({ subagentId: record.subagentId });
      }

      const next = new Map(prev);
      next.set(record.subagentId, record);

      return Effect.succeed([undefined, next] as const);
    });
  });

  const update = Effect.fn("SubagentCheckpoint.update")(function* (
    subagentId: SubagentId,
    fields: Partial<Omit<SubagentRecord, "subagentId">>,
  ) {
    yield* SubscriptionRef.modifyEffect(records, (prev) => {
      const record = prev.get(subagentId);

      if (record === undefined) {
        return SubagentNotFoundError.make({ subagentId });
      }

      const next = new Map(prev);
      next.set(subagentId, { ...record, ...fields });

      return Effect.succeed([undefined, next] as const);
    });
  });

  const get = Effect.fn("SubagentCheckpoint.get")(function* (subagentId: SubagentId) {
    const ref = yield* SubscriptionRef.get(records);
    const record = ref.get(subagentId);

    if (record === undefined) {
      return yield* SubagentNotFoundError.make({ subagentId });
    }

    return record;
  });

  const changes = (subagentId: SubagentId) =>
    SubscriptionRef.changes(records).pipe(
      Stream.mapEffect((ref) => {
        const record = ref.get(subagentId);

        if (record === undefined) {
          return SubagentNotFoundError.make({ subagentId });
        }

        return Effect.succeed(record);
      }),
      Stream.changes,
    );

  return { put, update, get, changes };
});

export class SubagentCheckpoint extends Context.Service<SubagentCheckpoint>()(
  "@smith/subagent/SubagentCheckpoint",
  { make },
) {
  static readonly layer = Layer.effect(SubagentCheckpoint, SubagentCheckpoint.make);
}

import { Context, Effect, Layer, Queue } from "effect";

import type { SubagentId } from "./SubagentId.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

const make = Effect.gen(function* () {
  const queue = yield* Queue.unbounded<{
    readonly subagentId: SubagentId;
    readonly spec: SubagentSpec;
  }>();

  const worker = Effect.forever(
    Effect.gen(function* () {
      yield* Queue.take(queue);

      // oxlint-disable-next-line no-warning-comments
      // TODO: Replace with SubagentSupervisor execution.
      return yield* Effect.never;
    }),
  );

  for (let index = 0; index < 10; index++) {
    yield* Effect.forkScoped(worker, { startImmediately: true });
  }

  const submit = Effect.fn("SubagentPool.submit")(function* (
    subagentId: SubagentId,
    spec: SubagentSpec,
  ) {
    yield* Queue.offer(queue, { subagentId, spec });
  });

  return { submit };
});

export class SubagentPool extends Context.Service<SubagentPool>()("@smith/subagent/SubagentPool", {
  make,
}) {
  static readonly layer = Layer.effect(SubagentPool, SubagentPool.make);
}

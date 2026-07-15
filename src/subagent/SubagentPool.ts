import { Context, Effect, Layer, Queue } from "effect";

import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import type { SubagentId } from "./SubagentId.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";
import { SubagentSupervisor } from "./SubagentSupervisor.ts";

const make = Effect.gen(function* () {
  const checkpoint = yield* SubagentCheckpoint;
  const supervisor = yield* SubagentSupervisor;
  const queue = yield* Queue.unbounded<{
    readonly subagentId: SubagentId;
    readonly spec: SubagentSpec;
  }>();

  const worker = Effect.fn("SubagentPool.worker", { root: true })(
    function* (subagentId: SubagentId, spec: SubagentSpec) {
      yield* Effect.annotateCurrentSpan({ subagentId });

      yield* checkpoint.update(subagentId, { status: "starting" });
      const child = yield* supervisor.start(subagentId, spec);
      yield* checkpoint.update(subagentId, { status: "running" });
      yield* child.await;
    },
    Effect.catch(Effect.logError),
    (effect, subagentId) => Effect.annotateLogs(effect, { subagentId }),
  );

  for (let index = 0; index < 10; index++) {
    yield* Effect.forkScoped(
      Effect.forever(
        Queue.take(queue).pipe(Effect.flatMap(({ spec, subagentId }) => worker(subagentId, spec))),
      ),
      { startImmediately: true },
    );
  }

  const submit = Effect.fn("SubagentPool.submit")(function* (
    subagentId: SubagentId,
    spec: SubagentSpec,
  ) {
    yield* Effect.annotateCurrentSpan({ subagentId });
    yield* Queue.offer(queue, { subagentId, spec });
  });

  return { submit };
});

export class SubagentPool extends Context.Service<SubagentPool>()("@smith/subagent/SubagentPool", {
  make,
}) {
  static readonly layerNoDeps = Layer.effect(SubagentPool, SubagentPool.make);

  static readonly layer = SubagentPool.layerNoDeps.pipe(
    Layer.provideMerge(Layer.merge(SubagentCheckpoint.layer, SubagentSupervisor.layer)),
  );
}

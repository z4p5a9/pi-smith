import { Context, Effect, Fiber, FiberMap, Layer, Schema, Semaphore } from "effect";

import { SubagentId } from "./SubagentId.ts";
import { spawnSubagentProcess } from "./SubagentProcess.ts";
import { type SubagentAlreadyRegisteredError, SubagentRegistry } from "./SubagentRegistry.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

export class SubagentAlreadyStartedError extends Schema.TaggedErrorClass<SubagentAlreadyStartedError>()(
  "SubagentAlreadyStartedError",
  {
    subagentId: SubagentId,
  },
) {}

const make = Effect.gen(function* () {
  const registry = yield* SubagentRegistry;
  const children = yield* FiberMap.make<SubagentId, never, SubagentAlreadyRegisteredError>();
  const startLock = yield* Semaphore.make(1);

  const start = Effect.fn("SubagentSupervisor.start")(function* (
    subagentId: SubagentId,
    spec: SubagentSpec,
  ) {
    yield* Effect.annotateCurrentSpan({ subagentId });

    if (yield* FiberMap.has(children, subagentId)) {
      return yield* SubagentAlreadyStartedError.make({ subagentId });
    }

    const child = Effect.scoped(
      Effect.gen(function* () {
        const process = yield* spawnSubagentProcess(subagentId, spec);

        yield* Effect.acquireRelease(registry.register(subagentId, process), () =>
          registry.unregister(subagentId),
        );

        return yield* process.await;
      }),
    );

    const fiber = yield* FiberMap.run(
      children,
      subagentId,
      child.pipe(
        Effect.withSpan("SubagentSupervisor.child", {
          attributes: { subagentId },
        }),
        Effect.annotateLogs({ subagentId }),
      ),
      { startImmediately: true },
    );

    return { await: Fiber.await(fiber) };
  }, Semaphore.withPermit(startLock));

  return { start };
});

export class SubagentSupervisor extends Context.Service<SubagentSupervisor>()(
  "@smith/subagent/SubagentSupervisor",
  { make },
) {
  static readonly layerNoDeps = Layer.effect(SubagentSupervisor, SubagentSupervisor.make);

  static readonly layer = SubagentSupervisor.layerNoDeps.pipe(
    Layer.provideMerge(SubagentRegistry.layer),
  );
}

import { Context, Effect, Fiber, FiberMap, Layer, Schema, Semaphore } from "effect";

import { SubagentId } from "./SubagentId.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

export class SubagentAlreadyStartedError extends Schema.TaggedErrorClass<SubagentAlreadyStartedError>()(
  "SubagentAlreadyStartedError",
  {
    subagentId: SubagentId,
  },
) {}

const make = Effect.gen(function* () {
  const children = yield* FiberMap.make<SubagentId, never, never>();
  const startLock = yield* Semaphore.make(1);

  const start = Effect.fn("SubagentSupervisor.start")(function* (
    subagentId: SubagentId,
    _spec: SubagentSpec,
  ) {
    yield* Effect.annotateCurrentSpan({ subagentId });

    if (yield* FiberMap.has(children, subagentId)) {
      return yield* SubagentAlreadyStartedError.make({ subagentId });
    }

    const fiber = yield* FiberMap.run(
      children,
      subagentId,
      // oxlint-disable-next-line no-warning-comments
      // TODO: Replace with SubagentProcess.
      Effect.never.pipe(
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
  static readonly layer = Layer.effect(SubagentSupervisor, SubagentSupervisor.make);
}

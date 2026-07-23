import { Context, Effect, Layer, Semaphore } from "effect";

export class SubagentCapacity extends Context.Service<SubagentCapacity>()(
  "@smith/subagent/SubagentCapacity",
  {
    make: Effect.fn("SubagentCapacity.make")(function* (permits: number) {
      return yield* Semaphore.make(permits);
    }),
  },
) {
  static readonly layer = (permits: number) =>
    Layer.effect(SubagentCapacity, SubagentCapacity.make(permits));
}

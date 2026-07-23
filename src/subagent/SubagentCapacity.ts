import { Context, Layer, Semaphore } from "effect";

export class SubagentCapacity extends Context.Service<SubagentCapacity>()(
  "@smith/subagent/SubagentCapacity",
  { make: Semaphore.make },
) {
  static readonly layer = (permits: number) =>
    Layer.effect(SubagentCapacity, SubagentCapacity.make(permits));
}

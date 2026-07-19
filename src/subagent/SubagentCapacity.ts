import { Context, Layer, Semaphore } from "effect";

export class SubagentCapacity extends Context.Service<SubagentCapacity, Semaphore.Semaphore>()(
  "@smith/subagent/SubagentCapacity",
) {
  static readonly layer = (permits: number) =>
    Layer.effect(SubagentCapacity, Semaphore.make(permits));
}

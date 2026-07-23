import { Context, Effect, Layer, Ref } from "effect";

import type { SubagentId } from "./SubagentId.ts";
import type { SubagentRef } from "./SubagentRef.ts";

export class SubagentRegistry extends Context.Service<SubagentRegistry>()(
  "@smith/subagent/SubagentRegistry",
  {
    make: Effect.fn("SubagentRegistry.make")(function* () {
      const refs = yield* Ref.make(new Map<SubagentId, SubagentRef>());

      const register = Effect.fn("SubagentRegistry.register")(function* (
        subagentId: SubagentId,
        ref: SubagentRef,
      ) {
        yield* Ref.update(refs, (prev) => {
          const next = new Map(prev);
          next.set(subagentId, ref);
          return next;
        });
      });

      const unregister = Effect.fn("SubagentRegistry.unregister")(function* (
        subagentId: SubagentId,
        ref: SubagentRef,
      ) {
        yield* Ref.update(refs, (prev) => {
          if (prev.get(subagentId) !== ref) {
            return prev;
          }

          const next = new Map(prev);
          next.delete(subagentId);
          return next;
        });
      });

      const lookup = Effect.fn("SubagentRegistry.lookup")(function* (subagentId: SubagentId) {
        const current = yield* Ref.get(refs);
        return current.get(subagentId);
      });

      return { register, unregister, lookup };
    }),
  },
) {
  static readonly layer = Layer.effect(SubagentRegistry, SubagentRegistry.make());
}

import { Context, Effect, Layer, Ref, Schema } from "effect";

import { SubagentId } from "./SubagentId.ts";
import type { SubagentProcess } from "./SubagentProcess.ts";

export class SubagentAlreadyRegisteredError extends Schema.TaggedErrorClass<SubagentAlreadyRegisteredError>()(
  "SubagentAlreadyRegisteredError",
  {
    subagentId: SubagentId,
  },
) {}

export class SubagentNotRegisteredError extends Schema.TaggedErrorClass<SubagentNotRegisteredError>()(
  "SubagentNotRegisteredError",
  {
    subagentId: SubagentId,
  },
) {}

const make = Effect.gen(function* () {
  const processes = yield* Ref.make(new Map<SubagentId, SubagentProcess>());

  const register = Effect.fn("SubagentRegistry.register")(function* (process: SubagentProcess) {
    const subagentId = process.subagentId;
    const registered = yield* Ref.modify(processes, (prev) => {
      if (prev.has(subagentId)) {
        return [false, prev] as const;
      }

      const next = new Map(prev);
      next.set(subagentId, process);

      return [true, next] as const;
    });

    if (!registered) {
      return yield* SubagentAlreadyRegisteredError.make({ subagentId });
    }

    return yield* Effect.void;
  });

  const get = Effect.fn("SubagentRegistry.get")(function* (subagentId: SubagentId) {
    const ref = yield* Ref.get(processes);
    const process = ref.get(subagentId);

    if (process === undefined) {
      return yield* SubagentNotRegisteredError.make({ subagentId });
    }

    return process;
  });

  const unregister = Effect.fn("SubagentRegistry.unregister")(function* (subagentId: SubagentId) {
    yield* Ref.update(processes, (prev) => {
      if (!prev.has(subagentId)) {
        return prev;
      }

      const next = new Map(prev);
      next.delete(subagentId);

      return next;
    });
  });

  return { register, get, unregister };
});

export class SubagentRegistry extends Context.Service<SubagentRegistry>()(
  "@smith/subagent/SubagentRegistry",
  { make },
) {
  static readonly layer = Layer.effect(SubagentRegistry, SubagentRegistry.make);
}

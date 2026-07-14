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

  const register = Effect.fn("SubagentRegistry.register")(function* (
    subagentId: SubagentId,
    process: SubagentProcess,
  ) {
    const registered = yield* Ref.modify(processes, (currentProcesses) => {
      if (currentProcesses.has(subagentId)) {
        return [false, currentProcesses] as const;
      }

      const updatedProcesses = new Map(currentProcesses);
      updatedProcesses.set(subagentId, process);

      return [true, updatedProcesses] as const;
    });

    if (!registered) {
      return yield* SubagentAlreadyRegisteredError.make({ subagentId });
    }

    return yield* Effect.void;
  });

  const get = Effect.fn("SubagentRegistry.get")(function* (subagentId: SubagentId) {
    const currentProcesses = yield* Ref.get(processes);
    const process = currentProcesses.get(subagentId);

    if (process === undefined) {
      return yield* SubagentNotRegisteredError.make({ subagentId });
    }

    return process;
  });

  const unregister = Effect.fn("SubagentRegistry.unregister")(function* (subagentId: SubagentId) {
    yield* Ref.update(processes, (currentProcesses) => {
      if (!currentProcesses.has(subagentId)) {
        return currentProcesses;
      }

      const updatedProcesses = new Map(currentProcesses);
      updatedProcesses.delete(subagentId);

      return updatedProcesses;
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

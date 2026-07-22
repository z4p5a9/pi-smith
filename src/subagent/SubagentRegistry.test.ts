import { expect, it } from "@effect/vitest";
import { Effect } from "effect";

import { decodeSubagentId } from "./SubagentId.ts";
import { SubagentMessageId } from "./SubagentMessageId.ts";
import type { SubagentRef } from "./SubagentRef.ts";
import { SubagentRegistry } from "./SubagentRegistry.ts";

it.describe("SubagentRegistry", () => {
  it.effect("registers and unregisters the same reference", () =>
    Effect.gen(function* () {
      const registry = yield* SubagentRegistry;
      const subagentId = yield* decodeSubagentId("sa_12345678_registered");
      const ref: SubagentRef = {
        send: () => Effect.succeed(SubagentMessageId.make("msg_123456789012345678901234")),
      };

      yield* registry.register(subagentId, ref);
      expect(yield* registry.lookup(subagentId)).toBe(ref);

      yield* registry.unregister(subagentId, ref);
      expect(yield* registry.lookup(subagentId)).toBeUndefined();
    }).pipe(Effect.provide(SubagentRegistry.layer)),
  );

  it.effect("does not remove a newer registration with an older reference", () =>
    Effect.gen(function* () {
      const registry = yield* SubagentRegistry;
      const subagentId = yield* decodeSubagentId("sa_12345678_replaced");
      const firstRef: SubagentRef = {
        send: () => Effect.succeed(SubagentMessageId.make("msg_123456789012345678901234")),
      };
      const secondRef: SubagentRef = {
        send: () => Effect.succeed(SubagentMessageId.make("msg_abcdefghijklmnopqrstuvwx")),
      };

      yield* registry.register(subagentId, firstRef);
      yield* registry.register(subagentId, secondRef);

      yield* registry.unregister(subagentId, firstRef);
      expect(yield* registry.lookup(subagentId)).toBe(secondRef);

      yield* registry.unregister(subagentId, secondRef);
      expect(yield* registry.lookup(subagentId)).toBeUndefined();
    }).pipe(Effect.provide(SubagentRegistry.layer)),
  );
});

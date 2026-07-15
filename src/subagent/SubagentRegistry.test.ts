import { expect, it } from "@effect/vitest";
import { Effect } from "effect";

import { decodeSubagentId } from "./SubagentId.ts";
import { spawnSubagentProcess } from "./SubagentProcess.ts";
import {
  SubagentAlreadyRegisteredError,
  SubagentNotRegisteredError,
  SubagentRegistry,
} from "./SubagentRegistry.ts";

it.describe("SubagentRegistry", () => {
  it.effect("registers and gets a subagent process", () =>
    Effect.gen(function* () {
      const registry = yield* SubagentRegistry;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const process = yield* spawnSubagentProcess(subagentId, {
        title: "Review API",
        cwd: "/worktree",
      });

      yield* registry.register(process);

      expect(yield* registry.get(subagentId)).toBe(process);
    }).pipe(Effect.provide(SubagentRegistry.layer)),
  );

  it.effect("rejects an existing subagent ID", () =>
    Effect.gen(function* () {
      const registry = yield* SubagentRegistry;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const process = yield* spawnSubagentProcess(subagentId, {
        title: "Review API",
        cwd: "/worktree",
      });

      yield* registry.register(process);
      const error = yield* registry.register(process).pipe(Effect.flip);

      expect(error).toBeInstanceOf(SubagentAlreadyRegisteredError);
      expect(error.subagentId).toBe(subagentId);
    }).pipe(Effect.provide(SubagentRegistry.layer)),
  );

  it.effect("rejects an unknown subagent ID", () =>
    Effect.gen(function* () {
      const registry = yield* SubagentRegistry;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const error = yield* registry.get(subagentId).pipe(Effect.flip);

      expect(error).toBeInstanceOf(SubagentNotRegisteredError);
      expect(error.subagentId).toBe(subagentId);
    }).pipe(Effect.provide(SubagentRegistry.layer)),
  );

  it.effect("unregisters a subagent process", () =>
    Effect.gen(function* () {
      const registry = yield* SubagentRegistry;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const process = yield* spawnSubagentProcess(subagentId, {
        title: "Review API",
        cwd: "/worktree",
      });

      yield* registry.register(process);
      yield* registry.unregister(subagentId);
      const error = yield* registry.get(subagentId).pipe(Effect.flip);

      expect(error).toBeInstanceOf(SubagentNotRegisteredError);
      expect(error.subagentId).toBe(subagentId);
    }).pipe(Effect.provide(SubagentRegistry.layer)),
  );
});

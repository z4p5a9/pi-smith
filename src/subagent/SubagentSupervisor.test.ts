import { expect, it } from "@effect/vitest";
import { Effect, Fiber, Option, Schema } from "effect";
import { TestClock } from "effect/testing";

import { decodeSubagentId } from "./SubagentId.ts";
import { SubagentRegistry } from "./SubagentRegistry.ts";
import { SubagentAlreadyStartedError, SubagentSupervisor } from "./SubagentSupervisor.ts";

it.describe("SubagentSupervisor", () => {
  it.effect("starts a subagent child", () =>
    Effect.gen(function* () {
      const supervisor = yield* SubagentSupervisor;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const child = yield* supervisor.start(subagentId, { title: "Review API" });
      const result = yield* child.await.pipe(Effect.timeoutOption("1 millis"), Effect.forkChild);

      yield* TestClock.adjust("1 millis");

      expect(Option.isNone(yield* Fiber.join(result))).toBe(true);
    }).pipe(Effect.provide(SubagentSupervisor.layer)),
  );

  it.effect("rejects one of two concurrent starts for the same subagent ID", () =>
    Effect.gen(function* () {
      const supervisor = yield* SubagentSupervisor;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");
      const start = supervisor
        .start(subagentId, { title: "Review API" })
        .pipe(
          Effect.as("started" as const),
          Effect.catchTag("SubagentAlreadyStartedError", Effect.succeed),
        );
      const results = yield* Effect.all([start, start], { concurrency: "unbounded" });

      expect(results.filter((result) => result === "started")).toHaveLength(1);
      expect(results.filter(Schema.is(SubagentAlreadyStartedError))).toHaveLength(1);
    }).pipe(Effect.provide(SubagentSupervisor.layer)),
  );

  it.effect("registers a running child", () =>
    Effect.gen(function* () {
      const supervisor = yield* SubagentSupervisor;
      const registry = yield* SubagentRegistry;
      const subagentId = yield* decodeSubagentId("sa_12345678_review-api");

      yield* supervisor.start(subagentId, { title: "Review API" });
      const process = yield* registry.get(subagentId);

      expect(yield* process.status).toBe("running");
    }).pipe(Effect.provide(SubagentSupervisor.layer)),
  );
});

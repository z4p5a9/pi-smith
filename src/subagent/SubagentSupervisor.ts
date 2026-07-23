import { Deferred, Effect, Fiber, Stream } from "effect";

import { SubagentEventOutbox } from "./SubagentEventOutbox.ts";
import type { SubagentId } from "./SubagentId.ts";
import { makeSubagentProcess } from "./SubagentProcess.ts";
import type { SubagentProcessResult } from "./SubagentProcessResult.ts";
import { SubagentRegistry } from "./SubagentRegistry.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

export interface SubagentSupervisor {
  readonly start: Effect.Effect<void>;
  readonly interrupt: Effect.Effect<void>;
  readonly await: Effect.Effect<SubagentProcessResult>;
}

export const makeSubagentSupervisor = Effect.fn("SubagentSupervisor.make")(function* (
  subagentId: SubagentId,
  spec: SubagentSpec,
) {
  const eventOutbox = yield* SubagentEventOutbox;
  const registry = yield* SubagentRegistry;
  const process = yield* makeSubagentProcess(subagentId, spec);
  const started = yield* Deferred.make<void>();
  const runtime = yield* Deferred.make<Fiber.Fiber<void>>();
  const result = yield* Deferred.make<SubagentProcessResult>();

  const start = Effect.gen(function* () {
    const fiber = yield* Effect.gen(function* () {
      yield* Effect.acquireRelease(registry.register(subagentId, process.ref), () =>
        registry.unregister(subagentId, process.ref),
      );
      const running = yield* Effect.all(
        [
          process.run,
          process.events.pipe(
            Stream.runForEach((event) => eventOutbox.publish({ subagentId, event })),
          ),
        ],
        {
          concurrency: "unbounded",
          discard: true,
        },
      ).pipe(Effect.forkScoped({ startImmediately: true }));

      yield* Deferred.succeed(started, undefined);
      yield* Fiber.join(running);
    }).pipe(
      Effect.scoped,
      Effect.ensuring(
        process.await.pipe(
          Effect.flatMap((terminal) => Deferred.succeed(result, terminal)),
          Effect.asVoid,
        ),
      ),
      Effect.forkDetach({ startImmediately: true }),
    );

    yield* Deferred.succeed(runtime, fiber);
    yield* Deferred.await(started);
  }).pipe(Effect.withSpan("SubagentSupervisor.start"));

  const interrupt = Effect.gen(function* () {
    const fiber = yield* Deferred.await(runtime);
    yield* Fiber.interrupt(fiber);
    yield* Deferred.await(result);
  }).pipe(Effect.withSpan("SubagentSupervisor.interrupt"));

  return {
    start,
    interrupt,
    await: Deferred.await(result),
  } satisfies SubagentSupervisor;
});

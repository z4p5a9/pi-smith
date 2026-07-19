import { Context, Effect, Layer, Queue, Stream } from "effect";

import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import type { SubagentEventEnvelope } from "./SubagentEvent.ts";
import { generateSubagentId, type SubagentId } from "./SubagentId.ts";
import { makeSubagentProcess } from "./SubagentProcess.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

interface Admission {
  readonly subagentId: SubagentId;
  readonly spec: SubagentSpec;
}

const make = Effect.fn("SubagentCoordinator.make")(function* () {
  const checkpoint = yield* SubagentCheckpoint;
  const admissions = yield* Queue.unbounded<Admission>();
  const events = yield* Queue.unbounded<SubagentEventEnvelope>();

  const worker = Effect.forever(
    Effect.gen(function* () {
      const { spec, subagentId } = yield* Queue.take(admissions);
      const process = yield* makeSubagentProcess(subagentId, spec);
      const aggregate = process.events.pipe(
        Stream.runForEach((event) => Queue.offer(events, { subagentId, event })),
      );

      yield* Effect.all([process.run, aggregate], {
        concurrency: "unbounded",
        discard: true,
      });
    }),
  );

  for (let index = 0; index < 10; index++) {
    yield* worker.pipe(Effect.forkScoped({ startImmediately: true }));
  }

  yield* Effect.addFinalizer(() =>
    Queue.shutdown(admissions).pipe(Effect.andThen(Queue.shutdown(events))),
  );

  const create = Effect.fn("SubagentCoordinator.create")(function* (spec: SubagentSpec) {
    const subagentId = yield* generateSubagentId(spec.title);

    yield* checkpoint.put({ subagentId, status: "queued", ...spec });
    yield* Queue.offer(admissions, { subagentId, spec });

    return subagentId;
  });

  return {
    create,
    events: Stream.fromQueue(events),
  };
});

export class SubagentCoordinator extends Context.Service<SubagentCoordinator>()(
  "@smith/subagent/SubagentCoordinator",
  { make },
) {
  static readonly layer = Layer.effect(SubagentCoordinator, SubagentCoordinator.make());
}

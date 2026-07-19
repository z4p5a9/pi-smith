import { Context, Effect, FiberMap, Layer, Queue, Schema, Stream } from "effect";

import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import type { SubagentEventEnvelope } from "./SubagentEvent.ts";
import { generateSubagentId, SubagentId } from "./SubagentId.ts";
import { makeSubagentProcess, type SubagentProcess } from "./SubagentProcess.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

export class SubagentUnknownError extends Schema.TaggedErrorClass<SubagentUnknownError>()(
  "SubagentUnknownError",
  {
    subagentId: SubagentId,
  },
) {}

export class SubagentInactiveError extends Schema.TaggedErrorClass<SubagentInactiveError>()(
  "SubagentInactiveError",
  {
    subagentId: SubagentId,
  },
) {}

interface Admission {
  readonly subagentId: SubagentId;
  readonly spec: SubagentSpec;
}

const make = Effect.fn("SubagentCoordinator.make")(function* () {
  const checkpoint = yield* SubagentCheckpoint;
  const admissions = yield* Queue.unbounded<Admission>();
  const events = yield* Queue.unbounded<SubagentEventEnvelope>();
  const children = yield* FiberMap.make<SubagentId>();
  const registry = new Map<SubagentId, SubagentProcess>();

  yield* Effect.forever(
    Effect.gen(function* () {
      const { spec, subagentId } = yield* Queue.take(admissions);
      const process = yield* makeSubagentProcess(subagentId, spec);
      const aggregate = process.events.pipe(
        Stream.runForEach((event) => Queue.offer(events, { subagentId, event })),
      );

      registry.set(subagentId, process);
      yield* FiberMap.run(
        children,
        subagentId,
        Effect.all([process.run, aggregate], { concurrency: "unbounded", discard: true }),
        { startImmediately: true },
      );
    }),
  ).pipe(Effect.forkScoped({ startImmediately: true }));

  yield* Effect.addFinalizer(() =>
    Queue.shutdown(admissions).pipe(
      Effect.andThen(FiberMap.clear(children)),
      Effect.andThen(Queue.shutdown(events)),
    ),
  );

  const create = Effect.fn("SubagentCoordinator.create")(function* (spec: SubagentSpec) {
    const subagentId = yield* generateSubagentId(spec.title);

    yield* checkpoint.put({ subagentId, status: "queued", ...spec });
    yield* Queue.offer(admissions, { subagentId, spec });

    return subagentId;
  });

  const send = Effect.fn("SubagentCoordinator.send")(function* (
    subagentId: SubagentId,
    content: string,
  ) {
    const process = registry.get(subagentId);

    if (process === undefined) {
      return yield* SubagentUnknownError.make({ subagentId });
    }

    const delivered = yield* process.send(content);

    if (!delivered) {
      return yield* SubagentInactiveError.make({ subagentId });
    }

    return yield* Effect.void;
  });

  const kill = Effect.fn("SubagentCoordinator.kill")(function* (subagentId: SubagentId) {
    if (!registry.has(subagentId)) {
      return yield* SubagentUnknownError.make({ subagentId });
    }

    return yield* FiberMap.remove(children, subagentId);
  });

  return {
    create,
    send,
    kill,
    events: Stream.fromQueue(events),
  };
});

export class SubagentCoordinator extends Context.Service<SubagentCoordinator>()(
  "@smith/subagent/SubagentCoordinator",
  { make },
) {
  static readonly layer = Layer.effect(SubagentCoordinator, SubagentCoordinator.make());
}

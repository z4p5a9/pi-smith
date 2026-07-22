import { Context, Deferred, Effect, FiberMap, Layer, Queue, Schema, Stream } from "effect";

import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import { SubagentEventOutbox } from "./SubagentEventOutbox.ts";
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
  readonly ready: Deferred.Deferred<void>;
}

const make = Effect.fn("SubagentCoordinator.make")(function* () {
  const checkpoint = yield* SubagentCheckpoint;
  const eventOutbox = yield* SubagentEventOutbox;
  const admissions = yield* Queue.unbounded<Admission>();
  const children = yield* FiberMap.make<SubagentId>();
  const registry = new Map<SubagentId, SubagentProcess>();

  yield* Effect.forever(
    Effect.gen(function* () {
      const admission = yield* Queue.take(admissions);
      const process = yield* makeSubagentProcess(admission.subagentId, admission.spec);
      const aggregate = process.events.pipe(
        Stream.runForEach((event) =>
          eventOutbox.publish({ subagentId: admission.subagentId, event }),
        ),
      );

      yield* Effect.uninterruptible(
        Effect.gen(function* () {
          registry.set(admission.subagentId, process);
          yield* FiberMap.run(
            children,
            admission.subagentId,
            Effect.all([process.run, aggregate], {
              concurrency: "unbounded",
              discard: true,
            }).pipe(Effect.ensuring(Effect.sync(() => registry.delete(admission.subagentId)))),
            { startImmediately: true },
          );
          yield* Deferred.succeed(admission.ready, undefined);
        }),
      );
    }),
  ).pipe(Effect.forkScoped({ startImmediately: true }));

  yield* Effect.addFinalizer(() =>
    Queue.shutdown(admissions).pipe(Effect.andThen(FiberMap.clear(children))),
  );

  const create = Effect.fn("SubagentCoordinator.create")(function* (spec: SubagentSpec) {
    const subagentId = yield* generateSubagentId(spec.title);
    const ready = yield* Deferred.make<void>();

    yield* checkpoint.put({ subagentId, status: "queued", ...spec });
    yield* Queue.offer(admissions, { subagentId, spec, ready });
    yield* Deferred.await(ready);

    return subagentId;
  });

  const send = Effect.fn("SubagentCoordinator.send")(function* (
    subagentId: SubagentId,
    content: string,
  ) {
    const process = registry.get(subagentId);

    if (process === undefined) {
      if (!(yield* checkpoint.has(subagentId))) {
        return yield* SubagentUnknownError.make({ subagentId });
      }

      return yield* SubagentInactiveError.make({ subagentId });
    }

    const messageId = yield* process.send(content);

    if (messageId === undefined) {
      return yield* SubagentInactiveError.make({ subagentId });
    }

    return messageId;
  });

  const kill = Effect.fn("SubagentCoordinator.kill")(function* (subagentId: SubagentId) {
    return yield* Effect.uninterruptible(
      Effect.gen(function* () {
        const process = registry.get(subagentId);

        if (process === undefined) {
          if (!(yield* checkpoint.has(subagentId))) {
            return yield* SubagentUnknownError.make({ subagentId });
          }

          return yield* SubagentInactiveError.make({ subagentId });
        }

        yield* FiberMap.remove(children, subagentId);

        const result = yield* process.await;

        if (result.kind !== "killed") {
          return yield* SubagentInactiveError.make({ subagentId });
        }

        return yield* checkpoint.update(subagentId, { status: "killed" });
      }).pipe(
        Effect.catchTag("SubagentNotFoundError", () => SubagentUnknownError.make({ subagentId })),
      ),
    );
  });

  return {
    create,
    send,
    kill,
  };
});

export class SubagentCoordinator extends Context.Service<SubagentCoordinator>()(
  "@smith/subagent/SubagentCoordinator",
  { make },
) {
  static readonly layer = Layer.effect(SubagentCoordinator, SubagentCoordinator.make());
}

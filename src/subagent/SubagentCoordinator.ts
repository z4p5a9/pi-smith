import { Context, Deferred, Effect, Layer, Queue, Ref, Schema } from "effect";

import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import { generateSubagentId, SubagentId } from "./SubagentId.ts";
import { makeSubagentSupervisor, type SubagentSupervisor } from "./SubagentSupervisor.ts";
import { SubagentRegistry } from "./SubagentRegistry.ts";
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

export class SubagentCoordinator extends Context.Service<SubagentCoordinator>()(
  "@smith/subagent/SubagentCoordinator",
  {
    make: Effect.fn("SubagentCoordinator.make")(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const supervisors = yield* SubagentSupervisorRegistry.make();
      const admissions = yield* Queue.unbounded<{
        readonly subagentId: SubagentId;
        readonly spec: SubagentSpec;
        readonly ready: Deferred.Deferred<void>;
      }>();

      yield* Effect.addFinalizer(() =>
        Queue.shutdown(admissions).pipe(
          Effect.andThen(
            supervisors.values.pipe(
              Effect.flatMap((current) =>
                Effect.forEach(current, (supervisor) => supervisor.interrupt, {
                  concurrency: "unbounded",
                  discard: true,
                }),
              ),
            ),
          ),
        ),
      );

      yield* Effect.forever(
        Effect.gen(function* () {
          const admission = yield* Queue.take(admissions);
          const supervisor = yield* makeSubagentSupervisor(admission.subagentId, admission.spec);

          yield* Effect.uninterruptible(
            Effect.gen(function* () {
              yield* supervisors.register(admission.subagentId, supervisor);
              yield* supervisor.await.pipe(
                Effect.ensuring(supervisors.unregister(admission.subagentId, supervisor)),
                Effect.forkDetach({ startImmediately: true }),
              );
              yield* supervisor.start;
              yield* Deferred.succeed(admission.ready, undefined);
            }),
          );
        }),
      ).pipe(Effect.forkScoped({ startImmediately: true }));

      const create = Effect.fn("SubagentCoordinator.create")(function* (spec: SubagentSpec) {
        const subagentId = yield* generateSubagentId(spec.title);
        const ready = yield* Deferred.make<void>();

        yield* checkpoint.put({ subagentId, status: "queued", ...spec });
        yield* Queue.offer(admissions, { subagentId, spec, ready });
        yield* Deferred.await(ready);

        return subagentId;
      });

      const kill = Effect.fn("SubagentCoordinator.kill")(function* (subagentId: SubagentId) {
        return yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const supervisor = yield* supervisors.lookup(subagentId);

            if (supervisor === undefined) {
              if (!(yield* checkpoint.has(subagentId))) {
                return yield* SubagentUnknownError.make({ subagentId });
              }

              return yield* SubagentInactiveError.make({ subagentId });
            }

            yield* supervisor.interrupt;
            const result = yield* supervisor.await;

            if (result.kind !== "killed") {
              return yield* SubagentInactiveError.make({ subagentId });
            }

            return yield* checkpoint.update(subagentId, { status: "killed" });
          }).pipe(
            Effect.catchTag("SubagentNotFoundError", () =>
              SubagentUnknownError.make({ subagentId }),
            ),
          ),
        );
      });

      return {
        create,
        kill,
      };
    }),
  },
) {
  static readonly layer = Layer.effect(SubagentCoordinator, SubagentCoordinator.make()).pipe(
    Layer.provideMerge(SubagentRegistry.layer),
  );
}

class SubagentSupervisorRegistry extends Context.Service<SubagentSupervisorRegistry>()(
  "@smith/subagent/SubagentSupervisorRegistry",
  {
    make: Effect.fn("SubagentSupervisorRegistry.make")(function* () {
      const supervisors = yield* Ref.make(new Map<SubagentId, SubagentSupervisor>());

      const register = Effect.fn("SubagentSupervisorRegistry.register")(function* (
        subagentId: SubagentId,
        supervisor: SubagentSupervisor,
      ) {
        yield* Ref.update(supervisors, (prev) => {
          const next = new Map(prev);
          next.set(subagentId, supervisor);
          return next;
        });
      });

      const unregister = Effect.fn("SubagentSupervisorRegistry.unregister")(function* (
        subagentId: SubagentId,
        supervisor: SubagentSupervisor,
      ) {
        yield* Ref.update(supervisors, (prev) => {
          if (prev.get(subagentId) !== supervisor) {
            return prev;
          }

          const next = new Map(prev);
          next.delete(subagentId);
          return next;
        });
      });

      const lookup = Effect.fn("SubagentSupervisorRegistry.lookup")(function* (
        subagentId: SubagentId,
      ) {
        const current = yield* Ref.get(supervisors);
        return current.get(subagentId);
      });

      const values = Ref.get(supervisors).pipe(
        Effect.map((current) => Array.from(current.values())),
      );

      return { register, unregister, lookup, values };
    }),
  },
) {}

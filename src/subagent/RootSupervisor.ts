import { Context, Deferred, Effect, Fiber, Layer, Queue, Ref, Schema } from "effect";

import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import { SubagentEventOutbox } from "./SubagentEventOutbox.ts";
import { generateSubagentId, SubagentId } from "./SubagentId.ts";
import * as SubagentSupervisor from "./SubagentSupervisor.ts";
import { SubagentRegistry } from "./SubagentRegistry.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

export class SubagentKillUnknownError extends Schema.TaggedErrorClass<SubagentKillUnknownError>()(
  "SubagentKillUnknownError",
  {
    subagentId: SubagentId,
  },
) {}

export class SubagentKillInactiveError extends Schema.TaggedErrorClass<SubagentKillInactiveError>()(
  "SubagentKillInactiveError",
  {
    subagentId: SubagentId,
  },
) {}

export class RootSupervisor extends Context.Service<RootSupervisor>()(
  "@smith/subagent/RootSupervisor",
  {
    make: Effect.fn("RootSupervisor.make")(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const supervisors = yield* SubagentSupervisorRegistry.make();
      const admissions = yield* Queue.unbounded<{
        readonly subagentId: SubagentId;
        readonly spec: SubagentSpec;
        readonly ready: Deferred.Deferred<void>;
      }>();

      yield* Effect.acquireRelease(
        Effect.forever(
          Effect.gen(function* () {
            const admission = yield* Queue.take(admissions);
            const supervisor = yield* SubagentSupervisor.make(admission.subagentId, admission.spec);

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
        ).pipe(Effect.forkDetach({ startImmediately: true })),
        (admissionsFiber) =>
          Queue.shutdown(admissions).pipe(
            Effect.andThen(Fiber.interrupt(admissionsFiber)),
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

      const create = Effect.fn("RootSupervisor.create")(function* (spec: SubagentSpec) {
        const subagentId = yield* generateSubagentId(spec.title);
        const ready = yield* Deferred.make<void>();

        yield* checkpoint.put({ subagentId, status: "queued", ...spec });
        yield* Queue.offer(admissions, { subagentId, spec, ready });
        yield* Deferred.await(ready);

        return subagentId;
      });

      const kill = Effect.fn("RootSupervisor.kill")(function* (subagentId: SubagentId) {
        return yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const supervisor = yield* supervisors.lookup(subagentId);

            if (supervisor === undefined) {
              if (!(yield* checkpoint.has(subagentId))) {
                return yield* SubagentKillUnknownError.make({ subagentId });
              }

              return yield* SubagentKillInactiveError.make({ subagentId });
            }

            yield* supervisor.interrupt;
            const result = yield* supervisor.await;

            if (result.kind !== "killed") {
              return yield* SubagentKillInactiveError.make({ subagentId });
            }

            return yield* checkpoint.update(subagentId, { status: "killed" });
          }).pipe(
            Effect.catchTag("SubagentNotFoundError", () =>
              SubagentKillUnknownError.make({ subagentId }),
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
  static readonly layerNoDeps = Layer.effect(RootSupervisor, RootSupervisor.make());

  static readonly layer = RootSupervisor.layerNoDeps.pipe(
    Layer.provideMerge(SubagentCheckpoint.layer),
    Layer.provideMerge(SubagentEventOutbox.layer),
    Layer.provideMerge(SubagentRegistry.layer),
  );
}

class SubagentSupervisorRegistry extends Context.Service<SubagentSupervisorRegistry>()(
  "@smith/subagent/SubagentSupervisorRegistry",
  {
    make: Effect.fn("SubagentSupervisorRegistry.make")(function* () {
      const supervisors = yield* Ref.make(
        new Map<SubagentId, SubagentSupervisor.SubagentSupervisor>(),
      );

      const register = Effect.fn("SubagentSupervisorRegistry.register")(function* (
        subagentId: SubagentId,
        supervisor: SubagentSupervisor.SubagentSupervisor,
      ) {
        yield* Ref.update(supervisors, (prev) => {
          const next = new Map(prev);
          next.set(subagentId, supervisor);
          return next;
        });
      });

      const unregister = Effect.fn("SubagentSupervisorRegistry.unregister")(function* (
        subagentId: SubagentId,
        supervisor: SubagentSupervisor.SubagentSupervisor,
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
) {
  static readonly layer = Layer.effect(
    SubagentSupervisorRegistry,
    SubagentSupervisorRegistry.make(),
  );
}

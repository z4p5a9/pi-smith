import { Cause, Deferred, Effect, Fiber, Predicate, Queue, Stream } from "effect";

import { SubagentHarness } from "../harness/Harness.ts";
import { SubagentHost } from "../host/Host.ts";
import { SubagentCapacity } from "./SubagentCapacity.ts";
import { SubagentCheckpoint, type SubagentRecord } from "./SubagentCheckpoint.ts";
import type { SubagentEvent } from "./SubagentEvent.ts";
import type { SubagentId } from "./SubagentId.ts";
import type { SubagentProcessResult } from "./SubagentProcessResult.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

export interface SubagentProcess {
  readonly subagentId: SubagentId;
  readonly events: Stream.Stream<SubagentEvent>;
  readonly await: Effect.Effect<SubagentProcessResult>;
  readonly send: (content: string) => Effect.Effect<boolean>;
  readonly run: Effect.Effect<void>;
}

export const makeSubagentProcess = Effect.fn("SubagentProcess.make")(function* (
  subagentId: SubagentId,
  spec: SubagentSpec,
) {
  const capacity = yield* SubagentCapacity;
  const checkpoint = yield* SubagentCheckpoint;
  const harness = yield* SubagentHarness;
  const host = yield* SubagentHost;
  const events = yield* Queue.unbounded<SubagentEvent, Cause.Done>();
  const outbox = yield* Queue.unbounded<string, Cause.Done>();
  const result = yield* Deferred.make<SubagentProcessResult>();

  const project = (fields: Partial<Omit<SubagentRecord, "subagentId">>) =>
    checkpoint
      .update(subagentId, fields)
      .pipe(
        Effect.catch((error) =>
          Effect.logDebug("Skipped subagent checkpoint projection", error).pipe(
            Effect.annotateLogs({ subagentId }),
          ),
        ),
      );

  const accept = (event: SubagentEvent) =>
    Effect.uninterruptible(
      project(
        spec.mode === "ephemeral"
          ? {
              status: event.kind === "message" ? "completed" : "failed",
              latestEvent: event,
            }
          : { latestEvent: event },
      ).pipe(Effect.andThen(Queue.offer(events, event)), Effect.asVoid),
    );

  const run = Effect.gen(function* () {
    const command = yield* harness.makeCommand(subagentId, spec);
    const session = yield* capacity.withPermit(
      Effect.gen(function* () {
        yield* project({ status: "starting" });

        const started = yield* host.start(subagentId, command);

        yield* project({ status: "running" });

        const eventFiber = yield* started.take.pipe(Effect.forkScoped({ startImmediately: true }));
        let event: SubagentEvent | undefined;

        while (event === undefined) {
          const wake = yield* Effect.raceFirst(
            Fiber.join(eventFiber).pipe(
              Effect.map((childEvent) => ({ kind: "event" as const, event: childEvent })),
            ),
            Queue.take(outbox).pipe(Effect.map((content) => ({ kind: "send" as const, content }))),
          );

          if (wake.kind === "event") {
            event = wake.event;
          } else {
            yield* started.send(wake.content);
          }
        }

        yield* accept(event);
        return started;
      }),
    );

    if (spec.mode === "ephemeral") {
      yield* Deferred.succeed(result, { kind: "exited" });
      return yield* Effect.void;
    }

    let eventFiber = yield* session.take.pipe(Effect.forkScoped({ startImmediately: true }));

    return yield* Effect.forever(
      Effect.gen(function* () {
        yield* project({ status: "idle" });

        const wake = yield* Effect.raceFirst(
          Fiber.join(eventFiber).pipe(Effect.map((event) => ({ kind: "event" as const, event }))),
          Queue.take(outbox).pipe(Effect.map((content) => ({ kind: "send" as const, content }))),
        );

        if (wake.kind === "event") {
          yield* accept(wake.event);
          eventFiber = yield* session.take.pipe(Effect.forkScoped({ startImmediately: true }));
          return yield* Effect.void;
        }

        yield* project({ status: "waiting" });
        return yield* capacity.withPermit(
          Effect.gen(function* () {
            yield* project({ status: "running" });
            yield* session.send(wake.content);

            let event: SubagentEvent | undefined;

            while (event === undefined) {
              const turnWake = yield* Effect.raceFirst(
                Fiber.join(eventFiber).pipe(
                  Effect.map((childEvent) => ({ kind: "event" as const, event: childEvent })),
                ),
                Queue.take(outbox).pipe(
                  Effect.map((content) => ({ kind: "send" as const, content })),
                ),
              );

              if (turnWake.kind === "event") {
                event = turnWake.event;
              } else {
                yield* session.send(turnWake.content);
              }
            }

            yield* accept(event);
            eventFiber = yield* session.take.pipe(Effect.forkScoped({ startImmediately: true }));
          }),
        );
      }),
    );
  }).pipe(
    Effect.scoped,
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.interrupt
        : Effect.suspend(() => {
            const squashed = Cause.squash(cause);
            const reason =
              Predicate.hasProperty(squashed, "reason") && typeof squashed.reason === "string"
                ? squashed.reason
                : String(squashed);
            const failure = { kind: "failure", reason } as const;

            return Effect.uninterruptible(
              project({ status: "failed", latestEvent: failure }).pipe(
                Effect.andThen(Queue.offer(events, failure)),
                Effect.andThen(Deferred.succeed(result, { kind: "failed", reason })),
                Effect.asVoid,
              ),
            );
          }),
    ),
    // The result must resolve on every exit path; interruption resolves `killed`.
    Effect.onExit(() => Deferred.succeed(result, { kind: "killed" }).pipe(Effect.asVoid)),
    Effect.ensuring(Queue.end(events).pipe(Effect.andThen(Queue.end(outbox)))),
    Effect.annotateLogs({ subagentId }),
  );

  return {
    subagentId,
    events: Stream.fromQueue(events),
    await: Deferred.await(result),
    send: (content: string) => Queue.offer(outbox, content),
    run,
  } satisfies SubagentProcess;
});

import { Cause, Deferred, Effect, Predicate, Queue, Stream } from "effect";

import { SubagentHarness } from "../harness/Harness.ts";
import { SubagentHost, type SubagentHostSession } from "../host/Host.ts";
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
  const accepted = yield* Queue.unbounded<SubagentEvent, Cause.Done>();
  const mailbox = yield* Queue.unbounded<string, Cause.Done>();
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
      ).pipe(Effect.andThen(Queue.offer(accepted, event)), Effect.asVoid),
    );

  // A turn ends at the next child event; sends arriving mid-turn are steering
  // and forwarded immediately.
  const awaitTurnEnd = (session: SubagentHostSession) =>
    Effect.gen(function* () {
      let turnEnd: SubagentEvent | undefined;

      while (turnEnd === undefined) {
        const wake = yield* Effect.raceFirst(
          session.take.pipe(Effect.map((event) => ({ kind: "event" as const, event }))),
          Queue.take(mailbox).pipe(Effect.map((content) => ({ kind: "send" as const, content }))),
        );

        if (wake.kind === "event") {
          turnEnd = wake.event;
        } else {
          yield* session.send(wake.content);
        }
      }

      return turnEnd;
    });

  const run = Effect.gen(function* () {
    const command = yield* harness.makeCommand(subagentId, spec);
    const session = yield* capacity.withPermit(
      Effect.gen(function* () {
        yield* project({ status: "starting" });

        const started = yield* host.start(subagentId, command);

        yield* project({ status: "running" });

        const event = yield* awaitTurnEnd(started);

        yield* accept(event);
        return started;
      }),
    );

    if (spec.mode === "ephemeral") {
      yield* Deferred.succeed(result, { kind: "exited" });
      return yield* Effect.void;
    }

    return yield* Effect.forever(
      Effect.gen(function* () {
        yield* project({ status: "idle" });

        const wake = yield* Effect.raceFirst(
          Queue.take(mailbox).pipe(Effect.map((content) => ({ kind: "send" as const, content }))),
          session.take.pipe(Effect.map((event) => ({ kind: "event" as const, event }))),
        );

        if (wake.kind === "event") {
          return yield* accept(wake.event);
        }

        yield* project({ status: "waiting" });
        return yield* capacity.withPermit(
          Effect.gen(function* () {
            yield* project({ status: "running" });
            yield* session.send(wake.content);

            const event = yield* awaitTurnEnd(session);

            yield* accept(event);
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
                Effect.andThen(Queue.offer(accepted, failure)),
                Effect.andThen(Deferred.succeed(result, { kind: "failed", reason })),
                Effect.asVoid,
              ),
            );
          }),
    ),
    // The result must resolve on every exit path; interruption resolves `killed`.
    Effect.onExit(() => Deferred.succeed(result, { kind: "killed" }).pipe(Effect.asVoid)),
    Effect.ensuring(Queue.end(accepted).pipe(Effect.andThen(Queue.end(mailbox)))),
    Effect.annotateLogs({ subagentId }),
  );

  return {
    subagentId,
    events: Stream.fromQueue(accepted),
    await: Deferred.await(result),
    send: (content: string) => Queue.offer(mailbox, content),
    run,
  } satisfies SubagentProcess;
});

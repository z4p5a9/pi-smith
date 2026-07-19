import { Cause, Deferred, Effect, Option, Queue, Stream } from "effect";

import { SubagentHarness } from "../harness/Harness.ts";
import { SubagentHost } from "../host/Host.ts";
import { SubagentCheckpoint, type SubagentRecord } from "./SubagentCheckpoint.ts";
import type { SubagentEvent } from "./SubagentEvent.ts";
import type { SubagentId } from "./SubagentId.ts";
import type { SubagentProcessResult } from "./SubagentProcessResult.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

export interface SubagentProcess {
  readonly subagentId: SubagentId;
  readonly events: Stream.Stream<SubagentEvent>;
  readonly await: Effect.Effect<SubagentProcessResult>;
  readonly run: Effect.Effect<void>;
}

export const makeSubagentProcess = Effect.fn("SubagentProcess.make")(function* (
  subagentId: SubagentId,
  spec: SubagentSpec,
) {
  const checkpoint = yield* SubagentCheckpoint;
  const harness = yield* SubagentHarness;
  const host = yield* SubagentHost;
  const accepted = yield* Queue.unbounded<SubagentEvent, Cause.Done>();
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

  const finish = (event: SubagentEvent, processResult: SubagentProcessResult) =>
    Effect.uninterruptible(
      project({
        status: event.kind === "message" ? "completed" : "failed",
        latestEvent: event,
      }).pipe(
        Effect.andThen(Queue.offer(accepted, event)),
        Effect.andThen(Deferred.succeed(result, processResult)),
        Effect.asVoid,
      ),
    );

  const run = Effect.gen(function* () {
    yield* project({ status: "starting" });

    const command = yield* harness.makeCommand(subagentId, spec);
    const session = yield* host.start(subagentId, command);

    yield* project({ status: "running" });

    // Ephemeral policy: the first event ends execution; later frames are
    // acknowledged by the Bridge and dropped unread with the connection.
    const first = yield* session.events.pipe(Stream.runHead);

    if (Option.isSome(first)) {
      return yield* finish(first.value, { kind: "exited" });
    }

    yield* session.await;

    const failure = {
      kind: "failure",
      reason: "Subagent disconnected before reporting",
    } as const;

    return yield* finish(failure, { kind: "failed", reason: failure.reason });
  }).pipe(
    Effect.scoped,
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause)
        ? Effect.interrupt
        : Effect.suspend(() => {
            const failure = { kind: "failure", reason: String(Cause.squash(cause)) } as const;

            return finish(failure, { kind: "failed", reason: failure.reason });
          }),
    ),
    // The result must resolve on every exit path; interruption resolves `killed`.
    Effect.onExit(() => Deferred.succeed(result, { kind: "killed" }).pipe(Effect.asVoid)),
    Effect.ensuring(Queue.end(accepted)),
    Effect.annotateLogs({ subagentId }),
  );

  return {
    subagentId,
    events: Stream.fromQueue(accepted),
    await: Deferred.await(result),
    run,
  } satisfies SubagentProcess;
});

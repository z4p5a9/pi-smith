import { Cause, Deferred, Effect, Fiber, Predicate, Queue, Stream } from "effect";

import { SubagentHarness } from "../harness/Harness.ts";
import { SubagentHost, type SubagentHostSession } from "../host/Host.ts";
import { SubagentCapacity } from "./SubagentCapacity.ts";
import { SubagentCheckpoint, type SubagentRecord } from "./SubagentCheckpoint.ts";
import type { SubagentEvent, SubagentProcessEvent } from "./SubagentEvent.ts";
import type { SubagentId } from "./SubagentId.ts";
import { generateSubagentMessageId, type SubagentMessageId } from "./SubagentMessageId.ts";
import type { SubagentProcessResult } from "./SubagentProcessResult.ts";
import type { SubagentRef } from "./SubagentRef.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

interface SubagentMessage {
  readonly messageId: SubagentMessageId;
  readonly content: string;
}

export interface SubagentProcess {
  readonly subagentId: SubagentId;
  readonly events: Stream.Stream<SubagentProcessEvent>;
  readonly await: Effect.Effect<SubagentProcessResult>;
  readonly ref: SubagentRef;
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
  const events = yield* Queue.unbounded<SubagentProcessEvent, Cause.Done>();
  const outbox = yield* Queue.unbounded<SubagentMessage, Cause.Done>();
  const result = yield* Deferred.make<SubagentProcessResult>();

  const sendToSession = (session: SubagentHostSession, message: SubagentMessage) =>
    session.send(message.content).pipe(
      Effect.as(true),
      Effect.catchTag("LinkFrameTooLargeError", (error) =>
        Effect.uninterruptible(
          Queue.offer(events, {
            kind: "message-rejected",
            messageId: message.messageId,
            reason: "frame-too-large",
            actualBytes: error.actualBytes,
            maxBytes: error.maxBytes,
          }).pipe(Effect.as(false)),
        ),
      ),
    );

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

  const accept = Effect.fn("SubagentProcess.accept")(function* (event: SubagentEvent) {
    const exits = spec.mode === "ephemeral";

    yield* project({ latestEvent: event });

    if (exits) {
      yield* project({ status: "exited" });
      yield* Deferred.succeed(result, { kind: "exited" });
    }

    yield* Queue.offer(events, event);

    return exits;
  }, Effect.uninterruptible);

  const run = Effect.gen(function* () {
    const command = yield* harness.makeCommand(subagentId, spec);
    const session = yield* capacity.withPermit(
      Effect.gen(function* () {
        yield* project({ status: "starting" });

        const started = yield* host.start(subagentId, command);

        yield* project({ status: "running" });

        let inboundFiber = yield* started.take.pipe(Effect.forkScoped({ startImmediately: true }));
        let pendingEvents = 1;

        while (pendingEvents > 0) {
          const next = yield* Effect.raceFirst(
            Fiber.join(inboundFiber).pipe(
              Effect.map((payload) => ({
                kind: "inbound" as const,
                payload,
              })),
            ),
            Queue.take(outbox).pipe(
              Effect.map((payload) => ({
                kind: "outbound" as const,
                payload,
              })),
            ),
          );

          if (next.kind === "inbound") {
            if (yield* accept(next.payload)) {
              return undefined;
            }

            pendingEvents -= 1;

            if (pendingEvents > 0) {
              inboundFiber = yield* started.take.pipe(
                Effect.forkScoped({ startImmediately: true }),
              );
            }
          } else {
            const sent = yield* sendToSession(started, next.payload);

            if (sent) {
              pendingEvents += 1;
            }
          }
        }

        return started;
      }),
    );

    if (session === undefined) {
      return yield* Effect.void;
    }

    let inboundFiber = yield* session.take.pipe(Effect.forkScoped({ startImmediately: true }));

    return yield* Effect.forever(
      Effect.gen(function* () {
        yield* project({ status: "idle" });

        const next = yield* Effect.raceFirst(
          Fiber.join(inboundFiber).pipe(
            Effect.map((payload) => ({
              kind: "inbound" as const,
              payload,
            })),
          ),
          Queue.take(outbox).pipe(
            Effect.map((payload) => ({
              kind: "outbound" as const,
              payload,
            })),
          ),
        );

        if (next.kind === "inbound") {
          yield* accept(next.payload);
          inboundFiber = yield* session.take.pipe(Effect.forkScoped({ startImmediately: true }));
          return yield* Effect.void;
        }

        yield* project({ status: "queued" });
        return yield* capacity.withPermit(
          Effect.gen(function* () {
            yield* project({ status: "running" });
            const sent = yield* sendToSession(session, next.payload);

            if (!sent) {
              return yield* Effect.void;
            }

            let pendingEvents = 1;

            while (pendingEvents > 0) {
              const turn = yield* Effect.raceFirst(
                Fiber.join(inboundFiber).pipe(
                  Effect.map((payload) => ({
                    kind: "inbound" as const,
                    payload,
                  })),
                ),
                Queue.take(outbox).pipe(
                  Effect.map((payload) => ({
                    kind: "outbound" as const,
                    payload,
                  })),
                ),
              );

              if (turn.kind === "inbound") {
                yield* accept(turn.payload);
                pendingEvents -= 1;

                if (pendingEvents > 0) {
                  inboundFiber = yield* session.take.pipe(
                    Effect.forkScoped({
                      startImmediately: true,
                    }),
                  );
                }
              } else {
                const outboundSent = yield* sendToSession(session, turn.payload);

                if (outboundSent) {
                  pendingEvents += 1;
                }
              }
            }

            inboundFiber = yield* session.take.pipe(Effect.forkScoped({ startImmediately: true }));
            return yield* Effect.void;
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
              project({
                status: "failed",
                latestEvent: failure,
              }).pipe(
                Effect.andThen(Queue.offer(events, failure)),
                Effect.andThen(
                  Deferred.succeed(result, {
                    kind: "failed",
                    reason,
                  }),
                ),
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

  const send = Effect.fn("SubagentProcess.send")(function* (content: string) {
    if (spec.mode === "ephemeral") {
      return undefined;
    }

    return yield* Effect.uninterruptible(
      Effect.gen(function* () {
        const messageId = yield* generateSubagentMessageId();
        const offered = yield* Queue.offer(outbox, { messageId, content });

        return offered ? messageId : undefined;
      }),
    );
  });

  return {
    subagentId,
    events: Stream.fromQueue(events),
    await: Deferred.await(result),
    ref: { send },
    run,
  } satisfies SubagentProcess;
});

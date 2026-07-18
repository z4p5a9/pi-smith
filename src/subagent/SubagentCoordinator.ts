import {
  Cause,
  Context,
  Effect,
  Exit,
  Fiber,
  FiberMap,
  Layer,
  Queue,
  Schema,
  Semaphore,
  Scope,
  Stream,
} from "effect";

import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import type { SubagentEvent, SubagentEventEnvelope } from "./SubagentEvent.ts";
import { generateSubagentId, SubagentId } from "./SubagentId.ts";
import { spawnSubagentProcess } from "./SubagentProcess.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

export class SubagentCoordinatorTerminalEventRejectedError extends Schema.TaggedErrorClass<SubagentCoordinatorTerminalEventRejectedError>()(
  "SubagentCoordinatorTerminalEventRejectedError",
  { subagentId: SubagentId },
) {}

const make = Effect.fn("SubagentCoordinator.make")(function* () {
  const checkpoint = yield* SubagentCheckpoint;
  const jobs = yield* Queue.unbounded<{
    readonly subagentId: SubagentId;
    readonly spec: SubagentSpec;
  }>();
  const inbox = yield* Queue.bounded<SubagentEventEnvelope>(10);
  const outbox = yield* Queue.bounded<SubagentEventEnvelope>(10);
  const coordinatorScope = yield* Scope.Scope;
  const childrenScope = yield* Scope.fork(coordinatorScope);
  const children = yield* FiberMap.make<SubagentId>();

  const routeEvent = Effect.fn("SubagentCoordinator.routeEvent")(function* ({
    event,
    subagentId,
  }: SubagentEventEnvelope) {
    yield* checkpoint.update(subagentId, {
      status: event.kind,
      latestEvent: event,
    });
    yield* Queue.offer(outbox, { subagentId, event });
  });

  yield* Effect.forever(
    Queue.take(inbox).pipe(
      Effect.flatMap(routeEvent),
      Effect.catch((error) => Effect.logError("Failed to route subagent event", error)),
    ),
  ).pipe(Effect.forkScoped({ startImmediately: true }));

  const run = Effect.fn("SubagentCoordinator.run")(function* (
    subagentId: SubagentId,
    spec: SubagentSpec,
  ) {
    const childScope = yield* Scope.fork(childrenScope);
    const terminalGate = yield* Semaphore.make(1);
    let terminalAccepted = false;

    const acceptTerminalEvent = Effect.fn("SubagentCoordinator.acceptTerminalEvent")(function* (
      event: SubagentEvent,
    ) {
      return yield* terminalGate.withPermit(
        Effect.uninterruptibleMask((restore) =>
          Effect.suspend(() => {
            if (terminalAccepted) {
              return Effect.logWarning("Rejected duplicate terminal subagent event").pipe(
                Effect.annotateLogs({ subagentId }),
                Effect.as("rejected" as const),
              );
            }

            return restore(Queue.offer(inbox, { subagentId, event })).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  terminalAccepted = true;
                }),
              ),
              Effect.as("accepted" as const),
            );
          }),
        ),
      );
    });

    const execute = Effect.gen(function* () {
      yield* checkpoint.update(subagentId, { status: "starting" });
      const process = yield* spawnSubagentProcess(subagentId, spec);

      yield* checkpoint.update(subagentId, { status: "running" });
      const pump = process.events.pipe(
        Stream.runForEach((delivery) =>
          acceptTerminalEvent(delivery.event).pipe(
            Effect.flatMap((acceptance) =>
              acceptance === "accepted"
                ? delivery.acknowledge
                : SubagentCoordinatorTerminalEventRejectedError.make({ subagentId }),
            ),
          ),
        ),
      );

      yield* Effect.all([process.await, pump], {
        concurrency: "unbounded",
        discard: true,
      });
    }).pipe(
      Scope.use(childScope),
      Effect.withSpan("SubagentCoordinator.child", { attributes: { subagentId } }),
      Effect.annotateLogs({ subagentId }),
    );

    const fiber = yield* FiberMap.run(children, subagentId, execute, {
      startImmediately: true,
    });

    yield* Fiber.join(fiber).pipe(
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause)
          ? Effect.failCause(cause)
          : acceptTerminalEvent({
              kind: "failed",
              reason: String(Cause.squash(cause)),
            }).pipe(Effect.asVoid),
      ),
    );
  });

  for (let index = 0; index < 10; index++) {
    yield* Effect.forever(
      Queue.take(jobs).pipe(Effect.flatMap(({ spec, subagentId }) => run(subagentId, spec))),
    ).pipe(Effect.forkScoped({ startImmediately: true }));
  }

  yield* Effect.addFinalizer(() =>
    Queue.shutdown(jobs).pipe(
      Effect.andThen(FiberMap.clear(children)),
      Effect.andThen(Scope.close(childrenScope, Exit.void)),
      Effect.andThen(
        Effect.all([Queue.shutdown(inbox), Queue.shutdown(outbox)], {
          discard: true,
        }),
      ),
    ),
  );

  const create = Effect.fn("SubagentCoordinator.create")(function* (spec: SubagentSpec) {
    const subagentId = yield* generateSubagentId(spec.title);

    yield* checkpoint.put({ subagentId, status: "queued", ...spec });
    yield* Queue.offer(jobs, { subagentId, spec });

    return subagentId;
  });

  return {
    create,
    events: Stream.fromQueue(outbox),
  };
});

export class SubagentCoordinator extends Context.Service<SubagentCoordinator>()(
  "@smith/subagent/SubagentCoordinator",
  { make },
) {
  static readonly layer = Layer.effect(SubagentCoordinator, SubagentCoordinator.make());
}

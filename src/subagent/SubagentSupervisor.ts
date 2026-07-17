import {
  Context,
  Effect,
  Exit,
  Fiber,
  FiberMap,
  Layer,
  PubSub,
  RcMap,
  Schema,
  Scope,
  Semaphore,
  Stream,
} from "effect";

import type {
  SubagentBridgeDisconnectedError,
  SubagentBridgeProtocolError,
} from "./SubagentBridge.ts";
import type { SubagentEvent } from "./SubagentEvent.ts";
import type { SubagentCommand } from "./SubagentHost.ts";
import { SubagentId } from "./SubagentId.ts";
import { type SubagentProcess, spawnSubagentProcess } from "./SubagentProcess.ts";
import { SubagentRegistry } from "./SubagentRegistry.ts";

export class SubagentAlreadyStartedError extends Schema.TaggedErrorClass<SubagentAlreadyStartedError>()(
  "SubagentAlreadyStartedError",
  {
    subagentId: SubagentId,
  },
) {}

const make = Effect.gen(function* () {
  const supervisorScope = yield* Scope.Scope;
  const registry = yield* SubagentRegistry;
  const eventPubSub = yield* PubSub.bounded<{
    readonly subagentId: SubagentId;
    readonly event: SubagentEvent;
  }>(1);
  const children = yield* FiberMap.make<
    SubagentId,
    void,
    SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
  >();
  const startLocks = yield* RcMap.make({
    lookup: (_subagentId: SubagentId) => Semaphore.make(1),
  });

  yield* Effect.addFinalizer(() => PubSub.shutdown(eventPubSub));

  const supervise = Effect.fn("SubagentSupervisor.supervise")(function* (
    process: SubagentProcess,
    childScope: Scope.Closeable,
  ) {
    const subagentId = process.subagentId;
    yield* Effect.annotateCurrentSpan({ subagentId });

    yield* Effect.acquireRelease(registry.register(process), () =>
      registry.unregister(subagentId),
    ).pipe(Scope.provide(childScope));

    yield* process.events.pipe(
      Stream.runForEach((delivery) =>
        Effect.gen(function* () {
          const published = yield* PubSub.publish(eventPubSub, {
            subagentId,
            event: delivery.event,
          });

          if (published) {
            yield* delivery.acknowledge;
          }
        }),
      ),
      Effect.forkScoped({ startImmediately: true }),
      Scope.provide(childScope),
    );

    const fiber = yield* FiberMap.run(
      children,
      subagentId,
      process.await.pipe(
        Effect.onExit((exit) => Scope.close(childScope, exit)),
        Effect.withSpan("SubagentSupervisor.child", {
          attributes: { subagentId },
        }),
        Effect.annotateLogs({ subagentId }),
      ),
    );

    return { await: Fiber.join(fiber) };
  });

  const start = Effect.fn("SubagentSupervisor.start")(
    function* (subagentId: SubagentId, command: SubagentCommand) {
      yield* Effect.annotateCurrentSpan({ subagentId });

      if (yield* FiberMap.has(children, subagentId)) {
        return yield* SubagentAlreadyStartedError.make({ subagentId });
      }

      const childScope = yield* Scope.fork(supervisorScope);
      const exit = yield* Effect.gen(function* () {
        const process = yield* spawnSubagentProcess(subagentId, command).pipe(
          Scope.provide(childScope),
        );

        return yield* supervise(process, childScope);
      }).pipe(Effect.interruptible, Effect.exit);

      if (Exit.isFailure(exit)) {
        yield* Scope.close(childScope, exit);
        return yield* exit;
      }

      return exit.value;
    },
    (effect, subagentId) =>
      Effect.scoped(
        RcMap.get(startLocks, subagentId).pipe(
          Effect.flatMap((startLock) => startLock.withPermit(effect)),
        ),
      ).pipe(Effect.uninterruptible),
  );

  return { start, events: Stream.fromPubSub(eventPubSub) };
});

export class SubagentSupervisor extends Context.Service<SubagentSupervisor>()(
  "@smith/subagent/SubagentSupervisor",
  { make },
) {
  static readonly layerNoDeps = Layer.effect(SubagentSupervisor, SubagentSupervisor.make);

  static readonly layer = SubagentSupervisor.layerNoDeps.pipe(
    Layer.provideMerge(SubagentRegistry.layer),
  );
}

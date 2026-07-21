import { Deferred, Effect, Ref, Semaphore } from "effect";

export const make = Effect.fn("SendWindow.make")(function* () {
  const gate = yield* Semaphore.make(1);
  const state = yield* Ref.make<{
    readonly nextSequence: number;
    readonly inFlight?: {
      readonly sequence: number;
      readonly acknowledged: Deferred.Deferred<void>;
    };
  }>({ nextSequence: 0 });

  const acknowledge = Effect.fn("SendWindow.acknowledge")(function* (sequence: number) {
    const inFlight = (yield* Ref.get(state)).inFlight;

    if (inFlight === undefined || inFlight.sequence !== sequence) {
      return false;
    }

    yield* Deferred.succeed(inFlight.acknowledged, undefined);
    return true;
  });

  const withDelivery = Effect.fn("SendWindow.withDelivery")(function* <A, E, R>(
    use: (delivery: {
      readonly sequence: number;
      readonly acknowledged: Effect.Effect<void>;
    }) => Effect.Effect<A, E, R>,
  ) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const permits = yield* restore(gate.take(1));
        const acknowledged = yield* Deferred.make<void>();
        const sequence = yield* Ref.modify(
          state,
          (current) =>
            [
              current.nextSequence,
              {
                nextSequence: current.nextSequence + 1,
                inFlight: { sequence: current.nextSequence, acknowledged },
              },
            ] as const,
        );

        return yield* restore(use({ sequence, acknowledged: Deferred.await(acknowledged) })).pipe(
          Effect.ensuring(
            Ref.update(state, (current) => ({ nextSequence: current.nextSequence })).pipe(
              Effect.andThen(gate.release(permits)),
            ),
          ),
        );
      }),
    );
  });

  return { acknowledge, withDelivery };
});

import { Effect, Ref } from "effect";

export const make = Effect.fn("ReceiveWindow.make")(function* (options: {
  readonly stopped: Effect.Effect<void>;
}) {
  const state = yield* Ref.make<
    | { readonly state: "open" }
    | {
        readonly state: "awaitingAck" | "acknowledging";
        readonly sequence: number;
        readonly token: object;
      }
  >({ state: "open" });

  const admit = Effect.fn("ReceiveWindow.admit")(function* (sequence: number) {
    const token = {};
    const previous = yield* Ref.modify(
      state,
      (current) =>
        [
          current,
          current.state === "open" ? { state: "awaitingAck" as const, sequence, token } : current,
        ] as const,
    );

    if (previous.state !== "open") {
      return yield* Effect.fail({ heldSequence: previous.sequence } as const);
    }

    const acknowledge = Effect.fn("ReceiveWindow.acknowledge")(function* <E, R>(
      attempt: Effect.Effect<void, E, R>,
    ) {
      return yield* Effect.uninterruptible(
        Effect.gen(function* () {
          const claimed = yield* Ref.modify(state, (current) =>
            current.state === "awaitingAck" && current.token === token
              ? ([true, { state: "acknowledging" as const, sequence, token }] as const)
              : ([false, current] as const),
          );

          if (!claimed) {
            return;
          }

          const written = yield* Effect.raceFirst(
            attempt.pipe(Effect.as(true as const)),
            options.stopped.pipe(Effect.as(false as const)),
          );

          if (written) {
            yield* Ref.update(state, (current) =>
              current.state === "acknowledging" && current.token === token
                ? { state: "open" as const }
                : current,
            );
          }
        }),
      );
    });

    return { acknowledge };
  });

  return { admit };
});

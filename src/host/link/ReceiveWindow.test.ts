import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber } from "effect";

import * as ReceiveWindow from "./ReceiveWindow.ts";

it.describe("ReceiveWindow", () => {
  it.effect("executes one acknowledgement attempt once across concurrent calls", () =>
    Effect.gen(function* () {
      const stopped = yield* Deferred.make<void>();
      const attemptStarted = yield* Deferred.make<void>();
      const releaseAttempt = yield* Deferred.make<void>();
      const window = yield* ReceiveWindow.make({ stopped: Deferred.await(stopped) });
      const delivery = yield* window.admit(1);
      let attempts = 0;
      const attempt = Effect.sync(() => {
        attempts += 1;
      }).pipe(
        Effect.andThen(Deferred.succeed(attemptStarted, undefined)),
        Effect.andThen(Deferred.await(releaseAttempt)),
      );

      const first = yield* delivery
        .acknowledge(attempt)
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(attemptStarted);

      const second = yield* delivery
        .acknowledge(attempt)
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Fiber.join(second);

      expect(attempts).toBe(1);

      yield* Deferred.succeed(releaseAttempt, undefined);
      yield* Fiber.join(first);
      yield* window.admit(2);
    }),
  );

  it.effect("interrupts a blocked attempt when stopped and keeps the delivery held", () =>
    Effect.gen(function* () {
      const stopped = yield* Deferred.make<void>();
      const attemptStarted = yield* Deferred.make<void>();
      const attemptInterrupted = yield* Deferred.make<void>();
      const window = yield* ReceiveWindow.make({ stopped: Deferred.await(stopped) });
      const delivery = yield* window.admit(1);
      const acknowledging = yield* delivery
        .acknowledge(
          Deferred.succeed(attemptStarted, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() => Deferred.succeed(attemptInterrupted, undefined)),
          ),
        )
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(attemptStarted);
      yield* Deferred.succeed(stopped, undefined);
      yield* Fiber.join(acknowledging);
      yield* Deferred.await(attemptInterrupted);

      const occupied = yield* window.admit(2).pipe(Effect.flip);

      expect(occupied).toEqual({ heldSequence: 1 });
    }),
  );

  it.effect("propagates acknowledgement failure and keeps the delivery held", () =>
    Effect.gen(function* () {
      const stopped = yield* Deferred.make<void>();
      const window = yield* ReceiveWindow.make({ stopped: Deferred.await(stopped) });
      const delivery = yield* window.admit(1);
      const failure = { kind: "attempt-failed" as const };

      const error = yield* delivery.acknowledge(Effect.fail(failure)).pipe(Effect.flip);
      const occupied = yield* window.admit(2).pipe(Effect.flip);

      expect(error).toEqual(failure);
      expect(occupied).toEqual({ heldSequence: 1 });
    }),
  );
});

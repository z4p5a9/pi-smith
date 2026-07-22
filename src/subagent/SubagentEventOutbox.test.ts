import { expect, it } from "@effect/vitest";
import { Effect, Stream } from "effect";

import { decodeSubagentId } from "./SubagentId.ts";
import { SubagentEventOutbox } from "./SubagentEventOutbox.ts";

it.describe("SubagentEventOutbox", () => {
  it.effect("publishes envelopes FIFO to the single consumer", () =>
    Effect.gen(function* () {
      const eventOutbox = yield* SubagentEventOutbox;
      const firstSubagentId = yield* decodeSubagentId("sa_12345678_first");
      const secondSubagentId = yield* decodeSubagentId("sa_12345678_second");

      yield* eventOutbox.publish({
        subagentId: firstSubagentId,
        event: { kind: "message", content: "First." },
      });
      yield* eventOutbox.publish({
        subagentId: secondSubagentId,
        event: { kind: "failure", reason: "Second." },
      });

      expect(yield* eventOutbox.events.pipe(Stream.take(2), Stream.runCollect)).toEqual([
        {
          subagentId: firstSubagentId,
          event: { kind: "message", content: "First." },
        },
        {
          subagentId: secondSubagentId,
          event: { kind: "failure", reason: "Second." },
        },
      ]);
    }).pipe(Effect.scoped, Effect.provide(SubagentEventOutbox.layer)),
  );
});

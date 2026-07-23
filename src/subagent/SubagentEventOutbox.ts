import { Context, Effect, Layer, Queue, Stream } from "effect";

import type { SubagentEventEnvelope } from "./SubagentEvent.ts";

export class SubagentEventOutbox extends Context.Service<SubagentEventOutbox>()(
  "@smith/subagent/SubagentEventOutbox",
  {
    make: Effect.fn("SubagentEventOutbox.make")(function* () {
      const events = yield* Queue.unbounded<SubagentEventEnvelope>();

      yield* Effect.addFinalizer(() => Queue.shutdown(events));

      const publish = Effect.fn("SubagentEventOutbox.publish")(function* (
        envelope: SubagentEventEnvelope,
      ) {
        yield* Queue.offer(events, envelope);
      });

      return {
        publish,
        events: Stream.fromQueue(events),
      };
    }),
  },
) {
  static readonly layer = Layer.effect(SubagentEventOutbox, SubagentEventOutbox.make());
}

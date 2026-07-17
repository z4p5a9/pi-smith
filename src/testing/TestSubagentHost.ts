import { Context, Effect, Layer, Queue, Ref } from "effect";

import {
  SubagentHost,
  type SubagentCommand,
  type SubagentHostResponseError,
  type SubagentHostStartError,
  type SubagentHostUnavailableError,
} from "../subagent/SubagentHost.ts";
import type { SubagentId } from "../subagent/SubagentId.ts";

const make = Effect.gen(function* () {
  const startCalls = yield* Queue.unbounded<{
    readonly subagentId: SubagentId;
    readonly command: SubagentCommand;
  }>();
  const starts = yield* Queue.unbounded<SubagentId>();
  const state = yield* Ref.make<{
    readonly stubs: Array<
      null | SubagentHostUnavailableError | SubagentHostStartError | SubagentHostResponseError
    >;
    readonly calls: Array<{
      readonly subagentId: SubagentId;
      readonly command: SubagentCommand;
    }>;
    readonly active: Set<SubagentId>;
  }>({
    stubs: [],
    calls: [],
    active: new Set(),
  });

  const start = Effect.fn("TestSubagentHost.start")(function* (
    subagentId: SubagentId,
    command: SubagentCommand,
  ) {
    const configured = yield* Ref.modify(state, (prev) => {
      const [stub, ...stubs] = prev.stubs;
      const next = {
        ...prev,
        stubs,
        calls: [...prev.calls, { subagentId, command }],
      };

      return [stub, next] as const;
    });

    yield* Queue.offer(startCalls, { subagentId, command });

    if (configured === undefined) {
      return yield* Effect.die("Unexpected subagent host start");
    }

    if (configured !== null) {
      return yield* configured;
    }

    yield* Effect.acquireRelease(
      Ref.update(state, (prev) => {
        const active = new Set(prev.active);
        active.add(subagentId);
        const next = { ...prev, active };

        return next;
      }),
      () =>
        Ref.update(state, (prev) => {
          const active = new Set(prev.active);
          active.delete(subagentId);
          const next = { ...prev, active };

          return next;
        }),
    );

    yield* Queue.offer(starts, subagentId);

    return yield* Effect.void;
  });

  const stub = Effect.fn("TestSubagentHost.stub")(function* (
    stubs: ReadonlyArray<
      null | SubagentHostUnavailableError | SubagentHostStartError | SubagentHostResponseError
    >,
  ) {
    yield* Ref.update(state, (prev) => {
      const next = { ...prev, stubs: [...prev.stubs, ...stubs] };

      return next;
    });
  });

  const calls = Effect.gen(function* () {
    const ref = yield* Ref.get(state);

    return [...ref.calls];
  });

  const active = Effect.gen(function* () {
    const ref = yield* Ref.get(state);

    return [...ref.active];
  });

  const verify = Effect.gen(function* () {
    const ref = yield* Ref.get(state);

    if (ref.stubs.length > 0) {
      return yield* Effect.die(`${ref.stubs.length} subagent host stubs were unused`);
    }

    return yield* Effect.void;
  });

  return {
    start,
    stub,
    calls,
    active,
    takeStartCall: Queue.take(startCalls),
    takeStart: Queue.take(starts),
    verify,
  };
});

export class TestSubagentHost extends Context.Service<TestSubagentHost>()(
  "@smith/testing/TestSubagentHost",
  { make },
) {
  static readonly layerNoDeps = Layer.effect(TestSubagentHost, TestSubagentHost.make);

  static readonly layer = Layer.effect(SubagentHost, TestSubagentHost).pipe(
    Layer.provideMerge(TestSubagentHost.layerNoDeps),
  );
}

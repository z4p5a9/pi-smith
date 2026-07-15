import { Context, Effect, Layer, Ref } from "effect";

import { SubagentBridge } from "../subagent/SubagentBridge.ts";
import {
  SubagentHost,
  type SubagentCommand,
  type SubagentHostHandle,
  type SubagentHostResponseError,
  type SubagentHostStartError,
  type SubagentHostUnavailableError,
} from "../subagent/SubagentHost.ts";
import type { SubagentId } from "../subagent/SubagentId.ts";
import type { SubagentSpec } from "../subagent/SubagentSpec.ts";

const make = Effect.gen(function* () {
  const bridge = yield* SubagentBridge;
  const state = yield* Ref.make<{
    readonly stubs: Array<
      | { readonly hostId: string }
      | {
          readonly error:
            | SubagentHostUnavailableError
            | SubagentHostStartError
            | SubagentHostResponseError;
        }
    >;
    readonly calls: Array<{
      readonly subagentId: SubagentId;
      readonly spec: SubagentSpec;
      readonly command: SubagentCommand;
    }>;
    readonly active: Map<SubagentId, SubagentHostHandle>;
  }>({
    stubs: [],
    calls: [],
    active: new Map(),
  });

  const start = Effect.fn("TestSubagentHost.start")(function* (
    subagentId: SubagentId,
    spec: SubagentSpec,
    command: SubagentCommand,
  ) {
    const configured = yield* Ref.modify(state, (prev) => {
      const [stub, ...stubs] = prev.stubs;
      const next = {
        ...prev,
        stubs,
        calls: [...prev.calls, { subagentId, spec, command }],
      };

      return [stub, next] as const;
    });

    if (configured === undefined) {
      return yield* Effect.die("Unexpected subagent host start");
    }

    if ("error" in configured) {
      return yield* configured.error;
    }

    const handle = yield* Effect.acquireRelease(
      Ref.update(state, (prev) => {
        const active = new Map(prev.active);
        active.set(subagentId, configured);
        const next = { ...prev, active };

        return next;
      }).pipe(Effect.as(configured)),
      (acquired) =>
        Ref.update(state, (prev) => {
          if (prev.active.get(subagentId) !== acquired) {
            return prev;
          }

          const active = new Map(prev.active);
          active.delete(subagentId);
          const next = { ...prev, active };

          return next;
        }),
    );

    yield* bridge.connect(subagentId).pipe(Effect.forkScoped({ startImmediately: true }));

    return handle;
  });

  const stub = Effect.fn("TestSubagentHost.stub")(function* (
    stubs: ReadonlyArray<
      | { readonly hostId: string }
      | {
          readonly error:
            | SubagentHostUnavailableError
            | SubagentHostStartError
            | SubagentHostResponseError;
        }
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

    return [...ref.active.values()];
  });

  const verify = Effect.gen(function* () {
    const ref = yield* Ref.get(state);

    if (ref.stubs.length > 0) {
      return yield* Effect.die(`${ref.stubs.length} subagent host stubs were unused`);
    }

    return yield* Effect.void;
  });

  return { start, stub, calls, active, verify };
});

export class TestSubagentHost extends Context.Service<TestSubagentHost>()(
  "@smith/testing/TestSubagentHost",
  { make },
) {
  static readonly layer = Layer.effect(SubagentHost, TestSubagentHost).pipe(
    Layer.provideMerge(Layer.effect(TestSubagentHost, TestSubagentHost.make)),
  );
}

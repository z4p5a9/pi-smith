import { Context, Effect, Layer, Queue, Ref } from "effect";

import { SubagentLinkTransport } from "../host/link/Transport.ts";
import * as UnixSocketTransport from "../host/link/unix/UnixSocketTransport.ts";
import * as Protocol from "../host/Protocol.ts";
import {
  SubagentHost,
  type SubagentCommand,
  type SubagentHostResponseError,
  type SubagentHostStartError,
  type SubagentHostUnavailableError,
} from "../host/Host.ts";
import type { SubagentId } from "../subagent/SubagentId.ts";

export class TestHost extends Context.Service<TestHost>()("@smith/testing/TestHost", {
  make: Effect.gen(function* () {
    const transport = yield* SubagentLinkTransport;
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

    const start = Effect.fn("TestHost.start")(function* (
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

      const listener = yield* Protocol.listen(subagentId).pipe(
        Effect.provideService(SubagentLinkTransport, transport),
        Effect.orDie,
      );

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

      return yield* listener.accept;
    });

    const stub = Effect.fn("TestHost.stub")(function* (
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
  }).pipe(Effect.withSpan("TestHost.make")),
}) {
  static readonly layerNoDeps = Layer.effect(TestHost, TestHost.make);

  static readonly layer = Layer.effect(SubagentHost, TestHost).pipe(
    Layer.provideMerge(TestHost.layerNoDeps),
    Layer.provide(UnixSocketTransport.layer),
  );
}

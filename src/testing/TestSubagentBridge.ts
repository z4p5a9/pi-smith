import { Context, Deferred, Effect, Layer, Ref } from "effect";

import {
  SubagentBridge,
  SubagentBridgeConnectError,
  SubagentBridgeDisconnectedError,
  SubagentBridgeListenError,
  type SubagentBridgeSession,
} from "../subagent/SubagentBridge.ts";
import type { SubagentId } from "../subagent/SubagentId.ts";

const make = Effect.gen(function* () {
  const state = yield* Ref.make<{
    readonly listeners: Map<
      SubagentId,
      {
        readonly accepted: Deferred.Deferred<SubagentBridgeSession>;
        readonly connected: boolean;
        readonly closed?: Deferred.Deferred<void>;
      }
    >;
    readonly blocked: Map<SubagentId, Deferred.Deferred<void>>;
    readonly calls: Array<
      | { readonly operation: "listen"; readonly subagentId: SubagentId }
      | { readonly operation: "connect"; readonly subagentId: SubagentId }
    >;
  }>({
    listeners: new Map(),
    blocked: new Map(),
    calls: [],
  });

  const listen = Effect.fn("TestSubagentBridge.listen")(function* (subagentId: SubagentId) {
    const accepted = yield* Deferred.make<SubagentBridgeSession>();

    yield* Ref.modify(state, (prev) => {
      if (prev.listeners.has(subagentId)) {
        const next = {
          ...prev,
          calls: [...prev.calls, { operation: "listen" as const, subagentId }],
        };

        return [false, next] as const;
      }

      const listeners = new Map(prev.listeners);
      listeners.set(subagentId, { accepted, connected: false });
      const next = {
        ...prev,
        listeners,
        calls: [...prev.calls, { operation: "listen" as const, subagentId }],
      };

      return [true, next] as const;
    }).pipe(
      Effect.flatMap((registered) =>
        registered
          ? Effect.void
          : SubagentBridgeListenError.make({
              subagentId,
              reason: "A bridge listener already exists",
            }),
      ),
    );

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        const closed = yield* Ref.modify(state, (prev) => {
          const listener = prev.listeners.get(subagentId);

          if (listener?.accepted !== accepted) {
            return [undefined, prev] as const;
          }

          const listeners = new Map(prev.listeners);
          listeners.delete(subagentId);
          const next = { ...prev, listeners };

          return [listener.closed, next] as const;
        });

        if (closed !== undefined) {
          yield* Deferred.succeed(closed, undefined);
        }
      }),
    );

    return { accept: Deferred.await(accepted) };
  });

  const connect = Effect.fn("TestSubagentBridge.connect")(function* (subagentId: SubagentId) {
    const blocked = yield* Ref.modify(state, (prev) => {
      const next = {
        ...prev,
        calls: [...prev.calls, { operation: "connect" as const, subagentId }],
      };

      return [prev.blocked.get(subagentId), next] as const;
    });

    if (blocked !== undefined) {
      yield* Deferred.await(blocked);
    }

    const closed = yield* Deferred.make<void>();
    const accepted = yield* Ref.modify(state, (prev) => {
      const listener = prev.listeners.get(subagentId);

      if (listener === undefined || listener.connected) {
        return [undefined, prev] as const;
      }

      const listeners = new Map(prev.listeners);
      listeners.set(subagentId, { ...listener, connected: true, closed });
      const next = { ...prev, listeners };

      return [listener.accepted, next] as const;
    });

    if (accepted === undefined) {
      return yield* SubagentBridgeConnectError.make({
        subagentId,
        reason: "No available bridge listener exists",
      });
    }

    const rootSession = {
      await: Deferred.await(closed).pipe(
        Effect.andThen(
          SubagentBridgeDisconnectedError.make({
            subagentId,
            reason: "Bridge connection closed",
          }),
        ),
      ),
    } satisfies SubagentBridgeSession;
    const childSession = {
      await: Deferred.await(closed).pipe(
        Effect.andThen(
          SubagentBridgeDisconnectedError.make({
            subagentId,
            reason: "Bridge connection closed",
          }),
        ),
      ),
    } satisfies SubagentBridgeSession;

    if (!(yield* Deferred.succeed(accepted, rootSession))) {
      yield* Deferred.succeed(closed, undefined);

      return yield* SubagentBridgeConnectError.make({
        subagentId,
        reason: "Bridge listener closed before the connection was accepted",
      });
    }

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        yield* Ref.update(state, (prev) => {
          const listener = prev.listeners.get(subagentId);

          if (listener?.closed !== closed) {
            return prev;
          }

          const listeners = new Map(prev.listeners);
          listeners.set(subagentId, {
            accepted: listener.accepted,
            connected: listener.connected,
          });
          const next = { ...prev, listeners };

          return next;
        });
        yield* Deferred.succeed(closed, undefined);
      }),
    );

    return childSession;
  });

  const block = Effect.fn("TestSubagentBridge.block")(function* (subagentId: SubagentId) {
    const blocked = yield* Deferred.make<void>();

    yield* Ref.update(state, (prev) => {
      if (prev.blocked.has(subagentId)) {
        return prev;
      }

      const nextBlocked = new Map(prev.blocked);
      nextBlocked.set(subagentId, blocked);
      const next = { ...prev, blocked: nextBlocked };

      return next;
    });
  });

  const unblock = Effect.fn("TestSubagentBridge.unblock")(function* (subagentId: SubagentId) {
    const blocked = yield* Ref.modify(state, (prev) => {
      const nextBlocked = new Map(prev.blocked);
      nextBlocked.delete(subagentId);
      const next = { ...prev, blocked: nextBlocked };

      return [prev.blocked.get(subagentId), next] as const;
    });

    if (blocked !== undefined) {
      yield* Deferred.succeed(blocked, undefined);
    }
  });

  const disconnect = Effect.fn("TestSubagentBridge.disconnect")(function* (subagentId: SubagentId) {
    const closed = yield* Ref.modify(state, (prev) => {
      const listener = prev.listeners.get(subagentId);

      if (listener?.closed === undefined) {
        return [undefined, prev] as const;
      }

      const listeners = new Map(prev.listeners);
      listeners.set(subagentId, {
        accepted: listener.accepted,
        connected: listener.connected,
      });
      const next = { ...prev, listeners };

      return [listener.closed, next] as const;
    });

    if (closed !== undefined) {
      yield* Deferred.succeed(closed, undefined);
    }
  });

  const isListening = Effect.fn("TestSubagentBridge.isListening")(function* (
    subagentId: SubagentId,
  ) {
    const ref = yield* Ref.get(state);

    return ref.listeners.has(subagentId);
  });

  const isConnected = Effect.fn("TestSubagentBridge.isConnected")(function* (
    subagentId: SubagentId,
  ) {
    const ref = yield* Ref.get(state);

    return ref.listeners.get(subagentId)?.closed !== undefined;
  });

  const calls = Effect.gen(function* () {
    const ref = yield* Ref.get(state);

    return [...ref.calls];
  });

  return {
    listen,
    connect,
    block,
    unblock,
    disconnect,
    isListening,
    isConnected,
    calls,
  };
});

export class TestSubagentBridge extends Context.Service<TestSubagentBridge>()(
  "@smith/testing/TestSubagentBridge",
  { make },
) {
  static readonly layer = Layer.effect(SubagentBridge, TestSubagentBridge).pipe(
    Layer.provideMerge(Layer.effect(TestSubagentBridge, TestSubagentBridge.make)),
  );
}

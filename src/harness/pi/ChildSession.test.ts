import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Stream } from "effect";
import * as Socket from "effect/unstable/socket/Socket";

import { SubagentLinkConnectError, SubagentLinkTransport } from "../../host/link/Transport.ts";
import * as UnixSocketTransport from "../../host/link/unix/UnixSocketTransport.ts";
import * as Protocol from "../../host/Protocol.ts";
import { decodeSubagentId } from "../../subagent/SubagentId.ts";
import { ChildSession } from "./ChildSession.ts";

it.effect("constructs without connecting the link", () =>
  Effect.gen(function* () {
    const subagentId = yield* decodeSubagentId("sa_12345678_child-session");

    yield* Effect.void.pipe(
      Effect.provide(
        ChildSession.layer(subagentId).pipe(
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    );
  }),
);

it.effect("starts one connection across concurrent calls", () =>
  Effect.gen(function* () {
    const subagentId = yield* decodeSubagentId("sa_12345678_child-session-concurrent-start");
    const transport = yield* SubagentLinkTransport;
    const connectStarted = yield* Deferred.make<void>();
    const releaseConnect = yield* Deferred.make<void>();
    let connectCount = 0;
    const listener = yield* Protocol.listen(subagentId);

    yield* Effect.gen(function* () {
      const session = yield* ChildSession;
      const first = yield* session.start.pipe(Effect.forkChild({ startImmediately: true }));

      yield* Deferred.await(connectStarted);

      const second = yield* session.start.pipe(Effect.forkChild({ startImmediately: true }));

      expect(connectCount).toBe(1);

      yield* Deferred.succeed(releaseConnect, undefined);
      yield* listener.accept;
      yield* Fiber.join(first);
      yield* Fiber.join(second);

      expect(connectCount).toBe(1);
    }).pipe(
      Effect.provide(
        ChildSession.layer(subagentId).pipe(
          Layer.provide(
            Layer.succeed(
              SubagentLinkTransport,
              SubagentLinkTransport.of({
                listen: transport.listen,
                connect: (connectingSubagentId) =>
                  Effect.gen(function* () {
                    connectCount += 1;
                    yield* Deferred.succeed(connectStarted, undefined);
                    yield* Deferred.await(releaseConnect);
                    return yield* transport.connect(connectingSubagentId);
                  }),
              }),
            ),
          ),
        ),
      ),
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
  ),
);

it.effect("retries start after connection failure", () =>
  Effect.gen(function* () {
    const subagentId = yield* decodeSubagentId("sa_12345678_child-session-retry-start");
    const transport = yield* SubagentLinkTransport;
    let connectCount = 0;
    const listener = yield* Protocol.listen(subagentId);

    yield* Effect.gen(function* () {
      const session = yield* ChildSession;
      const error = yield* session.start.pipe(Effect.flip);

      expect(error).toEqual(
        SubagentLinkConnectError.make({
          subagentId,
          reason: "Test connection failure",
        }),
      );

      const retry = yield* session.start.pipe(Effect.forkChild({ startImmediately: true }));

      yield* listener.accept;
      yield* Fiber.join(retry);

      expect(connectCount).toBe(2);
    }).pipe(
      Effect.provide(
        ChildSession.layer(subagentId).pipe(
          Layer.provide(
            Layer.succeed(
              SubagentLinkTransport,
              SubagentLinkTransport.of({
                listen: transport.listen,
                connect: (connectingSubagentId) =>
                  Effect.gen(function* () {
                    connectCount += 1;

                    if (connectCount === 1) {
                      return yield* SubagentLinkConnectError.make({
                        subagentId: connectingSubagentId,
                        reason: "Test connection failure",
                      });
                    }

                    return yield* transport.connect(connectingSubagentId);
                  }),
              }),
            ),
          ),
        ),
      ),
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
  ),
);

it.effect("publishes the session when start is interrupted after root acceptance", () =>
  Effect.gen(function* () {
    const subagentId = yield* decodeSubagentId("sa_12345678_child-session-interrupted-start");
    const transport = yield* SubagentLinkTransport;
    const ackReadStarted = yield* Deferred.make<void>();
    const releaseAckRead = yield* Deferred.make<void>();
    let connectCount = 0;
    const listener = yield* Protocol.listen(subagentId);

    yield* Effect.gen(function* () {
      const session = yield* ChildSession;
      const starting = yield* session.start.pipe(Effect.forkChild({ startImmediately: true }));
      const root = yield* listener.accept;

      yield* Deferred.await(ackReadStarted);

      const interrupting = yield* Fiber.interrupt(starting).pipe(
        Effect.forkChild({ startImmediately: true }),
      );

      expect(interrupting.pollUnsafe()).toBeUndefined();
      expect(starting.pollUnsafe()).toBeUndefined();

      yield* Deferred.succeed(releaseAckRead, undefined);
      yield* Fiber.join(interrupting);

      const exit = yield* Fiber.await(starting);

      expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true);

      const sending = yield* root
        .send("Session survived interruption.")
        .pipe(Effect.forkChild({ startImmediately: true }));

      expect(yield* session.inbox.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption))).toEqual({
        kind: "message",
        content: "Session survived interruption.",
      });

      yield* Fiber.join(sending);

      expect(connectCount).toBe(1);
    }).pipe(
      Effect.provide(
        ChildSession.layer(subagentId).pipe(
          Layer.provide(
            Layer.succeed(
              SubagentLinkTransport,
              SubagentLinkTransport.of({
                listen: transport.listen,
                connect: (connectingSubagentId) =>
                  Effect.gen(function* () {
                    connectCount += 1;
                    const socket = yield* transport.connect(connectingSubagentId);

                    return Socket.make({
                      runRaw: (handler, options) =>
                        socket.runRaw(
                          (data) =>
                            Effect.gen(function* () {
                              yield* Deferred.succeed(ackReadStarted, undefined);
                              yield* Deferred.await(releaseAckRead);

                              const handled = handler(data);

                              if (Effect.isEffect(handled)) {
                                yield* handled;
                              }
                            }),
                          options,
                        ),
                      writer: socket.writer,
                    });
                  }),
              }),
            ),
          ),
        ),
      ),
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(UnixSocketTransport.layer.pipe(Layer.provide(NodeFileSystem.layer))),
  ),
);

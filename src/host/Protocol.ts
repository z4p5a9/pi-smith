import { Deferred, Effect, Option, Ref, Stream } from "effect";
import type * as Socket from "effect/unstable/socket/Socket";

import type { SubagentHostSession } from "./Host.ts";
import * as Link from "./link/Link.ts";
import { SubagentLinkTransport } from "./link/Transport.ts";
import type { SubagentEvent } from "../subagent/SubagentEvent.ts";
import type { SubagentId } from "../subagent/SubagentId.ts";

export interface SubagentListener {
  readonly accept: Effect.Effect<SubagentHostSession>;
}

export interface SubagentChildSession {
  readonly send: (event: SubagentEvent) => Effect.Effect<void, Link.LinkDisconnectedError>;
  readonly inbox: Stream.Stream<SubagentEvent>;
  readonly await: Effect.Effect<void, Link.LinkDisconnectedError | Link.LinkProtocolError>;
}

export const listen = Effect.fn("SubagentProtocol.listen")(function* (subagentId: SubagentId) {
  yield* Effect.annotateCurrentSpan({ subagentId });

  const transport = yield* SubagentLinkTransport;
  const server = yield* transport.listen(subagentId);
  const accepted = yield* Deferred.make<SubagentHostSession>();

  const serve = Effect.fn("SubagentProtocol.serve")(
    function* (socket: Socket.Socket) {
      const link = yield* Link.make(socket, subagentId);

      // Establishment is the first valid datagram: identity is proven per
      // frame by the link, so any well-formed first frame surfaces the session.
      const first = yield* link.recv;
      const buffered = yield* Ref.make(
        first.data.kind === "hello"
          ? Option.none<{ readonly event: SubagentEvent; readonly ack: Effect.Effect<void> }>()
          : Option.some({ event: first.data, ack: first.ack }),
      );

      const take = Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const held = yield* Ref.getAndSet(buffered, Option.none());

          if (Option.isSome(held)) {
            yield* held.value.ack;
            return held.value.event;
          }

          let event: SubagentEvent | undefined;

          while (event === undefined) {
            const { ack, data } = yield* restore(link.recv);
            yield* ack;

            if (data.kind === "hello") {
              yield* Effect.logDebug("Dropped duplicate subagent hello").pipe(
                Effect.annotateLogs({ subagentId }),
              );
            } else {
              event = data;
            }
          }

          return event;
        }),
      );

      const session = {
        take,
        send: (content: string) => link.send({ kind: "message", content }),
        await: link.closed,
      } satisfies SubagentHostSession;

      if (!(yield* Deferred.succeed(accepted, session))) {
        return yield* Effect.logDebug("Closed extra subagent link connection").pipe(
          Effect.annotateLogs({ subagentId }),
        );
      }

      if (first.data.kind === "hello") {
        yield* first.ack;
      }

      // Session failures surface through the session itself; holding the scope
      // open here is what keeps the accepted connection alive.
      return yield* link.closed.pipe(Effect.exit, Effect.asVoid);
    },
    (effect) =>
      effect.pipe(
        Effect.catch((error) =>
          Effect.logWarning("Rejected subagent link connection", error).pipe(
            Effect.annotateLogs({ subagentId }),
          ),
        ),
        Effect.scoped,
      ),
  );

  yield* server.run(serve).pipe(Effect.forkScoped);

  return { accept: Deferred.await(accepted) } satisfies SubagentListener;
});

export const connect = Effect.fn("SubagentProtocol.connect")(function* (subagentId: SubagentId) {
  yield* Effect.annotateCurrentSpan({ subagentId });

  const transport = yield* SubagentLinkTransport;
  const socket = yield* transport.connect(subagentId);
  const link = yield* Link.make(socket, subagentId);

  // The hello's acknowledgement doubles as liveness: the root took the frame.
  yield* link.send({ kind: "hello" });

  const inbox = Stream.fromEffectRepeat(
    Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        let datagram: SubagentEvent | undefined;

        while (datagram === undefined) {
          const { ack, data } = yield* restore(link.recv);
          yield* ack;

          if (data.kind === "hello") {
            yield* Effect.logDebug("Dropped root hello datagram").pipe(
              Effect.annotateLogs({ subagentId }),
            );
          } else {
            datagram = data;
          }
        }

        return datagram;
      }),
    ),
  ).pipe(Stream.catchCause(() => Stream.empty));

  return {
    send: (event: SubagentEvent) => link.send(event),
    inbox,
    await: link.closed,
  } satisfies SubagentChildSession;
});

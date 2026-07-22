import type { ExtensionContext, SessionEntry } from "@earendil-works/pi-coding-agent";
import { Context, Effect, Layer, Option, Ref, Scope, Semaphore, Stream } from "effect";

import { SubagentLinkTransport } from "../../../host/link/Transport.ts";
import * as Protocol from "../../../host/Protocol.ts";
import type { SubagentId } from "../../../subagent/SubagentId.ts";

const make = Effect.fn("PiSubagentSession.make")(function* (subagentId: SubagentId) {
  const scope = yield* Scope.Scope;
  const transport = yield* SubagentLinkTransport;
  const session = yield* Ref.make(Option.none<Protocol.SubagentChildSession>());
  const startPermit = yield* Semaphore.make(1);

  const start = startPermit
    .withPermit(
      Effect.uninterruptible(
        Effect.gen(function* () {
          if (Option.isSome(yield* Ref.get(session))) {
            return yield* Effect.void;
          }

          const connected = yield* Protocol.connect(subagentId).pipe(
            Effect.provideService(SubagentLinkTransport, transport),
            Scope.provide(scope),
          );

          return yield* Ref.set(session, Option.some(connected));
        }),
      ),
    )
    .pipe(Effect.withSpan("PiSubagentSession.start"));

  const sendSettled = Effect.fn("PiSubagentSession.sendSettled")(function* (
    sessionManager: ExtensionContext["sessionManager"],
  ) {
    const current = yield* Ref.get(session);

    if (Option.isNone(current)) {
      return yield* Effect.die("Pi child session has not started");
    }

    const branch = sessionManager.getBranch();
    let entry: SessionEntry | undefined;

    for (let index = branch.length - 1; index >= 0; index--) {
      const currentEntry = branch[index];

      if (currentEntry?.type === "message" && currentEntry.message.role === "assistant") {
        entry = currentEntry;
        break;
      }
    }

    if (entry === undefined || entry.type !== "message" || entry.message.role !== "assistant") {
      return yield* current.value.send({
        kind: "failure",
        reason: "Pi settled without an assistant response",
      });
    }

    if (entry.message.stopReason === "error" || entry.message.stopReason === "aborted") {
      return yield* current.value.send({
        kind: "failure",
        reason: entry.message.errorMessage ?? `Request ${entry.message.stopReason}`,
      });
    }

    const content: Array<string> = [];

    for (const block of entry.message.content) {
      if (block.type === "text") {
        content.push(block.text);
      }
    }

    return yield* current.value.send({ kind: "message", content: content.join("\n") });
  });

  return {
    start,
    sendSettled,
    inbox: Stream.unwrap(
      Ref.get(session).pipe(
        Effect.map((current) => (Option.isSome(current) ? current.value.inbox : Stream.empty)),
      ),
    ),
    await: Ref.get(session).pipe(
      Effect.flatMap((current) => (Option.isSome(current) ? current.value.await : Effect.void)),
    ),
  };
});

export class PiSubagentSession extends Context.Service<PiSubagentSession>()(
  "@smith/harness/pi/extension/PiSubagentSession",
  { make },
) {
  static readonly layer = (subagentId: SubagentId) =>
    Layer.effect(PiSubagentSession, PiSubagentSession.make(subagentId));
}

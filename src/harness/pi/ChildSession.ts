import type { ExtensionContext, SessionEntry } from "@earendil-works/pi-coding-agent";
import { Context, Effect, Layer, Scope, Stream } from "effect";

import { SubagentBridge, type SubagentBridgeChildSession } from "../../host/bridge/Bridge.ts";
import type { SubagentId } from "../../subagent/SubagentId.ts";

const make = Effect.fn("PiChildSession.make")(function* (subagentId: SubagentId) {
  const scope = yield* Scope.Scope;
  const bridge = yield* SubagentBridge;
  let session: SubagentBridgeChildSession | undefined;

  const start = Effect.gen(function* () {
    if (session !== undefined) {
      return yield* Effect.void;
    }

    session = yield* bridge.connect(subagentId).pipe(Scope.provide(scope));
    return yield* Effect.void;
  }).pipe(Effect.withSpan("PiChildSession.start"));

  const sendSettled = Effect.fn("PiChildSession.sendSettled")(function* (
    sessionManager: ExtensionContext["sessionManager"],
  ) {
    if (session === undefined) {
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
      return yield* session.sendEvent({
        kind: "failure",
        reason: "Pi settled without an assistant response",
      });
    }

    if (entry.message.stopReason === "error" || entry.message.stopReason === "aborted") {
      return yield* session.sendEvent({
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

    return yield* session.sendEvent({ kind: "message", content: content.join("\n") });
  });

  return {
    start,
    sendSettled,
    messages: Stream.unwrap(
      Effect.sync(() => (session === undefined ? Stream.empty : session.messages)),
    ),
    await: Effect.suspend(() => (session === undefined ? Effect.void : session.await)),
  };
});

export class ChildSession extends Context.Service<ChildSession>()(
  "@smith/harness/pi/ChildSession",
  { make },
) {
  static readonly layer = (subagentId: SubagentId) =>
    Layer.effect(ChildSession, ChildSession.make(subagentId));
}

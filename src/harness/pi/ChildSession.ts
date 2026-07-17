import type { ExtensionContext, SessionEntry } from "@earendil-works/pi-coding-agent";
import { Context, Effect, Layer, Scope } from "effect";

import * as SubagentBridge from "../../subagent/SubagentBridge.ts";
import type { SubagentBridgeChildSession } from "../../subagent/SubagentBridge.ts";
import { SubagentBridgeTransport } from "../../subagent/SubagentBridgeTransport.ts";
import type { SubagentId } from "../../subagent/SubagentId.ts";

const make = Effect.fn("PiChildSession.make")(function* (subagentId: SubagentId) {
  const scope = yield* Scope.Scope;
  const transport = yield* SubagentBridgeTransport;
  let session: SubagentBridgeChildSession | undefined;

  const start = Effect.gen(function* () {
    if (session !== undefined) {
      return yield* Effect.void;
    }

    session = yield* SubagentBridge.connect(subagentId).pipe(
      Effect.provideService(SubagentBridgeTransport, transport),
      Scope.provide(scope),
    );
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
        kind: "failed",
        reason: "Pi settled without an assistant response",
      });
    }

    if (entry.message.stopReason === "error" || entry.message.stopReason === "aborted") {
      return yield* session.sendEvent({
        kind: "failed",
        reason: entry.message.errorMessage ?? `Request ${entry.message.stopReason}`,
      });
    }

    const content: Array<string> = [];

    for (const block of entry.message.content) {
      if (block.type === "text") {
        content.push(block.text);
      }
    }

    return yield* session.sendEvent({ kind: "completed", report: content.join("\n") });
  });

  const close = Effect.suspend(() => (session === undefined ? Effect.void : session.close));

  return { start, sendSettled, close };
});

export class ChildSession extends Context.Service<ChildSession>()(
  "@smith/harness/pi/ChildSession",
  { make },
) {
  static readonly layer = (subagentId: SubagentId) =>
    Layer.effect(ChildSession, ChildSession.make(subagentId));
}

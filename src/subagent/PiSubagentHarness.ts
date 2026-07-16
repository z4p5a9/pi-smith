import { fileURLToPath } from "node:url";

import { Context, Effect, Layer, Schema } from "effect";

import { SubagentBridge } from "./SubagentBridge.ts";
import type { SubagentCommand } from "./SubagentHost.ts";
import type { SubagentId } from "./SubagentId.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

export class PiSubagentEntrypointUnavailableError extends Schema.TaggedErrorClass<PiSubagentEntrypointUnavailableError>()(
  "PiSubagentEntrypointUnavailableError",
  {},
) {}

const make = Effect.fn("PiSubagentHarness.make")(function* (subagentId: SubagentId) {
  const bridge = yield* SubagentBridge;
  const session = yield* bridge.connect(subagentId);

  yield* session.sendEvent({ kind: "ready" });
  yield* session.await.pipe(
    Effect.catchTag(["SubagentBridgeProtocolError", "SubagentBridgeDisconnectedError"], (error) =>
      Effect.logWarning("Subagent bridge disconnected", error).pipe(
        Effect.annotateLogs({ subagentId }),
      ),
    ),
    Effect.forkScoped,
  );

  return { sendEvent: session.sendEvent };
});

export class PiSubagentHarness extends Context.Service<PiSubagentHarness>()(
  "@smith/subagent/PiSubagentHarness",
  { make },
) {
  static readonly layer = (subagentId: SubagentId) =>
    Layer.effect(PiSubagentHarness, PiSubagentHarness.make(subagentId)).pipe(
      Layer.provide(SubagentBridge.layer),
    );
}

export const makePiSubagentCommand = Effect.fn("makePiSubagentCommand")(function* (
  subagentId: SubagentId,
  spec: SubagentSpec,
): Effect.fn.Return<SubagentCommand, PiSubagentEntrypointUnavailableError> {
  const piEntrypoint = process.argv[1];

  if (piEntrypoint === undefined) {
    return yield* PiSubagentEntrypointUnavailableError.make({});
  }

  return {
    executable: process.execPath,
    args: [
      piEntrypoint,
      "--extension",
      fileURLToPath(new URL("../extension/PiSubagent.ts", import.meta.url)),
      "--name",
      spec.title,
      spec.prompt,
    ],
    cwd: spec.cwd,
    env: { SMITH_SUBAGENT_ID: subagentId },
  };
});

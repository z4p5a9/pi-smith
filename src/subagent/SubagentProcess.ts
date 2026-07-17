import { Effect, Schema, type Stream } from "effect";

import * as SubagentBridge from "./SubagentBridge.ts";
import type {
  SubagentBridgeDisconnectedError,
  SubagentBridgeProtocolError,
  SubagentEventDelivery,
} from "./SubagentBridge.ts";
import { SubagentHost } from "./SubagentHost.ts";
import { SubagentId } from "./SubagentId.ts";
import { makePiSubagentCommand } from "./PiSubagentHarness.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

export interface SubagentProcess {
  readonly subagentId: SubagentId;
  readonly status: Effect.Effect<"running">;
  readonly events: Stream.Stream<SubagentEventDelivery>;
  readonly await: Effect.Effect<
    void,
    SubagentBridgeProtocolError | SubagentBridgeDisconnectedError
  >;
}

export class SubagentProcessStartTimeoutError extends Schema.TaggedErrorClass<SubagentProcessStartTimeoutError>()(
  "SubagentProcessStartTimeoutError",
  {
    subagentId: SubagentId,
  },
) {}

export const spawnSubagentProcess = Effect.fn("SubagentProcess.spawn")(
  function* (subagentId: SubagentId, spec: SubagentSpec) {
    yield* Effect.annotateCurrentSpan({ subagentId });

    const host = yield* SubagentHost;
    const listener = yield* SubagentBridge.listen(subagentId);
    const command = yield* makePiSubagentCommand(subagentId, spec);

    yield* host.start(subagentId, spec, command);

    const session = yield* listener.accept;

    return {
      subagentId,
      status: Effect.succeed("running" as const),
      events: session.events,
      await: session.await,
    } satisfies SubagentProcess;
  },
  (effect, subagentId) =>
    effect.pipe(
      Effect.timeoutOrElse({
        duration: "30 seconds",
        orElse: () => SubagentProcessStartTimeoutError.make({ subagentId }),
      }),
    ),
);

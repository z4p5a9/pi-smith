import { Effect, Schema, type Stream } from "effect";

import {
  SubagentBridge,
  type SubagentBridgeDisconnectedError,
  type SubagentBridgeProtocolError,
  type SubagentEventDelivery,
} from "../bridge/Bridge.ts";
import { SubagentHarness } from "../harness/Harness.ts";
import { SubagentHost } from "../host/Host.ts";
import { SubagentId } from "./SubagentId.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

export interface SubagentProcess {
  readonly subagentId: SubagentId;
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

    const bridge = yield* SubagentBridge;
    const harness = yield* SubagentHarness;
    const host = yield* SubagentHost;
    const command = yield* harness.makeCommand(subagentId, spec);
    const listener = yield* bridge.listen(subagentId);

    yield* host.start(subagentId, command);

    const session = yield* listener.accept;

    return {
      subagentId,
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

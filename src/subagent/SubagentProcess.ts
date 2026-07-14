import { Effect } from "effect";

import type { SubagentId } from "./SubagentId.ts";
import type { SubagentSpec } from "./SubagentSpec.ts";

export interface SubagentProcess {
  readonly subagentId: SubagentId;
  readonly status: Effect.Effect<"running">;
  readonly await: Effect.Effect<never>;
}

export const spawnSubagentProcess = Effect.fn("SubagentProcess.spawn")(function* (
  subagentId: SubagentId,
  _spec: SubagentSpec,
) {
  yield* Effect.annotateCurrentSpan({ subagentId });

  return {
    subagentId,
    status: Effect.succeed("running" as const),
    // oxlint-disable-next-line no-warning-comments
    // TODO: Replace with the concrete process lifetime.
    await: Effect.never,
  } satisfies SubagentProcess;
});

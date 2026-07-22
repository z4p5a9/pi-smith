import type { Effect } from "effect";

import type { SubagentMessageId } from "./SubagentMessageId.ts";

export interface SubagentRef {
  readonly send: (content: string) => Effect.Effect<SubagentMessageId | undefined>;
}

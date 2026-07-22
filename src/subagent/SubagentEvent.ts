import { Schema } from "effect";

import type { SubagentId } from "./SubagentId.ts";
import type { SubagentMessageId } from "./SubagentMessageId.ts";

export const SubagentEvent = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("message"),
    content: Schema.String,
  }),
  Schema.Struct({
    kind: Schema.Literal("failure"),
    reason: Schema.String,
  }),
]);

export type SubagentEvent = typeof SubagentEvent.Type;

export type SubagentProcessEvent =
  | SubagentEvent
  | {
      readonly kind: "message-rejected";
      readonly messageId: SubagentMessageId;
      readonly reason: "frame-too-large";
      readonly actualBytes: number;
      readonly maxBytes: number;
    };

export interface SubagentEventEnvelope {
  readonly subagentId: SubagentId;
  readonly event: SubagentProcessEvent;
}

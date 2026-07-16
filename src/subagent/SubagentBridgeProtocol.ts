import { Schema } from "effect";

import { SubagentEvent } from "./SubagentEvent.ts";
import { SubagentId } from "./SubagentId.ts";

export const maxSubagentBridgeEventBytes = 1024 * 1024;
export const maxSubagentBridgeAcknowledgementBytes = 256;

export const SubagentBridgeEventFrame = Schema.Struct({
  version: Schema.Literal(1),
  subagentId: SubagentId,
  event: SubagentEvent,
});

export type SubagentBridgeEventFrame = typeof SubagentBridgeEventFrame.Type;

export const SubagentBridgeAcknowledgementFrame = Schema.Struct({
  kind: Schema.Literal("ack"),
  version: Schema.Literal(1),
  subagentId: SubagentId,
});

export type SubagentBridgeAcknowledgementFrame = typeof SubagentBridgeAcknowledgementFrame.Type;

export const encodeSubagentBridgeEventFrame = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeEventFrame),
);

export const encodeSubagentBridgeAcknowledgementFrame = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeAcknowledgementFrame),
);

import { Schema } from "effect";

import { SubagentEvent } from "./SubagentEvent.ts";
import { SubagentId } from "./SubagentId.ts";

export const maxSubagentBridgeChildFrameBytes = 1024 * 1024;
export const maxSubagentBridgeAcknowledgementBytes = 256;

export const SubagentBridgeEventFrame = Schema.Struct({
  kind: Schema.Literal("event"),
  version: Schema.Literal(1),
  subagentId: SubagentId,
  event: SubagentEvent,
});

export type SubagentBridgeEventFrame = typeof SubagentBridgeEventFrame.Type;

export const SubagentBridgeCloseFrame = Schema.Struct({
  kind: Schema.Literal("close"),
  version: Schema.Literal(1),
  subagentId: SubagentId,
});

export type SubagentBridgeCloseFrame = typeof SubagentBridgeCloseFrame.Type;

export const SubagentBridgeChildFrame = Schema.Union([
  SubagentBridgeEventFrame,
  SubagentBridgeCloseFrame,
]);

export const SubagentBridgeAcknowledgementFrame = Schema.Struct({
  kind: Schema.Literal("ack"),
  version: Schema.Literal(1),
  subagentId: SubagentId,
});

export type SubagentBridgeAcknowledgementFrame = typeof SubagentBridgeAcknowledgementFrame.Type;

export const encodeSubagentBridgeEventFrame = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeEventFrame),
);

export const encodeSubagentBridgeCloseFrame = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeCloseFrame),
);

export const encodeSubagentBridgeAcknowledgementFrame = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeAcknowledgementFrame),
);

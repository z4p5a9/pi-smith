import { Schema } from "effect";

import { SubagentEvent } from "../../subagent/SubagentEvent.ts";
import { SubagentId } from "../../subagent/SubagentId.ts";

export const maxSubagentBridgeChildFrameBytes = 1024 * 1024;
export const maxSubagentBridgeAcknowledgementBytes = 256;

export const SubagentBridgeHelloFrame = Schema.Struct({
  kind: Schema.Literal("hello"),
  version: Schema.Literal(1),
  subagentId: SubagentId,
});

export type SubagentBridgeHelloFrame = typeof SubagentBridgeHelloFrame.Type;

export const SubagentBridgeEventFrame = Schema.Struct({
  kind: Schema.Literal("event"),
  version: Schema.Literal(1),
  subagentId: SubagentId,
  event: SubagentEvent,
});

export type SubagentBridgeEventFrame = typeof SubagentBridgeEventFrame.Type;

export const SubagentBridgeChildFrame = Schema.Union([
  SubagentBridgeHelloFrame,
  SubagentBridgeEventFrame,
]);

export const SubagentBridgeAcknowledgementFrame = Schema.Struct({
  kind: Schema.Literal("ack"),
  version: Schema.Literal(1),
  subagentId: SubagentId,
});

export type SubagentBridgeAcknowledgementFrame = typeof SubagentBridgeAcknowledgementFrame.Type;

export const encodeSubagentBridgeHelloFrame = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeHelloFrame),
);

export const encodeSubagentBridgeEventFrame = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeEventFrame),
);

export const encodeSubagentBridgeAcknowledgementFrame = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeAcknowledgementFrame),
);

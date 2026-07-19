import { Schema } from "effect";

import { SubagentEvent } from "../../subagent/SubagentEvent.ts";
import { SubagentId } from "../../subagent/SubagentId.ts";

export const maxSubagentBridgeFrameBytes = 1024 * 1024;

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

export const SubagentBridgeAcknowledgementFrame = Schema.Struct({
  kind: Schema.Literal("ack"),
  version: Schema.Literal(1),
  subagentId: SubagentId,
});

export type SubagentBridgeAcknowledgementFrame = typeof SubagentBridgeAcknowledgementFrame.Type;

export const SubagentBridgeMessageFrame = Schema.Struct({
  kind: Schema.Literal("message"),
  version: Schema.Literal(1),
  subagentId: SubagentId,
  content: Schema.String,
});

export type SubagentBridgeMessageFrame = typeof SubagentBridgeMessageFrame.Type;

export const SubagentBridgeChildFrame = Schema.Union([
  SubagentBridgeHelloFrame,
  SubagentBridgeEventFrame,
  SubagentBridgeAcknowledgementFrame,
]);

export const SubagentBridgeRootFrame = Schema.Union([
  SubagentBridgeAcknowledgementFrame,
  SubagentBridgeMessageFrame,
]);

export const encodeSubagentBridgeHelloFrame = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeHelloFrame),
);

export const encodeSubagentBridgeEventFrame = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeEventFrame),
);

export const encodeSubagentBridgeAcknowledgementFrame = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeAcknowledgementFrame),
);

export const encodeSubagentBridgeMessageFrame = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeMessageFrame),
);

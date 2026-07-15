import { Schema } from "effect";

import { SubagentId } from "./SubagentId.ts";

export const maxSubagentBridgeHandshakeBytes = 256;

export const SubagentBridgeHandshake = Schema.Struct({
  version: Schema.Literal(1),
  subagentId: SubagentId,
});

export type SubagentBridgeHandshake = typeof SubagentBridgeHandshake.Type;

export const encodeSubagentBridgeHandshake = Schema.encodeEffect(
  Schema.fromJsonString(SubagentBridgeHandshake),
);

export const decodeSubagentBridgeHandshake = Schema.decodeUnknownEffect(
  Schema.fromJsonString(SubagentBridgeHandshake),
);

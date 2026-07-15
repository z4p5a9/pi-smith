import { Context, Schema, type Effect, type Scope } from "effect";

import { SubagentId } from "./SubagentId.ts";

export interface SubagentBridgeSession {
  readonly await: Effect.Effect<void, SubagentBridgeDisconnectedError>;
}

export interface SubagentBridgeListener {
  readonly accept: Effect.Effect<SubagentBridgeSession>;
}

export class SubagentBridgeListenError extends Schema.TaggedErrorClass<SubagentBridgeListenError>()(
  "SubagentBridgeListenError",
  {
    subagentId: SubagentId,
    reason: Schema.String,
  },
) {}

export class SubagentBridgeConnectError extends Schema.TaggedErrorClass<SubagentBridgeConnectError>()(
  "SubagentBridgeConnectError",
  {
    subagentId: SubagentId,
    reason: Schema.String,
  },
) {}

export class SubagentBridgeHandshakeError extends Schema.TaggedErrorClass<SubagentBridgeHandshakeError>()(
  "SubagentBridgeHandshakeError",
  {
    subagentId: SubagentId,
    reason: Schema.String,
  },
) {}

export class SubagentBridgeDisconnectedError extends Schema.TaggedErrorClass<SubagentBridgeDisconnectedError>()(
  "SubagentBridgeDisconnectedError",
  {
    subagentId: SubagentId,
    reason: Schema.String,
  },
) {}

export class SubagentBridge extends Context.Service<
  SubagentBridge,
  {
    readonly listen: (
      subagentId: SubagentId,
    ) => Effect.Effect<SubagentBridgeListener, SubagentBridgeListenError, Scope.Scope>;
    readonly connect: (
      subagentId: SubagentId,
    ) => Effect.Effect<SubagentBridgeSession, SubagentBridgeConnectError, Scope.Scope>;
  }
>()("@smith/subagent/SubagentBridge") {}

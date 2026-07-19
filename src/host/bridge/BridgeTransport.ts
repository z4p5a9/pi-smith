import { Context, type Effect, Schema, type Scope } from "effect";
import type * as Socket from "effect/unstable/socket/Socket";
import type * as SocketServer from "effect/unstable/socket/SocketServer";

import { SubagentId } from "../../subagent/SubagentId.ts";

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

export class SubagentBridgeTransport extends Context.Service<
  SubagentBridgeTransport,
  {
    readonly listen: (
      subagentId: SubagentId,
    ) => Effect.Effect<
      SocketServer.SocketServer["Service"],
      SubagentBridgeListenError,
      Scope.Scope
    >;
    readonly connect: (
      subagentId: SubagentId,
    ) => Effect.Effect<Socket.Socket, SubagentBridgeConnectError, Scope.Scope>;
  }
>()("@smith/host/bridge/BridgeTransport") {}

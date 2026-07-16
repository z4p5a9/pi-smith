import { Context, type Effect, type Scope } from "effect";
import type * as Socket from "effect/unstable/socket/Socket";
import type * as SocketServer from "effect/unstable/socket/SocketServer";

import type { SubagentBridgeConnectError, SubagentBridgeListenError } from "./SubagentBridge.ts";
import type { SubagentId } from "./SubagentId.ts";

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
>()("@smith/subagent/SubagentBridgeTransport") {}

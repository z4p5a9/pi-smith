import { Context, type Effect, Schema, type Scope } from "effect";
import type * as Socket from "effect/unstable/socket/Socket";
import type * as SocketServer from "effect/unstable/socket/SocketServer";

import { SubagentId } from "../../subagent/SubagentId.ts";

export class SubagentLinkListenError extends Schema.TaggedErrorClass<SubagentLinkListenError>()(
  "SubagentLinkListenError",
  {
    subagentId: SubagentId,
    reason: Schema.String,
  },
) {}

export class SubagentLinkConnectError extends Schema.TaggedErrorClass<SubagentLinkConnectError>()(
  "SubagentLinkConnectError",
  {
    subagentId: SubagentId,
    reason: Schema.String,
  },
) {}

export class SubagentLinkTransport extends Context.Service<
  SubagentLinkTransport,
  {
    readonly listen: (
      subagentId: SubagentId,
    ) => Effect.Effect<SocketServer.SocketServer["Service"], SubagentLinkListenError, Scope.Scope>;
    readonly connect: (
      subagentId: SubagentId,
    ) => Effect.Effect<Socket.Socket, SubagentLinkConnectError, Scope.Scope>;
  }
>()("@smith/host/link/Transport") {}

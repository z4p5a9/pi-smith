import { NodeFileSystem } from "@effect/platform-node";
import { it } from "@effect/vitest";
import { Effect, Layer } from "effect";

import { SubagentBridge } from "../../subagent/SubagentBridge.ts";
import { decodeSubagentId } from "../../subagent/SubagentId.ts";
import { layer as unixSocketSubagentBridgeTransportLayer } from "../../subagent/UnixSocketSubagentBridgeTransport.ts";
import { ChildSession } from "./ChildSession.ts";

it.effect("constructs without connecting the Bridge", () =>
  Effect.gen(function* () {
    const subagentId = yield* decodeSubagentId("sa_12345678_child-session");

    yield* Effect.void.pipe(
      Effect.provide(
        ChildSession.layer(subagentId).pipe(
          Layer.provide(SubagentBridge.layer),
          Layer.provide(unixSocketSubagentBridgeTransportLayer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    );
  }),
);

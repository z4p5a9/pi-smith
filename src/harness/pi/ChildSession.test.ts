import { NodeFileSystem } from "@effect/platform-node";
import { it } from "@effect/vitest";
import { Effect, Layer } from "effect";

import * as UnixSocketTransport from "../../host/link/unix/UnixSocketTransport.ts";
import { decodeSubagentId } from "../../subagent/SubagentId.ts";
import { ChildSession } from "./ChildSession.ts";

it.effect("constructs without connecting the link", () =>
  Effect.gen(function* () {
    const subagentId = yield* decodeSubagentId("sa_12345678_child-session");

    yield* Effect.void.pipe(
      Effect.provide(
        ChildSession.layer(subagentId).pipe(
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    );
  }),
);

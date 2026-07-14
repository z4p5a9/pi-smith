import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Effect, ManagedRuntime } from "effect";
import { Type } from "typebox";

import { SubagentCheckpoint } from "./subagent/SubagentCheckpoint.ts";
import { generateSubagentId } from "./subagent/SubagentId.ts";
import { SubagentPool } from "./subagent/SubagentPool.ts";

export default function extension(pi: ExtensionAPI): void {
  const runtime = ManagedRuntime.make(SubagentPool.layer);

  pi.on("session_shutdown", () => runtime.dispose());

  pi.registerTool({
    name: "subagent",
    label: "Subagent",
    description: "Create a subagent identity from a title.",
    parameters: Type.Object(
      {
        title: Type.String({
          description: "Human-readable title for the subagent.",
          minLength: 1,
          pattern: "\\S",
        }),
      },
      { additionalProperties: false },
    ),
    execute(_toolCallId, { title }) {
      return runtime.runPromise(
        Effect.gen(function* () {
          const subagentId = yield* generateSubagentId(title);
          const checkpoint = yield* SubagentCheckpoint;
          const pool = yield* SubagentPool;
          const spec = { title };

          yield* checkpoint.put({ subagentId, status: "queued", ...spec });
          yield* pool.submit(subagentId, spec);

          return {
            content: [{ type: "text" as const, text: subagentId }],
            details: { subagentId },
          };
        }),
      );
    },
  });
}

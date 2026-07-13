import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Effect } from "effect";
import { Type } from "typebox";

import { generateSubagentId } from "./subagent/SubagentId.ts";

export default function extension(pi: ExtensionAPI): void {
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
      return Effect.runPromise(
        generateSubagentId(title).pipe(
          Effect.map((subagentId) => ({
            content: [{ type: "text" as const, text: subagentId }],
            details: { subagentId },
          })),
        ),
      );
    },
  });
}

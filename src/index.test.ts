import { expect, it } from "@effect/vitest";
import { Effect } from "effect";

import extension from "./index.ts";

it.effect("exports a Pi extension factory", () =>
  Effect.sync(() => {
    expect(extension).toBeTypeOf("function");
  }),
);

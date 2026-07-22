import { fileURLToPath } from "node:url";

import { discoverAndLoadExtensions, type ExtensionContext } from "@earendil-works/pi-coding-agent";
import { expect, it, vi } from "@effect/vitest";
import { Effect } from "effect";

import { deliverSubagentEvents } from "./index.ts";
import { SubagentEventOutbox } from "./subagent/SubagentEventOutbox.ts";
import { decodeSubagentId } from "./subagent/SubagentId.ts";
import { SubagentMessageId } from "./subagent/SubagentMessageId.ts";

it.describe("root extension", () => {
  it.effect("registers the subagent tool", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", undefined);
    vi.stubEnv("CMUX_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111");
    vi.stubEnv("CMUX_SURFACE_ID", "22222222-2222-4222-8222-222222222222");

    return Effect.gen(function* () {
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );

      expect(result.errors).toEqual([]);
      expect(result.extensions).toHaveLength(1);
      expect(result.extensions[0]?.tools.has("subagent")).toBe(true);
      expect(result.extensions[0]?.tools.has("subagent_send")).toBe(true);
      expect(result.extensions[0]?.tools.has("subagent_kill")).toBe(true);
      expect(result.extensions[0]?.tools.has("subagent_status")).toBe(true);
    }).pipe(Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())));
  });

  it.effect("does not register inside a subagent Pi process", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", "sa_12345678_review-api");

    return Effect.gen(function* () {
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );

      expect(result.errors).toEqual([]);
      expect(result.extensions).toHaveLength(1);
      expect(result.extensions[0]?.tools.size).toBe(0);
      expect(result.extensions[0]?.handlers.size).toBe(0);
    }).pipe(Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())));
  });

  it.effect("routes status validation and missing records through the loaded tool", () => {
    vi.stubEnv("SMITH_SUBAGENT_ID", undefined);
    vi.stubEnv("CMUX_WORKSPACE_ID", "11111111-1111-4111-8111-111111111111");
    vi.stubEnv("CMUX_SURFACE_ID", "22222222-2222-4222-8222-222222222222");

    return Effect.gen(function* () {
      const result = yield* Effect.promise(() =>
        discoverAndLoadExtensions(
          [fileURLToPath(new URL("./index.ts", import.meta.url))],
          "/tmp/smith-extension-test",
          "/tmp/smith-extension-test",
        ),
      );
      const loaded = yield* Effect.fromNullishOr(result.extensions[0]);
      const status = yield* Effect.fromNullishOr(loaded.tools.get("subagent_status"));
      const shutdown = yield* Effect.fromNullishOr(loaded.handlers.get("session_shutdown")?.[0]);
      const unusedContext: ExtensionContext = {
        get ui(): ExtensionContext["ui"] {
          throw new Error("Status tool accessed UI context");
        },
        mode: "print",
        hasUI: false,
        cwd: "/tmp/smith-extension-test",
        get sessionManager(): ExtensionContext["sessionManager"] {
          throw new Error("Status tool accessed the session manager");
        },
        get modelRegistry(): ExtensionContext["modelRegistry"] {
          throw new Error("Status tool accessed the model registry");
        },
        model: undefined,
        isIdle: () => true,
        isProjectTrusted: () => true,
        signal: undefined,
        abort: () => undefined,
        hasPendingMessages: () => false,
        shutdown: () => undefined,
        getContextUsage: () => undefined,
        compact: () => undefined,
        getSystemPrompt: () => "",
      };

      yield* Effect.gen(function* () {
        const invalid = yield* Effect.promise(() =>
          status.definition.execute(
            "status-invalid",
            { subagentId: "invalid" },
            undefined,
            undefined,
            unusedContext,
          ),
        );
        const unknown = yield* Effect.promise(() =>
          status.definition.execute(
            "status-unknown",
            { subagentId: "sa_12345678_unknown" },
            undefined,
            undefined,
            unusedContext,
          ),
        );

        expect(invalid).toEqual({
          content: [{ type: "text", text: "Invalid subagent ID: invalid" }],
          details: { subagentId: "invalid" },
        });
        expect(unknown).toEqual({
          content: [{ type: "text", text: "Unknown subagent: sa_12345678_unknown" }],
          details: { subagentId: "sa_12345678_unknown" },
        });
      }).pipe(Effect.ensuring(Effect.promise(() => shutdown())));
    }).pipe(Effect.ensuring(Effect.sync(() => vi.unstubAllEnvs())));
  });

  it.effect("delivers every outbox event variant to root Pi", () =>
    Effect.gen(function* () {
      const eventOutbox = yield* SubagentEventOutbox;
      const subagentId = yield* decodeSubagentId("sa_12345678_delivery");
      const messageId = SubagentMessageId.make("msg_123456789012345678901234");
      const sendMessage = vi.fn((): void => undefined);
      const notify = vi.fn((): void => undefined);

      yield* deliverSubagentEvents({ sendMessage }, { hasUI: true, ui: { notify } }).pipe(
        Effect.forkChild({ startImmediately: true }),
      );

      yield* eventOutbox.publish({
        subagentId,
        event: { kind: "message", content: "Task complete." },
      });
      yield* eventOutbox.publish({
        subagentId,
        event: { kind: "failure", reason: "Model failed." },
      });
      yield* eventOutbox.publish({
        subagentId,
        event: {
          kind: "message-rejected",
          messageId,
          reason: "frame-too-large",
          actualBytes: 101,
          maxBytes: 100,
        },
      });
      yield* Effect.suspend(() =>
        sendMessage.mock.calls.length === 3
          ? Effect.void
          : Effect.fail("Events have not been delivered"),
      ).pipe(Effect.eventually);

      expect(sendMessage.mock.calls).toEqual([
        [
          {
            customType: "smith-subagent",
            content: `Subagent ${subagentId} reported:\n\nTask complete.`,
            display: false,
            details: {
              subagentId,
              event: { kind: "message", content: "Task complete." },
            },
          },
          { deliverAs: "followUp", triggerTurn: true },
        ],
        [
          {
            customType: "smith-subagent",
            content: `Subagent ${subagentId} failed:\n\nModel failed.`,
            display: false,
            details: {
              subagentId,
              event: { kind: "failure", reason: "Model failed." },
            },
          },
          { deliverAs: "followUp", triggerTurn: true },
        ],
        [
          {
            customType: "smith-subagent",
            content:
              `Message ${messageId} to subagent ${subagentId} was rejected before ` +
              "delivery: 101 bytes exceeds the 100-byte limit.",
            display: false,
            details: {
              subagentId,
              event: {
                kind: "message-rejected",
                messageId,
                reason: "frame-too-large",
                actualBytes: 101,
                maxBytes: 100,
              },
            },
          },
          { deliverAs: "followUp", triggerTurn: true },
        ],
      ]);
      expect(notify.mock.calls).toEqual([
        [`Subagent ${subagentId} reported`, "info"],
        [`Subagent ${subagentId} failed`, "error"],
        [`Message ${messageId} to subagent ${subagentId} was rejected`, "error"],
      ]);
    }).pipe(Effect.scoped, Effect.provide(SubagentEventOutbox.layer)),
  );

  it.effect("continues after root Pi rejects an event", () =>
    Effect.gen(function* () {
      const eventOutbox = yield* SubagentEventOutbox;
      const subagentId = yield* decodeSubagentId("sa_12345678_delivery-error");
      const sendMessage = vi.fn((_message: unknown, _options?: unknown): void => {
        if (sendMessage.mock.calls.length === 1) {
          throw new Error("Pi rejected the event");
        }
      });

      yield* deliverSubagentEvents(
        { sendMessage },
        { hasUI: false, ui: { notify: vi.fn((): void => undefined) } },
      ).pipe(Effect.forkChild({ startImmediately: true }));

      yield* eventOutbox.publish({
        subagentId,
        event: { kind: "message", content: "First." },
      });
      yield* eventOutbox.publish({
        subagentId,
        event: { kind: "message", content: "Second." },
      });
      yield* Effect.suspend(() =>
        sendMessage.mock.calls.length === 2
          ? Effect.void
          : Effect.fail("Second event has not been delivered"),
      ).pipe(Effect.eventually);

      expect(sendMessage.mock.calls[1]?.[0]).toMatchObject({
        content: `Subagent ${subagentId} reported:\n\nSecond.`,
      });
    }).pipe(Effect.scoped, Effect.provide(SubagentEventOutbox.layer)),
  );
});

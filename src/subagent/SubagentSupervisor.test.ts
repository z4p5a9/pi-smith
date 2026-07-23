import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Layer, Stream } from "effect";

import { SubagentHarness } from "../harness/Harness.ts";
import { SubagentHostStartError } from "../host/Host.ts";
import * as UnixSocketTransport from "../host/link/unix/UnixSocketTransport.ts";
import * as Protocol from "../host/Protocol.ts";
import { TestHost } from "../testing/TestHost.ts";
import { SubagentCapacity } from "./SubagentCapacity.ts";
import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import type { SubagentEventEnvelope } from "./SubagentEvent.ts";
import { SubagentEventOutbox } from "./SubagentEventOutbox.ts";
import { decodeSubagentId } from "./SubagentId.ts";
import { SubagentRegistry } from "./SubagentRegistry.ts";
import * as SubagentSupervisor from "./SubagentSupervisor.ts";

it.describe("SubagentSupervisor", () => {
  it.effect("registers before start returns and publishes the final event before teardown", () =>
    Effect.gen(function* () {
      const eventOutbox = yield* SubagentEventOutbox.make();
      const publishing = yield* Deferred.make<SubagentEventEnvelope>();
      const releasePublication = yield* Deferred.make<void>();
      const blockedEventOutbox = SubagentEventOutbox.of({
        ...eventOutbox,
        publish: Effect.fn("BlockedSubagentEventOutbox.publish")(function* (
          envelope: SubagentEventEnvelope,
        ) {
          yield* Deferred.succeed(publishing, envelope);
          yield* Deferred.await(releasePublication);
          yield* eventOutbox.publish(envelope);
        }),
      });

      yield* Effect.gen(function* () {
        const checkpoint = yield* SubagentCheckpoint;
        const registry = yield* SubagentRegistry;
        const testHost = yield* TestHost;
        const subagentId = yield* decodeSubagentId("sa_12345678_supervisor-exit");

        yield* testHost.stub([null]);
        yield* checkpoint.put({
          subagentId,
          status: "queued",
          title: "Review API",
          prompt: "Complete the task.",
          cwd: "/worktree",
          mode: "ephemeral",
        });

        const supervisor = yield* SubagentSupervisor.make(subagentId, {
          title: "Review API",
          prompt: "Complete the task.",
          cwd: "/worktree",
          mode: "ephemeral",
        });

        yield* supervisor.start;

        expect(yield* registry.lookup(subagentId)).toBeDefined();
        expect(yield* testHost.takeStart).toBe(subagentId);

        const child = yield* Protocol.connect(subagentId);

        yield* child.send({ kind: "message", content: "Task complete." });
        yield* Deferred.await(publishing);

        expect(yield* registry.lookup(subagentId)).toBeDefined();

        const completed = yield* supervisor.await.pipe(
          Effect.forkChild({ startImmediately: true }),
        );

        expect(completed.pollUnsafe()).toBeUndefined();
        yield* Deferred.succeed(releasePublication, undefined);

        expect(yield* supervisor.await).toEqual({ kind: "exited" });
        expect(
          yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
        ).toEqual({
          subagentId,
          event: { kind: "message", content: "Task complete." },
        });
        expect(yield* registry.lookup(subagentId)).toBeUndefined();
        expect(yield* testHost.active).toEqual([]);
        yield* testHost.verify;
      }).pipe(
        Effect.scoped,
        Effect.provide(
          TestHost.layer.pipe(
            Layer.provideMerge(Layer.succeed(SubagentEventOutbox, blockedEventOutbox)),
            Layer.provideMerge(SubagentRegistry.layer),
            Layer.provideMerge(SubagentCheckpoint.layer),
            Layer.provideMerge(SubagentCapacity.layer(1)),
            Layer.provideMerge(
              Layer.succeed(
                SubagentHarness,
                SubagentHarness.of({
                  makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
                }),
              ),
            ),
            Layer.provideMerge(UnixSocketTransport.layer),
            Layer.provide(NodeFileSystem.layer),
          ),
        ),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("publishes failure before releasing registry membership", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const eventOutbox = yield* SubagentEventOutbox;
      const registry = yield* SubagentRegistry;
      const testHost = yield* TestHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_supervisor-failure");

      yield* testHost.stub([
        SubagentHostStartError.make({
          subagentId,
          host: "test",
          reason: "Pane creation failed",
        }),
      ]);
      yield* checkpoint.put({
        subagentId,
        status: "queued",
        title: "Host failure",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      const supervisor = yield* SubagentSupervisor.make(subagentId, {
        title: "Host failure",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "ephemeral",
      });

      yield* supervisor.start;

      expect(yield* supervisor.await).toEqual({
        kind: "failed",
        reason: "Pane creation failed",
      });
      expect(
        yield* eventOutbox.events.pipe(Stream.runHead, Effect.flatMap(Effect.fromOption)),
      ).toEqual({
        subagentId,
        event: { kind: "failure", reason: "Pane creation failed" },
      });
      expect(yield* registry.lookup(subagentId)).toBeUndefined();
      expect(yield* checkpoint.get(subagentId)).toMatchObject({
        status: "failed",
        latestEvent: { kind: "failure", reason: "Pane creation failed" },
      });
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        TestHost.layer.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentCapacity.layer(1)),
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );

  it.effect("interrupts and tears down without projecting killed", () =>
    Effect.gen(function* () {
      const checkpoint = yield* SubagentCheckpoint;
      const registry = yield* SubagentRegistry;
      const testHost = yield* TestHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_supervisor-interrupt");

      yield* testHost.stub([null]);
      yield* checkpoint.put({
        subagentId,
        status: "queued",
        title: "Interrupted",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "persistent",
      });

      const supervisor = yield* SubagentSupervisor.make(subagentId, {
        title: "Interrupted",
        prompt: "Complete the task.",
        cwd: "/worktree",
        mode: "persistent",
      });

      yield* supervisor.start;

      expect(yield* registry.lookup(subagentId)).toBeDefined();
      expect(yield* testHost.takeStart).toBe(subagentId);
      yield* Protocol.connect(subagentId);
      yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((record) => record.status === "running"),
        Stream.runHead,
      );

      yield* supervisor.interrupt;

      expect(yield* supervisor.await).toEqual({ kind: "killed" });
      expect(yield* registry.lookup(subagentId)).toBeUndefined();
      expect((yield* checkpoint.get(subagentId)).status).toBe("running");
      expect(yield* testHost.active).toEqual([]);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        TestHost.layer.pipe(
          Layer.provideMerge(SubagentEventOutbox.layer),
          Layer.provideMerge(SubagentRegistry.layer),
          Layer.provideMerge(SubagentCheckpoint.layer),
          Layer.provideMerge(SubagentCapacity.layer(1)),
          Layer.provideMerge(
            Layer.succeed(
              SubagentHarness,
              SubagentHarness.of({
                makeCommand: () => Effect.succeed({ executable: "pi", args: [] }),
              }),
            ),
          ),
          Layer.provideMerge(UnixSocketTransport.layer),
          Layer.provide(NodeFileSystem.layer),
        ),
      ),
    ),
  );
});

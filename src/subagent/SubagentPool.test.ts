import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, Exit, Fiber, Layer, Scope, Stream } from "effect";

import { TestSubagentHost } from "../testing/TestSubagentHost.ts";
import { SubagentBridge } from "./SubagentBridge.ts";
import { SubagentCheckpoint } from "./SubagentCheckpoint.ts";
import { SubagentHostUnavailableError } from "./SubagentHost.ts";
import { decodeSubagentId } from "./SubagentId.ts";
import { SubagentPool } from "./SubagentPool.ts";
import { layer as unixSocketSubagentBridgeTransportLayer } from "./UnixSocketSubagentBridgeTransport.ts";

it.describe("SubagentPool", () => {
  it.effect("submits a subagent spec", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const pool = yield* SubagentPool;
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestSubagentHost;
      const subagentId = yield* decodeSubagentId("sa_12345678_pool-submit");

      yield* testHost.stub([{ hostId: "test-host" }]);
      yield* checkpoint.put({
        subagentId,
        status: "queued",
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });
      yield* pool.submit(subagentId, {
        title: "Review API",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      expect(yield* testHost.takeStart).toBe(subagentId);

      const connection = yield* bridge.connect(subagentId);
      const sentReady = yield* connection
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      const record = yield* checkpoint.changes(subagentId).pipe(
        Stream.filter((currentRecord) => currentRecord.status === "running"),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );
      yield* Fiber.join(sentReady);
      expect(record.status).toBe("running");
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentPool.layer.pipe(
          Layer.provideMerge(TestSubagentHost.layer),
          Layer.provideMerge(
            SubagentBridge.layer.pipe(
              Layer.provide(unixSocketSubagentBridgeTransportLayer),
              Layer.provide(NodeFileSystem.layer),
            ),
          ),
        ),
      ),
    ),
  );

  it.effect("continues consuming after a checkpoint update failure", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const pool = yield* SubagentPool;
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestSubagentHost;
      const missingSubagentId = yield* decodeSubagentId("sa_12345678_pool-missing");
      const probeSubagentId = yield* decodeSubagentId("sa_87654321_pool-missing-probe");

      yield* testHost.stub([{ hostId: "test-host" }]);

      for (let index = 0; index < 10; index++) {
        yield* pool.submit(missingSubagentId, {
          title: "Missing",
          prompt: "Complete the task.",
          cwd: "/worktree",
        });
      }

      yield* checkpoint.put({
        subagentId: probeSubagentId,
        status: "queued",
        title: "Probe",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });
      yield* pool.submit(probeSubagentId, {
        title: "Probe",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      expect(yield* testHost.takeStart).toBe(probeSubagentId);

      const connection = yield* bridge.connect(probeSubagentId);
      const sentReady = yield* connection
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      const record = yield* checkpoint.changes(probeSubagentId).pipe(
        Stream.filter((currentRecord) => currentRecord.status === "running"),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );
      yield* Fiber.join(sentReady);
      expect(record.status).toBe("running");
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentPool.layer.pipe(
          Layer.provideMerge(TestSubagentHost.layer),
          Layer.provideMerge(
            SubagentBridge.layer.pipe(
              Layer.provide(unixSocketSubagentBridgeTransportLayer),
              Layer.provide(NodeFileSystem.layer),
            ),
          ),
        ),
      ),
    ),
  );

  it.effect("continues consuming after a duplicate supervisor start", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const pool = yield* SubagentPool;
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestSubagentHost;
      const duplicateSubagentId = yield* decodeSubagentId("sa_12345678_pool-duplicate");
      const probeSubagentId = yield* decodeSubagentId("sa_87654321_pool-duplicate-probe");

      yield* testHost.stub([{ hostId: "duplicate-host" }, { hostId: "probe-host" }]);

      yield* checkpoint.put({
        subagentId: duplicateSubagentId,
        status: "queued",
        title: "Duplicate",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });
      yield* pool.submit(duplicateSubagentId, {
        title: "Duplicate",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      expect(yield* testHost.takeStart).toBe(duplicateSubagentId);

      const duplicateConnection = yield* bridge.connect(duplicateSubagentId);
      const sentDuplicateReady = yield* duplicateConnection
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      yield* checkpoint.changes(duplicateSubagentId).pipe(
        Stream.filter((record) => record.status === "running"),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
        Effect.asVoid,
      );
      yield* Fiber.join(sentDuplicateReady);

      for (let index = 0; index < 10; index++) {
        yield* pool.submit(duplicateSubagentId, {
          title: "Duplicate",
          prompt: "Complete the task.",
          cwd: "/worktree",
        });
      }

      yield* checkpoint.put({
        subagentId: probeSubagentId,
        status: "queued",
        title: "Probe",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });
      yield* pool.submit(probeSubagentId, {
        title: "Probe",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      expect(yield* testHost.takeStart).toBe(probeSubagentId);

      const probeConnection = yield* bridge.connect(probeSubagentId);
      const sentProbeReady = yield* probeConnection
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      const record = yield* checkpoint.changes(probeSubagentId).pipe(
        Stream.filter((currentRecord) => currentRecord.status === "running"),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );
      yield* Fiber.join(sentProbeReady);
      expect(record.status).toBe("running");
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentPool.layer.pipe(
          Layer.provideMerge(TestSubagentHost.layer),
          Layer.provideMerge(
            SubagentBridge.layer.pipe(
              Layer.provide(unixSocketSubagentBridgeTransportLayer),
              Layer.provide(NodeFileSystem.layer),
            ),
          ),
        ),
      ),
    ),
  );

  it.effect("continues consuming after every worker encounters a startup failure", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const pool = yield* SubagentPool;
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestSubagentHost;
      const failedSubagentId = yield* decodeSubagentId("sa_12345678_pool-start-failed");
      const probeSubagentId = yield* decodeSubagentId("sa_87654321_pool-start-probe");

      yield* testHost.stub([
        ...Array.from({ length: 10 }, () => ({
          error: SubagentHostUnavailableError.make({
            subagentId: failedSubagentId,
            host: "cmux-pane" as const,
            reason: "CMUX unavailable",
          }),
        })),
        { hostId: "probe-host" },
      ]);

      yield* checkpoint.put({
        subagentId: failedSubagentId,
        status: "queued",
        title: "Failed",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      for (let index = 0; index < 10; index++) {
        yield* pool.submit(failedSubagentId, {
          title: "Failed",
          prompt: "Complete the task.",
          cwd: "/worktree",
        });
      }

      for (let index = 0; index < 10; index++) {
        expect((yield* testHost.takeStartCall).subagentId).toBe(failedSubagentId);
      }

      yield* checkpoint.put({
        subagentId: probeSubagentId,
        status: "queued",
        title: "Probe",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });
      yield* pool.submit(probeSubagentId, {
        title: "Probe",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      expect(yield* testHost.takeStart).toBe(probeSubagentId);

      const connection = yield* bridge.connect(probeSubagentId);
      const sentReady = yield* connection
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      const record = yield* checkpoint.changes(probeSubagentId).pipe(
        Stream.filter((currentRecord) => currentRecord.status === "running"),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );
      yield* Fiber.join(sentReady);

      expect(record.status).toBe("running");
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentPool.layer.pipe(
          Layer.provideMerge(TestSubagentHost.layer),
          Layer.provideMerge(
            SubagentBridge.layer.pipe(
              Layer.provide(unixSocketSubagentBridgeTransportLayer),
              Layer.provide(NodeFileSystem.layer),
            ),
          ),
        ),
      ),
    ),
  );

  it.effect("records failure and reuses the slot after a child disconnects", () =>
    Effect.gen(function* () {
      const bridge = yield* SubagentBridge;
      const pool = yield* SubagentPool;
      const checkpoint = yield* SubagentCheckpoint;
      const testHost = yield* TestSubagentHost;
      const parentScope = yield* Scope.Scope;
      const failedSubagentId = yield* decodeSubagentId("sa_00000001_worker-1");
      const subagentIds = [
        failedSubagentId,
        ...(yield* Effect.forEach(
          Array.from(
            { length: 9 },
            (_, index) => `sa_${String(index + 2).padStart(8, "0")}_worker-${String(index + 2)}`,
          ),
          decodeSubagentId,
        )),
      ];
      const probeSubagentId = yield* decodeSubagentId("sa_87654321_probe");

      yield* testHost.stub([
        ...subagentIds.map((_, index) => ({ hostId: `host-${String(index + 1)}` })),
        { hostId: "probe-host" },
      ]);

      for (const [index, subagentId] of subagentIds.entries()) {
        const title = `Worker ${String(index + 1)}`;

        yield* checkpoint.put({
          subagentId,
          status: "queued",
          title,
          prompt: "Complete the task.",
          cwd: "/worktree",
        });
        yield* pool.submit(subagentId, { title, prompt: "Complete the task.", cwd: "/worktree" });
      }

      const childScopes = new Map<string, Scope.Closeable>();

      for (let index = 0; index < subagentIds.length; index++) {
        const startedSubagentId = yield* testHost.takeStart;
        const childScope = yield* Scope.fork(parentScope);
        const connection = yield* bridge.connect(startedSubagentId).pipe(Scope.provide(childScope));
        const sentReady = yield* connection
          .sendEvent({ kind: "ready" })
          .pipe(Effect.forkChild({ startImmediately: true }));

        childScopes.set(startedSubagentId, childScope);
        yield* Fiber.join(sentReady);
      }

      yield* Effect.forEach(
        subagentIds,
        (subagentId) =>
          checkpoint.changes(subagentId).pipe(
            Stream.filter((record) => record.status === "running"),
            Stream.runHead,
            Effect.flatMap(Effect.fromOption),
          ),
        { concurrency: "unbounded" },
      );

      yield* checkpoint.put({
        subagentId: probeSubagentId,
        status: "queued",
        title: "Probe",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });
      yield* pool.submit(probeSubagentId, {
        title: "Probe",
        prompt: "Complete the task.",
        cwd: "/worktree",
      });

      expect((yield* checkpoint.get(probeSubagentId)).status).toBe("queued");

      const failedChildScope = yield* Effect.fromNullishOr(childScopes.get(failedSubagentId));

      yield* Scope.close(failedChildScope, Exit.void);

      expect(yield* testHost.takeStart).toBe(probeSubagentId);

      const probeConnection = yield* bridge.connect(probeSubagentId);
      const sentProbeReady = yield* probeConnection
        .sendEvent({ kind: "ready" })
        .pipe(Effect.forkChild({ startImmediately: true }));

      const failed = yield* checkpoint.changes(failedSubagentId).pipe(
        Stream.filter((record) => record.status === "failed"),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );
      const probe = yield* checkpoint.changes(probeSubagentId).pipe(
        Stream.filter((record) => record.status === "running"),
        Stream.runHead,
        Effect.flatMap(Effect.fromOption),
      );
      yield* Fiber.join(sentProbeReady);

      expect(failed.status).toBe("failed");
      expect(probe.status).toBe("running");
      expect(yield* testHost.active).toHaveLength(10);
      yield* testHost.verify;
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SubagentPool.layer.pipe(
          Layer.provideMerge(TestSubagentHost.layer),
          Layer.provideMerge(
            SubagentBridge.layer.pipe(
              Layer.provide(unixSocketSubagentBridgeTransportLayer),
              Layer.provide(NodeFileSystem.layer),
            ),
          ),
        ),
      ),
    ),
  );
});

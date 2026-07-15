import { expect, it } from "@effect/vitest";
import { Effect, Exit, PlatformError, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { TestChildProcessSpawner } from "./TestChildProcessSpawner.ts";

it.describe("TestChildProcessSpawner", () => {
  it.effect("returns stubs and records calls", () =>
    Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const command = ChildProcess.make("command", ["argument"]);

      yield* childProcesses.stub([
        { exitCode: Effect.succeed(7), stdout: "output", stderr: "error" },
      ]);

      const process = yield* spawner.spawn(command);
      const [exitCode, stdout, stderr] = yield* Effect.all([
        process.exitCode,
        process.stdout.pipe(Stream.decodeText, Stream.mkString),
        process.stderr.pipe(Stream.decodeText, Stream.mkString),
      ]);

      expect(exitCode).toBe(7);
      expect(stdout).toBe("output");
      expect(stderr).toBe("error");
      expect(yield* childProcesses.calls).toEqual([command]);
      yield* childProcesses.verify;
    }).pipe(Effect.provide(TestChildProcessSpawner.layer)),
  );

  it.effect("returns spawn failures", () =>
    Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const error = PlatformError.systemError({
        _tag: "NotFound",
        module: "ChildProcess",
        method: "spawn",
      });

      yield* childProcesses.stub([{ error }]);

      const actual = yield* spawner.spawn(ChildProcess.make("missing")).pipe(Effect.flip);

      expect(actual).toBe(error);
      yield* childProcesses.verify;
    }).pipe(Effect.provide(TestChildProcessSpawner.layer)),
  );

  it.effect("detects unused stubs", () =>
    Effect.gen(function* () {
      const childProcesses = yield* TestChildProcessSpawner;

      yield* childProcesses.stub([{ exitCode: Effect.succeed(0) }]);

      const exit = yield* childProcesses.verify.pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
    }).pipe(Effect.provide(TestChildProcessSpawner.layer)),
  );
});

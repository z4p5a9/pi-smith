import { Context, Effect, Layer, Ref, Sink, Stream, type PlatformError } from "effect";
import { ChildProcessSpawner, type ChildProcess } from "effect/unstable/process";

type SpawnStub =
  | {
      readonly exitCode: Effect.Effect<number, PlatformError.PlatformError>;
      readonly stdout?: string;
      readonly stderr?: string;
    }
  | {
      readonly error: PlatformError.PlatformError;
    };

export class TestChildProcessSpawner extends Context.Service<TestChildProcessSpawner>()(
  "@smith/testing/TestChildProcessSpawner",
  {
    make: Effect.gen(function* () {
      const state = yield* Ref.make<{
        readonly stubs: Array<SpawnStub>;
        readonly calls: Array<ChildProcess.Command>;
        readonly nextPid: number;
      }>({
        stubs: [],
        calls: [],
        nextPid: 1,
      });

      const spawn = Effect.fn("TestChildProcessSpawner.spawn")(function* (
        command: ChildProcess.Command,
      ) {
        const result = yield* Ref.modify(state, (prev) => {
          const [stub, ...stubs] = prev.stubs;
          const next = {
            stubs,
            calls: [...prev.calls, command],
            nextPid: stub !== undefined && !("error" in stub) ? prev.nextPid + 1 : prev.nextPid,
          };

          return [{ stub, pid: prev.nextPid }, next] as const;
        });

        if (result.stub === undefined) {
          return yield* Effect.die("Unexpected child process spawn");
        }

        if ("error" in result.stub) {
          return yield* result.stub.error;
        }

        const stdout = Stream.succeed(new TextEncoder().encode(result.stub.stdout ?? ""));
        const stderr = Stream.succeed(new TextEncoder().encode(result.stub.stderr ?? ""));
        const exitCode = result.stub.exitCode.pipe(Effect.map(ChildProcessSpawner.ExitCode));

        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(result.pid),
          exitCode,
          isRunning: Effect.succeed(false),
          kill: () => Effect.void,
          stdin: Sink.drain,
          stdout,
          stderr,
          all: Stream.merge(stdout, stderr),
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
          unref: Effect.succeed(Effect.void),
        });
      });

      const stub = Effect.fn("TestChildProcessSpawner.stub")(function* (
        stubs: ReadonlyArray<SpawnStub>,
      ) {
        yield* Ref.update(state, (prev) => {
          const next = { ...prev, stubs: [...prev.stubs, ...stubs] };

          return next;
        });
      });

      const calls = Effect.gen(function* () {
        const ref = yield* Ref.get(state);

        return [...ref.calls];
      });

      const verify = Effect.gen(function* () {
        const ref = yield* Ref.get(state);

        if (ref.stubs.length > 0) {
          return yield* Effect.die(`${ref.stubs.length} child process stubs were unused`);
        }

        return yield* Effect.void;
      });

      return { ...ChildProcessSpawner.make(spawn), stub, calls, verify };
    }),
  },
) {
  static readonly layer = Layer.effect(
    ChildProcessSpawner.ChildProcessSpawner,
    TestChildProcessSpawner,
  ).pipe(Layer.provideMerge(Layer.effect(TestChildProcessSpawner, TestChildProcessSpawner.make)));
}

import { Duration, Schema } from "effect";

export const isPositiveFiniteDuration = () =>
  Schema.makeFilter<Duration.Duration>((duration) =>
    Duration.isFinite(duration) && Duration.isPositive(duration)
      ? undefined
      : "Expected a positive finite duration",
  );

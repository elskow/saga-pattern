import { Data, Effect } from "effect";
import { Pattern } from "@/types";

export class FetchError extends Data.TaggedError("FetchError")<{
  readonly cause: unknown;
  readonly message: string;
}> {}

export class ApiError extends Data.TaggedError("ApiError")<{
  readonly status: number;
  readonly message: string;
}> {}

export class DecodeError extends Data.TaggedError("DecodeError")<{
  readonly cause: unknown;
  readonly message: string;
}> {}

export const requestJson = <T>(
  url: string,
  init?: RequestInit
): Effect.Effect<T, FetchError | ApiError | DecodeError> =>
  Effect.gen(function* () {
    const res = yield* Effect.tryPromise({
      try: () => fetch(url, init),
      catch: (e) =>
        new FetchError({ cause: e, message: e instanceof Error ? e.message : "Network error" }),
    });

    if (!res.ok) {
      const text = yield* Effect.tryPromise({
        try: () => res.text(),
        catch: (e) => new FetchError({ cause: e, message: "Failed to read text" }),
      }).pipe(Effect.catchAll(() => Effect.succeed("Unknown error")));
      let message = `HTTP ${res.status}`;
      if (text) {
        try {
          const json = JSON.parse(text);
          if (json && typeof json.error === "string") {
            message = json.error;
          } else {
            message = text;
          }
        } catch {
          message = text;
        }
      }
      return yield* Effect.fail(new ApiError({ status: res.status, message }));
    }

    const text = yield* Effect.tryPromise({
      try: () => res.text(),
      catch: (e) =>
        new FetchError({ cause: e, message: e instanceof Error ? e.message : "Failed to read body" }),
    });
    
    if (!text) {
      return null as T;
    }

    try {
      return JSON.parse(text) as T;
    } catch (e) {
      return yield* Effect.fail(
        new DecodeError({ cause: e, message: "Invalid JSON response" })
      );
    }
  });

const PORT_MAP = {
  choreography: { payment: 8082, inventory: 8083, shipping: 8084, order: 8081 },
  orchestration: { payment: 8092, inventory: 8093, shipping: 8094, order: 8091 },
};

export function getServiceUrl(
  service: "order" | "shipping" | "inventory" | "payment",
  pattern: Pattern,
  serverSide: boolean = true
): string {
  if (!serverSide) {
    if (service === "order") return `/proxy/${pattern}`;
    return `/proxy/${pattern}-${service}`;
  }

  const envName = `${pattern.toUpperCase()}_${service.toUpperCase()}_URL`;
  const envBase =
    typeof process !== "undefined" ? process.env[envName] : undefined;
  return envBase ?? `http://127.0.0.1:${PORT_MAP[pattern][service]}`;
}

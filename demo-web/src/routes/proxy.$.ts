import { createFileRoute } from "@tanstack/react-router";

const PORTS: Record<string, number> = {
  "choreography-order": 8081,
  "choreography-payment": 8082,
  "choreography-inventory": 8083,
  "choreography-shipping": 8084,
  "orchestration-order": 8091,
  "orchestration-payment": 8092,
  "orchestration-inventory": 8093,
  "orchestration-shipping": 8094,
};

function resolveTarget(first: string): string | null {
  const m = first.match(/^(choreography|orchestration)(?:-(payment|inventory|shipping))?$/);
  if (!m) return null;
  const prefix = m[1];
  const name = m[2] ?? "order";
  const envBase = process.env[`${prefix.toUpperCase()}_${name.toUpperCase()}_URL`];
  return envBase ?? `http://127.0.0.1:${PORTS[`${prefix}-${name}`]}`;
}

async function forward({
  request,
  params,
}: {
  request: Request;
  params: Record<string, string | undefined>;
}): Promise<Response> {
  const splat = params._splat ?? Object.values(params)[0] ?? "";
  const [first, ...rest] = splat.split("/");
  const target = resolveTarget(first ?? "");
  if (!target) return new Response("unknown proxy target", { status: 404 });
  const search = new URL(request.url).search;
  const headers = new Headers(request.headers);
  headers.delete("host");
  headers.delete("content-length");
  const upstream = await fetch(`${target}/${rest.join("/")}${search}`, {
    method: request.method,
    headers,
    body:
      request.method === "GET" || request.method === "HEAD"
        ? undefined
        : request.body,
    duplex: "half",
    signal: AbortSignal.timeout(20000),
  }).catch(
    () =>
      new Response(JSON.stringify({ error: "backend unreachable" }), {
        status: 502,
        headers: { "Content-Type": "application/json" },
      })
  );
  if (upstream instanceof Response && upstream.headers.get("content-type") === null) {
    return upstream;
  }
  const outHeaders = new Headers();
  const contentType = upstream.headers.get("content-type");
  if (contentType) outHeaders.set("content-type", contentType);
  return new Response(upstream.body, { status: upstream.status, headers: outHeaders });
}

export const Route = createFileRoute("/proxy/$")({
  server: {
    handlers: {
      GET: forward,
      POST: forward,
      PUT: forward,
      PATCH: forward,
      DELETE: forward,
    },
  },
});

import { createFileRoute } from "@tanstack/react-router";
import { getServiceUrl } from "@/lib/effect-services";
import type { Pattern } from "@/types";

const services: Array<"payment" | "inventory" | "shipping"> = ["payment", "inventory", "shipping"];
const patterns: Pattern[] = ["choreography", "orchestration"];

export const Route = createFileRoute("/api/admin/delay")({
  server: {
    handlers: {
      POST: async ({ request }) => {
        const body = await request.json().catch(() => null);
        const delayMs = Number(body?.delay_ms ?? body?.delayMs ?? 0);

        void Promise.allSettled(
          patterns.flatMap((pattern) =>
            services.map((service) =>
              fetch(`${getServiceUrl(service, pattern, true)}/api/admin/delay`, {
                method: "PUT",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ delay_ms: Number.isFinite(delayMs) ? delayMs : 0 }),
                signal: AbortSignal.timeout(5000),
              })
            )
          )
        ).then((results) => {
          const rejected = results.filter((result) => result.status === "rejected");
          if (rejected.length > 0) {
            console.error("Failed to update simulated delay", rejected);
          }
        });

        return Response.json({ accepted: true });
      },
    },
  },
});

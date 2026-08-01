"use client";

import { useState } from "react";
import {
  fetchAllOrdersServer,
  fetchAllServiceHealthServer,
  computeMetrics,
  fetchFailureModeServer,
  setFailureModeServer,
  FAILURE_SERVICE_KEYS,
  FAILURE_SERVICE_LABELS_MAP,
} from "@/lib/admin";
import { AdminMetrics, Order, ServiceHealth } from "@/types";
import { Skeleton } from "@/components/ui/skeleton";
import { Button } from "@/components/ui/button";
import { Link } from "@tanstack/react-router";
import { cn } from "@/lib/utils";

function MetricCard({ label, value, sub }: { label: string; value: string | number; sub?: string }) {
  return (
    <div className="space-y-1.5 rounded-md border border-border bg-card p-4">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p className="text-2xl font-semibold tabular-nums tracking-tight text-foreground">{value}</p>
      {sub && <p className="text-xs text-muted-foreground">{sub}</p>}
    </div>
  );
}

function HealthDot({ healthy }: { healthy: boolean }) {
  return (
    <span
      className={cn(
        "inline-flex h-2 w-2 shrink-0 rounded-full",
        healthy ? "bg-emerald-500" : "bg-red-500"
      )}
    />
  );
}

function FailureInjectionPanel() {
  const [modes, setModes] = useState<Record<string, boolean>>({});
  const [loading, setLoading] = useState<Record<string, boolean>>({});
  const [ready, setReady] = useState(false);
  const [bootstrapping, setBootstrapping] = useState(false);
  const [simulatedDelay, setSimulatedDelay] = useState<number>(() => {
    if (typeof window !== "undefined") {
      return parseInt(localStorage.getItem("saga-simulated-delay") || "0");
    }
    return 0;
  });

  const handleDelayChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const val = parseInt(e.target.value);
    setSimulatedDelay(val);
    localStorage.setItem("saga-simulated-delay", val.toString());
    window.setTimeout(() => {
      void fetch("/api/admin/delay", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ delay_ms: val }),
        keepalive: true,
      }).catch((err) => {
        console.error("Failed to update simulated delay", err);
      });
    }, 0);
  };

  const loadModes = async () => {
    setBootstrapping(true);
    const results = await Promise.allSettled(
      FAILURE_SERVICE_KEYS.map(async (key) => {
        try {
          const status = await fetchFailureModeServer({ data: key });
          return { key, enabled: status.enabled };
        } catch {
          return { key, enabled: false };
        }
      })
    );
    const m: Record<string, boolean> = {};
    results.forEach((r) => {
      if (r.status === "fulfilled") {
        m[r.value.key] = r.value.enabled;
      }
    });
    setModes(m);
    setBootstrapping(false);
  };

  const toggle = async (key: string) => {
    const current = modes[key] ?? false;
    setLoading((prev) => ({ ...prev, [key]: true }));
    try {
      await setFailureModeServer({ data: { key: key as import("@/types").FailureServiceKey, enabled: !current } });
      setModes((prev) => ({ ...prev, [key]: !current }));
    } catch (err) {
      console.error(`Failed to toggle ${key}:`, err);
    } finally {
      setLoading((prev) => ({ ...prev, [key]: false }));
    }
  };

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h2 className="text-base font-semibold tracking-tight text-foreground">Saga controls</h2>
          <p className="mt-0.5 text-sm text-muted-foreground">
            Failure modes and execution delays
          </p>
        </div>
        <span className="rounded-sm border border-border bg-muted/40 px-2 py-0.5 text-[11px] text-muted-foreground">
          Testing only
        </span>
      </div>

      <div className="rounded-md border border-border bg-card p-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h3 className="text-sm font-medium text-foreground">Simulated execution delay</h3>
            <p className="mt-0.5 text-sm text-muted-foreground">Slows saga steps for clearer timelines.</p>
          </div>
          <select
            value={simulatedDelay}
            onChange={handleDelayChange}
            className="w-full text-xs sm:w-48"
          >
            <option value={0}>Fast (0ms)</option>
            <option value={1000}>Slow (1s)</option>
            <option value={2000}>Very slow (2s)</option>
            <option value={5000}>Extremely slow (5s)</option>
          </select>
        </div>
      </div>

      {!ready ? (
        <div className="rounded-md border border-dashed border-border bg-card p-4">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <p className="text-sm text-muted-foreground">
              Load failure switches only when needed during a test run.
            </p>
            <Button
              variant="outline"
              size="sm"
              className="rounded-md text-xs"
              onClick={() => {
                setReady(true);
                void loadModes();
              }}
            >
              Load failure controls
            </Button>
          </div>
        </div>
      ) : bootstrapping ? (
        <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
          {FAILURE_SERVICE_KEYS.map((key) => (
            <Skeleton key={key} className="h-16 rounded-md" />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-2 md:grid-cols-2">
          {FAILURE_SERVICE_KEYS.map((key) => {
            const enabled = modes[key] ?? false;
            const isLoading = loading[key] ?? false;
            const label = (FAILURE_SERVICE_LABELS_MAP as Record<string, string>)[key] ?? key;
            return (
              <button
                key={key}
                onClick={() => toggle(key)}
                disabled={isLoading}
                className="rounded-md border border-border bg-card p-3 text-left transition-colors hover:bg-muted/30 disabled:cursor-not-allowed disabled:opacity-50"
              >
                <div className="flex items-center justify-between gap-3">
                  <div className="min-w-0">
                    <p className="text-sm font-medium text-foreground">{label}</p>
                    <p className="text-xs text-muted-foreground">
                      {enabled ? "Failures active — next saga will fail" : "Normal operation"}
                    </p>
                  </div>
                  <span
                    className={cn(
                      "shrink-0 rounded-sm border px-2 py-0.5 text-[11px] font-medium",
                      enabled
                        ? "border-red-200 bg-red-50 text-red-700"
                        : "border-border bg-muted/40 text-muted-foreground"
                    )}
                  >
                    {enabled ? "On" : "Off"}
                  </span>
                </div>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

interface AdminDashboardClientProps {
  initialOrders: Order[];
  initialHealth: ServiceHealth[];
  initialError: string | null;
}

export default function AdminDashboardClient({
  initialOrders,
  initialHealth,
  initialError,
}: AdminDashboardClientProps) {
  const [orders, setOrders] = useState<Order[]>(initialOrders);
  const [health, setHealth] = useState<ServiceHealth[]>(initialHealth);
  const [metrics, setMetrics] = useState<AdminMetrics | null>(computeMetrics(initialOrders));
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(initialError);

  const refresh = async () => {
    setLoading(true);
    const results = await Promise.allSettled([fetchAllOrdersServer(), fetchAllServiceHealthServer()]);
    const [ordersResult, healthResult] = results;

    if (ordersResult.status === "fulfilled") {
      setOrders(ordersResult.value);
      setMetrics(computeMetrics(ordersResult.value));
      setError(null);
    } else {
      setOrders([]);
      setMetrics(computeMetrics([]));
      setError(ordersResult.reason instanceof Error ? ordersResult.reason.message : "Live order listing unavailable");
    }

    if (healthResult.status === "fulfilled") {
      setHealth(healthResult.value);
    } else {
      setHealth([]);
    }

    setLoading(false);
  };

  const choreoHealth = health.filter((h) => h.pattern === "choreography");
  const orchHealth = health.filter((h) => h.pattern === "orchestration");

  return (
    <div className="w-full max-w-none space-y-8">
      <div className="mb-2 flex flex-col gap-3 border-b border-border pb-5 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-xl font-semibold tracking-tight text-foreground">Dashboard</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Live view across saga services
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={refresh} className="rounded-md text-xs">
          Refresh
        </Button>
      </div>

      {error && (
        <div className="rounded-md border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
          {error}
        </div>
      )}

      {loading ? (
        <div className="grid grid-cols-2 gap-3 lg:grid-cols-3">
          {[1, 2, 3, 4, 5, 6].map((i) => (
            <Skeleton key={i} className="h-24 rounded-md" />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-2 gap-3 lg:grid-cols-3">
          <MetricCard label="Total orders" value={metrics?.totalOrders ?? 0} />
          <MetricCard label="Completed" value={metrics?.completedOrders ?? 0} sub={`${metrics?.successRate ?? 0}% success rate`} />
          <MetricCard label="Failed" value={metrics?.failedOrders ?? 0} />
          <MetricCard label="Compensated" value={metrics?.compensatedOrders ?? 0} sub="Rollbacks completed" />
          <MetricCard label="In progress" value={metrics?.pendingOrders ?? 0} />
          <MetricCard label="Success rate" value={`${metrics?.successRate ?? 0}%`} />
        </div>
      )}

      <div className="space-y-3">
        <h2 className="text-base font-semibold tracking-tight text-foreground">Service health</h2>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          {[
            { label: "Choreography (8081–8084)", services: choreoHealth },
            { label: "Orchestration (8091–8094)", services: orchHealth },
          ].map(({ label, services }) => (
            <div key={label} className="space-y-3 rounded-md border border-border bg-card p-4">
              <p className="text-sm font-medium text-foreground">{label}</p>
              {loading ? (
                <div className="space-y-2">
                  {[1, 2, 3, 4].map((i) => (
                    <Skeleton key={i} className="h-5 w-full rounded-sm" />
                  ))}
                </div>
              ) : services.length === 0 ? (
                <p className="py-4 text-center text-xs text-muted-foreground">No health data available</p>
              ) : (
                <div className="divide-y divide-border">
                  {services.map((svc) => (
                    <div key={`${svc.pattern}-${svc.name}-${svc.port}`} className="flex items-center justify-between py-2 text-sm">
                      <span className="flex items-center gap-2.5">
                        <HealthDot healthy={svc.healthy} />
                        <span className="font-medium text-foreground">{svc.name}</span>
                        <span className="font-mono text-xs text-muted-foreground">
                          :{svc.port}
                        </span>
                      </span>
                      <span
                        className={cn(
                          "text-xs font-medium",
                          svc.healthy ? "text-emerald-700" : "text-red-700"
                        )}
                      >
                        {svc.healthy ? "Up" : "Down"}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      <FailureInjectionPanel />

      {orders.length > 0 && (
        <div className="space-y-3 pt-2">
          <div className="flex items-center justify-between">
            <h2 className="text-base font-semibold tracking-tight text-foreground">Recent orders</h2>
            <Link
              to="/admin/orders"
              className="text-sm font-medium text-primary hover:text-primary/80 transition-colors"
            >
              View all
            </Link>
          </div>
          <div className="divide-y divide-border overflow-hidden rounded-md border border-border bg-card">
            {[...orders].slice(-5).reverse().map((o) => {
              const id = o.id ?? o.orderId;
              return (
                <div
                  key={id}
                  className="flex items-center gap-4 px-4 py-3 text-sm transition-colors hover:bg-muted/30"
                >
                  <span className="flex-1 truncate font-mono text-xs text-muted-foreground">
                    #{id}
                  </span>
                  <span className="text-xs capitalize text-muted-foreground">
                    {o.pattern}
                  </span>
                  <span
                    className={cn(
                      "text-xs font-medium",
                      o.status === "COMPLETED" ? "text-emerald-700" : "text-muted-foreground"
                    )}
                  >
                    {o.status}
                  </span>
                  <Link
                    to="/orders/$orderId"
                    params={{ orderId: id }}
                    search={{ pattern: o.pattern }}
                    className="text-xs font-medium text-primary hover:text-primary/80"
                  >
                    Open
                  </Link>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}

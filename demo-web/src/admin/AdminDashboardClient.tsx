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
import {
  ShoppingBag,
  CheckCircle2,
  XCircle,
  RotateCcw,
  Clock,
  Activity,
  Wifi,
  WifiOff,
  AlertCircle,
  Zap,
  ZapOff,
  ToggleLeft,
  ToggleRight,
  ArrowLeft,
} from "lucide-react";
import { Link } from "@tanstack/react-router";
import { cn } from "@/lib/utils";

function MetricCard({ label, value, sub, icon: Icon, accent }: any) {
  return (
    <div className="rounded-xl border border-border bg-card p-5 space-y-3 shadow-sm">
      <div className="flex items-center justify-between">
        <p className="text-xs text-muted-foreground font-semibold uppercase tracking-wider">
          {label}
        </p>
        <Icon className={cn("h-4 w-4", accent ?? "text-muted-foreground")} />
      </div>
      <p className="text-3xl font-bold tracking-tight text-foreground">{value}</p>
      {sub && <p className="text-xs text-muted-foreground font-medium">{sub}</p>}
    </div>
  );
}

function HealthDot({ healthy }: { healthy: boolean }) {
  return (
    <span className="relative flex h-2.5 w-2.5">
      {healthy && (
        <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-20"></span>
      )}
      <span
        className={cn(
          "relative inline-flex rounded-full h-2.5 w-2.5",
          healthy ? "bg-green-500" : "bg-red-500"
        )}
      />
    </span>
  );
}

function FailureInjectionPanel() {
  const [modes, setModes] = useState<Record<string, boolean>>({});
  const [loading, setLoading] = useState<Record<string, boolean>>({});
  const [ready, setReady] = useState(false);
  const [bootstrapping, setBootstrapping] = useState(false);

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
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold tracking-tight">Failure Injection</h2>
          <p className="text-sm text-muted-foreground mt-0.5">
            Toggle failure mode per service to simulate saga failures
          </p>
        </div>
        <div className="flex items-center gap-2 rounded-full border border-amber-200 bg-amber-50/50 px-3 py-1.5 text-xs font-medium text-amber-800">
          <Zap className="h-3.5 w-3.5" />
          Testing only
        </div>
      </div>
      {!ready ? (
        <div className="rounded-xl border border-dashed border-border bg-card/50 p-5">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <p className="text-sm text-muted-foreground">
              Load failure switches only when you need them during a test run.
            </p>
            <Button
              variant="outline"
              size="sm"
              className="rounded-full gap-2 text-xs"
              onClick={() => {
                setReady(true);
                void loadModes();
              }}
            >
              <Zap className="h-3.5 w-3.5" />
              Load Failure Controls
            </Button>
          </div>
        </div>
      ) : bootstrapping ? (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {FAILURE_SERVICE_KEYS.map((key) => (
            <Skeleton key={key} className="h-28 rounded-xl" />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {FAILURE_SERVICE_KEYS.map((key) => {
            const enabled = modes[key] ?? false;
            const isLoading = loading[key] ?? false;
            const label = (FAILURE_SERVICE_LABELS_MAP as Record<string, string>)[key] ?? key;
            return (
              <button
                key={key}
                onClick={() => toggle(key)}
                disabled={isLoading}
                className="group rounded-xl border border-border bg-card p-5 shadow-sm transition-all hover:shadow-md text-left disabled:opacity-50 disabled:cursor-not-allowed"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className={`flex h-10 w-10 items-center justify-center rounded-lg ${enabled ? "bg-red-100 dark:bg-red-900/30" : "bg-green-100 dark:bg-green-900/30"}`}>
                      {enabled ? (
                        <ZapOff className="h-5 w-5 text-red-600 dark:text-red-400" />
                      ) : (
                        <Zap className="h-5 w-5 text-green-600 dark:text-green-400" />
                      )}
                    </div>
                    <div>
                      <p className="text-sm font-semibold text-foreground">{label}</p>
                      <p className="text-xs text-muted-foreground">
                        {enabled ? "Failures active — next saga will fail" : "Normal operation"}
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    {enabled ? (
                      <div className="flex items-center gap-1.5 rounded-full bg-red-100 dark:bg-red-900/30 px-2.5 py-1 text-xs font-bold text-red-700 dark:text-red-400">
                        <ToggleRight className="h-3.5 w-3.5" />
                        ON
                      </div>
                    ) : (
                      <div className="flex items-center gap-1.5 rounded-full bg-muted/60 px-2.5 py-1 text-xs font-bold text-muted-foreground">
                        <ToggleLeft className="h-3.5 w-3.5" />
                        OFF
                      </div>
                    )}
                  </div>
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
    <div className="space-y-8 w-full max-w-none">
      <div className="border-b border-border/60 pb-5 mb-6">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <h1 className="text-2xl font-bold tracking-tight text-foreground">Dashboard</h1>
            <p className="text-sm text-muted-foreground mt-1">
              Real-time view across all saga services
            </p>
          </div>
          <Button variant="outline" size="sm" onClick={refresh} className="gap-1.5 rounded-full text-xs">
            Refresh
          </Button>
        </div>
      </div>

      {error && (
        <div className="flex items-center gap-3 rounded-xl border border-amber-200 bg-amber-50/50 p-4 text-sm text-amber-800">
          <AlertCircle className="h-4 w-4 text-amber-600 shrink-0" />
          <p className="font-medium">{error}</p>
        </div>
      )}

      {loading ? (
        <div className="grid grid-cols-2 lg:grid-cols-3 gap-4">
          {[1, 2, 3, 4, 5, 6].map((i) => (
            <Skeleton key={i} className="h-32 rounded-xl" />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-2 lg:grid-cols-3 gap-4">
          <MetricCard label="Total Orders" value={metrics?.totalOrders ?? 0} icon={ShoppingBag} />
          <MetricCard label="Completed" value={metrics?.completedOrders ?? 0} sub={`${metrics?.successRate ?? 0}% success rate`} icon={CheckCircle2} accent="text-green-600" />
          <MetricCard label="Failed" value={metrics?.failedOrders ?? 0} icon={XCircle} accent="text-red-500" />
          <MetricCard label="Compensated" value={metrics?.compensatedOrders ?? 0} sub="Rollbacks completed" icon={RotateCcw} accent="text-orange-500" />
          <MetricCard label="In Progress" value={metrics?.pendingOrders ?? 0} icon={Clock} accent="text-amber-500" />
          <MetricCard label="Success Rate" value={`${metrics?.successRate ?? 0}%`} icon={Activity} accent={(metrics?.successRate ?? 0) >= 80 ? "text-green-600" : "text-red-500"} />
        </div>
      )}

      <div className="space-y-4">
        <h2 className="text-lg font-semibold tracking-tight">Service Health</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {[
            { label: "Choreography (8081–8084)", services: choreoHealth },
            { label: "Orchestration (8091–8094)", services: orchHealth },
          ].map(({ label, services }) => (
            <div key={label} className="rounded-xl border border-border bg-card p-5 space-y-4 shadow-sm">
              <p className="text-sm font-semibold text-foreground">{label}</p>
              {loading ? (
                <div className="space-y-3">
                  {[1, 2, 3, 4].map((i) => (
                    <Skeleton key={i} className="h-6 w-full rounded" />
                  ))}
                </div>
              ) : services.length === 0 ? (
                <div className="flex items-center justify-center py-6 border border-dashed border-border rounded-lg bg-muted/20">
                  <span className="text-xs font-medium text-muted-foreground">No health data available</span>
                </div>
              ) : (
                <div className="space-y-3">
                  {services.map((svc) => (
                    <div key={svc.port} className="flex items-center justify-between text-sm">
                      <span className="flex items-center gap-3">
                        <HealthDot healthy={svc.healthy} />
                        <span className="text-foreground font-medium">{svc.name}</span>
                        <span className="text-muted-foreground text-xs font-mono bg-muted px-1.5 py-0.5 rounded">
                          :{svc.port}
                        </span>
                      </span>
                      <span className="flex items-center gap-1.5 text-xs font-bold px-2 py-1 rounded-md bg-muted/40">
                        {svc.healthy ? (
                          <>
                            <Wifi className="h-3 w-3 text-green-500" />
                            <span className="text-green-600">UP</span>
                          </>
                        ) : (
                          <>
                            <WifiOff className="h-3 w-3 text-red-500" />
                            <span className="text-red-600">DOWN</span>
                          </>
                        )}
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
        <div className="space-y-4 pt-4">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold tracking-tight">Recent Orders</h2>
            <Link to="/admin/orders" className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors">
              View all &rarr;
            </Link>
          </div>
          <div className="rounded-xl border border-border bg-card overflow-hidden divide-y divide-border shadow-sm">
            {[...orders].slice(-5).reverse().map((o) => {
              const id = o.id ?? o.orderId;
              return (
                <div key={id} className="flex items-center gap-4 px-5 py-3.5 text-sm hover:bg-muted/30 transition-colors">
                  <span className="font-mono text-xs font-semibold text-muted-foreground flex-1 truncate">
                    #{id}
                  </span>
                  <span className="capitalize text-[10px] font-bold border border-border bg-muted/30 rounded-full px-2.5 py-1 text-foreground">
                    {o.pattern}
                  </span>
                  <span className={cn("text-xs font-medium", o.status === "COMPLETED" ? "text-green-600" : "text-muted-foreground")}>
                    {o.status}
                  </span>
                  <Link to={`/orders/${id}?pattern=${o.pattern}`} className="text-muted-foreground hover:text-foreground">
                    <ArrowLeft className="h-4 w-4 rotate-180" />
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

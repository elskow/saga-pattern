import { createFileRoute, Link } from "@tanstack/react-router";
import { Activity, ArrowRight, Network, RotateCcw, ShoppingCart, CreditCard, Package, Truck } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/about")({
  component: AboutPage,
});

function AboutPage() {
  const orderSteps = [
    { icon: ShoppingCart, label: "Order Created", desc: "Your order is validated and prepared" },
    { icon: CreditCard, label: "Payment", desc: "Payment is processed and authorized" },
    { icon: Package, label: "Inventory", desc: "Items are reserved from the warehouse" },
    { icon: Truck, label: "Shipping", desc: "Delivery is scheduled and tracking assigned" },
  ];

  return (
    <div className="max-w-3xl mx-auto space-y-16 py-10 pb-20">
      <div className="space-y-4 border-b border-border/60 pb-10">
        <span className="rounded-full border border-border/60 bg-muted/50 px-3 py-1 text-[10px] font-bold uppercase tracking-[0.25em] text-muted-foreground">
          How It Works
        </span>
        <h1 className="text-4xl font-bold tracking-tight text-foreground">About SagaStore</h1>
        <p className="text-base text-muted-foreground leading-relaxed max-w-2xl">
          SagaStore keeps every purchase coordinated from checkout to delivery.
          Choose between standard fulfillment and priority coordination, then follow your order as payment,
          inventory, and shipping move in sync.
        </p>
      </div>

      <section className="space-y-6">
        <h2 className="text-2xl font-bold tracking-tight text-foreground">The Order Journey</h2>
        <p className="text-sm text-muted-foreground leading-relaxed">
          Every order moves through payment, inventory, and shipping before it reaches the delivery stage.
          If fulfillment cannot continue, completed steps are safely rolled back in reverse order.
        </p>
        <ol className="space-y-0">
          {orderSteps.map((step, i) => {
            const Icon = step.icon;
            const isLast = i === orderSteps.length - 1;
            return (
              <li key={step.label} className="flex gap-4">
                <div className="flex flex-col items-center">
                  <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-foreground text-background shadow-sm">
                    <Icon className="h-4 w-4" />
                  </div>
                  {!isLast && <div className="mt-1 w-px flex-1 bg-border min-h-[2rem]" />}
                </div>
                <div className={cn("pb-6 pt-1 min-w-0", isLast && "pb-0")}>
                  <p className="text-sm font-semibold text-foreground">{step.label}</p>
                  <p className="text-sm text-muted-foreground mt-0.5">{step.desc}</p>
                </div>
              </li>
            );
          })}
        </ol>
        <div className="rounded-xl border border-orange-200 bg-orange-50/50 px-4 py-3 flex items-start gap-3 text-sm text-orange-800">
          <RotateCcw className="h-4 w-4 mt-0.5 shrink-0 text-orange-600" />
          <p>
            <strong>If something goes wrong</strong>, rollback runs in reverse — Shipping → Inventory → Payment — so your order state stays consistent.
          </p>
        </div>
      </section>

      <section className="space-y-6">
        <h2 className="text-2xl font-bold tracking-tight text-foreground">Two Fulfillment Styles, One Checkout</h2>
        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          <div className="rounded-2xl border border-border/60 bg-foreground p-6 space-y-3 text-background shadow-sm">
            <div className="flex items-center gap-2">
              <Activity className="h-5 w-5 text-background" />
              <h3 className="text-base font-bold text-background">Standard Fulfillment</h3>
            </div>
            <p className="text-sm text-background/75 leading-relaxed">
              Each fulfillment team reacts as soon as the previous stage is ready, keeping the order flow lightweight and responsive.
            </p>
            <ul className="space-y-1 text-xs text-background/80 font-medium">
              <li>· Fast stage-by-stage processing</li>
              <li>· Live stock reservation</li>
              <li>· Automatic delivery handoff</li>
              <li>· Best for regular checkout flow</li>
            </ul>
          </div>
          <div className="rounded-2xl border border-border/60 bg-muted/30 p-6 space-y-3">
            <div className="flex items-center gap-2">
              <Network className="h-5 w-5 text-foreground" />
              <h3 className="text-base font-bold text-foreground">Priority Coordination</h3>
            </div>
            <p className="text-sm text-muted-foreground leading-relaxed">
              A central order coordinator supervises each stage, giving the checkout flow stronger sequencing and clearer status updates.
            </p>
            <ul className="space-y-1 text-xs text-foreground font-medium">
              <li>· Coordinated payment and stock checks</li>
              <li>· Clear step-by-step status</li>
              <li>· Supervised delivery scheduling</li>
              <li>· Best for high-confidence fulfillment</li>
            </ul>
          </div>
        </div>
      </section>

      <section className="space-y-4">
        <h2 className="text-2xl font-bold tracking-tight text-foreground">Behind the Order</h2>
        <div className="rounded-2xl border border-border/60 bg-card p-6 space-y-3 text-sm">
          <div className="grid grid-cols-2 gap-x-8 gap-y-2 text-sm">
            {[
              ["Order service", "Creates and tracks checkout state"],
              ["Payment service", "Authorizes customer payments"],
              ["Inventory service", "Reserves live stock"],
              ["Shipping service", "Schedules delivery and tracking"],
              ["Status updates", "Live order progress"],
              ["Rollback", "Automatic recovery when fulfillment cannot continue"],
            ].map(([label, value]) => (
              <div key={label} className="contents">
                <span className="text-muted-foreground font-medium">{label}</span>
                <span className="text-foreground font-mono text-xs">{value}</span>
              </div>
            ))}
          </div>
        </div>
      </section>

      <div className="flex flex-col sm:flex-row gap-3 pt-4 border-t border-border/60">
        <Link to="/">
          <Button className="rounded-full gap-2 shadow-sm">
            Start Shopping
            <ArrowRight className="h-4 w-4" />
          </Button>
        </Link>
        <Link to="/admin">
          <Button variant="outline" className="rounded-full gap-2 shadow-sm">
            Admin Dashboard
          </Button>
        </Link>
      </div>
    </div>
  );
}

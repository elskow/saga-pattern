import { createFileRoute, Link, Outlet, useLocation } from '@tanstack/react-router'
import {
  LayoutDashboard,
  Package,
  ShoppingBag,
  CreditCard,
  Truck,
  ArrowLeft,
} from "lucide-react";
import { cn } from "@/lib/utils";

const nav = [
  { href: "/admin",            label: "Dashboard",  icon: LayoutDashboard },
  { href: "/admin/inventory",  label: "Inventory",  icon: Package },
  { href: "/admin/orders",     label: "Orders",     icon: ShoppingBag },
  { href: "/admin/payment",    label: "Payment",    icon: CreditCard },
  { href: "/admin/shipments",  label: "Shipments",  icon: Truck },
];

function AdminSidebar() {
  const location = useLocation();
  const pathname = location.pathname;

  return (
    <div className="flex flex-col space-y-6">
      <div>
        <h2 className="text-[11px] font-bold text-muted-foreground uppercase tracking-widest mb-3 px-3">
          Admin Console
        </h2>
        <nav className="flex flex-col space-y-1">
          {nav.map((item) => {
            const Icon = item.icon;
            const exact = item.href === "/admin";
            const active = exact ? pathname === item.href : pathname.startsWith(item.href);
            return (
              <Link
                key={item.href}
                to={item.href}
                className={cn(
                  "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-all",
                  active
                    ? "bg-foreground text-background shadow-sm"
                    : "text-muted-foreground hover:bg-muted/50 hover:text-foreground"
                )}
              >
                <Icon className="h-4 w-4 shrink-0" />
                {item.label}
              </Link>
            );
          })}
        </nav>
      </div>

      <div className="px-3 pt-4 border-t border-border/60">
        <Link
          to="/"
          className="flex items-center gap-2 text-sm font-medium text-muted-foreground hover:text-foreground transition-colors group"
        >
          <ArrowLeft className="h-4 w-4 shrink-0 transition-transform group-hover:-translate-x-1" />
          Back to store
        </Link>
      </div>
    </div>
  );
}

export const Route = createFileRoute('/admin')({
  component: AdminLayout,
})

function AdminLayout() {
  return (
    <div className="mx-auto max-w-7xl w-full px-4 sm:px-6 py-8 flex flex-col md:flex-row gap-10">
      <aside className="w-full md:w-52 shrink-0">
        <AdminSidebar />
      </aside>
      <main className="flex-1 min-w-0 pt-4 md:pt-0">
        <Outlet />
      </main>
    </div>
  );
}

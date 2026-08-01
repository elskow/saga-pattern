import { createFileRoute, Link, Outlet, redirect, useLocation } from "@tanstack/react-router";
import {
  LayoutDashboard,
  Package,
  ShoppingBag,
  CreditCard,
  Truck,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/lib/store";

const nav = [
  { href: "/admin", label: "Dashboard", icon: LayoutDashboard },
  { href: "/admin/inventory", label: "Inventory", icon: Package },
  { href: "/admin/orders", label: "Orders", icon: ShoppingBag },
  { href: "/admin/payment", label: "Payment", icon: CreditCard },
  { href: "/admin/shipments", label: "Shipments", icon: Truck },
];

function AdminSidebar() {
  const location = useLocation();
  const pathname = location.pathname;
  const user = useAuthStore((s) => s.user);

  return (
    <div className="flex h-full flex-col">
      <div className="px-1 pb-4">
        <p className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
          Admin console
        </p>
        <p className="mt-1 text-xs text-muted-foreground">{user?.username}</p>
      </div>

      <nav className="flex flex-col gap-0.5">
        {nav.map((item) => {
          const Icon = item.icon;
          const exact = item.href === "/admin";
          const active = exact ? pathname === item.href : pathname.startsWith(item.href);
          return (
            <Link
              key={item.href}
              to={item.href}
              className={cn(
                "flex items-center gap-2.5 rounded-md px-3 py-2 text-sm transition-colors",
                active
                  ? "bg-secondary font-medium text-primary"
                  : "text-muted-foreground hover:bg-muted/60 hover:text-foreground"
              )}
            >
              <Icon className="h-4 w-4 shrink-0" />
              {item.label}
            </Link>
          );
        })}
      </nav>
    </div>
  );
}

export const Route = createFileRoute("/admin")({
  beforeLoad: ({ location }) => {
    if (location.pathname === "/admin/login") {
      return;
    }

    if (typeof window === "undefined") {
      return;
    }

    const { user } = useAuthStore.getState();
    if (!user || user.role !== "admin") {
      throw redirect({ to: "/admin/login" });
    }
  },
  component: AdminLayout,
});

function AdminLayout() {
  const pathname = useLocation().pathname;

  if (pathname === "/admin/login") {
    return <Outlet />;
  }

  return (
    <div className="flex w-full flex-col gap-6 md:flex-row md:gap-8">
      <aside className="w-full shrink-0 md:sticky md:top-20 md:w-56 md:self-start md:border-r md:border-border md:pr-6">
        <AdminSidebar />
      </aside>
      <main className="min-w-0 flex-1">
        <Outlet />
      </main>
    </div>
  );
}

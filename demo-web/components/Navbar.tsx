"use client";

import { useState } from "react";
import { Link, useNavigate } from "@tanstack/react-router";
import { useLocation } from "@tanstack/react-router";
import { CartSidebar } from "./CartSidebar";
import { PatternToggle } from "./PatternToggle";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/lib/store";
import { LogOut, Menu, Package, ShieldCheck, ShoppingBag, User } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
    Sheet,
    SheetContent,
    SheetHeader,
    SheetTitle,
    SheetTrigger,
} from "@/components/ui/sheet";

export function Navbar() {
    const pathname = useLocation().pathname;
    const navigate = useNavigate();
    const user = useAuthStore((s) => s.user);
    const logout = useAuthStore((s) => s.logout);
    const [mobileOpen, setMobileOpen] = useState(false);

    const links = [
        { href: "/", label: "Shop", icon: ShoppingBag },
        { href: "/orders", label: "Orders", icon: Package },
    ];

    const handleLogout = () => {
        logout();
        toast.success("Signed out.");
        setMobileOpen(false);
        void navigate({ to: "/" });
    };

    return (
        <div className="fixed top-4 left-0 right-0 z-50 flex justify-center w-full px-4 sm:px-6 pointer-events-none">
            <header className="flex h-16 w-full max-w-7xl items-center justify-between px-6 sm:px-8 bg-background/80 backdrop-blur-2xl border border-border/50 shadow-[0_8px_32px_0_rgba(0,0,0,0.05)] rounded-full supports-[backdrop-filter]:bg-background/50 pointer-events-auto transition-all duration-300 hover:shadow-[0_12px_40px_0_rgba(0,0,0,0.08)] hover:border-border/80">

                <Link
                    to="/"
                    id="nav-logo"
                    className="flex items-center gap-2 text-xl font-black tracking-tighter text-foreground group relative"
                >
                    SagaStore
                </Link>

                <nav className="hidden md:flex items-center gap-1 absolute left-1/2 -translate-x-1/2 bg-muted/40 p-1.5 rounded-full border border-border/40 backdrop-blur-sm">
                    {links.map((link) => {
                        const isActive = pathname === link.href;
                        return (
                            <Link
                                key={link.href}
                                to={link.href}
                                className={cn(
                                    "relative px-5 py-1.5 rounded-full text-sm font-semibold transition-all duration-300",
                                    isActive
                                        ? "text-background bg-foreground shadow-sm"
                                        : "text-muted-foreground hover:text-foreground hover:bg-muted/80"
                                )}
                            >
                                <span className="relative z-10">{link.label}</span>
                            </Link>
                        );
                    })}
                </nav>

                <div className="flex items-center gap-2 sm:gap-3">
                    {user ? (
                        <div className="hidden md:flex items-center overflow-hidden rounded-full border border-border/60 bg-muted/30 p-1">
                            {user.role === "admin" ? (
                                <Link
                                    to="/admin"
                                    className="flex items-center gap-1.5 rounded-full px-3 py-1.5 text-sm font-semibold text-muted-foreground transition-colors hover:bg-background/70 hover:text-foreground"
                                >
                                    <ShieldCheck className="h-4 w-4" />
                                    <span>Admin</span>
                                </Link>
                            ) : (
                                <Link
                                    to="/profile"
                                    className="flex items-center gap-1.5 rounded-full px-3 py-1.5 text-sm font-medium text-muted-foreground transition-colors hover:bg-background/70 hover:text-foreground"
                                >
                                    <User className="h-4 w-4" />
                                    <span>{user.username}</span>
                                </Link>
                            )}
                            <button
                                onClick={handleLogout}
                                className="flex items-center gap-1.5 rounded-full px-3 py-1.5 text-sm font-medium text-muted-foreground transition-colors hover:bg-destructive/10 hover:text-destructive"
                                aria-label="Sign out"
                            >
                                <LogOut className="h-4 w-4" />
                                <span>Sign out</span>
                            </button>
                        </div>
                    ) : (
                        <Link
                            to="/login"
                            className="hidden md:flex items-center gap-2 text-sm font-semibold bg-foreground text-background px-4 py-2 rounded-full hover:scale-105 hover:shadow-lg hover:shadow-foreground/20 transition-all duration-300"
                        >
                            <User className="h-4 w-4" />
                            Sign in
                        </Link>
                    )}

                    <div className="h-6 w-px bg-foreground/10 hidden md:block mx-1" />

                    <div className="hover:scale-105 transition-transform duration-300 flex items-center">
                        <CartSidebar />
                    </div>

                    <Sheet open={mobileOpen} onOpenChange={setMobileOpen}>
                        <SheetTrigger
                            render={
                                <Button
                                    variant="ghost"
                                    size="icon"
                                    className="md:hidden rounded-full hover:bg-muted/50"
                                    aria-label="Open menu"
                                />
                            }
                        >
                            <Menu className="h-5 w-5" />
                        </SheetTrigger>

                        <SheetContent side="left" className="flex w-72 flex-col bg-background p-0">
                            <SheetHeader className="border-b border-border/50 px-6 py-5">
                                <SheetTitle className="text-lg font-black tracking-tighter text-foreground">
                                    SagaStore
                                </SheetTitle>
                            </SheetHeader>

                            <nav className="flex flex-col gap-1 px-4 py-4">
                                {links.map((link) => {
                                    const Icon = link.icon;
                                    const isActive = pathname === link.href;
                                    return (
                                        <Link
                                            key={link.href}
                                            to={link.href}
                                            onClick={() => setMobileOpen(false)}
                                            className={cn(
                                                "flex items-center gap-3 rounded-full px-4 py-2.5 text-sm font-semibold transition-all",
                                                isActive
                                                    ? "bg-foreground text-background shadow-sm"
                                                    : "text-muted-foreground hover:bg-muted/60 hover:text-foreground"
                                            )}
                                        >
                                            <Icon className="h-4 w-4 shrink-0" />
                                            {link.label}
                                        </Link>
                                    );
                                })}
                            </nav>

                            <div className="mt-auto border-t border-border/50 px-6 py-5 space-y-4">
                                <div className="space-y-1">
                                    <p className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground px-1 mb-2">
                                        Backend
                                    </p>
                                    <PatternToggle />
                                </div>

                                {user ? (
                                    <div className="space-y-2 pt-2 border-t border-border/40">
                                        <p className="text-xs text-muted-foreground px-1">
                                            Signed in as <span className="font-semibold text-foreground">{user.username}</span>
                                        </p>
                                        <button
                                            onClick={handleLogout}
                                            className="flex w-full items-center gap-2 rounded-full px-4 py-2 text-sm font-medium text-destructive hover:bg-destructive/10 transition-colors"
                                        >
                                            <LogOut className="h-4 w-4" />
                                            Sign out
                                        </button>
                                    </div>
                                ) : (
                                    <Link
                                        to="/login"
                                        onClick={() => setMobileOpen(false)}
                                        className="flex w-full items-center justify-center gap-2 rounded-full bg-foreground px-4 py-2.5 text-sm font-semibold text-background transition-all hover:bg-foreground/90"
                                    >
                                        <User className="h-4 w-4" />
                                        Sign in
                                    </Link>
                                )}
                            </div>
                        </SheetContent>
                    </Sheet>
                </div>
            </header>
        </div>
    );
}

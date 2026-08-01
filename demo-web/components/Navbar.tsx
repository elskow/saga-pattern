"use client";

import { useEffect, useRef, useState } from "react";
import { Link, useNavigate } from "@tanstack/react-router";
import { useLocation } from "@tanstack/react-router";
import { CartSidebar } from "./CartSidebar";
import { Logo } from "./Logo";
import { PatternToggle } from "./PatternToggle";
import { cn } from "@/lib/utils";
import { useAuthStore } from "@/lib/store";
import { ChevronDown, LayoutDashboard, LogOut, Menu, User } from "lucide-react";
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
    const [userMenuOpen, setUserMenuOpen] = useState(false);
    const userMenuRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (!userMenuOpen) return;
        const onPointerDown = (event: MouseEvent) => {
            if (!userMenuRef.current?.contains(event.target as Node)) {
                setUserMenuOpen(false);
            }
        };
        const onKeyDown = (event: KeyboardEvent) => {
            if (event.key === "Escape") setUserMenuOpen(false);
        };
        document.addEventListener("mousedown", onPointerDown);
        document.addEventListener("keydown", onKeyDown);
        return () => {
            document.removeEventListener("mousedown", onPointerDown);
            document.removeEventListener("keydown", onKeyDown);
        };
    }, [userMenuOpen]);

    const links = [
        { href: "/", label: "Shop" },
        { href: "/orders", label: "Orders" },
    ];

    const handleLogout = () => {
        logout();
        toast.success("Signed out.");
        setMobileOpen(false);
        setUserMenuOpen(false);
        void navigate({ to: "/" });
    };

    return (
        <header className="fixed inset-x-0 top-0 z-50 h-14 border-b border-border bg-card">
            <div className="mx-auto flex h-full max-w-7xl items-center gap-4 px-5 sm:px-8">
                <Link
                    to="/"
                    id="nav-logo"
                    className="shrink-0 transition-opacity hover:opacity-80"
                >
                    <Logo size={24} />
                </Link>

                <nav className="ml-2 hidden items-center gap-5 md:flex">
                    {links.map((link) => {
                        const isActive = pathname === link.href;
                        return (
                            <Link
                                key={link.href}
                                to={link.href}
                                className={cn(
                                    "text-sm transition-colors",
                                    isActive
                                        ? "font-medium text-foreground"
                                        : "text-muted-foreground hover:text-foreground"
                                )}
                            >
                                {link.label}
                            </Link>
                        );
                    })}
                </nav>

                <div className="ml-auto flex items-center gap-3 sm:gap-4">
                    <CartSidebar />

                    {user ? (
                        <div ref={userMenuRef} className="relative hidden md:block">
                            <button
                                type="button"
                                onClick={() => setUserMenuOpen((open) => !open)}
                                aria-haspopup="menu"
                                aria-expanded={userMenuOpen}
                                className={cn(
                                    "flex items-center gap-2 rounded-md px-2 py-1.5 text-sm transition-colors",
                                    userMenuOpen
                                        ? "bg-secondary text-primary"
                                        : "text-muted-foreground hover:bg-muted/60 hover:text-foreground"
                                )}
                            >
                                <span className="flex h-7 w-7 items-center justify-center rounded-full bg-primary text-[11px] font-semibold text-primary-foreground">
                                    {user.username.slice(0, 2).toUpperCase()}
                                </span>
                                <span className="font-medium text-foreground">{user.username}</span>
                                <ChevronDown
                                    className={cn(
                                        "h-3.5 w-3.5 transition-transform",
                                        userMenuOpen && "rotate-180"
                                    )}
                                />
                            </button>

                            {userMenuOpen ? (
                                <div
                                    role="menu"
                                    className="absolute right-0 top-full z-50 mt-2 w-64 overflow-hidden rounded-md border border-border bg-card py-1 shadow-card"
                                >
                                    <div className="px-3 pb-1 pt-2">
                                        <p className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
                                            Saga pattern
                                        </p>
                                        <div className="mt-1.5 [&>div]:w-full [&_button]:flex-1">
                                            <PatternToggle />
                                        </div>
                                    </div>
                                    <div className="my-1 h-px bg-border" />
                                    <Link
                                        to="/profile"
                                        role="menuitem"
                                        onClick={() => setUserMenuOpen(false)}
                                        className="flex items-center gap-2 px-3 py-2 text-sm text-foreground transition-colors hover:bg-muted/60"
                                    >
                                        <User className="h-4 w-4 text-muted-foreground" />
                                        Profile
                                    </Link>
                                    {user.role === "admin" ? (
                                        <Link
                                            to="/admin"
                                            role="menuitem"
                                            onClick={() => setUserMenuOpen(false)}
                                            className="flex items-center gap-2 px-3 py-2 text-sm text-foreground transition-colors hover:bg-muted/60"
                                        >
                                            <LayoutDashboard className="h-4 w-4 text-muted-foreground" />
                                            Admin dashboard
                                        </Link>
                                    ) : null}
                                    <div className="my-1 h-px bg-border" />
                                    <button
                                        type="button"
                                        role="menuitem"
                                        onClick={handleLogout}
                                        className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-destructive transition-colors hover:bg-destructive/10"
                                    >
                                        <LogOut className="h-4 w-4" />
                                        Sign out
                                    </button>
                                </div>
                            ) : null}
                        </div>
                    ) : (
                        <Link
                            to="/login"
                            className="hidden text-sm font-medium text-foreground transition-colors hover:text-primary md:inline"
                        >
                            Sign in
                        </Link>
                    )}

                    <Sheet open={mobileOpen} onOpenChange={setMobileOpen}>
                        <SheetTrigger
                            render={
                                <Button
                                    variant="ghost"
                                    size="icon"
                                    className="rounded-md md:hidden"
                                    aria-label="Open menu"
                                />
                            }
                        >
                            <Menu className="h-5 w-5" />
                        </SheetTrigger>

                        <SheetContent side="left" className="flex w-80 flex-col bg-card p-0">
                            <SheetHeader className="border-b border-border px-5 py-4">
                                <SheetTitle>
                                    <Logo size={24} />
                                </SheetTitle>
                            </SheetHeader>

                            <div className="flex flex-1 flex-col gap-6 overflow-y-auto p-5">
                                <nav className="flex flex-col gap-0.5">
                                    {links.map((link) => {
                                        const isActive = pathname === link.href;
                                        return (
                                            <Link
                                                key={link.href}
                                                to={link.href}
                                                onClick={() => setMobileOpen(false)}
                                                className={cn(
                                                    "rounded-md px-3 py-2 text-sm transition-colors",
                                                    isActive
                                                        ? "bg-muted font-medium text-foreground"
                                                        : "text-muted-foreground hover:bg-muted/60 hover:text-foreground"
                                                )}
                                            >
                                                {link.label}
                                            </Link>
                                        );
                                    })}
                                </nav>

                                <div className="space-y-2">
                                    <p className="text-xs text-muted-foreground">Order processing</p>
                                    <div className="w-full [&_[role=radiogroup]]:flex [&_[role=radiogroup]]:h-auto [&_[role=radiogroup]]:w-full [&_button]:flex-1">
                                        <PatternToggle />
                                    </div>
                                </div>

                                <div className="mt-auto space-y-1 border-t border-border pt-4">
                                    {user ? (
                                        <>
                                            {user.role === "admin" ? (
                                                <Link
                                                    to="/admin"
                                                    onClick={() => setMobileOpen(false)}
                                                    className="block rounded-md px-3 py-2 text-sm text-foreground hover:bg-muted/60"
                                                >
                                                    Admin
                                                </Link>
                                            ) : (
                                                <Link
                                                    to="/profile"
                                                    onClick={() => setMobileOpen(false)}
                                                    className="block rounded-md px-3 py-2 text-sm text-foreground hover:bg-muted/60"
                                                >
                                                    {user.username}
                                                </Link>
                                            )}
                                            <button
                                                onClick={handleLogout}
                                                className="flex w-full items-center gap-2 rounded-md px-3 py-2 text-sm text-destructive hover:bg-destructive/10"
                                            >
                                                <LogOut className="h-4 w-4" />
                                                Sign out
                                            </button>
                                        </>
                                    ) : (
                                        <Link
                                            to="/login"
                                            onClick={() => setMobileOpen(false)}
                                            className="block rounded-md px-3 py-2 text-sm font-medium text-foreground hover:bg-muted/60"
                                        >
                                            Sign in
                                        </Link>
                                    )}
                                </div>
                            </div>
                        </SheetContent>
                    </Sheet>
                </div>
            </div>
        </header>
    );
}

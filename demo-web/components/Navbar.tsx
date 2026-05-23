"use client";

import { Link } from "@tanstack/react-router";
import { useLocation } from "@tanstack/react-router";
import { CartSidebar } from "./CartSidebar";
import { cn } from "@/lib/utils";

export function Navbar() {
    const pathname = useLocation().pathname;
    const links = [
        { href: "/", label: "Shop" },
        { href: "/orders", label: "Orders" },
    ];

    return (
        <div className="fixed top-0 left-0 right-0 z-50 w-full bg-background/95 backdrop-blur-md border-b border-border/40 shadow-sm supports-[backdrop-filter]:bg-background/60">
            <header className="flex h-16 w-full max-w-7xl mx-auto items-center justify-between px-4 sm:px-6 lg:px-8">

                <Link
                    to="/"
                    id="nav-logo"
                    className="flex items-center gap-2 text-base font-bold tracking-tight text-foreground group"
                >
                    <span className="bg-clip-text text-transparent bg-gradient-to-r from-foreground to-foreground/70">
                        SagaStore
                    </span>
                </Link>

                <nav className="hidden md:flex items-center gap-1 absolute left-1/2 -translate-x-1/2">
                    {links.map((link) => {
                        const isActive = pathname === link.href;
                        return (
                            <Link
                                key={link.href}
                                to={link.href}
                                className={cn(
                                    "px-4 py-1.5 rounded-full text-sm font-medium transition-all duration-300",
                                    isActive
                                        ? "bg-muted text-foreground shadow-sm"
                                        : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
                                )}
                            >
                                {link.label}
                            </Link>
                        );
                    })}
                </nav>

                <div className="flex items-center gap-3 sm:gap-4">
                    <div className="h-5 w-px bg-foreground/10 hidden sm:block" />

                    <CartSidebar />
                </div>
            </header>
        </div>
    );
}

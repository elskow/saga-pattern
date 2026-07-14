"use client";

import { useCartStore } from "@/lib/store";
import { Pattern } from "@/types";
import { cn } from "@/lib/utils";
import { Activity, Network } from "lucide-react";

const patterns: { value: Pattern; label: string; icon: React.ElementType }[] = [
    { value: "choreography", label: "Choreography", icon: Activity },
    { value: "orchestration", label: "Orchestration", icon: Network },
];

export function PatternToggle() {
    const { pattern, setPattern } = useCartStore();

    return (
        <div
            role="radiogroup"
            aria-label="Microservice communication pattern"
            className="flex w-full flex-col gap-1 rounded-[1.25rem] border border-border/80 bg-muted/50 p-1 shadow-sm sm:inline-flex sm:w-auto sm:flex-row sm:rounded-full"
        >
            {patterns.map((p) => {
                const Icon = p.icon;
                const isActive = pattern === p.value;

                return (
                    <button
                        key={p.value}
                        id={`pattern-toggle-${p.value}`}
                        role="radio"
                        aria-checked={isActive}
                        tabIndex={isActive ? 0 : -1} // Standard radio behavior: only active is focusable
                        onClick={() => setPattern(p.value)}
                        onKeyDown={(e) => {
                            if (e.key === "ArrowRight" || e.key === "ArrowLeft") {
                                setPattern(p.value === "choreography" ? "orchestration" : "choreography");
                                document.getElementById(`pattern-toggle-${pattern === "choreography" ? "orchestration" : "choreography"}`)?.focus();
                            }
                        }}
                        className={cn(
                            "group relative flex items-center justify-center gap-2 rounded-full px-4 py-2 text-sm font-medium transition-all",
                            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1",
                            isActive
                                ? "bg-foreground text-background shadow-sm"
                                : "text-muted-foreground hover:bg-background/50 hover:text-foreground"
                        )}
                    >
                        <Icon
                            className={cn(
                                "h-4 w-4 transition-colors",
                                isActive ? "text-background" : "text-muted-foreground group-hover:text-foreground"
                            )}
                        />
                        <span>{p.label}</span>
                    </button>
                );
            })}
        </div>
    );
}

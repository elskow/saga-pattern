"use client";

import { useCartStore } from "@/lib/store";
import { Pattern } from "@/types";
import { cn } from "@/lib/utils";

const patterns: { value: Pattern; label: string }[] = [
    { value: "choreography", label: "Choreography" },
    { value: "orchestration", label: "Orchestration" },
];

export function PatternToggle() {
    const { pattern, setPattern } = useCartStore();

    return (
        <div
            role="radiogroup"
            aria-label="Microservice communication pattern"
            className="inline-flex h-8 items-center gap-0.5 rounded-md border border-border bg-muted/50 p-0.5"
        >
            {patterns.map((p) => {
                const isActive = pattern === p.value;

                return (
                    <button
                        key={p.value}
                        id={`pattern-toggle-${p.value}`}
                        role="radio"
                        aria-checked={isActive}
                        tabIndex={isActive ? 0 : -1}
                        onClick={() => setPattern(p.value)}
                        onKeyDown={(e) => {
                            if (e.key === "ArrowRight" || e.key === "ArrowLeft") {
                                setPattern(p.value === "choreography" ? "orchestration" : "choreography");
                                document.getElementById(`pattern-toggle-${pattern === "choreography" ? "orchestration" : "choreography"}`)?.focus();
                            }
                        }}
                        className={cn(
                            "inline-flex h-7 items-center justify-center rounded-sm px-2.5 text-[12px] font-medium leading-none transition-colors",
                            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1",
                            isActive
                                ? "bg-primary font-semibold text-primary-foreground shadow-sm"
                                : "text-muted-foreground hover:bg-card hover:text-foreground"
                        )}
                    >
                        {p.label}
                    </button>
                );
            })}
        </div>
    );
}

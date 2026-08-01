import { cn } from "@/lib/utils";

interface LogoMarkProps {
    size?: number;
    className?: string;
}

interface LogoProps extends LogoMarkProps {
    withWordmark?: boolean;
}

export function LogoMark({ size = 28, className }: LogoMarkProps) {
    return (
        <svg
            viewBox="0 0 64 64"
            width={size}
            height={size}
            role="img"
            aria-label="SagaStore"
            className={cn("shrink-0", className)}
        >
            <rect width="64" height="64" rx="16" fill="#111111" />
            <path
                d="M18 22c0-4.4 3.6-8 8-8h18v8H26v7h12c4.4 0 8 3.6 8 8v5c0 4.4-3.6 8-8 8H18v-8h20v-7H26c-4.4 0-8-3.6-8-8v-5Z"
                fill="#f8f5ef"
            />
        </svg>
    );
}

export function Logo({ size = 28, className, withWordmark = true }: LogoProps) {
    if (!withWordmark) {
        return <LogoMark size={size} className={className} />;
    }

    return (
        <span className={cn("inline-flex items-center gap-2", className)}>
            <LogoMark size={size} />
            <span className="font-semibold tracking-tight text-foreground">
                SagaStore
            </span>
        </span>
    );
}

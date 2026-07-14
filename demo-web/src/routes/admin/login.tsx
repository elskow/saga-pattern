import { createFileRoute, useNavigate, Link } from "@tanstack/react-router";
import { useState } from "react";
import { useAuthStore } from "@/lib/store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { toast } from "sonner";
import { Eye, EyeOff, ShieldCheck, ArrowLeft } from "lucide-react";

export const Route = createFileRoute("/admin/login")({
  component: AdminLoginPage,
});

function AdminLoginPage() {
  const navigate = useNavigate();
  const login = useAuthStore((s) => s.login);

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!username.trim() || !password.trim()) {
      setError("Please enter your credentials.");
      return;
    }
    setLoading(true);
    setError(null);
    await new Promise((r) => setTimeout(r, 400));
    const result = login(username.trim(), password);
    setLoading(false);
    if (result === "ok") {
      const role = useAuthStore.getState().user?.role;
      if (role !== "admin") {
        useAuthStore.getState().logout();
        setError("This account does not have admin access.");
        return;
      }
      toast.success("Welcome to the admin console.");
      void navigate({ to: "/admin" });
    } else {
      setError("Invalid credentials.");
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center px-4 bg-[#f8f9fa] text-foreground">
      <div className="pointer-events-none fixed inset-0 -z-10 bg-[radial-gradient(ellipse_80%_50%_at_50%_0%,rgba(0,0,0,0.04),transparent)]" />

      <div className="w-full max-w-sm space-y-8">
        <div className="flex flex-col items-center gap-3 text-center">
          <div className="flex h-12 w-12 items-center justify-center rounded-2xl border border-border bg-foreground text-background shadow-lg">
            <ShieldCheck className="h-6 w-6" />
          </div>
          <div>
            <h1 className="text-2xl font-bold tracking-tight text-foreground">Admin Console</h1>
            <p className="mt-1 text-sm text-muted-foreground">Sign in with your administrator credentials.</p>
          </div>
        </div>

        <div className="rounded-2xl border border-border bg-card p-8 shadow-xl shadow-muted/20 space-y-6">
          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="admin-username" className="text-muted-foreground">Username</Label>
              <Input
                id="admin-username"
                type="text"
                placeholder="admin"
                value={username}
                onChange={(e) => { setUsername(e.target.value); setError(null); }}
                disabled={loading}
                autoComplete="username"
                autoFocus
                className="border-border bg-background placeholder:text-muted-foreground/60"
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="admin-password" className="text-muted-foreground">Password</Label>
              <div className="relative">
                <Input
                  id="admin-password"
                  type={showPassword ? "text" : "password"}
                  placeholder="••••••••"
                  value={password}
                  onChange={(e) => { setPassword(e.target.value); setError(null); }}
                  disabled={loading}
                  autoComplete="current-password"
                  className="pr-10 border-border bg-background placeholder:text-muted-foreground/60"
                />
                <button
                  type="button"
                  onClick={() => setShowPassword((v) => !v)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground transition-colors"
                  tabIndex={-1}
                  aria-label={showPassword ? "Hide password" : "Show password"}
                >
                  {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                </button>
              </div>
            </div>

            {error && (
              <p className="rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-sm font-medium text-red-700">{error}</p>
            )}

            <Button type="submit" className="w-full rounded-full bg-foreground text-background hover:bg-foreground/90" disabled={loading}>
              {loading ? "Signing in…" : "Sign in"}
            </Button>
          </form>

          <div className="pt-2 flex justify-center border-t border-border/60 mt-6">
            <Link to="/" className="inline-flex items-center gap-2 text-sm font-medium text-muted-foreground hover:text-foreground transition-colors group mt-4">
              <ArrowLeft className="h-4 w-4 transition-transform group-hover:-translate-x-1" />
              Back to main page
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}

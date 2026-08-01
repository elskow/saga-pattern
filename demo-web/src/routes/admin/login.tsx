import { createFileRoute, useNavigate, Link } from "@tanstack/react-router";
import { useState } from "react";
import { useAuthStore } from "@/lib/store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Logo } from "@/components/Logo";
import { toast } from "sonner";
import { Eye, EyeOff } from "lucide-react";

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
    <div className="flex min-h-screen items-center justify-center bg-background px-4 py-12">
      <div className="w-full max-w-[400px]">
        <div className="mb-6 text-center">
          <Link to="/" className="inline-flex transition-opacity hover:opacity-80">
            <Logo size={32} className="mb-4" />
          </Link>
          <p className="mb-1 text-xs font-medium uppercase tracking-wide text-muted-foreground">Admin</p>
          <h1 className="text-xl font-semibold tracking-tight text-foreground">Sign in</h1>
          <p className="mt-1 text-sm text-muted-foreground">Administrator credentials required.</p>
        </div>

        <div className="rounded-md border border-border bg-card">
          <form onSubmit={handleSubmit} className="space-y-4 p-6">
            <div className="space-y-1.5">
              <Label htmlFor="admin-username" className="text-sm font-medium text-foreground">
                Username
              </Label>
              <Input
                id="admin-username"
                type="text"
                placeholder="admin"
                value={username}
                onChange={(e) => {
                  setUsername(e.target.value);
                  setError(null);
                }}
                disabled={loading}
                autoComplete="username"
                autoFocus
                className="h-10 rounded-md bg-background"
              />
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="admin-password" className="text-sm font-medium text-foreground">
                Password
              </Label>
              <div className="relative">
                <Input
                  id="admin-password"
                  type={showPassword ? "text" : "password"}
                  placeholder="••••••••"
                  value={password}
                  onChange={(e) => {
                    setPassword(e.target.value);
                    setError(null);
                  }}
                  disabled={loading}
                  autoComplete="current-password"
                  className="h-10 rounded-md bg-background pr-10"
                />
                <button
                  type="button"
                  onClick={() => setShowPassword((v) => !v)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground transition-colors hover:text-foreground"
                  tabIndex={-1}
                  aria-label={showPassword ? "Hide password" : "Show password"}
                >
                  {showPassword ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                </button>
              </div>
            </div>

            {error && (
              <p role="alert" className="text-sm text-destructive">
                {error}
              </p>
            )}

            <Button type="submit" size="lg" className="h-10 w-full rounded-md font-medium" disabled={loading}>
              {loading ? "Signing in…" : "Sign in"}
            </Button>
          </form>

          <div className="border-t border-border px-6 py-4">
            <Link
              to="/"
              className="text-sm text-muted-foreground transition-colors hover:text-foreground"
            >
              Back to shop
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}

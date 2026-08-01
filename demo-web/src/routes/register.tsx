import { createFileRoute, useNavigate, Link } from "@tanstack/react-router";
import { useState } from "react";
import { useAuthStore } from "@/lib/store";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Logo } from "@/components/Logo";

export const Route = createFileRoute("/register")({
  component: RegisterPage,
});

function RegisterPage() {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [email, setEmail] = useState("");
  const [address, setAddress] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const register = useAuthStore((s) => s.register);
  const router = useNavigate();

  const handleRegister = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!username.trim() || !password.trim()) {
      setError("Username and password are required.");
      return;
    }
    setLoading(true);
    setError(null);
    await new Promise((r) => setTimeout(r, 400));
    const result = register(username.trim(), password, email, address);
    setLoading(false);
    if (result === "ok") {
      toast.success("Account created successfully!");
      router({ to: "/" });
    } else {
      setError("Username already exists.");
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-background px-4 py-12">
      <div className="w-full max-w-[400px]">
        <div className="mb-6 text-center">
          <Link to="/" className="inline-flex transition-opacity hover:opacity-80">
            <Logo size={32} className="mb-4" />
          </Link>
          <h1 className="text-xl font-semibold tracking-tight text-foreground">Create account</h1>
          <p className="mt-1 text-sm text-muted-foreground">Join SagaStore to track orders.</p>
        </div>

        <div className="rounded-md border border-border bg-card">
          <form onSubmit={handleRegister} className="space-y-4 p-6">
            <div className="space-y-1.5">
              <Label htmlFor="reg-username" className="text-sm font-medium text-foreground">
                Username
              </Label>
              <Input
                id="reg-username"
                type="text"
                required
                value={username}
                onChange={(e) => {
                  setUsername(e.target.value);
                  setError(null);
                }}
                disabled={loading}
                placeholder="Choose a username"
                autoFocus
                className="h-10 rounded-md bg-background"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="reg-password" className="text-sm font-medium text-foreground">
                Password
              </Label>
              <Input
                id="reg-password"
                type="password"
                required
                value={password}
                onChange={(e) => {
                  setPassword(e.target.value);
                  setError(null);
                }}
                disabled={loading}
                placeholder="Choose a password"
                className="h-10 rounded-md bg-background"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="reg-email" className="text-sm font-medium text-foreground">
                Email <span className="font-normal text-muted-foreground">(optional)</span>
              </Label>
              <Input
                id="reg-email"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                disabled={loading}
                placeholder="you@example.com"
                className="h-10 rounded-md bg-background"
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="reg-address" className="text-sm font-medium text-foreground">
                Default address <span className="font-normal text-muted-foreground">(optional)</span>
              </Label>
              <Input
                id="reg-address"
                type="text"
                value={address}
                onChange={(e) => setAddress(e.target.value)}
                disabled={loading}
                placeholder="123 Main St"
                className="h-10 rounded-md bg-background"
              />
            </div>

            {error && (
              <p role="alert" className="text-sm text-destructive">
                {error}
              </p>
            )}

            <Button type="submit" size="lg" className="h-10 w-full rounded-md font-medium" disabled={loading}>
              {loading ? "Creating account…" : "Create account"}
            </Button>
          </form>

          <div className="border-t border-border px-6 py-4">
            <p className="text-sm text-muted-foreground">
              Already have an account?{" "}
              <Link to="/login" className="font-medium text-primary hover:underline">
                Sign in
              </Link>
            </p>
            <Link
              to="/"
              className="mt-2 inline-block text-sm text-muted-foreground transition-colors hover:text-foreground"
            >
              Back to shop
            </Link>
          </div>
        </div>

        <p className="mt-4 text-xs text-muted-foreground">
          Sample: <span className="font-mono">user</span> / <span className="font-mono">user123</span>
        </p>
      </div>
    </div>
  );
}

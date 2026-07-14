import { createFileRoute, useNavigate, Link } from '@tanstack/react-router';
import { useState } from 'react';
import { useAuthStore } from '@/lib/store';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { UserPlus, Zap } from 'lucide-react';

export const Route = createFileRoute('/register')({
  component: RegisterPage,
});

function RegisterPage() {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [email, setEmail] = useState('');
  const [address, setAddress] = useState('');
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
    <div className="min-h-screen flex items-center justify-center bg-background px-4">
      {/* Subtle background gradient — matches login */}
      <div className="pointer-events-none fixed inset-0 -z-10 bg-[radial-gradient(ellipse_80%_60%_at_50%_-10%,rgba(0,0,0,0.06),transparent)]" />

      <div className="w-full max-w-sm space-y-8">
        {/* Brand */}
        <div className="flex flex-col items-center gap-3 text-center">
          <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-foreground text-background shadow-lg">
            <UserPlus className="h-6 w-6" />
          </div>
          <div>
            <h1 className="text-2xl font-bold tracking-tight text-foreground">Create an account</h1>
            <p className="mt-1 text-sm text-muted-foreground">Join SagaStore to track your orders.</p>
          </div>
        </div>

        {/* Card */}
        <div className="rounded-2xl border border-border bg-card p-8 shadow-sm space-y-6">
          <form onSubmit={handleRegister} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="reg-username">Username</Label>
              <Input
                id="reg-username"
                type="text"
                required
                value={username}
                onChange={(e) => { setUsername(e.target.value); setError(null); }}
                disabled={loading}
                placeholder="Choose a username"
                autoFocus
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="reg-password">Password</Label>
              <Input
                id="reg-password"
                type="password"
                required
                value={password}
                onChange={(e) => { setPassword(e.target.value); setError(null); }}
                disabled={loading}
                placeholder="Choose a password"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="reg-email">
                Email <span className="text-muted-foreground font-normal">(optional)</span>
              </Label>
              <Input
                id="reg-email"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                disabled={loading}
                placeholder="you@example.com"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="reg-address">
                Default address <span className="text-muted-foreground font-normal">(optional)</span>
              </Label>
              <Input
                id="reg-address"
                type="text"
                value={address}
                onChange={(e) => setAddress(e.target.value)}
                disabled={loading}
                placeholder="123 Main St"
              />
            </div>

            {error && (
              <p className="text-sm text-destructive font-medium">{error}</p>
            )}

            <Button type="submit" className="w-full rounded-full" disabled={loading}>
              {loading ? "Creating account…" : "Create account"}
            </Button>
          </form>

          <div className="flex items-start gap-2.5 rounded-xl border border-border bg-muted/40 px-3.5 py-3 text-xs text-muted-foreground">
            <Zap className="h-3.5 w-3.5 mt-0.5 shrink-0 text-amber-500" />
            <div>
              <span className="font-semibold text-foreground">Already have an account?</span>
              <br />
              <Link to="/login" className="font-semibold text-foreground hover:underline">
                Sign in here
              </Link>
              {" · "}Sample account: <code className="font-mono">user</code> / <code className="font-mono">user123</code>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

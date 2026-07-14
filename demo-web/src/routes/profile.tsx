import { createFileRoute, useNavigate, Link } from '@tanstack/react-router';
import { useState, useEffect } from 'react';
import { useAuthStore } from '@/lib/store';
import { toast } from 'sonner';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { UserCircle, Save, LogOut, Package, Mail, MapPin, ShieldCheck } from 'lucide-react';

export const Route = createFileRoute('/profile')({
  component: ProfilePage,
});

function ProfilePage() {
  const user = useAuthStore((s) => s.user);
  const updateProfile = useAuthStore((s) => s.updateProfile);
  const logout = useAuthStore((s) => s.logout);
  const router = useNavigate();

  const [email, setEmail] = useState('');
  const [address, setAddress] = useState('');

  useEffect(() => {
    if (!user) {
      router({ to: "/login" });
    } else {
      setEmail(user.email || '');
      setAddress(user.address || '');
    }
  }, [user, router]);

  if (!user) return null;

  const handleSave = (e: React.FormEvent) => {
    e.preventDefault();
    updateProfile({ email, address });
    toast.success("Profile updated successfully");
  };

  const handleLogout = () => {
    logout();
    router({ to: "/" });
  };

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-10 space-y-8">
      {/* Page header — matches orders/checkout header pattern */}
      <div className="flex flex-col sm:flex-row sm:items-end justify-between gap-4 border-b border-border/60 pb-5">
        <div className="flex items-center gap-4">
          <div className="h-14 w-14 rounded-full border border-border/60 bg-muted/30 flex items-center justify-center shadow-sm">
            <UserCircle className="h-7 w-7 text-muted-foreground" />
          </div>
          <div>
            <h1 className="text-3xl font-bold tracking-tight text-foreground">Your Profile</h1>
            <p className="text-sm text-muted-foreground mt-1">Manage your account and preferences</p>
          </div>
        </div>
        <Button
          variant="outline"
          onClick={handleLogout}
          className="rounded-full gap-2 text-xs font-medium text-destructive hover:bg-destructive/10 hover:text-destructive border-border/60 shadow-sm"
        >
          <LogOut className="h-3.5 w-3.5" />
          Logout
        </Button>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        {/* Sidebar — Account Info */}
        <div className="lg:col-span-1 space-y-6">
          <div className="rounded-2xl border border-border/60 bg-card p-6 shadow-sm">
            <h3 className="text-lg font-semibold tracking-tight mb-4">Account Info</h3>
            <div className="space-y-5">
              <div className="flex items-start gap-3">
                <div className="h-8 w-8 rounded-full bg-muted/50 flex items-center justify-center shrink-0 mt-0.5">
                  <UserCircle className="h-4 w-4 text-muted-foreground" />
                </div>
                <div>
                  <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Username</p>
                  <p className="font-medium text-foreground mt-0.5">{user.username}</p>
                </div>
              </div>
              <div className="flex items-start gap-3">
                <div className="h-8 w-8 rounded-full bg-muted/50 flex items-center justify-center shrink-0 mt-0.5">
                  <ShieldCheck className="h-4 w-4 text-muted-foreground" />
                </div>
                <div>
                  <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Role</p>
                  <p className="mt-1">
                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full bg-muted text-xs font-medium capitalize text-foreground">
                      {user.role}
                    </span>
                  </p>
                </div>
              </div>
              {user.email && (
                <div className="flex items-start gap-3">
                  <div className="h-8 w-8 rounded-full bg-muted/50 flex items-center justify-center shrink-0 mt-0.5">
                    <Mail className="h-4 w-4 text-muted-foreground" />
                  </div>
                  <div>
                    <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Email</p>
                    <p className="font-medium text-foreground mt-0.5">{user.email}</p>
                  </div>
                </div>
              )}
              {user.address && (
                <div className="flex items-start gap-3">
                  <div className="h-8 w-8 rounded-full bg-muted/50 flex items-center justify-center shrink-0 mt-0.5">
                    <MapPin className="h-4 w-4 text-muted-foreground" />
                  </div>
                  <div>
                    <p className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">Address</p>
                    <p className="font-medium text-foreground mt-0.5">{user.address}</p>
                  </div>
                </div>
              )}
            </div>

            <div className="mt-6 pt-5 border-t border-border/60">
              <Link to="/orders">
                <Button variant="outline" className="w-full rounded-full gap-2 text-xs font-medium shadow-sm hover:bg-muted/50">
                  <Package className="h-3.5 w-3.5" />
                  View Order History
                </Button>
              </Link>
            </div>
          </div>
        </div>

        {/* Main — Edit Form */}
        <div className="lg:col-span-2">
          <form onSubmit={handleSave} className="rounded-2xl border border-border/60 bg-card shadow-sm">
            <div className="p-6 space-y-1.5">
              <h3 className="text-lg font-semibold tracking-tight">Shipping Details</h3>
              <p className="text-sm text-muted-foreground">These details will be pre-filled during checkout.</p>
            </div>

            <div className="px-6 pb-6 space-y-5">
              <div className="rounded-2xl border border-border/60 bg-muted/20 p-6 space-y-5">
                <div className="space-y-2">
                  <Label htmlFor="profile-email" className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                    Email Address
                  </Label>
                  <Input
                    id="profile-email"
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    className="bg-card rounded-xl border-border/60 h-11 focus-visible:ring-1"
                    placeholder="you@example.com"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="profile-address" className="text-xs font-semibold text-muted-foreground uppercase tracking-wider">
                    Default Shipping Address
                  </Label>
                  <Input
                    id="profile-address"
                    type="text"
                    value={address}
                    onChange={(e) => setAddress(e.target.value)}
                    className="bg-card rounded-xl border-border/60 h-11 focus-visible:ring-1"
                    placeholder="123 Main St, City, State"
                  />
                </div>
              </div>
            </div>

            <div className="px-6 pb-6 flex justify-end border-t border-border/60 pt-5">
              <Button type="submit" className="rounded-full shadow-sm gap-2 font-semibold text-sm">
                <Save className="h-4 w-4" />
                Save Changes
              </Button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}

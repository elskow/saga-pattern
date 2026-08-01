import { createFileRoute, useNavigate, Link } from "@tanstack/react-router";
import { useState, useEffect } from "react";
import { useAuthStore } from "@/lib/store";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";

export const Route = createFileRoute("/profile")({
  component: ProfilePage,
});

function ProfilePage() {
  const user = useAuthStore((s) => s.user);
  const updateProfile = useAuthStore((s) => s.updateProfile);
  const router = useNavigate();

  const [email, setEmail] = useState("");
  const [address, setAddress] = useState("");

  useEffect(() => {
    if (!user) {
      router({ to: "/login" });
    } else {
      setEmail(user.email || "");
      setAddress(user.address || "");
    }
  }, [user, router]);

  if (!user) return null;

  const handleSave = (e: React.FormEvent) => {
    e.preventDefault();
    updateProfile({ email, address });
    toast.success("Profile updated successfully");
  };

  return (
    <div className="space-y-8">
      <div className="border-b border-border pb-6">
        <h1 className="text-xl font-semibold tracking-tight text-foreground">Profile</h1>
        <p className="mt-0.5 text-sm text-muted-foreground">Manage account details</p>
      </div>

      <div className="grid grid-cols-1 gap-6 lg:grid-cols-3">
        <div className="lg:col-span-1">
          <div className="rounded-md border border-border bg-card p-5">
            <h3 className="mb-4 text-sm font-medium text-foreground">Account info</h3>
            <div className="space-y-4">
              <div>
                <p className="text-xs text-muted-foreground">Username</p>
                <p className="mt-0.5 text-sm font-medium text-foreground">{user.username}</p>
              </div>
              <div>
                <p className="text-xs text-muted-foreground">Role</p>
                <p className="mt-0.5 text-sm capitalize text-foreground">{user.role}</p>
              </div>
              {user.email && (
                <div>
                  <p className="text-xs text-muted-foreground">Email</p>
                  <p className="mt-0.5 text-sm font-medium text-foreground">{user.email}</p>
                </div>
              )}
              {user.address && (
                <div>
                  <p className="text-xs text-muted-foreground">Address</p>
                  <p className="mt-0.5 text-sm font-medium text-foreground">{user.address}</p>
                </div>
              )}
            </div>

            <div className="mt-5 border-t border-border pt-4">
              <Link to="/orders">
                <Button variant="outline" className="h-9 w-full rounded-md text-sm font-medium">
                  View order history
                </Button>
              </Link>
            </div>
          </div>
        </div>

        <div className="lg:col-span-2">
          <form onSubmit={handleSave} className="overflow-hidden rounded-md border border-border bg-card">
            <div className="space-y-1 border-b border-border px-5 py-4">
              <h3 className="text-sm font-medium text-foreground">Shipping details</h3>
              <p className="text-sm text-muted-foreground">Pre-filled at checkout.</p>
            </div>

            <div className="space-y-4 px-5 py-5">
              <div className="space-y-1.5">
                <Label htmlFor="profile-email" className="text-sm font-medium text-foreground">
                  Email address
                </Label>
                <Input
                  id="profile-email"
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="h-10 bg-background"
                  placeholder="you@example.com"
                />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="profile-address" className="text-sm font-medium text-foreground">
                  Default shipping address
                </Label>
                <Input
                  id="profile-address"
                  type="text"
                  value={address}
                  onChange={(e) => setAddress(e.target.value)}
                  className="h-10 bg-background"
                  placeholder="123 Main St, City, State"
                />
              </div>
            </div>

            <div className="flex justify-end border-t border-border px-5 py-3">
              <Button type="submit" className="h-9 rounded-md text-sm font-medium">
                Save changes
              </Button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}

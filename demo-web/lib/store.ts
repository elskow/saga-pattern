import { CartItem, CatalogProduct, Pattern } from "@/types";
import { create } from "zustand";
import { persist } from "zustand/middleware";

export type UserRole = "admin" | "user";

export interface AuthUser {
  username: string;
  role: UserRole;
  email?: string;
  address?: string;
}

type LoginResult = "ok" | "invalid-credentials";
type RegisterResult = "ok" | "user-exists";

interface AuthStore {
  user: AuthUser | null;
  users: Record<string, { password: string; role: UserRole; email?: string; address?: string }>;
  login: (username: string, password: string) => LoginResult;
  register: (username: string, password: string, email?: string, address?: string) => RegisterResult;
  updateProfile: (updates: Partial<AuthUser>) => void;
  logout: () => void;
  isAdmin: () => boolean;
  isLoggedIn: () => boolean;
}

const USERS: Record<string, { password: string; role: UserRole }> = {
  admin: { password: "admin123", role: "admin" },
  user:  { password: "user123",  role: "user"  },
};

export const useAuthStore = create<AuthStore>()(
  persist(
    (set, get) => ({
      user: null,
      users: { ...USERS },

      login: (username, password) => {
        const record = get().users[username.toLowerCase()];
        if (!record || record.password !== password) {
          return "invalid-credentials";
        }
        set({ user: { username: username.toLowerCase(), role: record.role, email: record.email, address: record.address } });
        return "ok";
      },

      register: (username, password, email, address) => {
        const normalized = username.toLowerCase();
        if (get().users[normalized]) {
          return "user-exists";
        }
        set((state) => ({
          users: { ...state.users, [normalized]: { password, role: "user", email, address } },
          user: { username: normalized, role: "user", email, address }
        }));
        return "ok";
      },

      updateProfile: (updates) => {
        set((state) => {
          if (!state.user) return state;
          const updatedUser = { ...state.user, ...updates };
          const updatedUsers = { ...state.users };
          if (updatedUsers[state.user.username]) {
            updatedUsers[state.user.username] = {
              ...updatedUsers[state.user.username],
              email: updatedUser.email,
              address: updatedUser.address
            };
          }
          return { user: updatedUser, users: updatedUsers };
        });
      },

      logout: () => set({ user: null }),

      isAdmin: () => get().user?.role === "admin",

      isLoggedIn: () => get().user !== null,
    }),
    {
      name: "saga-demo-auth",
      partialize: (state) => ({ user: state.user, users: state.users }),
    }
  )
);

type CartMutationReason = "out-of-stock" | "limit";

interface CartMutationResult {
  addedQuantity: number;
  quantity: number;
  stock: number;
  reason: CartMutationReason | null;
}

interface ReconciledCartItem {
  item: CartItem;
  stock: number;
}

interface CartReconciliationResult {
  removed: CartItem[];
  clamped: ReconciledCartItem[];
}

interface CartStore {
  items: CartItem[];
  pattern: Pattern;
  hasHydrated: boolean;
  addItem: (item: CartItem) => CartMutationResult;
  removeItem: (productId: string) => void;
  updateQuantity: (productId: string, quantity: number) => void;
  reconcileWithCatalog: (catalog: CatalogProduct[]) => CartReconciliationResult;
  clearCart: () => void;
  setHasHydrated: (hasHydrated: boolean) => void;
  setPattern: (pattern: Pattern) => void;
  totalItems: () => number;
  totalPrice: () => number;
}

export const useCartStore = create<CartStore>()(
  persist(
    (set, get) => ({
      items: [],
      pattern: "choreography",
      hasHydrated: false,

      addItem: (newItem) => {
        const stock = Math.max(0, newItem.product.stock);
        const existing = get().items.find((item) => item.product.id === newItem.product.id);
        const currentQuantity = existing?.quantity ?? 0;

        if (stock <= 0 || newItem.quantity <= 0) {
          return { addedQuantity: 0, quantity: currentQuantity, stock, reason: "out-of-stock" };
        }

        const nextQuantity = Math.min(currentQuantity + newItem.quantity, stock);
        const addedQuantity = nextQuantity - currentQuantity;

        if (addedQuantity <= 0) {
          return { addedQuantity: 0, quantity: currentQuantity, stock, reason: "limit" };
        }

        set((state) => ({
          items: existing
            ? state.items.map((item) =>
                item.product.id === newItem.product.id
                  ? { ...item, product: newItem.product, quantity: nextQuantity }
                  : item
              )
            : [...state.items, { ...newItem, quantity: nextQuantity }],
        }));

        return {
          addedQuantity,
          quantity: nextQuantity,
          stock,
          reason: addedQuantity < newItem.quantity ? "limit" : null,
        };
      },

      removeItem: (productId) => {
        set((state) => ({
          items: state.items.filter((i) => i.product.id !== productId),
        }));
      },

      updateQuantity: (productId, quantity) => {
        set((state) => ({
          items: state.items.flatMap((item) => {
            if (item.product.id !== productId) {
              return [item];
            }

            const stock = Math.max(0, item.product.stock);
            const nextQuantity = Math.min(quantity, stock);
            return nextQuantity <= 0 ? [] : [{ ...item, quantity: nextQuantity }];
          }),
        }));
      },

      reconcileWithCatalog: (catalog) => {
        const liveProducts = new Map(catalog.map((product) => [product.productId, product]));
        const removed: CartItem[] = [];
        const clamped: ReconciledCartItem[] = [];
        const nextItems: CartItem[] = [];

        for (const item of get().items) {
          const liveProduct = liveProducts.get(item.product.productId);
          const stock = Math.max(0, liveProduct?.stock ?? 0);

          if (!liveProduct || stock <= 0) {
            removed.push(item);
            continue;
          }

          const quantity = Math.min(item.quantity, stock);
          if (quantity < item.quantity) {
            clamped.push({ item, stock });
          }

          nextItems.push({ product: liveProduct, quantity });
        }

        set({ items: nextItems });
        return { removed, clamped };
      },

      clearCart: () => set({ items: [] }),

      setHasHydrated: (hasHydrated) => set({ hasHydrated }),

      setPattern: (pattern) => set({ pattern }),

      totalItems: () =>
        get().items.reduce((sum, item) => sum + item.quantity, 0),

      totalPrice: () =>
        get().items.reduce(
          (sum, item) => sum + item.product.price * item.quantity,
          0
        ),
    }),
    {
      name: "saga-demo-cart",
      partialize: (state) => ({ items: state.items, pattern: state.pattern }),
      onRehydrateStorage: () => (state) => {
        state?.setHasHydrated(true);
      },
    }
  )
);

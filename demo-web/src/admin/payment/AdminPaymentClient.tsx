"use client";

import { useState } from "react";
import {
  fetchDepositBalanceServer,
  setDepositBalanceServer,
  DEPOSIT_SERVICE_KEYS,
  DEPOSIT_SERVICE_LABELS_MAP,
} from "@/lib/admin";
import { DepositBalanceStatus, DepositServiceKey } from "@/types";
import { Banknote, Gauge } from "lucide-react";
import { formatPrice } from "@/lib/currency";

interface AdminPaymentClientProps {
  initialBalances: Record<string, DepositBalanceStatus>;
}

export default function AdminPaymentClient({ initialBalances }: AdminPaymentClientProps) {
  const [balances, setBalances] = useState<Record<string, DepositBalanceStatus>>(initialBalances);
  const [loading, setLoading] = useState<Record<string, boolean>>({});
  const [editing, setEditing] = useState<Record<string, boolean>>({});
  const [inputValues, setInputValues] = useState<Record<string, string>>(
    Object.fromEntries(
      Object.entries(initialBalances).map(([key, status]) => [key, status.unlimited ? "" : status.balance])
    )
  );

  const refreshOne = async (key: string) => {
    try {
      const status = await fetchDepositBalanceServer({ data: key as DepositServiceKey });
      setBalances((prev) => ({ ...prev, [key]: status }));
      setInputValues((prev) => ({ ...prev, [key]: status.unlimited ? "" : status.balance }));
    } catch {
      setBalances((prev) => ({ ...prev, [key]: { balance: "", unlimited: true } }));
      setInputValues((prev) => ({ ...prev, [key]: "" }));
    }
  };

  const save = async (key: string) => {
    setLoading((prev) => ({ ...prev, [key]: true }));
    setEditing((prev) => ({ ...prev, [key]: false }));
    try {
      const val = inputValues[key];
      await setDepositBalanceServer({ data: { key: key as DepositServiceKey, balance: val || null } });
      await refreshOne(key);
    } catch (err) {
      console.error(`Failed to update ${key}:`, err);
    } finally {
      setLoading((prev) => ({ ...prev, [key]: false }));
    }
  };

  const startEdit = (key: string) => {
    const current = balances[key];
    setInputValues((prev) => ({
      ...prev,
      [key]: current?.unlimited ? "" : current.balance,
    }));
    setEditing((prev) => ({ ...prev, [key]: true }));
  };

  const resetUnlimited = async (key: string) => {
    setLoading((prev) => ({ ...prev, [key]: true }));
    try {
      await setDepositBalanceServer({ data: { key: key as DepositServiceKey, balance: null } });
      await refreshOne(key);
    } catch (err) {
      console.error(`Failed to reset ${key}:`, err);
    } finally {
      setLoading((prev) => ({ ...prev, [key]: false }));
    }
  };

  return (
    <div className="space-y-8 w-full max-w-none">
      <div className="border-b border-border/60 pb-5 mb-6">
        <h1 className="text-2xl font-bold tracking-tight text-foreground">Payment</h1>
        <p className="text-sm text-muted-foreground mt-1">
          Manage deposit balances for payment services
        </p>
      </div>
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold tracking-tight">Deposit Balance</h2>
            <p className="text-sm text-muted-foreground mt-0.5">
              Set available deposit per payment service. Payments exceeding balance will fail. Leave empty for unlimited.
            </p>
          </div>
          <div className="flex items-center gap-2 rounded-full border border-emerald-200 bg-emerald-50/50 px-3 py-1.5 text-xs font-medium text-emerald-800">
            <Banknote className="h-3.5 w-3.5" />
            Wallet
          </div>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {DEPOSIT_SERVICE_KEYS.map((key) => {
            const status = balances[key] ?? { balance: "", unlimited: true };
            const isLoading = loading[key] ?? false;
            const isEditing = editing[key] ?? false;
            const label = (DEPOSIT_SERVICE_LABELS_MAP as Record<string, string>)[key] ?? key;
            return (
              <div
                key={key}
                className="rounded-xl border border-border bg-card p-5 shadow-sm space-y-3"
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <div className={`flex h-10 w-10 items-center justify-center rounded-lg ${status.unlimited ? "bg-emerald-100 dark:bg-emerald-900/30" : "bg-blue-100 dark:bg-blue-900/30"}`}>
                      <Banknote className={`h-5 w-5 ${status.unlimited ? "text-emerald-600 dark:text-emerald-400" : "text-blue-600 dark:text-blue-400"}`} />
                    </div>
                    <div>
                      <p className="text-sm font-semibold text-foreground">{label}</p>
                      <p className="text-xs text-muted-foreground">
                        {status.unlimited ? "Unlimited balance" : `Balance: ${formatPrice(parseFloat(status.balance))}`}
                      </p>
                    </div>
                  </div>
                  <Gauge className={`h-4 w-4 ${status.unlimited ? "text-emerald-500" : "text-blue-500"}`} />
                </div>
                {isEditing ? (
                  <div className="flex items-center gap-2 pt-2 border-t border-border/50">
                    <div className="relative flex-1">
                      <span className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground text-sm font-medium">Rp</span>
                      <input
                        type="text"
                        inputMode="numeric"
                        pattern="[0-9]*"
                        placeholder="e.g. 50000000"
                        value={inputValues[key] ?? ""}
                        onChange={(e) => setInputValues((prev) => ({ ...prev, [key]: e.target.value }))}
                        className="w-full pl-10 pr-3 py-2 text-sm rounded-lg border border-border bg-background focus:outline-none focus:ring-2 focus:ring-ring"
                        autoFocus
                        onKeyDown={(e) => {
                          if (e.key === "Enter") save(key);
                          if (e.key === "Escape") setEditing((prev) => ({ ...prev, [key]: false }));
                        }}
                      />
                    </div>
                    <button
                      onClick={() => save(key)}
                      disabled={isLoading}
                      className="px-3 py-2 text-xs font-semibold rounded-lg bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50 transition-colors"
                    >
                      Save
                    </button>
                    <button
                      onClick={() => setEditing((prev) => ({ ...prev, [key]: false }))}
                      className="px-3 py-2 text-xs font-semibold rounded-lg border border-border hover:bg-muted transition-colors"
                    >
                      Cancel
                    </button>
                  </div>
                ) : (
                  <div className="flex items-center gap-2 pt-2 border-t border-border/50">
                    <button
                      onClick={() => startEdit(key)}
                      disabled={isLoading}
                      className="flex-1 px-3 py-2 text-xs font-semibold rounded-lg border border-border hover:bg-muted transition-colors disabled:opacity-50"
                    >
                      {status.unlimited ? "Set Balance" : "Edit Balance"}
                    </button>
                    {!status.unlimited && (
                      <button
                        onClick={() => resetUnlimited(key)}
                        disabled={isLoading}
                        className="px-3 py-2 text-xs font-semibold rounded-lg border border-emerald-200 text-emerald-700 hover:bg-emerald-50 dark:border-emerald-800 dark:text-emerald-400 dark:hover:bg-emerald-900/20 transition-colors disabled:opacity-50"
                      >
                        Reset
                      </button>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

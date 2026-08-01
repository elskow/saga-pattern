"use client";

import { useState } from "react";
import {
  fetchDepositBalanceServer,
  setDepositBalanceServer,
  DEPOSIT_SERVICE_KEYS,
  DEPOSIT_SERVICE_LABELS_MAP,
} from "@/lib/admin";
import { DepositBalanceStatus, DepositServiceKey } from "@/types";
import { AlertCircle, RefreshCw } from "lucide-react";
import { formatCurrencyInput, formatPrice, normalizeCurrencyInput } from "@/lib/currency";
import { Button } from "@/components/ui/button";

interface AdminPaymentClientProps {
  initialBalances: Record<string, DepositBalanceStatus>;
  initialError: string | null;
}

const getErrorMessage = (err: unknown, fallback: string) => (err instanceof Error ? err.message : fallback);

export default function AdminPaymentClient({ initialBalances, initialError }: AdminPaymentClientProps) {
  const [balances, setBalances] = useState<Record<string, DepositBalanceStatus>>(initialBalances);
  const [loading, setLoading] = useState<Record<string, boolean>>({});
  const [editing, setEditing] = useState<Record<string, boolean>>({});
  const [error, setError] = useState<string | null>(initialError);
  const [inputValues, setInputValues] = useState<Record<string, string>>(
    Object.fromEntries(
      Object.entries(initialBalances).map(([key, status]) => [
        key,
        status.unlimited ? "" : formatCurrencyInput(status.balance),
      ])
    )
  );

  const refreshOne = async (key: string) => {
    try {
      const status = await fetchDepositBalanceServer({ data: key as DepositServiceKey });
      setBalances((prev) => ({ ...prev, [key]: status }));
      setInputValues((prev) => ({
        ...prev,
        [key]: status.unlimited ? "" : formatCurrencyInput(status.balance),
      }));
      setError(null);
    } catch (err) {
      setError(getErrorMessage(err, `Failed to refresh ${key} balance`));
      throw err;
    }
  };

  const refreshAll = async () => {
    setError(null);
    const results = await Promise.allSettled(DEPOSIT_SERVICE_KEYS.map((key) => refreshOne(key)));
    const rejected = results.find((result) => result.status === "rejected");
    if (rejected && rejected.status === "rejected") {
      setError(getErrorMessage(rejected.reason, "One or more payment balances could not be refreshed"));
    }
  };

  const save = async (key: string) => {
    setLoading((prev) => ({ ...prev, [key]: true }));
    setEditing((prev) => ({ ...prev, [key]: false }));
    try {
      const val = normalizeCurrencyInput(inputValues[key] ?? "");
      await setDepositBalanceServer({ data: { key: key as DepositServiceKey, balance: val || null } });
      await refreshOne(key);
    } catch (err) {
      setError(getErrorMessage(err, `Failed to update ${key}`));
    } finally {
      setLoading((prev) => ({ ...prev, [key]: false }));
    }
  };

  const startEdit = (key: string) => {
    const current = balances[key];
    setInputValues((prev) => ({
      ...prev,
      [key]: current?.unlimited ? "" : formatCurrencyInput(current.balance),
    }));
    setEditing((prev) => ({ ...prev, [key]: true }));
  };

  const resetUnlimited = async (key: string) => {
    setLoading((prev) => ({ ...prev, [key]: true }));
    try {
      await setDepositBalanceServer({ data: { key: key as DepositServiceKey, balance: null } });
      await refreshOne(key);
    } catch (err) {
      setError(getErrorMessage(err, `Failed to reset ${key}`));
    } finally {
      setLoading((prev) => ({ ...prev, [key]: false }));
    }
  };

  return (
    <div className="w-full max-w-none space-y-8">
      <div className="mb-2 flex flex-col gap-3 border-b border-border pb-5 sm:flex-row sm:items-end sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-foreground">Payment</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Manage deposit balances for payment services
          </p>
        </div>
        <Button variant="outline" size="sm" onClick={refreshAll} className="gap-1.5 text-xs">
          <RefreshCw className="h-3.5 w-3.5" />
          Refresh balances
        </Button>
      </div>

      {error && (
        <div className="flex items-center gap-2 rounded-md border border-amber-200/80 bg-amber-50 p-3 text-sm text-amber-800">
          <AlertCircle className="h-4 w-4 shrink-0 text-amber-600" />
          <p className="font-medium">{error}</p>
        </div>
      )}

      <div className="space-y-4">
        <div>
          <h2 className="text-base font-semibold tracking-tight text-foreground">Deposit balance</h2>
          <p className="mt-0.5 text-sm text-muted-foreground">
            Set available deposit per payment service. Payments over balance fail. Leave empty for unlimited.
          </p>
        </div>

        <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
          {DEPOSIT_SERVICE_KEYS.map((key) => {
            const status = balances[key] ?? { balance: "", unlimited: true };
            const isLoading = loading[key] ?? false;
            const isEditing = editing[key] ?? false;
            const label = (DEPOSIT_SERVICE_LABELS_MAP as Record<string, string>)[key] ?? key;
            return (
              <div key={key} className="space-y-3 rounded-md border border-border bg-card p-4">
                <div className="min-w-0">
                  <p className="text-sm font-semibold text-foreground">{label}</p>
                  <p className="text-xs text-muted-foreground">
                    {status.unlimited
                      ? "Unlimited balance"
                      : `Balance: ${formatPrice(parseFloat(status.balance))}`}
                  </p>
                </div>

                {isEditing ? (
                  <div className="flex items-center gap-2 border-t border-border pt-3">
                    <div className="relative flex-1">
                      <span className="absolute left-3 top-1/2 -translate-y-1/2 text-sm font-medium text-muted-foreground">
                        Rp
                      </span>
                      <input
                        type="text"
                        inputMode="numeric"
                        pattern="[0-9]*"
                        placeholder="e.g. 50.000.000"
                        value={inputValues[key] ?? ""}
                        onChange={(e) =>
                          setInputValues((prev) => ({
                            ...prev,
                            [key]: formatCurrencyInput(e.target.value),
                          }))
                        }
                        className="w-full rounded-md border border-border bg-background py-2 pl-10 pr-3 text-sm outline-none focus:border-primary/40 focus:ring-1 focus:ring-ring"
                        autoFocus
                        onKeyDown={(e) => {
                          if (e.key === "Enter") save(key);
                          if (e.key === "Escape")
                            setEditing((prev) => ({ ...prev, [key]: false }));
                        }}
                      />
                    </div>
                    <Button size="sm" onClick={() => save(key)} disabled={isLoading} className="rounded-md text-xs">
                      Save
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setEditing((prev) => ({ ...prev, [key]: false }))}
                      className="rounded-md text-xs"
                    >
                      Cancel
                    </Button>
                  </div>
                ) : (
                  <div className="flex items-center gap-2 border-t border-border pt-3">
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => startEdit(key)}
                      disabled={isLoading}
                      className="flex-1 rounded-md text-xs"
                    >
                      {status.unlimited ? "Set balance" : "Edit balance"}
                    </Button>
                    {!status.unlimited && (
                      <Button
                        variant="secondary"
                        size="sm"
                        onClick={() => resetUnlimited(key)}
                        disabled={isLoading}
                        className="rounded-md text-xs"
                      >
                        Reset
                      </Button>
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

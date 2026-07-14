const formatter = new Intl.NumberFormat("id-ID", {
  style: "currency",
  currency: "IDR",
  minimumFractionDigits: 0,
  maximumFractionDigits: 0,
});

export function formatPrice(amount: number): string {
  return formatter.format(amount);
}

export function normalizeCurrencyInput(value: string): string {
  return value.replace(/\D/g, "");
}

export function formatCurrencyInput(value: string | number | null | undefined): string {
  const normalized = normalizeCurrencyInput(String(value ?? ""));
  if (!normalized) return "";
  return new Intl.NumberFormat("id-ID").format(Number(normalized));
}

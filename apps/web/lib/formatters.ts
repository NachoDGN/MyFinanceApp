import { Decimal } from "decimal.js";

export function formatCurrency(
  amount: string | null | undefined,
  currency: string,
) {
  if (amount === null || amount === undefined) return "N/A";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency,
    maximumFractionDigits: 2,
  }).format(Number(amount));
}

export function formatPercent(value: string | null | undefined) {
  if (value === null || value === undefined) return "N/A";
  return `${Number(value).toFixed(2)}%`;
}

export function formatQuantity(value: string | null | undefined) {
  if (value === null || value === undefined || value.trim() === "") return "—";

  try {
    const quantity = new Decimal(value);
    return quantity.isInteger()
      ? quantity.toFixed(0)
      : quantity.toFixed(8).replace(/\.?0+$/, "");
  } catch {
    return value;
  }
}

export function formatDate(
  value: string,
  options?: { lenient?: boolean },
) {
  const normalized =
    options?.lenient && value.length <= 10
      ? `${value.slice(0, 10)}T00:00:00Z`
      : `${value}T00:00:00Z`;
  const date = new Date(normalized);
  if (options?.lenient && Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  }).format(date);
}

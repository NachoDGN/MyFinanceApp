import type { DomainDataset } from "@myfinance/domain";

import { convertBaseEurToDisplayAmount } from "./currency";
import { formatMonthLabel } from "./dashboard";
import { formatCurrency } from "./formatters";

export const FLOW_CATEGORY_PAGE_SIZE = 8;

export const FLOW_SERIES_COLORS = [
  "#ff4a22",
  "#005f73",
  "#0a9396",
  "#2a9d8f",
  "#e9c46a",
  "#f4a261",
  "#457b9d",
  "#3d405b",
  "#6d597a",
  "#8d99ae",
  "#2f3e46",
  "#bc4749",
] as const;

export function flowSeriesColor(index: number) {
  return FLOW_SERIES_COLORS[index % FLOW_SERIES_COLORS.length]!;
}

export function startOfMonthIso(value: string) {
  return `${value.slice(0, 7)}-01`;
}

export function formatFlowDisplayAmount(
  dataset: DomainDataset,
  amountBaseEur: string | null | undefined,
  currency: string,
  transactionDate: string,
) {
  if (amountBaseEur === null || amountBaseEur === undefined) {
    return "N/A";
  }

  return formatCurrency(
    convertBaseEurToDisplayAmount(
      dataset,
      amountBaseEur,
      currency,
      transactionDate,
    ),
    currency,
  );
}

export function formatFlowCategoryLabel(
  dataset: DomainDataset,
  categoryCode: string | null | undefined,
  fallbackLabel: string,
) {
  if (!categoryCode) {
    return fallbackLabel;
  }

  return (
    dataset.categories.find((category) => category.code === categoryCode)
      ?.displayName ?? fallbackLabel
  );
}

export function formatTransactionClassLabel(transactionClass: string) {
  return transactionClass.replace(/_/g, " ");
}

export function paginateFlowRows<T>(
  rows: readonly T[],
  value: string | string[] | undefined,
  pageSize = FLOW_CATEGORY_PAGE_SIZE,
) {
  const rawValue = Array.isArray(value) ? value[0] : value;
  const requestedPage = Number.parseInt(rawValue ?? "1", 10);
  const normalizedPage =
    Number.isFinite(requestedPage) && requestedPage > 0 ? requestedPage : 1;
  const pageCount = Math.max(1, Math.ceil(rows.length / pageSize));
  const page = Math.min(normalizedPage, pageCount);
  const startIndex = (page - 1) * pageSize;
  const visibleRows = rows.slice(startIndex, startIndex + pageSize);
  const rangeLabel =
    rows.length > 0
      ? `${startIndex + 1}-${Math.min(startIndex + pageSize, rows.length)} of ${rows.length}`
      : "0 categories";

  return {
    page,
    pageCount,
    pageSize,
    rangeLabel,
    startIndex,
    visibleRows,
    hasMultiplePages: rows.length > pageSize,
  };
}

export function formatFallbackFxRange(months: readonly string[]) {
  if (months.length === 0) {
    return null;
  }

  if (months.length === 1) {
    return formatMonthLabel(months[0]!);
  }

  return `${formatMonthLabel(months[0]!)}-${formatMonthLabel(
    months[months.length - 1]!,
  )}`;
}

import type {
  DashboardSummaryResponse,
  DomainDataset,
  PeriodSelection,
  Scope,
  Transaction,
} from "@myfinance/domain";
import {
  filterTransactionsByPeriod,
  filterTransactionsByScope,
  isTransactionResolvedForAnalytics,
  todayIso,
} from "@myfinance/domain";

export type ReadModelInput = {
  scope: Scope;
  displayCurrency: string;
  period?: PeriodSelection;
  referenceDate?: string;
};

export type AnalyticsReadModelContext = {
  dataset: DomainDataset;
  input: ReadModelInput;
  summary: DashboardSummaryResponse;
  referenceDate: string;
  scopedTransactions: Transaction[];
  scopedPeriodTransactions: Transaction[];
  resolvedScopedTransactions: Transaction[];
  resolvedScopedPeriodTransactions: Transaction[];
  accountById: Map<string, DomainDataset["accounts"][number]>;
  categoryByCode: Map<string, DomainDataset["categories"][number]>;
  entityById: Map<string, DomainDataset["entities"][number]>;
};

function buildIndexById<T extends { id: string }>(rows: T[]) {
  return new Map(rows.map((row) => [row.id, row]));
}

export function buildAnalyticsReadModelContext(
  dataset: DomainDataset,
  input: ReadModelInput,
  summary: DashboardSummaryResponse,
): AnalyticsReadModelContext {
  const scopedTransactions = filterTransactionsByScope(dataset, input.scope);
  const scopedPeriodTransactions = filterTransactionsByPeriod(
    scopedTransactions,
    summary.period,
  );

  return {
    dataset,
    input,
    summary,
    referenceDate: input.referenceDate ?? todayIso(),
    scopedTransactions,
    scopedPeriodTransactions,
    resolvedScopedTransactions: scopedTransactions.filter((transaction) =>
      isTransactionResolvedForAnalytics(transaction),
    ),
    resolvedScopedPeriodTransactions: scopedPeriodTransactions.filter(
      (transaction) => isTransactionResolvedForAnalytics(transaction),
    ),
    accountById: buildIndexById(dataset.accounts),
    categoryByCode: new Map(
      dataset.categories.map((category) => [category.code, category]),
    ),
    entityById: buildIndexById(dataset.entities),
  };
}

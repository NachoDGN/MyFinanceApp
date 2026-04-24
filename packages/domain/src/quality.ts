import { Decimal } from "decimal.js";

import {
  filterTransactionsByPeriod,
  filterTransactionsByReferenceDate,
  filterTransactionsByScope,
  getScopeLatestDate,
  resolvePeriodSelection,
  resolveScopeQuoteFreshness,
  todayIso,
} from "./finance";
import {
  isTransactionPendingEnrichment,
  needsTransactionManualReview,
} from "./transaction-review";
import type {
  DomainDataset,
  PeriodSelection,
  QualitySummary,
  Scope,
  Transaction,
} from "./types";
import { resolveAccountStaleThresholdDays } from "./workspace-settings";

function transactionMagnitudeEur(transaction: Transaction) {
  return new Decimal(transaction.amountBaseEur).abs();
}

function scopedAccounts(dataset: DomainDataset, scope: Scope) {
  if (scope.kind === "entity" && scope.entityId) {
    return dataset.accounts.filter(
      (account) => account.entityId === scope.entityId,
    );
  }
  if (scope.kind === "account" && scope.accountId) {
    return dataset.accounts.filter((account) => account.id === scope.accountId);
  }
  return dataset.accounts;
}

export function buildQualitySummary(
  dataset: DomainDataset,
  scope: Scope,
  options: {
    referenceDate?: string;
    period?: PeriodSelection;
  } = {},
): QualitySummary {
  const referenceDate = options.referenceDate ?? todayIso();
  const period =
    options.period ?? resolvePeriodSelection({ preset: "mtd", referenceDate });
  const transactions = filterTransactionsByReferenceDate(
    filterTransactionsByScope(dataset, scope),
    referenceDate,
  );
  const accounts = scopedAccounts(dataset, scope);
  const staleAccounts = accounts
    .map((account) => {
      const lastImportDate = account.lastImportedAt
        ? new Date(account.lastImportedAt)
        : null;
      const threshold = resolveAccountStaleThresholdDays(
        dataset.profile,
        account.assetDomain,
        account.staleAfterDays,
      );
      const ageDays = lastImportDate
        ? Math.floor(
            (Date.parse(`${referenceDate}T12:00:00Z`) -
              lastImportDate.getTime()) /
              86400000,
          )
        : threshold + 1;

      return { account, ageDays, threshold };
    })
    .filter((row) => row.ageDays > row.threshold)
    .map((row) => ({
      accountId: row.account.id,
      accountName: row.account.displayName,
      staleSinceDays: row.ageDays,
    }));

  return {
    pendingEnrichmentCount: transactions.filter((row) =>
      isTransactionPendingEnrichment(row),
    ).length,
    pendingReviewCount: transactions.filter((row) =>
      needsTransactionManualReview(row),
    ).length,
    unclassifiedAmountMtdEur: filterTransactionsByPeriod(transactions, period)
      .filter((row) => row.categoryCode?.startsWith("uncategorized"))
      .reduce(
        (sum, row) => sum.plus(transactionMagnitudeEur(row)),
        new Decimal(0),
      )
      .toFixed(2),
    staleAccountsCount: staleAccounts.length,
    staleAccounts,
    latestImportDateByAccount: accounts.map((account) => ({
      accountId: account.id,
      accountName: account.displayName,
      latestImportDate: account.lastImportedAt?.slice(0, 10) ?? null,
    })),
    latestDataDateByScope: getScopeLatestDate(dataset, scope, referenceDate),
    priceFreshness: resolveScopeQuoteFreshness(dataset, scope, referenceDate),
  };
}

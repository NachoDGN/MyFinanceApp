import { Decimal } from "decimal.js";

import {
  buildCryptoBalanceRows,
  buildLiveHoldingRows,
  getLatestInvestmentCashBalances,
  resolveScopeEntityIds,
  summarizeQuoteFreshness,
  todayIso,
} from "./finance";
import type { DomainDataset, HoldingsResponse, Scope } from "./types";

export function buildHoldingsSnapshot(
  dataset: DomainDataset,
  scope: Scope,
  referenceDate = todayIso(),
): HoldingsResponse {
  const holdings = buildLiveHoldingRows(dataset, scope, referenceDate);
  const cryptoBalances = buildCryptoBalanceRows(dataset, scope, referenceDate);
  const entityIds = new Set(resolveScopeEntityIds(dataset, scope));
  const brokerageCashEur = getLatestInvestmentCashBalances(
    dataset,
    referenceDate,
  )
    .filter((snapshot) => {
      const account = dataset.accounts.find(
        (candidate) => candidate.id === snapshot.accountId,
      );
      return (
        account?.assetDomain === "investment" &&
        entityIds.has(account.entityId) &&
        (scope.kind !== "account" || account.id === scope.accountId)
      );
    })
    .reduce(
      (sum, snapshot) => sum.plus(snapshot.balanceBaseEur),
      new Decimal(0),
    )
    .toFixed(2);
  const quoteStates = [
    ...holdings.map((row) => row.quoteFreshness),
    ...cryptoBalances.map((row) => row.quoteFreshness),
  ];

  return {
    schemaVersion: "v1",
    scope,
    holdings,
    cryptoBalances,
    quoteFreshness: summarizeQuoteFreshness(quoteStates),
    brokerageCashEur,
    generatedAt: new Date().toISOString(),
  };
}

import { Decimal } from "decimal.js";

import type {
  AccountBalanceSnapshot,
  DailyPortfolioSnapshot,
  DomainDataset,
  HoldingRow,
  PeriodSelection,
  Scope,
  Transaction,
} from "./types";

type PeriodPreset = PeriodSelection["preset"];

function toDate(value: string) {
  return new Date(`${value}T00:00:00Z`);
}

function toIsoDate(value: Date) {
  return value.toISOString().slice(0, 10);
}

function safeDividePercent(numerator: Decimal, denominator: Decimal) {
  if (denominator.eq(0)) return null;
  return numerator.div(denominator).mul(100).toFixed(2);
}

export function todayIso(now = new Date()) {
  return now.toISOString().slice(0, 10);
}

export function shiftIsoDate(value: string, days: number) {
  const next = toDate(value);
  next.setUTCDate(next.getUTCDate() + days);
  return toIsoDate(next);
}

export function startOfMonthIso(value: string) {
  return `${value.slice(0, 7)}-01`;
}

export function startOfYearIso(value: string) {
  return `${value.slice(0, 4)}-01-01`;
}

export function startOfWeekIso(value: string) {
  const date = toDate(value);
  const daysFromMonday = (date.getUTCDay() + 6) % 7;
  return shiftIsoDate(value, -daysFromMonday);
}

export function startOfTrailingMonthsIso(value: string, monthCount: number) {
  const date = toDate(startOfMonthIso(value));
  date.setUTCMonth(date.getUTCMonth() - Math.max(0, monthCount - 1));
  return toIsoDate(date);
}

function isIsoDate(value: string | undefined): value is string {
  return Boolean(value && /^\d{4}-\d{2}-\d{2}$/.test(value));
}

function dayCountInclusive(start: string, end: string) {
  return Math.floor((toDate(end).getTime() - toDate(start).getTime()) / 86400000) + 1;
}

export function resolvePeriodSelection(input: {
  preset?: string;
  start?: string;
  end?: string;
  referenceDate?: string;
}): PeriodSelection {
  const referenceDate = isIsoDate(input.referenceDate) ? input.referenceDate : todayIso();
  const preset = input.preset ?? "mtd";

  if (preset === "custom" && isIsoDate(input.start) && isIsoDate(input.end)) {
    return {
      start: input.start,
      end: input.end,
      preset: "custom",
    };
  }

  if (preset === "week") {
    return {
      start: startOfWeekIso(referenceDate),
      end: referenceDate,
      preset: "week",
    };
  }

  if (preset === "24m") {
    return {
      start: startOfTrailingMonthsIso(referenceDate, 24),
      end: referenceDate,
      preset: "24m",
    };
  }

  if (preset === "ytd") {
    return {
      start: startOfYearIso(referenceDate),
      end: referenceDate,
      preset: "ytd",
    };
  }

  return {
    start: startOfMonthIso(referenceDate),
    end: referenceDate,
    preset: "mtd",
  };
}

export function getPreviousComparablePeriod(period: PeriodSelection): PeriodSelection {
  if (period.preset === "mtd") {
    const currentMonthStart = toDate(period.start);
    const previousMonthStart = new Date(Date.UTC(
      currentMonthStart.getUTCFullYear(),
      currentMonthStart.getUTCMonth() - 1,
      1,
    ));
    const previousMonthEnd = new Date(Date.UTC(
      currentMonthStart.getUTCFullYear(),
      currentMonthStart.getUTCMonth(),
      0,
    ));
    const currentDayNumber = Number(period.end.slice(8, 10));
    const previousEndDay = Math.min(currentDayNumber, previousMonthEnd.getUTCDate());

    return {
      start: toIsoDate(previousMonthStart),
      end: toIsoDate(
        new Date(Date.UTC(
          previousMonthStart.getUTCFullYear(),
          previousMonthStart.getUTCMonth(),
          previousEndDay,
        )),
      ),
      preset: "mtd",
    };
  }

  const days = dayCountInclusive(period.start, period.end);
  return {
    start: shiftIsoDate(period.start, -days),
    end: shiftIsoDate(period.start, -1),
    preset: "custom",
  };
}

export function getDatasetLatestDate(dataset: DomainDataset, fallback = todayIso()) {
  const latest = [
    ...dataset.transactions.map((row) => row.transactionDate),
    ...dataset.importBatches.flatMap((row) => [
      row.detectedDateRange?.end ?? "",
    ]),
    ...dataset.accountBalanceSnapshots.map((row) => row.asOfDate),
    ...dataset.securityPrices.map((row) => row.priceDate),
    ...dataset.fxRates.map((row) => row.asOfDate),
    ...dataset.holdingAdjustments.map((row) => row.effectiveDate),
    ...dataset.dailyPortfolioSnapshots.map((row) => row.snapshotDate),
    ...dataset.monthlyCashFlowRollups.map((row) => row.month),
  ]
    .filter(Boolean)
    .sort()
    .at(-1);

  return latest || fallback;
}

export function resolveScopeEntityIds(dataset: DomainDataset, scope: Scope): string[] {
  if (scope.kind === "consolidated") {
    return dataset.entities.map((entity) => entity.id);
  }

  if (scope.kind === "entity" && scope.entityId) {
    return [scope.entityId];
  }

  if (scope.kind === "account" && scope.accountId) {
    const account = dataset.accounts.find((row) => row.id === scope.accountId);
    return account ? [account.entityId] : [];
  }

  return [];
}

export function filterTransactionsByScope(dataset: DomainDataset, scope: Scope): Transaction[] {
  if (scope.kind === "consolidated") {
    return dataset.transactions;
  }

  if (scope.kind === "entity" && scope.entityId) {
    return dataset.transactions.filter((row) => row.economicEntityId === scope.entityId);
  }

  if (scope.kind === "account" && scope.accountId) {
    return dataset.transactions.filter((row) => row.accountId === scope.accountId);
  }

  return dataset.transactions;
}

export function filterTransactionsByPeriod(
  transactions: Transaction[],
  period: PeriodSelection,
) {
  return transactions.filter(
    (row) => row.transactionDate >= period.start && row.transactionDate <= period.end,
  );
}

export function resolveFxRate(
  dataset: DomainDataset,
  from: string,
  to: string,
  asOfDate = todayIso(),
) {
  if (from === to) return new Decimal(1);

  const direct = [...dataset.fxRates]
    .filter(
      (row) =>
        row.baseCurrency === from &&
        row.quoteCurrency === to &&
        row.asOfDate <= asOfDate,
    )
    .sort((left, right) => right.asOfDate.localeCompare(left.asOfDate))[0];
  if (direct) return new Decimal(direct.rate);

  const reverse = [...dataset.fxRates]
    .filter(
      (row) =>
        row.baseCurrency === to &&
        row.quoteCurrency === from &&
        row.asOfDate <= asOfDate,
    )
    .sort((left, right) => right.asOfDate.localeCompare(left.asOfDate))[0];
  if (reverse) return new Decimal(1).div(reverse.rate);

  return new Decimal(1);
}

export function getLatestBalanceSnapshots(
  snapshots: AccountBalanceSnapshot[],
  asOfDate?: string,
) {
  const byAccount = new Map<string, AccountBalanceSnapshot>();

  for (const snapshot of snapshots) {
    if (asOfDate && snapshot.asOfDate > asOfDate) continue;
    const current = byAccount.get(snapshot.accountId);
    if (!current || current.asOfDate < snapshot.asOfDate) {
      byAccount.set(snapshot.accountId, snapshot);
    }
  }

  return [...byAccount.values()];
}

function parseImportedBalance(
  transaction: Transaction,
  accountCurrency: string,
) {
  const rawPayload =
    transaction.rawPayload && typeof transaction.rawPayload === "object"
      ? transaction.rawPayload
      : {};
  const importPayloadCandidate =
    (rawPayload as Record<string, unknown>).Import ??
    (rawPayload as Record<string, unknown>)._import ??
    (rawPayload as Record<string, unknown>).import;
  const importPayload =
    importPayloadCandidate && typeof importPayloadCandidate === "object"
      ? (importPayloadCandidate as Record<string, unknown>)
      : null;
  const balanceOriginalValue = importPayload?.balanceOriginal;
  if (
    balanceOriginalValue === undefined ||
    balanceOriginalValue === null ||
    `${balanceOriginalValue}`.trim() === ""
  ) {
    return null;
  }

  const balanceOriginal = new Decimal(String(balanceOriginalValue)).toFixed(8);
  const balanceCurrency =
    typeof importPayload?.balanceCurrency === "string" && importPayload.balanceCurrency.trim()
      ? importPayload.balanceCurrency.trim()
      : accountCurrency;

  return {
    balanceOriginal,
    balanceCurrency,
  };
}

export function getLatestInvestmentCashBalances(
  dataset: DomainDataset,
  asOfDate = todayIso(),
) {
  const seededSnapshots = getLatestBalanceSnapshots(dataset.accountBalanceSnapshots, asOfDate);
  const snapshotsByAccount = new Map(seededSnapshots.map((snapshot) => [snapshot.accountId, snapshot]));

  for (const account of dataset.accounts) {
    if (account.assetDomain !== "investment" || snapshotsByAccount.has(account.id)) {
      continue;
    }

    const latestTransaction = [...dataset.transactions]
      .filter((transaction) => transaction.accountId === account.id && transaction.transactionDate <= asOfDate)
      .sort(
        (left, right) =>
          `${right.transactionDate}${right.createdAt}`.localeCompare(`${left.transactionDate}${left.createdAt}`),
      )
      .find((transaction) => parseImportedBalance(transaction, account.defaultCurrency));

    if (!latestTransaction) {
      continue;
    }

    const parsedBalance = parseImportedBalance(latestTransaction, account.defaultCurrency);
    if (!parsedBalance) {
      continue;
    }

    const balanceBaseEur = new Decimal(parsedBalance.balanceOriginal)
      .mul(resolveFxRate(dataset, parsedBalance.balanceCurrency, "EUR", asOfDate))
      .toFixed(8);

    snapshotsByAccount.set(account.id, {
      accountId: account.id,
      asOfDate: latestTransaction.transactionDate,
      balanceOriginal: parsedBalance.balanceOriginal,
      balanceCurrency: parsedBalance.balanceCurrency,
      balanceBaseEur,
      sourceKind: "statement",
      importBatchId: latestTransaction.importBatchId ?? null,
    });
  }

  return [...snapshotsByAccount.values()];
}

function latestSecurityPrice(
  dataset: DomainDataset,
  securityId: string,
  asOfDate: string,
) {
  return [...dataset.securityPrices]
    .filter((row) => row.securityId === securityId && row.priceDate <= asOfDate)
    .sort(
      (left, right) =>
        right.priceDate.localeCompare(left.priceDate) ||
        right.quoteTimestamp.localeCompare(left.quoteTimestamp),
    )[0] ?? null;
}

export function buildHoldingRows(
  dataset: DomainDataset,
  scope: Scope,
  asOfDate = todayIso(),
): HoldingRow[] {
  const entityIds = new Set(resolveScopeEntityIds(dataset, scope));

  return dataset.investmentPositions
    .filter((position) => entityIds.has(position.entityId))
    .map((position) => {
      const security = dataset.securities.find((row) => row.id === position.securityId);
      const price = latestSecurityPrice(dataset, position.securityId, asOfDate);
      const priceFx = price
        ? resolveFxRate(dataset, price.currency, "EUR", price.priceDate)
        : new Decimal(1);
      const currentValueEur = price
        ? new Decimal(position.openQuantity).mul(price.price).mul(priceFx).toFixed(2)
        : null;
      const unrealizedPnlEur = currentValueEur
        ? new Decimal(currentValueEur).minus(position.openCostBasisEur).toFixed(2)
        : null;

      return {
        securityId: position.securityId,
        accountId: position.accountId,
        entityId: position.entityId,
        symbol: security?.displaySymbol ?? position.securityId,
        securityName: security?.name ?? position.securityId,
        quantity: position.openQuantity,
        avgCostEur: position.avgCostEur,
        currentPrice: price?.price ?? null,
        currentPriceCurrency: price?.currency ?? null,
        currentValueEur,
        unrealizedPnlEur,
        unrealizedPnlPercent: unrealizedPnlEur
          ? safeDividePercent(new Decimal(unrealizedPnlEur), new Decimal(position.openCostBasisEur))
          : null,
        quoteFreshness: price ? "delayed" : "missing",
        quoteTimestamp: price?.quoteTimestamp ?? null,
        unrealizedComplete: position.unrealizedComplete,
      };
    });
}

export function sumSnapshotField(
  snapshots: DailyPortfolioSnapshot[],
  field: "totalPortfolioValueEur" | "cashBalanceEur" | "unrealizedPnlEur",
) {
  return snapshots.reduce((sum, row) => sum.plus(row[field] ?? 0), new Decimal(0)).toFixed(2);
}

type MutableInvestmentPosition = {
  userId: string;
  entityId: string;
  accountId: string;
  securityId: string;
  openQuantity: Decimal;
  openCostBasisEur: Decimal;
  realizedPnlEur: Decimal;
  dividendsEur: Decimal;
  interestEur: Decimal;
  feesEur: Decimal;
  lastTradeDate: string | null;
  provenanceJson: Record<string, unknown>;
};

function getPositionMapKey(
  entityId: string,
  accountId: string,
  securityId: string,
) {
  return `${entityId}:${accountId}:${securityId}`;
}

export function rebuildInvestmentState(
  dataset: DomainDataset,
  referenceDate = todayIso(),
): {
  positions: DomainDataset["investmentPositions"];
  snapshots: DomainDataset["dailyPortfolioSnapshots"];
} {
  const investmentAccounts = new Map(
    dataset.accounts
      .filter((account) => account.assetDomain === "investment")
      .map((account) => [account.id, account]),
  );
  const positions = new Map<string, MutableInvestmentPosition>();

  const ensurePosition = (entityId: string, accountId: string, securityId: string) => {
    const key = getPositionMapKey(entityId, accountId, securityId);
    const existing = positions.get(key);
    if (existing) {
      return existing;
    }

    const created: MutableInvestmentPosition = {
      userId: dataset.profile.id,
      entityId,
      accountId,
      securityId,
      openQuantity: new Decimal(0),
      openCostBasisEur: new Decimal(0),
      realizedPnlEur: new Decimal(0),
      dividendsEur: new Decimal(0),
      interestEur: new Decimal(0),
      feesEur: new Decimal(0),
      lastTradeDate: null,
      provenanceJson: { source: "transactions" },
    };
    positions.set(key, created);
    return created;
  };

  const events = [
    ...dataset.transactions
      .filter((transaction) => {
        if (transaction.transactionDate > referenceDate) return false;
        if (transaction.excludeFromAnalytics || transaction.voidedAt) return false;
        return investmentAccounts.has(transaction.accountId);
      })
      .map((transaction) => ({
        type: "transaction" as const,
        sortKey: `${transaction.transactionDate}:1:${transaction.createdAt}`,
        transaction,
      })),
    ...dataset.holdingAdjustments
      .filter((adjustment) => adjustment.effectiveDate <= referenceDate)
      .map((adjustment) => ({
        type: "adjustment" as const,
        sortKey: `${adjustment.effectiveDate}:0:${adjustment.createdAt}`,
        adjustment,
      })),
  ].sort((left, right) => left.sortKey.localeCompare(right.sortKey));

  for (const event of events) {
    if (event.type === "adjustment") {
      const adjustment = event.adjustment;
      const position = ensurePosition(
        adjustment.entityId,
        adjustment.accountId,
        adjustment.securityId,
      );
      position.openQuantity = position.openQuantity.plus(adjustment.shareDelta);
      position.openCostBasisEur = position.openCostBasisEur.plus(adjustment.costBasisDeltaEur ?? 0);
      position.lastTradeDate = adjustment.effectiveDate;
      continue;
    }

    const transaction = event.transaction;
    if (!transaction.securityId) {
      continue;
    }

    const quantity = new Decimal(transaction.quantity ?? 0);
    const amountEur = new Decimal(transaction.amountBaseEur);
    const position = ensurePosition(
      transaction.economicEntityId,
      transaction.accountId,
      transaction.securityId,
    );

    switch (transaction.transactionClass) {
      case "investment_trade_buy": {
        if (quantity.lte(0)) break;
        position.openQuantity = position.openQuantity.plus(quantity);
        position.openCostBasisEur = position.openCostBasisEur.plus(amountEur.abs());
        position.lastTradeDate = transaction.transactionDate;
        break;
      }
      case "investment_trade_sell": {
        if (quantity.lte(0) || position.openQuantity.lte(0)) break;
        const sellQuantity = Decimal.min(position.openQuantity, quantity);
        const currentAverageCost = position.openQuantity.eq(0)
          ? new Decimal(0)
          : position.openCostBasisEur.div(position.openQuantity);
        const removedCostBasis = currentAverageCost.mul(sellQuantity);
        const proportionalProceeds = amountEur.abs().mul(sellQuantity.div(quantity));

        position.openQuantity = position.openQuantity.minus(sellQuantity);
        position.openCostBasisEur = Decimal.max(
          new Decimal(0),
          position.openCostBasisEur.minus(removedCostBasis),
        );
        position.realizedPnlEur = position.realizedPnlEur.plus(
          proportionalProceeds.minus(removedCostBasis),
        );
        position.lastTradeDate = transaction.transactionDate;
        break;
      }
      case "dividend":
        position.dividendsEur = position.dividendsEur.plus(amountEur.abs());
        break;
      case "interest":
        position.interestEur = position.interestEur.plus(amountEur.abs());
        break;
      case "fee":
        position.feesEur = position.feesEur.plus(amountEur.abs());
        break;
      default:
        break;
    }
  }

  const materializedPositions = [...positions.values()]
    .filter((position) => position.openQuantity.gt(0))
    .map((position) => ({
      userId: position.userId,
      entityId: position.entityId,
      accountId: position.accountId,
      securityId: position.securityId,
      openQuantity: position.openQuantity.toFixed(8),
      openCostBasisEur: position.openCostBasisEur.toFixed(8),
      avgCostEur: position.openQuantity.eq(0)
        ? "0.00000000"
        : position.openCostBasisEur.div(position.openQuantity).toFixed(8),
      realizedPnlEur: position.realizedPnlEur.toFixed(8),
      dividendsEur: position.dividendsEur.toFixed(8),
      interestEur: position.interestEur.toFixed(8),
      feesEur: position.feesEur.toFixed(8),
      lastTradeDate: position.lastTradeDate,
      lastRebuiltAt: new Date().toISOString(),
      provenanceJson: position.provenanceJson,
      unrealizedComplete: true,
    }));

  const holdingsDataset = {
    ...dataset,
    investmentPositions: materializedPositions,
  } satisfies DomainDataset;
  const holdingRows = buildHoldingRows(holdingsDataset, { kind: "consolidated" }, referenceDate);
  const holdingsByAccount = new Map<string, HoldingRow[]>();
  for (const row of holdingRows) {
    const existing = holdingsByAccount.get(row.accountId) ?? [];
    existing.push(row);
    holdingsByAccount.set(row.accountId, existing);
  }
  const cashSnapshotsByAccount = new Map(
    getLatestInvestmentCashBalances(dataset, referenceDate).map((snapshot) => [snapshot.accountId, snapshot]),
  );

  const snapshots = [...investmentAccounts.values()].map((account) => {
    const accountHoldings = holdingsByAccount.get(account.id) ?? [];
    const marketValueEur = accountHoldings
      .reduce((sum, holding) => sum.plus(holding.currentValueEur ?? 0), new Decimal(0))
      .toFixed(8);
    const costBasisEur = materializedPositions
      .filter((position) => position.accountId === account.id)
      .reduce((sum, position) => sum.plus(position.openCostBasisEur), new Decimal(0))
      .toFixed(8);
    const unrealizedPnlEur = accountHoldings
      .reduce((sum, holding) => sum.plus(holding.unrealizedPnlEur ?? 0), new Decimal(0))
      .toFixed(8);
    const cashBalanceEur = cashSnapshotsByAccount.get(account.id)?.balanceBaseEur ?? "0.00000000";

    return {
      snapshotDate: referenceDate,
      userId: dataset.profile.id,
      entityId: account.entityId,
      accountId: account.id,
      securityId: null,
      marketValueEur,
      costBasisEur,
      unrealizedPnlEur,
      cashBalanceEur,
      totalPortfolioValueEur: new Decimal(marketValueEur).plus(cashBalanceEur).toFixed(8),
      generatedAt: new Date().toISOString(),
    };
  });

  return {
    positions: materializedPositions,
    snapshots,
  };
}

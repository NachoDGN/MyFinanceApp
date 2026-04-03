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

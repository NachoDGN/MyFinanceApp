import { Decimal } from "decimal.js";

import type {
  DashboardSummaryResponse,
  DomainDataset,
  InsightCard,
  MetricResult,
  PeriodSelection,
  QualitySummary,
  Scope,
  Transaction,
} from "@myfinance/domain";
import {
  buildHoldingRows,
  filterTransactionsByPeriod,
  filterTransactionsByScope,
  getLatestBalanceSnapshots,
  getLatestInvestmentCashBalances,
  getPreviousComparablePeriod,
  getScopeLatestDate,
  resolveFxRate,
  resolvePeriodSelection,
  resolveScopeEntityIds,
  shiftIsoDate,
  startOfMonthIso,
  sumSnapshotField,
  todayIso,
} from "@myfinance/domain";
import { metricRegistry } from "./registry";

export { metricRegistry } from "./registry";

const investmentLedgerClasses = [
  "investment_trade_buy",
  "investment_trade_sell",
  "transfer_internal",
  "dividend",
  "interest",
  "fee",
  "fx_conversion",
  "unknown",
] satisfies Transaction["transactionClass"][];

const processedInvestmentLedgerClasses = investmentLedgerClasses.filter(
  (transactionClass) => transactionClass !== "unknown",
) as Transaction["transactionClass"][];

function sumStrings(values: Array<string | null | undefined>) {
  return values
    .reduce((sum, value) => sum.plus(new Decimal(value ?? 0)), new Decimal(0))
    .toFixed(2);
}

function safeDividePercent(numerator: Decimal, denominator: Decimal) {
  if (denominator.eq(0)) return null;
  return numerator.div(denominator).mul(100).toFixed(2);
}

function toDisplayAmount(
  dataset: DomainDataset,
  amountEur: string | null,
  currency: string,
  asOfDate = todayIso(),
) {
  if (amountEur === null) return null;
  if (currency === "EUR") return new Decimal(amountEur).toFixed(2);
  return new Decimal(amountEur)
    .mul(resolveFxRate(dataset, "EUR", currency, asOfDate))
    .toFixed(2);
}

function amountMagnitudeEur(transaction: Transaction) {
  return new Decimal(transaction.amountBaseEur).abs();
}

function isIncomeLike(transaction: Transaction) {
  return ["income", "dividend", "interest"].includes(
    transaction.transactionClass,
  );
}

function isExcludedIncome(transaction: Transaction) {
  return [
    "owner_contribution",
    "reimbursement",
    "refund",
    "transfer_internal",
    "transfer_external",
    "loan_inflow",
    "investment_trade_sell",
  ].includes(transaction.transactionClass);
}

function isSpendingLike(transaction: Transaction) {
  return ["expense", "fee", "refund"].includes(transaction.transactionClass);
}

function isExcludedSpending(transaction: Transaction) {
  return [
    "transfer_internal",
    "transfer_external",
    "investment_trade_buy",
    "investment_trade_sell",
    "owner_draw",
    "reimbursement",
    "loan_principal_payment",
    "fx_conversion",
  ].includes(transaction.transactionClass);
}

function currentCashTotal(
  dataset: DomainDataset,
  scope: Scope,
  asOfDate: string,
) {
  const entityIds = new Set(resolveScopeEntityIds(dataset, scope));
  return getLatestBalanceSnapshots(dataset.accountBalanceSnapshots, asOfDate)
    .filter((snapshot) => {
      const account = dataset.accounts.find(
        (row) => row.id === snapshot.accountId,
      );
      return account && entityIds.has(account.entityId);
    })
    .reduce(
      (sum, snapshot) => sum.plus(snapshot.balanceBaseEur),
      new Decimal(0),
    )
    .toFixed(2);
}

function currentPortfolioValue(
  dataset: DomainDataset,
  scope: Scope,
  asOfDate: string,
) {
  return sumStrings(
    buildHoldingRows(dataset, scope, asOfDate).map(
      (row) => row.currentValueEur,
    ),
  );
}

function currentPortfolioUnrealized(
  dataset: DomainDataset,
  scope: Scope,
  asOfDate: string,
) {
  return sumStrings(
    buildHoldingRows(dataset, scope, asOfDate).map(
      (row) => row.unrealizedPnlEur,
    ),
  );
}

function latestPortfolioSnapshotDate(
  dataset: DomainDataset,
  scope: Scope,
  asOfDate: string,
) {
  const entityIds = new Set(resolveScopeEntityIds(dataset, scope));
  return (
    dataset.dailyPortfolioSnapshots
      .filter(
        (row) => entityIds.has(row.entityId) && row.snapshotDate <= asOfDate,
      )
      .map((row) => row.snapshotDate)
      .sort()
      .at(-1) ?? null
  );
}

function currentValueComparison(
  dataset: DomainDataset,
  scope: Scope,
  selector: "cash" | "portfolio" | "networth" | "unrealized",
  referenceDate: string,
) {
  const previousMonthEnd = shiftIsoDate(startOfMonthIso(referenceDate), -1);
  const entityIds = new Set(resolveScopeEntityIds(dataset, scope));
  const snapshotDate = latestPortfolioSnapshotDate(
    dataset,
    scope,
    previousMonthEnd,
  );
  const snapshots = snapshotDate
    ? dataset.dailyPortfolioSnapshots.filter(
        (row) =>
          row.snapshotDate === snapshotDate && entityIds.has(row.entityId),
      )
    : [];
  const priorCash = currentCashTotal(dataset, scope, previousMonthEnd);

  if (selector === "cash") return priorCash;
  if (selector === "portfolio") {
    return new Decimal(sumSnapshotField(snapshots, "totalPortfolioValueEur"))
      .minus(priorCash)
      .toFixed(2);
  }
  if (selector === "networth") {
    return sumSnapshotField(snapshots, "totalPortfolioValueEur");
  }
  return sumSnapshotField(snapshots, "unrealizedPnlEur");
}

function flowMetric(
  dataset: DomainDataset,
  scope: Scope,
  period: PeriodSelection,
  kind: "income" | "spending",
) {
  const transactions = filterTransactionsByPeriod(
    filterTransactionsByScope(dataset, scope),
    period,
  ).filter((row) => !row.excludeFromAnalytics);

  if (kind === "income") {
    return transactions
      .filter((row) => isIncomeLike(row) && !isExcludedIncome(row))
      .reduce((sum, row) => sum.plus(row.amountBaseEur), new Decimal(0))
      .toFixed(2);
  }

  return transactions
    .filter((row) => isSpendingLike(row) && !isExcludedSpending(row))
    .reduce((sum, row) => {
      if (row.transactionClass === "refund") {
        return sum.minus(row.amountBaseEur);
      }
      return sum.plus(amountMagnitudeEur(row));
    }, new Decimal(0))
    .toFixed(2);
}

function qualitySummary(
  dataset: DomainDataset,
  scope: Scope,
  referenceDate: string,
): QualitySummary {
  const currentPeriod = resolvePeriodSelection({
    preset: "mtd",
    referenceDate,
  });
  const scopedTransactions = filterTransactionsByScope(dataset, scope);
  const scopedAccounts =
    scope.kind === "consolidated"
      ? dataset.accounts
      : scope.kind === "entity" && scope.entityId
        ? dataset.accounts.filter(
            (account) => account.entityId === scope.entityId,
          )
        : scope.kind === "account" && scope.accountId
          ? dataset.accounts.filter((account) => account.id === scope.accountId)
          : dataset.accounts;
  const staleAccounts = scopedAccounts
    .map((account) => {
      const lastImportDate = account.lastImportedAt
        ? new Date(account.lastImportedAt)
        : null;
      const threshold =
        account.staleAfterDays ??
        (account.assetDomain === "investment" ? 3 : 7);
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
    pendingReviewCount: scopedTransactions.filter((row) => row.needsReview)
      .length,
    unclassifiedAmountMtdEur: filterTransactionsByPeriod(
      scopedTransactions,
      currentPeriod,
    )
      .filter((row) => row.categoryCode?.startsWith("uncategorized"))
      .reduce((sum, row) => sum.plus(amountMagnitudeEur(row)), new Decimal(0))
      .toFixed(2),
    staleAccountsCount: staleAccounts.length,
    staleAccounts,
    latestImportDateByAccount: scopedAccounts.map((account) => ({
      accountId: account.id,
      accountName: account.displayName,
      latestImportDate: account.lastImportedAt?.slice(0, 10) ?? null,
    })),
    latestDataDateByScope: getScopeLatestDate(dataset, scope, referenceDate),
    priceFreshness: dataset.securityPrices.every((price) => price.isDelayed)
      ? "delayed"
      : "fresh",
  };
}

export function buildMetricResult(
  dataset: DomainDataset,
  scope: Scope,
  displayCurrency: string,
  metricId: string,
  options: { referenceDate?: string } = {},
): MetricResult {
  const definition = metricRegistry.find(
    (metric) => metric.metricId === metricId,
  );
  if (!definition) {
    throw new Error(`Metric ${metricId} is not registered.`);
  }

  const referenceDate = options.referenceDate ?? todayIso();
  const currentPeriod = resolvePeriodSelection({
    preset: "mtd",
    referenceDate,
  });
  const comparisonPeriod = getPreviousComparablePeriod(currentPeriod);
  let valueBaseEur: string | null = null;
  let comparisonBaseEur: string | null = null;

  switch (metricId) {
    case "net_worth_current": {
      const current = new Decimal(
        currentCashTotal(dataset, scope, referenceDate),
      ).plus(currentPortfolioValue(dataset, scope, referenceDate));
      valueBaseEur = current.toFixed(2);
      comparisonBaseEur = currentValueComparison(
        dataset,
        scope,
        "networth",
        referenceDate,
      );
      break;
    }
    case "cash_total_current":
      valueBaseEur = currentCashTotal(dataset, scope, referenceDate);
      comparisonBaseEur = currentValueComparison(
        dataset,
        scope,
        "cash",
        referenceDate,
      );
      break;
    case "income_mtd_total":
      valueBaseEur = flowMetric(dataset, scope, currentPeriod, "income");
      comparisonBaseEur = flowMetric(
        dataset,
        scope,
        comparisonPeriod,
        "income",
      );
      break;
    case "spending_mtd_total":
      valueBaseEur = flowMetric(dataset, scope, currentPeriod, "spending");
      comparisonBaseEur = flowMetric(
        dataset,
        scope,
        comparisonPeriod,
        "spending",
      );
      break;
    case "operating_net_cash_flow_mtd": {
      const income = new Decimal(
        flowMetric(dataset, scope, currentPeriod, "income"),
      );
      const spending = new Decimal(
        flowMetric(dataset, scope, currentPeriod, "spending"),
      );
      const comparisonIncome = new Decimal(
        flowMetric(dataset, scope, comparisonPeriod, "income"),
      );
      const comparisonSpending = new Decimal(
        flowMetric(dataset, scope, comparisonPeriod, "spending"),
      );
      valueBaseEur = income.minus(spending).toFixed(2);
      comparisonBaseEur = comparisonIncome.minus(comparisonSpending).toFixed(2);
      break;
    }
    case "portfolio_market_value_current":
      valueBaseEur = currentPortfolioValue(dataset, scope, referenceDate);
      comparisonBaseEur = currentValueComparison(
        dataset,
        scope,
        "portfolio",
        referenceDate,
      );
      break;
    case "portfolio_unrealized_pnl_current":
      valueBaseEur = currentPortfolioUnrealized(dataset, scope, referenceDate);
      comparisonBaseEur = currentValueComparison(
        dataset,
        scope,
        "unrealized",
        referenceDate,
      );
      break;
    case "pending_review_count":
      valueBaseEur = String(
        qualitySummary(dataset, scope, referenceDate).pendingReviewCount,
      );
      break;
    case "unclassified_amount_mtd":
      valueBaseEur = qualitySummary(
        dataset,
        scope,
        referenceDate,
      ).unclassifiedAmountMtdEur;
      break;
    case "stale_accounts_count":
      valueBaseEur = String(
        qualitySummary(dataset, scope, referenceDate).staleAccountsCount,
      );
      break;
    default:
      valueBaseEur = null;
  }

  const valueDisplay =
    definition.unitType === "currency"
      ? toDisplayAmount(dataset, valueBaseEur, displayCurrency, referenceDate)
      : valueBaseEur;
  const comparisonValueDisplay =
    definition.unitType === "currency"
      ? toDisplayAmount(
          dataset,
          comparisonBaseEur,
          displayCurrency,
          referenceDate,
        )
      : comparisonBaseEur;

  let deltaDisplay: string | null = null;
  let deltaPercent: string | null = null;
  if (
    definition.unitType === "currency" &&
    valueBaseEur !== null &&
    comparisonBaseEur !== null
  ) {
    const delta = new Decimal(valueBaseEur).minus(comparisonBaseEur);
    deltaDisplay = toDisplayAmount(
      dataset,
      delta.toFixed(2),
      displayCurrency,
      referenceDate,
    );
    deltaPercent = safeDividePercent(delta, new Decimal(comparisonBaseEur));
  }

  return {
    metricId: definition.metricId,
    displayName: definition.displayName,
    unitType: definition.unitType,
    baseCurrency: "EUR",
    displayCurrency,
    valueBaseEur,
    valueDisplay,
    comparisonValueBaseEur: comparisonBaseEur,
    comparisonValueDisplay,
    deltaDisplay,
    deltaPercent,
    asOfDate: referenceDate,
    explanation: definition.description,
  };
}

export function buildInsights(
  dataset: DomainDataset,
  scope: Scope,
  options: { referenceDate?: string } = {},
): InsightCard[] {
  const referenceDate = options.referenceDate ?? todayIso();
  const period = resolvePeriodSelection({ preset: "mtd", referenceDate });
  const spendingMetric = buildMetricResult(
    dataset,
    scope,
    "EUR",
    "spending_mtd_total",
    {
      referenceDate,
    },
  );
  const quality = qualitySummary(dataset, scope, referenceDate);
  const holdings = buildHoldingRows(dataset, scope, referenceDate)
    .filter((row) => row.currentValueEur)
    .sort(
      (left, right) =>
        Number(right.currentValueEur) - Number(left.currentValueEur),
    );
  const largestHolding = holdings[0];
  const latestLargeOutflow = filterTransactionsByPeriod(
    filterTransactionsByScope(dataset, scope),
    period,
  )
    .filter((row) => new Decimal(row.amountBaseEur).lt(0))
    .sort((left, right) =>
      Number(amountMagnitudeEur(right).minus(amountMagnitudeEur(left))),
    )[0];

  return [
    {
      id: "spending-mtd",
      title: "Spending pace for the current month",
      severity:
        Number(spendingMetric.deltaPercent ?? "0") > 0 ? "warning" : "info",
      body:
        Number(spendingMetric.deltaPercent ?? "0") > 0
          ? "Outflows are ahead of the previous comparable month-to-date window."
          : "Outflows are at or below the previous comparable month-to-date window.",
      evidence: [
        `Current MTD spending: ${spendingMetric.valueBaseEur ?? "0.00"} EUR`,
        `Comparison window: ${spendingMetric.comparisonValueBaseEur ?? "0.00"} EUR`,
      ],
    },
    {
      id: "largest-outflow",
      title: "Largest current-period outflow",
      severity: "info",
      body: latestLargeOutflow
        ? `${latestLargeOutflow.descriptionRaw} is the largest outflow in the selected scope.`
        : "No outflow rows were found in the selected scope and period.",
      evidence: latestLargeOutflow
        ? [
            `Date: ${latestLargeOutflow.transactionDate}`,
            `Amount: ${latestLargeOutflow.amountBaseEur} EUR`,
          ]
        : [],
    },
    {
      id: "portfolio-concentration",
      title: "Largest priced holding",
      severity: "info",
      body: largestHolding
        ? `${largestHolding.securityName} is currently the largest priced position.`
        : "No priced holdings are available for the selected scope.",
      evidence: largestHolding
        ? [
            `Holding: ${largestHolding.symbol}`,
            `Value: ${largestHolding.currentValueEur ?? "0.00"} EUR`,
          ]
        : [],
    },
    {
      id: "data-quality",
      title: "Data quality status",
      severity:
        quality.pendingReviewCount > 0 || quality.staleAccountsCount > 0
          ? "warning"
          : "positive",
      body:
        quality.pendingReviewCount > 0 || quality.staleAccountsCount > 0
          ? "Some rows or accounts still need attention before totals are fully trusted."
          : "No outstanding review or freshness issues are currently flagged.",
      evidence: [
        `Pending review: ${quality.pendingReviewCount}`,
        `Stale accounts: ${quality.staleAccountsCount}`,
        `Unclassified amount MTD: ${quality.unclassifiedAmountMtdEur} EUR`,
      ],
    },
  ];
}

export function buildDashboardSummary(
  dataset: DomainDataset,
  input: {
    scope: Scope;
    displayCurrency: string;
    period?: PeriodSelection;
    referenceDate?: string;
  },
): DashboardSummaryResponse {
  const referenceDate = input.referenceDate ?? todayIso();
  const period =
    input.period ?? resolvePeriodSelection({ preset: "mtd", referenceDate });
  const metrics = [
    "net_worth_current",
    "cash_total_current",
    "income_mtd_total",
    "spending_mtd_total",
    "operating_net_cash_flow_mtd",
    "portfolio_market_value_current",
    "portfolio_unrealized_pnl_current",
  ].map((metricId) =>
    buildMetricResult(dataset, input.scope, input.displayCurrency, metricId, {
      referenceDate,
    }),
  );

  const entityIds = new Set(resolveScopeEntityIds(dataset, input.scope));
  const monthStart = startOfMonthIso(period.start);
  const monthEnd = startOfMonthIso(period.end);
  const monthLimit = period.preset === "24m" ? 24 : 12;
  const monthlySeries = dataset.monthlyCashFlowRollups
    .filter(
      (row) =>
        entityIds.has(row.entityId) &&
        row.month >= monthStart &&
        row.month <= monthEnd,
    )
    .reduce<
      Array<{
        month: string;
        incomeEur: string;
        spendingEur: string;
        operatingNetEur: string;
      }>
    >((rows, row) => {
      const current = rows.find((item) => item.month === row.month);
      if (current) {
        current.incomeEur = new Decimal(current.incomeEur)
          .plus(row.incomeEur)
          .toFixed(2);
        current.spendingEur = new Decimal(current.spendingEur)
          .plus(row.spendingEur)
          .toFixed(2);
        current.operatingNetEur = new Decimal(current.operatingNetEur)
          .plus(row.operatingNetEur)
          .toFixed(2);
        return rows;
      }

      rows.push({
        month: row.month,
        incomeEur: row.incomeEur,
        spendingEur: row.spendingEur,
        operatingNetEur: row.operatingNetEur,
      });
      return rows;
    }, [])
    .sort((left, right) => left.month.localeCompare(right.month))
    .slice(-monthLimit);

  const scopedTransactions = filterTransactionsByPeriod(
    filterTransactionsByScope(dataset, input.scope),
    period,
  );
  const spendingByCategory = [
    ...new Map(
      scopedTransactions
        .filter((row) => row.transactionClass === "expense" && row.categoryCode)
        .map((row) => [
          row.categoryCode as string,
          {
            categoryCode: row.categoryCode as string,
            label:
              dataset.categories.find(
                (category) => category.code === row.categoryCode,
              )?.displayName ??
              row.categoryCode ??
              "Uncategorized",
            amountEur: "0.00",
          },
        ]),
    ).values(),
  ]
    .map((row) => ({
      ...row,
      amountEur: scopedTransactions
        .filter(
          (transaction) =>
            transaction.categoryCode === row.categoryCode &&
            transaction.transactionClass === "expense",
        )
        .reduce(
          (sum, transaction) => sum.plus(amountMagnitudeEur(transaction)),
          new Decimal(0),
        )
        .toFixed(2),
    }))
    .sort((left, right) => Number(right.amountEur) - Number(left.amountEur));

  const holdings = buildHoldingRows(dataset, input.scope, referenceDate).sort(
    (left, right) =>
      Number(right.currentValueEur ?? 0) - Number(left.currentValueEur ?? 0),
  );
  const totalPortfolio = new Decimal(
    sumStrings(holdings.map((row) => row.currentValueEur)),
  );
  const portfolioAllocation = holdings.map((row) => ({
    label: row.symbol,
    amountEur: row.currentValueEur ?? "0.00",
    allocationPercent: totalPortfolio.eq(0)
      ? "0.00"
      : new Decimal(row.currentValueEur ?? 0)
          .div(totalPortfolio)
          .mul(100)
          .toFixed(2),
  }));

  const recentLargeTransactions = [...scopedTransactions]
    .sort((left, right) => {
      const byDate = right.transactionDate.localeCompare(left.transactionDate);
      if (byDate !== 0) return byDate;
      return Number(amountMagnitudeEur(right).minus(amountMagnitudeEur(left)));
    })
    .slice(0, 6);

  return {
    schemaVersion: "v1",
    scope: input.scope,
    period,
    metrics,
    monthlySeries,
    spendingByCategory,
    portfolioAllocation,
    topHoldings: holdings.slice(0, 5),
    recentLargeTransactions,
    insights: buildInsights(dataset, input.scope, { referenceDate }),
    quality: qualitySummary(dataset, input.scope, referenceDate),
    generatedAt: new Date().toISOString(),
  };
}

function sortTransactionsNewestFirst(transactions: Transaction[]) {
  return [...transactions].sort((left, right) => {
    const byDate = right.transactionDate.localeCompare(left.transactionDate);
    if (byDate !== 0) return byDate;
    return Number(amountMagnitudeEur(right).minus(amountMagnitudeEur(left)));
  });
}

function scopedTransactions(
  dataset: DomainDataset,
  scope: Scope,
  period: PeriodSelection,
  classes: string[],
) {
  return sortTransactionsNewestFirst(
    filterTransactionsByPeriod(
      filterTransactionsByScope(dataset, scope),
      period,
    ).filter((row) => classes.includes(row.transactionClass)),
  );
}

function sumTransactionAmounts(
  transactions: Transaction[],
  selector: (transaction: Transaction) => Decimal,
) {
  return transactions
    .reduce(
      (sum, transaction) => sum.plus(selector(transaction)),
      new Decimal(0),
    )
    .toFixed(2);
}

function aggregateAmountRows<T>(
  rows: T[],
  labelFor: (row: T) => string,
  amountFor: (row: T) => Decimal,
) {
  return [
    ...rows
      .reduce((totals, row) => {
        const label = labelFor(row);
        const amount = amountFor(row);
        totals.set(
          label,
          totals.has(label) ? totals.get(label)!.plus(amount) : amount,
        );
        return totals;
      }, new Map<string, Decimal>())
      .entries(),
  ]
    .map(([label, amount]) => ({
      label,
      amountEur: amount.toFixed(2),
    }))
    .sort((left, right) => Number(right.amountEur) - Number(left.amountEur));
}

function averageMonthlySeries(
  monthlySeries: DashboardSummaryResponse["monthlySeries"],
  key: "incomeEur" | "spendingEur",
  count: number,
) {
  if (monthlySeries.length === 0) return "0.00";
  const rows = monthlySeries.slice(-count);
  return rows
    .reduce((sum, row) => sum.plus(row[key]), new Decimal(0))
    .div(rows.length)
    .toFixed(2);
}

function buildHoldingsSnapshot(
  dataset: DomainDataset,
  scope: Scope,
  referenceDate: string,
) {
  const holdings = buildHoldingRows(dataset, scope, referenceDate);
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
        account?.assetDomain === "investment" && entityIds.has(account.entityId)
      );
    })
    .reduce(
      (sum, snapshot) => sum.plus(snapshot.balanceBaseEur),
      new Decimal(0),
    )
    .toFixed(2);

  return {
    schemaVersion: "v1" as const,
    scope,
    holdings,
    quoteFreshness: holdings.some((row) => row.quoteFreshness === "fresh")
      ? ("fresh" as const)
      : holdings.some((row) => row.quoteFreshness === "delayed")
        ? ("delayed" as const)
        : holdings.some((row) => row.quoteFreshness === "stale")
          ? ("stale" as const)
          : ("missing" as const),
    brokerageCashEur,
    generatedAt: new Date().toISOString(),
  };
}

export function findMetric(
  summary: DashboardSummaryResponse,
  metricId: string,
) {
  return summary.metrics.find((metric) => metric.metricId === metricId);
}

export function buildDashboardReadModel(
  dataset: DomainDataset,
  input: {
    scope: Scope;
    displayCurrency: string;
    period?: PeriodSelection;
    referenceDate?: string;
  },
) {
  const summary = buildDashboardSummary(dataset, input);
  const personalEntityId = dataset.entities.find(
    (entity) => entity.entityKind === "personal",
  )?.id;
  const personalMetric = personalEntityId
    ? findMetric(
        buildDashboardSummary(dataset, {
          ...input,
          scope: { kind: "entity", entityId: personalEntityId },
        }),
        "net_worth_current",
      )
    : undefined;
  const totalMetric = findMetric(summary, "net_worth_current");

  return {
    summary,
    summaryBreakdown: {
      personal: personalMetric,
      companies: {
        valueDisplay: new Decimal(totalMetric?.valueDisplay ?? 0)
          .minus(personalMetric?.valueDisplay ?? 0)
          .toFixed(2),
      },
    },
  };
}

export function buildSpendingReadModel(
  dataset: DomainDataset,
  input: {
    scope: Scope;
    displayCurrency: string;
    period?: PeriodSelection;
    referenceDate?: string;
  },
) {
  const summary = buildDashboardSummary(dataset, input);
  const transactions = scopedTransactions(
    dataset,
    input.scope,
    summary.period,
    ["expense", "fee", "refund"],
  );
  const spendMetric = findMetric(summary, "spending_mtd_total");
  const merchantRows = aggregateAmountRows(
    transactions,
    (transaction) =>
      transaction.merchantNormalized ?? transaction.descriptionClean,
    (transaction) =>
      transaction.transactionClass === "refund"
        ? new Decimal(transaction.amountBaseEur).neg()
        : amountMagnitudeEur(transaction),
  );
  const coverage = spendMetric?.valueBaseEur
    ? new Decimal(1)
        .minus(
          new Decimal(summary.quality.unclassifiedAmountMtdEur).div(
            Decimal.max(new Decimal(spendMetric.valueBaseEur), new Decimal(1)),
          ),
        )
        .mul(100)
        .toFixed(2)
    : "100.00";

  return {
    summary,
    transactions,
    spendMetric,
    trailingThreeMonthAverage: averageMonthlySeries(
      summary.monthlySeries,
      "spendingEur",
      3,
    ),
    coverage,
    topCategory: summary.spendingByCategory[0],
    merchantRows,
    topMerchant: merchantRows[0] ?? null,
  };
}

export function buildIncomeReadModel(
  dataset: DomainDataset,
  input: {
    scope: Scope;
    displayCurrency: string;
    period?: PeriodSelection;
    referenceDate?: string;
  },
) {
  const summary = buildDashboardSummary(dataset, input);
  const transactions = scopedTransactions(
    dataset,
    input.scope,
    summary.period,
    ["income", "dividend", "interest"],
  );
  const incomeMetric = findMetric(summary, "income_mtd_total");
  const sourceRows = aggregateAmountRows(
    transactions,
    (transaction) =>
      transaction.counterpartyName ??
      transaction.merchantNormalized ??
      transaction.descriptionClean,
    (transaction) => new Decimal(transaction.amountBaseEur),
  );
  const investmentIncomeRows = transactions.filter((transaction) =>
    ["dividend", "interest"].includes(transaction.transactionClass),
  );
  const topSourceShare =
    sourceRows[0] && incomeMetric?.valueBaseEur
      ? new Decimal(sourceRows[0].amountEur)
          .div(
            Decimal.max(new Decimal(incomeMetric.valueBaseEur), new Decimal(1)),
          )
          .mul(100)
          .toFixed(2)
      : "0.00";

  return {
    summary,
    transactions,
    incomeMetric,
    sourceRows,
    investmentIncomeRows,
    trailingThreeMonthAverage: averageMonthlySeries(
      summary.monthlySeries,
      "incomeEur",
      3,
    ),
    topSourceShare,
    investmentIncome: sumTransactionAmounts(
      investmentIncomeRows,
      (transaction) => new Decimal(transaction.amountBaseEur),
    ),
  };
}

export function buildInvestmentsReadModel(
  dataset: DomainDataset,
  input: {
    scope: Scope;
    displayCurrency: string;
    period?: PeriodSelection;
    referenceDate?: string;
  },
) {
  const referenceDate = input.referenceDate ?? todayIso();
  const summary = buildDashboardSummary(dataset, input);
  const holdings = buildHoldingsSnapshot(dataset, input.scope, referenceDate);
  const investmentRows = scopedTransactions(
    dataset,
    input.scope,
    summary.period,
    [...investmentLedgerClasses],
  );
  const ytdPeriod = resolvePeriodSelection({ preset: "ytd", referenceDate });
  const ytdInvestmentRows = scopedTransactions(
    dataset,
    input.scope,
    ytdPeriod,
    ["dividend", "interest", "transfer_internal"],
  );
  const accountAllocation = aggregateAmountRows(
    holdings.holdings,
    (holding) =>
      dataset.accounts.find((account) => account.id === holding.accountId)
        ?.displayName ?? holding.accountId,
    (holding) => new Decimal(holding.currentValueEur ?? 0),
  );
  const unresolved = sortTransactionsNewestFirst(
    filterTransactionsByScope(dataset, input.scope).filter((transaction) => {
      const account = dataset.accounts.find(
        (candidate) => candidate.id === transaction.accountId,
      );
      return (
        account?.assetDomain === "investment" &&
        transaction.transactionDate <= referenceDate &&
        transaction.needsReview
      );
    }),
  );
  const processedRows = sortTransactionsNewestFirst(
    filterTransactionsByScope(dataset, input.scope).filter((transaction) => {
      const account = dataset.accounts.find(
        (candidate) => candidate.id === transaction.accountId,
      );
      return (
        account?.assetDomain === "investment" &&
        transaction.transactionDate <= referenceDate &&
        !transaction.needsReview &&
        processedInvestmentLedgerClasses.includes(transaction.transactionClass)
      );
    }),
  );

  return {
    holdings,
    investmentRows,
    processedRows,
    metrics: {
      portfolioValue: buildMetricResult(
        dataset,
        input.scope,
        input.displayCurrency,
        "portfolio_market_value_current",
        { referenceDate },
      ),
      unrealized: buildMetricResult(
        dataset,
        input.scope,
        input.displayCurrency,
        "portfolio_unrealized_pnl_current",
        { referenceDate },
      ),
    },
    dividendsYtd: sumTransactionAmounts(
      ytdInvestmentRows.filter(
        (transaction) => transaction.transactionClass === "dividend",
      ),
      (transaction) => new Decimal(transaction.amountBaseEur),
    ),
    interestYtd: sumTransactionAmounts(
      ytdInvestmentRows.filter(
        (transaction) => transaction.transactionClass === "interest",
      ),
      (transaction) => new Decimal(transaction.amountBaseEur),
    ),
    netContributionsYtd: sumTransactionAmounts(
      ytdInvestmentRows.filter(
        (transaction) => transaction.transactionClass === "transfer_internal",
      ),
      (transaction) => new Decimal(transaction.amountBaseEur),
    ),
    unresolved,
    accountAllocation,
  };
}

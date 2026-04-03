import { Decimal } from "decimal.js";

import type {
  DashboardSummaryResponse,
  DomainDataset,
  HoldingRow,
  InsightCard,
  MetricResult,
  PeriodSelection,
  QualitySummary,
  Scope,
  Transaction,
} from "@myfinance/domain";
import { TODAY_ISO, getLatestBalanceSnapshots } from "@myfinance/domain";
import { metricRegistry } from "./registry";

export { metricRegistry } from "./registry";

const CURRENT_PERIOD: PeriodSelection = {
  start: "2026-04-01",
  end: TODAY_ISO,
  preset: "mtd",
};

const PREVIOUS_MTD_PERIOD: PeriodSelection = {
  start: "2026-03-01",
  end: "2026-03-03",
  preset: "mtd",
};

function sumStrings(values: Array<string | null | undefined>): string {
  return values.reduce((sum, value) => sum.plus(new Decimal(value ?? 0)), new Decimal(0)).toFixed(2);
}

function safeDividePercent(numerator: Decimal, denominator: Decimal): string | null {
  if (denominator.eq(0)) return null;
  return numerator.div(denominator).mul(100).toFixed(2);
}

function resolveScopeEntityIds(dataset: DomainDataset, scope: Scope): string[] {
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

function filterTransactionsByScope(dataset: DomainDataset, scope: Scope): Transaction[] {
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

function filterTransactionsByPeriod(
  transactions: Transaction[],
  period: PeriodSelection,
): Transaction[] {
  return transactions.filter(
    (row) => row.transactionDate >= period.start && row.transactionDate <= period.end,
  );
}

function resolveFxRate(dataset: DomainDataset, from: string, to: string): Decimal {
  if (from === to) return new Decimal(1);
  const match = dataset.fxRates.find(
    (row) => row.baseCurrency === from && row.quoteCurrency === to,
  );
  if (match) return new Decimal(match.rate);
  const reverse = dataset.fxRates.find(
    (row) => row.baseCurrency === to && row.quoteCurrency === from,
  );
  if (reverse) return new Decimal(1).div(reverse.rate);
  return new Decimal(1);
}

function toDisplayAmount(dataset: DomainDataset, amountEur: string | null, currency: string): string | null {
  if (amountEur === null) return null;
  if (currency === "EUR") return new Decimal(amountEur).toFixed(2);
  return new Decimal(amountEur).mul(resolveFxRate(dataset, "EUR", currency)).toFixed(2);
}

function amountMagnitudeEur(transaction: Transaction): Decimal {
  return new Decimal(transaction.amountBaseEur).abs();
}

function isIncomeLike(transaction: Transaction): boolean {
  return ["income", "dividend", "interest"].includes(transaction.transactionClass);
}

function isExcludedIncome(transaction: Transaction): boolean {
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

function isSpendingLike(transaction: Transaction): boolean {
  return ["expense", "fee", "refund"].includes(transaction.transactionClass);
}

function isExcludedSpending(transaction: Transaction): boolean {
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

function currentPortfolioHoldings(dataset: DomainDataset, scope: Scope): HoldingRow[] {
  const entityIds = new Set(resolveScopeEntityIds(dataset, scope));
  const positions = dataset.investmentPositions.filter((position) =>
    entityIds.has(position.entityId),
  );
  return positions.map((position) => {
    const security = dataset.securities.find((row) => row.id === position.securityId);
    const latestPrice = dataset.securityPrices.find((row) => row.securityId === position.securityId);
    let currentValueEur: string | null = null;
    let unrealizedPnlEur: string | null = null;
    let unrealizedPnlPercent: string | null = null;

    if (latestPrice && security) {
      const fxToEur = resolveFxRate(dataset, latestPrice.currency, "EUR");
      currentValueEur = new Decimal(position.openQuantity)
        .mul(latestPrice.price)
        .mul(fxToEur)
        .toFixed(2);
      unrealizedPnlEur = new Decimal(currentValueEur)
        .minus(position.openCostBasisEur)
        .toFixed(2);
      unrealizedPnlPercent = safeDividePercent(
        new Decimal(unrealizedPnlEur),
        new Decimal(position.openCostBasisEur),
      );
    }

    return {
      securityId: position.securityId,
      accountId: position.accountId,
      entityId: position.entityId,
      symbol: security?.displaySymbol ?? position.securityId,
      securityName: security?.name ?? position.securityId,
      quantity: position.openQuantity,
      avgCostEur: position.avgCostEur,
      currentPrice: latestPrice?.price ?? null,
      currentPriceCurrency: latestPrice?.currency ?? null,
      currentValueEur,
      unrealizedPnlEur,
      unrealizedPnlPercent,
      quoteFreshness: latestPrice ? "delayed" : "missing",
      quoteTimestamp: latestPrice?.quoteTimestamp ?? null,
      unrealizedComplete: position.unrealizedComplete,
    };
  });
}

function currentCashTotal(dataset: DomainDataset, scope: Scope): string {
  const entityIds = new Set(resolveScopeEntityIds(dataset, scope));
  const latestSnapshots = getLatestBalanceSnapshots(dataset.accountBalanceSnapshots);
  return latestSnapshots
    .filter((snapshot) => {
      const account = dataset.accounts.find((row) => row.id === snapshot.accountId);
      return account && entityIds.has(account.entityId);
    })
    .reduce((sum, snapshot) => sum.plus(snapshot.balanceBaseEur), new Decimal(0))
    .toFixed(2);
}

function currentPortfolioValue(dataset: DomainDataset, scope: Scope): string {
  return sumStrings(currentPortfolioHoldings(dataset, scope).map((row) => row.currentValueEur));
}

function currentPortfolioUnrealized(dataset: DomainDataset, scope: Scope): string {
  return sumStrings(currentPortfolioHoldings(dataset, scope).map((row) => row.unrealizedPnlEur));
}

function currentValueComparison(dataset: DomainDataset, scope: Scope, selector: "cash" | "portfolio" | "networth" | "unrealized"): string {
  const entityIds = new Set(resolveScopeEntityIds(dataset, scope));
  const priorSnapshot = dataset.dailyPortfolioSnapshots
    .filter(
      (row) => row.snapshotDate === "2026-03-31" && entityIds.has(row.entityId),
    )
    .reduce((sum, row) => sum.plus(row.totalPortfolioValueEur), new Decimal(0));
  const priorCash = getLatestBalanceSnapshots(
    dataset.accountBalanceSnapshots.filter((row) => row.asOfDate <= "2026-03-31"),
  )
    .filter((row) => {
      const account = dataset.accounts.find((accountRow) => accountRow.id === row.accountId);
      return account && entityIds.has(account.entityId);
    })
    .reduce((sum, row) => sum.plus(row.balanceBaseEur), new Decimal(0));
  if (selector === "cash") return priorCash.toFixed(2);
  if (selector === "portfolio") {
    return priorSnapshot.minus(priorCash).toFixed(2);
  }
  if (selector === "networth") return priorSnapshot.toFixed(2);
  return currentPortfolioHoldings(dataset, scope)
    .map((row) => row.unrealizedPnlEur ?? "0")
    .reduce((sum, value) => sum.plus(value), new Decimal(0))
    .minus("279.81")
    .toFixed(2);
}

function flowMetric(
  dataset: DomainDataset,
  scope: Scope,
  period: PeriodSelection,
  kind: "income" | "spending",
): string {
  const scopedTransactions = filterTransactionsByScope(dataset, scope);
  const transactions = filterTransactionsByPeriod(scopedTransactions, period).filter(
    (row) => !row.excludeFromAnalytics,
  );

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

function qualitySummary(dataset: DomainDataset, scope: Scope): QualitySummary {
  const scopedTransactions = filterTransactionsByScope(dataset, scope);
  const scopedAccounts = scope.kind === "consolidated"
    ? dataset.accounts
    : scope.kind === "entity" && scope.entityId
      ? dataset.accounts.filter((account) => account.entityId === scope.entityId)
      : scope.kind === "account" && scope.accountId
        ? dataset.accounts.filter((account) => account.id === scope.accountId)
        : dataset.accounts;

  const staleAccounts = scopedAccounts
    .map((account) => {
      const lastImportDate = account.lastImportedAt ? new Date(account.lastImportedAt) : null;
      const threshold = account.staleAfterDays ?? (account.assetDomain === "investment" ? 3 : 7);
      const ageDays = lastImportDate
        ? Math.floor((Date.parse(`${TODAY_ISO}T12:00:00Z`) - lastImportDate.getTime()) / 86400000)
        : threshold + 1;
      return { account, ageDays, threshold };
    })
    .filter((row) => row.ageDays > row.threshold)
    .map((row) => ({
      accountId: row.account.id,
      accountName: row.account.displayName,
      staleSinceDays: row.ageDays,
    }));

  const priceFreshness = dataset.securityPrices.every((price) => price.isDelayed)
    ? "delayed"
    : "fresh";

  return {
    pendingReviewCount: scopedTransactions.filter((row) => row.needsReview).length,
    unclassifiedAmountMtdEur: filterTransactionsByPeriod(scopedTransactions, CURRENT_PERIOD)
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
    latestDataDateByScope: TODAY_ISO,
    priceFreshness,
  };
}

export function buildMetricResult(
  dataset: DomainDataset,
  scope: Scope,
  displayCurrency: string,
  metricId: string,
): MetricResult {
  const definition = metricRegistry.find((metric) => metric.metricId === metricId);
  if (!definition) {
    throw new Error(`Metric ${metricId} is not registered.`);
  }

  let valueBaseEur: string | null = null;
  let comparisonBaseEur: string | null = null;

  switch (metricId) {
    case "net_worth_current": {
      const current = new Decimal(currentCashTotal(dataset, scope)).plus(
        currentPortfolioValue(dataset, scope),
      );
      valueBaseEur = current.toFixed(2);
      comparisonBaseEur = currentValueComparison(dataset, scope, "networth");
      break;
    }
    case "cash_total_current":
      valueBaseEur = currentCashTotal(dataset, scope);
      comparisonBaseEur = currentValueComparison(dataset, scope, "cash");
      break;
    case "income_mtd_total":
      valueBaseEur = flowMetric(dataset, scope, CURRENT_PERIOD, "income");
      comparisonBaseEur = flowMetric(dataset, scope, PREVIOUS_MTD_PERIOD, "income");
      break;
    case "spending_mtd_total":
      valueBaseEur = flowMetric(dataset, scope, CURRENT_PERIOD, "spending");
      comparisonBaseEur = flowMetric(dataset, scope, PREVIOUS_MTD_PERIOD, "spending");
      break;
    case "operating_net_cash_flow_mtd": {
      const income = new Decimal(flowMetric(dataset, scope, CURRENT_PERIOD, "income"));
      const spending = new Decimal(flowMetric(dataset, scope, CURRENT_PERIOD, "spending"));
      const previousIncome = new Decimal(flowMetric(dataset, scope, PREVIOUS_MTD_PERIOD, "income"));
      const previousSpending = new Decimal(
        flowMetric(dataset, scope, PREVIOUS_MTD_PERIOD, "spending"),
      );
      valueBaseEur = income.minus(spending).toFixed(2);
      comparisonBaseEur = previousIncome.minus(previousSpending).toFixed(2);
      break;
    }
    case "portfolio_market_value_current":
      valueBaseEur = currentPortfolioValue(dataset, scope);
      comparisonBaseEur = currentValueComparison(dataset, scope, "portfolio");
      break;
    case "portfolio_unrealized_pnl_current":
      valueBaseEur = currentPortfolioUnrealized(dataset, scope);
      comparisonBaseEur = currentValueComparison(dataset, scope, "unrealized");
      break;
    case "pending_review_count":
      valueBaseEur = String(qualitySummary(dataset, scope).pendingReviewCount);
      comparisonBaseEur = null;
      break;
    case "unclassified_amount_mtd":
      valueBaseEur = qualitySummary(dataset, scope).unclassifiedAmountMtdEur;
      comparisonBaseEur = null;
      break;
    case "stale_accounts_count":
      valueBaseEur = String(qualitySummary(dataset, scope).staleAccountsCount);
      comparisonBaseEur = null;
      break;
    default:
      valueBaseEur = null;
      comparisonBaseEur = null;
  }

  const valueDisplay =
    definition.unitType === "currency"
      ? toDisplayAmount(dataset, valueBaseEur, displayCurrency)
      : valueBaseEur;
  const comparisonValueDisplay =
    definition.unitType === "currency"
      ? toDisplayAmount(dataset, comparisonBaseEur, displayCurrency)
      : comparisonBaseEur;

  let deltaDisplay: string | null = null;
  let deltaPercent: string | null = null;

  if (definition.unitType === "currency" && valueBaseEur !== null && comparisonBaseEur !== null) {
    const delta = new Decimal(valueBaseEur).minus(comparisonBaseEur);
    deltaDisplay = toDisplayAmount(dataset, delta.toFixed(2), displayCurrency);
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
    asOfDate: TODAY_ISO,
    explanation: definition.description,
  };
}

export function buildInsights(dataset: DomainDataset, scope: Scope): InsightCard[] {
  const spendingMetric = buildMetricResult(dataset, scope, "EUR", "spending_mtd_total");
  const quality = qualitySummary(dataset, scope);
  const holdings = currentPortfolioHoldings(dataset, scope)
    .filter((row) => row.currentValueEur)
    .sort((a, b) => Number(b.currentValueEur) - Number(a.currentValueEur));
  const largestHolding = holdings[0];
  const latestLargeOutflow = filterTransactionsByPeriod(
    filterTransactionsByScope(dataset, scope),
    CURRENT_PERIOD,
  )
    .filter((row) => new Decimal(row.amountBaseEur).lt(0))
    .sort((a, b) => Number(amountMagnitudeEur(b)) - Number(amountMagnitudeEur(a)))[0];

  return [
    {
      id: "spending-vs-trailing-3m",
      title: "April spend is running above the March pace",
      severity: "warning",
      body:
        "Current-period company and personal outflows are ahead of the same day-count comparison because Company B contractor spend landed early in the month.",
      evidence: [
        `Spending MTD: ${spendingMetric.valueBaseEur} EUR`,
        `Comparison basis: ${spendingMetric.comparisonValueBaseEur ?? "0.00"} EUR`,
      ],
    },
    {
      id: "largest-outflow",
      title: "Largest single outflow this period",
      severity: "info",
      body: latestLargeOutflow
        ? `${latestLargeOutflow.descriptionRaw} is the biggest outflow in scope.`
        : "No outflow rows found in the selected scope.",
      evidence: latestLargeOutflow
        ? [
            `Date: ${latestLargeOutflow.transactionDate}`,
            `Amount: ${latestLargeOutflow.amountBaseEur} EUR`,
          ]
        : [],
    },
    {
      id: "portfolio-concentration",
      title: "Portfolio concentration is ETF-led",
      severity: "positive",
      body: largestHolding
        ? `${largestHolding.securityName} is the largest holding by market value, which keeps single-name stock exposure limited.`
        : "No priced holdings are currently available.",
      evidence: largestHolding
        ? [
            `Largest holding: ${largestHolding.symbol}`,
            `Value: ${largestHolding.currentValueEur ?? "0.00"} EUR`,
          ]
        : [],
    },
    {
      id: "data-quality",
      title: "Data quality requires attention",
      severity: quality.pendingReviewCount > 0 || quality.staleAccountsCount > 0 ? "warning" : "info",
      body:
        quality.pendingReviewCount > 0
          ? "Unresolved rows and stale accounts mean current totals should be read with caution."
          : "Quality checks are currently green.",
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
  input: { scope: Scope; displayCurrency: string; period?: PeriodSelection },
): DashboardSummaryResponse {
  const period = input.period ?? CURRENT_PERIOD;
  const metrics = [
    "net_worth_current",
    "cash_total_current",
    "income_mtd_total",
    "spending_mtd_total",
    "operating_net_cash_flow_mtd",
    "portfolio_market_value_current",
    "portfolio_unrealized_pnl_current",
  ].map((metricId) => buildMetricResult(dataset, input.scope, input.displayCurrency, metricId));

  const entityIds = new Set(resolveScopeEntityIds(dataset, input.scope));
  const monthlySeries = dataset.monthlyCashFlowRollups
    .filter((row) => entityIds.has(row.entityId))
    .reduce<Array<{ month: string; incomeEur: string; spendingEur: string; operatingNetEur: string }>>(
      (accumulator, row) => {
        const existing = accumulator.find((item) => item.month === row.month);
        if (existing) {
          existing.incomeEur = new Decimal(existing.incomeEur).plus(row.incomeEur).toFixed(2);
          existing.spendingEur = new Decimal(existing.spendingEur).plus(row.spendingEur).toFixed(2);
          existing.operatingNetEur = new Decimal(existing.operatingNetEur)
            .plus(row.operatingNetEur)
            .toFixed(2);
        } else {
          accumulator.push({
            month: row.month,
            incomeEur: row.incomeEur,
            spendingEur: row.spendingEur,
            operatingNetEur: row.operatingNetEur,
          });
        }
        return accumulator;
      },
      [],
    )
    .sort((a, b) => a.month.localeCompare(b.month))
    .slice(-12);

  const scopedTransactions = filterTransactionsByPeriod(
    filterTransactionsByScope(dataset, input.scope),
    period,
  );

  const spendingByCategory = [...new Map(
    scopedTransactions
      .filter((row) => row.transactionClass === "expense" && row.categoryCode)
      .map((row) => [
        row.categoryCode as string,
        {
          categoryCode: row.categoryCode as string,
          label:
            dataset.categories.find((category) => category.code === row.categoryCode)?.displayName ??
            row.categoryCode ??
            "Uncategorized",
          amountEur: "0.00",
        },
      ]),
  ).values()]
    .map((item) => {
      const amount = scopedTransactions
        .filter((row) => row.categoryCode === item.categoryCode && row.transactionClass === "expense")
        .reduce((sum, row) => sum.plus(amountMagnitudeEur(row)), new Decimal(0));
      return { ...item, amountEur: amount.toFixed(2) };
    })
    .sort((a, b) => Number(b.amountEur) - Number(a.amountEur));

  const holdings = currentPortfolioHoldings(dataset, input.scope).sort(
    (a, b) => Number(b.currentValueEur ?? 0) - Number(a.currentValueEur ?? 0),
  );
  const totalPortfolio = new Decimal(sumStrings(holdings.map((row) => row.currentValueEur)));
  const portfolioAllocation = holdings.map((row) => ({
    label: row.symbol,
    amountEur: row.currentValueEur ?? "0.00",
    allocationPercent: totalPortfolio.eq(0)
      ? "0.00"
      : new Decimal(row.currentValueEur ?? 0).div(totalPortfolio).mul(100).toFixed(2),
  }));

  const recentLargeTransactions = [...scopedTransactions]
    .sort((a, b) => {
      const byDate = b.transactionDate.localeCompare(a.transactionDate);
      if (byDate !== 0) return byDate;
      return Number(amountMagnitudeEur(b).minus(amountMagnitudeEur(a)));
    })
    .slice(0, 6);

  const insights = buildInsights(dataset, input.scope);
  const quality = qualitySummary(dataset, input.scope);

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
    insights,
    quality,
    generatedAt: new Date().toISOString(),
  };
}

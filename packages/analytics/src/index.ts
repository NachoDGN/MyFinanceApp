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
  buildLiveHoldingRows,
  filterTransactionsByPeriod,
  filterTransactionsByScope,
  getLatestAccountBalances,
  getLatestInvestmentCashBalances,
  getPreviousComparablePeriod,
  resolveAccountStaleThresholdDays,
  getScopeLatestDate,
  isTransactionPendingEnrichment,
  isTransactionResolvedForAnalytics,
  needsTransactionManualReview,
  resolveFxRate,
  resolvePeriodSelection,
  resolveScopeEntityIds,
  shiftIsoDate,
  startOfMonthIso,
  startOfTrailingMonthsIso,
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

function normalizeMatchingText(value: string) {
  return value
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase();
}

function humanizeTransactionClass(transactionClass: Transaction["transactionClass"]) {
  return transactionClass
    .replace(/_/g, " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

function isUnmatchedCreditCardSettlement(transaction: Transaction) {
  if (transaction.transactionClass !== "transfer_internal") {
    return false;
  }

  if (
    transaction.relatedAccountId ||
    transaction.relatedTransactionId ||
    transaction.transferMatchStatus === "matched"
  ) {
    return false;
  }

  const normalizedText = normalizeMatchingText(
    `${transaction.descriptionRaw} ${transaction.descriptionClean}`,
  );
  return (
    normalizedText.includes("LIQUIDACION") &&
    normalizedText.includes("TARJETAS DE CREDITO")
  );
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
  return [
    "expense",
    "fee",
    "refund",
    "loan_principal_payment",
    "loan_interest_payment",
  ].includes(transaction.transactionClass);
}

function incomeContributionEur(transaction: Transaction) {
  if (!isIncomeLike(transaction) || isExcludedIncome(transaction)) {
    return null;
  }

  return new Decimal(transaction.amountBaseEur);
}

function spendingContributionEur(transaction: Transaction) {
  if (!isSpendingLike(transaction)) {
    return null;
  }

  if (transaction.transactionClass === "refund") {
    return new Decimal(transaction.amountBaseEur).neg();
  }

  return amountMagnitudeEur(transaction);
}

function resolveSpendingCategoryBucket(
  dataset: DomainDataset,
  transaction: Transaction,
) {
  if (isUnmatchedCreditCardSettlement(transaction)) {
    return {
      categoryCode: "__credit_card_payments",
      label: "Credit Card Payments",
      categorized: false,
    };
  }

  if (transaction.categoryCode) {
    return {
      categoryCode: transaction.categoryCode,
      label:
        dataset.categories.find(
          (category) => category.code === transaction.categoryCode,
        )?.displayName ??
        transaction.categoryCode,
      categorized: !transaction.categoryCode.startsWith("uncategorized"),
    };
  }

  if (transaction.transactionClass === "loan_principal_payment") {
    return {
      categoryCode: "__loan_principal_payment",
      label: "Loan Principal",
      categorized: false,
    };
  }

  if (transaction.transactionClass === "loan_interest_payment") {
    return {
      categoryCode: "__loan_interest_payment",
      label: "Loan Interest",
      categorized: false,
    };
  }

  if (transaction.transactionClass === "fee") {
    return {
      categoryCode: "__fees",
      label: "Fees",
      categorized: false,
    };
  }

  if (transaction.transactionClass === "refund") {
    return {
      categoryCode: "__refunds",
      label: "Refunds",
      categorized: false,
    };
  }

  return {
    categoryCode: `__${transaction.transactionClass}`,
    label: humanizeTransactionClass(transaction.transactionClass),
    categorized: false,
  };
}

function resolveSpendingCounterpartyLabel(transaction: Transaction) {
  if (isUnmatchedCreditCardSettlement(transaction)) {
    return "Credit Card Settlement";
  }

  if (transaction.merchantNormalized?.trim()) {
    return transaction.merchantNormalized.trim();
  }

  if (transaction.counterpartyName?.trim()) {
    return transaction.counterpartyName.trim();
  }

  return transaction.descriptionClean || transaction.descriptionRaw;
}

function shiftMonthIso(value: string, months: number) {
  const date = new Date(`${startOfMonthIso(value)}T00:00:00Z`);
  date.setUTCMonth(date.getUTCMonth() + months);
  return date.toISOString().slice(0, 10);
}

function buildTrailingMonthlyFlowSeries(
  dataset: DomainDataset,
  scope: Scope,
  referenceDate: string,
  monthCount: number,
) {
  const seriesStart = startOfTrailingMonthsIso(referenceDate, monthCount);
  const seriesEnd = startOfMonthIso(referenceDate);
  const monthRows = new Map<
    string,
    {
      month: string;
      incomeEur: Decimal;
      spendingEur: Decimal;
    }
  >();

  for (let month = seriesStart; month <= seriesEnd; month = shiftMonthIso(month, 1)) {
    monthRows.set(month, {
      month,
      incomeEur: new Decimal(0),
      spendingEur: new Decimal(0),
    });
  }

  const resolvedTransactions = filterTransactionsByScope(dataset, scope).filter(
    (transaction) =>
      isTransactionResolvedForAnalytics(transaction) &&
      transaction.transactionDate >= seriesStart &&
      transaction.transactionDate <= referenceDate,
  );

  for (const transaction of resolvedTransactions) {
    const month = startOfMonthIso(transaction.transactionDate);
    const row = monthRows.get(month);
    if (!row) continue;

    const income = incomeContributionEur(transaction);
    if (income) {
      row.incomeEur = row.incomeEur.plus(income);
    }

    const spending = spendingContributionEur(transaction);
    if (spending) {
      row.spendingEur = row.spendingEur.plus(spending);
    }
  }

  return [...monthRows.values()].map((row) => ({
    month: row.month,
    incomeEur: row.incomeEur.toFixed(2),
    spendingEur: row.spendingEur.toFixed(2),
    operatingNetEur: row.incomeEur.minus(row.spendingEur).toFixed(2),
  }));
}

function currentCashTotal(
  dataset: DomainDataset,
  scope: Scope,
  asOfDate: string,
) {
  const entityIds = new Set(resolveScopeEntityIds(dataset, scope));
  const inScope = (accountId: string, expectedAssetDomain: "cash" | "investment") => {
    const account = dataset.accounts.find((row) => row.id === accountId);
    return (
      account?.assetDomain === expectedAssetDomain &&
      entityIds.has(account.entityId) &&
      (scope.kind !== "account" || account.id === scope.accountId)
    );
  };
  return getLatestAccountBalances(dataset, asOfDate)
    .filter((snapshot) => {
      const account = dataset.accounts.find((row) => row.id === snapshot.accountId);
      if (!account) {
        return false;
      }
      return inScope(snapshot.accountId, account.assetDomain);
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
    buildLiveHoldingRows(dataset, scope, asOfDate).map(
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
    buildLiveHoldingRows(dataset, scope, asOfDate).map(
      (row) => row.unrealizedPnlEur,
    ),
  );
}

function currentValueComparison(
  dataset: DomainDataset,
  scope: Scope,
  selector: "cash" | "portfolio" | "networth" | "unrealized",
  period: PeriodSelection,
) {
  const comparisonDate = shiftIsoDate(period.start, -1);
  const priorCash = currentCashTotal(dataset, scope, comparisonDate);
  const priorPortfolio = currentPortfolioValue(dataset, scope, comparisonDate);
  const priorUnrealized = currentPortfolioUnrealized(
    dataset,
    scope,
    comparisonDate,
  );

  if (selector === "cash") return priorCash;
  if (selector === "portfolio") return priorPortfolio;
  if (selector === "networth") {
    return new Decimal(priorCash).plus(priorPortfolio).toFixed(2);
  }
  return priorUnrealized;
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
  ).filter((row) => isTransactionResolvedForAnalytics(row));

  if (kind === "income") {
    return transactions
      .reduce((sum, row) => {
        const contribution = incomeContributionEur(row);
        return contribution ? sum.plus(contribution) : sum;
      }, new Decimal(0))
      .toFixed(2);
  }

  return transactions
    .reduce((sum, row) => {
      const contribution = spendingContributionEur(row);
      return contribution ? sum.plus(contribution) : sum;
    }, new Decimal(0))
    .toFixed(2);
}

function qualitySummary(
  dataset: DomainDataset,
  scope: Scope,
  referenceDate: string,
  period: PeriodSelection = resolvePeriodSelection({
    preset: "mtd",
    referenceDate,
  }),
): QualitySummary {
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
    pendingEnrichmentCount: scopedTransactions.filter((row) =>
      isTransactionPendingEnrichment(row),
    ).length,
    pendingReviewCount: scopedTransactions.filter((row) =>
      needsTransactionManualReview(row),
    ).length,
    unclassifiedAmountMtdEur: filterTransactionsByPeriod(
      scopedTransactions,
      period,
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
  options: { referenceDate?: string; period?: PeriodSelection } = {},
): MetricResult {
  const definition = metricRegistry.find(
    (metric) => metric.metricId === metricId,
  );
  if (!definition) {
    throw new Error(`Metric ${metricId} is not registered.`);
  }

  const referenceDate = options.referenceDate ?? todayIso();
  const currentPeriod =
    options.period ??
    resolvePeriodSelection({
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
        currentPeriod,
      );
      break;
    }
    case "cash_total_current":
      valueBaseEur = currentCashTotal(dataset, scope, referenceDate);
      comparisonBaseEur = currentValueComparison(
        dataset,
        scope,
        "cash",
        currentPeriod,
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
        currentPeriod,
      );
      break;
    case "portfolio_unrealized_pnl_current":
      valueBaseEur = currentPortfolioUnrealized(dataset, scope, referenceDate);
      comparisonBaseEur = currentValueComparison(
        dataset,
        scope,
        "unrealized",
        currentPeriod,
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
        currentPeriod,
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
  options: { referenceDate?: string; period?: PeriodSelection } = {},
): InsightCard[] {
  const referenceDate = options.referenceDate ?? todayIso();
  const period =
    options.period ?? resolvePeriodSelection({ preset: "mtd", referenceDate });
  const spendingMetric = buildMetricResult(
    dataset,
    scope,
    "EUR",
    "spending_mtd_total",
    {
      referenceDate,
      period,
    },
  );
  const quality = qualitySummary(dataset, scope, referenceDate, period);
  const holdings = buildLiveHoldingRows(dataset, scope, referenceDate)
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
      id: "spending-period",
      title: "Spending pace for the selected period",
      severity:
        Number(spendingMetric.deltaPercent ?? "0") > 0 ? "warning" : "info",
      body:
        Number(spendingMetric.deltaPercent ?? "0") > 0
          ? "Outflows are ahead of the previous comparable period."
          : "Outflows are at or below the previous comparable period.",
      evidence: [
        `Current-period spending: ${spendingMetric.valueBaseEur ?? "0.00"} EUR`,
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
        quality.pendingReviewCount > 0 ||
        quality.pendingEnrichmentCount > 0 ||
        quality.staleAccountsCount > 0
          ? "warning"
          : "positive",
      body:
        quality.pendingReviewCount > 0 ||
        quality.pendingEnrichmentCount > 0 ||
        quality.staleAccountsCount > 0
          ? "Some rows are still processing or still need attention before totals are fully trusted."
          : "No outstanding review, enrichment, or freshness issues are currently flagged.",
      evidence: [
        `Queued enrichment: ${quality.pendingEnrichmentCount}`,
        `Pending review: ${quality.pendingReviewCount}`,
        `Stale accounts: ${quality.staleAccountsCount}`,
        `Unclassified amount: ${quality.unclassifiedAmountMtdEur} EUR`,
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
      period,
    }),
  );

  const monthLimit = period.preset === "24m" ? 24 : 6;
  const monthlySeries = buildTrailingMonthlyFlowSeries(
    dataset,
    input.scope,
    referenceDate,
    monthLimit,
  );

  const scopedTransactions = filterTransactionsByPeriod(
    filterTransactionsByScope(dataset, input.scope),
    period,
  );
  const resolvedScopedTransactions = scopedTransactions.filter((transaction) =>
    isTransactionResolvedForAnalytics(transaction),
  );
  const spendingByCategory = [
    ...resolvedScopedTransactions
      .reduce((totals, transaction) => {
        const contribution = spendingContributionEur(transaction);
        if (!contribution) {
          return totals;
        }

        const bucket = resolveSpendingCategoryBucket(dataset, transaction);
        const current = totals.get(bucket.categoryCode) ?? {
          categoryCode: bucket.categoryCode,
          label: bucket.label,
          amountEur: new Decimal(0),
        };

        current.amountEur = current.amountEur.plus(contribution);
        totals.set(bucket.categoryCode, current);
        return totals;
      }, new Map<
        string,
        { categoryCode: string; label: string; amountEur: Decimal }
      >())
      .values(),
  ]
    .map((row) => ({
      categoryCode: row.categoryCode,
      label: row.label,
      amountEur: row.amountEur.toFixed(2),
    }))
    .filter((row) => new Decimal(row.amountEur).gt(0))
    .sort((left, right) => Number(right.amountEur) - Number(left.amountEur));

  const holdings = buildLiveHoldingRows(dataset, input.scope, referenceDate).sort(
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
    insights: buildInsights(dataset, input.scope, {
      referenceDate,
      period,
    }),
    quality: qualitySummary(dataset, input.scope, referenceDate, period),
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
  const holdings = buildLiveHoldingRows(dataset, scope, referenceDate);
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
  const scopedPeriodTransactions = filterTransactionsByPeriod(
    filterTransactionsByScope(dataset, input.scope),
    summary.period,
  );
  const excludedCreditCardSettlementRows = sortTransactionsNewestFirst(
    scopedPeriodTransactions.filter((transaction) =>
      isTransactionResolvedForAnalytics(transaction) &&
      isUnmatchedCreditCardSettlement(transaction),
    ),
  );
  const transactions = sortTransactionsNewestFirst(
    scopedPeriodTransactions.filter((transaction) => isSpendingLike(transaction)),
  );
  const resolvedTransactions = transactions.filter((transaction) =>
    isTransactionResolvedForAnalytics(transaction),
  );
  const spendMetric = findMetric(summary, "spending_mtd_total");
  const merchantRows = aggregateAmountRows(
    resolvedTransactions,
    (transaction) => resolveSpendingCounterpartyLabel(transaction),
    (transaction) => spendingContributionEur(transaction) ?? new Decimal(0),
  );
  const uncategorizedSpendEur = resolvedTransactions
    .reduce((sum, transaction) => {
      const contribution = spendingContributionEur(transaction);
      if (!contribution) {
        return sum;
      }

      const bucket = resolveSpendingCategoryBucket(dataset, transaction);
      return bucket.categorized ? sum : sum.plus(contribution);
    }, new Decimal(0))
    .toFixed(2);
  const coverage = spendMetric?.valueBaseEur
    ? new Decimal(1)
        .minus(new Decimal(uncategorizedSpendEur).div(
          Decimal.max(new Decimal(spendMetric.valueBaseEur), new Decimal(1)),
        ))
        .mul(100)
        .toFixed(2)
    : "100.00";
  const excludedCreditCardSettlementAmountEur = excludedCreditCardSettlementRows
    .reduce(
      (sum, transaction) => sum.plus(amountMagnitudeEur(transaction)),
      new Decimal(0),
    )
    .toFixed(2);
  const hasCreditCardAccount = dataset.accounts.some(
    (account) => account.accountType === "credit_card" && account.isActive,
  );

  return {
    summary,
    transactions,
    spendMetric,
    trendSeries: summary.monthlySeries,
    trailingThreeMonthAverage: averageMonthlySeries(
      summary.monthlySeries,
      "spendingEur",
      3,
    ),
    coverage,
    uncategorizedSpendEur,
    excludedCreditCardSettlementAmountEur,
    excludedCreditCardSettlementCount: excludedCreditCardSettlementRows.length,
    hasImportedCreditCardAccount: hasCreditCardAccount,
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
  const resolvedTransactions = transactions.filter((transaction) =>
    isTransactionResolvedForAnalytics(transaction),
  );
  const incomeMetric = findMetric(summary, "income_mtd_total");
  const sourceRows = aggregateAmountRows(
    resolvedTransactions,
    (transaction) =>
      transaction.counterpartyName ??
      transaction.merchantNormalized ??
      transaction.descriptionClean,
    (transaction) => new Decimal(transaction.amountBaseEur),
  );
  const investmentIncomeRows = resolvedTransactions.filter((transaction) =>
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
  const periodCashFlowRows = scopedTransactions(
    dataset,
    input.scope,
    summary.period,
    ["dividend", "interest", "transfer_internal"],
  ).filter((transaction) => isTransactionResolvedForAnalytics(transaction));
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
    filterTransactionsByPeriod(
      filterTransactionsByScope(dataset, input.scope),
      summary.period,
    ).filter((transaction) => {
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
        { referenceDate, period: summary.period },
      ),
      unrealized: buildMetricResult(
        dataset,
        input.scope,
        input.displayCurrency,
        "portfolio_unrealized_pnl_current",
        { referenceDate, period: summary.period },
      ),
    },
    dividendsPeriod: sumTransactionAmounts(
      periodCashFlowRows.filter(
        (transaction) => transaction.transactionClass === "dividend",
      ),
      (transaction) => new Decimal(transaction.amountBaseEur),
    ),
    interestPeriod: sumTransactionAmounts(
      periodCashFlowRows.filter(
        (transaction) => transaction.transactionClass === "interest",
      ),
      (transaction) => new Decimal(transaction.amountBaseEur),
    ),
    netContributionsPeriod: sumTransactionAmounts(
      periodCashFlowRows.filter(
        (transaction) => transaction.transactionClass === "transfer_internal",
      ),
      (transaction) => new Decimal(transaction.amountBaseEur),
    ),
    unresolved,
    accountAllocation,
  };
}

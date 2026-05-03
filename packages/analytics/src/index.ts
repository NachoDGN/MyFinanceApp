import { Decimal } from "decimal.js";

import type {
  DashboardSummaryResponse,
  DomainDataset,
  InsightCard,
  MetricResult,
  PeriodSelection,
  Scope,
  Transaction,
} from "@myfinance/domain";
import {
  buildCryptoBalanceRows,
  buildLiveHoldingRows,
  filterTransactionsByPeriod,
  filterTransactionsByReferenceDate,
  filterTransactionsByScope,
  getLatestAccountBalances,
  getPreviousComparablePeriod,
  needsCreditCardStatementUpload,
  isTransactionResolvedForAnalytics,
  isCryptoCurrency,
  isUnmatchedCreditCardSettlementTransaction,
  needsTransactionManualReview,
  buildHoldingsSnapshot,
  buildQualitySummary,
  resolveFxRate,
  resolvePeriodSelection,
  resolveScopeEntityIds,
  shiftIsoDate,
  startOfMonthIso,
  startOfTrailingMonthsIso,
  todayIso,
  normalizeMatcherText,
} from "@myfinance/domain";
import { metricRegistry } from "./registry";
import { buildAnalyticsReadModelContext } from "./read-model-context";

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

function humanizeTransactionClass(
  transactionClass: Transaction["transactionClass"],
) {
  return transactionClass
    .replace(/_/g, " ")
    .replace(/\b\w/g, (character) => character.toUpperCase());
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
  categoryByCode: Map<string, DomainDataset["categories"][number]>,
  transaction: Transaction,
) {
  if (isUnmatchedCreditCardSettlementTransaction(transaction)) {
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
        categoryByCode.get(transaction.categoryCode)?.displayName ??
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
  if (isUnmatchedCreditCardSettlementTransaction(transaction)) {
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

type SpendingCategoryAmountRow = {
  categoryCode: string;
  label: string;
  amountEur: string;
};

type SpendingCategoryMonthlySeriesRow = {
  month: string;
  totalSpendingEur: string;
  categories: SpendingCategoryAmountRow[];
};

function resolveSpendingCategoryLabel(
  categoryByCode: Map<string, DomainDataset["categories"][number]>,
  categoryCode: string,
) {
  const explicitCategory = categoryByCode.get(categoryCode);
  if (explicitCategory) {
    return explicitCategory.displayName;
  }

  switch (categoryCode) {
    case "__credit_card_payments":
      return "Credit Card Payments";
    case "__loan_principal_payment":
      return "Loan Principal";
    case "__loan_interest_payment":
      return "Loan Interest";
    case "__fees":
      return "Fees";
    case "__refunds":
      return "Refunds";
    default:
      return categoryCode.startsWith("__")
        ? humanizeTransactionClass(
            categoryCode.slice(2) as Transaction["transactionClass"],
          )
        : null;
  }
}

function aggregateSpendingCategoryRows(
  transactions: Transaction[],
  categoryByCode: Map<string, DomainDataset["categories"][number]>,
): SpendingCategoryAmountRow[] {
  return [
    ...transactions
      .reduce((totals, transaction) => {
        const contribution = spendingContributionEur(transaction);
        if (!contribution) {
          return totals;
        }

        const bucket = resolveSpendingCategoryBucket(
          categoryByCode,
          transaction,
        );
        const current = totals.get(bucket.categoryCode) ?? {
          categoryCode: bucket.categoryCode,
          label: bucket.label,
          amountEur: new Decimal(0),
        };

        current.amountEur = current.amountEur.plus(contribution);
        totals.set(bucket.categoryCode, current);
        return totals;
      }, new Map<string, { categoryCode: string; label: string; amountEur: Decimal }>())
      .values(),
  ]
    .map((row) => ({
      categoryCode: row.categoryCode,
      label: row.label,
      amountEur: row.amountEur.toFixed(2),
    }))
    .filter((row) => new Decimal(row.amountEur).gt(0))
    .sort((left, right) => Number(right.amountEur) - Number(left.amountEur));
}

function shiftMonthIso(value: string, months: number) {
  const date = new Date(`${startOfMonthIso(value)}T00:00:00Z`);
  date.setUTCMonth(date.getUTCMonth() + months);
  return date.toISOString().slice(0, 10);
}

function resolvedTransactionsInWindow(
  dataset: DomainDataset,
  scope: Scope,
  startDate: string,
  endDate: string,
) {
  return filterTransactionsByReferenceDate(
    filterTransactionsByScope(dataset, scope),
    endDate,
  ).filter(
    (transaction) =>
      isTransactionResolvedForAnalytics(transaction) &&
      transaction.transactionDate >= startDate,
  );
}

function buildMonthlyTransactionSeries<T extends Record<string, Decimal>, R>(
  dataset: DomainDataset,
  scope: Scope,
  startDate: string,
  endDate: string,
  seed: () => T,
  fold: (row: { month: string } & T, transaction: Transaction) => void,
  serialize: (row: { month: string } & T) => R,
) {
  const seriesStart = startOfMonthIso(startDate);
  const seriesEnd = startOfMonthIso(endDate);
  const monthRows = new Map<string, { month: string } & T>();

  for (
    let month = seriesStart;
    month <= seriesEnd;
    month = shiftMonthIso(month, 1)
  ) {
    monthRows.set(month, { month, ...seed() });
  }

  for (const transaction of resolvedTransactionsInWindow(
    dataset,
    scope,
    startDate,
    endDate,
  )) {
    const month = startOfMonthIso(transaction.transactionDate);
    const row = monthRows.get(month);
    if (!row) continue;
    fold(row, transaction);
  }

  return [...monthRows.values()].map(serialize);
}

function buildMonthlyFlowSeries(
  dataset: DomainDataset,
  scope: Scope,
  startDate: string,
  endDate: string,
) {
  return buildMonthlyTransactionSeries(
    dataset,
    scope,
    startDate,
    endDate,
    () => ({ incomeEur: new Decimal(0), spendingEur: new Decimal(0) }),
    (row, transaction) => {
      const income = incomeContributionEur(transaction);
      const spending = spendingContributionEur(transaction);
      if (income) row.incomeEur = row.incomeEur.plus(income);
      if (spending) row.spendingEur = row.spendingEur.plus(spending);
    },
    (row) => ({
      month: row.month,
      incomeEur: row.incomeEur.toFixed(2),
      spendingEur: row.spendingEur.toFixed(2),
      operatingNetEur: row.incomeEur.minus(row.spendingEur).toFixed(2),
    }),
  );
}

function buildMonthlySpendingCategorySeries(
  dataset: DomainDataset,
  scope: Scope,
  startDate: string,
  endDate: string,
  categoryByCode: Map<string, DomainDataset["categories"][number]>,
): SpendingCategoryMonthlySeriesRow[] {
  const seriesStart = startOfMonthIso(startDate);
  const seriesEnd = startOfMonthIso(endDate);
  const monthRows = new Map<
    string,
    {
      month: string;
      totalSpendingEur: Decimal;
      categoryTotals: Map<
        string,
        { categoryCode: string; label: string; amountEur: Decimal }
      >;
    }
  >();

  for (
    let month = seriesStart;
    month <= seriesEnd;
    month = shiftMonthIso(month, 1)
  ) {
    monthRows.set(month, {
      month,
      totalSpendingEur: new Decimal(0),
      categoryTotals: new Map(),
    });
  }

  for (const transaction of resolvedTransactionsInWindow(
    dataset,
    scope,
    startDate,
    endDate,
  )) {
    const contribution = spendingContributionEur(transaction);
    if (!contribution) {
      continue;
    }

    const month = startOfMonthIso(transaction.transactionDate);
    const row = monthRows.get(month);
    if (!row) {
      continue;
    }

    const bucket = resolveSpendingCategoryBucket(categoryByCode, transaction);
    const category = row.categoryTotals.get(bucket.categoryCode) ?? {
      categoryCode: bucket.categoryCode,
      label: bucket.label,
      amountEur: new Decimal(0),
    };

    category.amountEur = category.amountEur.plus(contribution);
    row.totalSpendingEur = row.totalSpendingEur.plus(contribution);
    row.categoryTotals.set(bucket.categoryCode, category);
  }

  return [...monthRows.values()].map((row) => ({
    month: row.month,
    totalSpendingEur: row.totalSpendingEur.toFixed(2),
    categories: [...row.categoryTotals.values()]
      .map((category) => ({
        categoryCode: category.categoryCode,
        label: category.label,
        amountEur: category.amountEur.toFixed(2),
      }))
      .filter((category) => new Decimal(category.amountEur).gt(0))
      .sort((left, right) => Number(right.amountEur) - Number(left.amountEur)),
  }));
}

function resolveScopedSeriesStartDate(
  dataset: DomainDataset,
  scope: Scope,
  referenceDate: string,
) {
  const scopedTransactions = filterTransactionsByReferenceDate(
    filterTransactionsByScope(dataset, scope),
    referenceDate,
  );
  if (scopedTransactions.length === 0) {
    return referenceDate;
  }

  return scopedTransactions.reduce(
    (earliest, transaction) =>
      transaction.transactionDate < earliest
        ? transaction.transactionDate
        : earliest,
    scopedTransactions[0]!.transactionDate,
  );
}

function buildMonthlyIncomeComposition(
  dataset: DomainDataset,
  scope: Scope,
  startDate: string,
  endDate: string,
) {
  return buildMonthlyTransactionSeries(
    dataset,
    scope,
    startDate,
    endDate,
    () => ({
      operatingIncomeEur: new Decimal(0),
      investmentIncomeEur: new Decimal(0),
    }),
    (row, transaction) => {
      const contribution = incomeContributionEur(transaction);
      if (!contribution) return;
      if (["dividend", "interest"].includes(transaction.transactionClass)) {
        row.investmentIncomeEur = row.investmentIncomeEur.plus(contribution);
      } else {
        row.operatingIncomeEur = row.operatingIncomeEur.plus(contribution);
      }
    },
    (row) => ({
      month: row.month,
      operatingIncomeEur: row.operatingIncomeEur.toFixed(2),
      investmentIncomeEur: row.investmentIncomeEur.toFixed(2),
      totalIncomeEur: row.operatingIncomeEur
        .plus(row.investmentIncomeEur)
        .toFixed(2),
    }),
  );
}

function resolveMonthlySeriesWindow(
  dataset: DomainDataset,
  scope: Scope,
  period: PeriodSelection,
  referenceDate: string,
  fallbackMonthCount: number,
) {
  if (period.preset === "custom")
    return { start: period.start, end: period.end };
  if (period.preset === "all") {
    return {
      start: resolveScopedSeriesStartDate(dataset, scope, referenceDate),
      end: period.end,
    };
  }
  return {
    start: startOfTrailingMonthsIso(referenceDate, fallbackMonthCount),
    end: referenceDate,
  };
}

function currentCashTotal(
  dataset: DomainDataset,
  scope: Scope,
  asOfDate: string,
) {
  const entityIds = new Set(resolveScopeEntityIds(dataset, scope));
  const inScope = (
    accountId: string,
    expectedAssetDomain: "cash" | "investment",
    balanceCurrency?: string,
  ) => {
    const account = dataset.accounts.find((row) => row.id === accountId);
    return (
      account?.assetDomain === expectedAssetDomain &&
      !(
        expectedAssetDomain === "cash" && account.accountType === "credit_card"
      ) &&
      !(
        expectedAssetDomain === "cash" &&
        isCryptoCurrency(balanceCurrency ?? account.defaultCurrency)
      ) &&
      entityIds.has(account.entityId) &&
      (scope.kind !== "account" || account.id === scope.accountId)
    );
  };
  return getLatestAccountBalances(dataset, asOfDate)
    .filter((snapshot) => {
      const account = dataset.accounts.find(
        (row) => row.id === snapshot.accountId,
      );
      if (!account) {
        return false;
      }
      return inScope(
        snapshot.accountId,
        account.assetDomain,
        snapshot.balanceCurrency,
      );
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
  const holdingsValue = sumStrings(
    buildLiveHoldingRows(dataset, scope, asOfDate).map(
      (row) => row.currentValueEur,
    ),
  );
  const cryptoValue = sumStrings(
    buildCryptoBalanceRows(dataset, scope, asOfDate).map(
      (row) => row.currentValueEur,
    ),
  );
  return new Decimal(holdingsValue).plus(cryptoValue).toFixed(2);
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
        buildQualitySummary(dataset, scope, { referenceDate })
          .pendingReviewCount,
      );
      break;
    case "unclassified_amount_mtd":
      valueBaseEur = buildQualitySummary(dataset, scope, {
        referenceDate,
        period: currentPeriod,
      }).unclassifiedAmountMtdEur;
      break;
    case "stale_accounts_count":
      valueBaseEur = String(
        buildQualitySummary(dataset, scope, { referenceDate })
          .staleAccountsCount,
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
  const quality = buildQualitySummary(dataset, scope, {
    referenceDate,
    period,
  });
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
  const categoryByCode = new Map(
    dataset.categories.map((category) => [category.code, category]),
  );
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

  const monthlyWindow = resolveMonthlySeriesWindow(
    dataset,
    input.scope,
    period,
    referenceDate,
    period.preset === "24m" ? 24 : 6,
  );
  const monthlySeries = buildMonthlyFlowSeries(
    dataset,
    input.scope,
    monthlyWindow.start,
    monthlyWindow.end,
  );

  const scopedTransactions = filterTransactionsByPeriod(
    filterTransactionsByScope(dataset, input.scope),
    period,
  );
  const resolvedScopedTransactions = scopedTransactions.filter((transaction) =>
    isTransactionResolvedForAnalytics(transaction),
  );
  const spendingByCategory = aggregateSpendingCategoryRows(
    resolvedScopedTransactions,
    categoryByCode,
  );

  const holdings = buildLiveHoldingRows(
    dataset,
    input.scope,
    referenceDate,
  ).sort(
    (left, right) =>
      Number(right.currentValueEur ?? 0) - Number(left.currentValueEur ?? 0),
  );
  const cryptoBalances = buildCryptoBalanceRows(
    dataset,
    input.scope,
    referenceDate,
  );
  const portfolioAllocationRows = [
    ...holdings.map((row) => ({
      label: row.symbol,
      amountEur: row.currentValueEur ?? "0.00",
    })),
    ...cryptoBalances.map((row) => ({
      label: row.currency,
      amountEur: row.currentValueEur ?? "0.00",
    })),
  ];
  const totalPortfolio = new Decimal(
    sumStrings(portfolioAllocationRows.map((row) => row.amountEur)),
  );
  const portfolioAllocation = portfolioAllocationRows.map((row) => ({
    label: row.label,
    amountEur: row.amountEur,
    allocationPercent: totalPortfolio.eq(0)
      ? "0.00"
      : new Decimal(row.amountEur).div(totalPortfolio).mul(100).toFixed(2),
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
    quality: buildQualitySummary(dataset, input.scope, {
      referenceDate,
      period,
    }),
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

function selectTransactions(
  transactions: Transaction[],
  predicate: (transaction: Transaction) => boolean,
) {
  return sortTransactionsNewestFirst(transactions.filter(predicate));
}

function hasTransactionClass(classes: readonly string[]) {
  return (transaction: Transaction) =>
    classes.includes(transaction.transactionClass);
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

const SOURCE_LEGAL_SUFFIX_TOKENS = new Set([
  "BV",
  "CO",
  "CORP",
  "CORPORATION",
  "GMBH",
  "INC",
  "LIMITED",
  "LLC",
  "LP",
  "LTD",
  "SA",
  "SAU",
  "SL",
  "SRL",
  "S",
  "R",
  "L",
]);

function resolveIncomeSourceLabel(transaction: Transaction) {
  return (
    transaction.counterpartyName ??
    transaction.merchantNormalized ??
    transaction.descriptionClean
  );
}

function canonicalizeIncomeSourceLabel(label: string) {
  const tokens = normalizeMatcherText(label)
    .split(/[^A-Z0-9]+/)
    .map((token) => token.trim())
    .filter(Boolean)
    .filter((token) => !SOURCE_LEGAL_SUFFIX_TOKENS.has(token));

  return tokens.join("");
}

function aggregateIncomeSourceRows(transactions: Transaction[]) {
  const totals = new Map<
    string,
    {
      amount: Decimal;
      aliasTotals: Map<string, Decimal>;
    }
  >();

  for (const transaction of transactions) {
    const label = resolveIncomeSourceLabel(transaction);
    const contribution = incomeContributionEur(transaction);
    if (!label || !contribution) {
      continue;
    }

    const key =
      canonicalizeIncomeSourceLabel(label) || normalizeMatcherText(label);
    const existing = totals.get(key) ?? {
      amount: new Decimal(0),
      aliasTotals: new Map<string, Decimal>(),
    };
    existing.amount = existing.amount.plus(contribution);
    existing.aliasTotals.set(
      label,
      (existing.aliasTotals.get(label) ?? new Decimal(0)).plus(contribution),
    );
    totals.set(key, existing);
  }

  return [...totals.values()]
    .map((row) => {
      const aliases = [...row.aliasTotals.entries()]
        .sort(
          (left, right) =>
            right[1].cmp(left[1]) || left[0].localeCompare(right[0]),
        )
        .map(([alias]) => alias);
      return {
        label: aliases[0] ?? "Unknown Source",
        aliases,
        amountEur: row.amount.toFixed(2),
      };
    })
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
  if (input.scope.kind !== "consolidated") {
    return {
      summary,
      summaryBreakdown: null,
    };
  }

  const personalEntityId = dataset.entities.find(
    (entity) => entity.entityKind === "personal",
  )?.id;
  const referenceDate = input.referenceDate ?? todayIso();
  const personalMetric = personalEntityId
    ? buildMetricResult(
        dataset,
        { kind: "entity", entityId: personalEntityId },
        input.displayCurrency,
        "net_worth_current",
        { referenceDate, period: summary.period },
      )
    : undefined;
  const totalMetric = findMetric(summary, "net_worth_current")!;

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
  const context = buildAnalyticsReadModelContext(dataset, input, summary);
  const excludedCreditCardSettlementRows = selectTransactions(
    context.scopedPeriodTransactions,
    needsCreditCardStatementUpload,
  );
  const transactions = selectTransactions(
    context.scopedPeriodTransactions,
    isSpendingLike,
  );
  const resolvedTransactions = selectTransactions(
    context.resolvedScopedPeriodTransactions,
    isSpendingLike,
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

      const bucket = resolveSpendingCategoryBucket(
        context.categoryByCode,
        transaction,
      );
      return bucket.categorized ? sum : sum.plus(contribution);
    }, new Decimal(0))
    .toFixed(2);
  const coverage = spendMetric?.valueBaseEur
    ? new Decimal(1)
        .minus(
          new Decimal(uncategorizedSpendEur).div(
            Decimal.max(new Decimal(spendMetric.valueBaseEur), new Decimal(1)),
          ),
        )
        .mul(100)
        .toFixed(2)
    : "100.00";
  const excludedCreditCardSettlementAmountEur = excludedCreditCardSettlementRows
    .reduce(
      (sum, transaction) => sum.plus(amountMagnitudeEur(transaction)),
      new Decimal(0),
    )
    .toFixed(2);
  const monthlyWindow = resolveMonthlySeriesWindow(
    dataset,
    input.scope,
    summary.period,
    context.referenceDate,
    summary.period.preset === "24m" ? 24 : 6,
  );
  const spendingCategoryMonthlySeries = buildMonthlySpendingCategorySeries(
    dataset,
    input.scope,
    monthlyWindow.start,
    monthlyWindow.end,
    context.categoryByCode,
  );
  const hasCreditCardAccount = dataset.accounts.some(
    (account) => account.accountType === "credit_card" && account.isActive,
  );

  return {
    summary,
    transactions,
    spendMetric,
    trendSeries: summary.monthlySeries,
    spendingCategoryMonthlySeries,
    trailingThreeMonthAverage: averageMonthlySeries(
      summary.monthlySeries,
      "spendingEur",
      3,
    ),
    coverage,
    uncategorizedSpendEur,
    excludedCreditCardSettlementAmountEur,
    excludedCreditCardSettlementCount: excludedCreditCardSettlementRows.length,
    creditCardSettlementRows: excludedCreditCardSettlementRows,
    hasImportedCreditCardAccount: hasCreditCardAccount,
    topCategory: summary.spendingByCategory[0],
    merchantRows,
    topMerchant: merchantRows[0] ?? null,
  };
}

function isSpendingCategoryTransaction(
  categoryByCode: Map<string, DomainDataset["categories"][number]>,
  categoryCode: string,
  transaction: Transaction,
) {
  const contribution = spendingContributionEur(transaction);
  if (!contribution) {
    return false;
  }
  return (
    resolveSpendingCategoryBucket(categoryByCode, transaction).categoryCode ===
    categoryCode
  );
}

function spendingCategoryAmountForPeriod(
  dataset: DomainDataset,
  scope: Scope,
  period: PeriodSelection,
  categoryByCode: Map<string, DomainDataset["categories"][number]>,
  categoryCode: string,
) {
  return filterTransactionsByPeriod(
    filterTransactionsByScope(dataset, scope),
    period,
  )
    .filter((transaction) => isTransactionResolvedForAnalytics(transaction))
    .reduce((sum, transaction) => {
      if (
        !isSpendingCategoryTransaction(
          categoryByCode,
          categoryCode,
          transaction,
        )
      ) {
        return sum;
      }
      return sum.plus(spendingContributionEur(transaction) ?? new Decimal(0));
    }, new Decimal(0));
}

export function buildSpendingCategoryReadModel(
  dataset: DomainDataset,
  input: {
    scope: Scope;
    displayCurrency: string;
    categoryCode: string;
    period?: PeriodSelection;
    referenceDate?: string;
  },
) {
  const summary = buildDashboardSummary(dataset, input);
  const context = buildAnalyticsReadModelContext(dataset, input, summary);
  const categoryLabel = resolveSpendingCategoryLabel(
    context.categoryByCode,
    input.categoryCode,
  );
  const transactions = selectTransactions(
    context.resolvedScopedPeriodTransactions,
    (transaction) =>
      isSpendingCategoryTransaction(
        context.categoryByCode,
        input.categoryCode,
        transaction,
      ),
  );
  const categoryAmount = transactions.reduce(
    (sum, transaction) =>
      sum.plus(spendingContributionEur(transaction) ?? new Decimal(0)),
    new Decimal(0),
  );
  const spendMetric = findMetric(summary, "spending_mtd_total");
  const spendTotal = new Decimal(spendMetric?.valueBaseEur ?? 0);
  const comparisonPeriod = getPreviousComparablePeriod(summary.period);
  const comparisonAmount = spendingCategoryAmountForPeriod(
    dataset,
    input.scope,
    comparisonPeriod,
    context.categoryByCode,
    input.categoryCode,
  );
  const periodSharePercent = spendTotal.gt(0)
    ? categoryAmount.div(spendTotal).mul(100).toFixed(2)
    : null;
  const comparisonDeltaPercent = comparisonAmount.eq(0)
    ? null
    : categoryAmount
        .minus(comparisonAmount)
        .div(comparisonAmount.abs())
        .mul(100)
        .toFixed(2);
  const monthlySeries = buildMonthlySpendingCategorySeries(
    dataset,
    input.scope,
    summary.period.start,
    summary.period.end,
    context.categoryByCode,
  ).map((row) => {
    const category = row.categories.find(
      (candidate) => candidate.categoryCode === input.categoryCode,
    );
    return {
      month: row.month,
      amountEur: category?.amountEur ?? "0.00",
      totalSpendingEur: row.totalSpendingEur,
    };
  });
  const merchantRows = aggregateAmountRows(
    transactions,
    (transaction) => resolveSpendingCounterpartyLabel(transaction),
    (transaction) => spendingContributionEur(transaction) ?? new Decimal(0),
  );
  const largestTransaction =
    [...transactions].sort(
      (left, right) =>
        Math.abs(Number(right.amountBaseEur)) -
        Math.abs(Number(left.amountBaseEur)),
    )[0] ?? null;
  const averageTransactionEur =
    transactions.length > 0
      ? categoryAmount.div(transactions.length).toFixed(2)
      : "0.00";
  const category =
    summary.spendingByCategory.find(
      (row) => row.categoryCode === input.categoryCode,
    ) ??
    (categoryLabel
      ? {
          categoryCode: input.categoryCode,
          label: categoryLabel,
          amountEur: categoryAmount.toFixed(2),
        }
      : null);

  return {
    summary,
    category,
    transactions,
    spendMetric,
    amountEur: categoryAmount.toFixed(2),
    periodSharePercent,
    comparisonAmountEur: comparisonAmount.toFixed(2),
    comparisonDeltaPercent,
    monthlySeries,
    merchantRows,
    topMerchant: merchantRows[0] ?? null,
    transactionCount: transactions.length,
    averageTransactionEur,
    largestTransaction,
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
  const context = buildAnalyticsReadModelContext(dataset, input, summary);
  const isIncomeTransaction = hasTransactionClass([
    "income",
    "dividend",
    "interest",
  ]);
  const transactions = selectTransactions(
    context.scopedPeriodTransactions,
    isIncomeTransaction,
  );
  const resolvedTransactions = selectTransactions(
    context.resolvedScopedPeriodTransactions,
    isIncomeTransaction,
  );
  const incomeMetric = findMetric(summary, "income_mtd_total");
  const sourceRows = aggregateIncomeSourceRows(resolvedTransactions);
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
  const trailingThreeMonthAverage = averageMonthlySeries(
    summary.monthlySeries,
    "incomeEur",
    3,
  );
  const monthlyIncomeWindow = resolveMonthlySeriesWindow(
    dataset,
    input.scope,
    summary.period,
    context.referenceDate,
    summary.monthlySeries.length,
  );
  const ytdPeriod = resolvePeriodSelection({
    preset: "ytd",
    referenceDate: context.referenceDate,
  });
  const incomeCompletenessPercent =
    transactions.length === 0
      ? "100.00"
      : new Decimal(resolvedTransactions.length)
          .div(transactions.length)
          .mul(100)
          .toFixed(2);

  return {
    summary,
    transactions,
    incomeMetric,
    sourceRows,
    investmentIncomeRows,
    trailingThreeMonthAverage,
    monthlyIncomeComposition: buildMonthlyIncomeComposition(
      dataset,
      input.scope,
      monthlyIncomeWindow.start,
      monthlyIncomeWindow.end,
    ),
    topSourceShare,
    activeSourceCount: sourceRows.length,
    ytdIncomeTotal: flowMetric(dataset, input.scope, ytdPeriod, "income"),
    projectedYearIncome: new Decimal(trailingThreeMonthAverage)
      .mul(12)
      .toFixed(2),
    incomeCompletenessPercent,
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
  const summary = buildDashboardSummary(dataset, input);
  const context = buildAnalyticsReadModelContext(dataset, input, summary);
  const isInvestmentLedgerTransaction = hasTransactionClass(
    investmentLedgerClasses,
  );
  const isInvestmentCashFlowTransaction = hasTransactionClass([
    "dividend",
    "interest",
    "transfer_internal",
  ]);
  const holdings = buildHoldingsSnapshot(
    dataset,
    input.scope,
    context.referenceDate,
  );
  const investmentRows = selectTransactions(
    context.scopedPeriodTransactions,
    isInvestmentLedgerTransaction,
  );
  const periodCashFlowRows = selectTransactions(
    context.resolvedScopedPeriodTransactions,
    isInvestmentCashFlowTransaction,
  );
  const accountAllocation = aggregateAmountRows(
    [...holdings.holdings, ...holdings.cryptoBalances],
    (row) =>
      context.accountById.get(row.accountId)?.displayName ?? row.accountId,
    (row) => new Decimal(row.currentValueEur ?? 0),
  );
  const unresolved = sortTransactionsNewestFirst(
    context.scopedTransactions.filter((transaction) => {
      const account = context.accountById.get(transaction.accountId);
      return (
        account?.assetDomain === "investment" &&
        transaction.transactionDate <= context.referenceDate &&
        needsTransactionManualReview(transaction)
      );
    }),
  );
  const processedRows = sortTransactionsNewestFirst(
    context.scopedPeriodTransactions.filter((transaction) => {
      const account = context.accountById.get(transaction.accountId);
      return (
        account?.assetDomain === "investment" &&
        transaction.transactionDate <= context.referenceDate &&
        !needsTransactionManualReview(transaction) &&
        processedInvestmentLedgerClasses.includes(transaction.transactionClass)
      );
    }),
  );

  return {
    scope: input.scope,
    holdings,
    investmentRows,
    processedRows,
    metrics: {
      portfolioValue: findMetric(summary, "portfolio_market_value_current")!,
      unrealized: findMetric(summary, "portfolio_unrealized_pnl_current")!,
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

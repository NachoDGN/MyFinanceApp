import { Decimal } from "decimal.js";

import {
  filterTransactionsByReferenceDate,
  filterTransactionsByScope,
  isInvestmentAccountType,
  needsTransactionManualReview,
  resolveFxRate,
} from "@myfinance/domain";

import { convertBaseEurToDisplayAmount } from "./currency";
import {
  formatCurrency,
  formatDate,
  formatPercent,
  formatQuantity,
} from "./formatters";
import { getInvestmentsModel, type InvestmentsModel } from "./queries";
import {
  buildHoldingDisplayMetricsMap,
  type HoldingDisplayMetric,
  getHoldingDisplayMetricKey,
} from "./investment-display";
import {
  buildManualInvestmentMatchHaystack,
  parseManualInvestmentMatcherTerms,
} from "./manual-investment-matching";

type ProcessedRow = InvestmentsModel["processedRows"][number];
type UnresolvedRow = InvestmentsModel["unresolved"][number];
type Holding = InvestmentsModel["holdings"]["holdings"][number];
type Security = InvestmentsModel["dataset"]["securities"][number] & {
  metadataJson?: unknown;
};

type InvestmentsPageSearchParams =
  | Promise<Record<string, string | string[] | undefined>>
  | Record<string, string | string[] | undefined>;

function normalizeParam(
  params: Record<string, string | string[] | undefined>,
  key: string,
): string | undefined {
  const value = params[key];
  if (Array.isArray(value)) return value[0];
  return value;
}

function normalizeInstrumentText(value: string | null | undefined) {
  return value?.trim().toUpperCase() ?? "";
}

function describeProcessedRowsPeriodLabel(period: InvestmentsModel["period"]) {
  if (period.preset === "all") return "all-time";
  if (period.preset === "ytd") return "YTD";
  if (period.preset === "mtd") return "MTD";
  return "selected-period";
}

function describeInvestmentsPeriodLabel(period: InvestmentsModel["period"]) {
  if (period.preset === "all") return "All Time";
  if (period.preset === "ytd") return "YTD";
  if (period.preset === "mtd") return "MTD";
  return "Selected Period";
}

function describeInvestmentsComparisonLabel(
  period: InvestmentsModel["period"],
) {
  if (period.preset === "all") return "inception";
  if (period.preset === "ytd") return "year-start";
  if (period.preset === "mtd") return "month-start";
  return formatDate(period.start);
}

function readOptionalRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function readOptionalString(value: unknown) {
  return typeof value === "string" && value.trim() !== "" ? value : null;
}

function splitIsoDate(value: string) {
  const [year, month, day] = value.split("-");
  return {
    top: year ? `${year}-` : value,
    bottom: month && day ? `${month}-${day}` : "",
  };
}

function humanizeHoldingFreshness(
  freshness: "fresh" | "delayed" | "stale" | "missing",
) {
  switch (freshness) {
    case "fresh":
      return "Fresh";
    case "delayed":
      return "Needs refresh soon";
    case "stale":
      return "Stale";
    default:
      return "Missing";
  }
}

function holdingLooksLikeFund(security: Security | null | undefined) {
  if (!security) return false;
  if (security.assetType === "stock" || security.assetType === "etf") {
    return false;
  }

  const metadata = readOptionalRecord(security.metadataJson);
  const instrumentType = normalizeInstrumentText(
    readOptionalString(metadata?.instrumentType),
  );
  const combined = normalizeInstrumentText(
    [instrumentType, security.exchangeName, security.name]
      .filter(Boolean)
      .join(" "),
  );

  return (
    combined.includes("FUND") ||
    combined.includes("MUTUAL") ||
    combined.includes("OEIC") ||
    combined.includes("INDEX") ||
    combined.includes("VANGUARD")
  );
}

function getTransactionSecurityLabel(
  model: InvestmentsModel,
  row: ProcessedRow | UnresolvedRow,
) {
  const security = model.dataset.securities.find(
    (candidate) => candidate.id === row.securityId,
  );
  if (security?.displaySymbol) {
    return security.displaySymbol;
  }
  if (security?.isin) {
    return security.isin;
  }

  const llmPayload = readOptionalRecord(row.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);
  const rawOutput = readOptionalRecord(llmNode?.rawOutput);
  return (
    readOptionalString(rawOutput?.resolved_instrument_isin) ??
    readOptionalString(rawOutput?.resolved_instrument_ticker) ??
    "—"
  );
}

function matchesTransactionSecurityFilter(
  model: InvestmentsModel,
  row: ProcessedRow,
  query: string,
) {
  const normalizedQuery = query.trim().toLowerCase();
  if (!normalizedQuery) return true;

  const security = model.dataset.securities.find(
    (candidate) => candidate.id === row.securityId,
  );
  const securityLabel = getTransactionSecurityLabel(model, row);

  return [
    security?.displaySymbol,
    security?.name,
    security?.isin,
    securityLabel,
    row.descriptionRaw,
  ].some((value) => value?.toLowerCase().includes(normalizedQuery));
}

function countManualInvestmentFundingMatches(
  dataset: InvestmentsModel["dataset"],
  investment: InvestmentsModel["dataset"]["manualInvestments"][number],
  snapshotDate: string | null,
) {
  if (!snapshotDate) {
    return 0;
  }

  const matcherTerms = parseManualInvestmentMatcherTerms(
    investment.matcherText,
  );
  if (matcherTerms.length === 0) {
    return 0;
  }

  return dataset.transactions.filter((transaction) => {
    if (
      transaction.accountId !== investment.fundingAccountId ||
      transaction.economicEntityId !== investment.entityId ||
      transaction.transactionDate > snapshotDate ||
      transaction.voidedAt !== null
    ) {
      return false;
    }

    const haystack = buildManualInvestmentMatchHaystack(transaction);
    return matcherTerms.some((term) => haystack.includes(term));
  }).length;
}

function formatCurrentPrice(
  model: InvestmentsModel,
  price: string | null | undefined,
  priceCurrency: string | null | undefined,
): { primary: string; secondary: string | null } {
  if (!price || !priceCurrency) {
    return {
      primary: "N/A",
      secondary: null,
    };
  }

  const native = formatCurrency(price, priceCurrency);
  if (priceCurrency === model.currency) {
    return {
      primary: native,
      secondary: null,
    };
  }

  const converted = new Decimal(price)
    .mul(
      resolveFxRate(
        model.dataset,
        priceCurrency,
        model.currency,
        model.referenceDate,
      ),
    )
    .toFixed(2);

  return {
    primary: formatCurrency(converted, model.currency),
    secondary: native,
  };
}

function toDisplayAmount(
  model: InvestmentsModel,
  amount: string | null | undefined,
  effectiveDate = model.referenceDate,
) {
  return convertBaseEurToDisplayAmount(
    model.dataset,
    amount,
    model.currency,
    effectiveDate,
  );
}

function formatDisplayAmount(
  model: InvestmentsModel,
  amount: string | null | undefined,
  effectiveDate = model.referenceDate,
) {
  return formatCurrency(
    toDisplayAmount(model, amount, effectiveDate),
    model.currency,
  );
}

function toDisplayChartValue(
  model: InvestmentsModel,
  amount: string | null | undefined,
  effectiveDate = model.referenceDate,
) {
  return Number(
    toDisplayAmount(model, amount, effectiveDate) ??
      toDisplayAmount(model, amount) ??
      amount ??
      0,
  );
}

function safePercent(numerator: Decimal, denominator: Decimal) {
  return denominator.eq(0)
    ? null
    : numerator.div(denominator).mul(100).toFixed(2);
}

function formatCompactPercent(value: string | null | undefined) {
  if (value === null || value === undefined) return "N/A";
  return `${Number(value).toFixed(1)}%`;
}

function formatSignedCurrency(
  amount: string | null | undefined,
  currency: string,
) {
  const formatted = formatCurrency(amount, currency);
  if (amount === null || amount === undefined) return formatted;
  return Number(amount) > 0 ? `+${formatted}` : formatted;
}

function formatSignedPercent(value: string | null | undefined) {
  const formatted = formatPercent(value);
  if (value === null || value === undefined) return formatted;
  return Number(value) > 0 ? `+${formatted}` : formatted;
}

function buildHoldingBucketSummary(
  model: InvestmentsModel,
  rows: Holding[],
  label: string,
  totalPortfolioValueDisplay: Decimal,
  getHoldingDisplayMetric: (holding: Holding) => HoldingDisplayMetric,
) {
  const pricedRows = rows.filter((holding) => {
    const metric = getHoldingDisplayMetric(holding);
    return (
      metric.currentValueDisplay !== null && metric.unrealizedDisplay !== null
    );
  });
  const marketValueDisplay = pricedRows.reduce(
    (sum, holding) =>
      sum.plus(getHoldingDisplayMetric(holding).currentValueDisplay ?? 0),
    new Decimal(0),
  );
  const unrealizedPnlDisplay = pricedRows.reduce(
    (sum, holding) =>
      sum.plus(getHoldingDisplayMetric(holding).unrealizedDisplay ?? 0),
    new Decimal(0),
  );
  const costBasisDisplay = pricedRows.reduce(
    (sum, holding) =>
      sum.plus(getHoldingDisplayMetric(holding).openCostBasisDisplay ?? 0),
    new Decimal(0),
  );

  return {
    key: label,
    label,
    title:
      label === "Funds"
        ? `${rows.length} fund position${rows.length === 1 ? "" : "s"}`
        : `${rows.length} stock/ETF position${rows.length === 1 ? "" : "s"}`,
    value: formatCurrency(marketValueDisplay.toFixed(2), model.currency),
    pill: formatPercent(
      safePercent(marketValueDisplay, totalPortfolioValueDisplay),
    ),
    metaPrimary: `${formatCurrency(unrealizedPnlDisplay.toFixed(2), model.currency)} / ${formatPercent(
      safePercent(unrealizedPnlDisplay, costBasisDisplay),
    )}`,
    metaSecondary:
      rows.length - pricedRows.length > 0
        ? `${rows.length - pricedRows.length} position${rows.length - pricedRows.length === 1 ? "" : "s"} without a current quote`
        : "All positions in this bucket are currently priced.",
    returnClass: (Number(unrealizedPnlDisplay.toFixed(2)) >= 0
      ? "positive"
      : "negative") as "positive" | "negative",
    missingQuoteCount: rows.length - pricedRows.length,
  };
}

function buildTransactionRow(
  model: InvestmentsModel,
  row: ProcessedRow | UnresolvedRow,
) {
  const securityLabel = getTransactionSecurityLabel(model, row);
  const { top: dateTop, bottom: dateBottom } = splitIsoDate(
    row.transactionDate,
  );
  return {
    id: row.id,
    dateTop,
    dateBottom,
    descriptionRaw: row.descriptionRaw,
    transactionClass: row.transactionClass,
    categoryCode: row.categoryCode ?? null,
    quantity: row.quantity ?? null,
    quantityDisplay: formatQuantity(row.quantity),
    securityLabel,
    amountDisplay: formatDisplayAmount(
      model,
      row.amountBaseEur,
      row.transactionDate,
    ),
    reviewSecuritySymbol: securityLabel === "—" ? null : securityLabel,
    needsReview: row.needsReview,
    reviewReason:
      typeof row.reviewReason === "string" ? row.reviewReason : null,
    manualNotes: row.manualNotes ?? null,
    classificationSource: row.classificationSource ?? null,
    llmPayload: row.llmPayload,
  };
}

function buildPositionRow(
  model: InvestmentsModel,
  holding: Holding,
  meta: {
    symbol: string;
    exchange?: string | null;
    quantityDisplay: string;
  },
  getHoldingDisplayMetric: (holding: Holding) => HoldingDisplayMetric,
) {
  const displayMetric = getHoldingDisplayMetric(holding);
  return {
    key: holding.securityId,
    title: holding.securityName,
    symbol: meta.symbol,
    exchange: meta.exchange ?? null,
    quantityDisplay: meta.quantityDisplay,
    value: formatCurrency(displayMetric.currentValueDisplay, model.currency),
    returnAmountDisplay: displayMetric.currentValueDisplay
      ? formatCurrency(displayMetric.unrealizedDisplay, model.currency)
      : undefined,
    returnPercentDisplay: formatPercent(displayMetric.unrealizedDisplayPercent),
    returnClass:
      Number(displayMetric.unrealizedDisplay ?? "0") >= 0
        ? "positive"
        : "negative",
    fallbackNote: displayMetric.currentValueDisplay
      ? undefined
      : "Current quote unavailable",
  };
}

export function buildInvestmentsPageHref(
  model: InvestmentsModel,
  securityFilter: string,
  page: number,
) {
  const query = new URLSearchParams({
    scope: model.scopeParam,
    currency: model.currency,
    period: model.period.preset,
  });
  if (model.referenceDate) {
    query.set("asOf", model.referenceDate);
  }
  if (model.period.preset === "custom") {
    query.set("start", model.period.start);
    query.set("end", model.period.end);
  }
  if (securityFilter.trim()) {
    query.set("security", securityFilter.trim());
  }
  if (page > 1) {
    query.set("page", String(page));
  }
  return `/investments?${query.toString()}`;
}

export function buildInvestmentsPageModel(
  model: InvestmentsModel,
  params: Record<string, string | string[] | undefined>,
) {
  const pageParam = normalizeParam(params, "page");
  const securityParam = normalizeParam(params, "security");
  const securityFilter =
    typeof securityParam === "string" ? securityParam.trim() : "";
  const requestedPage = Number.parseInt(String(pageParam ?? "1"), 10);
  const currentPage =
    Number.isFinite(requestedPage) && requestedPage > 0 ? requestedPage : 1;
  const pageSize = 10;
  const filteredProcessedRows = model.processedRows.filter((row) =>
    matchesTransactionSecurityFilter(model, row, securityFilter),
  );
  const totalProcessedRows = filteredProcessedRows.length;
  const totalProcessedRowsOverall = model.processedRows.length;
  const totalPages = Math.max(1, Math.ceil(totalProcessedRows / pageSize));
  const safePage = Math.min(currentPage, totalPages);
  const processedRows = filteredProcessedRows.slice(
    (safePage - 1) * pageSize,
    safePage * pageSize,
  );

  const securityById = new Map(
    model.dataset.securities.map((security) => [security.id, security]),
  );
  const accountById = new Map(
    model.dataset.accounts.map((account) => [account.id, account]),
  );
  const investmentAccountIds = new Set(
    model.dataset.accounts
      .filter((account) => account.assetDomain === "investment")
      .map((account) => account.id),
  );
  const entityById = new Map(
    model.dataset.entities.map((entity) => [entity.id, entity]),
  );
  const allTimeResolvedTradeRows = filterTransactionsByReferenceDate(
    filterTransactionsByScope(model.dataset, model.scope),
    model.referenceDate,
  ).filter(
    (transaction) =>
      investmentAccountIds.has(transaction.accountId) &&
      !needsTransactionManualReview(transaction) &&
      ["investment_trade_buy", "investment_trade_sell"].includes(
        transaction.transactionClass,
      ),
  ).length;
  const latestManualValuationByInvestmentId = new Map<
    string,
    (typeof model.dataset.manualInvestmentValuations)[number]
  >();
  for (const valuation of [...model.dataset.manualInvestmentValuations].sort(
    (left, right) =>
      right.snapshotDate.localeCompare(left.snapshotDate) ||
      right.updatedAt.localeCompare(left.updatedAt) ||
      right.createdAt.localeCompare(left.createdAt),
  )) {
    if (
      !latestManualValuationByInvestmentId.has(valuation.manualInvestmentId)
    ) {
      latestManualValuationByInvestmentId.set(
        valuation.manualInvestmentId,
        valuation,
      );
    }
  }

  const isManualHolding = (holding: Holding) =>
    holding.holdingSource === "manual_valuation";
  const sortedHoldings = [...model.holdings.holdings].sort((left, right) => {
    const rightValue = Number(right.currentValueEur ?? -1);
    const leftValue = Number(left.currentValueEur ?? -1);
    if (rightValue !== leftValue) {
      return rightValue - leftValue;
    }
    return left.securityName.localeCompare(right.securityName);
  });
  const holdingDisplayMetrics = buildHoldingDisplayMetricsMap(
    model.dataset,
    sortedHoldings,
    model.currency,
    model.referenceDate,
  );
  const getHoldingDisplayMetric = (holding: Holding) =>
    holdingDisplayMetrics.get(getHoldingDisplayMetricKey(holding)) ?? {
      avgCostDisplay: null,
      openCostBasisDisplay: null,
      currentValueDisplay: toDisplayAmount(model, holding.currentValueEur),
      unrealizedDisplay: null,
      unrealizedDisplayPercent: null,
    };
  const manualHoldings = sortedHoldings.filter(isManualHolding);
  const fundHoldings = sortedHoldings.filter(
    (holding) =>
      !isManualHolding(holding) &&
      holdingLooksLikeFund(securityById.get(holding.securityId)),
  );
  const stockHoldings = sortedHoldings.filter(
    (holding) =>
      !isManualHolding(holding) &&
      !holdingLooksLikeFund(securityById.get(holding.securityId)),
  );
  const cryptoBalances = [...model.holdings.cryptoBalances].sort(
    (left, right) =>
      Number(right.currentValueEur ?? 0) - Number(left.currentValueEur ?? 0),
  );
  const manualInvestmentSummaries = manualHoldings
    .map((holding) => {
      const investment = model.dataset.manualInvestments.find(
        (row) => row.id === holding.securityId,
      );
      if (!investment) {
        return null;
      }

      const latestValuation = latestManualValuationByInvestmentId.get(
        investment.id,
      );
      const displayMetric = getHoldingDisplayMetric(holding);
      const fundingAccount = accountById.get(investment.fundingAccountId);
      const matchedFundingTransactionCount =
        countManualInvestmentFundingMatches(
          model.dataset,
          investment,
          latestValuation?.snapshotDate ?? null,
        );

      return {
        id: investment.id,
        entityId: investment.entityId,
        entityName:
          entityById.get(investment.entityId)?.displayName ??
          investment.entityId,
        fundingAccountId: investment.fundingAccountId,
        fundingAccountName: fundingAccount
          ? `${fundingAccount.displayName} (${fundingAccount.defaultCurrency})`
          : investment.fundingAccountId,
        label: investment.label,
        matcherText: investment.matcherText,
        note: investment.note ?? null,
        latestSnapshotDate: latestValuation?.snapshotDate ?? null,
        latestValueOriginal: latestValuation?.currentValueOriginal ?? null,
        latestValueCurrency: latestValuation?.currentValueCurrency ?? null,
        currentValueDisplay: formatCurrency(
          displayMetric.currentValueDisplay,
          model.currency,
        ),
        investedAmountDisplay: formatCurrency(
          displayMetric.openCostBasisDisplay,
          model.currency,
        ),
        unrealizedDisplay: formatCurrency(
          displayMetric.unrealizedDisplay,
          model.currency,
        ),
        unrealizedPercent: displayMetric.unrealizedDisplayPercent,
        matchedFundingTransactionCount,
        freshnessLabel: humanizeHoldingFreshness(holding.quoteFreshness),
      };
    })
    .filter((row) => row !== null);

  const pricedPortfolioValueEur = model.holdings.holdings.reduce(
    (sum, holding) => sum.plus(holding.currentValueEur ?? 0),
    new Decimal(0),
  );
  const cryptoPortfolioValueEur = cryptoBalances.reduce(
    (sum, balance) => sum.plus(balance.currentValueEur ?? 0),
    new Decimal(0),
  );
  const cashValueEur = new Decimal(model.holdings.brokerageCashEur);
  const totalPortfolioValueEur = pricedPortfolioValueEur
    .plus(cryptoPortfolioValueEur)
    .plus(cashValueEur);
  const totalPortfolioValueDisplay = new Decimal(
    toDisplayAmount(model, totalPortfolioValueEur.toFixed(2)) ?? 0,
  );
  const fundsSummary = buildHoldingBucketSummary(
    model,
    fundHoldings,
    "Funds",
    totalPortfolioValueDisplay,
    getHoldingDisplayMetric,
  );
  const stocksSummary = buildHoldingBucketSummary(
    model,
    stockHoldings,
    "Stocks & ETF",
    totalPortfolioValueDisplay,
    getHoldingDisplayMetric,
  );
  const visiblePositionHoldings = [...fundHoldings, ...stockHoldings];
  const pricedVisiblePositionHoldings = visiblePositionHoldings.filter(
    (holding) => {
      const metric = getHoldingDisplayMetric(holding);
      return (
        metric.currentValueDisplay !== null &&
        metric.unrealizedDisplay !== null &&
        metric.openCostBasisDisplay !== null
      );
    },
  );
  const totalUnrealizedDisplay = pricedVisiblePositionHoldings.reduce(
    (sum, holding) =>
      sum.plus(getHoldingDisplayMetric(holding).unrealizedDisplay ?? 0),
    new Decimal(0),
  );
  const totalOpenCostBasisDisplay = pricedVisiblePositionHoldings.reduce(
    (sum, holding) =>
      sum.plus(getHoldingDisplayMetric(holding).openCostBasisDisplay ?? 0),
    new Decimal(0),
  );
  const bestPerformingHolding = visiblePositionHoldings
    .map((holding) => ({
      holding,
      metric: getHoldingDisplayMetric(holding),
      security: securityById.get(holding.securityId),
    }))
    .filter((row) => row.metric.unrealizedDisplayPercent !== null)
    .sort(
      (left, right) =>
        Number(right.metric.unrealizedDisplayPercent) -
        Number(left.metric.unrealizedDisplayPercent),
    )[0];
  const fundsValueDisplay = fundHoldings.reduce(
    (sum, holding) =>
      sum.plus(getHoldingDisplayMetric(holding).currentValueDisplay ?? 0),
    new Decimal(0),
  );
  const stocksValueDisplay = stockHoldings.reduce(
    (sum, holding) =>
      sum.plus(getHoldingDisplayMetric(holding).currentValueDisplay ?? 0),
    new Decimal(0),
  );
  const cashValueDisplay = new Decimal(
    toDisplayAmount(model, model.holdings.brokerageCashEur) ?? 0,
  );
  const cryptoValueDisplay = new Decimal(
    toDisplayAmount(model, cryptoPortfolioValueEur.toFixed(2)) ?? 0,
  );
  const buildAllocationRow = (
    key: "funds" | "stocks" | "cash" | "crypto",
    label: string,
    value: Decimal,
  ) => {
    const percent = safePercent(value, totalPortfolioValueDisplay);
    const numericPercent = Math.max(0, Math.min(100, Number(percent ?? 0)));
    return {
      key,
      label,
      percentDisplay: formatCompactPercent(percent),
      width: `${numericPercent}%`,
      showInLegend: value.gt(0) && numericPercent >= 0.5,
    };
  };
  const periodLabel = describeInvestmentsPeriodLabel(model.period);
  const comparisonLabel = describeInvestmentsComparisonLabel(model.period);
  const processedRowsPeriodLabel = describeProcessedRowsPeriodLabel(
    model.period,
  );
  const periodInvestmentIncome = new Decimal(model.dividendsPeriod).plus(
    model.interestPeriod,
  );
  const processedRowsPage = processedRows.map((row) =>
    buildTransactionRow(model, row),
  );
  const unresolvedRows = model.unresolved.map((row) =>
    buildTransactionRow(model, row),
  );
  const holdingRows = sortedHoldings.map((holding) => {
    const displayMetric = getHoldingDisplayMetric(holding);
    const manualInvestment = isManualHolding(holding)
      ? model.dataset.manualInvestments.find(
          (row) => row.id === holding.securityId,
        )
      : null;
    const currentPrice = formatCurrentPrice(
      model,
      holding.currentPrice,
      holding.currentPriceCurrency,
    );
    return {
      key: holding.securityId,
      securityName: holding.securityName,
      symbol: manualInvestment ? "MANUAL" : holding.symbol,
      accountName:
        accountById.get(holding.accountId)?.displayName ?? holding.accountId,
      quantityDisplay: isManualHolding(holding)
        ? "—"
        : formatQuantity(holding.quantity),
      avgCostDisplay: formatCurrency(
        displayMetric.avgCostDisplay,
        model.currency,
      ),
      currentPricePrimary: manualInvestment
        ? holding.currentPrice && holding.currentPriceCurrency
          ? formatCurrency(holding.currentPrice, holding.currentPriceCurrency)
          : "N/A"
        : currentPrice.primary,
      currentPriceSecondary: manualInvestment ? null : currentPrice.secondary,
      currentPriceNote: manualInvestment
        ? holding.quoteTimestamp
          ? `Manual snapshot ${formatDate(holding.quoteTimestamp.slice(0, 10))}`
          : null
        : holding.quoteTimestamp
          ? `Last quote ${formatDate(holding.quoteTimestamp.slice(0, 10))}`
          : null,
      currentValueDisplay: formatCurrency(
        displayMetric.currentValueDisplay,
        model.currency,
      ),
      unrealizedDisplay: `${formatCurrency(displayMetric.unrealizedDisplay, model.currency)} (${formatPercent(displayMetric.unrealizedDisplayPercent)})`,
      unrealizedPercent: displayMetric.unrealizedDisplayPercent,
      freshnessLabel: manualInvestment
        ? `MANUAL · ${holding.quoteFreshness.toUpperCase()}`
        : holding.quoteFreshness.toUpperCase(),
      isManual: isManualHolding(holding),
    };
  });
  const portfolioAllocationRows = [
    ...sortedHoldings.map((holding) => ({
      label: holding.symbol,
      amountEur: toDisplayAmount(model, holding.currentValueEur) ?? "0.00",
    })),
    ...cryptoBalances.map((balance) => ({
      label: balance.currency,
      amountEur: toDisplayAmount(model, balance.currentValueEur) ?? "0.00",
    })),
  ];

  return {
    dataset: model.dataset,
    scopeParam: model.scopeParam,
    currency: model.currency,
    referenceDate: model.referenceDate,
    period: model.period,
    navigationState: model.navigationState,
    scopeOptions: model.scopeOptions,
    securityFilter,
    pageSize,
    currentPage,
    safePage,
    totalPages,
    totalProcessedRows,
    totalProcessedRowsOverall,
    processedRowsPeriodLabel,
    allTimeResolvedTradeRows,
    processedRows: processedRowsPage,
    unresolvedRows,
    portfolioOverview: {
      totalPortfolioValueDisplay: formatCurrency(
        totalPortfolioValueDisplay.toFixed(2),
        model.currency,
      ),
      totalUnrealizedDisplay: formatSignedCurrency(
        totalUnrealizedDisplay.toFixed(2),
        model.currency,
      ),
      totalUnrealizedPercentDisplay: `${formatSignedPercent(
        safePercent(totalUnrealizedDisplay, totalOpenCostBasisDisplay),
      )} All Time`,
      totalUnrealizedClass:
        Number(totalUnrealizedDisplay.toFixed(2)) >= 0
          ? ("positive" as const)
          : ("negative" as const),
      bestPerformingAsset: bestPerformingHolding
        ? {
            title: bestPerformingHolding.holding.securityName,
            subtitle: [
              bestPerformingHolding.holding.symbol,
              bestPerformingHolding.security?.exchangeName,
            ]
              .filter(Boolean)
              .join(" · "),
            returnDisplay: formatSignedPercent(
              bestPerformingHolding.metric.unrealizedDisplayPercent,
            ),
            returnClass:
              Number(
                bestPerformingHolding.metric.unrealizedDisplayPercent ?? "0",
              ) >= 0
                ? ("positive" as const)
                : ("negative" as const),
          }
        : null,
      allocationRows: [
        buildAllocationRow("funds", "Funds", fundsValueDisplay),
        buildAllocationRow("stocks", "Stocks & ETF", stocksValueDisplay),
        buildAllocationRow("cash", "Cash", cashValueDisplay),
        buildAllocationRow("crypto", "Crypto", cryptoValueDisplay),
      ],
    },
    metricCards: [
      {
        label: "Portfolio Market Value",
        value: formatCurrency(
          model.metrics.portfolioValue.valueDisplay,
          model.currency,
        ),
        badge: "All Time",
        badgeTone: "neutral" as const,
        subtitle: `Current holdings snapshot as of ${formatDate(model.referenceDate)}`,
        chartValues: [
          ...model.holdings.holdings.map((holding) =>
            toDisplayChartValue(model, holding.currentValueEur),
          ),
          ...cryptoBalances.map((balance) =>
            toDisplayChartValue(model, balance.currentValueEur),
          ),
        ],
      },
      {
        label: "Unrealized Gain",
        value: formatCurrency(
          model.metrics.unrealized.valueDisplay,
          model.currency,
        ),
        badge: `${model.metrics.unrealized.deltaPercent ?? "0.00"}%`,
        badgeTone:
          Number(model.metrics.unrealized.valueDisplay ?? "0") >= 0
            ? ("accent" as const)
            : ("neutral" as const),
        subtitle: `${formatCurrency(model.metrics.unrealized.deltaDisplay, model.currency)} vs ${comparisonLabel}`,
        chartValues: model.holdings.holdings.map((holding) =>
          Number(getHoldingDisplayMetric(holding).unrealizedDisplay ?? 0),
        ),
      },
      {
        label: `Investment Income ${periodLabel}`,
        value: formatDisplayAmount(model, periodInvestmentIncome.toFixed(2)),
        badge: "Income",
        subtitle: `${formatDisplayAmount(model, model.dividendsPeriod)} dividends + ${formatDisplayAmount(model, model.interestPeriod)} interest`,
        chartValues: model.investmentRows
          .filter((row) =>
            ["dividend", "interest"].includes(row.transactionClass),
          )
          .map((row) =>
            toDisplayChartValue(model, row.amountBaseEur, row.transactionDate),
          ),
      },
      {
        label: "Brokerage Cash",
        value: formatDisplayAmount(model, model.holdings.brokerageCashEur),
        badge: formatDisplayAmount(model, model.netContributionsPeriod),
        subtitle: "Latest broker cash balance",
        chartValues: [
          toDisplayChartValue(model, model.holdings.brokerageCashEur),
          toDisplayChartValue(model, model.dividendsPeriod),
          toDisplayChartValue(model, model.interestPeriod),
          toDisplayChartValue(model, model.netContributionsPeriod),
        ],
      },
    ],
    assetSummaries: [
      {
        key: "cash",
        label: "Cash",
        title: "Brokerage Cash",
        value: formatDisplayAmount(model, model.holdings.brokerageCashEur),
        pill: formatPercent(safePercent(cashValueEur, totalPortfolioValueEur)),
        metaPrimary: "Current broker cash balance",
        metaSecondary: "No unrealized P/L applies to cash.",
        returnClass: undefined,
      },
      {
        key: "crypto",
        label: "Crypto",
        title: `${cryptoBalances.length} crypto balance${cryptoBalances.length === 1 ? "" : "s"}`,
        value: formatDisplayAmount(model, cryptoPortfolioValueEur.toFixed(2)),
        pill: formatPercent(
          safePercent(cryptoPortfolioValueEur, totalPortfolioValueEur),
        ),
        metaPrimary: "BTC and ETH treasury balances now roll into portfolio.",
        metaSecondary:
          "Cost basis is not tracked yet, so unrealized P/L stays outside this bucket.",
        returnClass: undefined,
      },
      fundsSummary,
      stocksSummary,
    ],
    portfolioAllocationRows,
    fundRows: fundHoldings.map((holding) => {
      return buildPositionRow(
        model,
        holding,
        {
          symbol: holding.symbol,
          quantityDisplay: `${formatQuantity(holding.quantity)} units`,
        },
        getHoldingDisplayMetric,
      );
    }),
    stockRows: stockHoldings.map((holding) => {
      const security = securityById.get(holding.securityId);
      return buildPositionRow(
        model,
        holding,
        {
          symbol: holding.symbol,
          exchange: security?.exchangeName ?? "Unknown exchange",
          quantityDisplay: `${formatQuantity(holding.quantity)} units`,
        },
        getHoldingDisplayMetric,
      );
    }),
    cryptoRows: cryptoBalances.map((balance) => ({
      key: `${balance.accountId}:${balance.currency}`,
      title: balance.currency,
      subtitle: `${accountById.get(balance.accountId)?.displayName ?? balance.accountId} · ${formatQuantity(balance.balanceOriginal)} units`,
      value: formatDisplayAmount(model, balance.currentValueEur),
      fallbackNote: balance.currentPriceEur
        ? `${formatDisplayAmount(model, balance.currentPriceEur)} per ${balance.currency}`
        : "Current quote unavailable",
    })),
    accountAllocationRows: model.accountAllocation.map((row) => ({
      ...row,
      amountEur: toDisplayAmount(model, row.amountEur) ?? "0.00",
    })),
    holdingRows,
    manualInvestmentSummaries,
    manualInvestmentEntities: model.dataset.entities
      .filter((entity) => entity.active)
      .map((entity) => ({ id: entity.id, label: entity.displayName })),
    manualInvestmentCashAccounts: model.dataset.accounts
      .filter(
        (account) =>
          account.isActive &&
          (account.assetDomain === "cash" ||
            isInvestmentAccountType(account.accountType)),
      )
      .map((account) => ({
        id: account.id,
        entityId: account.entityId,
        label: `${account.displayName} (${account.defaultCurrency})`,
      })),
    periodLabel,
    comparisonLabel,
    processedLedgerColumns:
      "100px 200px 180px 60px 100px 110px minmax(320px, 1fr)",
    unresolvedLedgerColumns: "100px 240px 70px 160px 110px minmax(320px, 1fr)",
    buildHref: (page: number, nextSecurityFilter = securityFilter) =>
      buildInvestmentsPageHref(model, nextSecurityFilter, page),
  };
}

export type InvestmentsPageModel = ReturnType<typeof buildInvestmentsPageModel>;

export async function resolveInvestmentsPageModel(
  searchParams: InvestmentsPageSearchParams,
) {
  const params = await searchParams;
  const model = await getInvestmentsModel(params);
  return buildInvestmentsPageModel(model, params);
}

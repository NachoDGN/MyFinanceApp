import { Decimal } from "decimal.js";

import { resolveFxRate } from "@myfinance/domain";

import { convertBaseEurToDisplayAmount } from "./currency";
import { formatCurrency, formatDate, formatPercent, formatQuantity } from "./formatters";
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

export type InvestmentsPageMetricCard = {
  label: string;
  value: string;
  badge: string;
  badgeTone?: "accent" | "neutral";
  subtitle: string;
  chartValues: number[];
};

export type InvestmentsPageAssetSummary = {
  key: string;
  label: string;
  title: string;
  value: string;
  pill: string;
  metaPrimary: string;
  metaSecondary: string;
  returnClass?: "positive" | "negative";
  missingQuoteCount?: number;
};

export type InvestmentsPageHoldingRow = {
  key: string;
  securityName: string;
  symbol: string;
  accountName: string;
  quantityDisplay: string;
  avgCostDisplay: string;
  currentPricePrimary: string;
  currentPriceSecondary: string | null;
  currentPriceNote: string | null;
  currentValueDisplay: string;
  unrealizedDisplay: string;
  unrealizedPercent: string | null;
  freshnessLabel: string;
  isManual: boolean;
};

export type InvestmentsPagePositionRow = {
  key: string;
  title: string;
  subtitle: string;
  value: string;
  returnDisplay?: string;
  returnClass?: "positive" | "negative";
  fallbackNote?: string;
};

export type InvestmentsPageTransactionRow = {
  id: string;
  dateTop: string;
  dateBottom: string;
  descriptionRaw: string;
  transactionClass: string;
  quantity: string | null;
  quantityDisplay: string;
  securityLabel: string;
  amountDisplay: string;
  reviewSecuritySymbol: string | null;
  needsReview: boolean;
  reviewReason: string | null;
  manualNotes: string | null;
  classificationSource: string | null;
  llmPayload: unknown;
};

export type InvestmentsPageManualInvestmentSummary = {
  id: string;
  entityId: string;
  entityName: string;
  fundingAccountId: string;
  fundingAccountName: string;
  label: string;
  matcherText: string;
  note: string | null;
  latestSnapshotDate: string | null;
  latestValueOriginal: string | null;
  latestValueCurrency: string | null;
  currentValueDisplay: string;
  investedAmountDisplay: string;
  unrealizedDisplay: string;
  unrealizedPercent: string | null;
  matchedFundingTransactionCount: number;
  freshnessLabel: string;
};

type InvestmentsPageSearchParams =
  | Promise<Record<string, string | string[] | undefined>>
  | Record<string, string | string[] | undefined>;

export type InvestmentsPageModel = {
  dataset: InvestmentsModel["dataset"];
  scopeParam: string;
  currency: string;
  referenceDate: string;
  period: InvestmentsModel["period"];
  navigationState: InvestmentsModel["navigationState"];
  securityFilter: string;
  pageSize: number;
  currentPage: number;
  safePage: number;
  totalPages: number;
  totalProcessedRows: number;
  totalProcessedRowsOverall: number;
  processedRows: InvestmentsPageTransactionRow[];
  unresolvedRows: InvestmentsPageTransactionRow[];
  metricCards: InvestmentsPageMetricCard[];
  assetSummaries: InvestmentsPageAssetSummary[];
  portfolioAllocationRows: Array<{ label: string; amountEur: string }>;
  fundRows: InvestmentsPagePositionRow[];
  stockRows: InvestmentsPagePositionRow[];
  cryptoRows: InvestmentsPagePositionRow[];
  holdingRows: InvestmentsPageHoldingRow[];
  accountAllocationRows: Array<{ label: string; amountEur: string }>;
  manualInvestmentSummaries: InvestmentsPageManualInvestmentSummary[];
  manualInvestmentEntities: Array<{ id: string; label: string }>;
  manualInvestmentCashAccounts: Array<{
    id: string;
    entityId: string;
    label: string;
  }>;
  periodLabel: string;
  comparisonLabel: string;
  processedLedgerColumns: string;
  unresolvedLedgerColumns: string;
  buildHref: (page: number, nextSecurityFilter?: string) => string;
  scopeOptions: InvestmentsModel["scopeOptions"];
};

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

  const matcherTerms = parseManualInvestmentMatcherTerms(investment.matcherText);
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

function safePercent(numerator: Decimal, denominator: Decimal) {
  return denominator.eq(0) ? null : numerator.div(denominator).mul(100).toFixed(2);
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
    return metric.currentValueDisplay !== null && metric.unrealizedDisplay !== null;
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
    pill: safePercent(marketValueDisplay, totalPortfolioValueDisplay)
      ? formatPercent(safePercent(marketValueDisplay, totalPortfolioValueDisplay))
      : "N/A",
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
): InvestmentsPageModel {
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
  const entityById = new Map(
    model.dataset.entities.map((entity) => [entity.id, entity]),
  );
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
    if (!latestManualValuationByInvestmentId.has(valuation.manualInvestmentId)) {
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
  const fundHoldings = sortedHoldings.filter(
    (holding) =>
      isManualHolding(holding) ||
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
  const manualInvestmentSummaries = fundHoldings
    .filter(isManualHolding)
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
      const matchedFundingTransactionCount = countManualInvestmentFundingMatches(
        model.dataset,
        investment,
        latestValuation?.snapshotDate ?? null,
      );

      return {
        id: investment.id,
        entityId: investment.entityId,
        entityName:
          entityById.get(investment.entityId)?.displayName ?? investment.entityId,
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
      } satisfies InvestmentsPageManualInvestmentSummary;
    })
    .filter((row): row is InvestmentsPageManualInvestmentSummary => row !== null);

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
  const periodLabel =
    model.period.preset === "ytd"
      ? "YTD"
      : model.period.preset === "mtd"
        ? "MTD"
        : "Selected Period";
  const comparisonLabel =
    model.period.preset === "ytd"
      ? "year-start"
      : model.period.preset === "mtd"
        ? "month-start"
        : formatDate(model.period.start);
  const periodInvestmentIncome = new Decimal(model.dividendsPeriod).plus(
    model.interestPeriod,
  );
  const processedLedgerColumns =
    "100px 200px 180px 60px 100px 110px minmax(320px, 1fr)";
  const unresolvedLedgerColumns =
    "100px 240px 70px 160px 110px minmax(320px, 1fr)";

  const processedRowsPage = processedRows.map((row) => {
    const securityLabel = getTransactionSecurityLabel(model, row);
    const { top: dateTop, bottom: dateBottom } = splitIsoDate(row.transactionDate);
    return {
      id: row.id,
      dateTop,
      dateBottom,
      descriptionRaw: row.descriptionRaw,
      transactionClass: row.transactionClass,
      quantity: row.quantity ?? null,
      quantityDisplay: formatQuantity(row.quantity),
      securityLabel,
      amountDisplay: formatCurrency(
        toDisplayAmount(model, row.amountBaseEur, row.transactionDate),
        model.currency,
      ),
      reviewSecuritySymbol: securityLabel === "—" ? null : securityLabel,
      needsReview: row.needsReview,
      reviewReason:
        typeof row.reviewReason === "string" ? row.reviewReason : null,
      manualNotes: row.manualNotes ?? null,
      classificationSource: row.classificationSource ?? null,
      llmPayload: row.llmPayload,
    };
  });
  const unresolvedRows = model.unresolved.map((row) => {
    const securityLabel = getTransactionSecurityLabel(model, row);
    const { top: dateTop, bottom: dateBottom } = splitIsoDate(row.transactionDate);
    return {
      id: row.id,
      dateTop,
      dateBottom,
      descriptionRaw: row.descriptionRaw,
      transactionClass: row.transactionClass,
      quantity: row.quantity ?? null,
      quantityDisplay: formatQuantity(row.quantity),
      securityLabel,
      amountDisplay: formatCurrency(
        toDisplayAmount(model, row.amountBaseEur, row.transactionDate),
        model.currency,
      ),
      reviewSecuritySymbol: securityLabel === "—" ? null : securityLabel,
      needsReview: row.needsReview,
      reviewReason:
        typeof row.reviewReason === "string" ? row.reviewReason : null,
      manualNotes: row.manualNotes ?? null,
      classificationSource: row.classificationSource ?? null,
      llmPayload: row.llmPayload,
    };
  });
  const holdingRows = sortedHoldings.map((holding) => {
    const displayMetric = getHoldingDisplayMetric(holding);
    const manualInvestment = isManualHolding(holding)
      ? model.dataset.manualInvestments.find((row) => row.id === holding.securityId)
      : null;
    const currentPrice = formatCurrentPrice(
      model,
      holding.currentPrice,
      holding.currentPriceCurrency,
    );
    const manualSnapshotDate = manualInvestment
      ? latestManualValuationByInvestmentId.get(manualInvestment.id)?.snapshotDate
      : null;
    return {
      key: holding.securityId,
      securityName: holding.securityName,
      symbol: manualInvestment ? "MANUAL" : holding.symbol,
      accountName:
        accountById.get(holding.accountId)?.displayName ?? holding.accountId,
      quantityDisplay: isManualHolding(holding)
        ? "—"
        : formatQuantity(holding.quantity),
      avgCostDisplay: formatCurrency(displayMetric.avgCostDisplay, model.currency),
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
    processedRows: processedRowsPage,
    unresolvedRows,
    metricCards: [
      {
        label: "Portfolio Market Value",
        value: formatCurrency(model.metrics.portfolioValue.valueDisplay, model.currency),
        badge: `${model.metrics.portfolioValue.deltaPercent ?? "0.00"}%`,
        badgeTone:
          Number(model.metrics.portfolioValue.deltaDisplay ?? "0") >= 0
            ? "accent"
            : "neutral",
        subtitle: `${formatCurrency(model.metrics.portfolioValue.deltaDisplay, model.currency)} vs ${comparisonLabel}`,
        chartValues: [
          ...model.holdings.holdings.map((holding) =>
            Number(holding.currentValueEur ?? 0),
          ),
          ...cryptoBalances.map((balance) =>
            Number(balance.currentValueEur ?? 0),
          ),
        ],
      },
      {
        label: "Unrealized Gain",
        value: formatCurrency(model.metrics.unrealized.valueDisplay, model.currency),
        badge: `${model.metrics.unrealized.deltaPercent ?? "0.00"}%`,
        badgeTone:
          Number(model.metrics.unrealized.valueDisplay ?? "0") >= 0
            ? "accent"
            : "neutral",
        subtitle: `${formatCurrency(model.metrics.unrealized.deltaDisplay, model.currency)} vs ${comparisonLabel}`,
        chartValues: model.holdings.holdings.map((holding) =>
          Number(getHoldingDisplayMetric(holding).unrealizedDisplay ?? 0),
        ),
      },
      {
        label: `Investment Income ${periodLabel}`,
        value: formatCurrency(
          toDisplayAmount(model, periodInvestmentIncome.toFixed(2)),
          model.currency,
        ),
        badge: "Income",
        subtitle: `${formatCurrency(
          toDisplayAmount(model, model.dividendsPeriod),
          model.currency,
        )} dividends + ${formatCurrency(
          toDisplayAmount(model, model.interestPeriod),
          model.currency,
        )} interest`,
        chartValues: model.investmentRows
          .filter((row) => ["dividend", "interest"].includes(row.transactionClass))
          .map((row) => Number(row.amountBaseEur)),
      },
      {
        label: "Brokerage Cash",
        value: formatCurrency(
          toDisplayAmount(model, model.holdings.brokerageCashEur),
          model.currency,
        ),
        badge: formatCurrency(
          toDisplayAmount(model, model.netContributionsPeriod),
          model.currency,
        ),
        subtitle: "Latest broker cash balance",
        chartValues: [
          Number(model.holdings.brokerageCashEur),
          Number(model.dividendsPeriod),
          Number(model.interestPeriod),
          Number(model.netContributionsPeriod),
        ],
      },
    ],
    assetSummaries: [
      {
        key: "cash",
        label: "Cash",
        title: "Brokerage Cash",
        value: formatCurrency(
          toDisplayAmount(model, model.holdings.brokerageCashEur),
          model.currency,
        ),
        pill:
          safePercent(cashValueEur, totalPortfolioValueEur) !== null
            ? formatPercent(safePercent(cashValueEur, totalPortfolioValueEur))
            : "N/A",
        metaPrimary: "Current broker cash balance",
        metaSecondary: "No unrealized P/L applies to cash.",
      },
      {
        key: "crypto",
        label: "Crypto",
        title: `${cryptoBalances.length} crypto balance${cryptoBalances.length === 1 ? "" : "s"}`,
        value: formatCurrency(
          toDisplayAmount(model, cryptoPortfolioValueEur.toFixed(2)),
          model.currency,
        ),
        pill:
          safePercent(cryptoPortfolioValueEur, totalPortfolioValueEur) !== null
            ? formatPercent(
                safePercent(cryptoPortfolioValueEur, totalPortfolioValueEur),
              )
            : "N/A",
        metaPrimary: "BTC and ETH treasury balances now roll into portfolio.",
        metaSecondary:
          "Cost basis is not tracked yet, so unrealized P/L stays outside this bucket.",
      },
      fundsSummary,
      stocksSummary,
    ],
    portfolioAllocationRows,
    fundRows: fundHoldings.map((holding) => {
      const displayMetric = getHoldingDisplayMetric(holding);
      const manualInvestment = isManualHolding(holding)
        ? model.dataset.manualInvestments.find((row) => row.id === holding.securityId)
        : null;
      const manualSnapshotDate = manualInvestment
        ? latestManualValuationByInvestmentId.get(manualInvestment.id)?.snapshotDate
        : null;
      return {
        key: holding.securityId,
        title: holding.securityName,
        subtitle: manualInvestment
          ? `${accountById.get(holding.accountId)?.displayName ?? holding.accountId} · ${manualSnapshotDate ? `snapshot ${formatDate(manualSnapshotDate)}` : "manual valuation"}`
          : `${holding.symbol} · ${formatQuantity(holding.quantity)} units`,
        value: formatCurrency(displayMetric.currentValueDisplay, model.currency),
        returnDisplay: displayMetric.currentValueDisplay
          ? `${formatCurrency(displayMetric.unrealizedDisplay, model.currency)} / ${formatPercent(displayMetric.unrealizedDisplayPercent)}`
          : undefined,
        returnClass:
          Number(displayMetric.unrealizedDisplay ?? "0") >= 0
            ? "positive"
            : "negative",
        fallbackNote: displayMetric.currentValueDisplay
          ? undefined
          : "Current quote unavailable",
      };
    }),
    stockRows: stockHoldings.map((holding) => {
      const displayMetric = getHoldingDisplayMetric(holding);
      const security = securityById.get(holding.securityId);
      return {
        key: holding.securityId,
        title: holding.securityName,
        subtitle: `${holding.symbol} · ${security?.exchangeName ?? "Unknown exchange"} · ${formatQuantity(holding.quantity)} units`,
        value: formatCurrency(displayMetric.currentValueDisplay, model.currency),
        returnDisplay: displayMetric.currentValueDisplay
          ? `${formatCurrency(displayMetric.unrealizedDisplay, model.currency)} / ${formatPercent(displayMetric.unrealizedDisplayPercent)}`
          : undefined,
        returnClass:
          Number(displayMetric.unrealizedDisplay ?? "0") >= 0
            ? "positive"
            : "negative",
        fallbackNote: displayMetric.currentValueDisplay
          ? undefined
          : "Current quote unavailable",
      };
    }),
    cryptoRows: cryptoBalances.map((balance) => ({
      key: `${balance.accountId}:${balance.currency}`,
      title: balance.currency,
      subtitle: `${accountById.get(balance.accountId)?.displayName ?? balance.accountId} · ${formatQuantity(balance.balanceOriginal)} units`,
      value: formatCurrency(
        toDisplayAmount(model, balance.currentValueEur),
        model.currency,
      ),
      fallbackNote: balance.currentPriceEur
        ? `${formatCurrency(balance.currentPriceEur, "EUR")} per ${balance.currency}`
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
      .filter((account) => account.isActive && account.assetDomain === "cash")
      .map((account) => ({
        id: account.id,
        entityId: account.entityId,
        label: `${account.displayName} (${account.defaultCurrency})`,
      })),
    periodLabel,
    comparisonLabel,
    processedLedgerColumns,
    unresolvedLedgerColumns,
    buildHref: (page: number, nextSecurityFilter = securityFilter) =>
      buildInvestmentsPageHref(model, nextSecurityFilter, page),
  };
}

export async function resolveInvestmentsPageModel(
  searchParams: InvestmentsPageSearchParams,
) {
  const params = await searchParams;
  const model = await getInvestmentsModel(params);
  return buildInvestmentsPageModel(model, params);
}

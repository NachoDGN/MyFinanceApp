import { Decimal } from "decimal.js";

import { resolveFxRate } from "@myfinance/domain";

import { AppShell } from "../../components/app-shell";
import { InvestmentPriceRefreshButton } from "../../components/investment-price-refresh-button";
import { ManualInvestmentWorkbench } from "../../components/manual-investment-workbench";
import {
  DistributionList,
  InvestmentAllocationCard,
  InvestmentMetricCard,
  SectionCard,
  SimpleTable,
} from "../../components/primitives";
import { ReviewEditorCell } from "../../components/review-editor-cell";
import {
  formatCurrency,
  formatDate,
  formatQuantity,
  formatPercent,
  getInvestmentsModel,
} from "../../lib/queries";
import { convertBaseEurToDisplayAmount } from "../../lib/currency";
import {
  buildHoldingDisplayMetricsMap,
  getHoldingDisplayMetricKey,
} from "../../lib/investment-display";
import {
  buildManualInvestmentMatchHaystack,
  parseManualInvestmentMatcherTerms,
} from "../../lib/manual-investment-matching";

function normalizeInstrumentText(value: string | null | undefined) {
  return value?.trim().toUpperCase() ?? "";
}

function splitIsoDate(value: string) {
  const [year, month, day] = value.split("-");
  return {
    top: year ? `${year}-` : value,
    bottom: month && day ? `${month}-${day}` : "",
  };
}

function readOptionalRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function readOptionalString(value: unknown) {
  return typeof value === "string" && value.trim() !== "" ? value : null;
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

function countManualInvestmentFundingMatches(
  dataset: Awaited<ReturnType<typeof getInvestmentsModel>>["dataset"],
  investment: Awaited<ReturnType<typeof getInvestmentsModel>>["dataset"]["manualInvestments"][number],
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

function holdingLooksLikeFund(
  security:
    | (Awaited<
        ReturnType<typeof getInvestmentsModel>
      >["dataset"]["securities"][number] & {
        metadataJson?: unknown;
      })
    | null
    | undefined,
) {
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
  model: Awaited<ReturnType<typeof getInvestmentsModel>>,
  row: (typeof model.processedRows)[number] | (typeof model.unresolved)[number],
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
  model: Awaited<ReturnType<typeof getInvestmentsModel>>,
  row: (typeof model.processedRows)[number],
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

export default async function InvestmentsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const model = await getInvestmentsModel(params);
  const pageParam = Array.isArray(params.page) ? params.page[0] : params.page;
  const securityParam = Array.isArray(params.security)
    ? params.security[0]
    : params.security;
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
  const toDisplayAmount = (amount: string | null | undefined) => {
    return convertBaseEurToDisplayAmount(
      model.dataset,
      amount,
      model.currency,
      model.referenceDate,
    );
  };

  const safePercent = (numerator: Decimal, denominator: Decimal) =>
    denominator.eq(0) ? null : numerator.div(denominator).mul(100).toFixed(2);

  const formatDisplayAmount = (
    amount: string | null | undefined,
    effectiveDate = model.referenceDate,
  ) =>
    formatCurrency(
      convertBaseEurToDisplayAmount(
        model.dataset,
        amount,
        model.currency,
        effectiveDate,
      ),
      model.currency,
    );

  const formatCurrentPrice = (
    price: string | null | undefined,
    priceCurrency: string | null | undefined,
  ): { primary: string; secondary: string | null } => {
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
  };

  const buildInvestmentsPageHref = (
    page: number,
    nextSecurityFilter = securityFilter,
  ) => {
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
    if (nextSecurityFilter.trim()) {
      query.set("security", nextSecurityFilter.trim());
    }
    if (page > 1) {
      query.set("page", String(page));
    }
    return `/investments?${query.toString()}`;
  };

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
    if (
      !latestManualValuationByInvestmentId.has(valuation.manualInvestmentId)
    ) {
      latestManualValuationByInvestmentId.set(
        valuation.manualInvestmentId,
        valuation,
      );
    }
  }
  const isManualHolding = (holding: (typeof model.holdings.holdings)[number]) =>
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
  const getHoldingDisplayMetric = (holding: (typeof sortedHoldings)[number]) =>
    holdingDisplayMetrics.get(getHoldingDisplayMetricKey(holding)) ?? {
      avgCostDisplay: null,
      openCostBasisDisplay: null,
      currentValueDisplay: toDisplayAmount(holding.currentValueEur),
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
          entityById.get(investment.entityId)?.displayName ??
          investment.entityId,
        fundingAccountId: investment.fundingAccountId,
        fundingAccountName:
          fundingAccount
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
    .filter((row): row is NonNullable<typeof row> => row !== null);
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
    toDisplayAmount(totalPortfolioValueEur.toFixed(2)) ?? 0,
  );
  const totalDisplayUnrealized = sortedHoldings
    .reduce(
      (sum, holding) =>
        sum.plus(getHoldingDisplayMetric(holding).unrealizedDisplay ?? 0),
      new Decimal(0),
    )
    .toFixed(2);
  const buildHoldingBucketSummary = (
    rows: typeof model.holdings.holdings,
    label: string,
  ) => {
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
      label,
      count: rows.length,
      marketValueDisplay: marketValueDisplay.toFixed(2),
      unrealizedPnlDisplay: unrealizedPnlDisplay.toFixed(2),
      unrealizedPnlPercent: safePercent(unrealizedPnlDisplay, costBasisDisplay),
      allocationPercent: safePercent(
        marketValueDisplay,
        totalPortfolioValueDisplay,
      ),
      missingQuoteCount: rows.length - pricedRows.length,
    };
  };
  const fundsSummary = buildHoldingBucketSummary(fundHoldings, "Funds");
  const stocksSummary = buildHoldingBucketSummary(
    stockHoldings,
    "Stocks & ETF",
  );
  const cryptoAllocationPercent = safePercent(
    cryptoPortfolioValueEur,
    totalPortfolioValueEur,
  );
  const cashAllocationPercent = safePercent(
    cashValueEur,
    totalPortfolioValueEur,
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

  return (
    <AppShell
      pathname="/investments"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid">
        <div className="page-header investments-page-header">
          <div>
            <h1 className="page-title">Investments</h1>
            <p className="page-subtitle">
              Holdings are rebuilt live from resolved investment rows, explicit
              opening adjustments, manual fund valuations, and crypto treasury
              balances held in BTC and ETH business accounts. Global totals stay
              consolidated by default and can then be filtered by entity using
              the buttons above.
            </p>
          </div>
          <InvestmentPriceRefreshButton />
        </div>

        <div className="investments-hero">
          <div className="metrics-row metrics-row-investments">
            <InvestmentMetricCard
              label="Portfolio Market Value"
              value={formatCurrency(
                model.metrics.portfolioValue.valueDisplay,
                model.currency,
              )}
              badge={`${model.metrics.portfolioValue.deltaPercent ?? "0.00"}%`}
              badgeTone={
                Number(model.metrics.portfolioValue.deltaDisplay ?? "0") >= 0
                  ? "accent"
                  : "neutral"
              }
              subtitle={`${formatCurrency(model.metrics.portfolioValue.deltaDisplay, model.currency)} vs ${comparisonLabel}`}
              chartValues={[
                ...model.holdings.holdings.map((holding) =>
                  Number(holding.currentValueEur ?? 0),
                ),
                ...cryptoBalances.map((balance) =>
                  Number(balance.currentValueEur ?? 0),
                ),
              ]}
            />
            <InvestmentMetricCard
              label="Unrealized Gain"
              value={formatCurrency(totalDisplayUnrealized, model.currency)}
              badge={`${model.metrics.unrealized.deltaPercent ?? "0.00"}%`}
              badgeTone={
                Number(totalDisplayUnrealized) >= 0 ? "accent" : "neutral"
              }
              subtitle={`${formatCurrency(model.metrics.unrealized.deltaDisplay, model.currency)} vs ${comparisonLabel}`}
              chartValues={model.holdings.holdings.map((holding) =>
                Number(getHoldingDisplayMetric(holding).unrealizedDisplay ?? 0),
              )}
            />
            <InvestmentMetricCard
              label={`Investment Income ${periodLabel}`}
              value={formatDisplayAmount(periodInvestmentIncome.toFixed(2))}
              badge="Income"
              subtitle={`${formatDisplayAmount(model.dividendsPeriod)} dividends + ${formatDisplayAmount(model.interestPeriod)} interest`}
              chartValues={model.investmentRows
                .filter((row) =>
                  ["dividend", "interest"].includes(row.transactionClass),
                )
                .map((row) => Number(row.amountBaseEur))}
            />
            <InvestmentMetricCard
              label="Brokerage Cash"
              value={formatDisplayAmount(model.holdings.brokerageCashEur)}
              badge={formatDisplayAmount(model.netContributionsPeriod)}
              subtitle="Latest broker cash balance"
              chartValues={[
                Number(model.holdings.brokerageCashEur),
                Number(model.dividendsPeriod),
                Number(model.interestPeriod),
                Number(model.netContributionsPeriod),
              ]}
            />
          </div>
          <InvestmentAllocationCard
            rows={[
              ...model.holdings.holdings.map((holding) => ({
                label: holding.symbol,
                amountEur: toDisplayAmount(holding.currentValueEur) ?? "0.00",
              })),
              ...cryptoBalances.map((balance) => ({
                label: balance.currency,
                amountEur: toDisplayAmount(balance.currentValueEur) ?? "0.00",
              })),
            ]}
            currency={model.currency}
          />
        </div>

        <SectionCard
          title="Snapshot by Asset Class"
          subtitle="Live rebuilt market values and open-position returns"
          span="span-12"
        >
          <div className="investment-breakdown-grid">
            <article className="investment-summary-card">
              <div className="investment-summary-head">
                <div>
                  <span className="label-sm">Cash</span>
                  <h3 className="investment-summary-title">Brokerage Cash</h3>
                </div>
                <span className="pill">
                  {cashAllocationPercent
                    ? formatPercent(cashAllocationPercent)
                    : "N/A"}
                </span>
              </div>
              <div className="investment-summary-value">
                {formatDisplayAmount(model.holdings.brokerageCashEur)}
              </div>
              <div className="investment-summary-meta">
                <span>Current broker cash balance</span>
                <span className="muted">
                  No unrealized P/L applies to cash.
                </span>
              </div>
            </article>

            <article className="investment-summary-card">
              <div className="investment-summary-head">
                <div>
                  <span className="label-sm">Crypto</span>
                  <h3 className="investment-summary-title">
                    {cryptoBalances.length} crypto balance
                    {cryptoBalances.length === 1 ? "" : "s"}
                  </h3>
                </div>
                <span className="pill">
                  {cryptoAllocationPercent
                    ? formatPercent(cryptoAllocationPercent)
                    : "N/A"}
                </span>
              </div>
              <div className="investment-summary-value">
                {formatDisplayAmount(cryptoPortfolioValueEur.toFixed(2))}
              </div>
              <div className="investment-summary-meta">
                <span>BTC and ETH treasury balances now roll into portfolio.</span>
                <span className="muted">
                  Cost basis is not tracked yet, so unrealized P/L stays
                  outside this bucket.
                </span>
              </div>
            </article>

            {[fundsSummary, stocksSummary].map((bucket) => {
              const positiveReturn =
                Number(bucket.unrealizedPnlDisplay ?? "0") >= 0
                  ? "positive"
                  : "negative";
              const bucketCountLabel =
                bucket.label === "Funds"
                  ? `${bucket.count} fund position${bucket.count === 1 ? "" : "s"}`
                  : `${bucket.count} stock/ETF position${bucket.count === 1 ? "" : "s"}`;

              return (
                <article className="investment-summary-card" key={bucket.label}>
                  <div className="investment-summary-head">
                    <div>
                      <span className="label-sm">{bucket.label}</span>
                      <h3 className="investment-summary-title">
                        {bucketCountLabel}
                      </h3>
                    </div>
                    <span className="pill">
                      {bucket.allocationPercent
                        ? formatPercent(bucket.allocationPercent)
                        : "N/A"}
                    </span>
                  </div>
                  <div className="investment-summary-value">
                    {formatCurrency(bucket.marketValueDisplay, model.currency)}
                  </div>
                  <div className="investment-summary-meta">
                    <span className={`investment-return ${positiveReturn}`}>
                      {formatCurrency(
                        bucket.unrealizedPnlDisplay,
                        model.currency,
                      )}{" "}
                      / {formatPercent(bucket.unrealizedPnlPercent)}
                    </span>
                    {bucket.missingQuoteCount > 0 ? (
                      <span className="muted">
                        {bucket.missingQuoteCount} position
                        {bucket.missingQuoteCount === 1 ? "" : "s"} without a
                        current quote
                      </span>
                    ) : (
                      <span className="muted">
                        All positions in this bucket are currently priced.
                      </span>
                    )}
                  </div>
                </article>
              );
            })}
          </div>
        </SectionCard>

        <ManualInvestmentWorkbench
          entities={model.dataset.entities
            .filter((entity) => entity.active)
            .map((entity) => ({
              id: entity.id,
              label: entity.displayName,
            }))}
          cashAccounts={model.dataset.accounts
            .filter(
              (account) => account.isActive && account.assetDomain === "cash",
            )
            .map((account) => ({
              id: account.id,
              entityId: account.entityId,
              label: `${account.displayName} (${account.defaultCurrency})`,
            }))}
          manualInvestments={manualInvestmentSummaries}
          referenceDate={model.referenceDate}
        />
        {manualInvestmentSummaries.length === 0 ? (
          <div className="status-note" style={{ marginTop: -8 }}>
            No manual fund valuations are configured right now. The fund values
            you do see on this page are coming from your broker-imported fund
            holdings, not from separate manual company fund inputs.
          </div>
        ) : null}

        <SectionCard
          title="Funds"
          subtitle="Current value, unrealized EUR, and return %"
          span="span-4"
        >
          <div className="investment-position-list">
            {fundHoldings.map((holding) => {
              const displayMetric = getHoldingDisplayMetric(holding);
              const positiveReturn =
                Number(displayMetric.unrealizedDisplay ?? "0") >= 0
                  ? "positive"
                  : "negative";
              const manualInvestment = isManualHolding(holding)
                ? model.dataset.manualInvestments.find(
                    (row) => row.id === holding.securityId,
                  )
                : null;
              const manualSnapshotDate = manualInvestment
                ? latestManualValuationByInvestmentId.get(manualInvestment.id)
                    ?.snapshotDate
                : null;

              return (
                <article
                  className="investment-position-card"
                  key={holding.securityId}
                >
                  <div className="investment-position-head">
                    <div className="investment-position-copy">
                      <h3 className="investment-position-name">
                        {holding.securityName}
                      </h3>
                      <p className="investment-position-symbol">
                        {manualInvestment
                          ? `${accountById.get(holding.accountId)?.displayName ?? holding.accountId} · ${manualSnapshotDate ? `snapshot ${formatDate(manualSnapshotDate)}` : "manual valuation"}`
                          : `${holding.symbol} · ${formatQuantity(holding.quantity)} units`}
                      </p>
                    </div>
                    <div className="investment-position-values">
                      <strong>
                        {formatCurrency(
                          displayMetric.currentValueDisplay,
                          model.currency,
                        )}
                      </strong>
                      {displayMetric.currentValueDisplay ? (
                        <span className={`investment-return ${positiveReturn}`}>
                          {formatCurrency(
                            displayMetric.unrealizedDisplay,
                            model.currency,
                          )}{" "}
                          /{" "}
                          {formatPercent(
                            displayMetric.unrealizedDisplayPercent,
                          )}
                        </span>
                      ) : (
                        <span className="muted">Current quote unavailable</span>
                      )}
                    </div>
                  </div>
                </article>
              );
            })}
          </div>
        </SectionCard>

        <SectionCard
          title="Stocks & ETF"
          subtitle="Current value, unrealized EUR, and return %"
          span="span-4"
        >
          <div className="investment-position-list">
            {stockHoldings.map((holding) => {
              const displayMetric = getHoldingDisplayMetric(holding);
              const positiveReturn =
                Number(displayMetric.unrealizedDisplay ?? "0") >= 0
                  ? "positive"
                  : "negative";
              const security = securityById.get(holding.securityId);
              const exchangeLabel =
                security?.exchangeName ?? "Unknown exchange";

              return (
                <article
                  className="investment-position-card"
                  key={holding.securityId}
                >
                  <div className="investment-position-head">
                    <div className="investment-position-copy">
                      <h3 className="investment-position-name">
                        {holding.securityName}
                      </h3>
                      <p className="investment-position-symbol">
                        {holding.symbol} · {exchangeLabel} ·{" "}
                        {formatQuantity(holding.quantity)} units
                      </p>
                    </div>
                    <div className="investment-position-values">
                      <strong>
                        {formatCurrency(
                          displayMetric.currentValueDisplay,
                          model.currency,
                        )}
                      </strong>
                      {displayMetric.currentValueDisplay ? (
                        <span className={`investment-return ${positiveReturn}`}>
                          {formatCurrency(
                            displayMetric.unrealizedDisplay,
                            model.currency,
                          )}{" "}
                          /{" "}
                          {formatPercent(
                            displayMetric.unrealizedDisplayPercent,
                          )}
                        </span>
                      ) : (
                        <span className="muted">Current quote unavailable</span>
                      )}
                    </div>
                  </div>
                </article>
              );
            })}
          </div>
        </SectionCard>

        <SectionCard
          title="Crypto Treasury"
          subtitle="Current EUR value of BTC and ETH balances"
          span="span-4"
        >
          <div className="investment-position-list">
            {cryptoBalances.length === 0 ? (
              <div className="table-empty-state">
                No crypto treasury balances are available for this scope.
              </div>
            ) : (
              cryptoBalances.map((balance) => (
                <article
                  className="investment-position-card"
                  key={`${balance.accountId}:${balance.currency}`}
                >
                  <div className="investment-position-head">
                    <div className="investment-position-copy">
                      <h3 className="investment-position-name">
                        {balance.currency}
                      </h3>
                      <p className="investment-position-symbol">
                        {accountById.get(balance.accountId)?.displayName ??
                          balance.accountId}{" "}
                        · {formatQuantity(balance.balanceOriginal)} units
                      </p>
                    </div>
                    <div className="investment-position-values">
                      <strong>
                        {formatDisplayAmount(balance.currentValueEur)}
                      </strong>
                      <span className="muted">
                        {balance.currentPriceEur
                          ? `${formatCurrency(balance.currentPriceEur, "EUR")} per ${balance.currency}`
                          : "Current quote unavailable"}
                      </span>
                    </div>
                  </div>
                </article>
              ))
            )}
          </div>
        </SectionCard>

        <SectionCard
          title="Allocation by Account"
          subtitle="Broker, treasury, and crypto split"
          span="span-12"
        >
          <DistributionList
            rows={model.accountAllocation.map((row) => ({
              ...row,
              amountEur: toDisplayAmount(row.amountEur) ?? "0.00",
            }))}
            currency={model.currency}
          />
        </SectionCard>

        <SimpleTable
          span="span-12"
          headers={[
            "Security",
            "Ticker",
            "Account",
            "Qty",
            "Avg Cost",
            "Current Price",
            "Current Value",
            "Unrealized",
            "Freshness",
          ]}
          rows={model.holdings.holdings.map((holding) => {
            const currentPrice = formatCurrentPrice(
              holding.currentPrice,
              holding.currentPriceCurrency,
            );
            const displayMetric = getHoldingDisplayMetric(holding);
            const manualHolding = isManualHolding(holding);

            return [
              holding.securityName,
              manualHolding ? "MANUAL" : holding.symbol,
              accountById.get(holding.accountId)?.displayName ??
                holding.accountId,
              manualHolding ? "—" : formatQuantity(holding.quantity),
              formatCurrency(displayMetric.avgCostDisplay, model.currency),
              manualHolding ? (
                <div style={{ display: "grid", gap: 4 }}>
                  <span>
                    {holding.currentPrice && holding.currentPriceCurrency
                      ? formatCurrency(
                          holding.currentPrice,
                          holding.currentPriceCurrency,
                        )
                      : "N/A"}
                  </span>
                  {holding.quoteTimestamp ? (
                    <span className="muted" style={{ fontSize: 12 }}>
                      Manual snapshot{" "}
                      {formatDate(holding.quoteTimestamp.slice(0, 10))}
                    </span>
                  ) : null}
                </div>
              ) : (
                <div style={{ display: "grid", gap: 4 }}>
                  <span>{currentPrice.primary}</span>
                  {currentPrice.secondary ? (
                    <span className="muted" style={{ fontSize: 12 }}>
                      {currentPrice.secondary} native
                    </span>
                  ) : null}
                  {holding.quoteTimestamp ? (
                    <span className="muted" style={{ fontSize: 12 }}>
                      Last quote{" "}
                      {formatDate(holding.quoteTimestamp.slice(0, 10))}
                    </span>
                  ) : null}
                </div>
              ),
              formatCurrency(displayMetric.currentValueDisplay, model.currency),
              `${formatCurrency(displayMetric.unrealizedDisplay, model.currency)} (${formatPercent(displayMetric.unrealizedDisplayPercent)})`,
              manualHolding
                ? `MANUAL · ${holding.quoteFreshness.toUpperCase()}`
                : holding.quoteFreshness.toUpperCase(),
            ];
          })}
        />

        <section className="section-card span-12 investment-review-section">
          <div className="investment-review-header">
            <div>
              <span className="investment-review-kicker">
                {securityFilter
                  ? `${totalProcessedRows} of ${totalProcessedRowsOverall} resolved rows`
                  : `${totalProcessedRows} resolved rows`}
              </span>
              <h2 className="investment-review-title">
                Processed Investment Transactions
              </h2>
              {securityFilter ? (
                <p className="muted" style={{ marginTop: 6, fontSize: 13 }}>
                  Security filter: {securityFilter}
                </p>
              ) : null}
            </div>
            <div
              style={{
                display: "flex",
                flexDirection: "column",
                alignItems: "flex-end",
                gap: 12,
              }}
            >
              <form
                action="/investments"
                method="get"
                className="inline-actions"
                style={{ justifyContent: "flex-end" }}
              >
                <input type="hidden" name="scope" value={model.scopeParam} />
                <input type="hidden" name="currency" value={model.currency} />
                <input
                  type="hidden"
                  name="period"
                  value={model.period.preset}
                />
                {model.referenceDate ? (
                  <input
                    type="hidden"
                    name="asOf"
                    value={model.referenceDate}
                  />
                ) : null}
                {model.period.preset === "custom" ? (
                  <>
                    <input
                      type="hidden"
                      name="start"
                      value={model.period.start}
                    />
                    <input type="hidden" name="end" value={model.period.end} />
                  </>
                ) : null}
                <label className="input-label" style={{ minWidth: 240 }}>
                  <span>Filter by security</span>
                  <input
                    className="input-field"
                    type="search"
                    name="security"
                    placeholder="Ticker, name, or ISIN"
                    defaultValue={securityFilter}
                  />
                </label>
                <button className="btn-ghost" type="submit">
                  Filter
                </button>
                {securityFilter ? (
                  <a
                    className="btn-ghost"
                    href={buildInvestmentsPageHref(1, "")}
                  >
                    Clear
                  </a>
                ) : null}
              </form>
              {totalPages > 1 ? (
                <div className="investment-review-pagination">
                  <span className="investment-review-page-pill">
                    Page {safePage} of {totalPages}
                  </span>
                  {safePage > 1 ? (
                    <a
                      className="btn-ghost"
                      href={buildInvestmentsPageHref(safePage - 1)}
                    >
                      Previous
                    </a>
                  ) : null}
                  {safePage < totalPages ? (
                    <a
                      className="btn-ghost"
                      href={buildInvestmentsPageHref(safePage + 1)}
                    >
                      Next
                    </a>
                  ) : null}
                </div>
              ) : null}
            </div>
          </div>
          {processedRows.length === 0 ? (
            <div className="table-empty-state">
              {securityFilter
                ? `No processed investment transactions match "${securityFilter}".`
                : "No processed investment transactions are available for this scope."}
            </div>
          ) : (
            <div className="investment-review-scroll">
              <div className="investment-review-table">
                <div
                  className="investment-review-grid-head"
                  style={{ gridTemplateColumns: processedLedgerColumns }}
                >
                  {[
                    "Date",
                    "Description",
                    "Class",
                    "Qty",
                    "Security",
                    "Amount",
                    "Review",
                  ].map((header, index) => (
                    <div
                      className={
                        index === 3
                          ? "investment-review-head-cell centered"
                          : index === 5
                            ? "investment-review-head-cell amount"
                            : "investment-review-head-cell"
                      }
                      key={header}
                    >
                      {header}
                    </div>
                  ))}
                </div>
                {processedRows.map((row) => {
                  const dateParts = splitIsoDate(row.transactionDate);
                  const securityLabel = getTransactionSecurityLabel(model, row);

                  return (
                    <div
                      className="investment-review-grid-row"
                      key={row.id}
                      style={{ gridTemplateColumns: processedLedgerColumns }}
                    >
                      <div className="investment-review-date">
                        <span>{dateParts.top}</span>
                        {dateParts.bottom ? (
                          <span>{dateParts.bottom}</span>
                        ) : null}
                      </div>
                      <div className="investment-review-description">
                        {row.descriptionRaw}
                      </div>
                      <div className="investment-review-copy">
                        {row.transactionClass}
                      </div>
                      <div className="investment-review-copy centered">
                        {formatQuantity(row.quantity)}
                      </div>
                      <div className="investment-review-copy breakable">
                        {securityLabel}
                      </div>
                      <div className="investment-review-copy amount">
                        {formatDisplayAmount(
                          row.amountBaseEur,
                          row.transactionDate,
                        )}
                      </div>
                      <div className="investment-review-panel">
                        <ReviewEditorCell
                          transactionId={row.id}
                          needsReview={row.needsReview}
                          reviewReason={row.reviewReason}
                          manualNotes={row.manualNotes}
                          transactionClass={row.transactionClass}
                          classificationSource={row.classificationSource}
                          securitySymbol={
                            securityLabel === "—" ? null : securityLabel
                          }
                          quantity={row.quantity}
                          llmPayload={row.llmPayload}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </section>

        <section className="section-card span-12 investment-review-section">
          <div className="investment-review-header">
            <div>
              <span className="investment-review-kicker">Review queue</span>
              <h2 className="investment-review-title">
                Unresolved Investment Events
              </h2>
            </div>
          </div>
          {model.unresolved.length === 0 ? (
            <div className="table-empty-state">
              No unresolved investment transactions are waiting for review.
            </div>
          ) : (
            <div className="investment-review-scroll">
              <div className="investment-review-table">
                <div
                  className="investment-review-grid-head"
                  style={{ gridTemplateColumns: unresolvedLedgerColumns }}
                >
                  {[
                    "Date",
                    "Description",
                    "Qty",
                    "Security",
                    "Amount",
                    "Review",
                  ].map((header, index) => (
                    <div
                      className={
                        index === 4
                          ? "investment-review-head-cell amount"
                          : index === 2
                            ? "investment-review-head-cell centered"
                            : "investment-review-head-cell"
                      }
                      key={header}
                    >
                      {header}
                    </div>
                  ))}
                </div>
                {model.unresolved.map((row) => {
                  const dateParts = splitIsoDate(row.transactionDate);
                  const securityLabel = getTransactionSecurityLabel(model, row);

                  return (
                    <div
                      className="investment-review-grid-row"
                      key={row.id}
                      style={{ gridTemplateColumns: unresolvedLedgerColumns }}
                    >
                      <div className="investment-review-date">
                        <span>{dateParts.top}</span>
                        {dateParts.bottom ? (
                          <span>{dateParts.bottom}</span>
                        ) : null}
                      </div>
                      <div className="investment-review-description">
                        {row.descriptionRaw}
                      </div>
                      <div className="investment-review-copy centered">
                        {formatQuantity(row.quantity)}
                      </div>
                      <div className="investment-review-copy breakable">
                        {securityLabel}
                      </div>
                      <div className="investment-review-copy amount">
                        {formatDisplayAmount(
                          row.amountBaseEur,
                          row.transactionDate,
                        )}
                      </div>
                      <div className="investment-review-panel">
                        <ReviewEditorCell
                          transactionId={row.id}
                          needsReview={row.needsReview}
                          reviewReason={row.reviewReason}
                          manualNotes={row.manualNotes}
                          transactionClass={row.transactionClass}
                          classificationSource={row.classificationSource}
                          securitySymbol={
                            securityLabel === "—" ? null : securityLabel
                          }
                          quantity={row.quantity}
                          llmPayload={row.llmPayload}
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </section>
      </div>
    </AppShell>
  );
}

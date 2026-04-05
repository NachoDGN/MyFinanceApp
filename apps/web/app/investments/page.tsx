import { Decimal } from "decimal.js";

import { resolveFxRate } from "@myfinance/domain";

import { AppShell } from "../../components/app-shell";
import {
  DistributionList,
  InvestmentAllocationCard,
  InvestmentMetricCard,
  SectionCard,
  SimpleTable,
} from "../../components/primitives";
import { ReviewEditorCell } from "../../components/review-editor-cell";
import {
  buildHref,
  formatCurrency,
  formatDate,
  formatQuantity,
  formatPercent,
  getInvestmentsModel,
} from "../../lib/queries";

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

export default async function InvestmentsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const model = await getInvestmentsModel(params);
  const pageParam = Array.isArray(params.page) ? params.page[0] : params.page;
  const requestedPage = Number.parseInt(String(pageParam ?? "1"), 10);
  const currentPage =
    Number.isFinite(requestedPage) && requestedPage > 0 ? requestedPage : 1;
  const pageSize = 10;
  const totalProcessedRows = model.processedRows.length;
  const totalPages = Math.max(1, Math.ceil(totalProcessedRows / pageSize));
  const safePage = Math.min(currentPage, totalPages);
  const processedRows = model.processedRows.slice(
    (safePage - 1) * pageSize,
    safePage * pageSize,
  );
  const eurToDisplayRate =
    model.currency === "EUR"
      ? new Decimal(1)
      : resolveFxRate(
          model.dataset,
          "EUR",
          model.currency,
          model.referenceDate,
        );

  const toDisplayAmount = (amount: string | null | undefined) => {
    if (amount === null || amount === undefined) return null;
    return new Decimal(amount).mul(eurToDisplayRate).toFixed(2);
  };

  const formatDisplayAmount = (amount: string | null | undefined) =>
    formatCurrency(toDisplayAmount(amount), model.currency);

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

  const buildInvestmentsPageHref = (page: number) =>
    `${buildHref(
      "/investments",
      {
        scopeParam: model.scopeParam,
        currency: model.currency,
        period: model.period.preset,
        referenceDate: model.referenceDate,
      },
      {},
    )}&page=${page}`;
  const processedLedgerColumns =
    "100px 200px 180px 60px 100px 110px minmax(320px, 1fr)";
  const unresolvedLedgerColumns =
    "100px 240px 70px 160px 110px minmax(320px, 1fr)";

  return (
    <AppShell
      pathname="/investments"
      scopeOptions={model.scopeOptions}
      state={{
        scopeParam: model.scopeParam,
        currency: model.currency,
        period: model.period.preset,
      }}
    >
      <div className="dashboard-grid">
        <div className="page-header">
          <div>
            <h1 className="page-title">Investments</h1>
            <p className="page-subtitle">
              Holdings are rebuilt from parsed investment rows plus explicit
              opening adjustments. Cash remains cash; only priced securities
              contribute to market value.
            </p>
          </div>
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
              subtitle={`${formatCurrency(model.metrics.portfolioValue.deltaDisplay, model.currency)} vs month-end`}
              chartValues={model.holdings.holdings.map((holding) =>
                Number(holding.currentValueEur ?? 0),
              )}
            />
            <InvestmentMetricCard
              label="Unrealized Gain"
              value={formatCurrency(
                model.metrics.unrealized.valueDisplay,
                model.currency,
              )}
              badge={`${model.metrics.unrealized.deltaPercent ?? "0.00"}%`}
              badgeTone={
                Number(model.metrics.unrealized.valueDisplay ?? "0") >= 0
                  ? "accent"
                  : "neutral"
              }
              subtitle="Current open-position P/L"
              chartValues={model.holdings.holdings.map((holding) =>
                Number(holding.unrealizedPnlEur ?? 0),
              )}
            />
            <InvestmentMetricCard
              label="Dividends YTD"
              value={formatDisplayAmount(model.dividendsYtd)}
              badge="Income"
              subtitle="Investment income year to date"
              chartValues={model.investmentRows
                .filter((row) => row.transactionClass === "dividend")
                .map((row) => Number(row.amountBaseEur))}
            />
            <InvestmentMetricCard
              label="Brokerage Cash"
              value={formatDisplayAmount(model.holdings.brokerageCashEur)}
              badge={formatDisplayAmount(model.netContributionsYtd)}
              subtitle="Latest broker cash balance"
              chartValues={[
                Number(model.holdings.brokerageCashEur),
                Number(model.dividendsYtd),
                Number(model.interestYtd),
                Number(model.netContributionsYtd),
              ]}
            />
          </div>
          <InvestmentAllocationCard
            rows={model.holdings.holdings.map((holding) => ({
              label: holding.symbol,
              amountEur: toDisplayAmount(holding.currentValueEur) ?? "0.00",
            }))}
            currency={model.currency}
          />
        </div>

        <SectionCard
          title="Allocation by Account"
          subtitle="Broker split"
          span="span-6"
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

            return [
              holding.securityName,
              holding.symbol,
              model.dataset.accounts.find(
                (account) => account.id === holding.accountId,
              )?.displayName ?? holding.accountId,
              formatQuantity(holding.quantity),
              formatDisplayAmount(holding.avgCostEur),
              <div style={{ display: "grid", gap: 4 }}>
                <span>{currentPrice.primary}</span>
                {currentPrice.secondary ? (
                  <span className="muted" style={{ fontSize: 12 }}>
                    {currentPrice.secondary} native
                  </span>
                ) : null}
                {holding.quoteTimestamp ? (
                  <span className="muted" style={{ fontSize: 12 }}>
                    Last quote {formatDate(holding.quoteTimestamp.slice(0, 10))}
                  </span>
                ) : null}
              </div>,
              formatDisplayAmount(holding.currentValueEur),
              `${formatDisplayAmount(holding.unrealizedPnlEur)} (${formatPercent(holding.unrealizedPnlPercent)})`,
              holding.quoteFreshness.toUpperCase(),
            ];
          })}
        />

        <section className="section-card span-12 investment-review-section">
          <div className="investment-review-header">
            <div>
              <span className="investment-review-kicker">
                {totalProcessedRows} resolved rows
              </span>
              <h2 className="investment-review-title">
                Processed Investment Transactions
              </h2>
            </div>
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
          {processedRows.length === 0 ? (
            <div className="table-empty-state">
              No processed investment transactions are available for this scope.
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
                        {dateParts.bottom ? <span>{dateParts.bottom}</span> : null}
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
                        {formatDisplayAmount(row.amountBaseEur)}
                      </div>
                      <div className="investment-review-panel">
                        <ReviewEditorCell
                          transactionId={row.id}
                          needsReview={row.needsReview}
                          reviewReason={row.reviewReason}
                          manualNotes={row.manualNotes}
                          transactionClass={row.transactionClass}
                          classificationSource={row.classificationSource}
                          securitySymbol={securityLabel === "—" ? null : securityLabel}
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
                  {["Date", "Description", "Qty", "Security", "Amount", "Review"].map(
                    (header, index) => (
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
                    ),
                  )}
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
                        {dateParts.bottom ? <span>{dateParts.bottom}</span> : null}
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
                        {formatDisplayAmount(row.amountBaseEur)}
                      </div>
                      <div className="investment-review-panel">
                        <ReviewEditorCell
                          transactionId={row.id}
                          needsReview={row.needsReview}
                          reviewReason={row.reviewReason}
                          manualNotes={row.manualNotes}
                          transactionClass={row.transactionClass}
                          classificationSource={row.classificationSource}
                          securitySymbol={securityLabel === "—" ? null : securityLabel}
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

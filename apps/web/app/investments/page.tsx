import { Decimal } from "decimal.js";

import { resolveFxRate } from "@myfinance/domain";

import { AppShell } from "../../components/app-shell";
import {
  DistributionList,
  InvestmentAllocationCard,
  InvestmentMetricCard,
  ReviewQueueList,
  ReviewStateCell,
  SectionCard,
  SimpleTable,
} from "../../components/primitives";
import {
  buildHref,
  formatCurrency,
  formatDate,
  formatQuantity,
  formatPercent,
  getInvestmentsModel,
} from "../../lib/queries";

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

        <SectionCard
          title="Processed Investment Transactions"
          subtitle={`${totalProcessedRows} resolved rows`}
          span="span-8"
          actions={
            totalPages > 1 ? (
              <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                {safePage > 1 ? (
                  <a
                    className="btn-ghost"
                    href={buildInvestmentsPageHref(safePage - 1)}
                  >
                    Previous
                  </a>
                ) : null}
                <span className="pill">
                  Page {safePage} of {totalPages}
                </span>
                {safePage < totalPages ? (
                  <a
                    className="btn-ghost"
                    href={buildInvestmentsPageHref(safePage + 1)}
                  >
                    Next
                  </a>
                ) : null}
              </div>
            ) : null
          }
        >
          {processedRows.length === 0 ? (
            <div className="table-empty-state">
              No processed investment transactions are available for this scope.
            </div>
          ) : (
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    {[
                      "Date",
                      "Description",
                      "Class",
                      "Qty",
                      "Security",
                      "Amount",
                      "Review",
                    ].map((header) => (
                      <th key={header}>{header}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {processedRows.map((row) => (
                    <tr key={row.id}>
                      <td>{row.transactionDate}</td>
                      <td>{row.descriptionRaw}</td>
                      <td>{row.transactionClass}</td>
                      <td>{formatQuantity(row.quantity)}</td>
                      <td>
                        {model.dataset.securities.find(
                          (security) => security.id === row.securityId,
                        )?.displaySymbol ?? "—"}
                      </td>
                      <td>{formatDisplayAmount(row.amountBaseEur)}</td>
                      <td>
                        <ReviewStateCell
                          needsReview={row.needsReview}
                          reviewReason={row.reviewReason}
                          transactionClass={row.transactionClass}
                          classificationSource={row.classificationSource}
                          securitySymbol={
                            model.dataset.securities.find(
                              (security) => security.id === row.securityId,
                            )?.displaySymbol ?? null
                          }
                          quantity={row.quantity}
                          llmPayload={row.llmPayload}
                        />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </SectionCard>

        <SectionCard
          title="Unresolved Investment Events"
          subtitle="Review queue"
          span="span-4"
        >
          <ReviewQueueList
            rows={model.unresolved.map((row) => ({
              label: row.descriptionRaw,
              amountEur: toDisplayAmount(row.amountBaseEur) ?? "0.00",
              reviewReason: row.reviewReason,
              securitySymbol:
                model.dataset.securities.find(
                  (security) => security.id === row.securityId,
                )?.displaySymbol ?? null,
              transactionClass: row.transactionClass,
            }))}
            currency={model.currency}
          />
        </SectionCard>
      </div>
    </AppShell>
  );
}

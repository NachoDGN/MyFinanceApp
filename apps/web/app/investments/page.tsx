import { AppShell } from "../../components/app-shell";
import {
  DistributionList,
  MetricCard,
  PortfolioAllocationCard,
  ReviewQueueList,
  ReviewStateCell,
  SectionCard,
  SimpleTable,
} from "../../components/primitives";
import {
  formatCurrency,
  formatPercent,
  getInvestmentsModel,
} from "../../lib/queries";

export default async function InvestmentsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getInvestmentsModel(searchParams);

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
          <div className="investments-hero-left">
            <div className="metrics-row metrics-row-compact">
              <MetricCard
                label="Portfolio Market Value"
                value={formatCurrency(
                  model.metrics.portfolioValue.valueDisplay,
                  model.currency,
                )}
                delta={`${model.metrics.portfolioValue.deltaPercent ?? "0.00"}%`}
                subtitle={`${formatCurrency(model.metrics.portfolioValue.deltaDisplay, model.currency)} vs month-end`}
                direction={
                  Number(model.metrics.portfolioValue.deltaDisplay ?? "0") >= 0
                    ? "up"
                    : "down"
                }
                chartValues={model.holdings.holdings.map((holding) =>
                  Number(holding.currentValueEur ?? 0),
                )}
                density="compact"
              />
              <MetricCard
                label="Unrealized Gain"
                value={formatCurrency(
                  model.metrics.unrealized.valueDisplay,
                  model.currency,
                )}
                delta={`${model.metrics.unrealized.deltaPercent ?? "0.00"}%`}
                subtitle="Current open-position P/L"
                direction={
                  Number(model.metrics.unrealized.valueDisplay ?? "0") >= 0
                    ? "up"
                    : "down"
                }
                chartValues={model.holdings.holdings.map((holding) =>
                  Number(holding.unrealizedPnlEur ?? 0),
                )}
                density="compact"
              />
              <MetricCard
                label="Dividends YTD"
                value={formatCurrency(model.dividendsYtd, model.currency)}
                delta="Income"
                subtitle="Investment income year to date"
                direction="up"
                chartValues={model.investmentRows
                  .filter((row) => row.transactionClass === "dividend")
                  .map((row) => Number(row.amountBaseEur))}
                density="compact"
              />
              <MetricCard
                label="Brokerage Cash"
                value={formatCurrency(
                  model.holdings.brokerageCashEur,
                  model.currency,
                )}
                delta={formatCurrency(
                  model.netContributionsYtd,
                  model.currency,
                )}
                subtitle="Latest broker cash balance"
                direction="up"
                chartValues={[
                  Number(model.holdings.brokerageCashEur),
                  Number(model.dividendsYtd),
                  Number(model.interestYtd),
                  Number(model.netContributionsYtd),
                ]}
                density="compact"
              />
            </div>
          </div>
          <PortfolioAllocationCard
            title="Portfolio Allocation"
            subtitle="By market value"
            rows={model.holdings.holdings.map((holding) => ({
              label: holding.symbol,
              amountEur: holding.currentValueEur ?? "0.00",
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
            rows={model.accountAllocation}
            currency={model.currency}
          />
        </SectionCard>

        <SimpleTable
          span="span-12"
          headers={[
            "Security",
            "Account",
            "Qty",
            "Avg Cost",
            "Current Price",
            "Current Value",
            "Unrealized",
            "Freshness",
          ]}
          rows={model.holdings.holdings.map((holding) => [
            holding.securityName,
            model.dataset.accounts.find(
              (account) => account.id === holding.accountId,
            )?.displayName ?? holding.accountId,
            holding.quantity,
            formatCurrency(holding.avgCostEur, "EUR"),
            holding.currentPrice
              ? `${holding.currentPrice} ${holding.currentPriceCurrency}`
              : "N/A",
            formatCurrency(holding.currentValueEur, model.currency),
            `${formatCurrency(holding.unrealizedPnlEur, model.currency)} (${formatPercent(holding.unrealizedPnlPercent)})`,
            holding.quoteFreshness.toUpperCase(),
          ])}
        />

        <SimpleTable
          span="span-8"
          headers={[
            "Date",
            "Description",
            "Class",
            "Qty",
            "Security",
            "Amount",
            "Review",
          ]}
          rows={model.investmentRows.map((row) => [
            row.transactionDate,
            row.descriptionRaw,
            row.transactionClass,
            row.quantity ?? "—",
            model.dataset.securities.find(
              (security) => security.id === row.securityId,
            )?.displaySymbol ?? "—",
            formatCurrency(row.amountBaseEur, model.currency),
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
            />,
          ])}
        />

        <SectionCard
          title="Unresolved Investment Events"
          subtitle="Review queue"
          span="span-4"
        >
          <ReviewQueueList
            rows={model.unresolved.map((row) => ({
              label: row.descriptionRaw,
              amountEur: row.amountBaseEur,
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

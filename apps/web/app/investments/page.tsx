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
  const eurToDisplayRate =
    model.currency === "EUR"
      ? new Decimal(1)
      : resolveFxRate(model.dataset, "EUR", model.currency, model.referenceDate);

  const toDisplayAmount = (amount: string | null | undefined) => {
    if (amount === null || amount === undefined) return null;
    return new Decimal(amount).mul(eurToDisplayRate).toFixed(2);
  };

  const formatDisplayAmount = (amount: string | null | undefined) =>
    formatCurrency(toDisplayAmount(amount), model.currency);

  const formatDisplayPrice = (
    price: string | null | undefined,
    priceCurrency: string | null | undefined,
  ) => {
    if (!price || !priceCurrency) return "N/A";
    const fxRate = resolveFxRate(
      model.dataset,
      priceCurrency,
      model.currency,
      model.referenceDate,
    );
    return formatCurrency(
      new Decimal(price).mul(fxRate).toFixed(2),
      model.currency,
    );
  };

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
          rows={model.holdings.holdings.map((holding) => [
            holding.securityName,
            holding.symbol,
            model.dataset.accounts.find(
              (account) => account.id === holding.accountId,
            )?.displayName ?? holding.accountId,
            holding.quantity,
            formatDisplayAmount(holding.avgCostEur),
            formatDisplayPrice(
              holding.currentPrice,
              holding.currentPriceCurrency,
            ),
            formatDisplayAmount(holding.currentValueEur),
            `${formatDisplayAmount(holding.unrealizedPnlEur)} (${formatPercent(holding.unrealizedPnlPercent)})`,
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
            formatDisplayAmount(row.amountBaseEur),
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

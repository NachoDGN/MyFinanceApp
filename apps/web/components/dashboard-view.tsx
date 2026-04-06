import type { DashboardModel } from "../lib/queries";
import { AppShell } from "./app-shell";
import {
  CategoryListCard,
  DistributionList,
  HighlightCard,
  InsightCards,
  MetricCard,
  MultiSeriesChart,
  QualityBanner,
  SectionCard,
  SimpleTable,
  TimelinePanel,
} from "./primitives";
import { formatCurrency, formatPercent, formatQuantity } from "../lib/queries";
import { convertBaseEurToDisplayAmount } from "../lib/currency";

export function DashboardView({
  pathname,
  scopeOptions,
  state,
  model,
}: {
  pathname: string;
  scopeOptions: Array<{ value: string; label: string }>;
  state: { scopeParam: string; currency: string; period: string };
  model: DashboardModel;
}) {
  const metricMap = new Map(
    model.summary.metrics.map((metric) => [metric.metricId, metric]),
  );
  const cashMetric = metricMap.get("cash_total_current");
  const incomeMetric = metricMap.get("income_mtd_total");
  const spendingMetric = metricMap.get("spending_mtd_total");
  const netMetric = metricMap.get("operating_net_cash_flow_mtd");
  const portfolioMetric = metricMap.get("portfolio_market_value_current");
  const unrealizedMetric = metricMap.get("portfolio_unrealized_pnl_current");
  const netWorthMetric = metricMap.get("net_worth_current");

  const chartValues = model.summary.monthlySeries.slice(-5);
  const chartFrom = (key: "incomeEur" | "spendingEur" | "operatingNetEur") =>
    chartValues.map((row) => Math.abs(Number(row[key])));

  const qualityRows = [
    {
      label: "Pending review",
      value: String(model.summary.quality.pendingReviewCount),
      meta:
        model.summary.quality.pendingReviewCount > 0
          ? "Rows flagged for manual follow-up, such as unmapped securities or ambiguous classifications"
          : "No unresolved rows in current scope",
    },
    {
      label: "Uncategorized amount",
      value: formatCurrency(
        model.summary.quality.unclassifiedAmountMtdEur,
        model.currency,
      ),
      meta: "Current month uncategorized exposure",
    },
    {
      label: "Stale accounts",
      value: String(model.summary.quality.staleAccountsCount),
      meta: model.summary.quality.staleAccounts[0]
        ? `${model.summary.quality.staleAccounts[0].accountName} needs a refresh`
        : "Imports are current",
    },
    {
      label: "Quote freshness",
      value: model.summary.quality.priceFreshness.toUpperCase(),
      meta: "Quotes are surfaced with delayed/fresh state",
    },
  ];

  return (
    <AppShell pathname={pathname} scopeOptions={scopeOptions} state={state}>
      <div className="dashboard-grid">
        <div className="summary-card">
          <div className="summary-content">
            <span className="label-sm dark-label">Total Net Wealth</span>
            <div className="total-wealth">
              {formatCurrency(netWorthMetric?.valueDisplay, model.currency)}
            </div>
          </div>
          <div className="wealth-breakdown">
            <div className="account-type">
              <span className="label-sm dark-label">Personal Accounts</span>
              <div className="account-value">
                {formatCurrency(
                  model.summaryBreakdown.personal?.valueDisplay,
                  model.currency,
                )}
              </div>
            </div>
            <div className="account-type">
              <span className="label-sm dark-label">Company Assets</span>
              <div className="account-value">
                {formatCurrency(
                  model.summaryBreakdown.companies.valueDisplay,
                  model.currency,
                )}
              </div>
            </div>
            <a
              className="btn-pill"
              href={`/transactions?currency=${model.currency}`}
            >
              Review Ledger
            </a>
          </div>
        </div>

        <MetricCard
          label="Cash Position"
          value={formatCurrency(cashMetric?.valueDisplay, model.currency)}
          delta={`${cashMetric?.deltaPercent ?? "0.00"}%`}
          subtitle={`${formatCurrency(cashMetric?.deltaDisplay, model.currency)} vs month-end`}
          direction={
            Number(cashMetric?.deltaDisplay ?? "0") >= 0 ? "up" : "down"
          }
          chartValues={chartFrom("incomeEur")}
        />
        <MetricCard
          label="Income MTD"
          value={formatCurrency(incomeMetric?.valueDisplay, model.currency)}
          delta={`${incomeMetric?.deltaPercent ?? "0.00"}%`}
          subtitle={`${formatCurrency(incomeMetric?.deltaDisplay, model.currency)} from prior pace`}
          direction={
            Number(incomeMetric?.deltaDisplay ?? "0") >= 0 ? "up" : "down"
          }
          chartValues={chartFrom("incomeEur")}
        />
        <MetricCard
          label="Spending MTD"
          value={formatCurrency(spendingMetric?.valueDisplay, model.currency)}
          delta={`${spendingMetric?.deltaPercent ?? "0.00"}%`}
          subtitle={`${formatCurrency(spendingMetric?.deltaDisplay, model.currency)} from prior pace`}
          direction={
            Number(spendingMetric?.deltaDisplay ?? "0") <= 0 ? "down" : "up"
          }
          chartValues={chartFrom("spendingEur")}
        />
        <MetricCard
          label="Operating Net"
          value={formatCurrency(netMetric?.valueDisplay, model.currency)}
          delta={`${netMetric?.deltaPercent ?? "0.00"}%`}
          subtitle={`${formatCurrency(netMetric?.deltaDisplay, model.currency)} from prior pace`}
          direction={
            Number(netMetric?.deltaDisplay ?? "0") >= 0 ? "up" : "down"
          }
          chartValues={chartFrom("operatingNetEur")}
        />
        <MetricCard
          label="Portfolio Value"
          value={formatCurrency(portfolioMetric?.valueDisplay, model.currency)}
          delta={`${portfolioMetric?.deltaPercent ?? "0.00"}%`}
          subtitle={`${formatCurrency(portfolioMetric?.deltaDisplay, model.currency)} vs month-end`}
          direction={
            Number(portfolioMetric?.deltaDisplay ?? "0") >= 0 ? "up" : "down"
          }
          chartValues={chartFrom("incomeEur")}
        />
        <MetricCard
          label="Unrealized Gain"
          value={formatCurrency(unrealizedMetric?.valueDisplay, model.currency)}
          delta={`${unrealizedMetric?.deltaPercent ?? "0.00"}%`}
          subtitle={`${formatCurrency(unrealizedMetric?.deltaDisplay, model.currency)} vs month-end`}
          direction={
            Number(unrealizedMetric?.deltaDisplay ?? "0") >= 0 ? "up" : "down"
          }
          chartValues={chartFrom("operatingNetEur")}
        />
        <MetricCard
          label="Pending Review"
          value={String(model.summary.quality.pendingReviewCount)}
          delta={
            model.summary.quality.pendingReviewCount > 0
              ? "Needs work"
              : "Clear"
          }
          subtitle={`${formatCurrency(model.summary.quality.unclassifiedAmountMtdEur, model.currency)} uncategorized`}
          direction={
            model.summary.quality.pendingReviewCount > 0 ? "down" : "up"
          }
          chartValues={chartValues.map((row) => Number(row.spendingEur) / 10)}
        />

        <TimelinePanel
          title="Recent Financial Activity"
          actions={
            <div style={{ display: "flex", gap: 8 }}>
              <a className="btn-ghost" href={buildTimeLink(model, "mtd")}>
                Month
              </a>
              <a className="btn-ghost" href={buildTimeLink(model, "ytd")}>
                Year
              </a>
            </div>
          }
          currency={model.currency}
          transactions={model.summary.recentLargeTransactions
            .slice(0, 4)
            .map((row) => ({
              id: row.id,
              transactionDate: row.transactionDate,
              descriptionRaw: row.descriptionRaw,
              amountDisplay: formatCurrency(
                convertBaseEurToDisplayAmount(
                  model.dataset,
                  row.amountBaseEur,
                  model.currency,
                  row.transactionDate,
                ),
                model.currency,
              ),
              positive: Number(row.amountBaseEur) > 0,
            }))}
        />

        <div className="sidebar-panel">
          <CategoryListCard
            title="Current-Month Spending Categories"
            rows={model.summary.spendingByCategory
              .slice(0, 4)
              .map((row, index) => ({
                label: row.label,
                value: formatCurrency(row.amountEur, model.currency),
                icon: ["💻", "🏠", "🍔", "🚗"][index] ?? "•",
              }))}
            ctaHref={`/spending?scope=${model.scopeParam}&currency=${model.currency}&period=${state.period}`}
            ctaLabel="Full Analysis"
          />
          <HighlightCard
            title="Insight"
            body={
              model.summary.insights[0]?.body ??
              "Structured insights are ready."
            }
            metric={formatCurrency(
              unrealizedMetric?.valueDisplay,
              model.currency,
            )}
            footer="Current unrealized gain"
          />
        </div>

        <QualityBanner rows={qualityRows} />

        <SectionCard
          title="12-Month Operating Cash Flow"
          subtitle="History"
          span="span-12"
        >
          <MultiSeriesChart rows={model.summary.monthlySeries} />
        </SectionCard>

        <SectionCard
          title="Current-Month Spending by Category"
          subtitle="Breakdown"
          span="span-6"
        >
          <DistributionList
            rows={model.summary.spendingByCategory.map((row) => ({
              label: row.label,
              amountEur: row.amountEur,
            }))}
            currency={model.currency}
          />
        </SectionCard>

        <SectionCard
          title="Portfolio Allocation by Security"
          subtitle="Current holdings"
          span="span-6"
        >
          <DistributionList
            rows={model.summary.portfolioAllocation}
            currency={model.currency}
          />
        </SectionCard>

        <SimpleTable
          span="span-8"
          headers={[
            "Security",
            "Qty",
            "Avg Cost",
            "Current Value",
            "Unrealized",
            "Freshness",
          ]}
          rows={model.summary.topHoldings.map((holding) => [
            holding.symbol,
            formatQuantity(holding.quantity),
            formatCurrency(holding.avgCostEur, "EUR"),
            formatCurrency(holding.currentValueEur, model.currency),
            `${formatCurrency(holding.unrealizedPnlEur, model.currency)} (${formatPercent(
              holding.unrealizedPnlPercent,
            )})`,
            holding.quoteFreshness.toUpperCase(),
          ])}
        />

        <SectionCard
          title="Insights"
          subtitle="Deterministic evidence"
          span="span-4"
        >
          <InsightCards insights={model.summary.insights} />
        </SectionCard>
      </div>
    </AppShell>
  );
}

function buildTimeLink(
  model: { scopeParam: string; currency: string },
  period: "mtd" | "ytd",
) {
  return `/?scope=${model.scopeParam}&currency=${model.currency}&period=${period}`;
}

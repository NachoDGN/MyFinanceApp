import type { DashboardModel } from "../lib/queries";
import { formatCurrency } from "../lib/formatters";
import { buildHref } from "../lib/queries";
import { convertBaseEurToDisplayAmount } from "../lib/currency";
import { AppShell } from "./app-shell";
import {
  HighlightCard,
  MetricCard,
  QualityBanner,
  TimelinePanel,
} from "./primitives";

function describePeriodLabel(period: string) {
  return period === "ytd" ? "YTD" : "MTD";
}

export function DashboardView({
  pathname,
  scopeOptions,
  state,
  model,
}: {
  pathname: string;
  scopeOptions: Array<{ value: string; label: string }>;
  state: {
    scopeParam: string;
    currency: string;
    period: string;
    referenceDate?: string;
    start?: string;
    end?: string;
  };
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
  const periodLabel = describePeriodLabel(state.period);

  const monthlySeries = model.summary.monthlySeries.slice(-5);
  const chartFrom = (key: "incomeEur" | "spendingEur" | "operatingNetEur") =>
    monthlySeries.map((row) => Math.abs(Number(row[key])));

  const qualityRows = [
    {
      label: "Auto analysis queue",
      value: String(model.summary.quality.pendingEnrichmentCount),
      meta:
        model.summary.quality.pendingEnrichmentCount > 0
          ? "Rows still moving through the automatic enrichment worker"
          : "No rows waiting for automatic analysis",
    },
    {
      label: "Manual review",
      value: String(model.summary.quality.pendingReviewCount),
      meta:
        model.summary.quality.pendingReviewCount > 0
          ? "Rows flagged for manual follow-up, such as unmapped securities or ambiguous classifications"
          : "No unresolved rows in the current scope",
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
      meta: "Quotes are surfaced with delayed or fresh state",
    },
  ];

  const transactionsHref = buildHref("/transactions", state, {});
  const spendingHref = buildHref("/spending", state, {});

  return (
    <AppShell pathname={pathname} scopeOptions={scopeOptions} state={state}>
      <div className="dashboard-grid dashboard-home-grid">
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
            <a className="btn-pill" href={transactionsHref}>
              Review Ledger
            </a>
          </div>
        </div>

        <div className="dashboard-metrics-grid">
          <MetricCard
            label="Cash Position"
            value={formatCurrency(cashMetric?.valueDisplay, model.currency)}
            delta={`${cashMetric?.deltaPercent ?? "0.00"}%`}
            subtitle={`${formatCurrency(cashMetric?.deltaDisplay, model.currency)} vs ${state.period === "ytd" ? "year-start" : "month-start"}`}
            direction={
              Number(cashMetric?.deltaDisplay ?? "0") >= 0 ? "up" : "down"
            }
            chartValues={chartFrom("incomeEur")}
          />
          <MetricCard
            label={`Income ${periodLabel}`}
            value={formatCurrency(incomeMetric?.valueDisplay, model.currency)}
            delta={`${incomeMetric?.deltaPercent ?? "0.00"}%`}
            subtitle={`${formatCurrency(incomeMetric?.deltaDisplay, model.currency)} from prior pace`}
            direction={
              Number(incomeMetric?.deltaDisplay ?? "0") >= 0 ? "up" : "down"
            }
            chartValues={chartFrom("incomeEur")}
          />
          <MetricCard
            label={`Spending ${periodLabel}`}
            value={formatCurrency(spendingMetric?.valueDisplay, model.currency)}
            delta={`${spendingMetric?.deltaPercent ?? "0.00"}%`}
            subtitle={`${formatCurrency(spendingMetric?.deltaDisplay, model.currency)} from prior pace`}
            direction={
              Number(spendingMetric?.deltaDisplay ?? "0") <= 0 ? "down" : "up"
            }
            chartValues={chartFrom("spendingEur")}
          />
          <MetricCard
            label={`Operating Net ${periodLabel}`}
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
            subtitle={`${formatCurrency(portfolioMetric?.deltaDisplay, model.currency)} vs ${state.period === "ytd" ? "year-start" : "month-start"}`}
            direction={
              Number(portfolioMetric?.deltaDisplay ?? "0") >= 0 ? "up" : "down"
            }
            chartValues={chartFrom("incomeEur")}
          />
          <MetricCard
            label="Unrealized Gain"
            value={formatCurrency(unrealizedMetric?.valueDisplay, model.currency)}
            delta={`${unrealizedMetric?.deltaPercent ?? "0.00"}%`}
            subtitle={`${formatCurrency(unrealizedMetric?.deltaDisplay, model.currency)} vs ${state.period === "ytd" ? "year-start" : "month-start"}`}
            direction={
              Number(unrealizedMetric?.deltaDisplay ?? "0") >= 0
                ? "up"
                : "down"
            }
            chartValues={chartFrom("operatingNetEur")}
          />
        </div>

        <div className="dashboard-secondary-grid">
          <div className="dashboard-sidebar-stack">
            <div className="dashboard-side-card">
              <div className="kpi-header">
                <span className="label-sm">Manual Review</span>
                <span className="trend-indicator trend-up">
                  {model.summary.quality.pendingReviewCount > 0
                    ? "Needs work"
                    : "Clear"}
                </span>
              </div>
              <div className="metric-value">
                {model.summary.quality.pendingReviewCount}
              </div>
              <div className="metric-nominal">
                {model.summary.quality.pendingEnrichmentCount} rows still in the auto-analysis queue
              </div>
            </div>

            <div className="dashboard-action-card">
              <span className="label-sm">Current-Month Spending Categories</span>
              <a className="btn-ghost dashboard-action-button" href={spendingHref}>
                Full Analysis
              </a>
            </div>

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

          <TimelinePanel
            title="Recent Financial Activity"
            actions={
              <div style={{ display: "flex", gap: 8 }}>
                <a
                  className="btn-ghost"
                  href={buildHref(pathname, state, { period: "mtd" })}
                >
                  Month
                </a>
                <a
                  className="btn-ghost"
                  href={buildHref(pathname, state, { period: "ytd" })}
                >
                  Year
                </a>
              </div>
            }
            currency={model.currency}
            viewAllHref={transactionsHref}
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
        </div>

        <QualityBanner rows={qualityRows} />
      </div>
    </AppShell>
  );
}

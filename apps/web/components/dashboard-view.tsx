import type { DashboardModel } from "../lib/queries";
import { formatCurrency, formatDate } from "../lib/formatters";
import { buildHref, type NavigationState } from "../lib/navigation";
import {
  convertBaseEurToDisplayAmount,
  formatBaseEurAmountForDisplay,
} from "../lib/currency";
import { AppShell } from "./app-shell";
import { MetricCard, QualityBanner, TimelinePanel } from "./primitives";

function describePeriodLabel(period: string) {
  if (period === "all") return "All Time";
  if (period === "ytd") return "YTD";
  if (period === "last_month") return "Last Month";
  if (period === "custom") return "Custom Range";
  return "MTD";
}

function describeComparisonLabel(period: string) {
  if (period === "all") return "inception";
  if (period === "ytd") return "year-start";
  if (period === "last_month") return "previous month";
  if (period === "custom") return "prior window";
  return "month-start";
}

function metricDirection(
  deltaDisplay: string | null | undefined,
): "up" | "down" {
  return Number(deltaDisplay ?? "0") >= 0 ? "up" : "down";
}

function formatAccountTypeLabel(value: string) {
  return value
    .split("_")
    .map((part) => `${part.slice(0, 1).toUpperCase()}${part.slice(1)}`)
    .join(" ");
}

function accountVisualKind(
  row: DashboardModel["accountBalances"]["rows"][number],
) {
  if (row.assetDomain === "investment") return "investment";
  if (row.accountType === "credit_card") return "card";
  if (row.accountType === "savings") return "savings";
  if (row.accountType === "company_bank") return "business";
  return "checking";
}

function accountVisualTone(
  row: DashboardModel["accountBalances"]["rows"][number],
) {
  if (row.assetDomain === "investment") return "indigo";
  if (row.accountType === "credit_card") return "orange";
  if (row.accountType === "savings") return "green";
  if (row.accountType === "company_bank") return "purple";
  return "blue";
}

function AccountBalancesPanel({
  pathname,
  state,
  model,
}: {
  pathname: string;
  state: NavigationState;
  model: DashboardModel;
}) {
  const hasFutureActivity = model.accountBalances.hasFutureActivity;
  const materializedDateLabel = formatDate(
    model.accountBalances.materializedReferenceDate,
    { lenient: true },
  );
  const projectedDateLabel = formatDate(
    model.accountBalances.projectedReferenceDate,
    { lenient: true },
  );

  return (
    <section
      className="account-balances-card"
      aria-labelledby="account-balances-title"
    >
      <div className="account-balances-header">
        <span className="label-sm" id="account-balances-title">
          Accounts Overview
        </span>
        <span className="account-balances-date">
          {hasFutureActivity
            ? `Projected ${projectedDateLabel}`
            : model.referenceDate
              ? `As of ${formatDate(model.referenceDate, { lenient: true })}`
              : "Current"}
        </span>
      </div>

      {hasFutureActivity ? (
        <p className="account-balances-note">
          Includes announced transactions through {projectedDateLabel}. Today’s
          materialized balance is shown under each projected value.
        </p>
      ) : null}

      <div className="account-balances-list">
        {model.accountBalances.rows.map((row) => {
          const projectedBalance = formatBaseEurAmountForDisplay(
            model.dataset,
            row.projectedBaseEur,
            model.currency,
            model.referenceDate,
          );
          const materializedBalance = formatBaseEurAmountForDisplay(
            model.dataset,
            row.materializedBaseEur,
            model.currency,
            model.accountBalances.materializedReferenceDate,
          );
          const futureDelta = formatBaseEurAmountForDisplay(
            model.dataset,
            row.futureDeltaBaseEur,
            model.currency,
            model.referenceDate,
          );
          const hasFutureDelta =
            hasFutureActivity && Number(row.futureDeltaBaseEur ?? "0") !== 0;
          const isNegative = Number(row.projectedBaseEur ?? "0") < 0;
          const subtitle = row.accountSuffix
            ? `${row.institutionName} · •••• ${row.accountSuffix}`
            : `${row.entityName} · ${formatAccountTypeLabel(row.accountType)}`;

          return (
            <a
              className="account-balance-row"
              href={buildHref(pathname, state, {
                scopeParam: `account:${row.id}`,
              })}
              key={row.id}
            >
              <span
                className={`account-balance-icon tone-${accountVisualTone(row)}`}
                data-kind={accountVisualKind(row)}
                aria-hidden="true"
              />
              <span className="account-balance-copy">
                <span className="account-balance-name">{row.displayName}</span>
                <span className="account-balance-meta">{subtitle}</span>
              </span>
              <span
                className={`account-balance-values${isNegative ? " negative" : ""}`}
              >
                <span className="account-balance-value">
                  {projectedBalance}
                </span>
                {hasFutureActivity ? (
                  <span className="account-balance-secondary">
                    {materializedDateLabel}: {materializedBalance}
                    {hasFutureDelta ? ` · Pending ${futureDelta}` : ""}
                  </span>
                ) : null}
              </span>
            </a>
          );
        })}
      </div>

      <div className="account-balances-total">
        <span>Total Balance</span>
        <strong>
          {formatBaseEurAmountForDisplay(
            model.dataset,
            model.accountBalances.projectedTotalBaseEur,
            model.currency,
            model.referenceDate,
          )}
        </strong>
      </div>
      {hasFutureActivity ? (
        <div className="account-balances-materialized-total">
          <span>{materializedDateLabel} balance</span>
          <strong>
            {formatBaseEurAmountForDisplay(
              model.dataset,
              model.accountBalances.materializedTotalBaseEur,
              model.currency,
              model.accountBalances.materializedReferenceDate,
            )}
          </strong>
        </div>
      ) : null}
    </section>
  );
}

export function DashboardView({
  pathname,
  scopeOptions,
  state,
  model,
}: {
  pathname: string;
  scopeOptions: Array<{ value: string; label: string }>;
  state: NavigationState;
  model: DashboardModel;
}) {
  const selectedScopeLabel =
    scopeOptions.find((option) => option.value === state.scopeParam)?.label ??
    "Selected scope";
  const metricMap = new Map(
    model.summary.metrics.map((metric) => [metric.metricId, metric]),
  );
  const cashMetric = metricMap.get("cash_total_current");
  const incomeMetric = metricMap.get("income_mtd_total");
  const spendingMetric = metricMap.get("spending_mtd_total");
  const netMetric = metricMap.get("operating_net_cash_flow_mtd");
  const netWorthMetric = metricMap.get("net_worth_current");
  const periodLabel = describePeriodLabel(state.period);
  const comparisonLabel = describeComparisonLabel(state.period);

  const monthlySeries = model.summary.monthlySeries.slice(-5);
  const chartFrom = (key: "incomeEur" | "spendingEur" | "operatingNetEur") =>
    monthlySeries.map((row) =>
      Math.abs(
        Number(
          convertBaseEurToDisplayAmount(
            model.dataset,
            row[key],
            model.currency,
            model.referenceDate,
          ) ?? row[key],
        ),
      ),
    );
  const metricCards: Array<{
    label: string;
    metric: DashboardModel["summary"]["metrics"][number] | undefined;
    subtitle: string;
    chartValues: number[];
    direction: "up" | "down";
  }> = [
    {
      label: "Cash Position",
      metric: cashMetric,
      subtitle: `vs ${comparisonLabel}`,
      chartValues: chartFrom("incomeEur"),
      direction: metricDirection(cashMetric?.deltaDisplay),
    },
    {
      label: `Income ${periodLabel}`,
      metric: incomeMetric,
      subtitle: "from prior pace",
      chartValues: chartFrom("incomeEur"),
      direction: metricDirection(incomeMetric?.deltaDisplay),
    },
    {
      label: `Spending ${periodLabel}`,
      metric: spendingMetric,
      subtitle: "from prior pace",
      chartValues: chartFrom("spendingEur"),
      direction:
        Number(spendingMetric?.deltaDisplay ?? "0") <= 0 ? "down" : "up",
    },
    {
      label: `Operating Net ${periodLabel}`,
      metric: netMetric,
      subtitle: "from prior pace",
      chartValues: chartFrom("operatingNetEur"),
      direction: metricDirection(netMetric?.deltaDisplay),
    },
  ];

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
      value: formatBaseEurAmountForDisplay(
        model.dataset,
        model.summary.quality.unclassifiedAmountMtdEur,
        model.currency,
        model.referenceDate,
      ),
      meta: "Selected-period uncategorized exposure",
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
  const showWealthBreakdown =
    state.scopeParam === "consolidated" && model.summaryBreakdown !== null;

  return (
    <AppShell pathname={pathname} scopeOptions={scopeOptions} state={state}>
      <div className="dashboard-grid dashboard-home-grid">
        <div className="dashboard-kpi-grid">
          <div className="summary-card">
            <div className="summary-grid-overlay" aria-hidden="true" />
            <div className="summary-glow" aria-hidden="true" />
            <div className="summary-card-top">
              <div className="summary-content">
                <span className="label-sm dark-label">Total Net Wealth</span>
                <div className="total-wealth">
                  {formatCurrency(netWorthMetric?.valueDisplay, model.currency)}
                </div>
              </div>
              <a className="summary-icon-button" href={transactionsHref}>
                <span className="summary-trend-icon" aria-hidden="true" />
                <span className="sr-only">Open ledger</span>
              </a>
            </div>
            <div className="wealth-breakdown">
              {showWealthBreakdown ? (
                <>
                  <div className="account-type">
                    <span className="label-sm dark-label">
                      <span className="account-dot accent" />
                      Personal Accounts
                    </span>
                    <div className="account-value">
                      {formatCurrency(
                        model.summaryBreakdown?.personal?.valueDisplay,
                        model.currency,
                      )}
                    </div>
                  </div>
                  <div className="account-type with-divider">
                    <span className="label-sm dark-label">
                      <span className="account-dot muted" />
                      Company Assets
                    </span>
                    <div className="account-value">
                      {formatCurrency(
                        model.summaryBreakdown?.companies.valueDisplay,
                        model.currency,
                      )}
                    </div>
                  </div>
                </>
              ) : (
                <div className="account-type">
                  <span className="label-sm dark-label">Selected Scope</span>
                  <div className="account-value">{selectedScopeLabel}</div>
                </div>
              )}
              <a className="summary-ledger-button" href={transactionsHref}>
                Review Full Ledger
              </a>
            </div>
          </div>

          {metricCards.map((card) => (
            <MetricCard
              key={card.label}
              label={card.label}
              value={formatCurrency(card.metric?.valueDisplay, model.currency)}
              delta={`${card.metric?.deltaPercent ?? "0.00"}%`}
              subtitle={`${formatCurrency(card.metric?.deltaDisplay, model.currency)} ${card.subtitle}`}
              direction={card.direction}
              chartValues={card.chartValues}
            />
          ))}
        </div>

        <div className="dashboard-secondary-grid">
          <div className="dashboard-sidebar-stack">
            <AccountBalancesPanel
              pathname={pathname}
              state={state}
              model={model}
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

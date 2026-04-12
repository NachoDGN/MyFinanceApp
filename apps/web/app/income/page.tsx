import { AppShell } from "../../components/app-shell";
import { SimpleTable } from "../../components/primitives";
import { ReviewEditorCell } from "../../components/review-editor-cell";
import {
  convertBaseEurToDisplayAmount,
  formatBaseEurAmountForDisplay,
} from "../../lib/currency";
import { formatCurrency } from "../../lib/formatters";
import { getIncomeModel } from "../../lib/queries";

function formatMonthLabel(value: string) {
  return new Intl.DateTimeFormat("en-US", {
    month: "short",
  }).format(new Date(`${value}T00:00:00Z`));
}

function formatMonthRange(start: string, end: string) {
  return `${formatMonthLabel(start)} ${start.slice(0, 4)} — ${formatMonthLabel(end)} ${end.slice(0, 4)}`;
}

function formatPercentLabel(value: string | null | undefined) {
  return `${Number(value ?? 0).toFixed(2)}%`;
}

export default async function IncomePage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getIncomeModel(searchParams);
  const chartRows = model.monthlyIncomeComposition.map((row) => {
    const operatingIncomeDisplay = Number(
      convertBaseEurToDisplayAmount(
        model.dataset,
        row.operatingIncomeEur,
        model.currency,
        row.month,
      ) ?? 0,
    );
    const investmentIncomeDisplay = Number(
      convertBaseEurToDisplayAmount(
        model.dataset,
        row.investmentIncomeEur,
        model.currency,
        row.month,
      ) ?? 0,
    );

    return {
      ...row,
      operatingIncomeDisplay,
      investmentIncomeDisplay,
      totalIncomeDisplay: operatingIncomeDisplay + investmentIncomeDisplay,
    };
  });
  const chartMax = Math.max(
    ...chartRows.map((row) => row.totalIncomeDisplay),
    1,
  );
  const chartAxisValues = [1, 0.66, 0.33, 0].map((step) =>
    formatCurrency((chartMax * step).toFixed(2), model.currency),
  );
  const chartRangeLabel =
    chartRows.length > 0
      ? formatMonthRange(chartRows[0].month, chartRows[chartRows.length - 1].month)
      : "No income data";
  const currentPeriodIncome = Number(model.incomeMetric?.valueBaseEur ?? 0);
  const completenessPercent = Number(model.incomeCompletenessPercent);
  const completenessLabel =
    completenessPercent >= 100
      ? "Fully Reconciled"
      : `${(100 - completenessPercent).toFixed(2)}% still pending`;
  const scopeDescription =
    model.scope.kind === "consolidated"
      ? "Global view across your personal and company entities. Use the scope pills above to isolate any one of them without losing the consolidated total."
      : "Scoped view of the selected entity. Switch back to the consolidated pill above to see the global total that rolls everything up together.";

  return (
    <AppShell
      pathname="/income"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid income-editorial-shell">
        <div className="income-editorial-watermark">INCOME</div>

        <div className="income-page-header span-12">
          <div>
            <h1 className="page-title">Income Overview</h1>
            <p className="page-subtitle">
              Primary income KPIs exclude reimbursements, refunds, owner
              contributions, loan proceeds, and internal transfers.{" "}
              {scopeDescription}
            </p>
          </div>
        </div>

        <div className="income-kpi-grid span-12">
          <article className="income-kpi-card income-kpi-card-accent">
            <div className="income-kpi-title">
              <span>
                {model.period.preset === "ytd"
                  ? "Current-Year Income"
                  : "Current-Period Income"}
              </span>
              <span className="income-kpi-icon">i</span>
            </div>
            <div className="income-kpi-value">
              {formatCurrency(model.incomeMetric?.valueDisplay, model.currency)}
            </div>
            <div className="income-kpi-badge accent">
              {formatPercentLabel(model.incomeMetric?.deltaPercent)} vs prior
            </div>
          </article>

          <article className="income-kpi-card">
            <div className="income-kpi-title">
              <span>Trailing 3-Month Avg</span>
            </div>
            <div className="income-kpi-value">
              {formatBaseEurAmountForDisplay(
                model.dataset,
                model.trailingThreeMonthAverage,
                model.currency,
                model.referenceDate,
              )}
            </div>
            <div className="income-kpi-badge neutral">Average baseline</div>
          </article>

          <article className="income-kpi-card">
            <div className="income-kpi-title">
              <span>Top Source Concentration</span>
            </div>
            <div className="income-kpi-value">
              {formatPercentLabel(model.topSourceShare)}
            </div>
            <div className="income-kpi-badge neutral">
              {model.sourceRows[0]?.label ?? "No active sources"}
            </div>
          </article>

          <article className="income-kpi-card income-kpi-card-accent">
            <div className="income-kpi-title">
              <span>Coverage / Completeness</span>
            </div>
            <div className="income-kpi-value">
              {Number.isFinite(completenessPercent)
                ? `${completenessPercent.toFixed(0)}%`
                : "N/A"}
            </div>
            <div
              className={`income-kpi-badge ${
                completenessPercent >= 100 ? "accent" : "neutral"
              }`}
            >
              {completenessLabel}
            </div>
          </article>
        </div>

        <section className="income-chart-card span-12">
          <div className="income-chart-header">
            <div>
              <h2 className="income-chart-title">Monthly Inflow Trend</h2>
            </div>
            <div className="income-kpi-badge neutral">{chartRangeLabel}</div>
          </div>

          <div className="income-chart-body">
            <div className="income-y-axis">
              {chartAxisValues.map((label) => (
                <span key={label}>{label}</span>
              ))}
            </div>
            <div className="income-grid-lines" aria-hidden="true">
              {chartAxisValues.map((label) => (
                <div className="income-grid-line" key={label} />
              ))}
            </div>
            <div className="income-chart-bars">
              {chartRows.map((row, index) => {
                const operatingHeight = Math.max(
                  0,
                  (row.operatingIncomeDisplay / chartMax) * 100,
                );
                const investmentHeight = Math.max(
                  0,
                  (row.investmentIncomeDisplay / chartMax) * 100,
                );

                return (
                  <div className="income-bar-group" key={row.month}>
                    <div
                      className="income-bar-segment income-bar-segment-investment"
                      style={{ height: `${investmentHeight}%` }}
                    />
                    <div
                      className="income-bar-segment income-bar-segment-operating"
                      style={{ height: `${operatingHeight}%` }}
                    />
                    <div
                      className={`income-bar-label ${
                        index === chartRows.length - 1 ? "active" : ""
                      }`}
                    >
                      {formatMonthLabel(row.month)}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </section>

        <section className="income-bottom-grid span-12">
          <article className="income-breakdown-card">
            <div className="income-chart-header">
              <h2 className="income-chart-title">Income Source Breakdown</h2>
            </div>

            <div className="income-breakdown-table">
              <div className="income-breakdown-head">
                <div>Source Entity</div>
                <div>Distribution</div>
                <div className="amount">Volume</div>
                <div className="amount">Share</div>
              </div>
              {model.sourceRows.length === 0 ? (
                <div className="table-empty-state">
                  No resolved income sources are available for this period.
                </div>
              ) : (
                model.sourceRows.map((row) => {
                  const amountDisplay =
                    convertBaseEurToDisplayAmount(
                      model.dataset,
                      row.amountEur,
                      model.currency,
                      model.referenceDate,
                    ) ?? "0.00";
                  const share =
                    currentPeriodIncome > 0
                      ? (Number(row.amountEur) / currentPeriodIncome) * 100
                      : 0;

                  return (
                    <div className="income-breakdown-row" key={row.label}>
                      <div className="source-name">{row.label}</div>
                      <div className="source-progress-track">
                        <div
                          className="source-progress-fill"
                          style={{ width: `${Math.max(share, 0)}%` }}
                        />
                      </div>
                      <div className="amount">
                        {formatCurrency(amountDisplay, model.currency)}
                      </div>
                      <div className="amount">{share.toFixed(2)}%</div>
                    </div>
                  );
                })
              )}
            </div>
          </article>

          <article className="income-summary-card">
            <div className="income-chart-header">
              <h2 className="income-chart-title">Period Summary</h2>
            </div>

            <div className="income-summary-stat">
              <div className="stat-label">Total Realized YTD</div>
              <div className="stat-value accent">
                {formatBaseEurAmountForDisplay(
                  model.dataset,
                  model.ytdIncomeTotal,
                  model.currency,
                  model.referenceDate,
                )}
              </div>
              <div className="stat-description">
                Includes all settled incoming transactions.
              </div>
            </div>

            <div className="income-summary-stat">
              <div className="stat-label">Projected EOY</div>
              <div className="stat-value">
                {formatBaseEurAmountForDisplay(
                  model.dataset,
                  model.projectedYearIncome,
                  model.currency,
                  model.referenceDate,
                )}
              </div>
              <div className="stat-description">
                Based on trailing 3-month run rate.
              </div>
            </div>

            <div className="income-summary-stat">
              <div className="stat-label">Active Sources</div>
              <div className="stat-value">
                {model.activeSourceCount}{" "}
                <span className="stat-inline-label">entities</span>
              </div>
              <div className="stat-description">
                Investment income for this period:{" "}
                {formatBaseEurAmountForDisplay(
                  model.dataset,
                  model.investmentIncome,
                  model.currency,
                  model.referenceDate,
                )}.
              </div>
            </div>
          </article>
        </section>

        <SimpleTable
          span="span-12"
          headers={[
            "Date",
            "Entity",
            "Account",
            "Source",
            "Class",
            "Category",
            "Amount",
            "Review",
            "Confidence",
          ]}
          rows={model.transactions.map((row) => [
            row.transactionDate,
            model.dataset.entities.find((entity) => entity.id === row.economicEntityId)
              ?.displayName ?? row.economicEntityId,
            model.dataset.accounts.find((account) => account.id === row.accountId)
              ?.displayName ?? row.accountId,
            row.counterpartyName ?? row.merchantNormalized ?? row.descriptionRaw,
            row.transactionClass,
            row.categoryCode ?? "—",
            formatCurrency(
              convertBaseEurToDisplayAmount(
                model.dataset,
                row.amountBaseEur,
                model.currency,
                row.transactionDate,
              ),
              model.currency,
            ),
            <ReviewEditorCell
              transactionId={row.id}
              needsReview={row.needsReview}
              reviewReason={row.reviewReason}
              manualNotes={row.manualNotes}
              transactionClass={row.transactionClass}
              classificationSource={row.classificationSource}
              quantity={row.quantity}
              llmPayload={row.llmPayload}
            />,
            row.classificationConfidence,
          ])}
        />
      </div>
    </AppShell>
  );
}

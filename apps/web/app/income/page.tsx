import { AppShell } from "../../components/app-shell";
import {
  FlowBreakdownCard,
  FlowKpiGrid,
  FlowPageHeader,
  FlowSummaryCard,
  FlowTrendChart,
} from "../../components/flow-overview";
import { SimpleTable } from "../../components/primitives";
import { ReviewEditorCell } from "../../components/review-editor-cell";
import {
  convertBaseEurToDisplayAmount,
  convertBaseEurToDisplayAmountWithFallback,
  endOfMonthIso,
  formatBaseEurAmountForDisplay,
} from "../../lib/currency";
import {
  formatMonthLabel,
  formatMonthRange,
  formatPercentLabel,
  getPeriodLabel,
} from "../../lib/dashboard";
import { formatCurrency } from "../../lib/formatters";
import { paginateFlowRows } from "../../lib/flow-page";
import { buildHref } from "../../lib/navigation";
import { getIncomeModel } from "../../lib/queries";

export default async function IncomePage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const model = await getIncomeModel(params);
  const categoryRows = model.incomeCategoryRows;
  const categoryPagination = paginateFlowRows(
    categoryRows,
    params.categoryPage,
  );
  const {
    page: categoryPage,
    pageCount: categoryPageCount,
    rangeLabel: categoryRangeLabel,
    visibleRows: visibleCategoryRows,
    hasMultiplePages: hasMultipleCategoryPages,
  } = categoryPagination;
  const chartRows = model.monthlyIncomeComposition.map((row) => {
    const effectiveDate =
      endOfMonthIso(row.month) <= model.referenceDate
        ? endOfMonthIso(row.month)
        : model.referenceDate;
    const operatingIncomeDisplay = convertBaseEurToDisplayAmountWithFallback(
      model.dataset,
      row.operatingIncomeEur,
      model.currency,
      effectiveDate,
      { fallbackDate: model.referenceDate },
    );
    const investmentIncomeDisplay = convertBaseEurToDisplayAmountWithFallback(
      model.dataset,
      row.investmentIncomeEur,
      model.currency,
      effectiveDate,
      { fallbackDate: model.referenceDate },
    );

    return {
      ...row,
      operatingIncomeDisplay: Number(operatingIncomeDisplay.amount ?? 0),
      investmentIncomeDisplay: Number(investmentIncomeDisplay.amount ?? 0),
      usedFallbackFx:
        model.currency !== "EUR" &&
        (operatingIncomeDisplay.usedFallbackFx ||
          investmentIncomeDisplay.usedFallbackFx),
      totalIncomeDisplay:
        Number(operatingIncomeDisplay.amount ?? 0) +
        Number(investmentIncomeDisplay.amount ?? 0),
    };
  });
  const fallbackFxMonths = chartRows
    .filter((row) => row.usedFallbackFx)
    .map((row) => formatMonthLabel(row.month));
  const fallbackFxRangeLabel =
    fallbackFxMonths.length > 0
      ? fallbackFxMonths.length === 1
        ? fallbackFxMonths[0]
        : `${fallbackFxMonths[0]}-${fallbackFxMonths[fallbackFxMonths.length - 1]}`
      : null;
  const chartMax = Math.max(
    ...chartRows.map((row) => row.totalIncomeDisplay),
    1,
  );
  const trendRows = chartRows.map((row) => ({
    month: row.month,
    segments: [
      {
        className: "income-bar-segment-investment",
        height: (row.investmentIncomeDisplay / chartMax) * 100,
      },
      {
        className: "income-bar-segment-operating",
        height: (row.operatingIncomeDisplay / chartMax) * 100,
      },
    ],
  }));
  const chartAxisValues = [1, 0.66, 0.33, 0].map((step) =>
    formatCurrency((chartMax * step).toFixed(2), model.currency),
  );
  const chartRangeLabel =
    chartRows.length > 0
      ? formatMonthRange(
          chartRows[0].month,
          chartRows[chartRows.length - 1].month,
        )
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
      pageQueryParams={{ categoryPage: String(categoryPage) }}
    >
      <div className="dashboard-grid income-editorial-shell">
        <FlowPageHeader
          watermark="INCOME"
          title="Income Overview"
          subtitle={
            <>
              Primary income KPIs exclude reimbursements, refunds, owner
              contributions, loan proceeds, and internal transfers.{" "}
              {scopeDescription}
            </>
          }
        />

        <FlowKpiGrid
          items={[
            {
              accent: true,
              title:
                model.period.preset === "ytd"
                  ? "Current-Year Income"
                  : "Current-Period Income",
              icon: <span className="income-kpi-icon">i</span>,
              value: formatCurrency(
                model.incomeMetric?.valueDisplay,
                model.currency,
              ),
              badge: `${formatPercentLabel(model.incomeMetric?.deltaPercent)} vs prior`,
              badgeTone: "accent",
            },
            {
              title: "Trailing 3-Month Avg",
              value: formatBaseEurAmountForDisplay(
                model.dataset,
                model.trailingThreeMonthAverage,
                model.currency,
                model.referenceDate,
              ),
              badge: "Average baseline",
            },
            {
              title: "Top Source Concentration",
              value: formatPercentLabel(model.topSourceShare),
              badge: model.sourceRows[0]?.label ?? "No active sources",
            },
            {
              accent: true,
              title: "Coverage / Completeness",
              value: Number.isFinite(completenessPercent)
                ? `${completenessPercent.toFixed(0)}%`
                : "N/A",
              badge: completenessLabel,
              badgeTone: completenessPercent >= 100 ? "accent" : "neutral",
            },
          ]}
        />

        <FlowTrendChart
          title="Monthly Inflow Trend"
          rangeLabel={chartRangeLabel}
          fallbackFxRangeLabel={fallbackFxRangeLabel}
          currency={model.currency}
          referenceDate={model.referenceDate}
          axisLabels={chartAxisValues}
          rows={trendRows}
        />

        <section className="income-bottom-grid span-12">
          <FlowBreakdownCard
            title="Income Category Breakdown"
            periodLabel={getPeriodLabel(model.period)}
            description="Resolved income categories for the selected period, ordered by current-period share."
            headers={["Category", "Distribution", "Volume", "Share"]}
          >
            {categoryRows.length === 0 ? (
              <div className="table-empty-state">
                No resolved income categories are available for this period.
              </div>
            ) : (
              visibleCategoryRows.map((row) => {
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
                const categoryHref = buildHref(
                  `/income/${encodeURIComponent(row.categoryCode)}`,
                  model.navigationState,
                  {},
                );

                return (
                  <a
                    className="income-breakdown-row spending-category-link-row"
                    href={categoryHref}
                    key={row.categoryCode}
                  >
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
                  </a>
                );
              })
            )}
            {hasMultipleCategoryPages ? (
              <div className="spending-category-pagination">
                <span>{categoryRangeLabel}</span>
                <div>
                  <a
                    className={
                      categoryPage <= 1
                        ? "spending-page-link disabled"
                        : "spending-page-link"
                    }
                    aria-disabled={categoryPage <= 1}
                    href={
                      categoryPage <= 1
                        ? undefined
                        : buildHref(
                            "/income",
                            model.navigationState,
                            {},
                            { categoryPage: String(categoryPage - 1) },
                          )
                    }
                  >
                    Previous
                  </a>
                  <a
                    className={
                      categoryPage >= categoryPageCount
                        ? "spending-page-link disabled"
                        : "spending-page-link"
                    }
                    aria-disabled={categoryPage >= categoryPageCount}
                    href={
                      categoryPage >= categoryPageCount
                        ? undefined
                        : buildHref(
                            "/income",
                            model.navigationState,
                            {},
                            { categoryPage: String(categoryPage + 1) },
                          )
                    }
                  >
                    Next
                  </a>
                </div>
              </div>
            ) : null}
          </FlowBreakdownCard>

          <FlowSummaryCard>
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
                )}
                .
              </div>
            </div>
          </FlowSummaryCard>
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
            model.dataset.entities.find(
              (entity) => entity.id === row.economicEntityId,
            )?.displayName ?? row.economicEntityId,
            model.dataset.accounts.find(
              (account) => account.id === row.accountId,
            )?.displayName ?? row.accountId,
            row.counterpartyName ??
              row.merchantNormalized ??
              row.descriptionRaw,
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
              categoryCode={row.categoryCode}
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

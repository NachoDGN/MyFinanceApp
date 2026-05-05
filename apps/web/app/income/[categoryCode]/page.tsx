import { notFound } from "next/navigation";

import { AppShell } from "../../../components/app-shell";
import {
  FlowBreakdownCard,
  FlowKpiGrid,
  FlowPageHeader,
  FlowSummaryCard,
  FlowTrendChart,
} from "../../../components/flow-overview";
import { ReviewEditorCell } from "../../../components/review-editor-cell";
import {
  convertBaseEurToDisplayAmountWithFallback,
  endOfMonthIso,
  formatBaseEurAmountForDisplay,
} from "../../../lib/currency";
import {
  formatDeltaBadge,
  formatMonthLabel,
  formatPercentLabel,
  getPeriodLabel,
} from "../../../lib/dashboard";
import { formatCurrency, formatDate } from "../../../lib/formatters";
import {
  createFlowSeriesColorResolver,
  formatFallbackFxRange,
  formatFlowChartRange,
  formatFlowCategoryLabel,
  formatFlowDisplayAmount,
  formatTransactionClassLabel,
  startOfMonthIso,
} from "../../../lib/flow-page";
import { buildHref } from "../../../lib/navigation";
import { getIncomeCategoryModel } from "../../../lib/queries";

type IncomeCategoryModel = Awaited<ReturnType<typeof getIncomeCategoryModel>>;
type IncomeCategoryTransaction = IncomeCategoryModel["transactions"][number];

function formatCategoryLabel(
  categoryCode: string | null | undefined,
  transactionClass: string,
  model: IncomeCategoryModel,
) {
  const category = model.category;
  const fallbackLabel =
    category && categoryCode === category.categoryCode
      ? category.label
      : (category?.label ?? formatTransactionClassLabel(transactionClass));
  return formatFlowCategoryLabel(model.dataset, categoryCode, fallbackLabel);
}

function resolveIncomeSourceLabel(transaction: IncomeCategoryTransaction) {
  return (
    transaction.counterpartyName?.trim() ||
    transaction.merchantNormalized?.trim() ||
    transaction.descriptionClean ||
    transaction.descriptionRaw
  );
}

function incomeContributionAmountEur(transaction: IncomeCategoryTransaction) {
  const amount = Number(transaction.amountBaseEur ?? 0);
  return Number.isFinite(amount) ? Math.max(amount, 0) : 0;
}

export default async function IncomeCategoryPage({
  params,
  searchParams,
}: {
  params: Promise<{ categoryCode: string }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const { categoryCode: rawCategoryCode } = await params;
  const categoryCode = decodeURIComponent(rawCategoryCode);
  const model = await getIncomeCategoryModel(searchParams, categoryCode);

  if (!model.category) {
    notFound();
  }

  const category = model.category;
  const categoryAmountNumber = Number(model.amountEur);
  const sourceLabelToRowLabel = new Map<string, string>();
  model.sourceRows.forEach((row) => {
    sourceLabelToRowLabel.set(row.label, row.label);
    row.aliases.forEach((alias) => sourceLabelToRowLabel.set(alias, row.label));
  });
  const transactionsBySource = model.transactions.reduce(
    (groups, transaction) => {
      const sourceLabel = resolveIncomeSourceLabel(transaction);
      const rowLabel = sourceLabelToRowLabel.get(sourceLabel) ?? sourceLabel;
      const existing = groups.get(rowLabel) ?? [];
      existing.push(transaction);
      groups.set(rowLabel, existing);
      return groups;
    },
    new Map<string, IncomeCategoryTransaction[]>(),
  );
  const resolveSourceColor = createFlowSeriesColorResolver(
    model.sourceRows.map((row) => row.label),
  );
  const sourceRows = model.sourceRows.map((row) => ({
    ...row,
    color: resolveSourceColor(row.label),
    transactions: transactionsBySource.get(row.label) ?? [],
  }));
  const sourceOrderByLabel = new Map(
    sourceRows.map((row, index) => [row.label, index]),
  );
  const monthlySourceAmounts = model.transactions.reduce(
    (monthGroups, transaction) => {
      const contribution = incomeContributionAmountEur(transaction);
      if (contribution <= 0) {
        return monthGroups;
      }

      const month = startOfMonthIso(transaction.transactionDate);
      const sourceLabel = resolveIncomeSourceLabel(transaction);
      const rowLabel = sourceLabelToRowLabel.get(sourceLabel) ?? sourceLabel;
      const sourceGroups = monthGroups.get(month) ?? new Map<string, number>();
      sourceGroups.set(
        rowLabel,
        (sourceGroups.get(rowLabel) ?? 0) + contribution,
      );
      monthGroups.set(month, sourceGroups);
      return monthGroups;
    },
    new Map<string, Map<string, number>>(),
  );

  const chartRows = model.monthlySeries.map((row) => {
    const effectiveDate =
      endOfMonthIso(row.month) <= model.referenceDate
        ? endOfMonthIso(row.month)
        : model.referenceDate;
    let usedFallbackFx = false;
    const sources = [
      ...(monthlySourceAmounts.get(row.month) ?? new Map<string, number>()),
    ]
      .sort((left, right) => {
        const leftOrder = sourceOrderByLabel.get(left[0]) ?? 9999;
        const rightOrder = sourceOrderByLabel.get(right[0]) ?? 9999;
        return leftOrder - rightOrder || right[1] - left[1];
      })
      .map(([label, amountEur]) => {
        const displayAmount = convertBaseEurToDisplayAmountWithFallback(
          model.dataset,
          amountEur.toFixed(2),
          model.currency,
          effectiveDate,
          { fallbackDate: model.referenceDate },
        );
        usedFallbackFx =
          usedFallbackFx ||
          (model.currency !== "EUR" && displayAmount.usedFallbackFx);

        return {
          label,
          color: resolveSourceColor(label),
          displayAmount: Math.max(Number(displayAmount.amount ?? 0), 0),
          valueLabel: formatCurrency(displayAmount.amount, model.currency),
        };
      });
    const incomeDisplay = sources.reduce(
      (sum, source) => sum + source.displayAmount,
      0,
    );

    return {
      ...row,
      sources,
      incomeDisplay,
      usedFallbackFx,
    };
  });
  const fallbackFxRangeLabel = formatFallbackFxRange(
    chartRows.filter((row) => row.usedFallbackFx).map((row) => row.month),
  );
  const chartMax = Math.max(...chartRows.map((row) => row.incomeDisplay), 1);
  const trendRows = chartRows.map((row) => ({
    month: row.month,
    segments: row.sources.map((source) => ({
      color: source.color,
      height: (source.displayAmount / chartMax) * 100,
      label: source.label,
      valueLabel: source.valueLabel,
      shareLabel:
        row.incomeDisplay > 0
          ? `${formatPercentLabel(
              (source.displayAmount / row.incomeDisplay) * 100,
            )} of ${formatMonthLabel(row.month)}`
          : undefined,
    })),
  }));
  const legendItems = sourceRows
    .filter((row) => Number(row.amountEur) > 0)
    .map((row) => ({
      label: row.label,
      color: row.color,
      valueLabel: formatBaseEurAmountForDisplay(
        model.dataset,
        row.amountEur,
        model.currency,
        model.referenceDate,
      ),
      shareLabel:
        categoryAmountNumber > 0
          ? formatPercentLabel(
              (Number(row.amountEur) / categoryAmountNumber) * 100,
            )
          : undefined,
    }));
  const chartAxisValues = [1, 0.75, 0.5, 0.25, 0].map((step) =>
    formatCurrency((chartMax * step).toFixed(2), model.currency),
  );
  const chartRangeLabel = formatFlowChartRange(
    chartRows,
    "No category income data",
  );
  const backHref = buildHref("/income", model.navigationState, {});
  const categoryAmount = formatBaseEurAmountForDisplay(
    model.dataset,
    model.amountEur,
    model.currency,
    model.referenceDate,
  );
  const comparisonAmount = formatBaseEurAmountForDisplay(
    model.dataset,
    model.comparisonAmountEur,
    model.currency,
    model.referenceDate,
  );
  const largestTransactionAmount = model.largestTransaction
    ? formatFlowDisplayAmount(
        model.dataset,
        model.largestTransaction.amountBaseEur,
        model.currency,
        model.largestTransaction.transactionDate,
      )
    : "N/A";

  return (
    <AppShell
      pathname={`/income/${encodeURIComponent(categoryCode)}`}
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid income-editorial-shell">
        <FlowPageHeader
          watermark="INCOME"
          title={`${category.label} Income`}
          subtitle={
            <>
              Category detail for {getPeriodLabel(model.period).toLowerCase()}.
              The totals, monthly trend, and expanded source buckets use the
              same resolved income logic as the overview page.
            </>
          }
          notice={
            <a className="btn-ghost spending-back-link" href={backHref}>
              Back to Income
            </a>
          }
        />

        <FlowKpiGrid
          items={[
            {
              accent: true,
              title: "Category Income",
              icon: <span className="income-kpi-icon">i</span>,
              value: categoryAmount,
              badge: `${formatPercentLabel(model.periodSharePercent)} of income`,
              badgeTone: "accent",
            },
            {
              title: "Trend vs Prior",
              value: model.comparisonDeltaPercent
                ? formatDeltaBadge(model.comparisonDeltaPercent)
                : "N/A",
              badge: `${comparisonAmount} prior period`,
              badgeTone:
                Number(model.comparisonDeltaPercent ?? 0) >= 0
                  ? "accent"
                  : "neutral",
            },
            {
              title: "Transactions",
              value: model.transactionCount.toString(),
              badge: `${formatBaseEurAmountForDisplay(
                model.dataset,
                model.averageTransactionEur,
                model.currency,
                model.referenceDate,
              )} average`,
            },
            {
              title: "Largest Transaction",
              value: largestTransactionAmount,
              badge:
                model.largestTransaction?.counterpartyName ??
                model.largestTransaction?.merchantNormalized ??
                model.largestTransaction?.descriptionClean ??
                "No transactions",
            },
          ]}
        />

        <FlowTrendChart
          title={`${category.label} Monthly Trend`}
          description="Source-level composition for this category in each month."
          rangeLabel={chartRangeLabel}
          fallbackFxRangeLabel={fallbackFxRangeLabel}
          currency={model.currency}
          referenceDate={model.referenceDate}
          axisLabels={chartAxisValues}
          rows={trendRows}
          legendItems={legendItems}
          summary={{
            title: `${category.label} Period Income`,
            value: categoryAmount,
            badge: `${formatPercentLabel(model.periodSharePercent)} of income`,
          }}
          emptyLabel={`No ${category.label.toLowerCase()} income is available for this period.`}
        />

        <section className="income-bottom-grid span-12">
          <FlowBreakdownCard
            title="Source Breakdown"
            periodLabel={getPeriodLabel(model.period)}
            description="Payer and counterparty buckets inside this category for the selected period."
            headers={["Source", "Distribution", "Volume", "Share"]}
          >
            {sourceRows.length === 0 ? (
              <div className="table-empty-state">
                No source buckets are available for this category.
              </div>
            ) : (
              sourceRows.map((row) => {
                const share =
                  categoryAmountNumber > 0
                    ? (Number(row.amountEur) / categoryAmountNumber) * 100
                    : 0;

                return (
                  <details
                    className="merchant-breakdown-details"
                    key={row.label}
                  >
                    <summary className="merchant-breakdown-summary">
                      <span className="source-name">{row.label}</span>
                      <span className="merchant-breakdown-distribution">
                        <span className="merchant-breakdown-count">
                          {row.transactions.length} transaction
                          {row.transactions.length === 1 ? "" : "s"}
                        </span>
                        <span className="source-progress-track">
                          <span
                            className="source-progress-fill"
                            style={{
                              width: `${Math.max(Math.min(share, 100), 0)}%`,
                            }}
                          />
                        </span>
                      </span>
                      <span className="amount">
                        {formatBaseEurAmountForDisplay(
                          model.dataset,
                          row.amountEur,
                          model.currency,
                          model.referenceDate,
                        )}
                      </span>
                      <span className="amount">
                        {formatPercentLabel(share)}
                      </span>
                    </summary>
                    <div className="merchant-breakdown-transactions">
                      {row.aliases.length > 1 ? (
                        <div className="muted" style={{ marginBottom: 12 }}>
                          Merged aliases: {row.aliases.join(", ")}
                        </div>
                      ) : null}
                      {row.transactions.length === 0 ? (
                        <div className="table-empty-state">
                          No transactions are attached to this source bucket.
                        </div>
                      ) : (
                        row.transactions.map((transaction) => {
                          const accountName =
                            model.dataset.accounts.find(
                              (account) => account.id === transaction.accountId,
                            )?.displayName ?? transaction.accountId;

                          return (
                            <div
                              className="merchant-breakdown-transaction"
                              key={transaction.id}
                            >
                              <div className="merchant-breakdown-transaction-copy">
                                <div className="merchant-breakdown-transaction-title">
                                  {transaction.descriptionRaw}
                                </div>
                                <div className="merchant-breakdown-transaction-meta">
                                  <span>
                                    {formatDate(transaction.transactionDate)}
                                  </span>
                                  <span>{accountName}</span>
                                  <span>
                                    {formatTransactionClassLabel(
                                      transaction.transactionClass,
                                    )}
                                  </span>
                                  <span>
                                    {formatCategoryLabel(
                                      transaction.categoryCode,
                                      transaction.transactionClass,
                                      model,
                                    )}
                                  </span>
                                </div>
                              </div>
                              <div className="merchant-breakdown-transaction-actions">
                                <strong>
                                  {formatFlowDisplayAmount(
                                    model.dataset,
                                    transaction.amountBaseEur,
                                    model.currency,
                                    transaction.transactionDate,
                                  )}
                                </strong>
                                <ReviewEditorCell
                                  transactionId={transaction.id}
                                  needsReview={transaction.needsReview}
                                  categoryCode={transaction.categoryCode}
                                  reviewReason={transaction.reviewReason}
                                  manualNotes={transaction.manualNotes}
                                  transactionClass={
                                    transaction.transactionClass
                                  }
                                  classificationSource={
                                    transaction.classificationSource
                                  }
                                  quantity={transaction.quantity}
                                  llmPayload={transaction.llmPayload}
                                  descriptionRaw={transaction.descriptionRaw}
                                  descriptionClean={
                                    transaction.descriptionClean
                                  }
                                />
                              </div>
                            </div>
                          );
                        })
                      )}
                    </div>
                  </details>
                );
              })
            )}
          </FlowBreakdownCard>

          <FlowSummaryCard>
            <div className="income-summary-stat">
              <div className="stat-label">Top Source</div>
              <div className="spending-summary-value">
                {model.topSource?.label ?? "N/A"}
              </div>
              <div className="stat-description">
                {model.topSource
                  ? `${formatBaseEurAmountForDisplay(
                      model.dataset,
                      model.topSource.amountEur,
                      model.currency,
                      model.referenceDate,
                    )} in ${category.label.toLowerCase()}`
                  : "No source totals available."}
              </div>
            </div>

            <div className="income-summary-stat">
              <div className="stat-label">Prior Period</div>
              <div className="stat-value accent">{comparisonAmount}</div>
              <div className="stat-description">
                The trend KPI compares this selected period against the prior
                comparable period.
              </div>
            </div>

            <div className="income-summary-stat">
              <div className="stat-label">Transaction Source</div>
              <div className="stat-value accent">
                {model.transactionCount.toString()}
              </div>
              <div className="stat-description">
                Expand the source rows to inspect the transactions that add up
                to the category total.
              </div>
            </div>
          </FlowSummaryCard>
        </section>
      </div>
    </AppShell>
  );
}

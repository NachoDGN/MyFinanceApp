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
  convertBaseEurToDisplayAmount,
  convertBaseEurToDisplayAmountWithFallback,
  endOfMonthIso,
  formatBaseEurAmountForDisplay,
} from "../../../lib/currency";
import {
  formatDeltaBadge,
  formatMonthLabel,
  formatMonthRange,
  formatPercentLabel,
  getPeriodLabel,
} from "../../../lib/dashboard";
import { formatCurrency, formatDate } from "../../../lib/formatters";
import { buildHref, getSpendingCategoryModel } from "../../../lib/queries";

type SpendingCategoryModel = Awaited<
  ReturnType<typeof getSpendingCategoryModel>
>;
type SpendingCategoryTransaction =
  SpendingCategoryModel["transactions"][number];

function formatDisplayAmount(
  amountBaseEur: string | null | undefined,
  currency: string,
  transactionDate: string,
  dataset: SpendingCategoryModel["dataset"],
) {
  if (amountBaseEur === null || amountBaseEur === undefined) {
    return "N/A";
  }

  return formatCurrency(
    convertBaseEurToDisplayAmount(
      dataset,
      amountBaseEur,
      currency,
      transactionDate,
    ),
    currency,
  );
}

function formatCategoryLabel(
  categoryCode: string | null | undefined,
  transactionClass: string,
  model: SpendingCategoryModel,
) {
  if (!categoryCode) {
    if (transactionClass === "loan_principal_payment") {
      return "Loan Principal";
    }
    if (transactionClass === "loan_interest_payment") {
      return "Loan Interest";
    }
    if (transactionClass === "fee") {
      return "Fees";
    }
    if (transactionClass === "refund") {
      return "Refunds";
    }
    return model.category?.label ?? "Uncategorized";
  }

  return (
    model.dataset.categories.find((category) => category.code === categoryCode)
      ?.displayName ??
    (categoryCode === model.category?.categoryCode
      ? model.category.label
      : categoryCode)
  );
}

const SPENDING_MERCHANT_COLORS = [
  "#ff4a22",
  "#005f73",
  "#0a9396",
  "#2a9d8f",
  "#e9c46a",
  "#f4a261",
  "#457b9d",
  "#3d405b",
  "#6d597a",
  "#8d99ae",
  "#2f3e46",
  "#bc4749",
];

function merchantColor(index: number) {
  return SPENDING_MERCHANT_COLORS[index % SPENDING_MERCHANT_COLORS.length]!;
}

function startOfMonthIso(value: string) {
  return `${value.slice(0, 7)}-01`;
}

function resolveMerchantLabel(transaction: SpendingCategoryTransaction) {
  if (transaction.merchantNormalized?.trim()) {
    return transaction.merchantNormalized.trim();
  }

  if (transaction.counterpartyName?.trim()) {
    return transaction.counterpartyName.trim();
  }

  return transaction.descriptionClean || transaction.descriptionRaw;
}

function spendingContributionAmountEur(
  transaction: SpendingCategoryTransaction,
) {
  const amount = Number(transaction.amountBaseEur ?? 0);
  if (!Number.isFinite(amount)) {
    return 0;
  }

  const contribution =
    transaction.transactionClass === "refund" ? -amount : Math.abs(amount);

  return Math.max(contribution, 0);
}

function formatTransactionClass(transactionClass: string) {
  return transactionClass.replace(/_/g, " ");
}

export default async function SpendingCategoryPage({
  params,
  searchParams,
}: {
  params: Promise<{ categoryCode: string }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const { categoryCode: rawCategoryCode } = await params;
  const categoryCode = decodeURIComponent(rawCategoryCode);
  const model = await getSpendingCategoryModel(searchParams, categoryCode);

  if (!model.category) {
    notFound();
  }
  const category = model.category;
  const categoryAmountNumber = Number(model.amountEur);
  const transactionsByMerchant = model.transactions.reduce(
    (groups, transaction) => {
      const label = resolveMerchantLabel(transaction);
      const existing = groups.get(label) ?? [];
      existing.push(transaction);
      groups.set(label, existing);
      return groups;
    },
    new Map<string, SpendingCategoryTransaction[]>(),
  );
  const merchantRows = model.merchantRows.map((row, index) => ({
    ...row,
    color: merchantColor(index),
    transactions: transactionsByMerchant.get(row.label) ?? [],
  }));
  const merchantColorByLabel = new Map(
    merchantRows.map((row) => [row.label, row.color]),
  );
  const merchantOrderByLabel = new Map(
    merchantRows.map((row, index) => [row.label, index]),
  );
  const monthlyMerchantAmounts = model.transactions.reduce(
    (monthGroups, transaction) => {
      const contribution = spendingContributionAmountEur(transaction);
      if (contribution <= 0) {
        return monthGroups;
      }

      const month = startOfMonthIso(transaction.transactionDate);
      const label = resolveMerchantLabel(transaction);
      const merchantGroups =
        monthGroups.get(month) ?? new Map<string, number>();
      merchantGroups.set(
        label,
        (merchantGroups.get(label) ?? 0) + contribution,
      );
      monthGroups.set(month, merchantGroups);
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
    const merchants = [...(monthlyMerchantAmounts.get(row.month) ?? new Map())]
      .sort((left, right) => {
        const leftOrder = merchantOrderByLabel.get(left[0]) ?? 9999;
        const rightOrder = merchantOrderByLabel.get(right[0]) ?? 9999;
        return leftOrder - rightOrder || right[1] - left[1];
      })
      .map(([label, amountEur], index) => {
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
          color:
            merchantColorByLabel.get(label) ??
            merchantColor(merchantColorByLabel.size + index),
          displayAmount: Math.max(Number(displayAmount.amount ?? 0), 0),
          valueLabel: formatCurrency(displayAmount.amount, model.currency),
        };
      });
    const spendingDisplay = merchants.reduce(
      (sum, merchant) => sum + merchant.displayAmount,
      0,
    );

    return {
      ...row,
      merchants,
      spendingDisplay,
      usedFallbackFx,
    };
  });
  const fallbackFxMonths = chartRows
    .filter((row) => row.usedFallbackFx)
    .map((row) => row.month);
  const fallbackFxRangeLabel =
    fallbackFxMonths.length > 0
      ? fallbackFxMonths.length === 1
        ? formatMonthLabel(fallbackFxMonths[0]!)
        : `${formatMonthLabel(fallbackFxMonths[0]!)}-${formatMonthLabel(
            fallbackFxMonths[fallbackFxMonths.length - 1]!,
          )}`
      : null;
  const chartMax = Math.max(...chartRows.map((row) => row.spendingDisplay), 1);
  const trendRows = chartRows.map((row) => ({
    month: row.month,
    segments: row.merchants.map((merchant) => ({
      color: merchant.color,
      height: (merchant.displayAmount / chartMax) * 100,
      label: merchant.label,
      valueLabel: merchant.valueLabel,
      shareLabel:
        row.spendingDisplay > 0
          ? `${formatPercentLabel(
              (merchant.displayAmount / row.spendingDisplay) * 100,
            )} of ${formatMonthLabel(row.month)}`
          : undefined,
    })),
  }));
  const legendItems = merchantRows
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
  const chartRangeLabel =
    chartRows.length > 0
      ? formatMonthRange(
          chartRows[0].month,
          chartRows[chartRows.length - 1].month,
        )
      : "No category spending data";
  const backHref = buildHref("/spending", model.navigationState, {});
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
    ? formatDisplayAmount(
        model.largestTransaction.amountBaseEur,
        model.currency,
        model.largestTransaction.transactionDate,
        model.dataset,
      )
    : "N/A";

  return (
    <AppShell
      pathname={`/spending/${encodeURIComponent(categoryCode)}`}
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid income-editorial-shell">
        <FlowPageHeader
          watermark="SPENDING"
          watermarkClassName="spending-editorial-watermark"
          title={`${category.label} Spend`}
          subtitle={
            <>
              Category detail for {getPeriodLabel(model.period).toLowerCase()}.
              The totals, monthly trend, and expanded merchant buckets use the
              same resolved spending logic as the overview page.
            </>
          }
          notice={
            <a className="btn-ghost spending-back-link" href={backHref}>
              Back to Spending
            </a>
          }
        />

        <FlowKpiGrid
          items={[
            {
              accent: true,
              title: "Category Spend",
              icon: <span className="income-kpi-icon">i</span>,
              value: categoryAmount,
              badge: `${formatPercentLabel(model.periodSharePercent)} of spend`,
              badgeTone: "accent",
            },
            {
              title: "Trend vs Prior",
              value: model.comparisonDeltaPercent
                ? formatDeltaBadge(model.comparisonDeltaPercent)
                : "N/A",
              badge: `${comparisonAmount} prior period`,
              badgeTone:
                Number(model.comparisonDeltaPercent ?? 0) > 0
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
                model.largestTransaction?.merchantNormalized ??
                model.largestTransaction?.counterpartyName ??
                model.largestTransaction?.descriptionClean ??
                "No transactions",
            },
          ]}
        />

        <FlowTrendChart
          title={`${category.label} Monthly Trend`}
          description="Merchant-level composition for this category in each month."
          rangeLabel={chartRangeLabel}
          fallbackFxRangeLabel={fallbackFxRangeLabel}
          currency={model.currency}
          referenceDate={model.referenceDate}
          axisLabels={chartAxisValues}
          rows={trendRows}
          legendItems={legendItems}
          summary={{
            title: `${category.label} Period Spend`,
            value: categoryAmount,
            badge: `${formatPercentLabel(model.periodSharePercent)} of spend`,
          }}
          emptyLabel={`No ${category.label.toLowerCase()} spend is available for this period.`}
        />

        <section className="income-bottom-grid span-12">
          <FlowBreakdownCard
            title="Merchant Breakdown"
            periodLabel={getPeriodLabel(model.period)}
            description="Merchant and counterparty buckets inside this category for the selected period."
            headers={["Merchant", "Distribution", "Volume", "Share"]}
          >
            {merchantRows.length === 0 ? (
              <div className="table-empty-state">
                No merchant buckets are available for this category.
              </div>
            ) : (
              merchantRows.map((row) => {
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
                      {row.transactions.length === 0 ? (
                        <div className="table-empty-state">
                          No transactions are attached to this merchant bucket.
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
                                    {formatTransactionClass(
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
                                  {formatDisplayAmount(
                                    transaction.amountBaseEur,
                                    model.currency,
                                    transaction.transactionDate,
                                    model.dataset,
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
                                  creditCardStatementStatus={
                                    transaction.creditCardStatementStatus
                                  }
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
              <div className="stat-label">Top Merchant</div>
              <div className="spending-summary-value">
                {model.topMerchant?.label ?? "N/A"}
              </div>
              <div className="stat-description">
                {model.topMerchant
                  ? `${formatBaseEurAmountForDisplay(
                      model.dataset,
                      model.topMerchant.amountEur,
                      model.currency,
                      model.referenceDate,
                    )} in ${category.label.toLowerCase()}`
                  : "No merchant totals available."}
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
                Expand the merchant rows to inspect the transactions that add up
                to the category total.
              </div>
            </div>
          </FlowSummaryCard>
        </section>
      </div>
    </AppShell>
  );
}

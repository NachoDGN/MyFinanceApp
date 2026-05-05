import type { ReactNode } from "react";

import type { DomainDataset, PeriodSelection, Transaction } from "@myfinance/domain";

import type { NavigationState } from "../lib/navigation";
import {
  convertBaseEurToDisplayAmountWithFallback,
  endOfMonthIso,
  formatBaseEurAmountForDisplay,
} from "../lib/currency";
import {
  formatDeltaBadge,
  formatMonthLabel,
  formatPercentLabel,
  getPeriodLabel,
} from "../lib/dashboard";
import { formatCurrency, formatDate } from "../lib/formatters";
import {
  createFlowSeriesColorResolver,
  formatFallbackFxRange,
  formatFlowChartRange,
  formatFlowDisplayAmount,
  formatTransactionClassLabel,
  startOfMonthIso,
} from "../lib/flow-page";
import { AppShell } from "./app-shell";
import {
  FlowBreakdownCard,
  FlowKpiGrid,
  FlowPageHeader,
  FlowSummaryCard,
  FlowTrendChart,
} from "./flow-overview";
import { ReviewEditorCell } from "./review-editor-cell";

export type FlowDetailTransaction = Pick<
  Transaction,
  | "id"
  | "accountId"
  | "transactionDate"
  | "amountBaseEur"
  | "transactionClass"
  | "categoryCode"
  | "descriptionRaw"
  | "descriptionClean"
  | "merchantNormalized"
  | "counterpartyName"
  | "needsReview"
  | "reviewReason"
  | "manualNotes"
  | "classificationSource"
  | "quantity"
  | "llmPayload"
  | "creditCardStatementStatus"
>;

type FlowDetailGroupRow<TTransaction extends FlowDetailTransaction> = {
  label: string;
  amountEur: string;
  aliases?: readonly string[];
  transactions?: readonly TTransaction[];
};

type FlowDetailModel<TTransaction extends FlowDetailTransaction> = {
  dataset: DomainDataset;
  category: { categoryCode: string; label: string; amountEur: string };
  scopeOptions: Array<{ value: string; label: string }>;
  navigationState: NavigationState;
  period: PeriodSelection;
  currency: string;
  referenceDate: string;
  amountEur: string;
  comparisonAmountEur: string;
  periodSharePercent: string | null;
  comparisonDeltaPercent: string | null;
  transactionCount: number;
  averageTransactionEur: string;
  largestTransaction: TTransaction | null;
  transactions: readonly TTransaction[];
  monthlySeries: readonly { month: string }[];
};

type FlowCategoryDetailPageProps<TTransaction extends FlowDetailTransaction> = {
  model: FlowDetailModel<TTransaction>;
  categoryCode: string;
  pathnameBase: string;
  groups: readonly FlowDetailGroupRow<TTransaction>[];
  topGroup: FlowDetailGroupRow<TTransaction> | null | undefined;
  labels: {
    watermark: string;
    watermarkClassName?: string;
    titleSuffix: string;
    flowNoun: string;
    flowLogicNoun: string;
    groupNoun: string;
    groupPlural: string;
    chartDescription: string;
    emptyChartLabel: string;
    breakdownTitle: string;
    breakdownDescription: string;
    breakdownHeader: string;
    emptyGroupLabel: string;
    emptyTransactionsLabel: string;
    topGroupStatLabel: string;
    noTopGroupLabel: string;
    transactionSourceDescription: string;
    backHref: string;
    backLabel: string;
  };
  deltaBadgeTone: (value: number) => "accent" | "neutral";
  resolveGroupLabel: (transaction: TTransaction) => string;
  contributionAmountEur: (transaction: TTransaction) => number;
  formatCategoryLabel: (transaction: TTransaction) => string;
  largestTransactionBadge: (transaction: TTransaction | null) => ReactNode;
};

export function FlowCategoryDetailPage<
  TTransaction extends FlowDetailTransaction,
>({
  model,
  categoryCode,
  pathnameBase,
  groups,
  topGroup,
  labels,
  deltaBadgeTone,
  resolveGroupLabel,
  contributionAmountEur,
  formatCategoryLabel,
  largestTransactionBadge,
}: FlowCategoryDetailPageProps<TTransaction>) {
  const category = model.category;
  const categoryAmountNumber = Number(model.amountEur);
  const groupAliasToLabel = new Map<string, string>();
  for (const row of groups) {
    groupAliasToLabel.set(row.label, row.label);
    row.aliases?.forEach((alias) => groupAliasToLabel.set(alias, row.label));
  }
  const transactionsByGroup = model.transactions.reduce((grouped, transaction) => {
    const label =
      groupAliasToLabel.get(resolveGroupLabel(transaction)) ??
      resolveGroupLabel(transaction);
    const existing = grouped.get(label) ?? [];
    existing.push(transaction);
    grouped.set(label, existing);
    return grouped;
  }, new Map<string, TTransaction[]>());
  const resolveGroupColor = createFlowSeriesColorResolver(
    groups.map((row) => row.label),
  );
  const groupRows = groups.map((row) => ({
    ...row,
    color: resolveGroupColor(row.label),
    transactions: transactionsByGroup.get(row.label) ?? [],
  }));
  const groupOrderByLabel = new Map(
    groupRows.map((row, index) => [row.label, index]),
  );
  const monthlyGroupAmounts = model.transactions.reduce((monthGroups, transaction) => {
    const contribution = contributionAmountEur(transaction);
    if (contribution <= 0) {
      return monthGroups;
    }

    const month = startOfMonthIso(transaction.transactionDate);
    const rawLabel = resolveGroupLabel(transaction);
    const label = groupAliasToLabel.get(rawLabel) ?? rawLabel;
    const groupAmounts = monthGroups.get(month) ?? new Map<string, number>();
    groupAmounts.set(label, (groupAmounts.get(label) ?? 0) + contribution);
    monthGroups.set(month, groupAmounts);
    return monthGroups;
  }, new Map<string, Map<string, number>>());
  const chartRows = model.monthlySeries.map((row) => {
    const effectiveDate =
      endOfMonthIso(row.month) <= model.referenceDate
        ? endOfMonthIso(row.month)
        : model.referenceDate;
    let usedFallbackFx = false;
    const buckets = [...(monthlyGroupAmounts.get(row.month) ?? new Map())]
      .sort((left, right) => {
        const leftOrder = groupOrderByLabel.get(left[0]) ?? 9999;
        const rightOrder = groupOrderByLabel.get(right[0]) ?? 9999;
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
          color: resolveGroupColor(label),
          displayAmount: Math.max(Number(displayAmount.amount ?? 0), 0),
          valueLabel: formatCurrency(displayAmount.amount, model.currency),
        };
      });
    const totalDisplay = buckets.reduce(
      (sum, bucket) => sum + bucket.displayAmount,
      0,
    );
    return { ...row, buckets, totalDisplay, usedFallbackFx };
  });
  const fallbackFxRangeLabel = formatFallbackFxRange(
    chartRows.filter((row) => row.usedFallbackFx).map((row) => row.month),
  );
  const chartMax = Math.max(...chartRows.map((row) => row.totalDisplay), 1);
  const formatBaseAmount = (amountEur: string) =>
    formatBaseEurAmountForDisplay(
      model.dataset,
      amountEur,
      model.currency,
      model.referenceDate,
    );
  const categoryAmount = formatBaseAmount(model.amountEur);
  const comparisonAmount = formatBaseAmount(model.comparisonAmountEur);
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
      pathname={`${pathnameBase}/${encodeURIComponent(categoryCode)}`}
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid income-editorial-shell">
        <FlowPageHeader
          watermark={labels.watermark}
          watermarkClassName={labels.watermarkClassName}
          title={`${category.label} ${labels.titleSuffix}`}
          subtitle={
            <>
              Category detail for {getPeriodLabel(model.period).toLowerCase()}.
              The totals, monthly trend, and expanded {labels.groupPlural} use
              the same resolved {labels.flowLogicNoun} logic as the overview
              page.
            </>
          }
          notice={
            <a className="btn-ghost spending-back-link" href={labels.backHref}>
              {labels.backLabel}
            </a>
          }
        />

        <FlowKpiGrid
          items={[
            {
              accent: true,
              title: `Category ${labels.titleSuffix}`,
              icon: <span className="income-kpi-icon">i</span>,
              value: categoryAmount,
              badge: `${formatPercentLabel(model.periodSharePercent)} of ${labels.flowNoun}`,
              badgeTone: "accent",
            },
            {
              title: "Trend vs Prior",
              value: model.comparisonDeltaPercent
                ? formatDeltaBadge(model.comparisonDeltaPercent)
                : "N/A",
              badge: `${comparisonAmount} prior period`,
              badgeTone: deltaBadgeTone(
                Number(model.comparisonDeltaPercent ?? 0),
              ),
            },
            {
              title: "Transactions",
              value: model.transactionCount.toString(),
              badge: `${formatBaseAmount(model.averageTransactionEur)} average`,
            },
            {
              title: "Largest Transaction",
              value: largestTransactionAmount,
              badge: largestTransactionBadge(model.largestTransaction),
            },
          ]}
        />

        <FlowTrendChart
          title={`${category.label} Monthly Trend`}
          description={labels.chartDescription}
          rangeLabel={formatFlowChartRange(chartRows, labels.emptyChartLabel)}
          fallbackFxRangeLabel={fallbackFxRangeLabel}
          currency={model.currency}
          referenceDate={model.referenceDate}
          axisLabels={[1, 0.75, 0.5, 0.25, 0].map((step) =>
            formatCurrency((chartMax * step).toFixed(2), model.currency),
          )}
          rows={chartRows.map((row) => ({
            month: row.month,
            segments: row.buckets.map((bucket) => ({
              color: bucket.color,
              height: (bucket.displayAmount / chartMax) * 100,
              label: bucket.label,
              valueLabel: bucket.valueLabel,
              shareLabel:
                row.totalDisplay > 0
                  ? `${formatPercentLabel(
                      (bucket.displayAmount / row.totalDisplay) * 100,
                    )} of ${formatMonthLabel(row.month)}`
                  : undefined,
            })),
          }))}
          legendItems={groupRows
            .filter((row) => Number(row.amountEur) > 0)
            .map((row) => ({
              label: row.label,
              color: row.color,
              valueLabel: formatBaseAmount(row.amountEur),
              shareLabel:
                categoryAmountNumber > 0
                  ? formatPercentLabel(
                      (Number(row.amountEur) / categoryAmountNumber) * 100,
                    )
                  : undefined,
            }))}
          summary={{
            title: `${category.label} Period ${labels.titleSuffix}`,
            value: categoryAmount,
            badge: `${formatPercentLabel(model.periodSharePercent)} of ${labels.flowNoun}`,
          }}
          emptyLabel={`No ${category.label.toLowerCase()} ${labels.flowNoun} is available for this period.`}
        />

        <section className="income-bottom-grid span-12">
          <FlowBreakdownCard
            title={labels.breakdownTitle}
            periodLabel={getPeriodLabel(model.period)}
            description={labels.breakdownDescription}
            headers={[
              labels.breakdownHeader,
              "Distribution",
              "Volume",
              "Share",
            ]}
          >
            {groupRows.length === 0 ? (
              <div className="table-empty-state">{labels.emptyGroupLabel}</div>
            ) : (
              groupRows.map((row) => {
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
                        {formatBaseAmount(row.amountEur)}
                      </span>
                      <span className="amount">
                        {formatPercentLabel(share)}
                      </span>
                    </summary>
                    <div className="merchant-breakdown-transactions">
                      {row.aliases && row.aliases.length > 1 ? (
                        <div className="muted" style={{ marginBottom: 12 }}>
                          Merged aliases: {row.aliases.join(", ")}
                        </div>
                      ) : null}
                      {row.transactions.length === 0 ? (
                        <div className="table-empty-state">
                          {labels.emptyTransactionsLabel}
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
                                  <span>{formatCategoryLabel(transaction)}</span>
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
              <div className="stat-label">{labels.topGroupStatLabel}</div>
              <div className="spending-summary-value">
                {topGroup?.label ?? "N/A"}
              </div>
              <div className="stat-description">
                {topGroup
                  ? `${formatBaseAmount(topGroup.amountEur)} in ${category.label.toLowerCase()}`
                  : labels.noTopGroupLabel}
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
                {labels.transactionSourceDescription}
              </div>
            </div>
          </FlowSummaryCard>
        </section>
      </div>
    </AppShell>
  );
}

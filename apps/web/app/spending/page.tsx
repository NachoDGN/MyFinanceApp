import { AppShell } from "../../components/app-shell";
import { CreditCardStatementUploadCell } from "../../components/credit-card-statement-upload-cell";
import { FlowCategoryBreakdownList } from "../../components/flow-category-breakdown-list";
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
  convertBaseEurToDisplayAmountWithFallback,
  endOfMonthIso,
  formatBaseEurAmountForDisplay,
} from "../../lib/currency";
import {
  formatDeltaBadge,
  formatMonthLabel,
  formatPercentLabel,
  getPeriodLabel,
} from "../../lib/dashboard";
import { formatCurrency, formatDate } from "../../lib/formatters";
import {
  createFlowSeriesColorResolver,
  formatFallbackFxRange,
  formatFlowChartRange,
  formatFlowDisplayAmount,
  paginateFlowRows,
} from "../../lib/flow-page";
import { getSpendingModel } from "../../lib/queries";

function formatCategoryLabel(
  categoryCode: string | null | undefined,
  transactionClass: string,
  descriptionRaw: string,
  dataset: Awaited<ReturnType<typeof getSpendingModel>>["dataset"],
) {
  if (
    transactionClass === "transfer_internal" &&
    /liquidaci[oó]n.*tarjetas? de cr[eé]dito/i.test(descriptionRaw)
  ) {
    return "Credit Card Payments";
  }

  if (categoryCode) {
    return (
      dataset.categories.find((category) => category.code === categoryCode)
        ?.displayName ?? categoryCode
    );
  }

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

  return "Uncategorized";
}

function formatStatementDateParts(value: string) {
  const date = new Date(`${value}T00:00:00Z`);
  return {
    monthDay: new Intl.DateTimeFormat("en-US", {
      month: "short",
      day: "numeric",
    }).format(date),
    year: new Intl.DateTimeFormat("en-US", {
      year: "numeric",
    }).format(date),
  };
}

export default async function SpendingPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const params = await searchParams;
  const model = await getSpendingModel(params);
  const categoryRows = model.summary.spendingByCategory;
  const ensureCategoryColor = createFlowSeriesColorResolver(
    categoryRows.map((row) => row.categoryCode),
  );
  model.spendingCategoryMonthlySeries.forEach((row) => {
    row.categories.forEach((category) =>
      ensureCategoryColor(category.categoryCode),
    );
  });
  const chartRows = model.spendingCategoryMonthlySeries.map((row) => {
    const effectiveDate =
      endOfMonthIso(row.month) <= model.referenceDate
        ? endOfMonthIso(row.month)
        : model.referenceDate;
    let usedFallbackFx = false;
    const categories = row.categories.map((category) => {
      const displayAmount = convertBaseEurToDisplayAmountWithFallback(
        model.dataset,
        category.amountEur,
        model.currency,
        effectiveDate,
        { fallbackDate: model.referenceDate },
      );
      usedFallbackFx =
        usedFallbackFx ||
        (model.currency !== "EUR" && displayAmount.usedFallbackFx);

      return {
        ...category,
        color: ensureCategoryColor(category.categoryCode),
        displayAmount: Math.max(Number(displayAmount.amount ?? 0), 0),
        valueLabel: formatCurrency(displayAmount.amount, model.currency),
      };
    });
    const spendingDisplay = categories.reduce(
      (sum, category) => sum + category.displayAmount,
      0,
    );

    return {
      ...row,
      categories,
      spendingDisplay,
      usedFallbackFx,
    };
  });
  const fallbackFxRangeLabel = formatFallbackFxRange(
    chartRows.filter((row) => row.usedFallbackFx).map((row) => row.month),
  );
  const chartMax = Math.max(...chartRows.map((row) => row.spendingDisplay), 1);
  const spendTotal = Number(model.spendMetric?.valueBaseEur ?? "0");
  const formatSpendingAmount = (amountEur: string) =>
    formatBaseEurAmountForDisplay(
      model.dataset,
      amountEur,
      model.currency,
      model.referenceDate,
    );
  const chartLegendTotals = [
    ...chartRows
      .flatMap((row) => row.categories)
      .reduce((totals, category) => {
        const current = totals.get(category.categoryCode) ?? {
          label: category.label,
          color: category.color,
          displayAmount: 0,
        };
        current.displayAmount += category.displayAmount;
        totals.set(category.categoryCode, current);
        return totals;
      }, new Map<string, { label: string; color: string; displayAmount: number }>())
      .values(),
  ].sort((left, right) => right.displayAmount - left.displayAmount);
  const chartLegendTotal = chartLegendTotals.reduce(
    (sum, row) => sum + row.displayAmount,
    0,
  );
  const trendRows = chartRows.map((row) => ({
    month: row.month,
    segments: row.categories.map((category) => ({
      color: category.color,
      height: (category.displayAmount / chartMax) * 100,
      label: category.label,
      valueLabel: category.valueLabel,
      shareLabel:
        row.spendingDisplay > 0
          ? `${formatPercentLabel(
              (category.displayAmount / row.spendingDisplay) * 100,
            )} of ${formatMonthLabel(row.month)}`
          : undefined,
    })),
  }));
  const legendItems = chartLegendTotals.map((row) => ({
    label: row.label,
    color: row.color,
    valueLabel: formatCurrency(row.displayAmount.toFixed(2), model.currency),
    shareLabel:
      chartLegendTotal > 0
        ? formatPercentLabel((row.displayAmount / chartLegendTotal) * 100)
        : undefined,
  }));
  const chartAxisValues = [1, 0.75, 0.5, 0.25, 0].map((step) =>
    formatCurrency((chartMax * step).toFixed(2), model.currency),
  );
  const chartRangeLabel = formatFlowChartRange(chartRows, "No spending data");
  const coveragePercent = Number(model.coverage);
  const uncategorizedShare = Math.max(100 - coveragePercent, 0);
  const completenessLabel =
    coveragePercent >= 100
      ? "Fully categorized"
      : `${uncategorizedShare.toFixed(2)}% uncategorized`;
  const topCategoryShare =
    model.topCategory && spendTotal > 0
      ? (Number(model.topCategory.amountEur) / spendTotal) * 100
      : 0;
  const topMerchantShare =
    model.topMerchant && spendTotal > 0
      ? (Number(model.topMerchant.amountEur) / spendTotal) * 100
      : 0;
  const creditCardTemplates = model.dataset.templates
    .filter((template) => template.compatibleAccountType === "credit_card")
    .map((template) => ({ id: template.id, name: template.name }));
  const importBatchBySettlementId = new Map(
    model.dataset.importBatches
      .filter((batch) => batch.creditCardSettlementTransactionId)
      .map((batch) => [batch.creditCardSettlementTransactionId!, batch]),
  );
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
  const largestTransactions = [...model.transactions]
    .sort(
      (left, right) =>
        Math.abs(Number(right.amountBaseEur)) -
        Math.abs(Number(left.amountBaseEur)),
    )
    .slice(0, 12);
  const scopeDescription =
    model.scope.kind === "consolidated"
      ? "Global view across your personal and company entities. Use the scope pills above to isolate any one of them without losing the consolidated total."
      : "Scoped view of the selected entity. Switch back to the consolidated pill above to see the full outflow picture that rolls everything up together.";

  return (
    <AppShell
      pathname="/spending"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
      pageQueryParams={{ categoryPage: String(categoryPage) }}
    >
      <div className="dashboard-grid income-editorial-shell">
        <FlowPageHeader
          watermark="SPENDING"
          watermarkClassName="spending-editorial-watermark"
          title="Spending Overview"
          subtitle={
            <>
              Primary spend KPIs exclude internal transfers and defer
              credit-card settlement liquidations until the matching card
              statement ledger is imported, so the dashboard reflects real
              merchant outflows instead of duplicate settlement payments.{" "}
              {scopeDescription}
            </>
          }
          notice={
            model.excludedCreditCardSettlementCount > 0 ? (
              <div className="status-note">
                Excluded {model.excludedCreditCardSettlementCount} credit-card
                settlement payment
                {model.excludedCreditCardSettlementCount === 1 ? "" : "s"}{" "}
                totaling{" "}
                {formatSpendingAmount(
                  model.excludedCreditCardSettlementAmountEur,
                )}
                . Their underlying card purchases stay out of the KPI layer
                until the matching statement is uploaded against the settlement
                row.
              </div>
            ) : null
          }
        />

        <FlowKpiGrid
          items={[
            {
              accent: true,
              title:
                model.period.preset === "ytd"
                  ? "Current-Year Spend"
                  : "Current-Period Spend",
              icon: <span className="income-kpi-icon">i</span>,
              value: formatCurrency(
                model.spendMetric?.valueDisplay,
                model.currency,
              ),
              badge: `${formatDeltaBadge(model.spendMetric?.deltaPercent)} vs prior`,
              badgeTone: "accent",
            },
            {
              title: "Trailing 3-Month Avg",
              value: formatSpendingAmount(model.trailingThreeMonthAverage),
              badge: "Average baseline",
            },
            {
              title: "Top Category Concentration",
              value: model.topCategory
                ? formatPercentLabel(topCategoryShare)
                : "N/A",
              badge: model.topCategory?.label ?? "No categorized spend",
            },
            {
              accent: true,
              title: "Coverage / Completeness",
              value: Number.isFinite(coveragePercent)
                ? `${coveragePercent.toFixed(0)}%`
                : "N/A",
              badge: completenessLabel,
              badgeTone: coveragePercent >= 100 ? "accent" : "neutral",
            },
          ]}
        />

        <FlowTrendChart
          title="Monthly Spend Trend"
          description="Category-level composition for each month in the selected range."
          rangeLabel={chartRangeLabel}
          fallbackFxRangeLabel={fallbackFxRangeLabel}
          currency={model.currency}
          referenceDate={model.referenceDate}
          axisLabels={chartAxisValues}
          rows={trendRows}
          legendItems={legendItems}
          summary={{
            title: "Total Period Spend",
            value: formatCurrency(
              model.spendMetric?.valueDisplay,
              model.currency,
            ),
            badge: `${formatDeltaBadge(model.spendMetric?.deltaPercent)} vs prior`,
          }}
          emptyLabel="No spending data is available for this period."
        />

        <section className="income-bottom-grid span-12">
          <FlowBreakdownCard
            title="Spending Category Breakdown"
            periodLabel={getPeriodLabel(model.period)}
            description="Resolved spending categories for the selected period, ordered by current-period share."
            headers={["Category", "Distribution", "Volume", "Share"]}
          >
            <FlowCategoryBreakdownList
              basePath="/spending"
              rows={categoryRows}
              visibleRows={visibleCategoryRows}
              totalAmount={spendTotal}
              navigationState={model.navigationState}
              emptyLabel="No resolved spending categories are available for this period."
              page={categoryPage}
              pageCount={categoryPageCount}
              rangeLabel={categoryRangeLabel}
              hasMultiplePages={hasMultipleCategoryPages}
              formatAmount={formatSpendingAmount}
            />
          </FlowBreakdownCard>

          <FlowSummaryCard>
            <div className="income-summary-stat">
              <div className="stat-label">Top Category</div>
              <div className="spending-summary-value">
                {model.topCategory?.label ?? "N/A"}
              </div>
              <div className="stat-description">
                {model.topCategory
                  ? `${formatSpendingAmount(model.topCategory.amountEur)} · ${topCategoryShare.toFixed(0)}% of current-period spend`
                  : "No categorized spend available."}
              </div>
            </div>

            <div className="income-summary-stat">
              <div className="stat-label">Top Merchant Bucket</div>
              <div className="spending-summary-value">
                {model.topMerchant?.label ?? "N/A"}
              </div>
              <div className="stat-description">
                {model.topMerchant
                  ? `${formatSpendingAmount(model.topMerchant.amountEur)} · ${topMerchantShare.toFixed(0)}% of current-period spend`
                  : "No merchant totals available."}
              </div>
            </div>

            <div className="income-summary-stat">
              <div className="stat-label">Uncategorized Spend</div>
              <div className="stat-value accent">
                {formatSpendingAmount(model.uncategorizedSpendEur)}
              </div>
              <div className="stat-description">
                {spendTotal > 0
                  ? `${formatPercentLabel(uncategorizedShare)} of current-period spend remains uncategorized or in proxy buckets.`
                  : "No current-period spend available."}
              </div>
            </div>
          </FlowSummaryCard>
        </section>

        <section className="income-chart-card spending-income-full-width">
          <div className="income-chart-header">
            <div>
              <h2 className="income-chart-title">
                Largest Current-Period Merchant Buckets
              </h2>
            </div>
          </div>
          <div className="spending-merchant-list">
            {model.merchantRows.slice(0, 6).map((row, index) => (
              <div
                className="spending-merchant-row"
                key={`${row.label}-${index}`}
              >
                <div className="spending-merchant-icon">
                  {row.label.slice(0, 1).toUpperCase()}
                </div>
                <div className="spending-merchant-copy">
                  <p className="spending-merchant-name">{row.label}</p>
                  <p className="spending-merchant-meta">
                    {spendTotal > 0
                      ? `${((Number(row.amountEur) / spendTotal) * 100).toFixed(0)}% of spend`
                      : "No share available"}
                  </p>
                </div>
                <div className="spending-merchant-amount">
                  {formatSpendingAmount(row.amountEur)}
                </div>
              </div>
            ))}
            {model.merchantRows.length === 0 ? (
              <div className="spending-empty-state">
                No merchant totals are available for the selected period.
              </div>
            ) : null}
          </div>
        </section>

        {model.creditCardSettlementRows.length > 0 ? (
          <section className="statement-resolution-card spending-income-full-width">
            <div className="statement-resolution-header">
              <div>
                <span className="statement-resolution-kicker">
                  Statement Resolution
                </span>
                <h2 className="statement-resolution-title">
                  Credit-card settlement rows awaiting statement upload
                </h2>
              </div>
            </div>
            <div className="statement-resolution-column-headings">
              <div>Date</div>
              <div>Account</div>
              <div>Description</div>
              <div className="statement-resolution-amount-heading">Payment</div>
              <div>Statement</div>
              <div>Review</div>
            </div>
            <div className="statement-resolution-list">
              {model.creditCardSettlementRows.map((row) => {
                const statementDate = formatStatementDateParts(
                  row.transactionDate,
                );
                const accountName =
                  model.dataset.accounts.find(
                    (account) => account.id === row.accountId,
                  )?.displayName ?? row.accountId;

                return (
                  <div className="statement-resolution-row" key={row.id}>
                    <div className="statement-resolution-cell">
                      <span className="statement-resolution-mobile-label">
                        Date
                      </span>
                      <div className="statement-resolution-date">
                        <span>{statementDate.monthDay}</span>
                        <span>{statementDate.year}</span>
                      </div>
                    </div>

                    <div className="statement-resolution-cell">
                      <span className="statement-resolution-mobile-label">
                        Account
                      </span>
                      <div className="statement-resolution-text">
                        {accountName}
                      </div>
                    </div>

                    <div className="statement-resolution-cell">
                      <span className="statement-resolution-mobile-label">
                        Description
                      </span>
                      <div className="statement-resolution-text statement-resolution-description">
                        {row.descriptionRaw}
                      </div>
                    </div>

                    <div className="statement-resolution-cell">
                      <span className="statement-resolution-mobile-label">
                        Payment
                      </span>
                      <div className="statement-resolution-amount">
                        {formatFlowDisplayAmount(
                          model.dataset,
                          row.amountBaseEur,
                          model.currency,
                          row.transactionDate,
                        )}
                      </div>
                    </div>

                    <div className="statement-resolution-cell">
                      <span className="statement-resolution-mobile-label">
                        Statement
                      </span>
                      <CreditCardStatementUploadCell
                        settlementTransactionId={row.id}
                        statementStatus={row.creditCardStatementStatus}
                        linkedCreditCardAccountName={
                          model.dataset.accounts.find(
                            (account) =>
                              account.id === row.linkedCreditCardAccountId,
                          )?.displayName ?? null
                        }
                        linkedImportFilename={
                          importBatchBySettlementId.get(row.id)
                            ?.originalFilename ?? null
                        }
                        linkedImportBatchId={
                          importBatchBySettlementId.get(row.id)?.id ?? null
                        }
                        templateOptions={creditCardTemplates}
                        variant="statement"
                      />
                    </div>

                    <div className="statement-resolution-cell">
                      <span className="statement-resolution-mobile-label">
                        Review
                      </span>
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
                        creditCardStatementStatus={
                          row.creditCardStatementStatus
                        }
                        descriptionRaw={row.descriptionRaw}
                        descriptionClean={row.descriptionClean}
                        variant="statement"
                      />
                    </div>
                  </div>
                );
              })}
            </div>
          </section>
        ) : null}

        <SimpleTable
          span="span-12"
          headers={[
            "Date",
            "Account",
            "Description",
            "Merchant",
            "Class",
            "Category",
            "Amount",
            "Review",
          ]}
          rows={largestTransactions.map((row) => [
            formatDate(row.transactionDate),
            model.dataset.accounts.find(
              (account) => account.id === row.accountId,
            )?.displayName ?? row.accountId,
            row.descriptionRaw,
            row.merchantNormalized ?? row.counterpartyName ?? "—",
            row.transactionClass.replace(/_/g, " "),
            formatCategoryLabel(
              row.categoryCode,
              row.transactionClass,
              row.descriptionRaw,
              model.dataset,
            ),
            formatFlowDisplayAmount(
              model.dataset,
              row.amountBaseEur,
              model.currency,
              row.transactionDate,
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
              creditCardStatementStatus={row.creditCardStatementStatus}
              descriptionRaw={row.descriptionRaw}
              descriptionClean={row.descriptionClean}
            />,
          ])}
        />
      </div>
    </AppShell>
  );
}

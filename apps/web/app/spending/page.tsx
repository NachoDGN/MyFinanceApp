import { AppShell } from "../../components/app-shell";
import { CreditCardStatementUploadCell } from "../../components/credit-card-statement-upload-cell";
import { SimpleTable } from "../../components/primitives";
import { ReviewEditorCell } from "../../components/review-editor-cell";
import {
  convertBaseEurToDisplayAmount,
  formatBaseEurAmountForDisplay,
} from "../../lib/currency";
import {
  buildAreaPath,
  buildLinePath,
  buildTrendPoints,
  formatDeltaBadge,
  formatMonthLabel,
} from "../../lib/dashboard";
import { formatCurrency, formatDate } from "../../lib/formatters";
import { getSpendingModel } from "../../lib/queries";

function formatDisplayAmount(
  amountBaseEur: string | null | undefined,
  currency: string,
  transactionDate: string,
  dataset: Awaited<ReturnType<typeof getSpendingModel>>["dataset"],
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

function describePeriodPill(period: string) {
  if (period === "ytd") return "Year to date";
  if (period === "custom") return "Custom range";
  return "Month to date";
}

function formatPercentLabel(value: number) {
  if (!Number.isFinite(value)) {
    return "0.00%";
  }

  return `${value.toFixed(2)}%`;
}

export default async function SpendingPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getSpendingModel(searchParams);
  const trendRows = model.trendSeries;
  const chartWidth = 920;
  const chartHeight = 260;
  const trendValues = trendRows.map((row) =>
    Number(
      convertBaseEurToDisplayAmount(
        model.dataset,
        row.spendingEur,
        model.currency,
        model.referenceDate,
      ) ?? row.spendingEur,
    ),
  );
  const chartPoints = buildTrendPoints(trendValues, chartWidth, chartHeight);
  const spendTotal = Number(model.spendMetric?.valueBaseEur ?? "0");
  const coveragePercent = Number(model.coverage);
  const uncategorizedShare = Math.max(100 - coveragePercent, 0);
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
  const largestTransactions = [...model.transactions]
    .sort(
      (left, right) =>
        Math.abs(Number(right.amountBaseEur)) -
        Math.abs(Number(left.amountBaseEur)),
    )
    .slice(0, 12);

  return (
    <AppShell
      pathname="/spending"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid income-editorial-shell">
        <div className="income-editorial-watermark">SPEND</div>

        <div className="income-page-header span-12">
          <div>
            <h1 className="page-title">Spending Overview</h1>
            <p className="page-subtitle">
              Personal cash spending stays personal. Company cash spending stays
              company. Credit-card statement liquidations are excluded from
              spend until the related card ledger is imported, because they
              represent prior-period purchases rather than fresh April spending.
            </p>
            {model.excludedCreditCardSettlementCount > 0 ? (
              <div className="status-note">
                Excluded {model.excludedCreditCardSettlementCount} credit-card
                settlement payment
                {model.excludedCreditCardSettlementCount === 1 ? "" : "s"}{" "}
                totaling{" "}
                {formatBaseEurAmountForDisplay(
                  model.dataset,
                  model.excludedCreditCardSettlementAmountEur,
                  model.currency,
                  model.referenceDate,
                )}
                . Their underlying card purchases stay out of the KPI layer
                until the matching statement is uploaded against the settlement
                row.
              </div>
            ) : null}
          </div>
        </div>

        <div className="income-kpi-grid span-12">
          <article className="income-kpi-card income-kpi-card-accent">
            <div className="income-kpi-title">
              <span>Current-Period Spend</span>
              <span className="income-kpi-icon">i</span>
            </div>
            <div className="income-kpi-value">
              {formatCurrency(model.spendMetric?.valueDisplay, model.currency)}
            </div>
            <div className="income-kpi-badge accent">
              {formatDeltaBadge(model.spendMetric?.deltaPercent)} vs prior
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
              <span>Top Category Concentration</span>
            </div>
            <div className="income-kpi-value">
              {model.topCategory ? formatPercentLabel(topCategoryShare) : "N/A"}
            </div>
            <div className="income-kpi-badge neutral">
              {model.topCategory?.label ?? "No categorized spend"}
            </div>
          </article>

          <article className="income-kpi-card income-kpi-card-accent">
            <div className="income-kpi-title">
              <span>Categorized Spend Share</span>
            </div>
            <div className="income-kpi-value">
              {Number.isFinite(coveragePercent)
                ? `${coveragePercent.toFixed(0)}%`
                : "N/A"}
            </div>
            <div className="income-kpi-badge neutral">
              {formatBaseEurAmountForDisplay(
                model.dataset,
                model.uncategorizedSpendEur,
                model.currency,
                model.referenceDate,
              )}{" "}
              uncategorized
            </div>
          </article>
        </div>

        <section className="income-chart-card span-12">
          <div className="income-chart-header">
            <div>
              <h2 className="income-chart-title">Monthly Spend Trend</h2>
            </div>
            <div className="income-kpi-badge neutral">
              {trendRows.length > 0
                ? `Trend through ${formatDate(trendRows[trendRows.length - 1]!.month)}`
                : "Trend unavailable"}
            </div>
          </div>

          <div className="spending-trend-chart spending-trend-chart-editorial">
            <svg
              viewBox={`0 0 ${chartWidth} ${chartHeight + 40}`}
              preserveAspectRatio="none"
            >
              <defs>
                <linearGradient
                  id="spendingAreaGradient"
                  x1="0%"
                  y1="0%"
                  x2="0%"
                  y2="100%"
                >
                  <stop offset="0%" stopColor="rgba(255,75,43,0.34)" />
                  <stop offset="100%" stopColor="rgba(255,75,43,0)" />
                </linearGradient>
              </defs>
              {[0.2, 0.4, 0.6, 0.8].map((ratio) => (
                <line
                  key={ratio}
                  x1="0"
                  x2={chartWidth}
                  y1={chartHeight * ratio}
                  y2={chartHeight * ratio}
                  className="spending-grid-line"
                />
              ))}
              <path
                d={buildAreaPath(chartPoints, chartWidth, chartHeight)}
                className="spending-area-path"
              />
              <path
                d={buildLinePath(chartPoints)}
                className="spending-line-path"
              />
              {chartPoints.map((point, index) => (
                <circle
                  key={`${trendRows[index]?.month ?? index}`}
                  cx={point.x}
                  cy={point.y}
                  r="5"
                  className="spending-line-dot"
                />
              ))}
            </svg>
            <div className="spending-trend-labels">
              {trendRows.map((row) => (
                <span key={row.month}>{formatMonthLabel(row.month)}</span>
              ))}
            </div>
          </div>
        </section>

        <section className="income-bottom-grid span-12">
          <article className="income-breakdown-card">
            <div className="income-chart-header">
              <h2 className="income-chart-title">
                Spending Category Breakdown
              </h2>
              <div className="income-kpi-badge neutral">
                {describePeriodPill(model.navigationState.period)}
              </div>
            </div>
            <div className="muted" style={{ marginTop: 8, lineHeight: 1.5 }}>
              Resolved spending categories for the selected period, ordered by
              current-period share.
            </div>

            <div className="income-breakdown-table">
              <div className="income-breakdown-head">
                <div>Category</div>
                <div>Distribution</div>
                <div className="amount">Volume</div>
                <div className="amount">Share</div>
              </div>
              {model.summary.spendingByCategory.length === 0 ? (
                <div className="table-empty-state">
                  No resolved spending categories are available for this period.
                </div>
              ) : (
                model.summary.spendingByCategory.slice(0, 6).map((row) => {
                  const share =
                    spendTotal > 0
                      ? (Number(row.amountEur) / spendTotal) * 100
                      : 0;

                  return (
                    <div
                      className="income-breakdown-row"
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
                        {formatBaseEurAmountForDisplay(
                          model.dataset,
                          row.amountEur,
                          model.currency,
                          model.referenceDate,
                        )}
                      </div>
                      <div className="amount">{formatPercentLabel(share)}</div>
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
              <div className="stat-label">Top Category</div>
              <div className="spending-summary-value">
                {model.topCategory?.label ?? "N/A"}
              </div>
              <div className="stat-description">
                {model.topCategory
                  ? `${formatBaseEurAmountForDisplay(
                      model.dataset,
                      model.topCategory.amountEur,
                      model.currency,
                      model.referenceDate,
                    )} · ${topCategoryShare.toFixed(0)}% of current-period spend`
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
                  ? `${formatBaseEurAmountForDisplay(
                      model.dataset,
                      model.topMerchant.amountEur,
                      model.currency,
                      model.referenceDate,
                    )} · ${topMerchantShare.toFixed(0)}% of current-period spend`
                  : "No merchant totals available."}
              </div>
            </div>

            <div className="income-summary-stat">
              <div className="stat-label">Uncategorized Spend</div>
              <div className="stat-value accent">
                {formatBaseEurAmountForDisplay(
                  model.dataset,
                  model.uncategorizedSpendEur,
                  model.currency,
                  model.referenceDate,
                )}
              </div>
              <div className="stat-description">
                {spendTotal > 0
                  ? `${formatPercentLabel(uncategorizedShare)} of current-period spend remains uncategorized or in proxy buckets.`
                  : "No current-period spend available."}
              </div>
            </div>
          </article>
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
                  {formatBaseEurAmountForDisplay(
                    model.dataset,
                    row.amountEur,
                    model.currency,
                    model.referenceDate,
                  )}
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
                        {formatDisplayAmount(
                          row.amountBaseEur,
                          model.currency,
                          row.transactionDate,
                          model.dataset,
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
            formatDisplayAmount(
              row.amountBaseEur,
              model.currency,
              row.transactionDate,
              model.dataset,
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

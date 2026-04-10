import { AppShell } from "../../components/app-shell";
import { CreditCardStatementUploadCell } from "../../components/credit-card-statement-upload-cell";
import { ReviewEditorCell } from "../../components/review-editor-cell";
import {
  formatCurrency,
  formatDate,
  getSpendingModel,
} from "../../lib/queries";
import { convertBaseEurToDisplayAmount } from "../../lib/currency";

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

function buildTrendPoints(values: number[], width: number, height: number) {
  const safeValues = values.length > 0 ? values : [0];
  const max = Math.max(...safeValues, 1);

  return safeValues.map((value, index) => {
    const x =
      safeValues.length === 1
        ? width / 2
        : (index / (safeValues.length - 1)) * width;
    const y = height - (Math.max(value, 0) / max) * height;
    return { x, y };
  });
}

function buildLinePath(points: Array<{ x: number; y: number }>) {
  if (points.length === 0) return "";
  return points
    .map(
      (point, index) =>
        `${index === 0 ? "M" : "L"} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`,
    )
    .join(" ");
}

function buildAreaPath(
  points: Array<{ x: number; y: number }>,
  width: number,
  height: number,
) {
  if (points.length === 0) return "";
  const line = buildLinePath(points);
  return `${line} L ${width} ${height} L 0 ${height} Z`;
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

function formatDeltaBadge(deltaPercent: string | null | undefined) {
  if (!deltaPercent) {
    return "0.00%";
  }

  const numeric = Number(deltaPercent);
  if (!Number.isFinite(numeric)) {
    return "0.00%";
  }

  return `${numeric.toFixed(2)}%`;
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
  const model = await getSpendingModel(searchParams);
  const trendRows = model.trendSeries;
  const chartWidth = 920;
  const chartHeight = 260;
  const chartPoints = buildTrendPoints(
    trendRows.map((row) => Number(row.spendingEur)),
    chartWidth,
    chartHeight,
  );
  const spendTotal = Number(model.spendMetric?.valueBaseEur ?? "0");
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
      <div className="spending-page">
        <section className="spending-hero">
          <div className="spending-hero-copy">
            <span className="spending-kicker">Spending overview</span>
            <h1 className="spending-title">
              Cash outflows, merchant concentration, and category pressure
            </h1>
            <p className="spending-subtitle">
              Personal cash spending stays personal. Company cash spending stays
              company. Credit-card statement liquidations are excluded from
              spend until the related card ledger is imported, because they
              represent prior-period purchases rather than fresh April spending.
            </p>
            {model.excludedCreditCardSettlementCount > 0 ? (
              <div className="spending-context-note">
                Excluded {model.excludedCreditCardSettlementCount} credit-card
                settlement payment
                {model.excludedCreditCardSettlementCount === 1 ? "" : "s"}{" "}
                totaling{" "}
                {formatCurrency(
                  model.excludedCreditCardSettlementAmountEur,
                  model.currency,
                )}
                . Their underlying card purchases stay out of the KPI layer
                until the matching statement is uploaded against the settlement
                row.
              </div>
            ) : null}
          </div>
          <div className="spending-hero-meta">
            <span className="spending-hero-pill">
              {model.navigationState.period === "ytd"
                ? "Year to date"
                : "Month to date"}
            </span>
            <span className="spending-hero-note">
              {trendRows.length > 0
                ? `Trend through ${formatDate(trendRows[trendRows.length - 1]!.month)}`
                : "Trend unavailable"}
            </span>
          </div>
        </section>

        {model.creditCardSettlementRows.length > 0 ? (
          <section className="statement-resolution-card">
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

        <div className="spending-layout">
          <aside className="spending-sidebar">
            <article className="spending-primary-card">
              <div className="spending-primary-header">
                <span className="spending-card-label">Current period</span>
                <span className="spending-card-badge">
                  {formatDeltaBadge(model.spendMetric?.deltaPercent)}
                </span>
              </div>
              <div className="spending-primary-value">
                {formatCurrency(
                  model.spendMetric?.valueDisplay,
                  model.currency,
                )}
              </div>
              <div className="spending-primary-footer">
                <span>
                  {formatCurrency(
                    model.spendMetric?.deltaDisplay,
                    model.currency,
                  )}{" "}
                  from prior pace
                </span>
              </div>
            </article>

            <article className="spending-coverage-card">
              <div className="spending-card-header">
                <div>
                  <span className="spending-card-label">Coverage</span>
                  <h2 className="spending-card-title">
                    Categorized spend share
                  </h2>
                </div>
                <span className="spending-inline-pill">
                  {formatCurrency(model.uncategorizedSpendEur, model.currency)}
                </span>
              </div>
              <div className="spending-coverage-value">{model.coverage}%</div>
              <p className="spending-card-note">
                Amount still sitting in uncategorized or proxy buckets this
                period.
              </p>
            </article>

            <article className="spending-breakdown-card">
              <div className="spending-card-header">
                <div>
                  <span className="spending-card-label">Distribution</span>
                  <h2 className="spending-card-title">
                    Top spending categories
                  </h2>
                </div>
              </div>
              <div className="spending-breakdown-list">
                {model.summary.spendingByCategory.slice(0, 6).map((row) => {
                  const share =
                    spendTotal > 0
                      ? (Number(row.amountEur) / spendTotal) * 100
                      : 0;
                  return (
                    <div
                      className="spending-breakdown-row"
                      key={row.categoryCode}
                    >
                      <div className="spending-breakdown-head">
                        <div>
                          <div className="spending-breakdown-label">
                            {row.label}
                          </div>
                          <div className="spending-breakdown-share">
                            {share.toFixed(0)}% of total
                          </div>
                        </div>
                        <span className="spending-breakdown-amount">
                          {formatCurrency(row.amountEur, model.currency)}
                        </span>
                      </div>
                      <div className="spending-breakdown-track">
                        <div
                          className="spending-breakdown-fill"
                          style={{
                            width: `${Math.min(share, 100).toFixed(2)}%`,
                          }}
                        />
                      </div>
                    </div>
                  );
                })}
                {model.summary.spendingByCategory.length === 0 ? (
                  <div className="spending-empty-state">
                    No resolved spending categories are available for the
                    selected scope.
                  </div>
                ) : null}
              </div>
            </article>
          </aside>

          <div className="spending-main">
            <article className="spending-trend-card">
              <div className="spending-card-header">
                <div>
                  <span className="spending-card-label accent">
                    Trend analysis
                  </span>
                  <h2 className="spending-trend-title">Monthly spend trend</h2>
                </div>
                <span className="spending-inline-pill">
                  {trendRows.length} months
                </span>
              </div>
              <div className="spending-trend-chart">
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
                    <span key={row.month}>
                      {new Intl.DateTimeFormat("en-US", {
                        month: "short",
                      }).format(new Date(`${row.month}T00:00:00Z`))}
                    </span>
                  ))}
                </div>
              </div>
            </article>

            <div className="spending-stat-grid">
              <article className="spending-stat-card">
                <span className="spending-card-label">Trailing 3-mo avg</span>
                <div className="spending-stat-value">
                  {formatCurrency(
                    model.trailingThreeMonthAverage,
                    model.currency,
                  )}
                </div>
                <p className="spending-card-note">
                  Average monthly spend based on the latest trend window.
                </p>
              </article>

              <article className="spending-stat-card">
                <span className="spending-card-label">Top category</span>
                <div className="spending-stat-value">
                  {model.topCategory?.label ?? "N/A"}
                </div>
                <p className="spending-card-note">
                  {model.topCategory
                    ? `${formatCurrency(model.topCategory.amountEur, model.currency)} · ${topCategoryShare.toFixed(0)}% of current-period spend`
                    : "No categorized spend available."}
                </p>
              </article>

              <article className="spending-stat-card">
                <span className="spending-card-label">Top merchant bucket</span>
                <div className="spending-stat-value">
                  {model.topMerchant?.label ?? "N/A"}
                </div>
                <p className="spending-card-note">
                  {model.topMerchant
                    ? `${formatCurrency(model.topMerchant.amountEur, model.currency)} · ${topMerchantShare.toFixed(0)}% of current-period spend`
                    : "No merchant totals available."}
                </p>
              </article>
            </div>

            <article className="spending-merchants-card">
              <div className="spending-card-header">
                <div>
                  <span className="spending-card-label">Detailed view</span>
                  <h2 className="spending-card-title">
                    Largest current-period merchant buckets
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
                      {formatCurrency(row.amountEur, model.currency)}
                    </div>
                  </div>
                ))}
                {model.merchantRows.length === 0 ? (
                  <div className="spending-empty-state">
                    No merchant totals are available for the selected period.
                  </div>
                ) : null}
              </div>
            </article>
          </div>
        </div>

        <section className="spending-transactions-card">
          <div className="spending-card-header">
            <div>
              <span className="spending-card-label">Ledger detail</span>
              <h2 className="spending-card-title">
                Largest current-period outflows
              </h2>
            </div>
          </div>
          <div className="table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Account</th>
                  <th>Description</th>
                  <th>Merchant</th>
                  <th>Class</th>
                  <th>Category</th>
                  <th>Amount</th>
                  <th>Review</th>
                </tr>
              </thead>
              <tbody>
                {largestTransactions.map((row) => (
                  <tr key={row.id}>
                    <td>{formatDate(row.transactionDate)}</td>
                    <td>
                      {model.dataset.accounts.find(
                        (account) => account.id === row.accountId,
                      )?.displayName ?? row.accountId}
                    </td>
                    <td>{row.descriptionRaw}</td>
                    <td>
                      {row.merchantNormalized ?? row.counterpartyName ?? "—"}
                    </td>
                    <td>{row.transactionClass.replace(/_/g, " ")}</td>
                    <td>
                      {formatCategoryLabel(
                        row.categoryCode,
                        row.transactionClass,
                        row.descriptionRaw,
                        model.dataset,
                      )}
                    </td>
                    <td>
                      {formatDisplayAmount(
                        row.amountBaseEur,
                        model.currency,
                        row.transactionDate,
                        model.dataset,
                      )}
                    </td>
                    <td>
                      <ReviewEditorCell
                        transactionId={row.id}
                        needsReview={row.needsReview}
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
                      />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </AppShell>
  );
}

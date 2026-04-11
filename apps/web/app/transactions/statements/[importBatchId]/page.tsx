import { notFound } from "next/navigation";

import { AppShell } from "../../../../components/app-shell";
import { ReviewEditorCell } from "../../../../components/review-editor-cell";
import {
  buildHref,
  formatCurrency,
  formatDate,
  getCreditCardStatementModel,
} from "../../../../lib/queries";
import { convertBaseEurToDisplayAmount } from "../../../../lib/currency";

function formatDisplayAmount(
  amountBaseEur: string | null | undefined,
  currency: string,
  transactionDate: string,
  dataset: Awaited<ReturnType<typeof getCreditCardStatementModel>>["dataset"],
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

export default async function CreditCardStatementPage({
  params,
  searchParams,
}: {
  params: Promise<{ importBatchId: string }>;
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const { importBatchId } = await params;
  const model = await getCreditCardStatementModel(searchParams, importBatchId);

  if (!model.importBatch) {
    notFound();
  }

  const queuedJobs = model.jobs.filter((job) => job.status === "queued").length;
  const runningJobs = model.jobs.filter(
    (job) => job.status === "running",
  ).length;
  const failedJobs = model.jobs.filter((job) => job.status === "failed").length;
  const backHref = buildHref("/transactions", model.navigationState, {});

  return (
    <AppShell
      pathname={`/transactions/statements/${importBatchId}`}
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid">
        <section className="section-card span-12">
          <div className="section-header">
            <div>
              <span className="label-sm">Statement Inspection</span>
              <h1 className="section-title">
                {model.importBatch.originalFilename}
              </h1>
            </div>
            <a className="btn-ghost" href={backHref}>
              Back to Transactions
            </a>
          </div>
          <div className="statement-detail-metrics">
            <div className="statement-detail-metric">
              <span className="label-sm">Linked Card Account</span>
              <strong>
                {model.linkedAccount?.displayName ?? "Unknown account"}
              </strong>
            </div>
            <div className="statement-detail-metric">
              <span className="label-sm">Validated Net Total</span>
              <strong>
                {formatDisplayAmount(
                  model.importBatch.statementNetAmountBaseEur,
                  model.currency,
                  model.settlementTransaction?.transactionDate ??
                    model.referenceDate,
                  model.dataset,
                )}
              </strong>
            </div>
            <div className="statement-detail-metric">
              <span className="label-sm">Rows</span>
              <strong>
                {model.importBatch.rowCountInserted} imported /{" "}
                {model.importBatch.rowCountParsed} parsed
              </strong>
            </div>
            <div className="statement-detail-metric">
              <span className="label-sm">Review Progress</span>
              <strong>
                {model.statementTransactions.length - model.unresolvedCount}{" "}
                resolved / {model.unresolvedCount} pending review
              </strong>
            </div>
            <div className="statement-detail-metric">
              <span className="label-sm">Pipeline</span>
              <strong>
                {runningJobs > 0
                  ? `${runningJobs} running`
                  : queuedJobs > 0
                    ? `${queuedJobs} queued`
                    : failedJobs > 0
                      ? `${failedJobs} failed`
                      : "No pending jobs"}
              </strong>
            </div>
            <div className="statement-detail-metric">
              <span className="label-sm">Settlement Row</span>
              <strong>
                {model.settlementTransaction
                  ? `${formatDate(model.settlementTransaction.transactionDate)} · ${formatDisplayAmount(
                      model.settlementTransaction.amountBaseEur,
                      model.currency,
                      model.settlementTransaction.transactionDate,
                      model.dataset,
                    )}`
                  : "Unavailable"}
              </strong>
            </div>
          </div>
          <p className="statement-detail-note">
            These statement rows inherit the same reviewer pipeline as any other
            transaction, but they remain linked to the settlement payment so KPI
            totals do not double count the monthly card liquidation.
          </p>
        </section>

        <section className="section-card span-12">
          <div className="section-header">
            <div>
              <span className="label-sm">Statement Rows</span>
              <h2 className="section-title">
                Imported credit-card transactions
              </h2>
            </div>
          </div>
          <div className="statement-inspection-table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Description</th>
                  <th>Amount</th>
                  <th>Class</th>
                  <th>Category</th>
                  <th>Review</th>
                </tr>
              </thead>
              <tbody>
                {model.statementTransactions.map((row) => {
                  const categoryLabel = row.categoryCode
                    ? (model.dataset.categories.find(
                        (category) => category.code === row.categoryCode,
                      )?.displayName ?? row.categoryCode)
                    : "—";

                  return (
                    <tr key={row.id}>
                      <td>{formatDate(row.transactionDate)}</td>
                      <td>{row.descriptionRaw}</td>
                      <td>
                        {formatDisplayAmount(
                          row.amountBaseEur,
                          model.currency,
                          row.transactionDate,
                          model.dataset,
                        )}
                      </td>
                      <td>{row.transactionClass}</td>
                      <td>{categoryLabel}</td>
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
                  );
                })}
                {model.statementTransactions.length === 0 ? (
                  <tr>
                    <td className="statement-table-empty" colSpan={6}>
                      No parsed statement rows were found in this import batch.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </section>
      </div>
    </AppShell>
  );
}

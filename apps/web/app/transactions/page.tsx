import { AppShell } from "../../components/app-shell";
import { CreditCardStatementUploadCell } from "../../components/credit-card-statement-upload-cell";
import { SectionCard, SimpleTable } from "../../components/primitives";
import { ReviewEditorCell } from "../../components/review-editor-cell";
import { convertBaseEurToDisplayAmount } from "../../lib/currency";
import { formatCurrency } from "../../lib/formatters";
import { getTransactionsModel } from "../../lib/queries";

export default async function TransactionsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getTransactionsModel(searchParams);
  const creditCardTemplates = model.dataset.templates
    .filter((template) => template.compatibleAccountType === "credit_card")
    .map((template) => ({ id: template.id, name: template.name }));
  const importBatchBySettlementId = new Map(
    model.dataset.importBatches
      .filter((batch) => batch.creditCardSettlementTransactionId)
      .map((batch) => [batch.creditCardSettlementTransactionId!, batch]),
  );

  return (
    <AppShell
      pathname="/transactions"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
    >
      <div className="dashboard-grid">
        <div className="page-header">
          <div>
            <h1 className="page-title">Transactions</h1>
            <p className="page-subtitle">
              Source-of-truth ledger with economic entity attribution,
              classification confidence, and review state.
            </p>
          </div>
        </div>

        <SectionCard
          title="Ledger Actions"
          subtitle="Manual operations"
          span="span-12"
        >
          <div className="split-grid">
            <div>
              <span className="label-sm">Supported edits</span>
              <div className="legend-list" style={{ marginTop: 12 }}>
                {[
                  "Reclassify",
                  "Change economic entity",
                  "Mark as internal transfer",
                  "Resolve security",
                  "Set or clear needs_review",
                  "Exclude from analytics",
                  "Create rule from this row",
                ].map((item) => (
                  <span key={item} className="pill">
                    {item}
                  </span>
                ))}
              </div>
            </div>
            <div>
              <span className="label-sm">Current quality state</span>
              <div style={{ marginTop: 12 }} className="metric-nominal">
                {model.ledger.quality.pendingEnrichmentCount} rows await
                automatic analysis, {model.ledger.quality.pendingReviewCount}{" "}
                rows need manual review, and{" "}
                {model.ledger.quality.staleAccountsCount} accounts are stale.
              </div>
            </div>
          </div>
        </SectionCard>

        <SimpleTable
          span="span-12"
          headers={[
            "Date",
            "Account",
            "Economic Entity",
            "Description",
            "Merchant",
            "Amount",
            "Class",
            "Category",
            "Statement",
            "Review",
            "Confidence",
          ]}
          rows={model.ledger.transactions.map((row) => [
            row.transactionDate,
            model.dataset.accounts.find(
              (account) => account.id === row.accountId,
            )?.displayName ?? row.accountId,
            model.dataset.entities.find(
              (entity) => entity.id === row.economicEntityId,
            )?.displayName ?? row.economicEntityId,
            row.descriptionRaw,
            row.merchantNormalized ?? "—",
            formatCurrency(
              convertBaseEurToDisplayAmount(
                model.dataset,
                row.amountBaseEur,
                model.currency,
                row.transactionDate,
              ),
              model.currency,
            ),
            row.transactionClass,
            row.categoryCode ?? "—",
            <CreditCardStatementUploadCell
              settlementTransactionId={row.id}
              statementStatus={row.creditCardStatementStatus}
              linkedCreditCardAccountName={
                model.dataset.accounts.find(
                  (account) => account.id === row.linkedCreditCardAccountId,
                )?.displayName ?? null
              }
              linkedImportFilename={
                importBatchBySettlementId.get(row.id)?.originalFilename ?? null
              }
              linkedImportBatchId={
                importBatchBySettlementId.get(row.id)?.id ?? null
              }
              templateOptions={creditCardTemplates}
            />,
            <ReviewEditorCell
              transactionId={row.id}
              needsReview={row.needsReview}
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
            row.classificationConfidence,
          ])}
        />
      </div>
    </AppShell>
  );
}

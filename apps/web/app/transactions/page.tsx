import { AppShell } from "../../components/app-shell";
import { SectionCard, SimpleTable } from "../../components/primitives";
import { formatCurrency, getTransactionsModel } from "../../lib/queries";

export default async function TransactionsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const model = await getTransactionsModel(searchParams);

  return (
    <AppShell
      pathname="/transactions"
      scopeOptions={model.scopeOptions}
      state={{
        scopeParam: model.scopeParam,
        currency: model.currency,
        period: model.period.preset,
      }}
    >
      <div className="dashboard-grid">
        <div className="page-header">
          <div>
            <h1 className="page-title">Transactions</h1>
            <p className="page-subtitle">
              Source-of-truth ledger with economic entity attribution, classification confidence, and review state.
            </p>
          </div>
        </div>

        <SectionCard title="Ledger Actions" subtitle="Manual operations" span="span-12">
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
                {model.ledger.quality.pendingReviewCount} rows need review and{" "}
                {model.ledger.quality.staleAccountsCount} accounts are stale.
              </div>
            </div>
          </div>
        </SectionCard>

        <SimpleTable
          span="span-12"
          headers={["Date", "Account", "Economic Entity", "Description", "Merchant", "Amount", "Class", "Category", "Review", "Confidence"]}
          rows={model.ledger.transactions.map((row) => [
            row.transactionDate,
            model.dataset.accounts.find((account) => account.id === row.accountId)?.displayName ?? row.accountId,
            model.dataset.entities.find((entity) => entity.id === row.economicEntityId)?.displayName ?? row.economicEntityId,
            row.descriptionRaw,
            row.merchantNormalized ?? "—",
            formatCurrency(row.amountBaseEur, model.currency),
            row.transactionClass,
            row.categoryCode ?? "—",
            row.needsReview ? "Needs review" : "OK",
            row.classificationConfidence,
          ])}
        />
      </div>
    </AppShell>
  );
}

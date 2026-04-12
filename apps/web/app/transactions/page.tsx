import { AppShell } from "../../components/app-shell";
import { CreditCardStatementUploadCell } from "../../components/credit-card-statement-upload-cell";
import { SectionCard, SimpleTable } from "../../components/primitives";
import { ReviewEditorCell } from "../../components/review-editor-cell";
import { convertBaseEurToDisplayAmount } from "../../lib/currency";
import { formatCurrency } from "../../lib/formatters";
import { buildHref, getTransactionsModel } from "../../lib/queries";

function getPeriodFallbackLabel(period: {
  preset: string;
  start: string;
  end: string;
}) {
  if (period.preset === "mtd") {
    return "Month to Date";
  }
  if (period.preset === "ytd") {
    return "Year to Date";
  }
  if (period.preset === "week") {
    return "Week to Date";
  }
  if (period.preset === "24m") {
    return "Trailing 24 Months";
  }
  return `${period.start} to ${period.end}`;
}

function getSearchSummary(input: {
  query: string;
  mode: "default" | "hybrid";
  semanticCandidateCount: number;
  keywordCandidateCount: number;
  warnings: string[];
  usedScopeFallback: boolean;
  usedPeriodFallback: boolean;
  scopeLabel: string | null;
  periodLabel: string;
}) {
  if (input.mode === "default" || !input.query.trim()) {
    return "Finder is idle. The current page selectors control the ledger list until you run a hybrid search.";
  }

  const fallbackBits = [
    input.usedScopeFallback && input.scopeLabel
      ? `scope fallback: ${input.scopeLabel}`
      : null,
    input.usedPeriodFallback ? `period fallback: ${input.periodLabel}` : null,
  ].filter(Boolean);

  const retrievalSummary =
    input.semanticCandidateCount > 0 && input.keywordCandidateCount > 0
      ? `Hybrid retrieval is active for "${input.query}". ${input.semanticCandidateCount} semantic candidates and ${input.keywordCandidateCount} BM25 candidates were fused.`
      : input.keywordCandidateCount > 0
        ? `Keyword retrieval is active for "${input.query}". ${input.keywordCandidateCount} BM25 candidates were returned.`
        : input.semanticCandidateCount > 0
          ? `Semantic retrieval is active for "${input.query}". ${input.semanticCandidateCount} semantic candidates were returned.`
          : `No indexed matches were found for "${input.query}".`;

  return [
    retrievalSummary,
    fallbackBits.length > 0
      ? `Current selectors were applied only where the query stayed silent: ${fallbackBits.join("; ")}.`
      : "The query already supplied its own scope and time constraints, so current-page selectors were not applied as filters.",
    ...input.warnings,
  ].join(" ");
}

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
  const activeScopeLabel =
    model.scopeOptions.find((option) => option.value === model.scopeParam)
      ?.label ?? null;
  const clearSearchHref = buildHref(
    "/transactions",
    model.navigationState,
    {},
    { q: undefined },
  );

  return (
    <AppShell
      pathname="/transactions"
      scopeOptions={model.scopeOptions}
      state={model.navigationState}
      pageQueryParams={{ q: model.transactionSearchQuery || undefined }}
    >
      <div className="dashboard-grid">
        <div className="page-header">
          <div>
            <h1 className="page-title">Transactions</h1>
            <p className="page-subtitle">
              Hybrid transaction finder with contextual retrieval, BM25,
              embeddings, reranking, and inline review updates.
            </p>
          </div>
        </div>

        <SectionCard
          title="Transaction Finder"
          subtitle="Contextual hybrid retrieval"
          span="span-12"
        >
          <form action="/transactions" method="get" className="inline-actions">
            <input type="hidden" name="scope" value={model.scopeParam} />
            <input type="hidden" name="currency" value={model.currency} />
            <input type="hidden" name="period" value={model.period.preset} />
            {model.referenceDate ? (
              <input type="hidden" name="asOf" value={model.referenceDate} />
            ) : null}
            {model.period.preset === "custom" ? (
              <>
                <input type="hidden" name="start" value={model.period.start} />
                <input type="hidden" name="end" value={model.period.end} />
              </>
            ) : null}
            <label className="input-label" style={{ flex: 1, minWidth: 320 }}>
              <span>Search transactions</span>
              <input
                className="input-field"
                type="search"
                name="q"
                placeholder="Example: Stripe payments received by Santander account in March 2026"
                defaultValue={model.transactionSearchQuery}
              />
            </label>
            <button className="btn-pill" type="submit">
              Search
            </button>
            {model.transactionSearchQuery ? (
              <a className="btn-ghost" href={clearSearchHref}>
                Clear
              </a>
            ) : null}
          </form>
          <div className="muted" style={{ marginTop: 12, lineHeight: 1.5 }}>
            {getSearchSummary({
              query: model.ledger.search.query ?? "",
              mode: model.ledger.search.mode,
              semanticCandidateCount:
                model.ledger.search.semanticCandidateCount,
              keywordCandidateCount: model.ledger.search.keywordCandidateCount,
              warnings: model.ledger.search.warnings,
              usedScopeFallback: model.ledger.search.filters.usedScopeFallback,
              usedPeriodFallback:
                model.ledger.search.filters.usedPeriodFallback,
              scopeLabel: activeScopeLabel,
              periodLabel: getPeriodFallbackLabel(model.period),
            })}
          </div>
        </SectionCard>

        <SectionCard
          title="Ledger Actions"
          subtitle="Review and statement tools"
          span="span-12"
        >
          <div className="split-grid">
            <div>
              <span className="label-sm">Search-ready metadata</span>
              <div className="legend-list" style={{ marginTop: 12 }}>
                {[
                  "Raw description stays visible",
                  "Account and entity context",
                  "Review state",
                  "Merchant and counterparty",
                  "Date and amount direction",
                  "Batch summary inheritance",
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

        {model.ledger.rows.length === 0 ? (
          <SectionCard
            title="Results"
            subtitle="No matching transactions"
            span="span-12"
          >
            <div className="table-empty-state">
              {model.transactionSearchQuery
                ? `No transactions matched "${model.transactionSearchQuery}".`
                : "No transactions are available for the current selector state."}
            </div>
          </SectionCard>
        ) : (
          <SimpleTable
            span="span-12"
            headers={[
              "Date",
              "Account",
              "Economic Entity",
              "Description",
              "Amount",
              "Match",
              "Statement",
              "Review",
            ]}
            rows={model.ledger.rows.map((row) => {
              const transaction = row.transaction;
              const account =
                model.dataset.accounts.find(
                  (candidate) => candidate.id === transaction.accountId,
                ) ?? null;
              const economicEntity =
                model.dataset.entities.find(
                  (candidate) => candidate.id === transaction.economicEntityId,
                ) ?? null;
              const statementBatch = importBatchBySettlementId.get(
                transaction.id,
              );

              return [
                transaction.transactionDate,
                account?.displayName ?? transaction.accountId,
                economicEntity?.displayName ?? transaction.economicEntityId,
                <div style={{ display: "grid", gap: 4 }}>
                  <span>{transaction.descriptionRaw}</span>
                  <span className="muted" style={{ fontSize: 12 }}>
                    {[
                      transaction.merchantNormalized
                        ? `merchant ${transaction.merchantNormalized}`
                        : null,
                      transaction.counterpartyName
                        ? `counterparty ${transaction.counterpartyName}`
                        : null,
                      transaction.categoryCode
                        ? `category ${transaction.categoryCode}`
                        : null,
                    ]
                      .filter(Boolean)
                      .join(" · ") ||
                      "No structured merchant/counterparty/category context yet."}
                  </span>
                </div>,
                formatCurrency(
                  convertBaseEurToDisplayAmount(
                    model.dataset,
                    transaction.amountBaseEur,
                    model.currency,
                    transaction.transactionDate,
                  ),
                  model.currency,
                ),
                row.searchDiagnostics ? (
                  <div style={{ display: "grid", gap: 4 }}>
                    <span>
                      Hybrid {row.searchDiagnostics.hybridScore.toFixed(4)}
                    </span>
                    <span className="muted" style={{ fontSize: 12 }}>
                      {[
                        row.searchDiagnostics.semanticRank
                          ? `semantic #${row.searchDiagnostics.semanticRank}`
                          : null,
                        row.searchDiagnostics.keywordRank
                          ? `bm25 #${row.searchDiagnostics.keywordRank}`
                          : null,
                        row.searchDiagnostics.rerankRank
                          ? `rerank #${row.searchDiagnostics.rerankRank}`
                          : null,
                      ]
                        .filter(Boolean)
                        .join(" · ")}
                    </span>
                    <span className="muted" style={{ fontSize: 12 }}>
                      {[
                        row.searchDiagnostics.direction,
                        row.searchDiagnostics.reviewState,
                        row.searchDiagnostics.matchedBy.join("+"),
                      ].join(" · ")}
                    </span>
                  </div>
                ) : (
                  "Default list"
                ),
                <CreditCardStatementUploadCell
                  settlementTransactionId={transaction.id}
                  statementStatus={transaction.creditCardStatementStatus}
                  linkedCreditCardAccountName={
                    model.dataset.accounts.find(
                      (candidate) =>
                        candidate.id === transaction.linkedCreditCardAccountId,
                    )?.displayName ?? null
                  }
                  linkedImportFilename={
                    statementBatch?.originalFilename ?? null
                  }
                  linkedImportBatchId={statementBatch?.id ?? null}
                  templateOptions={creditCardTemplates}
                />,
                <ReviewEditorCell
                  transactionId={transaction.id}
                  needsReview={transaction.needsReview}
                  categoryCode={transaction.categoryCode}
                  reviewReason={transaction.reviewReason}
                  manualNotes={transaction.manualNotes}
                  transactionClass={transaction.transactionClass}
                  classificationSource={transaction.classificationSource}
                  quantity={transaction.quantity}
                  llmPayload={transaction.llmPayload}
                  creditCardStatementStatus={
                    transaction.creditCardStatementStatus
                  }
                  descriptionRaw={transaction.descriptionRaw}
                  descriptionClean={transaction.descriptionClean}
                />,
              ];
            })}
          />
        )}
      </div>
    </AppShell>
  );
}

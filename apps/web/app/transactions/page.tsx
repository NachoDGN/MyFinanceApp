import {
  filterTransactionsByPeriod,
  filterTransactionsByReferenceDate,
  filterTransactionsByScope,
  isUncategorizedCategoryCode,
  needsTransactionManualReview,
} from "@myfinance/domain";

import { AppShell } from "../../components/app-shell";
import { CreditCardStatementUploadCell } from "../../components/credit-card-statement-upload-cell";
import { SectionCard, SimpleTable } from "../../components/primitives";
import { ReviewEditorCell } from "../../components/review-editor-cell";
import { TransactionCategoryManagementPanel } from "../../components/transaction-category-management-panel";
import { UnresolvedTransactionsReviewPanel } from "../../components/unresolved-transactions-review-panel";
import { convertBaseEurToDisplayAmount } from "../../lib/currency";
import { formatCurrency } from "../../lib/formatters";
import { buildHref, getTransactionsModel } from "../../lib/queries";

function getSearchSummary(input: {
  query: string;
  mode: "default" | "hybrid";
  semanticCandidateCount: number;
  keywordCandidateCount: number;
  warnings: string[];
}) {
  if (input.mode === "default" || !input.query.trim()) {
    return "Finder is idle. The current page selectors control the ledger list until you run a global hybrid search.";
  }

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
    "The finder searches across all indexed transactions. Add an account, entity, review state, or date to narrow the query.",
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
  const accountsById = new Map(
    model.dataset.accounts.map((account) => [account.id, account]),
  );
  const entitiesById = new Map(
    model.dataset.entities.map((entity) => [entity.id, entity]),
  );
  const clearSearchHref = buildHref(
    "/transactions",
    model.navigationState,
    {},
    { q: undefined },
  );
  const scopedTransactions = filterTransactionsByReferenceDate(
    filterTransactionsByScope(model.dataset, model.scope),
    model.referenceDate,
  );
  const scopedPeriodTransactions = filterTransactionsByPeriod(
    scopedTransactions,
    model.period,
  );
  const unresolvedTransactions = [...scopedPeriodTransactions]
    .filter((transaction) => needsTransactionManualReview(transaction))
    .sort((left, right) =>
      `${right.transactionDate}${right.createdAt}`.localeCompare(
        `${left.transactionDate}${left.createdAt}`,
      ),
    );
  const unresolvedPanelRows = unresolvedTransactions
    .slice(0, 12)
    .map((transaction) => {
      const account = accountsById.get(transaction.accountId) ?? null;
      const entity = entitiesById.get(transaction.economicEntityId) ?? null;
      const statementHref = transaction.importBatchId
        ? buildHref(
            `/transactions/statements/${transaction.importBatchId}`,
            model.navigationState,
            {},
          )
        : undefined;

      return {
        id: transaction.id,
        href: `#transaction-${transaction.id}`,
        ctaLabel: "Jump to ledger",
        secondaryHref: statementHref,
        secondaryLabel: statementHref ? "Batch" : undefined,
        date: transaction.transactionDate,
        account: (
          <div style={{ display: "grid", gap: 4 }}>
            <span>{account?.displayName ?? transaction.accountId}</span>
            <span className="muted" style={{ fontSize: 12 }}>
              {entity?.displayName ?? transaction.economicEntityId}
            </span>
          </div>
        ),
        description: (
          <div style={{ display: "grid", gap: 4 }}>
            <span>{transaction.descriptionRaw}</span>
            <span className="muted" style={{ fontSize: 12 }}>
              {[
                transaction.counterpartyName
                  ? `counterparty ${transaction.counterpartyName}`
                  : null,
                transaction.categoryCode
                  ? `category ${transaction.categoryCode}`
                  : null,
                transaction.reviewReason ? transaction.reviewReason : null,
              ]
                .filter(Boolean)
                .join(" · ") || "Awaiting manual review details."}
            </span>
          </div>
        ),
        amount: formatCurrency(
          convertBaseEurToDisplayAmount(
            model.dataset,
            transaction.amountBaseEur,
            model.currency,
            transaction.transactionDate,
          ),
          model.currency,
        ),
        review: (
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
            creditCardStatementStatus={transaction.creditCardStatementStatus}
            descriptionRaw={transaction.descriptionRaw}
            descriptionClean={transaction.descriptionClean}
          />
        ),
      };
    });
  const categoryPanelAccounts = model.dataset.accounts
    .filter((account) =>
      scopedTransactions.some((transaction) => transaction.accountId === account.id),
    )
    .map((account) => {
      const accountTransactions = scopedTransactions.filter(
        (transaction) => transaction.accountId === account.id,
      );

      return {
        id: account.id,
        displayName: account.displayName,
        institutionName: account.institutionName,
        entityName:
          entitiesById.get(account.entityId)?.displayName ?? account.entityId,
        assetDomain: account.assetDomain,
        totalTransactions: accountTransactions.length,
        categorizedTransactions: accountTransactions.filter(
          (transaction) =>
            Boolean(transaction.categoryCode) &&
            !isUncategorizedCategoryCode(transaction.categoryCode),
        ).length,
        uncategorizedTransactions: accountTransactions.filter(
          (transaction) =>
            !transaction.categoryCode ||
            isUncategorizedCategoryCode(transaction.categoryCode),
        ).length,
      };
    });
  const categoryPanelCategories = model.dataset.categories.map((category) => {
    const matchingTransactions = scopedTransactions.filter(
      (transaction) => transaction.categoryCode === category.code,
    );

    return {
      code: category.code,
      displayName: category.displayName,
      scopeKind: category.scopeKind,
      directionKind: category.directionKind,
      active: category.active,
      totalTransactionCount: matchingTransactions.length,
      lastTransactionDate:
        [...matchingTransactions]
          .sort((left, right) =>
            right.transactionDate.localeCompare(left.transactionDate),
          )
          .at(0)?.transactionDate ?? null,
      accountUsage: categoryPanelAccounts.map((account) => {
        const accountTransactions = matchingTransactions.filter(
          (transaction) => transaction.accountId === account.id,
        );

        return {
          accountId: account.id,
          transactionCount: accountTransactions.length,
          lastTransactionDate:
            [...accountTransactions]
              .sort((left, right) =>
                right.transactionDate.localeCompare(left.transactionDate),
              )
              .at(0)?.transactionDate ?? null,
        };
      }),
    };
  });
  const initialCategoryPanelAccountId =
    model.scope.kind === "account" ? model.scope.accountId ?? null : null;

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
            })}
          </div>
        </SectionCard>

        <UnresolvedTransactionsReviewPanel
          rows={unresolvedPanelRows}
          summaryPills={[
            {
              label: `${unresolvedTransactions.length} unresolved in the current scope`,
              tone: unresolvedTransactions.length > 0 ? "warning" : "default",
            },
            {
              label: `${new Set(unresolvedTransactions.map((row) => row.accountId)).size} accounts represented`,
            },
          ]}
          helperText="This queue stays tied to the current scope and period so you can work through unresolved rows directly, while the finder above now searches globally."
          footerNote={
            unresolvedTransactions.length > unresolvedPanelRows.length
              ? `Showing the newest ${unresolvedPanelRows.length} unresolved transactions for fast triage. Use the ledger below for the full queue.`
              : undefined
          }
        />

        <TransactionCategoryManagementPanel
          accounts={categoryPanelAccounts}
          categories={categoryPanelCategories}
          initialAccountId={initialCategoryPanelAccountId}
          emptyStateCopy="No category definitions are available in the current scope."
        />

        <SectionCard
          title="Ledger"
          subtitle="Detailed transaction results"
          span="span-12"
        >
          <div className="legend-list" style={{ marginTop: 12 }}>
            <span className="pill">
              {model.ledger.quality.pendingEnrichmentCount} queued for enrichment
            </span>
            <span
              className={
                model.ledger.quality.pendingReviewCount > 0
                  ? "pill warning"
                  : "pill"
              }
            >
              {model.ledger.quality.pendingReviewCount} manual review
            </span>
            <span className="pill">
              {model.ledger.quality.staleAccountsCount} stale accounts
            </span>
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
          <div id="ledger-results">
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
                const account = accountsById.get(transaction.accountId) ?? null;
                const economicEntity =
                  entitiesById.get(transaction.economicEntityId) ?? null;
                const statementBatch = importBatchBySettlementId.get(
                  transaction.id,
                );

                return [
                  transaction.transactionDate,
                  account?.displayName ?? transaction.accountId,
                  economicEntity?.displayName ?? transaction.economicEntityId,
                  <div
                    id={`transaction-${transaction.id}`}
                    style={{ display: "grid", gap: 4 }}
                  >
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
                      accountsById.get(
                        transaction.linkedCreditCardAccountId ?? "",
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
          </div>
        )}
      </div>
    </AppShell>
  );
}

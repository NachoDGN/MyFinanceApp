import {
  filterTransactionsByPeriod,
  filterTransactionsByReferenceDate,
  filterTransactionsByScope,
  needsTransactionManualReview,
} from "@myfinance/domain";

import { AppShell } from "../../components/app-shell";
import { CreditCardStatementUploadCell } from "../../components/credit-card-statement-upload-cell";
import { SectionCard } from "../../components/primitives";
import { ReviewEditorCell } from "../../components/review-editor-cell";
import { UnresolvedTransactionsReviewPanel } from "../../components/unresolved-transactions-review-panel";
import { convertBaseEurToDisplayAmount } from "../../lib/currency";
import { formatCurrency, formatDate } from "../../lib/formatters";
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

function readSingleSearchParam(
  value: string | string[] | undefined,
): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

export default async function TransactionsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const resolvedSearchParams = await searchParams;
  const model = await getTransactionsModel(resolvedSearchParams);
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
  const categoriesHref = buildHref("/categories", model.navigationState, {});
  const requestedPage = Number.parseInt(
    readSingleSearchParam(resolvedSearchParams.page) ?? "1",
    10,
  );
  const pageSize = 10;
  const totalLedgerRows = model.ledger.rows.length;
  const totalPages = Math.max(1, Math.ceil(totalLedgerRows / pageSize));
  const currentPage =
    Number.isFinite(requestedPage) && requestedPage > 0
      ? Math.min(requestedPage, totalPages)
      : 1;
  const pageStartIndex = (currentPage - 1) * pageSize;
  const pagedLedgerRows = model.ledger.rows.slice(
    pageStartIndex,
    pageStartIndex + pageSize,
  );
  const ledgerRangeStart = totalLedgerRows === 0 ? 0 : pageStartIndex + 1;
  const ledgerRangeEnd =
    totalLedgerRows === 0
      ? 0
      : Math.min(pageStartIndex + pageSize, totalLedgerRows);
  const clearSearchHref = buildHref(
    "/transactions",
    model.navigationState,
    {},
    { q: undefined, page: undefined },
  );
  const buildLedgerPageHref = (page: number) =>
    `${buildHref("/transactions", model.navigationState, {}, {
      q: model.transactionSearchQuery || undefined,
      page: page <= 1 ? undefined : String(page),
    })}#ledger-results`;
  const scopedTransactions = filterTransactionsByReferenceDate(
    filterTransactionsByScope(model.dataset, model.scope),
    model.referenceDate,
  );
  const scopedPeriodTransactions = filterTransactionsByPeriod(
    scopedTransactions,
    model.period,
  );
  const defaultLedgerPageByTransactionId = new Map(
    [...scopedPeriodTransactions]
      .sort((left, right) =>
        `${right.transactionDate}${right.createdAt}`.localeCompare(
          `${left.transactionDate}${left.createdAt}`,
        ),
      )
      .map((transaction, index) => [
        transaction.id,
        Math.floor(index / pageSize) + 1,
      ]),
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
      const ledgerPage = defaultLedgerPageByTransactionId.get(transaction.id) ?? 1;
      const statementHref = transaction.importBatchId
        ? buildHref(
            `/transactions/statements/${transaction.importBatchId}`,
            model.navigationState,
            {},
          )
        : undefined;

      return {
        id: transaction.id,
        href: `${buildHref("/transactions", model.navigationState, {}, {
          q: undefined,
          page: ledgerPage <= 1 ? undefined : String(ledgerPage),
        })}#transaction-${transaction.id}`,
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
          <a className="btn-ghost" href={categoriesHref}>
            Open Categories
          </a>
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

        <div
          id="ledger-results"
          style={{ gridColumn: "span 12", scrollMarginTop: 24 }}
        >
          <SectionCard
            title="Ledger"
            subtitle="Detailed transaction results"
            span="span-12"
          >
            <div style={{ display: "grid", gap: 20 }}>
              <div
                style={{
                  display: "flex",
                  alignItems: "flex-start",
                  justifyContent: "space-between",
                  gap: 16,
                  flexWrap: "wrap",
                }}
              >
                <div className="legend-list">
                  <span className="pill">
                    {model.ledger.quality.pendingEnrichmentCount} queued for
                    enrichment
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
                  {totalLedgerRows > 0 ? (
                    <span className="pill">
                      Showing {ledgerRangeStart}-{ledgerRangeEnd} of{" "}
                      {totalLedgerRows}
                    </span>
                  ) : null}
                </div>

                {totalPages > 1 ? (
                  <div
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 10,
                      flexWrap: "wrap",
                      justifyContent: "flex-end",
                    }}
                  >
                    {currentPage > 1 ? (
                      <a
                        className="btn-ghost"
                        href={buildLedgerPageHref(currentPage - 1)}
                      >
                        Previous
                      </a>
                    ) : (
                      <span className="btn-ghost" style={{ opacity: 0.45 }}>
                        Previous
                      </span>
                    )}
                    <span className="pill">
                      Page {currentPage} of {totalPages}
                    </span>
                    {currentPage < totalPages ? (
                      <a
                        className="btn-ghost"
                        href={buildLedgerPageHref(currentPage + 1)}
                      >
                        Next
                      </a>
                    ) : (
                      <span className="btn-ghost" style={{ opacity: 0.45 }}>
                        Next
                      </span>
                    )}
                  </div>
                ) : null}
              </div>

              {totalLedgerRows === 0 ? (
                <div className="table-empty-state">
                  {model.transactionSearchQuery
                    ? `No transactions matched "${model.transactionSearchQuery}".`
                    : "No transactions are available for the current selector state."}
                </div>
              ) : (
                <>
                  <div className="table-wrap">
                    <table
                      className="data-table"
                      style={{ tableLayout: "fixed", width: "100%" }}
                    >
                      <colgroup>
                        <col style={{ width: "11%" }} />
                        <col style={{ width: "34%" }} />
                        <col style={{ width: "12%" }} />
                        <col style={{ width: "18%" }} />
                        <col style={{ width: "25%" }} />
                      </colgroup>
                      <thead>
                        <tr>
                          <th>Date</th>
                          <th>Transaction</th>
                          <th>Amount</th>
                          <th>Retrieval & Statement</th>
                          <th>Review</th>
                        </tr>
                      </thead>
                      <tbody>
                        {pagedLedgerRows.map((row) => {
                          const transaction = row.transaction;
                          const account =
                            accountsById.get(transaction.accountId) ?? null;
                          const economicEntity =
                            entitiesById.get(transaction.economicEntityId) ??
                            null;
                          const statementBatch = importBatchBySettlementId.get(
                            transaction.id,
                          );
                          const displayAmount = formatCurrency(
                            convertBaseEurToDisplayAmount(
                              model.dataset,
                              transaction.amountBaseEur,
                              model.currency,
                              transaction.transactionDate,
                            ),
                            model.currency,
                          );

                          return (
                            <tr key={transaction.id}>
                              <td style={{ verticalAlign: "top" }}>
                                <div style={{ display: "grid", gap: 4 }}>
                                  <span style={{ fontWeight: 700 }}>
                                    {formatDate(transaction.transactionDate, {
                                      lenient: true,
                                    })}
                                  </span>
                                  <span
                                    className="muted"
                                    style={{ fontSize: 12 }}
                                  >
                                    {transaction.transactionDate}
                                  </span>
                                </div>
                              </td>
                              <td style={{ verticalAlign: "top" }}>
                                <div
                                  id={`transaction-${transaction.id}`}
                                  style={{
                                    display: "grid",
                                    gap: 10,
                                    scrollMarginTop: 24,
                                  }}
                                >
                                  <div
                                    style={{
                                      display: "flex",
                                      alignItems: "center",
                                      gap: 8,
                                      flexWrap: "wrap",
                                    }}
                                  >
                                    <span className="pill">
                                      {account?.displayName ??
                                        transaction.accountId}
                                    </span>
                                    <span className="pill">
                                      {economicEntity?.displayName ??
                                        transaction.economicEntityId}
                                    </span>
                                  </div>
                                  <div style={{ display: "grid", gap: 4 }}>
                                    <span
                                      style={{
                                        fontSize: 20,
                                        fontWeight: 600,
                                        lineHeight: 1.35,
                                      }}
                                    >
                                      {transaction.descriptionRaw}
                                    </span>
                                    <span
                                      className="muted"
                                      style={{ fontSize: 13, lineHeight: 1.5 }}
                                    >
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
                                        "No structured merchant, counterparty, or category context yet."}
                                    </span>
                                  </div>
                                </div>
                              </td>
                              <td style={{ verticalAlign: "top" }}>
                                <div
                                  style={{
                                    display: "grid",
                                    gap: 4,
                                    textAlign: "right",
                                  }}
                                >
                                  <span
                                    style={{
                                      fontSize: 24,
                                      fontWeight: 700,
                                      letterSpacing: "-0.02em",
                                      color:
                                        Number(transaction.amountBaseEur) < 0
                                          ? "var(--color-accent)"
                                          : "var(--color-text-main)",
                                    }}
                                  >
                                    {displayAmount}
                                  </span>
                                  <span
                                    className="muted"
                                    style={{ fontSize: 12 }}
                                  >
                                    {transaction.transactionClass.replace(
                                      /_/g,
                                      " ",
                                    )}
                                  </span>
                                </div>
                              </td>
                              <td style={{ verticalAlign: "top" }}>
                                <div style={{ display: "grid", gap: 12 }}>
                                  <div
                                    style={{
                                      display: "grid",
                                      gap: 6,
                                      padding: 14,
                                      borderRadius: 18,
                                      background: "rgba(0, 0, 0, 0.025)",
                                      border:
                                        "1px solid rgba(0, 0, 0, 0.05)",
                                    }}
                                  >
                                    <span
                                      className="label-sm"
                                      style={{ marginBottom: 0 }}
                                    >
                                      Retrieval
                                    </span>
                                    <span style={{ fontWeight: 700 }}>
                                      {row.searchDiagnostics
                                        ? `Hybrid ${row.searchDiagnostics.hybridScore.toFixed(4)}`
                                        : "Scoped ledger list"}
                                    </span>
                                    <span
                                      className="muted"
                                      style={{ fontSize: 12, lineHeight: 1.45 }}
                                    >
                                      {row.searchDiagnostics
                                        ? [
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
                                            .join(" · ")
                                        : "Ordered by the current scope and period selectors."}
                                    </span>
                                    {row.searchDiagnostics ? (
                                      <span
                                        className="muted"
                                        style={{
                                          fontSize: 12,
                                          lineHeight: 1.45,
                                        }}
                                      >
                                        {[
                                          row.searchDiagnostics.direction,
                                          row.searchDiagnostics.reviewState,
                                          row.searchDiagnostics.matchedBy.join(
                                            " + ",
                                          ),
                                        ].join(" · ")}
                                      </span>
                                    ) : null}
                                  </div>

                                  <div style={{ display: "grid", gap: 8 }}>
                                    <span
                                      className="label-sm"
                                      style={{ marginBottom: 0 }}
                                    >
                                      Statement
                                    </span>
                                    <CreditCardStatementUploadCell
                                      settlementTransactionId={transaction.id}
                                      statementStatus={
                                        transaction.creditCardStatementStatus
                                      }
                                      linkedCreditCardAccountName={
                                        accountsById.get(
                                          transaction.linkedCreditCardAccountId ??
                                            "",
                                        )?.displayName ?? null
                                      }
                                      linkedImportFilename={
                                        statementBatch?.originalFilename ?? null
                                      }
                                      linkedImportBatchId={
                                        statementBatch?.id ?? null
                                      }
                                      templateOptions={creditCardTemplates}
                                    />
                                  </div>
                                </div>
                              </td>
                              <td style={{ verticalAlign: "top" }}>
                                <ReviewEditorCell
                                  transactionId={transaction.id}
                                  needsReview={transaction.needsReview}
                                  categoryCode={transaction.categoryCode}
                                  reviewReason={transaction.reviewReason}
                                  manualNotes={transaction.manualNotes}
                                  transactionClass={transaction.transactionClass}
                                  classificationSource={
                                    transaction.classificationSource
                                  }
                                  quantity={transaction.quantity}
                                  llmPayload={transaction.llmPayload}
                                  creditCardStatementStatus={
                                    transaction.creditCardStatementStatus
                                  }
                                  descriptionRaw={transaction.descriptionRaw}
                                  descriptionClean={transaction.descriptionClean}
                                />
                              </td>
                            </tr>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>

                  {totalPages > 1 ? (
                    <div
                      style={{
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "space-between",
                        gap: 16,
                        flexWrap: "wrap",
                      }}
                    >
                      <span className="muted" style={{ fontSize: 13 }}>
                        Page {currentPage} of {totalPages}. Pagination is
                        anchored to the ledger so moving between pages keeps
                        you here.
                      </span>
                      <div
                        style={{
                          display: "flex",
                          alignItems: "center",
                          gap: 10,
                          flexWrap: "wrap",
                        }}
                      >
                        {currentPage > 1 ? (
                          <a
                            className="btn-ghost"
                            href={buildLedgerPageHref(currentPage - 1)}
                          >
                            Previous
                          </a>
                        ) : (
                          <span className="btn-ghost" style={{ opacity: 0.45 }}>
                            Previous
                          </span>
                        )}
                        {currentPage < totalPages ? (
                          <a
                            className="btn-ghost"
                            href={buildLedgerPageHref(currentPage + 1)}
                          >
                            Next
                          </a>
                        ) : (
                          <span className="btn-ghost" style={{ opacity: 0.45 }}>
                            Next
                          </span>
                        )}
                      </div>
                    </div>
                  ) : null}
                </>
              )}
            </div>
          </SectionCard>
        </div>
      </div>
    </AppShell>
  );
}

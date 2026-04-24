import type {
  Transaction,
  TransactionQuestionEvidence,
} from "@myfinance/domain";

import { mapFromSql } from "./sql-json";
import type { SqlClient } from "./sql-runtime";
import { sanitizeReadOnlyLedgerSql } from "./transaction-agent-sql-guard";
import type {
  TransactionAgentAnswerType,
  TransactionAgentToolContext,
  TransactionAgentToolName,
  TransactionAgentToolResult,
} from "./transaction-agent-types";
import { transactionColumnsSql } from "./transaction-columns";
import { searchTransactions } from "./transaction-search";

function normalizeLimit(value: unknown, fallback = 3) {
  const numeric = Number(value);
  return Number.isFinite(numeric)
    ? Math.max(1, Math.min(Math.floor(numeric), 8))
    : fallback;
}

function textArg(args: Record<string, unknown>, key: string) {
  const value = args[key];
  return typeof value === "string" ? value.trim() : "";
}

function nullableText(value: unknown) {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function evidenceTitleForTransaction(transaction: Transaction) {
  return (
    transaction.merchantNormalized ||
    transaction.counterpartyName ||
    transaction.descriptionClean ||
    transaction.descriptionRaw
  );
}

function summarizeTransaction(transaction: Transaction) {
  return [
    transaction.transactionDate,
    `${transaction.amountOriginal} ${transaction.currencyOriginal}`,
    transaction.transactionClass,
    transaction.categoryCode ?? "uncategorized",
    transaction.descriptionRaw,
  ].join(" | ");
}

function transactionEvidence(
  transaction: Transaction,
  metadata: Record<string, unknown> = {},
): TransactionQuestionEvidence {
  return {
    id: `transaction:${transaction.id}`,
    type: "transaction",
    title: evidenceTitleForTransaction(transaction),
    summary: summarizeTransaction(transaction),
    transactionId: transaction.id,
    sourceId: transaction.id,
    metadata: {
      transactionDate: transaction.transactionDate,
      amountOriginal: transaction.amountOriginal,
      currencyOriginal: transaction.currencyOriginal,
      amountBaseEur: transaction.amountBaseEur,
      merchantNormalized: transaction.merchantNormalized ?? null,
      counterpartyName: transaction.counterpartyName ?? null,
      transactionClass: transaction.transactionClass,
      categoryCode: transaction.categoryCode ?? null,
      accountId: transaction.accountId,
      economicEntityId: transaction.economicEntityId,
      ...metadata,
    },
  };
}

function normalizeAnswerType(value: unknown): TransactionAgentAnswerType {
  return value === "aggregate" ||
    value === "comparison" ||
    value === "clarification" ||
    value === "no_result" ||
    value === "single_transaction"
    ? value
    : "single_transaction";
}

function normalizeConfidence(value: unknown) {
  if (value === null || value === undefined) {
    return null;
  }

  const numeric = Number(value);
  return Number.isFinite(numeric)
    ? Math.max(0, Math.min(1, numeric))
    : null;
}

function resolveCitationEvidence(input: {
  citationIds: unknown;
  context: TransactionAgentToolContext;
  insufficiencyReason: string | null;
}) {
  const explicitIds = Array.isArray(input.citationIds)
    ? input.citationIds.filter((value): value is string => typeof value === "string")
    : [];
  const citations = explicitIds.flatMap((id) => {
    const evidence = input.context.state.evidenceById.get(id);
    return evidence ? [evidence] : [];
  });

  if (citations.length > 0) {
    return citations;
  }

  if (input.insufficiencyReason) {
    return [];
  }

  return input.context.state.evidenceOrder
    .slice(-4)
    .flatMap((id) => {
      const evidence = input.context.state.evidenceById.get(id);
      return evidence ? [evidence] : [];
    });
}

async function executeAnswer(
  args: Record<string, unknown>,
  context: TransactionAgentToolContext,
): Promise<TransactionAgentToolResult> {
  const answer = textArg(args, "answer");
  if (!answer) {
    throw new Error("Answer tool requires a non-empty answer.");
  }

  const insufficiencyReason = nullableText(args.insufficiencyReason);
  const citations = resolveCitationEvidence({
    citationIds: args.citationIds,
    context,
    insufficiencyReason,
  });

  if (citations.length === 0 && !insufficiencyReason) {
    throw new Error("Answer tool requires prior evidence citations.");
  }

  return {
    summary: `Prepared final answer with ${citations.length} citation${citations.length === 1 ? "" : "s"}.`,
    payload: {
      answer,
      answerType: normalizeAnswerType(args.answerType),
      citationIds: citations.map((citation) => citation.id),
      confidence: normalizeConfidence(args.confidence),
      insufficiencyReason,
    },
    evidence: [],
    finalAnswer: {
      answer,
      answerType: normalizeAnswerType(args.answerType),
      citations,
      confidence: normalizeConfidence(args.confidence),
      insufficiencyReason,
    },
  };
}

async function executeHybridSearch(
  sql: SqlClient,
  args: Record<string, unknown>,
  context: TransactionAgentToolContext,
): Promise<TransactionAgentToolResult> {
  const query = textArg(args, "query") || context.question;
  const limit = normalizeLimit(args.limit, 3);
  const retrieval = await searchTransactions(sql, context.userId, {
    dataset: context.input.dataset,
    scope: context.input.scope,
    period: context.input.period,
    referenceDate: context.input.referenceDate,
    query,
  });
  const rows = retrieval.rows.slice(0, limit);
  const evidence = rows.map((row) =>
    transactionEvidence(row.transaction, {
      source: "hybrid_transaction_search",
      originalText: row.originalText,
      contextualizedText: row.contextualizedText,
      documentSummary: row.documentSummary,
      searchDiagnostics: row.searchDiagnostics,
    }),
  );

  return {
    summary: `Retrieved ${rows.length} transaction${rows.length === 1 ? "" : "s"} for "${query}".`,
    payload: {
      query,
      resultCount: rows.length,
      semanticCandidateCount: retrieval.semanticCandidateCount,
      keywordCandidateCount: retrieval.keywordCandidateCount,
      warnings: retrieval.warnings,
      results: rows.map((row) => ({
        transactionId: row.transaction.id,
        transactionDate: row.transaction.transactionDate,
        amountOriginal: row.transaction.amountOriginal,
        currencyOriginal: row.transaction.currencyOriginal,
        amountBaseEur: row.transaction.amountBaseEur,
        merchantNormalized: row.transaction.merchantNormalized ?? null,
        counterpartyName: row.transaction.counterpartyName ?? null,
        transactionClass: row.transaction.transactionClass,
        categoryCode: row.transaction.categoryCode ?? null,
        descriptionRaw: row.transaction.descriptionRaw,
        hybridScore: row.searchDiagnostics?.hybridScore ?? null,
      })),
    },
    evidence,
    retrieval: {
      ...retrieval,
      rows,
    },
  };
}

async function executeSqlLedgerQuery(
  sql: SqlClient,
  args: Record<string, unknown>,
  context: TransactionAgentToolContext,
): Promise<TransactionAgentToolResult> {
  const query = sanitizeReadOnlyLedgerSql(textArg(args, "query"), {
    limit: 25,
  });
  await sql`set statement_timeout = '1500ms'`;
  const rows = (await sql.unsafe(query)) as Array<Record<string, unknown>>;
  const queryResultId = `ledger_query:${context.runId ?? "transient"}:${context.state.evidenceOrder.length + 1}`;
  const evidence: TransactionQuestionEvidence = {
    id: queryResultId,
    type: "ledger_query",
    title: "Ledger query result",
    summary: `SQL returned ${rows.length} row${rows.length === 1 ? "" : "s"}.`,
    sourceId: queryResultId,
    metadata: {
      query,
      rowCount: rows.length,
      rows: rows.slice(0, 25),
    },
  };

  return {
    summary: evidence.summary,
    payload: {
      query,
      queryResultId,
      rowCount: rows.length,
      rows: rows.slice(0, 25),
    },
    evidence: [evidence],
  };
}

async function selectTransactionContext(
  sql: SqlClient,
  userId: string,
  transactionId: string,
) {
  const rows = await sql`
    select ${transactionColumnsSql(sql, "t")}
    from public.transactions as t
    where t.user_id = ${userId}
      and t.id = ${transactionId}
    limit 1
  `;
  const row = rows[0] as Record<string, unknown> | undefined;
  if (!row) {
    return null;
  }

  const [accountRows, entityRows, categoryRows, importBatchRows, searchRows] =
    await Promise.all([
      sql`
        select id, display_name, institution_name, account_type, entity_id
        from public.accounts
        where user_id = ${userId}
          and id = ${String(row.account_id)}
        limit 1
      `,
      sql`
        select id, display_name, legal_name, entity_kind, slug
        from public.entities
        where user_id = ${userId}
          and id = ${String(row.economic_entity_id)}
        limit 1
      `,
      row.category_code
        ? sql`
            select code, display_name, scope_kind, direction_kind
            from public.categories
            where code = ${String(row.category_code)}
            limit 1
          `
        : Promise.resolve([]),
      row.import_batch_id
        ? sql`
            select id, original_filename, imported_at, status
            from public.import_batches
            where user_id = ${userId}
              and id = ${String(row.import_batch_id)}
            limit 1
          `
        : Promise.resolve([]),
      sql`
        select source_batch_key, original_text, contextualized_text, document_summary, review_state, direction
        from public.transaction_search_rows as r
        join public.transaction_search_batches as b
          on b.id = r.batch_id
        where r.user_id = ${userId}
          and r.transaction_id = ${transactionId}
        limit 1
      `,
    ]);

  return {
    transaction: mapFromSql<Transaction>(row),
    account: accountRows[0] ?? null,
    entity: entityRows[0] ?? null,
    category: categoryRows[0] ?? null,
    importBatch: importBatchRows[0] ?? null,
    searchRow: searchRows[0] ?? null,
  };
}

async function executeGetTransaction(
  sql: SqlClient,
  args: Record<string, unknown>,
  context: TransactionAgentToolContext,
): Promise<TransactionAgentToolResult> {
  const transactionId = textArg(args, "transactionId");
  if (!transactionId) {
    throw new Error("get_transaction requires transactionId.");
  }

  const contextRow = await selectTransactionContext(
    sql,
    context.userId,
    transactionId,
  );
  if (!contextRow) {
    return {
      summary: `Transaction ${transactionId} was not found.`,
      payload: { found: false, transactionId },
      evidence: [],
    };
  }

  const evidence = transactionEvidence(contextRow.transaction, {
    source: "get_transaction",
    account: contextRow.account,
    entity: contextRow.entity,
    category: contextRow.category,
    importBatch: contextRow.importBatch,
    searchRow: contextRow.searchRow,
  });

  return {
    summary: `Read transaction ${transactionId}.`,
    payload: {
      found: true,
      transaction: contextRow.transaction,
      account: contextRow.account,
      entity: contextRow.entity,
      category: contextRow.category,
      importBatch: contextRow.importBatch,
      searchRow: contextRow.searchRow,
    },
    evidence: [evidence],
  };
}

async function executeGetRelatedEntries(
  sql: SqlClient,
  args: Record<string, unknown>,
  context: TransactionAgentToolContext,
): Promise<TransactionAgentToolResult> {
  const transactionId = textArg(args, "transactionId");
  if (!transactionId) {
    throw new Error("get_related_entries requires transactionId.");
  }

  const source = await selectTransactionContext(
    sql,
    context.userId,
    transactionId,
  );
  if (!source) {
    return {
      summary: `Transaction ${transactionId} was not found.`,
      payload: { found: false, transactionId },
      evidence: [],
    };
  }

  const relatedRows = await sql`
    select ${transactionColumnsSql(sql, "t")}
    from public.transactions as t
    where t.user_id = ${context.userId}
      and t.id <> ${transactionId}
      and (
        (${source.transaction.relatedTransactionId ?? null}::uuid is not null and t.id = ${source.transaction.relatedTransactionId ?? null})
        or (${source.transaction.transferGroupId ?? null}::uuid is not null and t.transfer_group_id = ${source.transaction.transferGroupId ?? null})
        or t.related_transaction_id = ${transactionId}
        or t.correction_of_transaction_id = ${transactionId}
        or (${source.transaction.importBatchId ?? null}::uuid is not null and t.import_batch_id = ${source.transaction.importBatchId ?? null})
      )
    order by t.transaction_date desc, t.created_at desc
    limit 10
  `;
  const relatedTransactions = relatedRows.map((row) =>
    mapFromSql<Transaction>(row),
  );
  const auditRows = await sql`
    select id, command_name, object_type, object_id, notes, created_at
    from public.audit_events
    where actor_id = ${context.userId}
      and object_id = ${transactionId}
    order by created_at desc
    limit 8
  `;
  const evidence = relatedTransactions.map((transaction) =>
    transactionEvidence(transaction, {
      source: "get_related_entries",
      relatedToTransactionId: transactionId,
    }),
  );

  for (const auditRow of auditRows) {
    const auditId = String(auditRow.id ?? "");
    evidence.push({
      id: `audit_event:${auditId}`,
      type: "audit_event",
      title: `Audit event ${String(auditRow.command_name ?? "")}`,
      summary: String(auditRow.notes ?? auditRow.command_name ?? ""),
      sourceId: auditId,
      metadata: auditRow as Record<string, unknown>,
    });
  }

  return {
    summary: `Found ${relatedTransactions.length} related transaction${relatedTransactions.length === 1 ? "" : "s"} and ${auditRows.length} audit event${auditRows.length === 1 ? "" : "s"}.`,
    payload: {
      sourceTransactionId: transactionId,
      relatedTransactions,
      auditEvents: auditRows,
    },
    evidence,
  };
}

export async function executeTransactionAgentTool(input: {
  sql: SqlClient;
  toolName: TransactionAgentToolName;
  arguments: Record<string, unknown>;
  context: TransactionAgentToolContext;
}) {
  switch (input.toolName) {
    case "answer":
      return executeAnswer(input.arguments, input.context);
    case "hybrid_transaction_search":
      return executeHybridSearch(input.sql, input.arguments, input.context);
    case "sql_ledger_query":
      return executeSqlLedgerQuery(input.sql, input.arguments, input.context);
    case "get_transaction":
      return executeGetTransaction(input.sql, input.arguments, input.context);
    case "get_related_entries":
      return executeGetRelatedEntries(input.sql, input.arguments, input.context);
  }
}

export function summarizeTransactionAgentToolResult(
  toolName: TransactionAgentToolName,
  payload: Record<string, unknown>,
) {
  if (typeof payload.error === "string" && payload.error.trim()) {
    return [`${toolName} failed: ${payload.error}`];
  }

  if (toolName === "hybrid_transaction_search") {
    const rows = Array.isArray(payload.results) ? payload.results.slice(0, 3) : [];
    return rows.map((row) => {
      const result = row as Record<string, unknown>;
      return `transaction:${String(result.transactionId)} ${String(result.transactionDate)} ${String(result.amountOriginal)} ${String(result.currencyOriginal)} ${String(result.merchantNormalized ?? result.counterpartyName ?? result.descriptionRaw ?? "")}`;
    });
  }

  if (toolName === "sql_ledger_query") {
    return [
      `ledger_query:${String(payload.queryResultId ?? "")} returned ${String(payload.rowCount ?? 0)} rows.`,
    ];
  }

  if (toolName === "get_transaction") {
    const transaction = payload.transaction as Transaction | undefined;
    return transaction
      ? [`transaction:${transaction.id} ${summarizeTransaction(transaction)}`]
      : [`Transaction ${String(payload.transactionId ?? "")} was not found.`];
  }

  if (toolName === "get_related_entries") {
    return [
      `Related entries for ${String(payload.sourceTransactionId ?? "")}: ${Array.isArray(payload.relatedTransactions) ? payload.relatedTransactions.length : 0} transactions.`,
    ];
  }

  if (toolName === "answer") {
    return [`Final answer prepared.`];
  }

  return ["Tool result recorded."];
}

export function formatTransactionAgentObservation(
  toolName: TransactionAgentToolName,
  payload: Record<string, unknown>,
) {
  if (typeof payload.error === "string" && payload.error.trim()) {
    return [`Tool: ${toolName}`, `Error: ${payload.error}`].join("\n");
  }

  if (toolName === "hybrid_transaction_search") {
    return [
      "Tool: hybrid_transaction_search",
      `Query: ${String(payload.query ?? "")}`,
      `Rows: ${String(payload.resultCount ?? 0)}`,
      JSON.stringify(payload.results ?? [], null, 2),
    ].join("\n");
  }

  if (toolName === "sql_ledger_query") {
    return [
      "Tool: sql_ledger_query",
      `Query:\n${String(payload.query ?? "")}`,
      `Rows:\n${JSON.stringify(payload.rows ?? [], null, 2)}`,
    ].join("\n");
  }

  return [`Tool: ${toolName}`, JSON.stringify(payload, null, 2)].join("\n");
}

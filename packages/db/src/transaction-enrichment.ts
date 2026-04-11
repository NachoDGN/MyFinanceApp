import {
  enrichImportedTransaction,
  type TransactionEnrichmentDecision,
  type TransactionEnrichmentOptions,
} from "@myfinance/classification";
import {
  type Account,
  type DomainDataset,
  type Transaction,
} from "@myfinance/domain";

import { mapFromSql } from "./sql-json";
import { mergeEnrichmentDecisionWithExistingTransaction } from "./review-propagation-support";
import type { SqlClient } from "./sql-runtime";
import { transactionColumnsSql } from "./transaction-columns";
import { updateTransactionRecord } from "./transaction-record";

export async function selectTransactionRowById(
  sql: SqlClient,
  userId: string,
  transactionId: string,
) {
  const rows = await sql`
    select ${transactionColumnsSql(sql)}
    from public.transactions
    where id = ${transactionId}
      and user_id = ${userId}
    limit 1
  `;

  return rows[0] ?? null;
}

export async function updateTransactionFromEnrichmentDecision(
  sql: SqlClient,
  userId: string,
  transactionId: string,
  decision: TransactionEnrichmentDecision,
  options: {
    manualNotes?: string | null;
  } = {},
) {
  const beforeRow = await selectTransactionRowById(sql, userId, transactionId);
  const mergedDecision = beforeRow
    ? mergeEnrichmentDecisionWithExistingTransaction(
        mapFromSql<Transaction>(beforeRow),
        decision,
      )
    : decision;
  const updatePayload: Record<string, unknown> = {
    transaction_class: mergedDecision.transactionClass,
    category_code: mergedDecision.categoryCode ?? null,
    merchant_normalized: mergedDecision.merchantNormalized ?? null,
    counterparty_name: mergedDecision.counterpartyName ?? null,
    economic_entity_id: mergedDecision.economicEntityId,
    classification_status: mergedDecision.classificationStatus,
    classification_source: mergedDecision.classificationSource,
    classification_confidence: mergedDecision.classificationConfidence,
    quantity: mergedDecision.quantity ?? null,
    unit_price_original: mergedDecision.unitPriceOriginal ?? null,
    needs_review: mergedDecision.needsReview,
    review_reason: mergedDecision.reviewReason ?? null,
    updated_at: new Date().toISOString(),
  };
  if (options.manualNotes !== undefined) {
    updatePayload.manual_notes = options.manualNotes;
  }

  return updateTransactionRecord(sql, {
    userId,
    transactionId,
    updatePayload,
    llmPayload: mergedDecision.llmPayload,
  });
}

export async function executeTransactionEnrichmentPipeline(
  sql: SqlClient,
  userId: string,
  input: {
    dataset: DomainDataset;
    account: Account;
    transaction: Transaction;
    enrichmentOptions?: TransactionEnrichmentOptions;
    updateOptions?: {
      manualNotes?: string | null;
    };
  },
) {
  const decision = await enrichImportedTransaction(
    input.dataset,
    input.account,
    input.transaction,
    input.enrichmentOptions,
  );
  const afterRow = await updateTransactionFromEnrichmentDecision(
    sql,
    userId,
    input.transaction.id,
    decision,
    input.updateOptions,
  );

  return {
    decision,
    afterRow,
    afterTransaction: mapFromSql<Transaction>(afterRow),
  };
}

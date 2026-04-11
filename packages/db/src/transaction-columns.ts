import type { SqlClient } from "./sql-runtime";

export const TRANSACTION_SELECT_COLUMN_NAMES = [
  "id",
  "user_id",
  "account_id",
  "account_entity_id",
  "economic_entity_id",
  "import_batch_id",
  "provider_name",
  "provider_record_id",
  "source_fingerprint",
  "duplicate_key",
  "transaction_date",
  "posted_date",
  "amount_original",
  "currency_original",
  "amount_base_eur",
  "fx_rate_to_eur",
  "description_raw",
  "description_clean",
  "merchant_normalized",
  "counterparty_name",
  "transaction_class",
  "category_code",
  "subcategory_code",
  "transfer_group_id",
  "related_account_id",
  "related_transaction_id",
  "transfer_match_status",
  "cross_entity_flag",
  "reimbursement_status",
  "classification_status",
  "classification_source",
  "classification_confidence",
  "needs_review",
  "review_reason",
  "exclude_from_analytics",
  "correction_of_transaction_id",
  "voided_at",
  "manual_notes",
  "llm_payload",
  "raw_payload",
  "security_id",
  "quantity",
  "unit_price_original",
  "credit_card_statement_status",
  "linked_credit_card_account_id",
  "created_at",
  "updated_at",
] as const;

export const TRANSACTION_SELECT_COLUMNS =
  TRANSACTION_SELECT_COLUMN_NAMES.join(", ");

export function transactionColumnsSql(sql: SqlClient, alias?: string) {
  const prefix = alias ? `${alias}.` : "";
  return sql.unsafe(
    TRANSACTION_SELECT_COLUMN_NAMES.map((column) => `${prefix}${column}`).join(
      ", ",
    ),
  );
}

import { serializeJson } from "./sql-json";
import type { SqlClient } from "./sql-runtime";
import { transactionColumnsSql } from "./transaction-columns";

export async function updateTransactionRecord(
  sql: SqlClient,
  input: {
    userId: string;
    transactionId: string;
    updatePayload: Record<string, unknown>;
    llmPayload?: Record<string, unknown>;
    returning?: boolean;
  },
): Promise<Record<string, unknown> | null> {
  const llmPayloadSql =
    input.llmPayload !== undefined
      ? sql`, llm_payload = ${serializeJson(sql, input.llmPayload)}::jsonb`
      : sql``;

  if (input.returning !== false) {
    const rows = await sql`
      update public.transactions
      set ${sql(input.updatePayload)}${llmPayloadSql}
      where id = ${input.transactionId}
        and user_id = ${input.userId}
      returning ${transactionColumnsSql(sql)}
    `;
    if (!rows[0]) {
      throw new Error(
        `Transaction ${input.transactionId} was not found for update.`,
      );
    }
    return rows[0];
  }

  await sql`
    update public.transactions
    set ${sql(input.updatePayload)}${llmPayloadSql}
    where id = ${input.transactionId}
      and user_id = ${input.userId}
  `;
  return null;
}

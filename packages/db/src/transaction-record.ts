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
  if (input.returning === false) {
    if (input.llmPayload !== undefined) {
      await sql`
        update public.transactions
        set ${sql(input.updatePayload)},
            llm_payload = ${serializeJson(sql, input.llmPayload)}::jsonb
        where id = ${input.transactionId}
          and user_id = ${input.userId}
      `;
      return null;
    }

    await sql`
      update public.transactions
      set ${sql(input.updatePayload)}
      where id = ${input.transactionId}
        and user_id = ${input.userId}
    `;
    return null;
  }

  if (input.llmPayload !== undefined) {
    const rows = await sql`
      update public.transactions
      set ${sql(input.updatePayload)},
          llm_payload = ${serializeJson(sql, input.llmPayload)}::jsonb
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

  const rows = await sql`
    update public.transactions
    set ${sql(input.updatePayload)}
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

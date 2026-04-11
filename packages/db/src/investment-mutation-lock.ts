import type { SqlClient } from "./sql-runtime";

async function acquireInvestmentMutationLock(sql: SqlClient, userId: string) {
  await sql`
    select pg_advisory_lock(
      hashtext(${"investment_mutation"}),
      hashtext(${userId})
    )
  `;
}

async function releaseInvestmentMutationLock(sql: SqlClient, userId: string) {
  await sql`
    select pg_advisory_unlock(
      hashtext(${"investment_mutation"}),
      hashtext(${userId})
    )
  `;
}

export async function withInvestmentMutationLock<T>(
  sql: SqlClient,
  userId: string,
  runner: () => Promise<T>,
) {
  await acquireInvestmentMutationLock(sql, userId);
  try {
    return await runner();
  } finally {
    await releaseInvestmentMutationLock(sql, userId);
  }
}

import postgres from "postgres";

import { ensureWorkspaceRuntimeEnvLoaded } from "./runtime-env";

const DEFAULT_APP_USER_ID = "00000000-0000-0000-0000-000000000001";
const DEFAULT_LOCAL_DATABASE_URL =
  "postgresql://postgres:postgres@127.0.0.1:54322/postgres";

export interface DbRuntimeConfig {
  databaseUrl?: string;
  seededUserId: string;
}

export function getDbRuntimeConfig(): DbRuntimeConfig {
  ensureWorkspaceRuntimeEnvLoaded();
  const databaseUrl =
    process.env.DATABASE_URL?.trim() ||
    (process.env.NODE_ENV === "production"
      ? undefined
      : DEFAULT_LOCAL_DATABASE_URL);
  return {
    databaseUrl,
    seededUserId: process.env.APP_SEEDED_USER_ID ?? DEFAULT_APP_USER_ID,
  };
}

export function createSqlClient() {
  const { databaseUrl } = getDbRuntimeConfig();
  if (!databaseUrl) {
    throw new Error(
      "DATABASE_URL is required in production. In local development the app defaults to the local Supabase Postgres URL.",
    );
  }
  return postgres(databaseUrl, {
    max: 1,
    prepare: false,
    transform: {
      undefined: null,
    },
  });
}

export type SqlClient = ReturnType<typeof createSqlClient>;

export async function withSeededUserContext<T>(
  runner: (sql: SqlClient) => Promise<T>,
): Promise<T> {
  const sql = createSqlClient();
  const { seededUserId } = getDbRuntimeConfig();
  try {
    const beginTransaction = sql.begin as unknown as (
      callback: (transactionSql: SqlClient) => Promise<T>,
    ) => Promise<T>;
    return await beginTransaction(async (transactionSql) => {
      await transactionSql`select set_config('app.current_user_id', ${seededUserId}, true)`;
      return runner(transactionSql);
    });
  } finally {
    await sql.end({ timeout: 1 });
  }
}

export async function withSeededUserSession<T>(
  runner: (sql: SqlClient) => Promise<T>,
): Promise<T> {
  const sql = createSqlClient();
  const { seededUserId } = getDbRuntimeConfig();
  try {
    await sql`select set_config('app.current_user_id', ${seededUserId}, false)`;
    return await runner(sql);
  } finally {
    await sql.end({ timeout: 1 });
  }
}

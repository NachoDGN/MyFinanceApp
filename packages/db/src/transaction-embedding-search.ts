import { getReviewPropagationEmbeddingModel } from "@myfinance/classification";
import {
  normalizeInvestmentMatchingText,
  type Transaction,
} from "@myfinance/domain";
import {
  createTextEmbeddingClient,
  isTextEmbeddingConfigured,
  type TextEmbeddingClient,
} from "@myfinance/llm";

import { readOptionalRecord } from "./sql-json";
import type { SqlClient } from "./sql-runtime";

const TRANSACTION_DESCRIPTION_EMBEDDING_DIMENSIONS = 768;
export const DEFAULT_RESOLVED_REVIEW_SIMILARITY_THRESHOLD = 0.8;
export const DEFAULT_MAX_RESOLVED_REVIEW_SIMILAR_CONTEXT = 5;

export type TransactionEmbeddingSeedRow = {
  id: string;
  descriptionRaw: string;
  descriptionEmbedding: string | number[] | null;
};

export type SimilarUnresolvedTransactionMatch = {
  transactionId: string;
  similarity: number;
};

export type SimilarResolvedTransactionMatch =
  SimilarUnresolvedTransactionMatch;

export function readTransactionRawOutput(
  transaction: Transaction,
): Record<string, unknown> | null {
  const llmPayload = readOptionalRecord(transaction.llmPayload);
  const llmNode = readOptionalRecord(llmPayload?.llm);
  return readOptionalRecord(llmNode?.rawOutput);
}

export function readTransactionReviewContext(
  transaction: Transaction,
): Record<string, unknown> | null {
  return readOptionalRecord(
    readOptionalRecord(transaction.llmPayload)?.reviewContext,
  );
}

export function getReviewPropagationSimilarityThreshold() {
  const value = Number(
    process.env.REVIEW_PROPAGATION_SIMILARITY_THRESHOLD ?? "0.9",
  );
  if (!Number.isFinite(value) || value <= 0) {
    return 0.9;
  }

  // Resolved-source propagation must never become stricter than 0.9.
  return Math.min(value, 0.9);
}

export function normalizeStoredVectorLiteral(value: unknown) {
  if (typeof value === "string" && value.trim() !== "") {
    return value.trim();
  }
  if (Array.isArray(value)) {
    const numericValues = value.filter(
      (candidate): candidate is number =>
        typeof candidate === "number" && Number.isFinite(candidate),
    );
    return numericValues.length > 0 ? serializeVector(numericValues) : null;
  }
  return null;
}

export function serializeVector(values: number[]) {
  return `[${values
    .filter((value) => Number.isFinite(value))
    .map((value) => value.toString())
    .join(",")}]`;
}

export function parseTransactionEmbeddingSeedRow(
  row: Record<string, unknown>,
): TransactionEmbeddingSeedRow {
  return {
    id: typeof row.id === "string" ? row.id : "",
    descriptionRaw:
      typeof row.description_raw === "string" ? row.description_raw : "",
    descriptionEmbedding: normalizeStoredVectorLiteral(
      row.description_embedding,
    ),
  };
}

export async function ensureTransactionDescriptionEmbeddings(
  sql: SqlClient,
  userId: string,
  rows: readonly TransactionEmbeddingSeedRow[],
  embeddingClient?: TextEmbeddingClient | null,
) {
  const rowsMissingEmbeddings = rows.filter(
    (row) => !normalizeStoredVectorLiteral(row.descriptionEmbedding),
  );
  if (rowsMissingEmbeddings.length === 0) {
    return {
      generatedCount: 0,
      skippedCount: 0,
      skippedReason: null,
    };
  }

  let client = embeddingClient;
  if (client === undefined) {
    if (!isTextEmbeddingConfigured()) {
      return {
        generatedCount: 0,
        skippedCount: rowsMissingEmbeddings.length,
        skippedReason: "embedding_client_not_configured",
      };
    }

    try {
      client = createTextEmbeddingClient(getReviewPropagationEmbeddingModel());
    } catch {
      return {
        generatedCount: 0,
        skippedCount: rowsMissingEmbeddings.length,
        skippedReason: "embedding_client_unavailable",
      };
    }
  }

  if (!client) {
    return {
      generatedCount: 0,
      skippedCount: rowsMissingEmbeddings.length,
      skippedReason: "embedding_client_unavailable",
    };
  }

  const embeddings = await client.embedTexts({
    texts: rowsMissingEmbeddings.map(
      (row) => normalizeInvestmentMatchingText(row.descriptionRaw) || " ",
    ),
    taskType: "SEMANTIC_SIMILARITY",
    outputDimensionality: TRANSACTION_DESCRIPTION_EMBEDDING_DIMENSIONS,
  });

  for (const [index, row] of rowsMissingEmbeddings.entries()) {
    const vector = embeddings[index];
    if (!vector || vector.length === 0) {
      continue;
    }

    await sql`
      update public.transactions
      set description_embedding = ${serializeVector(vector)}::extensions.vector(768)
      where id = ${row.id}
        and user_id = ${userId}
    `;
  }

  return {
    generatedCount: embeddings.length,
    skippedCount: 0,
    skippedReason: null,
  };
}

export async function findSimilarUnresolvedTransactionsByDescriptionEmbedding(
  sql: SqlClient,
  input: {
    userId: string;
    sourceTransactionId: string;
    accountId: string;
    sourceEmbedding: string;
    threshold?: number;
    limit?: number;
  },
): Promise<SimilarUnresolvedTransactionMatch[]> {
  const threshold = input.threshold ?? getReviewPropagationSimilarityThreshold();
  const limit =
    input.limit && Number.isFinite(input.limit) && input.limit > 0
      ? Math.floor(input.limit)
      : 2147483647;
  const rows = await sql`
    select
      id,
      1 - (
        description_embedding <=>
        ${input.sourceEmbedding}::extensions.vector(768)
      ) as similarity
    from public.transactions
    where user_id = ${input.userId}
      and account_id = ${input.accountId}
      and id <> ${input.sourceTransactionId}
      and coalesce(needs_review, false) = true
      and voided_at is null
      and description_embedding is not null
      and 1 - (
        description_embedding <=>
        ${input.sourceEmbedding}::extensions.vector(768)
      ) >= ${threshold}
    order by
      description_embedding <=>
      ${input.sourceEmbedding}::extensions.vector(768) asc
    limit ${limit}
  `;

  return rows
    .map((row) => ({
      transactionId: typeof row.id === "string" ? row.id : "",
      similarity: Number(row.similarity ?? 0),
    }))
    .filter(
      (row) => row.transactionId !== "" && Number.isFinite(row.similarity),
    );
}

export async function findSimilarResolvedTransactionsByDescriptionEmbedding(
  sql: SqlClient,
  input: {
    userId: string;
    sourceTransactionId: string;
    accountId: string;
    sourceEmbedding: string;
    threshold?: number;
    limit?: number;
  },
): Promise<SimilarResolvedTransactionMatch[]> {
  const threshold =
    typeof input.threshold === "number" && Number.isFinite(input.threshold)
      ? input.threshold
      : DEFAULT_RESOLVED_REVIEW_SIMILARITY_THRESHOLD;
  const limit =
    input.limit && Number.isFinite(input.limit) && input.limit > 0
      ? Math.floor(input.limit)
      : DEFAULT_MAX_RESOLVED_REVIEW_SIMILAR_CONTEXT;
  const rows = await sql`
    select
      id,
      1 - (
        description_embedding <=>
        ${input.sourceEmbedding}::extensions.vector(768)
      ) as similarity
    from public.transactions
    where user_id = ${input.userId}
      and account_id = ${input.accountId}
      and id <> ${input.sourceTransactionId}
      and coalesce(needs_review, false) = false
      and voided_at is null
      and description_embedding is not null
      and 1 - (
        description_embedding <=>
        ${input.sourceEmbedding}::extensions.vector(768)
      ) >= ${threshold}
    order by
      description_embedding <=>
      ${input.sourceEmbedding}::extensions.vector(768) asc
    limit ${limit}
  `;

  return rows
    .map((row) => ({
      transactionId: typeof row.id === "string" ? row.id : "",
      similarity: Number(row.similarity ?? 0),
    }))
    .filter(
      (row) => row.transactionId !== "" && Number.isFinite(row.similarity),
    );
}

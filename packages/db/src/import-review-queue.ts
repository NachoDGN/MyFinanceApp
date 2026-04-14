import {
  buildAllowedCategoriesForAccount,
  needsTransactionManualReview,
  type DomainDataset,
  type Transaction,
} from "@myfinance/domain";

import { loadDatasetForUser } from "./dataset-loader";
import { mapFromSql, readOptionalRecord } from "./sql-json";
import {
  selectReviewPropagationCandidateMatches,
} from "./review-propagation-support";
import {
  getDbRuntimeConfig,
  withSeededUserContext,
  type SqlClient,
} from "./sql-runtime";

const DEFAULT_REVIEW_QUEUE_CANDIDATE_LIMIT = 100;
const DEFAULT_QUICK_CATEGORY_SUGGESTION_LIMIT = 4;
const QUICK_CATEGORY_SUGGESTION_PRIORITY = [
  "travel",
  "software",
  "office",
  "meals",
  "tax",
  "debt",
  "bank_fee",
  "client_payment",
  "government_subsidy",
  "other_expense",
  "other_income",
  "salary",
] as const;

export type ImportReviewQueueReadiness =
  | "waiting_for_classification"
  | "waiting_for_embeddings"
  | "ready"
  | "failed";

export interface ImportReviewQueueCategoryOption {
  code: string;
  displayName: string;
}

export interface ImportBatchReviewQueueTransaction {
  transactionId: string;
  accountId: string;
  accountDisplayName: string;
  transactionDate: string;
  postedDate: string | null;
  amountOriginal: string;
  currencyOriginal: string;
  descriptionRaw: string;
  reviewReason: string | null;
  manualNotes: string | null;
  categoryCode: string | null;
  transactionClass: string;
  categorySuggestions: ImportReviewQueueCategoryOption[];
  categoryOptions: ImportReviewQueueCategoryOption[];
}

export interface ImportBatchReviewQueueState {
  importBatchId: string;
  readiness: ImportReviewQueueReadiness;
  unresolvedCount: number;
  deferredSimilarCount: number;
  nextTransaction: ImportBatchReviewQueueTransaction | null;
  unresolvedTransactions: ImportBatchReviewQueueTransaction[];
  message: string | null;
}

function compareTransactionsNewestFirst(left: Transaction, right: Transaction) {
  if (left.transactionDate !== right.transactionDate) {
    return right.transactionDate.localeCompare(left.transactionDate);
  }
  return right.createdAt.localeCompare(left.createdAt);
}

function buildQueueTransactionRow(
  dataset: DomainDataset,
  transaction: Transaction,
): ImportBatchReviewQueueTransaction {
  const account =
    dataset.accounts.find((candidate) => candidate.id === transaction.accountId) ??
    null;
  const categoryOptions = account
    ? buildReviewQueueCategoryOptions(dataset, account, transaction)
    : [];

  return {
    transactionId: transaction.id,
    accountId: transaction.accountId,
    accountDisplayName:
      account?.displayName ?? transaction.accountId,
    transactionDate: transaction.transactionDate,
    postedDate: transaction.postedDate ?? null,
    amountOriginal: transaction.amountOriginal,
    currencyOriginal: transaction.currencyOriginal,
    descriptionRaw: transaction.descriptionRaw,
    reviewReason: transaction.reviewReason ?? null,
    manualNotes: transaction.manualNotes ?? null,
    categoryCode: transaction.categoryCode ?? null,
    transactionClass: transaction.transactionClass,
    categorySuggestions: buildQuickCategorySuggestions(categoryOptions),
    categoryOptions,
  };
}

function buildReviewQueueCategoryOptions(
  dataset: DomainDataset,
  account: DomainDataset["accounts"][number],
  transaction: Transaction,
): ImportReviewQueueCategoryOption[] {
  if (account.assetDomain !== "cash") {
    return [];
  }

  const numericAmount = Number(transaction.amountOriginal);
  const allowedDirectionKinds = new Set(
    numericAmount >= 0 ? ["income", "neutral"] : ["expense", "neutral"],
  );

  return buildAllowedCategoriesForAccount(dataset, account)
    .filter((category) => category.active)
    .filter((category) => !category.code.startsWith("uncategorized_"))
    .filter((category) => allowedDirectionKinds.has(category.directionKind))
    .sort(
      (left, right) =>
        left.sortOrder - right.sortOrder ||
        left.displayName.localeCompare(right.displayName),
    )
    .map((category) => ({
      code: category.code,
      displayName: category.displayName,
    }));
}

function buildQuickCategorySuggestions(
  categoryOptions: ImportReviewQueueCategoryOption[],
) {
  if (categoryOptions.length === 0) {
    return [];
  }

  const categoryByCode = new Map(
    categoryOptions.map((category) => [category.code, category]),
  );
  const prioritized = QUICK_CATEGORY_SUGGESTION_PRIORITY.flatMap((code) =>
    categoryByCode.has(code) ? [categoryByCode.get(code)!] : [],
  );
  const fallback = categoryOptions.filter(
    (category) =>
      !prioritized.some((candidate) => candidate.code === category.code),
  );

  return [...prioritized, ...fallback].slice(
    0,
    DEFAULT_QUICK_CATEGORY_SUGGESTION_LIMIT,
  );
}

type QueueReadinessInput = {
  classificationJobStatus: "queued" | "running" | "completed" | "failed" | null;
  classificationError: string | null;
  classificationPhase:
    | "search_index_bootstrap"
    | "parallel_first_pass"
    | "sequential_escalation"
    | "final_search_refresh"
    | null;
  hasUnresolvedTransactions: boolean;
  allUnresolvedEmbeddingsReady: boolean;
  latestSearchJobStatus: "queued" | "running" | "completed" | "failed" | null;
  latestSearchJobError: string | null;
};

export function resolveImportReviewQueueReadiness(
  input: QueueReadinessInput,
): { readiness: ImportReviewQueueReadiness; message: string | null } {
  if (input.classificationJobStatus === "failed") {
    return {
      readiness: "failed",
      message:
        input.classificationError ??
        "Import classification failed before review queue preparation finished.",
    };
  }

  const classificationDecisionsReady =
    input.classificationJobStatus === "completed" ||
    (input.classificationJobStatus === "running" &&
      input.classificationPhase === "final_search_refresh");

  if (
    input.classificationJobStatus === null ||
    input.classificationJobStatus === "queued" ||
    !classificationDecisionsReady
  ) {
    return {
      readiness: "waiting_for_classification",
      message: "Import classification is still running.",
    };
  }

  if (!input.hasUnresolvedTransactions || input.allUnresolvedEmbeddingsReady) {
    return {
      readiness: "ready",
      message: null,
    };
  }

  if (input.latestSearchJobStatus === "failed") {
    return {
      readiness: "failed",
      message:
        input.latestSearchJobError ??
        "Transaction search embeddings failed for this import batch.",
    };
  }

  return {
    readiness: "waiting_for_embeddings",
    message: "Waiting for transaction embeddings to finish indexing.",
  };
}

function readClassificationJobPhase(
  job: DomainDataset["jobs"][number] | null,
): QueueReadinessInput["classificationPhase"] {
  const progress = readOptionalRecord(readOptionalRecord(job?.payloadJson)?.progress);
  const phase = progress?.phase;
  return phase === "search_index_bootstrap" ||
    phase === "parallel_first_pass" ||
    phase === "sequential_escalation" ||
    phase === "final_search_refresh"
    ? phase
    : null;
}

async function readLatestBatchJob(
  sql: SqlClient,
  input: {
    jobType: "classification" | "transaction_search_index";
    importBatchId: string;
  },
) {
  const rows = await sql`
    select *
    from public.jobs
    where job_type = ${input.jobType}
      and (
        payload_json->>'importBatchId' = ${input.importBatchId}
        or ${input.jobType === "transaction_search_index"} and exists (
          select 1
          from jsonb_array_elements_text(
            coalesce(payload_json->'importBatchIds', '[]'::jsonb)
          ) as import_batch_id
          where import_batch_id = ${input.importBatchId}
        )
      )
    order by created_at desc
    limit 1
  `;

  return rows[0]
    ? mapFromSql<DomainDataset["jobs"][number]>(rows[0])
    : null;
}

async function readSearchRowReadiness(
  sql: SqlClient,
  input: {
    transactionIds: string[];
  },
) {
  if (input.transactionIds.length === 0) {
    return [];
  }

  return sql`
    select
      t.id as transaction_id,
      sr.transaction_id as indexed_transaction_id,
      sr.embedding is not null as has_embedding,
      sr.embedding_status,
      sb.status as batch_status
    from public.transactions as t
    left join public.transaction_search_rows as sr
      on sr.transaction_id = t.id
    left join public.transaction_search_batches as sb
      on sb.id = sr.batch_id
    where t.id in ${sql(input.transactionIds)}
  `;
}

async function readSimilarDeferredTransactionIds(
  sql: SqlClient,
  input: {
    userId: string;
    importBatchId: string;
    dataset: DomainDataset;
    unresolvedTransactions: Transaction[];
    reviewedSourceTransactionIds: string[];
  },
) {
  const deferredIds = new Set<string>();
  if (input.reviewedSourceTransactionIds.length === 0) {
    return deferredIds;
  }

  const unresolvedIds = input.unresolvedTransactions.map(
    (transaction) => transaction.id,
  );
  if (unresolvedIds.length === 0) {
    return deferredIds;
  }

  for (const sourceTransactionId of input.reviewedSourceTransactionIds) {
    const sourceTransaction =
      input.dataset.transactions.find(
        (transaction) => transaction.id === sourceTransactionId,
      ) ?? null;
    if (!sourceTransaction) {
      continue;
    }

    const account =
      input.dataset.accounts.find(
        (candidate) => candidate.id === sourceTransaction.accountId,
      ) ?? null;
    if (!account) {
      continue;
    }

    const rows = await sql`
      with source as (
        select embedding
        from public.transaction_search_rows
        where transaction_id = ${sourceTransactionId}
          and user_id = ${input.userId}
          and embedding is not null
        limit 1
      ),
      approximate_candidates as (
        select
          r.transaction_id,
          r.embedding
        from public.transaction_search_rows as r
        join public.transactions as t
          on t.id = r.transaction_id
        cross join source
        where r.user_id = ${input.userId}
          and r.account_id = ${account.id}
          and t.import_batch_id = ${input.importBatchId}
          and r.transaction_id in ${sql(unresolvedIds)}
          and r.transaction_id <> ${sourceTransactionId}
          and coalesce(t.needs_review, false) = true
          and t.voided_at is null
          and r.embedding_status in ('ready', 'stale')
        order by
          r.embedding::halfvec(3072) <=> source.embedding::halfvec(3072) asc
        limit ${Math.max(
          DEFAULT_REVIEW_QUEUE_CANDIDATE_LIMIT,
          unresolvedIds.length,
        )}
      )
      select
        candidate.transaction_id,
        1 - (candidate.embedding <=> source.embedding) as similarity
      from approximate_candidates as candidate
      cross join source
      order by candidate.embedding <=> source.embedding asc
    `;

    const embeddingMatches = rows.flatMap((row) => {
      const transactionId =
        typeof row.transaction_id === "string" ? row.transaction_id : "";
      const similarity = Number(row.similarity ?? 0);
      if (!transactionId || !Number.isFinite(similarity)) {
        return [];
      }

      return [{ transactionId, similarity }];
    });

    const candidateMatches = await selectReviewPropagationCandidateMatches({
      dataset: input.dataset,
      account,
      sourceTransaction,
      embeddingMatches,
    });

    for (const match of candidateMatches) {
      deferredIds.add(match.transactionId);
    }
  }

  return deferredIds;
}

export async function getImportBatchReviewQueueState(input: {
  importBatchId: string;
  reviewedSourceTransactionIds?: string[];
}) {
  const userId = getDbRuntimeConfig().seededUserId;

  return withSeededUserContext(async (sql) => {
    const dataset = await loadDatasetForUser(sql, userId);
    const importBatch = dataset.importBatches.find(
      (batch) => batch.id === input.importBatchId,
    );
    if (!importBatch) {
      throw new Error(`Import batch ${input.importBatchId} was not found.`);
    }

    const classificationJob = await readLatestBatchJob(sql, {
      jobType: "classification",
      importBatchId: input.importBatchId,
    });
    const searchJob = await readLatestBatchJob(sql, {
      jobType: "transaction_search_index",
      importBatchId: input.importBatchId,
    });

    const unresolvedTransactions = dataset.transactions
      .filter(
        (transaction) =>
          transaction.importBatchId === input.importBatchId &&
          !transaction.voidedAt &&
          needsTransactionManualReview(transaction),
      )
      .sort(compareTransactionsNewestFirst);

    const readinessRows = await readSearchRowReadiness(sql, {
      transactionIds: unresolvedTransactions.map((transaction) => transaction.id),
    });
    const allUnresolvedEmbeddingsReady = readinessRows.every(
      (row) =>
        typeof row.indexed_transaction_id === "string" &&
        row.has_embedding === true &&
        (row.embedding_status === "ready" || row.embedding_status === "stale"),
    );

    const readiness = resolveImportReviewQueueReadiness({
      classificationJobStatus: classificationJob?.status ?? null,
      classificationError: classificationJob?.lastError ?? null,
      classificationPhase: readClassificationJobPhase(classificationJob),
      hasUnresolvedTransactions: unresolvedTransactions.length > 0,
      allUnresolvedEmbeddingsReady,
      latestSearchJobStatus: searchJob?.status ?? null,
      latestSearchJobError: searchJob?.lastError ?? null,
    });

    if (readiness.readiness !== "ready") {
      return {
        importBatchId: input.importBatchId,
        readiness: readiness.readiness,
        unresolvedCount: unresolvedTransactions.length,
        deferredSimilarCount: 0,
        nextTransaction: null,
        unresolvedTransactions: unresolvedTransactions.map((transaction) =>
          buildQueueTransactionRow(dataset, transaction),
        ),
        message: readiness.message,
      } satisfies ImportBatchReviewQueueState;
    }

    const reviewedSourceTransactionIds = [
      ...new Set(
        (input.reviewedSourceTransactionIds ?? []).filter(
          (value) => typeof value === "string" && value.trim() !== "",
        ),
      ),
    ];
    const deferredTransactionIds = await readSimilarDeferredTransactionIds(sql, {
      userId,
      importBatchId: input.importBatchId,
      dataset,
      unresolvedTransactions,
      reviewedSourceTransactionIds,
    });
    const nextIndependentTransactions = unresolvedTransactions.filter(
      (transaction) =>
        !reviewedSourceTransactionIds.includes(transaction.id) &&
        !deferredTransactionIds.has(transaction.id),
    );
    const deferredSimilarCount = unresolvedTransactions.filter(
      (transaction) =>
        !reviewedSourceTransactionIds.includes(transaction.id) &&
        deferredTransactionIds.has(transaction.id),
    ).length;

    return {
      importBatchId: input.importBatchId,
      readiness: "ready",
      unresolvedCount: unresolvedTransactions.length,
      deferredSimilarCount,
      nextTransaction:
        nextIndependentTransactions[0] != null
          ? buildQueueTransactionRow(dataset, nextIndependentTransactions[0])
          : null,
      unresolvedTransactions: unresolvedTransactions.map((transaction) =>
        buildQueueTransactionRow(dataset, transaction),
      ),
      message: null,
    } satisfies ImportBatchReviewQueueState;
  });
}

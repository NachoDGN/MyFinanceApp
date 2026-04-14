import type { AuditEvent, DomainDataset } from "@myfinance/domain";

import { queueJob } from "./job-state";
import { mapFromSql } from "./sql-json";
import {
  getDbRuntimeConfig,
  withSeededUserContext,
  type SqlClient,
} from "./sql-runtime";

export type ReviewReanalysisMode =
  | "manual_review_update"
  | "manual_resolved_review";

export interface ReviewReanalysisProgress {
  stage:
    | "load_context"
    | "llm_reanalysis"
    | "apply_transaction_update"
    | "investment_rebuild"
    | "historical_price_lookup"
    | "metric_refresh"
    | "review_propagation";
  message: string;
  updatedAt?: string;
}

export interface QueueTransactionReviewReanalysisInput {
  transactionId: string;
  reviewContext?: string;
  selectedCategoryCode?: string | null;
  actorName: string;
  sourceChannel: AuditEvent["sourceChannel"];
}

export interface ReviewReanalysisFollowUpJobRef {
  id: string;
  jobType: "metric_refresh" | "review_propagation";
}

export interface ReviewReanalysisFollowUpJobStatus
  extends ReviewReanalysisFollowUpJobRef {
  status: "queued" | "running" | "completed" | "failed";
  createdAt: string;
  startedAt?: string | null;
  finishedAt?: string | null;
  lastError?: string | null;
}

export interface ReviewReanalysisJobStatus {
  id: string;
  jobType: string;
  status: "queued" | "running" | "completed" | "failed";
  createdAt: string;
  startedAt?: string | null;
  finishedAt?: string | null;
  lastError?: string | null;
  payloadJson: Record<string, unknown>;
  followUpJobs: ReviewReanalysisFollowUpJobStatus[];
}

function readUnknownArray(value: unknown) {
  return Array.isArray(value) ? value : null;
}

function normalizeJobProgress(value: unknown): ReviewReanalysisProgress | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  const record = value as Record<string, unknown>;
  const stage = typeof record.stage === "string" ? record.stage : null;
  const message = typeof record.message === "string" ? record.message : null;
  if (!stage || !message) {
    return null;
  }

  return {
    stage: stage as ReviewReanalysisProgress["stage"],
    message,
    updatedAt:
      typeof record.updatedAt === "string" ? record.updatedAt : undefined,
  };
}

function normalizeReviewReanalysisFollowUpJobRef(
  value: unknown,
): ReviewReanalysisFollowUpJobRef | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }

  const record = value as Record<string, unknown>;
  const id = typeof record.id === "string" ? record.id : null;
  const jobType =
    record.jobType === "metric_refresh" ||
    record.jobType === "review_propagation"
      ? record.jobType
      : null;
  if (!id || !jobType) {
    return null;
  }

  return {
    id,
    jobType,
  };
}

function normalizeReviewReanalysisFollowUpJobs(
  value: unknown,
): ReviewReanalysisFollowUpJobRef[] {
  return (readUnknownArray(value) ?? [])
    .map((entry) => normalizeReviewReanalysisFollowUpJobRef(entry))
    .filter((entry): entry is ReviewReanalysisFollowUpJobRef => Boolean(entry));
}

async function readReviewReanalysisFollowUpJobStatuses(
  sql: SqlClient,
  refs: ReviewReanalysisFollowUpJobRef[],
): Promise<ReviewReanalysisFollowUpJobStatus[]> {
  if (refs.length === 0) {
    return [];
  }

  const rows = await sql`
    select *
    from public.jobs
    where id in ${sql(refs.map((ref) => ref.id))}
  `;
  const jobsById = new Map(
    mapFromSql<DomainDataset["jobs"]>(rows).map((job) => [job.id, job]),
  );

  return refs.flatMap((ref) => {
    const job = jobsById.get(ref.id);
    if (!job) {
      return [];
    }

    return [
      {
        id: job.id,
        jobType: ref.jobType,
        status: job.status,
        createdAt: job.createdAt,
        startedAt: job.startedAt ?? null,
        finishedAt: job.finishedAt ?? null,
        lastError: job.lastError ?? null,
      },
    ];
  });
}

async function acquireReviewReanalysisQueueLock(
  sql: SqlClient,
  transactionId: string,
) {
  await sql`
    select pg_advisory_xact_lock(
      hashtext(${"review_reanalyze"}),
      hashtext(${transactionId})
    )
  `;
}

export async function queueTransactionReviewReanalysis(
  input: QueueTransactionReviewReanalysisInput,
) {
  const userId = getDbRuntimeConfig().seededUserId;

  return withSeededUserContext(async (sql) => {
    const transactionRows = await sql`
      select id, account_id, needs_review
      from public.transactions
      where id = ${input.transactionId}
        and user_id = ${userId}
      limit 1
    `;
    const transactionRow = transactionRows[0];
    if (!transactionRow) {
      throw new Error(`Transaction ${input.transactionId} not found.`);
    }
    const reviewMode: ReviewReanalysisMode =
      transactionRow.needs_review === true
        ? "manual_review_update"
        : "manual_resolved_review";

    const normalizedReviewContext = input.reviewContext?.trim() ?? "";
    const normalizedSelectedCategoryCode =
      typeof input.selectedCategoryCode === "string" &&
      input.selectedCategoryCode.trim() !== ""
        ? input.selectedCategoryCode.trim()
        : null;
    if (!normalizedReviewContext && !normalizedSelectedCategoryCode) {
      throw new Error(
        "Review input requires context or a selected category.",
      );
    }

    await acquireReviewReanalysisQueueLock(sql, input.transactionId);

    const existingRows = await sql`
      select *
      from public.jobs
      where job_type = ${"review_reanalyze"}
        and status in (${"queued"}, ${"running"})
        and payload_json->>'transactionId' = ${input.transactionId}
      order by created_at desc
      limit 1
    `;
    const existingJob = existingRows[0]
      ? mapFromSql<DomainDataset["jobs"]>(existingRows)[0]
      : null;
    if (existingJob) {
      return {
        queued: false,
        jobId: existingJob.id,
        status: existingJob.status,
      };
    }

    const jobId = await queueJob(sql, "review_reanalyze", {
      transactionId: input.transactionId,
      reviewContext: normalizedReviewContext,
      selectedCategoryCode: normalizedSelectedCategoryCode,
      reviewMode,
      actorName: input.actorName,
      sourceChannel: input.sourceChannel,
    });

    return {
      queued: true,
      jobId,
      status: "queued" as const,
    };
  });
}

export async function getReviewReanalysisJobStatus(jobId: string) {
  const userId = getDbRuntimeConfig().seededUserId;

  return withSeededUserContext(async (sql) => {
    const rows = await sql`
      select *
      from public.jobs
      where id = ${jobId}
        and job_type = ${"review_reanalyze"}
      limit 1
    `;
    const job = rows[0] ? mapFromSql<DomainDataset["jobs"]>(rows)[0] : null;
    if (!job) {
      throw new Error(`Review job ${jobId} not found.`);
    }

    const transactionId =
      typeof job.payloadJson.transactionId === "string"
        ? job.payloadJson.transactionId
        : null;
    if (!transactionId) {
      throw new Error(`Review job ${jobId} is missing transaction context.`);
    }

    const transactionRows = await sql`
      select id
      from public.transactions
      where id = ${transactionId}
        and user_id = ${userId}
      limit 1
    `;
    if (!transactionRows[0]) {
      throw new Error(`Review job ${jobId} is not available for this user.`);
    }

    const followUpJobs = await readReviewReanalysisFollowUpJobStatuses(
      sql,
      normalizeReviewReanalysisFollowUpJobs(job.payloadJson.followUpJobs),
    );

    return {
      ...job,
      payloadJson: {
        ...job.payloadJson,
        progress: normalizeJobProgress(job.payloadJson.progress),
      },
      followUpJobs,
    } satisfies ReviewReanalysisJobStatus;
  });
}

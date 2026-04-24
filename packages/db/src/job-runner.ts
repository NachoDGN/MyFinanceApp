import { randomUUID } from "node:crypto";

import { parseRuleDraftRequest, type JobRunResult } from "@myfinance/domain";
import type { PromptProfileOverrides } from "@myfinance/llm";

import { processClassificationJob } from "./classification-batch-job";
import { processFundNavBackfillJob } from "./fund-nav-backfill";
import { applyInvestmentRebuild } from "./investment-rebuild-runner";
import {
  claimNextQueuedJob,
  completeJob,
  failJob,
  recoverStaleRunningJobs,
  updateRunningJobPayload,
} from "./job-state";
import { loadDatasetForUser } from "./dataset-loader";
import { loadPromptOverrides } from "./prompt-profiles";
import { processRevolutSyncJob } from "./revolut-sync-job";
import {
  type ReviewReanalysisMode,
  type ReviewReanalysisProgress,
} from "./review-reanalysis";
import { processReviewPropagationJob } from "./review-propagation-job";
import { refreshFinanceAnalyticsArtifacts } from "./review-propagation-support";
import { parseJsonColumn } from "./sql-json";
import { withSeededUserSession, type SqlClient } from "./sql-runtime";
import { processTransactionSearchIndexJob } from "./transaction-search-index";

type ReviewReanalyzeJobHandler = (input: {
  transactionId: string;
  reviewContext: string;
  selectedCategoryCode: string | null;
  actorName: string;
  sourceChannel: "web" | "cli" | "worker" | "system";
  reviewMode?: ReviewReanalysisMode;
  propagateResolvedMatches: boolean;
  onProgress: (progress: ReviewReanalysisProgress) => Promise<void>;
}) => Promise<Record<string, unknown>>;

type JobHandlerInput = {
  sql: SqlClient;
  userId: string;
  jobId: string;
  payloadJson: Record<string, unknown>;
  getPromptOverrides: () => Promise<PromptProfileOverrides>;
  setPayload: (payloadJson: Record<string, unknown>) => Promise<void>;
  reanalyzeTransactionReview: ReviewReanalyzeJobHandler;
};

type JobHandler = (input: JobHandlerInput) => Promise<Record<string, unknown>>;

function requireString(
  payloadJson: Record<string, unknown>,
  key: string,
  message: string,
) {
  const value = payloadJson[key];
  if (typeof value === "string" && value.trim()) {
    return value;
  }
  throw new Error(message);
}

const jobHandlers: Record<string, JobHandler> = {
  async rule_parse({ sql, userId, payloadJson, getPromptOverrides }) {
    const requestText = requireString(
      payloadJson,
      "requestText",
      "Rule draft job is missing requestText.",
    );
    const parsedRule = await parseRuleDraftRequest(
      requestText,
      await loadDatasetForUser(sql, userId),
      await getPromptOverrides(),
    );
    return { ...payloadJson, parsedRule };
  },

  async classification({
    sql,
    userId,
    payloadJson,
    getPromptOverrides,
    setPayload,
  }) {
    const importBatchId = requireString(
      payloadJson,
      "importBatchId",
      "Classification job is missing importBatchId.",
    );
    let currentJobPayload = { ...payloadJson };
    const resultPayload = await processClassificationJob(sql, userId, {
      importBatchId,
      payloadJson,
      promptOverrides: await getPromptOverrides(),
      onProgress: async (nextPayloadJson) => {
        currentJobPayload = nextPayloadJson;
        await setPayload(currentJobPayload);
      },
    });

    return { ...currentJobPayload, ...resultPayload };
  },

  async bank_sync({ sql, userId, payloadJson }) {
    return {
      ...payloadJson,
      ...(await processRevolutSyncJob(sql, userId, payloadJson)),
    };
  },

  async transaction_search_index({ sql, userId, payloadJson }) {
    return {
      ...payloadJson,
      ...(await processTransactionSearchIndexJob(sql, userId, payloadJson)),
    };
  },

  async position_rebuild({ sql, userId, payloadJson }) {
    return {
      ...payloadJson,
      ...(await applyInvestmentRebuild(sql, userId)),
    };
  },

  async fund_nav_backfill({ sql, payloadJson }) {
    return {
      ...payloadJson,
      ...(await processFundNavBackfillJob(sql, payloadJson)),
    };
  },

  async metric_refresh({ sql, payloadJson }) {
    await refreshFinanceAnalyticsArtifacts(sql);
    return { ...payloadJson, refreshedAt: new Date().toISOString() };
  },

  async review_reanalyze({
    payloadJson,
    jobId,
    setPayload,
    reanalyzeTransactionReview,
  }) {
    const transactionId =
      typeof payloadJson.transactionId === "string"
        ? payloadJson.transactionId
        : "";
    const reviewContext =
      typeof payloadJson.reviewContext === "string"
        ? payloadJson.reviewContext
        : "";
    const actorName =
      typeof payloadJson.actorName === "string"
        ? payloadJson.actorName
        : "worker-review-editor";
    const reviewMode =
      payloadJson.reviewMode === "manual_resolved_review" ||
      payloadJson.reviewMode === "manual_review_update"
        ? (payloadJson.reviewMode as ReviewReanalysisMode)
        : undefined;
    const sourceChannel =
      typeof payloadJson.sourceChannel === "string" &&
      ["web", "cli", "worker", "system"].includes(payloadJson.sourceChannel)
        ? (payloadJson.sourceChannel as "web" | "cli" | "worker" | "system")
        : "worker";
    const selectedCategoryCode =
      typeof payloadJson.selectedCategoryCode === "string" &&
      payloadJson.selectedCategoryCode.trim() !== ""
        ? payloadJson.selectedCategoryCode.trim()
        : null;
    const propagateResolvedMatches =
      payloadJson.propagateResolvedMatches === true;

    if (!transactionId || (!reviewContext && !selectedCategoryCode)) {
      throw new Error(
        "Review reanalysis job is missing transactionId or review input.",
      );
    }

    const reportProgress = async (progress: ReviewReanalysisProgress) => {
      const nextPayloadJson = {
        ...payloadJson,
        progress: {
          ...progress,
          updatedAt: new Date().toISOString(),
        },
      };
      console.log(
        `[review_reanalyze] ${jobId} ${progress.stage}: ${progress.message}`,
      );
      await setPayload(nextPayloadJson);
    };

    await reportProgress({
      stage: "load_context",
      message: "Loading transaction context.",
    });

    return {
      ...payloadJson,
      ...(await reanalyzeTransactionReview({
        transactionId,
        reviewContext,
        selectedCategoryCode,
        actorName,
        sourceChannel,
        reviewMode,
        propagateResolvedMatches,
        onProgress: reportProgress,
      })),
    };
  },

  async review_propagation({ sql, userId, payloadJson, getPromptOverrides }) {
    return {
      ...payloadJson,
      ...(await processReviewPropagationJob(
        sql,
        userId,
        payloadJson,
        await getPromptOverrides(),
      )),
    };
  },
};

export async function runFinanceJobQueue(input: {
  apply: boolean;
  userId: string;
  reanalyzeTransactionReview: ReviewReanalyzeJobHandler;
}): Promise<JobRunResult> {
  return withSeededUserSession(async (sql) => {
    if (input.apply) {
      await recoverStaleRunningJobs(sql);
    }

    const queued = await sql`
      select * from public.jobs
      where status = 'queued'
        and available_at <= ${new Date().toISOString()}
      order by available_at asc, created_at asc
    `;
    const processedJobs: JobRunResult["processedJobs"] = [];

    if (input.apply && queued.length > 0) {
      const workerId = `worker:${process.pid}:${randomUUID()}`;
      let cachedPromptOverrides: PromptProfileOverrides | null = null;
      const getPromptOverrides = async () => {
        cachedPromptOverrides ??= await loadPromptOverrides(sql, input.userId);
        return cachedPromptOverrides;
      };

      while (true) {
        const job = await claimNextQueuedJob(sql, workerId);
        if (!job) break;

        const startedAt =
          typeof job.started_at === "string"
            ? job.started_at
            : new Date().toISOString();
        const payloadJson = parseJsonColumn<Record<string, unknown>>(
          job.payload_json ?? {},
        );

        try {
          const handler = jobHandlers[job.job_type];
          const completedPayload = handler
            ? await handler({
                sql,
                userId: input.userId,
                jobId: job.id,
                payloadJson,
                getPromptOverrides,
                setPayload: (nextPayloadJson) =>
                  updateRunningJobPayload(sql, job.id, nextPayloadJson),
                reanalyzeTransactionReview: input.reanalyzeTransactionReview,
              })
            : payloadJson;

          await completeJob(sql, job.id, startedAt, completedPayload);
          processedJobs.push({
            id: job.id,
            jobType: job.job_type,
            status: "completed",
          });
        } catch (error) {
          await failJob(sql, job.id, startedAt, error);
          processedJobs.push({
            id: job.id,
            jobType: job.job_type,
            status: "failed",
          });
        }
      }
    }

    return {
      schemaVersion: "v1",
      applied: input.apply,
      processedJobs: input.apply
        ? processedJobs
        : queued.map((job) => ({
            id: job.id,
            jobType: job.job_type,
            status: job.status,
          })),
      generatedAt: new Date().toISOString(),
    };
  });
}

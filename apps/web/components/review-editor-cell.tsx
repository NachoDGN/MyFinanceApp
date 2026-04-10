"use client";

import { getTransactionReviewState } from "@myfinance/domain/client";
import { useRouter } from "next/navigation";
import { useEffect, useState, useTransition } from "react";

import { ReviewStateCell } from "./primitives";

const REVIEW_JOB_POLL_INTERVAL_MS = 2_000;
const WORKER_QUEUE_WARNING_MS = 20_000;

type ReviewEditorCellProps = {
  transactionId: string;
  needsReview: boolean;
  reviewReason?: string | null;
  manualNotes?: string | null;
  transactionClass?: string | null;
  classificationSource?: string | null;
  securitySymbol?: string | null;
  quantity?: string | null;
  llmPayload?: unknown;
};

type ReviewJobStatusPayload = {
  status?: "queued" | "running" | "completed" | "failed";
  createdAt?: string;
  startedAt?: string | null;
  finishedAt?: string | null;
  lastError?: string | null;
  followUpJobs?: Array<{
    id: string;
    jobType?: "metric_refresh" | "review_propagation";
    status?: "queued" | "running" | "completed" | "failed";
    createdAt?: string;
    startedAt?: string | null;
    finishedAt?: string | null;
    lastError?: string | null;
  }>;
  payloadJson?: {
    changed?: boolean;
    changedFields?: string[];
    transaction?: { needsReview?: boolean | null } | null;
    progress?: {
      stage?: string;
      message?: string;
      updatedAt?: string;
    } | null;
  };
};

export function ReviewEditorCell({
  transactionId,
  needsReview,
  reviewReason,
  manualNotes,
  transactionClass,
  classificationSource,
  securitySymbol,
  quantity,
  llmPayload,
}: ReviewEditorCellProps) {
  const router = useRouter();
  const [draft, setDraft] = useState(manualNotes ?? "");
  const [feedback, setFeedback] = useState<string | null>(null);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [activeJobStatus, setActiveJobStatus] = useState<
    "queued" | "running" | null
  >(null);
  const [activeJobCreatedAt, setActiveJobCreatedAt] = useState<string | null>(
    null,
  );
  const [activeJobProgressMessage, setActiveJobProgressMessage] = useState<
    string | null
  >(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isRefreshing, startRefresh] = useTransition();

  const trimmedDraft = draft.trim();
  const reviewState = getTransactionReviewState({ needsReview, llmPayload });
  const isResolvedReview = reviewState === "resolved";
  const isPendingEnrichment = reviewState === "pending_enrichment";

  const formatChangedField = (field: string) =>
    ({
      transactionClass: "class",
      categoryCode: "category",
      merchantNormalized: "merchant",
      counterpartyName: "counterparty",
      economicEntityId: "economic entity",
      classificationStatus: "status",
      classificationSource: "source",
      classificationConfidence: "confidence",
      securityId: "security",
      quantity: "quantity",
      unitPriceOriginal: "unit price",
      needsReview: "review state",
      reviewReason: "review reason",
    })[field] ?? field;

  const getPendingFollowUpJob = (payload: ReviewJobStatusPayload) =>
    (payload.followUpJobs ?? []).find((job) => job.status === "running") ??
    (payload.followUpJobs ?? []).find((job) => job.status === "queued") ??
    null;

  const formatFollowUpProgressMessage = (
    job: NonNullable<ReturnType<typeof getPendingFollowUpJob>>,
  ) => {
    if (job.jobType === "review_propagation") {
      return job.status === "running"
        ? "Updating similar unresolved transactions in the background."
        : "Queued. Similar unresolved transactions will update in the background.";
    }

    return job.status === "running"
      ? "Refreshing portfolio metrics in the background."
      : "Queued. Portfolio metrics will refresh in the background.";
  };

  useEffect(() => {
    if (!activeJobId) {
      return;
    }

    let cancelled = false;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    const poll = async () => {
      try {
        const response = await fetch(`/api/review-jobs/${activeJobId}`, {
          cache: "no-store",
        });
        if (!response.ok) {
          const payload = (await response.json().catch(() => null)) as {
            error?: string;
          } | null;
          throw new Error(payload?.error || "Review job lookup failed.");
        }

        const payload = (await response
          .json()
          .catch(() => null)) as ReviewJobStatusPayload | null;
        if (cancelled || !payload?.status) {
          return;
        }

        if (payload.status === "completed") {
          const pendingFollowUpJob = getPendingFollowUpJob(payload);
          if (pendingFollowUpJob) {
            setActiveJobStatus(
              pendingFollowUpJob.status === "running" ? "running" : "queued",
            );
            setActiveJobCreatedAt(pendingFollowUpJob.createdAt ?? null);
            setActiveJobProgressMessage(
              formatFollowUpProgressMessage(pendingFollowUpJob),
            );
            setFeedback(
              pendingFollowUpJob.jobType === "review_propagation"
                ? "Transaction re-reviewed. Similar unresolved transactions are still updating."
                : "Transaction re-reviewed. Portfolio metrics are still refreshing.",
            );
            timeoutId = setTimeout(() => {
              void poll();
            }, REVIEW_JOB_POLL_INTERVAL_MS);
            return;
          }

          setActiveJobStatus(null);
          setActiveJobCreatedAt(null);
          setActiveJobProgressMessage(null);
          const changedFields = Array.isArray(
            payload.payloadJson?.changedFields,
          )
            ? payload.payloadJson.changedFields
            : [];
          if (!payload.payloadJson?.changed && changedFields.length === 0) {
            setFeedback(
              "Transaction re-reviewed. No visible changes were applied.",
            );
          } else if (payload.payloadJson?.transaction?.needsReview === false) {
            setFeedback(
              isResolvedReview
                ? "Resolved transaction re-reviewed."
                : "Transaction re-reviewed and resolved.",
            );
          } else {
            setFeedback(
              `Transaction re-reviewed. Updated ${changedFields
                .slice(0, 3)
                .map(formatChangedField)
                .join(", ")}${changedFields.length > 3 ? ", …" : ""}.`,
            );
          }
          setActiveJobId(null);
          startRefresh(() => {
            router.refresh();
          });
          return;
        }

        if (payload.status === "failed") {
          setActiveJobStatus(null);
          setActiveJobCreatedAt(null);
          setActiveJobProgressMessage(null);
          setFeedback(payload.lastError || "Review update failed.");
          setActiveJobId(null);
          startRefresh(() => {
            router.refresh();
          });
          return;
        }

        setActiveJobStatus(payload.status);
        setActiveJobCreatedAt(payload.createdAt ?? null);
        setActiveJobProgressMessage(
          typeof payload.payloadJson?.progress?.message === "string"
            ? payload.payloadJson.progress.message
            : null,
        );

        timeoutId = setTimeout(() => {
          void poll();
        }, REVIEW_JOB_POLL_INTERVAL_MS);
      } catch (error) {
        if (cancelled) {
          return;
        }
        setFeedback(
          error instanceof Error ? error.message : "Review job polling failed.",
        );
        setActiveJobStatus(null);
        setActiveJobCreatedAt(null);
        setActiveJobProgressMessage(null);
        setActiveJobId(null);
      }
    };

    void poll();

    return () => {
      cancelled = true;
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    };
  }, [activeJobId, isResolvedReview, router, startRefresh]);

  async function handleUpdate() {
    if (!trimmedDraft) {
      setFeedback("Add review context before updating.");
      return;
    }

    setFeedback(null);
    setIsSubmitting(true);
    try {
      const response = await fetch(
        `/api/transactions/${transactionId}/review`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            reviewContext: trimmedDraft,
          }),
        },
      );
      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as {
          error?: string;
        } | null;
        throw new Error(payload?.error || "Review update failed.");
      }

      const payload = (await response.json().catch(() => null)) as {
        queued?: boolean;
        jobId?: string;
        status?: string;
      } | null;
      if (!payload?.jobId) {
        throw new Error("Review job was queued without a job id.");
      }

      setActiveJobId(payload.jobId);
      setActiveJobStatus(payload.status === "running" ? "running" : "queued");
      setActiveJobCreatedAt(null);
      setActiveJobProgressMessage(null);
      setFeedback(
        payload.queued === false
          ? `${isResolvedReview ? "Resolved review" : "Review"} already queued. Waiting for the worker to finish.`
          : `${isResolvedReview ? "Resolved review" : "Review"} queued. Waiting for the worker to finish.`,
      );
    } catch (error) {
      setFeedback(
        error instanceof Error ? error.message : "Review update failed.",
      );
    } finally {
      setIsSubmitting(false);
    }
  }

  const isBusy = isSubmitting || Boolean(activeJobId) || isRefreshing;
  const queuedForMs =
    activeJobStatus === "queued" && activeJobCreatedAt
      ? Date.now() - new Date(activeJobCreatedAt).getTime()
      : 0;
  const progressMessage =
    activeJobStatus === "queued"
      ? (activeJobProgressMessage ??
        (queuedForMs >= WORKER_QUEUE_WARNING_MS
          ? "Queued for a while. The worker may not be running yet."
          : "Queued. Waiting for the worker to pick it up."))
      : activeJobStatus === "running"
        ? (activeJobProgressMessage ?? "Running analyzer and rebuild steps.")
        : null;

  return (
    <div style={{ display: "grid", gap: 8, minWidth: 260 }}>
      <ReviewStateCell
        needsReview={needsReview}
        reviewReason={reviewReason}
        transactionClass={transactionClass}
        classificationSource={classificationSource}
        securitySymbol={securitySymbol}
        quantity={quantity}
        llmPayload={llmPayload}
      />
      <textarea
        className="input-textarea"
        rows={3}
        value={draft}
        onChange={(event) => setDraft(event.target.value)}
        placeholder={
          isPendingEnrichment
            ? "Add optional context while automatic transaction analysis is still queued."
            : isResolvedReview
            ? "Explain why this resolved transaction should be reanalyzed from scratch."
            : reviewReason
              ? `Explain the correction. Current review: ${reviewReason}`
              : "Add context for the next LLM review."
        }
        style={{ minWidth: 260 }}
      />
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <button
          className="btn-pill"
          type="button"
          onClick={() => {
            void handleUpdate();
          }}
          disabled={isBusy || !trimmedDraft}
        >
          {isSubmitting
            ? "Queueing…"
            : activeJobId
              ? "Updating…"
              : isPendingEnrichment
                ? "Add context"
                : isResolvedReview
                  ? "Reanalyze"
                  : "Update"}
        </button>
        {feedback ? (
          <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
            {feedback}
          </span>
        ) : null}
      </div>
      {progressMessage ? (
        <span className="muted" style={{ fontSize: 12, lineHeight: 1.4 }}>
          {progressMessage}
        </span>
      ) : null}
    </div>
  );
}

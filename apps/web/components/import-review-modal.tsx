"use client";

import { useEffect, useRef, useState } from "react";

const REVIEW_QUEUE_POLL_INTERVAL_MS = 2_000;
const REVIEW_JOB_POLL_INTERVAL_MS = 2_000;

type ImportReviewQueueReadiness =
  | "waiting_for_classification"
  | "waiting_for_embeddings"
  | "ready"
  | "failed";

type ImportBatchReviewQueueTransaction = {
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
};

type ImportBatchReviewQueueState = {
  importBatchId: string;
  readiness: ImportReviewQueueReadiness;
  unresolvedCount: number;
  deferredSimilarCount: number;
  nextTransaction: ImportBatchReviewQueueTransaction | null;
  unresolvedTransactions: ImportBatchReviewQueueTransaction[];
  message: string | null;
};

type ReviewJobStatusPayload = {
  status?: "queued" | "running" | "completed" | "failed";
  lastError?: string | null;
  followUpJobs?: Array<{
    jobType?: "metric_refresh" | "review_propagation";
    status?: "queued" | "running" | "completed" | "failed";
  }>;
  payloadJson?: {
    progress?: {
      message?: string;
    } | null;
    transaction?: {
      needsReview?: boolean | null;
    } | null;
  };
};

function buildQueueUrl(
  importBatchId: string,
  reviewedSourceTransactionIds: string[],
) {
  const searchParams = new URLSearchParams();
  for (const transactionId of reviewedSourceTransactionIds) {
    searchParams.append("reviewedSourceTransactionId", transactionId);
  }

  const query = searchParams.toString();
  return `/api/imports/${importBatchId}/review-queue${
    query ? `?${query}` : ""
  }`;
}

async function fetchQueueState(
  importBatchId: string,
  reviewedSourceTransactionIds: string[],
) {
  const response = await fetch(
    buildQueueUrl(importBatchId, reviewedSourceTransactionIds),
    {
      cache: "no-store",
    },
  );
  if (!response.ok) {
    const payload = (await response.json().catch(() => null)) as {
      error?: string;
    } | null;
    throw new Error(payload?.error || "Import review queue lookup failed.");
  }

  return (await response.json()) as ImportBatchReviewQueueState;
}

function formatAmount(amount: string, currency: string) {
  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount)) {
    return `${amount} ${currency}`;
  }

  const absolute = Math.abs(numericAmount).toFixed(2);
  const sign = numericAmount > 0 ? "+" : numericAmount < 0 ? "-" : "";
  return `${sign}${absolute} ${currency}`;
}

function formatDate(value: string | null) {
  if (!value) {
    return null;
  }

  try {
    return new Intl.DateTimeFormat("en", {
      dateStyle: "medium",
      timeZone: "UTC",
    }).format(new Date(`${value}T00:00:00Z`));
  } catch {
    return value;
  }
}

function buildSessionNotice(input: {
  queueState: ImportBatchReviewQueueState;
  resolved: boolean;
  pendingPropagation: boolean;
  pendingMetrics: boolean;
}) {
  if (input.queueState.deferredSimilarCount > 0) {
    if (input.resolved && input.pendingPropagation) {
      return "No more independent unresolved transactions remain. Similar transactions are retrying in the background.";
    }

    return "No more independent unresolved transactions remain in this session. Similar transactions were deferred to avoid duplicate review.";
  }

  if (input.queueState.unresolvedCount === 0) {
    return input.pendingMetrics
      ? "The import review queue is complete. Background refresh jobs are still running."
      : "The import review queue is complete.";
  }

  return "No more independent unresolved transactions remain in this session.";
}

export function ImportReviewModal({
  importBatchId,
  onTrackedBatchSettled,
}: {
  importBatchId: string | null;
  onTrackedBatchSettled?: (importBatchId: string) => void;
}) {
  const [queueState, setQueueState] = useState<ImportBatchReviewQueueState | null>(
    null,
  );
  const [reviewedSourceTransactionIds, setReviewedSourceTransactionIds] =
    useState<string[]>([]);
  const [draft, setDraft] = useState("");
  const [feedback, setFeedback] = useState<string | null>(null);
  const [sessionNotice, setSessionNotice] = useState<string | null>(null);
  const [queueError, setQueueError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [activeJobStatus, setActiveJobStatus] = useState<
    "queued" | "running" | null
  >(null);
  const [activeJobMessage, setActiveJobMessage] = useState<string | null>(null);
  const [submittedTransactionId, setSubmittedTransactionId] = useState<
    string | null
  >(null);
  const [dismissed, setDismissed] = useState(false);
  const settledTrackedBatchIdRef = useRef<string | null>(null);

  const currentTransaction = queueState?.nextTransaction ?? null;
  const currentTransactionId = currentTransaction?.transactionId ?? null;

  useEffect(() => {
    setQueueState(null);
    setReviewedSourceTransactionIds([]);
    setDraft("");
    setFeedback(null);
    setSessionNotice(null);
    setQueueError(null);
    setIsSubmitting(false);
    setActiveJobId(null);
    setActiveJobStatus(null);
    setActiveJobMessage(null);
    setSubmittedTransactionId(null);
    setDismissed(false);
    settledTrackedBatchIdRef.current = null;
  }, [importBatchId]);

  useEffect(() => {
    if (!importBatchId || !onTrackedBatchSettled) {
      return;
    }

    const queueSettled =
      queueState?.readiness === "failed" ||
      (queueState?.readiness === "ready" && queueState.unresolvedCount === 0);
    if (!queueSettled || settledTrackedBatchIdRef.current === importBatchId) {
      return;
    }

    settledTrackedBatchIdRef.current = importBatchId;
    onTrackedBatchSettled(importBatchId);
  }, [
    importBatchId,
    onTrackedBatchSettled,
    queueState?.readiness,
    queueState?.unresolvedCount,
  ]);

  useEffect(() => {
    setDraft(currentTransaction?.manualNotes ?? "");
    setFeedback(null);
  }, [currentTransactionId, currentTransaction?.manualNotes]);

  useEffect(() => {
    if (!importBatchId || activeJobId) {
      return;
    }

    let cancelled = false;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    const poll = async () => {
      try {
        const nextQueueState = await fetchQueueState(
          importBatchId,
          reviewedSourceTransactionIds,
        );
        if (cancelled) {
          return;
        }

        setQueueState(nextQueueState);
        setQueueError(null);

        if (
          nextQueueState.readiness === "waiting_for_classification" ||
          nextQueueState.readiness === "waiting_for_embeddings"
        ) {
          timeoutId = setTimeout(() => {
            void poll();
          }, REVIEW_QUEUE_POLL_INTERVAL_MS);
          return;
        }

        if (nextQueueState.readiness === "failed") {
          setSessionNotice(
            nextQueueState.message ??
              "Import review queue preparation failed for this batch.",
          );
          return;
        }

        if (!nextQueueState.nextTransaction) {
          setSessionNotice(
            nextQueueState.deferredSimilarCount > 0
              ? "Independent unresolved transactions are exhausted for this session. Similar ones remain deferred."
              : nextQueueState.unresolvedCount === 0
                ? "This import batch has no unresolved transactions left to review."
                : "No more independent unresolved transactions remain in this session.",
          );
        }
      } catch (error) {
        if (cancelled) {
          return;
        }

        setQueueError(
          error instanceof Error
            ? error.message
            : "Import review queue polling failed.",
        );
      }
    };

    void poll();

    return () => {
      cancelled = true;
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    };
  }, [importBatchId, activeJobId, reviewedSourceTransactionIds]);

  useEffect(() => {
    if (!activeJobId || !importBatchId || !submittedTransactionId) {
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

        const payload = (await response.json()) as ReviewJobStatusPayload;
        if (cancelled || !payload.status) {
          return;
        }

        if (payload.status === "completed") {
          const nextReviewedSourceTransactionIds = [
            ...new Set([
              ...reviewedSourceTransactionIds,
              submittedTransactionId,
            ]),
          ];
          const nextQueueState = await fetchQueueState(
            importBatchId,
            nextReviewedSourceTransactionIds,
          );
          if (cancelled) {
            return;
          }

          const pendingPropagation = (payload.followUpJobs ?? []).some(
            (job) =>
              job.jobType === "review_propagation" &&
              (job.status === "queued" || job.status === "running"),
          );
          const pendingMetrics = (payload.followUpJobs ?? []).some(
            (job) =>
              job.jobType === "metric_refresh" &&
              (job.status === "queued" || job.status === "running"),
          );
          const resolved = payload.payloadJson?.transaction?.needsReview === false;

          setReviewedSourceTransactionIds(nextReviewedSourceTransactionIds);
          setQueueState(nextQueueState);
          setActiveJobId(null);
          setActiveJobStatus(null);
          setActiveJobMessage(null);
          setIsSubmitting(false);
          setSubmittedTransactionId(null);
          setQueueError(null);
          setSessionNotice(
            !nextQueueState.nextTransaction
              ? buildSessionNotice({
                  queueState: nextQueueState,
                  resolved,
                  pendingPropagation,
                  pendingMetrics,
                })
              : null,
          );
          return;
        }

        if (payload.status === "failed") {
          setFeedback(payload.lastError || "Review update failed.");
          setActiveJobId(null);
          setActiveJobStatus(null);
          setActiveJobMessage(null);
          setIsSubmitting(false);
          setSubmittedTransactionId(null);
          return;
        }

        setActiveJobStatus(payload.status);
        setActiveJobMessage(
          typeof payload.payloadJson?.progress?.message === "string"
            ? payload.payloadJson.progress.message
            : payload.status === "running"
              ? "Running analyzer and review update."
              : "Queued. Waiting for the worker to pick it up.",
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
        setActiveJobId(null);
        setActiveJobStatus(null);
        setActiveJobMessage(null);
        setIsSubmitting(false);
        setSubmittedTransactionId(null);
      }
    };

    void poll();

    return () => {
      cancelled = true;
      if (timeoutId) {
        clearTimeout(timeoutId);
      }
    };
  }, [
    activeJobId,
    importBatchId,
    reviewedSourceTransactionIds,
    submittedTransactionId,
  ]);

  async function handleSubmit() {
    if (!currentTransactionId || !draft.trim()) {
      setFeedback("Add review context before updating.");
      return;
    }

    setFeedback(null);
    setSessionNotice(null);
    setIsSubmitting(true);
    try {
      const response = await fetch(`/api/transactions/${currentTransactionId}/review`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          reviewContext: draft.trim(),
        }),
      });
      if (!response.ok) {
        const payload = (await response.json().catch(() => null)) as {
          error?: string;
        } | null;
        throw new Error(payload?.error || "Review update failed.");
      }

      const payload = (await response.json()) as {
        jobId?: string;
        queued?: boolean;
        status?: string;
      };
      if (!payload.jobId) {
        throw new Error("Review job was queued without a job id.");
      }

      setSubmittedTransactionId(currentTransactionId);
      setActiveJobId(payload.jobId);
      setActiveJobStatus(payload.status === "running" ? "running" : "queued");
      setActiveJobMessage(
        payload.queued === false
          ? "Review already queued. Waiting for the worker to finish."
          : "Review queued. Waiting for the worker to finish.",
      );
    } catch (error) {
      setFeedback(
        error instanceof Error ? error.message : "Review update failed.",
      );
      setIsSubmitting(false);
    }
  }

  if (!importBatchId) {
    return null;
  }

  const readinessMessage =
    queueError ??
    queueState?.message ??
    (queueState?.readiness === "ready" && !queueState.nextTransaction
      ? sessionNotice
      : null);
  const isReadyToReview =
    queueState?.readiness === "ready" && currentTransaction !== null;
  const isBusy = isSubmitting || Boolean(activeJobId);

  return (
    <>
      {readinessMessage || sessionNotice || dismissed ? (
        <div className="status-note import-review-inline-note">
          <strong>Import review queue</strong>
          <span>
            {readinessMessage ??
              sessionNotice ??
              "The guided review queue is paused for this batch."}
          </span>
          {dismissed && isReadyToReview ? (
            <button
              className="btn-ghost"
              type="button"
              onClick={() => {
                setDismissed(false);
                setSessionNotice(null);
              }}
            >
              Resume review
            </button>
          ) : null}
        </div>
      ) : null}

      {isReadyToReview && !dismissed ? (
        <div className="import-review-modal-backdrop">
          <div
            className="import-review-modal-card"
            role="dialog"
            aria-modal="true"
            aria-labelledby="import-review-modal-title"
          >
            <div className="import-review-modal-header">
              <div>
                <span className="label-sm">Guided Import Review</span>
                <h2
                  className="section-title"
                  id="import-review-modal-title"
                >
                  Review the next independent unresolved transaction
                </h2>
              </div>
              <div className="review-queue-meta">
                <span className="pill">
                  {queueState.unresolvedCount} unresolved
                </span>
                {queueState.deferredSimilarCount > 0 ? (
                  <span className="pill">
                    {queueState.deferredSimilarCount} deferred similar
                  </span>
                ) : null}
              </div>
            </div>

            <div className="import-review-modal-summary">
              <div>
                <span className="label-sm">Account</span>
                <div className="prompt-section-label">
                  {currentTransaction.accountDisplayName}
                </div>
              </div>
              <div>
                <span className="label-sm">Amount</span>
                <div className="prompt-section-label">
                  {formatAmount(
                    currentTransaction.amountOriginal,
                    currentTransaction.currencyOriginal,
                  )}
                </div>
              </div>
              <div>
                <span className="label-sm">Date</span>
                <div className="prompt-section-label">
                  {formatDate(currentTransaction.transactionDate) ??
                    currentTransaction.transactionDate}
                </div>
              </div>
            </div>

            <div className="builder-panel import-review-transaction-card">
              <span className="label-sm">Description</span>
              <p className="import-review-description">
                {currentTransaction.descriptionRaw}
              </p>
              <div className="review-queue-meta">
                <span className="pill">
                  {currentTransaction.transactionClass.replace(/_/g, " ")}
                </span>
                {currentTransaction.categoryCode ? (
                  <span className="pill">
                    {currentTransaction.categoryCode.replace(/_/g, " ")}
                  </span>
                ) : null}
                {currentTransaction.postedDate ? (
                  <span className="pill">
                    Posted {formatDate(currentTransaction.postedDate)}
                  </span>
                ) : null}
              </div>
              <p className="review-queue-reason">
                {currentTransaction.reviewReason ??
                  "The analyzer left this transaction unresolved."}
              </p>
            </div>

            <label className="input-label">
              User context
              <textarea
                className="input-textarea import-review-textarea"
                rows={5}
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
                placeholder="Explain how this transaction should be resolved, in terms that should help the analyzer handle similar rows in this account next time."
                disabled={isBusy}
              />
            </label>

            {feedback ? <div className="status-note">{feedback}</div> : null}
            {activeJobStatus ? (
              <div className="status-note">
                {activeJobMessage ??
                  (activeJobStatus === "running"
                    ? "Running analyzer and review update."
                    : "Queued. Waiting for the worker to pick it up.")}
              </div>
            ) : null}

            <div className="import-review-modal-actions">
              <button
                className="btn-ghost"
                type="button"
                disabled={isBusy}
                onClick={() => {
                  setDismissed(true);
                  setSessionNotice(
                    "Guided review paused. You can resume this batch from the inline notice below.",
                  );
                }}
              >
                Finish later
              </button>
              <button
                className="btn-pill"
                type="button"
                disabled={isBusy || draft.trim() === ""}
                onClick={() => {
                  void handleSubmit();
                }}
              >
                {isSubmitting
                  ? "Queueing..."
                  : activeJobId
                    ? "Updating..."
                    : "Apply context and continue"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}

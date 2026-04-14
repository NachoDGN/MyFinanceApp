"use client";

import { useRouter } from "next/navigation";
import { useEffect, useRef, useState, useTransition } from "react";

const REVIEW_QUEUE_POLL_INTERVAL_MS = 2_000;
const REVIEW_JOB_POLL_INTERVAL_MS = 2_000;

type ImportReviewQueueReadiness =
  | "waiting_for_classification"
  | "waiting_for_embeddings"
  | "ready"
  | "failed";

type ImportReviewQueueCategoryOption = {
  code: string;
  displayName: string;
};

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
  categorySuggestions: ImportReviewQueueCategoryOption[];
  categoryOptions: ImportReviewQueueCategoryOption[];
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

type BackgroundReviewJob = {
  jobId: string;
  transactionId: string;
  status: "queued" | "running" | "completed" | "failed";
  message: string | null;
  lastError: string | null;
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
  pendingBackgroundJobs: number;
}) {
  if (input.queueState.deferredSimilarCount > 0) {
    return input.pendingBackgroundJobs > 0
      ? "No more independent unresolved transactions remain. Similar transactions are deferred while dispatched reviews keep running in the background."
      : "No more independent unresolved transactions remain in this session. Similar transactions were deferred to avoid duplicate review.";
  }

  if (input.queueState.unresolvedCount === 0) {
    return input.pendingBackgroundJobs > 0
      ? "The import review queue is complete. Dispatched reviews are still finishing in the background."
      : "The import review queue is complete.";
  }

  return "No more independent unresolved transactions remain in this session.";
}

function buildBackgroundJobSummary(backgroundJobs: BackgroundReviewJob[]) {
  const pendingCount = backgroundJobs.filter(
    (job) => job.status === "queued" || job.status === "running",
  ).length;
  const failedCount = backgroundJobs.filter(
    (job) => job.status === "failed",
  ).length;

  if (pendingCount === 0 && failedCount === 0) {
    return null;
  }

  const parts = [];
  if (pendingCount > 0) {
    parts.push(
      `${pendingCount} dispatched review${pendingCount === 1 ? "" : "s"} still running in the background.`,
    );
  }
  if (failedCount > 0) {
    parts.push(
      `${failedCount} dispatched review${failedCount === 1 ? "" : "s"} failed and may need another pass later.`,
    );
  }

  return parts.join(" ");
}

export function ImportReviewModal({
  importBatchId,
  shouldAutoOpen = false,
  onAutoOpenHandled,
  onTrackedBatchSettled,
}: {
  importBatchId: string | null;
  shouldAutoOpen?: boolean;
  onAutoOpenHandled?: (importBatchId: string) => void;
  onTrackedBatchSettled?: (importBatchId: string) => void;
}) {
  const router = useRouter();
  const [, startRefresh] = useTransition();
  const [queueState, setQueueState] = useState<ImportBatchReviewQueueState | null>(
    null,
  );
  const [reviewedSourceTransactionIds, setReviewedSourceTransactionIds] =
    useState<string[]>([]);
  const [draft, setDraft] = useState("");
  const [selectedCategoryCode, setSelectedCategoryCode] = useState("");
  const [feedback, setFeedback] = useState<string | null>(null);
  const [sessionNotice, setSessionNotice] = useState<string | null>(null);
  const [queueError, setQueueError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [backgroundJobs, setBackgroundJobs] = useState<BackgroundReviewJob[]>([]);
  const [dismissed, setDismissed] = useState(false);
  const settledTrackedBatchIdRef = useRef<string | null>(null);
  const handledAutoOpenBatchIdRef = useRef<string | null>(null);

  const currentTransaction = queueState?.nextTransaction ?? null;
  const currentTransactionId = currentTransaction?.transactionId ?? null;
  const pendingBackgroundJobs = backgroundJobs.filter(
    (job) => job.status === "queued" || job.status === "running",
  ).length;
  const backgroundJobSummary = buildBackgroundJobSummary(backgroundJobs);

  useEffect(() => {
    setQueueState(null);
    setReviewedSourceTransactionIds([]);
    setDraft("");
    setSelectedCategoryCode("");
    setFeedback(null);
    setSessionNotice(null);
    setQueueError(null);
    setIsSubmitting(false);
    setBackgroundJobs([]);
    setDismissed(importBatchId ? !shouldAutoOpen : false);
    settledTrackedBatchIdRef.current = null;
    handledAutoOpenBatchIdRef.current = null;
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
    setSelectedCategoryCode("");
    setFeedback(null);
  }, [currentTransactionId, currentTransaction?.manualNotes]);

  useEffect(() => {
    if (
      !importBatchId ||
      !shouldAutoOpen ||
      !onAutoOpenHandled ||
      handledAutoOpenBatchIdRef.current === importBatchId
    ) {
      return;
    }

    if (queueState?.readiness !== "ready" || currentTransaction === null || dismissed) {
      return;
    }

    handledAutoOpenBatchIdRef.current = importBatchId;
    onAutoOpenHandled(importBatchId);
  }, [
    currentTransaction,
    dismissed,
    importBatchId,
    onAutoOpenHandled,
    queueState?.readiness,
    shouldAutoOpen,
  ]);

  useEffect(() => {
    if (!importBatchId) {
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
            buildSessionNotice({
              queueState: nextQueueState,
              pendingBackgroundJobs,
            }),
          );
        }

        timeoutId = setTimeout(() => {
          void poll();
        }, REVIEW_QUEUE_POLL_INTERVAL_MS);
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
  }, [importBatchId, pendingBackgroundJobs, reviewedSourceTransactionIds]);

  useEffect(() => {
    if (!importBatchId || pendingBackgroundJobs === 0) {
      return;
    }

    let cancelled = false;
    let timeoutId: ReturnType<typeof setTimeout> | null = null;

    const poll = async () => {
      const pendingJobs = backgroundJobs.filter(
        (job) => job.status === "queued" || job.status === "running",
      );
      if (pendingJobs.length === 0) {
        return;
      }

      try {
        const results = await Promise.all(
          pendingJobs.map(async (job) => {
            const response = await fetch(`/api/review-jobs/${job.jobId}`, {
              cache: "no-store",
            });
            if (!response.ok) {
              const payload = (await response.json().catch(() => null)) as {
                error?: string;
              } | null;
              throw new Error(payload?.error || "Review job lookup failed.");
            }

            return {
              jobId: job.jobId,
              payload: (await response.json()) as ReviewJobStatusPayload,
            };
          }),
        );
        if (cancelled) {
          return;
        }

        let shouldRefresh = false;
        let nextFeedback: string | null = null;
        const payloadByJobId = new Map(
          results.map((result) => [result.jobId, result.payload]),
        );

        setBackgroundJobs((current) =>
          current.map((job) => {
            const payload = payloadByJobId.get(job.jobId);
            if (!payload?.status) {
              return job;
            }

            const nextStatus = payload.status;
            const nextMessage =
              typeof payload.payloadJson?.progress?.message === "string"
                ? payload.payloadJson.progress.message
                : nextStatus === "running"
                  ? "Running analyzer and review update."
                  : nextStatus === "queued"
                    ? "Queued. Waiting for the worker to pick it up."
                    : null;

            if (nextStatus !== job.status) {
              shouldRefresh = true;
              if (nextStatus === "completed") {
                nextFeedback =
                  payload.payloadJson?.transaction?.needsReview === false
                    ? "A dispatched review resolved successfully."
                    : "A dispatched review finished, but the transaction still needs review.";
              } else if (nextStatus === "failed") {
                nextFeedback =
                  payload.lastError ?? "A dispatched review failed.";
              }
            }

            return {
              ...job,
              status: nextStatus,
              message: nextMessage,
              lastError: payload.lastError ?? null,
            };
          }),
        );

        if (nextFeedback) {
          setFeedback(nextFeedback);
        }
        if (shouldRefresh) {
          startRefresh(() => {
            router.refresh();
          });
        }

        const stillPending = results.some(
          (result) =>
            result.payload.status === "queued" || result.payload.status === "running",
        );
        if (stillPending) {
          timeoutId = setTimeout(() => {
            void poll();
          }, REVIEW_JOB_POLL_INTERVAL_MS);
        }
      } catch (error) {
        if (cancelled) {
          return;
        }

        setFeedback(
          error instanceof Error ? error.message : "Review job polling failed.",
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
  }, [backgroundJobs, importBatchId, pendingBackgroundJobs, router, startRefresh]);

  async function advanceQueue(anchorTransactionId: string) {
    if (!importBatchId) {
      return;
    }

    const nextReviewedSourceTransactionIds = [
      ...new Set([...reviewedSourceTransactionIds, anchorTransactionId]),
    ];
    setReviewedSourceTransactionIds(nextReviewedSourceTransactionIds);
    setDraft("");
    setSelectedCategoryCode("");

    try {
      const nextQueueState = await fetchQueueState(
        importBatchId,
        nextReviewedSourceTransactionIds,
      );
      setQueueState(nextQueueState);
      setQueueError(null);
      setSessionNotice(
        nextQueueState.nextTransaction
          ? null
          : buildSessionNotice({
              queueState: nextQueueState,
              pendingBackgroundJobs,
            }),
      );
    } catch (error) {
      setQueueError(
        error instanceof Error
          ? error.message
          : "Import review queue lookup failed.",
      );
    }
  }

  async function handlePass() {
    if (!currentTransactionId) {
      return;
    }

    setFeedback(null);
    setSessionNotice(null);
    await advanceQueue(currentTransactionId);
  }

  async function handleSubmit() {
    if (!currentTransactionId) {
      return;
    }

    const trimmedDraft = draft.trim();
    if (!trimmedDraft && !selectedCategoryCode) {
      setFeedback("Add review context or pick a category before updating.");
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
          reviewContext: trimmedDraft,
          selectedCategoryCode: selectedCategoryCode || undefined,
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
      const queuedJobId = payload.jobId;

      setBackgroundJobs((current) => {
        if (current.some((job) => job.jobId === queuedJobId)) {
          return current;
        }
        return [
          ...current,
          {
            jobId: queuedJobId,
            transactionId: currentTransactionId,
            status: payload.status === "running" ? "running" : "queued",
            message:
              payload.queued === false
                ? "Review already queued. Moving to the next independent transaction."
                : "Review dispatched. Moving to the next independent transaction.",
            lastError: null,
          },
        ];
      });
      await advanceQueue(currentTransactionId);
    } catch (error) {
      setFeedback(
        error instanceof Error ? error.message : "Review update failed.",
      );
    } finally {
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
  const canSubmit = draft.trim() !== "" || selectedCategoryCode !== "";

  return (
    <>
      {readinessMessage || sessionNotice || dismissed || backgroundJobSummary ? (
        <div className="status-note import-review-inline-note">
          <div style={{ display: "grid", gap: 4 }}>
            <strong>Import review queue</strong>
            <span>
              {readinessMessage ??
                sessionNotice ??
                backgroundJobSummary ??
                "The guided review queue is paused for this batch."}
            </span>
          </div>
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
                <h2 className="section-title" id="import-review-modal-title">
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

            {currentTransaction.categoryOptions.length > 0 ? (
              <div className="import-review-category-panel">
                <div>
                  <span className="label-sm">Quick category</span>
                  <div className="import-review-category-copy">
                    Pick a category when that is enough to unblock the transaction.
                  </div>
                </div>
                {currentTransaction.categorySuggestions.length > 0 ? (
                  <div className="import-review-category-grid">
                    {currentTransaction.categorySuggestions.map((category) => {
                      const isSelected = selectedCategoryCode === category.code;
                      return (
                        <button
                          key={category.code}
                          className={`import-review-category-chip${
                            isSelected ? " active" : ""
                          }`}
                          type="button"
                          onClick={() =>
                            setSelectedCategoryCode((current) =>
                              current === category.code ? "" : category.code,
                            )
                          }
                        >
                          {category.displayName}
                        </button>
                      );
                    })}
                  </div>
                ) : null}
                <label className="input-label">
                  Select another category
                  <select
                    className="input-select"
                    value={selectedCategoryCode}
                    onChange={(event) =>
                      setSelectedCategoryCode(event.target.value)
                    }
                    disabled={isSubmitting}
                  >
                    <option value="">Choose a category</option>
                    {currentTransaction.categoryOptions.map((category) => (
                      <option key={category.code} value={category.code}>
                        {category.displayName}
                      </option>
                    ))}
                  </select>
                </label>
              </div>
            ) : null}

            <label className="input-label">
              User context
              <textarea
                className="input-textarea import-review-textarea"
                rows={5}
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
                placeholder="Explain how this transaction should be resolved, in terms that should help the analyzer handle similar rows in this account next time."
                disabled={isSubmitting}
              />
            </label>

            {feedback ? <div className="status-note">{feedback}</div> : null}
            {backgroundJobSummary ? (
              <div className="status-note">{backgroundJobSummary}</div>
            ) : null}

            <div className="import-review-modal-actions">
              <button
                className="btn-ghost"
                type="button"
                disabled={isSubmitting}
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
                className="btn-ghost"
                type="button"
                disabled={isSubmitting}
                onClick={() => {
                  void handlePass();
                }}
              >
                Pass for now
              </button>
              <button
                className="btn-pill"
                type="button"
                disabled={isSubmitting || !canSubmit}
                onClick={() => {
                  void handleSubmit();
                }}
              >
                {isSubmitting ? "Dispatching..." : "Apply and continue"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </>
  );
}

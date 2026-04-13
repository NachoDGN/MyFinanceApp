export type RevolutSyncFeedbackJob = {
  id: string;
  jobType: string;
  status: string;
};

export type RevolutSyncFeedbackInput = {
  queued: boolean;
  inlineJobRun?: {
    processedJobs: RevolutSyncFeedbackJob[];
  } | null;
};

const REVOLUT_SYNC_JOB_TYPES = new Set([
  "bank_sync",
  "classification",
  "transaction_search_index",
]);

function selectRelevantRevolutJobs(processedJobs: RevolutSyncFeedbackJob[]) {
  return processedJobs.filter((job) => REVOLUT_SYNC_JOB_TYPES.has(job.jobType));
}

export function buildRevolutSyncFeedback(
  result: RevolutSyncFeedbackInput,
): string {
  const relevantJobs = selectRelevantRevolutJobs(
    result.inlineJobRun?.processedJobs ?? [],
  );

  if (relevantJobs.some((job) => job.status === "failed")) {
    return "Revolut sync ran, but some analyzer or indexing jobs failed.";
  }

  if (
    relevantJobs.some((job) => job.jobType === "transaction_search_index") ||
    (relevantJobs.some((job) => job.jobType === "bank_sync") &&
      relevantJobs.some((job) => job.jobType === "classification"))
  ) {
    return "Revolut sync completed. Transactions were analyzed and indexed.";
  }

  if (
    relevantJobs.some(
      (job) =>
        job.jobType === "bank_sync" || job.jobType === "classification",
    )
  ) {
    return "Revolut sync completed.";
  }

  return result.queued
    ? "Revolut sync queued."
    : "A Revolut sync is already queued or running.";
}

function formatUnknownError(error: unknown) {
  return error instanceof Error ? error.message : "unknown error";
}

function isEnvFlagEnabled(name: string) {
  return /^(1|true|yes)$/i.test(process.env[name] ?? "");
}

export function warnRevolutSyncExpensesSkipped(
  connectionId: string,
  error: unknown,
) {
  console.warn(
    `[revolut-sync] Expenses enrichment skipped for connection ${connectionId}: ${formatUnknownError(error)}`,
  );
}

export function logReviewReanalysisProgress(
  jobId: string,
  progress: {
    stage: string;
    message: string;
  },
) {
  console.log(
    `[review_reanalyze] ${jobId} ${progress.stage}: ${progress.message}`,
  );
}

export function redactApiKey(url: URL) {
  const copy = new URL(url);
  if (copy.searchParams.has("apikey")) {
    copy.searchParams.set("apikey", "***REDACTED***");
  }
  return copy.toString();
}

export function logTwelveDataDebug(
  event: string,
  details: Record<string, unknown>,
) {
  if (!isEnvFlagEnabled("TWELVE_DATA_DEBUG")) {
    return;
  }

  console.log(`[twelve-data] ${JSON.stringify({ event, ...details })}`);
}

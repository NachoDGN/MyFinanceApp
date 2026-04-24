import { queueRevolutConnectionSync } from "@myfinance/db";
import { type JobRunResult } from "@myfinance/domain";
import { domain } from "./action-service";
import { buildRevolutSyncFeedback } from "./revolut-sync-feedback";

export type RevolutInlineSyncResult = Awaited<
  ReturnType<typeof queueRevolutConnectionSync>
> & {
  inlineJobRun: JobRunResult;
};

type QueueRevolutConnectionSyncDeps = {
  queueRevolutConnectionSync: typeof queueRevolutConnectionSync;
  runPendingJobs: () => Promise<JobRunResult>;
};

const DEFAULT_QUEUE_DEPS: QueueRevolutConnectionSyncDeps = {
  queueRevolutConnectionSync,
  runPendingJobs: () => domain.runPendingJobs(true),
};

export async function queueAndRunRevolutSync(
  input: Parameters<typeof queueRevolutConnectionSync>[0],
  deps: QueueRevolutConnectionSyncDeps = DEFAULT_QUEUE_DEPS,
): Promise<RevolutInlineSyncResult> {
  const queued = await deps.queueRevolutConnectionSync(input);
  const inlineJobRun = await deps.runPendingJobs();
  return {
    ...queued,
    inlineJobRun,
  };
}

export { buildRevolutSyncFeedback };

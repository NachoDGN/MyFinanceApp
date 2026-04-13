import assert from "node:assert/strict";
import test from "node:test";

import {
  queueAndRunRevolutSync,
} from "../apps/web/lib/revolut-sync.ts";
import { buildRevolutSyncFeedback } from "../apps/web/lib/revolut-sync-feedback.ts";

test("queueAndRunRevolutSync drains the inline finance job queue after queueing", async () => {
  const calls: string[] = [];
  const result = await queueAndRunRevolutSync(
    {
      connectionId: "8f9b2689-2f6d-4d95-b3e7-a1b9444d0da7",
      trigger: "manual_sync",
    },
    {
      queueRevolutConnectionSync: async (input) => {
        calls.push(`queue:${input.connectionId}:${input.trigger}`);
        return {
          queued: true,
          jobId: "job-1",
        };
      },
      runPendingJobs: async () => {
        calls.push("runPendingJobs");
        return {
          schemaVersion: "v1",
          applied: true,
          processedJobs: [
            {
              id: "job-1",
              jobType: "bank_sync",
              status: "completed",
            },
          ],
          generatedAt: "2026-04-13T09:00:00.000Z",
        };
      },
    },
  );

  assert.deepEqual(calls, [
    "queue:8f9b2689-2f6d-4d95-b3e7-a1b9444d0da7:manual_sync",
    "runPendingJobs",
  ]);
  assert.equal(result.queued, true);
  assert.equal(result.inlineJobRun.processedJobs[0]?.jobType, "bank_sync");
});

test("buildRevolutSyncFeedback reports analyzed and indexed completion when the inline chain finishes", () => {
  assert.equal(
    buildRevolutSyncFeedback({
      queued: true,
      jobId: "job-1",
      inlineJobRun: {
        schemaVersion: "v1",
        applied: true,
        processedJobs: [
          {
            id: "job-1",
            jobType: "bank_sync",
            status: "completed",
          },
          {
            id: "job-2",
            jobType: "classification",
            status: "completed",
          },
          {
            id: "job-3",
            jobType: "transaction_search_index",
            status: "completed",
          },
        ],
        generatedAt: "2026-04-13T09:00:00.000Z",
      },
    }),
    "Revolut sync completed. Transactions were analyzed and indexed.",
  );
});

test("buildRevolutSyncFeedback surfaces analyzer failures from the Revolut sync chain", () => {
  assert.equal(
    buildRevolutSyncFeedback({
      queued: true,
      jobId: "job-1",
      inlineJobRun: {
        schemaVersion: "v1",
        applied: true,
        processedJobs: [
          {
            id: "job-1",
            jobType: "bank_sync",
            status: "completed",
          },
          {
            id: "job-2",
            jobType: "classification",
            status: "failed",
          },
        ],
        generatedAt: "2026-04-13T09:00:00.000Z",
      },
    }),
    "Revolut sync ran, but some analyzer or indexing jobs failed.",
  );
});

import assert from "node:assert/strict";
import test from "node:test";

import {
  getTransactionReviewState,
  isTransactionPendingEnrichment,
  isTransactionResolvedForAnalytics,
  needsTransactionManualReview,
} from "../packages/domain/src/finance.ts";

test("queued enrichment is not treated as manual review", () => {
  const transaction = {
    needsReview: true,
    excludeFromAnalytics: false,
    voidedAt: null,
    llmPayload: {
      analysisStatus: "pending",
    },
  } as const;

  assert.equal(isTransactionPendingEnrichment(transaction), true);
  assert.equal(needsTransactionManualReview(transaction), false);
  assert.equal(getTransactionReviewState(transaction), "pending_enrichment");
  assert.equal(isTransactionResolvedForAnalytics(transaction), false);
});

test("failed or unresolved analysis remains manual review", () => {
  const transaction = {
    needsReview: true,
    excludeFromAnalytics: false,
    voidedAt: null,
    llmPayload: {
      analysisStatus: "failed",
    },
  } as const;

  assert.equal(isTransactionPendingEnrichment(transaction), false);
  assert.equal(needsTransactionManualReview(transaction), true);
  assert.equal(getTransactionReviewState(transaction), "needs_review");
  assert.equal(isTransactionResolvedForAnalytics(transaction), false);
});

test("resolved transactions stay analytics-safe", () => {
  const transaction = {
    needsReview: false,
    excludeFromAnalytics: false,
    voidedAt: null,
    llmPayload: {
      analysisStatus: "done",
    },
  } as const;

  assert.equal(getTransactionReviewState(transaction), "resolved");
  assert.equal(isTransactionResolvedForAnalytics(transaction), true);
});

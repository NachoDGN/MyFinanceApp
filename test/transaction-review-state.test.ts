import assert from "node:assert/strict";
import test from "node:test";

import {
  getTransactionReviewReason,
  getTransactionReviewState,
  isTransactionPendingEnrichment,
  isTransactionResolvedForAnalytics,
  needsTransactionManualReview,
} from "../packages/domain/src/transaction-review.ts";

test("queued enrichment is not treated as manual review", () => {
  const transaction = {
    needsReview: true,
    creditCardStatementStatus: "not_applicable",
    descriptionRaw: "Coffee",
    descriptionClean: "COFFEE",
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
    creditCardStatementStatus: "not_applicable",
    descriptionRaw: "Coffee",
    descriptionClean: "COFFEE",
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
    categoryCode: "salary",
    creditCardStatementStatus: "not_applicable",
    descriptionRaw: "Salary",
    descriptionClean: "SALARY",
    excludeFromAnalytics: false,
    voidedAt: null,
    llmPayload: {
      analysisStatus: "done",
    },
  } as const;

  assert.equal(getTransactionReviewState(transaction), "resolved");
  assert.equal(isTransactionResolvedForAnalytics(transaction), true);
});

test("uncategorized transactions remain in manual review until a category is assigned", () => {
  const transaction = {
    needsReview: false,
    categoryCode: "uncategorized_expense",
    creditCardStatementStatus: "not_applicable",
    descriptionRaw: "Booking.com",
    descriptionClean: "BOOKING.COM",
    excludeFromAnalytics: false,
    voidedAt: null,
    llmPayload: {
      analysisStatus: "done",
    },
    reviewReason: null,
  } as const;

  assert.equal(needsTransactionManualReview(transaction), true);
  assert.equal(getTransactionReviewState(transaction), "needs_review");
  assert.equal(
    getTransactionReviewReason(transaction),
    "Assign a category before this transaction can be treated as resolved.",
  );
  assert.equal(isTransactionResolvedForAnalytics(transaction), false);
});

test("credit-card settlements stay in manual review until the statement is uploaded", () => {
  const transaction = {
    needsReview: false,
    creditCardStatementStatus: "upload_required",
    descriptionRaw: "Liquidacion de las tarjetas de credito del contrato 123",
    descriptionClean: "LIQUIDACION DE LAS TARJETAS DE CREDITO DEL CONTRATO 123",
    excludeFromAnalytics: false,
    voidedAt: null,
    llmPayload: {
      analysisStatus: "done",
    },
    reviewReason: null,
  } as const;

  assert.equal(isTransactionPendingEnrichment(transaction), false);
  assert.equal(needsTransactionManualReview(transaction), true);
  assert.equal(getTransactionReviewState(transaction), "needs_review");
  assert.equal(
    getTransactionReviewReason(transaction),
    "Upload the matching credit-card statement to resolve category KPIs.",
  );
  assert.equal(isTransactionResolvedForAnalytics(transaction), false);
});

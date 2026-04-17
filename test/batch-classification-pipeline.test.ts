import assert from "node:assert/strict";
import test from "node:test";

import { buildPromptProfilePreview } from "../packages/llm/src/prompts";
import {
  getBatchClassificationFirstPassConcurrency,
  getBatchClassificationPhase,
  isTrustedBatchResolution,
  shouldEscalateBatchTransaction,
} from "../packages/db/src/classification-batch-job";

test("deterministic resolved transactions are trusted immediately", () => {
  assert.equal(
    isTrustedBatchResolution({
      needsReview: false,
      classificationSource: "user_rule",
      classificationConfidence: "1.00",
    }),
    true,
  );
});

test("LLM resolutions need higher confidence before they become trusted exemplars", () => {
  assert.equal(
    isTrustedBatchResolution({
      needsReview: false,
      classificationSource: "llm",
      classificationConfidence: "0.84",
    }),
    false,
  );
  assert.equal(
    isTrustedBatchResolution({
      needsReview: false,
      classificationSource: "llm",
      classificationConfidence: "0.86",
    }),
    true,
  );
});

test("sequentially escalated unresolved transactions are not re-queued for escalation", () => {
  assert.equal(
    getBatchClassificationPhase({
      llmPayload: {
        batchPipeline: {
          phase: "sequential_escalation",
        },
      },
    }),
    "sequential_escalation",
  );

  assert.equal(
    shouldEscalateBatchTransaction({
      needsReview: true,
      llmPayload: {
        batchPipeline: {
          phase: "parallel_first_pass",
        },
      },
    }),
    true,
  );

  assert.equal(
    shouldEscalateBatchTransaction({
      needsReview: true,
      llmPayload: {
        batchPipeline: {
          phase: "sequential_escalation",
        },
      },
    }),
    false,
  );
});

test("transaction analyzer prompt previews expose batch-context placeholders", () => {
  const cashPreview = buildPromptProfilePreview("cash_transaction_analyzer");
  const investmentPreview = buildPromptProfilePreview(
    "investment_transaction_analyzer",
  );

  assert.match(cashPreview.userPrompt, /Batch summary: \{\{batch_summary\}\}\./);
  assert.match(
    investmentPreview.userPrompt,
    /Retriever context for this row: \{\{retrieval_context\}\}\./,
  );
});

test("batch classification first pass concurrency scales with import size up to 200", () => {
  assert.equal(getBatchClassificationFirstPassConcurrency(1), 1);
  assert.equal(getBatchClassificationFirstPassConcurrency(12), 12);
  assert.equal(getBatchClassificationFirstPassConcurrency(229), 200);
});

test("batch classification first pass concurrency honors an env cap", () => {
  const previous = process.env.BATCH_TRANSACTION_CLASSIFICATION_CONCURRENCY;
  process.env.BATCH_TRANSACTION_CLASSIFICATION_CONCURRENCY = "24";

  try {
    assert.equal(getBatchClassificationFirstPassConcurrency(12), 12);
    assert.equal(getBatchClassificationFirstPassConcurrency(229), 24);
  } finally {
    if (previous === undefined) {
      delete process.env.BATCH_TRANSACTION_CLASSIFICATION_CONCURRENCY;
    } else {
      process.env.BATCH_TRANSACTION_CLASSIFICATION_CONCURRENCY = previous;
    }
  }
});

import assert from "node:assert/strict";
import test from "node:test";

import {
  getTransactionSearchContextualizationConcurrency,
  normalizeTransactionSearchIndexJobPayload,
} from "../packages/db/src/transaction-search-index";

test("normalizeTransactionSearchIndexJobPayload accepts legacy singular scope keys", () => {
  const normalized = normalizeTransactionSearchIndexJobPayload({
    transactionId: "tx-1",
    importBatchId: "batch-1",
    accountId: "account-1",
    entityId: "entity-1",
  });

  assert.deepEqual(normalized, {
    transactionIds: ["tx-1"],
    importBatchIds: ["batch-1"],
    accountIds: ["account-1"],
    entityIds: ["entity-1"],
    trigger: "unknown",
  });
});

test("normalizeTransactionSearchIndexJobPayload merges singular and plural scope keys", () => {
  const normalized = normalizeTransactionSearchIndexJobPayload({
    transactionId: "tx-1",
    transactionIds: ["tx-2", "tx-1", ""],
    importBatchIds: ["batch-2"],
    importBatchId: "batch-1",
    accountIds: ["account-1"],
    trigger: "classification_completion",
  });

  assert.deepEqual(normalized, {
    transactionIds: ["tx-2", "tx-1"],
    importBatchIds: ["batch-2", "batch-1"],
    accountIds: ["account-1"],
    entityIds: [],
    trigger: "classification_completion",
  });
});

test("transaction search contextualization concurrency scales with batch size up to 200", () => {
  assert.equal(getTransactionSearchContextualizationConcurrency(1), 1);
  assert.equal(getTransactionSearchContextualizationConcurrency(25), 25);
  assert.equal(getTransactionSearchContextualizationConcurrency(229), 200);
});

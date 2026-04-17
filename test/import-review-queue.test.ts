import assert from "node:assert/strict";
import test from "node:test";

import {
  buildHistoricalReviewExamples,
  buildReviewPromptExamples,
} from "../packages/classification/src/investment-support.ts";
import {
  buildReviewQueueCategoryOptions,
  resolveImportReviewQueueReadiness,
} from "../packages/db/src/import-review-queue.ts";
import { selectReviewPropagationCandidateMatches } from "../packages/db/src/review-propagation-support.ts";
import {
  createAccount,
  createDataset,
  createTransaction,
} from "./support/create-dataset";

test("import review queue readiness waits for classification before embeddings", () => {
  assert.deepEqual(
    resolveImportReviewQueueReadiness({
      classificationJobStatus: "running",
      classificationError: null,
      classificationPhase: "parallel_first_pass",
      hasUnresolvedTransactions: true,
      allUnresolvedEmbeddingsReady: false,
      latestSearchJobStatus: "completed",
      latestSearchJobError: null,
    }),
    {
      readiness: "waiting_for_classification",
      message: "Import classification is still running.",
    },
  );
});

test("import review queue readiness waits for embeddings after classification", () => {
  assert.deepEqual(
    resolveImportReviewQueueReadiness({
      classificationJobStatus: "completed",
      classificationError: null,
      classificationPhase: "final_search_refresh",
      hasUnresolvedTransactions: true,
      allUnresolvedEmbeddingsReady: false,
      latestSearchJobStatus: "running",
      latestSearchJobError: null,
    }),
    {
      readiness: "waiting_for_embeddings",
      message: "Waiting for transaction embeddings to finish indexing.",
    },
  );
});

test("import review queue readiness fails when transaction search indexing fails", () => {
  assert.deepEqual(
    resolveImportReviewQueueReadiness({
      classificationJobStatus: "completed",
      classificationError: null,
      classificationPhase: "final_search_refresh",
      hasUnresolvedTransactions: true,
      allUnresolvedEmbeddingsReady: false,
      latestSearchJobStatus: "failed",
      latestSearchJobError: "Search indexing failed.",
    }),
    {
      readiness: "failed",
      message: "Search indexing failed.",
    },
  );
});

test("import review queue treats final search refresh as classification-ready", () => {
  assert.deepEqual(
    resolveImportReviewQueueReadiness({
      classificationJobStatus: "running",
      classificationError: null,
      classificationPhase: "final_search_refresh",
      hasUnresolvedTransactions: true,
      allUnresolvedEmbeddingsReady: true,
      latestSearchJobStatus: "completed",
      latestSearchJobError: null,
    }),
    {
      readiness: "ready",
      message: null,
    },
  );
});

test("historical review examples ignore unresolved manual re-reviews", () => {
  const account = createAccount({
    id: "cash-history-account",
    assetDomain: "cash",
    accountType: "checking",
  });
  const transaction = createTransaction({
    id: "cash-history-transaction",
    accountId: account.id,
    descriptionRaw: "SUPERMARKET MADRID",
    descriptionClean: "SUPERMARKET MADRID",
    transactionClass: "unknown",
    categoryCode: null,
    needsReview: true,
    reviewReason: "Still unresolved.",
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [transaction],
    auditEvents: [
      {
        id: "audit-unresolved-review",
        actorType: "user",
        actorId: "seed-user",
        actorName: "tester",
        sourceChannel: "web",
        commandName: "transactions.review_reanalyze",
        objectType: "transaction",
        objectId: transaction.id,
        beforeJson: {
          ...transaction,
          llmPayload: {
            llm: {
              model: "gpt-5.4",
            },
          },
        },
        afterJson: {
          ...transaction,
          manualNotes: "This is groceries.",
          llmPayload: {
            reviewContext: {
              userProvidedContext: "This is groceries.",
            },
          },
        },
        createdAt: "2026-04-14T10:00:00.000Z",
        notes: null,
      },
    ],
  });

  const examples = buildHistoricalReviewExamples(dataset, account, transaction);

  assert.deepEqual(examples, []);
});

test("review prompt examples include active same-account learned examples", () => {
  const account = createAccount({
    id: "cash-learned-account",
    assetDomain: "cash",
    accountType: "checking",
    institutionName: "Santander",
  });
  const sourceTransaction = createTransaction({
    id: "cash-learned-source",
    accountId: account.id,
    descriptionRaw: "AMAZON MARKETPLACE MADRID",
    descriptionClean: "AMAZON MARKETPLACE MADRID",
    needsReview: true,
    reviewReason: "Unknown merchant.",
    transactionClass: "unknown",
    categoryCode: null,
  });
  const targetTransaction = createTransaction({
    id: "cash-learned-target",
    accountId: account.id,
    descriptionRaw: "AMAZON MARKETPLACE BCN",
    descriptionClean: "AMAZON MARKETPLACE BCN",
    needsReview: true,
    reviewReason: "Unknown merchant.",
    transactionClass: "unknown",
    categoryCode: null,
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [sourceTransaction, targetTransaction],
    learnedReviewExamples: [
      {
        id: "learned-example-amazon",
        userId: "seed-user",
        accountId: account.id,
        sourceTransactionId: sourceTransaction.id,
        sourceAuditEventId: "audit-learned-amazon",
        promptProfileId: "cash_transaction_analyzer",
        userContext:
          "Whenever you see a Santander Amazon Marketplace card purchase, resolve it as shopping.",
        sourceTransactionSnapshotJson: {
          transactionDate: sourceTransaction.transactionDate,
          postedDate: sourceTransaction.postedDate,
          amountOriginal: sourceTransaction.amountOriginal,
          currencyOriginal: sourceTransaction.currencyOriginal,
          descriptionRaw: sourceTransaction.descriptionRaw,
          merchantNormalized: sourceTransaction.merchantNormalized,
          counterpartyName: sourceTransaction.counterpartyName,
          securityId: null,
          quantity: null,
          unitPriceOriginal: null,
        },
        initialInferenceSnapshotJson: {
          transactionClass: "unknown",
          categoryCode: null,
          classificationSource: "llm",
          classificationStatus: "needs_review",
          classificationConfidence: "0.42",
          needsReview: true,
          reviewReason: "Unknown merchant.",
          model: "gpt-5.4",
          explanation: null,
          reason: "Unknown merchant.",
        },
        correctedOutcomeSnapshotJson: {
          transactionClass: "expense",
          categoryCode: "shopping",
          merchantNormalized: "Amazon Marketplace",
          counterpartyName: "Amazon",
          securityId: null,
          quantity: null,
          unitPriceOriginal: null,
          needsReview: false,
          reviewReason: null,
        },
        metadataJson: {
          sourceImportBatchId: "import-batch-amazon",
        },
        active: true,
        createdAt: "2026-04-14T09:00:00.000Z",
        updatedAt: "2026-04-14T09:05:00.000Z",
      },
    ],
  });

  const examples = buildReviewPromptExamples(dataset, account, targetTransaction);

  assert.equal(examples.length, 1);
  assert.equal(examples[0]?.objectId, sourceTransaction.id);
  assert.equal(
    examples[0]?.userFeedback,
    "Whenever you see a Santander Amazon Marketplace card purchase, resolve it as shopping.",
  );
});

test("review propagation candidate selection never falls back without embedding matches", async () => {
  const account = createAccount({
    id: "propagation-no-fallback-account",
    assetDomain: "investment",
    accountType: "brokerage_account",
  });
  const sourceTransaction = createTransaction({
    id: "propagation-no-fallback-source",
    accountId: account.id,
    descriptionRaw: "VANGUARD US 500 STOCK EUR",
    descriptionClean: "VANGUARD US 500 STOCK EUR",
    transactionClass: "investment_trade_buy",
    categoryCode: "stock_buy",
    needsReview: true,
  });
  const candidateTransaction = createTransaction({
    id: "propagation-no-fallback-candidate",
    accountId: account.id,
    descriptionRaw: "VANGUARD US 500 STOCK EUR @ 2",
    descriptionClean: "VANGUARD US 500 STOCK EUR @ 2",
    transactionClass: "investment_trade_buy",
    categoryCode: "stock_buy",
    needsReview: true,
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [sourceTransaction, candidateTransaction],
  });

  const matches = await selectReviewPropagationCandidateMatches({
    dataset,
    account,
    sourceTransaction,
    embeddingMatches: [],
  });

  assert.deepEqual(matches, []);
});

test("review propagation candidate selection can include already-resolved matches when explicitly requested", async () => {
  const account = createAccount({
    id: "propagation-include-resolved-account",
    assetDomain: "investment",
    accountType: "brokerage_account",
  });
  const sourceTransaction = createTransaction({
    id: "propagation-include-resolved-source",
    accountId: account.id,
    descriptionRaw: "VANGUARD US 500 STOCK EUR",
    descriptionClean: "VANGUARD US 500 STOCK EUR",
    transactionClass: "investment_trade_buy",
    categoryCode: "stock_buy",
    needsReview: false,
  });
  const unresolvedCandidate = createTransaction({
    id: "propagation-unresolved-candidate",
    accountId: account.id,
    descriptionRaw: "VANGUARD US 500 STOCK EUR @ 2",
    descriptionClean: "VANGUARD US 500 STOCK EUR @ 2",
    transactionClass: "investment_trade_buy",
    categoryCode: "stock_buy",
    needsReview: true,
  });
  const resolvedCandidate = createTransaction({
    id: "propagation-resolved-candidate",
    accountId: account.id,
    descriptionRaw: "VANGUARD US 500 STOCK EUR INS",
    descriptionClean: "VANGUARD US 500 STOCK EUR INS",
    transactionClass: "investment_trade_buy",
    categoryCode: "stock_buy",
    needsReview: false,
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [sourceTransaction, unresolvedCandidate, resolvedCandidate],
  });

  const matches = await selectReviewPropagationCandidateMatches({
    dataset,
    account,
    sourceTransaction,
    embeddingMatches: [
      {
        transactionId: resolvedCandidate.id,
        similarity: 0.992,
      },
      {
        transactionId: unresolvedCandidate.id,
        similarity: 0.981,
      },
    ],
    includeResolvedTargets: true,
  });

  assert.deepEqual(
    matches.map((match) => match.transactionId),
    [resolvedCandidate.id, unresolvedCandidate.id],
  );
});

test("brokerage cash review queue only exposes the brokerage category for generic cash-like rows", () => {
  const account = createAccount({
    id: "brokerage-cash-review-account",
    assetDomain: "investment",
    accountType: "brokerage_cash",
  });
  const transaction = createTransaction({
    id: "brokerage-cash-review-transaction",
    accountId: account.id,
    transactionClass: "expense",
    amountOriginal: "-15.00",
    categoryCode: "other_expense",
    needsReview: true,
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [transaction],
    categories: [
      {
        code: "brokerage",
        displayName: "Brokerage",
        parentCode: null,
        scopeKind: "investment",
        directionKind: "neutral",
        sortOrder: 1,
        active: true,
        metadataJson: {},
      },
      {
        code: "other_expense",
        displayName: "Other",
        parentCode: null,
        scopeKind: "both",
        directionKind: "expense",
        sortOrder: 2,
        active: true,
        metadataJson: {},
      },
      {
        code: "other_income",
        displayName: "Other",
        parentCode: null,
        scopeKind: "both",
        directionKind: "income",
        sortOrder: 3,
        active: true,
        metadataJson: {},
      },
      {
        code: "transfer_between_accounts",
        displayName: "Transfer Between Accounts",
        parentCode: null,
        scopeKind: "system",
        directionKind: "neutral",
        sortOrder: 4,
        active: true,
        metadataJson: {},
      },
    ],
  });

  assert.deepEqual(buildReviewQueueCategoryOptions(dataset, account, transaction), [
    {
      code: "brokerage",
      displayName: "Brokerage",
    },
  ]);
});

test("brokerage cash review queue hides manual category picks for resolved investment trades", () => {
  const account = createAccount({
    id: "brokerage-cash-resolved-trade-account",
    assetDomain: "investment",
    accountType: "brokerage_cash",
  });
  const transaction = createTransaction({
    id: "brokerage-cash-resolved-trade",
    accountId: account.id,
    transactionClass: "investment_trade_buy",
    amountOriginal: "-100.00",
    categoryCode: "stock_buy",
    needsReview: false,
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [transaction],
    categories: [
      {
        code: "brokerage",
        displayName: "Brokerage",
        parentCode: null,
        scopeKind: "investment",
        directionKind: "neutral",
        sortOrder: 1,
        active: true,
        metadataJson: {},
      },
      {
        code: "stock_buy",
        displayName: "Stock Buy",
        parentCode: null,
        scopeKind: "investment",
        directionKind: "investment",
        sortOrder: 2,
        active: true,
        metadataJson: {},
      },
    ],
  });

  assert.deepEqual(buildReviewQueueCategoryOptions(dataset, account, transaction), []);
});

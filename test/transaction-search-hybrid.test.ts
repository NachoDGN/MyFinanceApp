import test from "node:test";
import assert from "node:assert/strict";

import { normalizeSqlDateValue } from "../packages/db/src/sql-date";
import { fuseTransactionSearchResults } from "../packages/db/src/transaction-search-fusion";
import {
  buildDeterministicTransactionSearchQuery,
  extractDistinctiveTransactionSearchEvidenceTokens,
  filterSemanticCandidatesByEvidence,
  resolveTransactionSearchFilters,
} from "../packages/db/src/transaction-search";

test("hybrid fusion uses reranked semantic order plus BM25 rank fusion", () => {
  const semanticCandidates = [
    {
      transactionId: "txn-a",
      batchId: "batch-1",
      sourceBatchKey: "import_batch:1",
      transactionDate: "2026-03-27",
      postedAt: "2026-03-27",
      amount: "100.00",
      currency: "EUR",
      merchant: "Stripe",
      counterparty: "Stripe Ltd",
      category: "income",
      accountId: "acc-1",
      accountName: "Santander",
      institutionName: "Santander",
      accountType: "checking" as const,
      economicEntityId: "ent-1",
      economicEntityName: "Personal",
      economicEntityKind: "personal" as const,
      direction: "credit" as const,
      reviewState: "resolved" as const,
      reviewReason: null,
      originalText: "Stripe LTD payment",
      contextualizedText: "Stripe payout\n\nStripe LTD payment",
      documentSummary: "March Stripe payouts into Santander.",
      semanticDistance: 0.08,
    },
    {
      transactionId: "txn-b",
      batchId: "batch-1",
      sourceBatchKey: "import_batch:1",
      transactionDate: "2026-03-15",
      postedAt: "2026-03-15",
      amount: "-12.00",
      currency: "EUR",
      merchant: "Stripe",
      counterparty: "Stripe fees",
      category: "fee",
      accountId: "acc-1",
      accountName: "Santander",
      institutionName: "Santander",
      accountType: "checking" as const,
      economicEntityId: "ent-1",
      economicEntityName: "Personal",
      economicEntityKind: "personal" as const,
      direction: "debit" as const,
      reviewState: "needs_review" as const,
      reviewReason: "Check fee category",
      originalText: "Stripe processing fee",
      contextualizedText: "Stripe fee\n\nStripe processing fee",
      documentSummary: "March Stripe payouts into Santander.",
      semanticDistance: 0.11,
    },
  ];

  const keywordCandidates = [
    { ...semanticCandidates[1], bm25Score: 0.04 },
    { ...semanticCandidates[0], bm25Score: 0.06 },
  ];

  const rerankedSemantic = [
    { transactionId: "txn-a", score: 0.92 },
    { transactionId: "txn-b", score: 0.84 },
  ];

  const fused = fuseTransactionSearchResults({
    semanticCandidates,
    rerankedSemantic,
    keywordCandidates,
    limit: 2,
  });

  assert.equal(fused.length, 2);
  assert.equal(fused[0].transactionId, "txn-a");
  assert.equal(fused[1].transactionId, "txn-b");
  assert.equal(fused[0].semanticRank, 1);
  assert.equal(fused[0].rerankRank, 1);
  assert.equal(fused[0].keywordRank, 2);
  assert.deepEqual(fused[0].matchedBy.sort(), ["keyword", "semantic"]);
});

test("selector fallback applies only when the query does not constrain scope or time", () => {
  const resolved = resolveTransactionSearchFilters({
    parsedQuery: {
      hasExplicitScopeConstraint: false,
      hasExplicitTimeConstraint: false,
      accountIds: [],
      entityIds: [],
      accountTypes: [],
      entityKinds: [],
      reviewStates: [],
      directions: [],
      dateStart: null,
      dateEnd: null,
      explanation: "heuristic_fallback",
    },
    scope: { kind: "account", accountId: "acc-1" },
    period: {
      preset: "custom",
      start: "2026-03-01",
      end: "2026-03-31",
    },
  });

  assert.deepEqual(resolved.accountIds, ["acc-1"]);
  assert.equal(resolved.dateStart, "2026-03-01");
  assert.equal(resolved.dateEnd, "2026-03-31");
  assert.equal(resolved.usedScopeFallback, true);
  assert.equal(resolved.usedPeriodFallback, true);
});

test("explicit query constraints suppress selector fallback", () => {
  const resolved = resolveTransactionSearchFilters({
    parsedQuery: {
      hasExplicitScopeConstraint: true,
      hasExplicitTimeConstraint: true,
      accountIds: ["acc-2"],
      entityIds: [],
      accountTypes: ["credit_card"],
      entityKinds: ["company"],
      reviewStates: ["unresolved"],
      directions: ["debit"],
      dateStart: "2026-02-01",
      dateEnd: "2026-02-28",
      explanation: "model_parse",
    },
    scope: { kind: "entity", entityId: "ent-1" },
    period: {
      preset: "mtd",
      start: "2026-04-01",
      end: "2026-04-12",
    },
  });

  assert.deepEqual(resolved.accountIds, ["acc-2"]);
  assert.deepEqual(resolved.accountTypes, ["credit_card"]);
  assert.deepEqual(resolved.entityKinds, ["company"]);
  assert.deepEqual(resolved.reviewStates, ["unresolved"]);
  assert.deepEqual(resolved.directions, ["debit"]);
  assert.equal(resolved.dateStart, "2026-02-01");
  assert.equal(resolved.dateEnd, "2026-02-28");
  assert.equal(resolved.usedScopeFallback, false);
  assert.equal(resolved.usedPeriodFallback, false);
});

test("distinctive evidence tokens strip structural scope and time hints", () => {
  const dataset = {
    accounts: [
      {
        id: "acc-santander",
        displayName: "Cuenta Personal",
        institutionName: "Santander",
        accountType: "checking",
        accountSuffix: null,
        matchingAliases: [],
      },
    ],
    entities: [],
  } as never;

  const query = "Stripe payments received by Santander account in March 2026";
  const parsedQuery = buildDeterministicTransactionSearchQuery({
    query,
    referenceDate: "2026-04-03",
    dataset,
  });

  assert.deepEqual(
    extractDistinctiveTransactionSearchEvidenceTokens({
      query,
      dataset,
      parsedQuery,
    }),
    ["STRIPE"],
  );
});

test("sql date normalization handles postgres Date objects", () => {
  const value = new Date("2026-03-27T00:00:00.000Z");

  assert.equal(normalizeSqlDateValue(value), "2026-03-27");
  assert.equal(normalizeSqlDateValue("2026-03-27"), "2026-03-27");
  assert.equal(normalizeSqlDateValue("2026-03-27T12:15:00.000Z"), "2026-03-27");
  assert.equal(normalizeSqlDateValue("Fri Mar 27 2026"), null);
});

test("deterministic query parsing stays conservative for ambiguous merchant text", () => {
  const parsed = buildDeterministicTransactionSearchQuery({
    query: "Payroll ACME",
    referenceDate: "2026-04-03",
    dataset: {
      accounts: [
        {
          id: "acc-company",
          displayName: "Company A Operating",
          institutionName: "BBVA",
          accountType: "company_bank",
          accountSuffix: null,
          matchingAliases: [],
        },
      ],
      entities: [
        {
          id: "ent-company",
          displayName: "Company A",
          legalName: "ACME Europe LLC",
          slug: "company-a",
          entityKind: "company",
        },
      ],
    } as never,
  });

  assert.equal(parsed.hasExplicitScopeConstraint, false);
  assert.deepEqual(parsed.accountIds, []);
  assert.deepEqual(parsed.entityIds, []);
  assert.deepEqual(parsed.entityKinds, []);
});

test("semantic evidence filtering keeps exact contextual support and drops garbage tails", () => {
  const semanticCandidates = [
    {
      transactionId: "txn-notion",
      batchId: "batch-1",
      sourceBatchKey: "import_batch:1",
      transactionDate: "2026-04-02",
      postedAt: "2026-04-02",
      amount: "-32.00",
      currency: "EUR",
      merchant: "NOTION",
      counterparty: null,
      category: "subscriptions",
      accountId: "acc-1",
      accountName: "Personal Checking",
      institutionName: "Santander",
      accountType: "checking" as const,
      economicEntityId: "ent-1",
      economicEntityName: "Personal",
      economicEntityKind: "personal" as const,
      direction: "debit" as const,
      reviewState: "resolved" as const,
      reviewReason: null,
      originalText: "Notion subscription",
      contextualizedText:
        "Subscription payment to Notion from Santander checking.\n\nNotion subscription",
      documentSummary: "April recurring subscriptions.",
      semanticDistance: 0.31,
    },
    {
      transactionId: "txn-random",
      batchId: "batch-2",
      sourceBatchKey: "import_batch:2",
      transactionDate: "2026-04-03",
      postedAt: "2026-04-03",
      amount: "-901.60",
      currency: "EUR",
      merchant: null,
      counterparty: null,
      category: "uncategorized_investment",
      accountId: "acc-2",
      accountName: "Personal Brokerage",
      institutionName: "Interactive Brokers",
      accountType: "brokerage_account" as const,
      economicEntityId: "ent-1",
      economicEntityName: "Personal",
      economicEntityKind: "personal" as const,
      direction: "debit" as const,
      reviewState: "needs_review" as const,
      reviewReason: "Ambiguous security",
      originalText: "ALPHABET INC @ 7",
      contextualizedText:
        "Stock purchase from Interactive Brokers.\n\nALPHABET INC @ 7",
      documentSummary: "April investment activity.",
      semanticDistance: 0.47,
    },
  ];

  const filtered = filterSemanticCandidatesByEvidence({
    query: "Notion",
    semanticCandidates,
    keywordCandidates: [],
  });

  assert.deepEqual(
    filtered.map((candidate) => candidate.transactionId),
    ["txn-notion"],
  );
});

test("semantic evidence filtering keeps named hits and drops structural false positives", () => {
  const semanticCandidates = [
    {
      transactionId: "txn-stripe",
      batchId: "batch-1",
      sourceBatchKey: "import_batch:1",
      transactionDate: "2026-03-27",
      postedAt: "2026-03-27",
      amount: "100.00",
      currency: "EUR",
      merchant: "Stripe",
      counterparty: "Stripe Ltd",
      category: "client_payment",
      accountId: "acc-1",
      accountName: "Cuenta Personal",
      institutionName: "Santander",
      accountType: "checking" as const,
      economicEntityId: "ent-1",
      economicEntityName: "Personal",
      economicEntityKind: "personal" as const,
      direction: "credit" as const,
      reviewState: "resolved" as const,
      reviewReason: null,
      originalText: "Stripe LTD payment",
      contextualizedText:
        "Client payment credit into Cuenta Personal at Santander.\n\nStripe LTD payment",
      documentSummary: "March client payments.",
      semanticDistance: 0.08,
    },
    {
      transactionId: "txn-transfer",
      batchId: "batch-2",
      sourceBatchKey: "import_batch:2",
      transactionDate: "2026-03-12",
      postedAt: "2026-03-12",
      amount: "600.00",
      currency: "EUR",
      merchant: null,
      counterparty: "TheWhiteBox Company",
      category: "transfer_between_accounts",
      accountId: "acc-1",
      accountName: "Cuenta Personal",
      institutionName: "Santander",
      accountType: "checking" as const,
      economicEntityId: "ent-1",
      economicEntityName: "Personal",
      economicEntityKind: "personal" as const,
      direction: "credit" as const,
      reviewState: "resolved" as const,
      reviewReason: null,
      originalText: "Transferencia recibida",
      contextualizedText:
        "Credit into Cuenta Personal at Santander on March 12, 2026.\n\nTransferencia recibida",
      documentSummary: "March credits.",
      semanticDistance: 0.09,
    },
  ];

  const filtered = filterSemanticCandidatesByEvidence({
    query: "Stripe payments received by Santander account in March 2026",
    semanticCandidates,
    keywordCandidates: [],
    distinctiveTokens: ["STRIPE"],
  });

  assert.deepEqual(
    filtered.map((candidate) => candidate.transactionId),
    ["txn-stripe"],
  );
});

test("semantic evidence filtering does not over-constrain structural-only queries", () => {
  const semanticCandidates = [
    {
      transactionId: "txn-credit-a",
      batchId: "batch-1",
      sourceBatchKey: "import_batch:1",
      transactionDate: "2026-03-27",
      postedAt: "2026-03-27",
      amount: "100.00",
      currency: "EUR",
      merchant: "Invoice 1001",
      counterparty: "Client A",
      category: "client_payment",
      accountId: "acc-1",
      accountName: "Cuenta Personal",
      institutionName: "Santander",
      accountType: "checking" as const,
      economicEntityId: "ent-1",
      economicEntityName: "Personal",
      economicEntityKind: "personal" as const,
      direction: "credit" as const,
      reviewState: "resolved" as const,
      reviewReason: null,
      originalText: "Cobro factura 1001",
      contextualizedText:
        "Credit into Cuenta Personal at Santander on March 27, 2026.\n\nCobro factura 1001",
      documentSummary: "March credits.",
      semanticDistance: 0.08,
    },
    {
      transactionId: "txn-credit-b",
      batchId: "batch-2",
      sourceBatchKey: "import_batch:2",
      transactionDate: "2026-03-12",
      postedAt: "2026-03-12",
      amount: "600.00",
      currency: "EUR",
      merchant: null,
      counterparty: "Client B",
      category: "client_payment",
      accountId: "acc-1",
      accountName: "Cuenta Personal",
      institutionName: "Santander",
      accountType: "checking" as const,
      economicEntityId: "ent-1",
      economicEntityName: "Personal",
      economicEntityKind: "personal" as const,
      direction: "credit" as const,
      reviewState: "resolved" as const,
      reviewReason: null,
      originalText: "Ingreso transferencia",
      contextualizedText:
        "Credit into Cuenta Personal at Santander on March 12, 2026.\n\nIngreso transferencia",
      documentSummary: "March credits.",
      semanticDistance: 0.09,
    },
  ];

  const filtered = filterSemanticCandidatesByEvidence({
    query: "payments received by Santander account in March 2026",
    semanticCandidates,
    keywordCandidates: [],
    distinctiveTokens: [],
  });

  assert.deepEqual(
    filtered.map((candidate) => candidate.transactionId),
    ["txn-credit-a", "txn-credit-b"],
  );
});

import assert from "node:assert/strict";
import test from "node:test";

import {
  buildInvestmentsReadModel,
  buildMetricResult,
  buildSpendingReadModel,
} from "../packages/analytics/src/index.ts";
import {
  analyzeBankTransaction,
  createLLMClient,
  lookupHistoricalFundPrice,
} from "../packages/llm/src/index.ts";
import {
  applyRuleMatch,
  enrichImportedTransaction,
  getInvestmentTransactionClassifierConfig,
  parseInvestmentEvent,
  rankReviewPropagationTransactions,
} from "../packages/classification/src/index.ts";
import { prepareInvestmentRebuild } from "../packages/db/src/investment-rebuild.ts";
import {
  buildHoldingRows,
  buildImportedTransactions,
  createTemplateConfig,
  getDatasetLatestDate,
  getLatestInvestmentCashBalances,
  getPreviousComparablePeriod,
  getScopeLatestDate,
  rebuildInvestmentState,
  resolvePeriodSelection,
} from "../packages/domain/src/index.ts";
import {
  buildReviewPropagationUserContext,
  buildResolvedSourcePrecedent,
  buildResolvedSourcePropagatedContextEntry,
  buildUnresolvedSourcePropagatedContextEntry,
  findSimilarResolvedTransactionsByDescriptionEmbedding,
  findSimilarUnresolvedTransactionsByDescriptionEmbedding,
  mergePropagatedContextHistory,
  shouldRunInvestmentRebuildAfterReviewPropagation,
  shouldQueueReviewPropagationAfterManualReview,
  TRANSACTION_SELECT_COLUMN_NAMES,
} from "../packages/db/src/index.ts";

import {
  createAccount,
  createDataset,
  createRule,
  createTransaction,
} from "./support/create-dataset";

function parseVectorLiteral(value: string) {
  return JSON.parse(value) as number[];
}

function cosineSimilarity(left: number[], right: number[]) {
  const length = Math.min(left.length, right.length);
  let score = 0;
  for (let index = 0; index < length; index += 1) {
    score += (left[index] ?? 0) * (right[index] ?? 0);
  }
  return score;
}

function createSimilaritySql(
  rows: Array<{
    id: string;
    userId: string;
    accountId: string;
    needsReview: boolean;
    voidedAt: string | null;
    descriptionEmbedding: string | null;
  }>,
) {
  const sql = (async (strings: TemplateStringsArray, ...values: unknown[]) => {
    const query = strings.join(" ");
    if (!query.includes("from public.transactions")) {
      throw new Error(`Unexpected SQL query: ${query}`);
    }
    const matchResolvedTransactions = query.includes(
      "coalesce(needs_review, false) = false",
    );

    const [
      sourceEmbeddingLiteral,
      userId,
      accountId,
      sourceTransactionId,
      _thresholdEmbeddingLiteral,
      threshold,
      _orderEmbeddingLiteral,
      limit,
    ] = values;
    const sourceEmbedding = parseVectorLiteral(String(sourceEmbeddingLiteral));

    return rows
      .filter((row) => row.userId === userId)
      .filter((row) => row.accountId === accountId)
      .filter((row) => row.id !== sourceTransactionId)
      .filter((row) =>
        matchResolvedTransactions ? row.needsReview === false : row.needsReview,
      )
      .filter((row) => row.voidedAt === null)
      .filter((row) => typeof row.descriptionEmbedding === "string")
      .map((row) => ({
        id: row.id,
        similarity: cosineSimilarity(
          sourceEmbedding,
          parseVectorLiteral(String(row.descriptionEmbedding)),
        ),
      }))
      .filter((row) => row.similarity >= Number(threshold))
      .sort((left, right) => right.similarity - left.similarity)
      .slice(0, Number(limit));
  }) as unknown as {
    (
      strings: TemplateStringsArray,
      ...values: unknown[]
    ): Promise<Array<{ id: string; similarity: number }>>;
    unsafe: (value: string) => string;
  };
  sql.unsafe = (value: string) => value;
  return sql;
}

test("import building deduplicates by fingerprint and keeps the dataset user id", () => {
  const input = {
    accountId: "account-1",
    templateId: "template-1",
    originalFilename: "upload.csv",
    filePath: "/tmp/upload.csv",
  } as const;
  const duplicateRow = {
    transaction_date: "2026-04-01",
    posted_date: "2026-04-01",
    description_raw: "Groceries",
    amount_original_signed: "-25.00",
    currency_original: "EUR",
  } as const;
  const seed = buildImportedTransactions(createDataset(), input, "seed-batch", [
    duplicateRow,
  ]);
  const dataset = createDataset({ transactions: seed.inserted });

  const result = buildImportedTransactions(dataset, input, "batch-1", [
    duplicateRow,
    {
      transaction_date: "2026-04-02",
      posted_date: "2026-04-02",
      description_raw: "Coffee",
      amount_original_signed: "-3.50",
      currency_original: "EUR",
    },
  ]);

  assert.equal(result.duplicateCount, 1);
  assert.equal(result.inserted.length, 1);
  assert.equal(result.inserted[0]?.userId, dataset.profile.id);
  assert.equal(result.inserted[0]?.descriptionClean, "COFFEE");
});

test("import building deduplicates rounded investment rows against precise existing trades", () => {
  const input = {
    accountId: "broker-1",
    templateId: "template-1",
    originalFilename: "upload.csv",
    filePath: "/tmp/upload.csv",
  } as const;
  const investmentAccount = createAccount({
    id: "broker-1",
    displayName: "Broker",
    accountType: "brokerage_account",
    assetDomain: "investment",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    securities: [
      {
        id: "security-amd",
        providerName: "twelve_data",
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices Inc",
        exchangeName: "NASDAQ",
        assetType: "stock",
        quoteCurrency: "USD",
        active: true,
        metadataJson: {},
        createdAt: "2026-01-01T00:00:00Z",
      },
    ],
    transactions: [
      createTransaction({
        id: "existing-investment-trade",
        accountId: investmentAccount.id,
        transactionDate: "2026-03-04",
        postedDate: "2026-03-05",
        amountOriginal: "-215.46000000",
        amountBaseEur: "-215.46000000",
        descriptionRaw: "ADVANCED MICRO DEVICES @ 1",
        descriptionClean: "ADVANCED MICRO DEVICES @ 1",
        transactionClass: "investment_trade_buy",
        categoryCode: "uncategorized_investment",
        rawPayload: {
          _import: {
            transaction_type_raw: "buy",
          },
        },
        securityId: "security-amd",
        quantity: "1.00000000",
        unitPriceOriginal: "215.46000000",
      }),
    ],
  });

  const result = buildImportedTransactions(dataset, input, "batch-1", [
    {
      transaction_date: "2026-03-04",
      posted_date: "2026-03-05",
      description_raw: "ADVANCED MICRO DEVICES @ 1",
      amount_original_signed: "-215.00",
      currency_original: "EUR",
      transaction_type_raw: "buy",
      security_symbol: "AMD",
      quantity: "1",
      unit_price_original: "215",
    },
  ]);

  assert.equal(result.duplicateCount, 1);
  assert.equal(result.inserted.length, 0);
});

test("month-to-date metrics use a dynamic comparison window and ignore internal transfers", () => {
  const dataset = createDataset({
    transactions: [
      createTransaction({
        id: "income-apr",
        transactionDate: "2026-04-01",
        postedDate: "2026-04-01",
        amountOriginal: "1000.00",
        amountBaseEur: "1000.00",
        transactionClass: "income",
        categoryCode: "salary",
        descriptionRaw: "Salary",
        descriptionClean: "SALARY",
      }),
      createTransaction({
        id: "spend-apr",
        transactionDate: "2026-04-02",
        postedDate: "2026-04-02",
        amountOriginal: "-100.00",
        amountBaseEur: "-100.00",
        transactionClass: "expense",
        categoryCode: "groceries",
        descriptionRaw: "Groceries",
        descriptionClean: "GROCERIES",
      }),
      createTransaction({
        id: "unresolved-spend-apr",
        transactionDate: "2026-04-02",
        postedDate: "2026-04-02",
        amountOriginal: "-40.00",
        amountBaseEur: "-40.00",
        transactionClass: "expense",
        categoryCode: "groceries",
        descriptionRaw: "Unresolved groceries",
        descriptionClean: "UNRESOLVED GROCERIES",
        needsReview: true,
        reviewReason: "Needs confirmation.",
      }),
      createTransaction({
        id: "transfer-apr",
        transactionDate: "2026-04-03",
        postedDate: "2026-04-03",
        amountOriginal: "-500.00",
        amountBaseEur: "-500.00",
        transactionClass: "transfer_internal",
        categoryCode: null,
        descriptionRaw: "Broker transfer",
        descriptionClean: "BROKER TRANSFER",
      }),
      createTransaction({
        id: "spend-mar",
        transactionDate: "2026-03-02",
        postedDate: "2026-03-02",
        amountOriginal: "-80.00",
        amountBaseEur: "-80.00",
        transactionClass: "expense",
        categoryCode: "groceries",
        descriptionRaw: "Groceries",
        descriptionClean: "GROCERIES",
      }),
    ],
  });

  const currentPeriod = resolvePeriodSelection({
    preset: "mtd",
    referenceDate: "2026-04-03",
  });
  const previousPeriod = getPreviousComparablePeriod(currentPeriod);
  const spending = buildMetricResult(
    dataset,
    { kind: "consolidated" },
    "EUR",
    "spending_mtd_total",
    { referenceDate: "2026-04-03" },
  );
  const operatingNet = buildMetricResult(
    dataset,
    { kind: "consolidated" },
    "EUR",
    "operating_net_cash_flow_mtd",
    { referenceDate: "2026-04-03" },
  );

  assert.deepEqual(currentPeriod, {
    start: "2026-04-01",
    end: "2026-04-03",
    preset: "mtd",
  });
  assert.deepEqual(previousPeriod, {
    start: "2026-03-01",
    end: "2026-03-03",
    preset: "mtd",
  });
  assert.equal(spending.valueBaseEur, "100.00");
  assert.equal(spending.comparisonValueBaseEur, "80.00");
  assert.equal(operatingNet.valueBaseEur, "900.00");
});

test("scope latest date prefers newer market and FX data over the latest transaction date", () => {
  const investmentAccount = createAccount({
    id: "brokerage-latest-date",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "USD",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    transactions: [
      createTransaction({
        id: "older-trade",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-03-24",
        postedDate: "2026-03-24",
        amountOriginal: "-100.00",
        amountBaseEur: "-92.00",
        currencyOriginal: "USD",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        securityId: "security-amd-latest-date",
        quantity: "1.00000000",
      }),
    ],
    securities: [
      {
        id: "security-amd-latest-date",
        providerName: "twelve_data",
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices Inc",
        exchangeName: "NASDAQ",
        micCode: "XNAS",
        assetType: "stock",
        quoteCurrency: "USD",
        country: "US",
        isin: null,
        figi: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
        createdAt: "2026-03-24T08:00:00Z",
      },
    ],
    securityPrices: [
      {
        securityId: "security-amd-latest-date",
        priceDate: "2026-04-03",
        quoteTimestamp: "2026-04-03T20:00:00Z",
        price: "110.00",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-04-03T20:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T20:00:00Z",
        rate: "0.92000000",
        sourceName: "twelve_data",
        rawJson: {},
      },
    ],
    investmentPositions: [
      {
        userId: "user-1",
        entityId: "entity-1",
        accountId: investmentAccount.id,
        securityId: "security-amd-latest-date",
        openQuantity: "1.00000000",
        openCostBasisEur: "92.00000000",
        avgCostEur: "92.00000000",
        realizedPnlEur: "0.00000000",
        dividendsEur: "0.00000000",
        interestEur: "0.00000000",
        feesEur: "0.00000000",
        lastTradeDate: "2026-03-24",
        lastRebuiltAt: "2026-04-03T20:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  assert.equal(
    getScopeLatestDate(dataset, { kind: "consolidated" }),
    "2026-04-03",
  );
});

test("saved classification rules win before fallback logic or LLM classification", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  process.env.OPENAI_API_KEY = "";

  try {
    const account = createAccount();
    const transaction = createTransaction({
      id: "notion-row",
      descriptionRaw: "Notion subscription",
      descriptionClean: "NOTION SUBSCRIPTION",
      categoryCode: null,
      transactionClass: "unknown",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
    });
    const dataset = createDataset({
      accounts: [account],
      rules: [createRule()],
    });

    const decision = await enrichImportedTransaction(
      dataset,
      account,
      transaction,
    );

    assert.equal(decision.classificationSource, "user_rule");
    assert.equal(decision.transactionClass, "expense");
    assert.equal(decision.categoryCode, "software");
    assert.equal(decision.merchantNormalized, "NOTION");
    assert.equal(decision.needsReview, false);
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
  }
});

test("latest date helpers cap future imports at the provided fallback date", () => {
  const account = createAccount({
    id: "broker-future-dates",
    assetDomain: "investment",
    accountType: "brokerage_account",
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [
      createTransaction({
        id: "future-import-row",
        accountId: account.id,
        accountEntityId: account.entityId,
        economicEntityId: account.entityId,
        securityId: "security-amd-latest-date",
        transactionDate: "2026-12-03",
        postedDate: "2026-12-03",
      }),
    ],
    securityPrices: [
      {
        securityId: "security-amd-latest-date",
        priceDate: "2026-04-02",
        quoteTimestamp: "2026-04-02T20:00:00Z",
        price: "110.00",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: { close: "110.00" },
        createdAt: "2026-04-02T20:00:00Z",
      },
    ],
    investmentPositions: [
      {
        userId: "user-1",
        entityId: account.entityId,
        accountId: account.id,
        securityId: "security-amd-latest-date",
        openQuantity: "1.00000000",
        openCostBasisEur: "92.00000000",
        avgCostEur: "92.00000000",
        realizedPnlEur: "0.00000000",
        dividendsEur: "0.00000000",
        interestEur: "0.00000000",
        feesEur: "0.00000000",
        lastTradeDate: "2026-04-02",
        lastRebuiltAt: "2026-04-02T20:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  assert.equal(getDatasetLatestDate(dataset, "2026-04-04"), "2026-04-02");
  assert.equal(
    getScopeLatestDate(
      dataset,
      { kind: "account", accountId: account.id },
      "2026-04-04",
    ),
    "2026-04-02",
  );
});

test("investment review routes import and follow-up models separately", () => {
  const previousImport = process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
  const previousFollowup =
    process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM;
  process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = "gpt-5.4-mini";
  process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM = "gpt-5.4";

  try {
    assert.equal(
      getInvestmentTransactionClassifierConfig().model,
      "gpt-5.4-mini",
    );
    assert.equal(
      getInvestmentTransactionClassifierConfig("import_classification").model,
      "gpt-5.4-mini",
    );
    assert.equal(
      getInvestmentTransactionClassifierConfig("manual_review_update").model,
      "gpt-5.4",
    );
    assert.equal(
      getInvestmentTransactionClassifierConfig("manual_resolved_review").model,
      "gpt-5.4-mini",
    );
    assert.equal(
      getInvestmentTransactionClassifierConfig("review_propagation").model,
      "gpt-5.4",
    );
  } finally {
    if (previousImport === undefined) {
      delete process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
    } else {
      process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = previousImport;
    }
    if (previousFollowup === undefined) {
      delete process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM;
    } else {
      process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM = previousFollowup;
    }
  }
});

test("manual re-review of a previously unresolved investment transaction always queues propagation", () => {
  const account = createAccount({
    id: "broker-propagation-source",
    assetDomain: "investment",
    accountType: "brokerage_account",
  });
  const transaction = createTransaction({
    id: "propagation-source",
    accountId: account.id,
    transactionClass: "investment_trade_buy",
    needsReview: true,
    reviewReason:
      'Security mapping unresolved after analyzer web search for "VANGUARD US 500 STOCK INDEX EU".',
  });

  assert.equal(
    shouldQueueReviewPropagationAfterManualReview(account, transaction),
    true,
  );
});

test("manual re-review propagation queueing stays limited to unresolved investment transactions", () => {
  const account = createAccount({
    id: "cash-propagation-skip",
    assetDomain: "cash",
    accountType: "checking",
  });
  const transaction = createTransaction({
    id: "propagation-resolved",
    accountId: account.id,
    needsReview: false,
  });

  assert.equal(
    shouldQueueReviewPropagationAfterManualReview(account, transaction),
    false,
  );
});

test("review propagation triggers investment rebuild when exact instrument resolution only changes inside llm payload", () => {
  const before = createTransaction({
    id: "propagation-stale-resolution",
    accountId: "broker-propagation-stale-resolution",
    transactionClass: "investment_trade_buy",
    categoryCode: "stock_buy",
    classificationStatus: "llm",
    classificationSource: "llm",
    classificationConfidence: "0.98",
    needsReview: false,
    reviewReason: null,
    securityId: "security-provider-stale",
    quantity: null,
    unitPriceOriginal: null,
    llmPayload: {
      llm: {
        rawOutput: {
          resolved_instrument_name:
            "Vanguard Eurozone Stock Index Fund Institutional Plus EUR Acc",
          resolved_instrument_isin: null,
          current_price_type: null,
        },
      },
    },
  });
  const after = createTransaction({
    ...before,
    llmPayload: {
      llm: {
        rawOutput: {
          resolved_instrument_name:
            "Vanguard Eurozone Stock Index Fund - EUR Acc",
          resolved_instrument_isin: "IE0008248803",
          current_price_type: "NAV",
        },
      },
    },
  });

  assert.equal(before.securityId, after.securityId);
  assert.equal(before.quantity, after.quantity);
  assert.equal(before.unitPriceOriginal, after.unitPriceOriginal);
  assert.equal(
    shouldRunInvestmentRebuildAfterReviewPropagation(before, after),
    true,
  );
});

test("review propagation user context includes resolved instrument evidence from the source transaction", () => {
  const transaction = createTransaction({
    id: "propagation-source-context",
    accountId: "broker-1",
    transactionClass: "investment_trade_buy",
    descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
    descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
    securityId: "security-vanguard-fund",
    needsReview: true,
    reviewReason:
      'Mapped to IE0032126645, but no reliable historical fund price was available to derive quantity for "VANGUARD US 500 STOCK INDEX EU".',
    llmPayload: {
      llm: {
        rawOutput: {
          resolved_instrument_name:
            "Vanguard U.S. 500 Stock Index Fund EUR Acc",
          resolved_instrument_isin: "IE0032126645",
          current_price: 69.39,
          current_price_currency: "EUR",
          current_price_timestamp: "2026-04-02T00:00:00Z",
          current_price_source: "Vanguard official fund page",
          current_price_type: "NAV",
        },
      },
    },
  });

  const reviewContext = buildReviewPropagationUserContext(transaction);

  assert.match(reviewContext, /manually re-reviewed/i);
  assert.match(reviewContext, /VANGUARD US 500 STOCK INDEX EU/);
  assert.match(reviewContext, /security-vanguard-fund/);
  assert.match(reviewContext, /IE0032126645/);
  assert.match(reviewContext, /69\.39 EUR/);
  assert.match(reviewContext, /NAV/);
});

test("vector similarity candidate lookup returns same-account unresolved transactions above threshold", async () => {
  const matches = await findSimilarUnresolvedTransactionsByDescriptionEmbedding(
    createSimilaritySql([
      {
        id: "candidate-high-similarity",
        userId: "user-1",
        accountId: "broker-1",
        needsReview: true,
        voidedAt: null,
        descriptionEmbedding: "[1,0]",
      },
      {
        id: "candidate-low-similarity",
        userId: "user-1",
        accountId: "broker-1",
        needsReview: true,
        voidedAt: null,
        descriptionEmbedding: "[0.4,0.6]",
      },
      {
        id: "candidate-other-account",
        userId: "user-1",
        accountId: "broker-2",
        needsReview: true,
        voidedAt: null,
        descriptionEmbedding: "[1,0]",
      },
      {
        id: "candidate-resolved",
        userId: "user-1",
        accountId: "broker-1",
        needsReview: false,
        voidedAt: null,
        descriptionEmbedding: "[1,0]",
      },
      {
        id: "candidate-voided",
        userId: "user-1",
        accountId: "broker-1",
        needsReview: true,
        voidedAt: "2026-04-01T00:00:00Z",
        descriptionEmbedding: "[1,0]",
      },
    ]),
    {
      userId: "user-1",
      sourceTransactionId: "source-1",
      accountId: "broker-1",
      sourceEmbedding: "[1,0]",
      threshold: 0.95,
      limit: 25,
    },
  );

  assert.deepEqual(matches, [
    {
      transactionId: "candidate-high-similarity",
      similarity: 1,
    },
  ]);
});

test("resolved review similarity lookup returns same-account resolved transactions above threshold", async () => {
  const matches = await findSimilarResolvedTransactionsByDescriptionEmbedding(
    createSimilaritySql([
      {
        id: "candidate-high-similarity",
        userId: "user-1",
        accountId: "broker-1",
        needsReview: false,
        voidedAt: null,
        descriptionEmbedding: "[1,0]",
      },
      {
        id: "candidate-still-unresolved",
        userId: "user-1",
        accountId: "broker-1",
        needsReview: true,
        voidedAt: null,
        descriptionEmbedding: "[1,0]",
      },
      {
        id: "candidate-other-account",
        userId: "user-1",
        accountId: "broker-2",
        needsReview: false,
        voidedAt: null,
        descriptionEmbedding: "[1,0]",
      },
      {
        id: "candidate-low-similarity",
        userId: "user-1",
        accountId: "broker-1",
        needsReview: false,
        voidedAt: null,
        descriptionEmbedding: "[0.4,0.6]",
      },
    ]),
    {
      userId: "user-1",
      sourceTransactionId: "source-1",
      accountId: "broker-1",
      sourceEmbedding: "[1,0]",
      threshold: 0.95,
      limit: 5,
    },
  );

  assert.deepEqual(matches, [
    {
      transactionId: "candidate-high-similarity",
      similarity: 1,
    },
  ]);
});

test("default review propagation threshold is never stricter than 0.9", async () => {
  const previousThreshold = process.env.REVIEW_PROPAGATION_SIMILARITY_THRESHOLD;
  process.env.REVIEW_PROPAGATION_SIMILARITY_THRESHOLD = "0.95";

  try {
    const matches =
      await findSimilarUnresolvedTransactionsByDescriptionEmbedding(
        createSimilaritySql([
          {
            id: "candidate-above-point-nine",
            userId: "user-1",
            accountId: "broker-1",
            needsReview: true,
            voidedAt: null,
            descriptionEmbedding: "[0.91,0.09]",
          },
          {
            id: "candidate-below-point-nine",
            userId: "user-1",
            accountId: "broker-1",
            needsReview: true,
            voidedAt: null,
            descriptionEmbedding: "[0.89,0.11]",
          },
        ]),
        {
          userId: "user-1",
          sourceTransactionId: "source-1",
          accountId: "broker-1",
          sourceEmbedding: "[1,0]",
        },
      );

    assert.deepEqual(
      matches.map((match) => match.transactionId),
      ["candidate-above-point-nine"],
    );
  } finally {
    if (previousThreshold === undefined) {
      delete process.env.REVIEW_PROPAGATION_SIMILARITY_THRESHOLD;
    } else {
      process.env.REVIEW_PROPAGATION_SIMILARITY_THRESHOLD = previousThreshold;
    }
  }
});

test("default review propagation lookup does not cap unresolved matches at 25", async () => {
  const rows = Array.from({ length: 30 }, (_, index) => ({
    id: `candidate-${index + 1}`,
    userId: "user-1",
    accountId: "broker-1",
    needsReview: true,
    voidedAt: null,
    descriptionEmbedding: "[1,0]",
  }));

  const matches = await findSimilarUnresolvedTransactionsByDescriptionEmbedding(
    createSimilaritySql(rows),
    {
      userId: "user-1",
      sourceTransactionId: "source-1",
      accountId: "broker-1",
      sourceEmbedding: "[1,0]",
    },
  );

  assert.equal(matches.length, 30);
  assert.deepEqual(
    matches.map((match) => match.transactionId),
    rows.map((row) => row.id),
  );
});

test("vector similarity propagation does not filter out buy sell or fee variants when embeddings are close", async () => {
  const matches = await findSimilarUnresolvedTransactionsByDescriptionEmbedding(
    createSimilaritySql([
      {
        id: "buy-variant",
        userId: "user-1",
        accountId: "broker-1",
        needsReview: true,
        voidedAt: null,
        descriptionEmbedding: "[0.99,0.01]",
      },
      {
        id: "sell-variant",
        userId: "user-1",
        accountId: "broker-1",
        needsReview: true,
        voidedAt: null,
        descriptionEmbedding: "[0.985,0.015]",
      },
      {
        id: "fee-variant",
        userId: "user-1",
        accountId: "broker-1",
        needsReview: true,
        voidedAt: null,
        descriptionEmbedding: "[0.98,0.02]",
      },
    ]),
    {
      userId: "user-1",
      sourceTransactionId: "source-1",
      accountId: "broker-1",
      sourceEmbedding: "[1,0]",
      threshold: 0.95,
      limit: 25,
    },
  );

  assert.deepEqual(
    matches.map((match) => match.transactionId),
    ["buy-variant", "sell-variant", "fee-variant"],
  );
});

test("unresolved-source propagation appends structured propagated context entries without overwriting history", () => {
  const previousFetch = globalThis.fetch;
  let fetchCalled = false;
  globalThis.fetch = async () => {
    fetchCalled = true;
    throw new Error("Unresolved-source propagation should not call the LLM.");
  };

  try {
    const sourceTransaction = createTransaction({
      id: "source-unresolved",
      accountId: "broker-1",
      transactionClass: "investment_trade_buy",
      descriptionRaw: "VANGUARD EUROZONE STOCK INDEX",
      descriptionClean: "VANGUARD EUROZONE STOCK INDEX",
      needsReview: true,
      reviewReason: "Security mapping unresolved.",
      manualNotes: "This looks like the Vanguard Eurozone index fund.",
    });
    const existingEntry = buildUnresolvedSourcePropagatedContextEntry({
      sourceTransaction: createTransaction({
        id: "older-source",
        accountId: "broker-1",
        needsReview: true,
        reviewReason: "Older unresolved reason.",
      }),
      sourceAuditEventId: "audit-old",
      similarity: 0.97,
      propagatedAt: "2026-04-01T08:00:00Z",
    });
    const nextEntry = buildUnresolvedSourcePropagatedContextEntry({
      sourceTransaction,
      sourceAuditEventId: "audit-new",
      similarity: 0.99,
      propagatedAt: "2026-04-05T08:00:00Z",
    });

    const merged = mergePropagatedContextHistory([existingEntry], nextEntry);

    assert.equal(merged.length, 2);
    assert.equal(merged[0]?.sourceTransactionId, "source-unresolved");
    assert.equal(merged[0]?.kind, "unresolved_source_context");
    assert.equal(merged[0]?.userProvidedContext, sourceTransaction.manualNotes);
    assert.match(merged[0]?.summaryText ?? "", /still unresolved/i);
    assert.equal(merged[1]?.sourceTransactionId, "older-source");
    assert.equal(fetchCalled, false);
  } finally {
    globalThis.fetch = previousFetch;
  }
});

test("source precedent includes resolution_process and rebuild evidence when quantity was derived from historical NAV", () => {
  const sourceTransaction = createTransaction({
    id: "source-resolved",
    accountId: "broker-1",
    transactionClass: "investment_trade_buy",
    descriptionRaw: "VANGUARD EUROZONE STOCK INDEX",
    descriptionClean: "VANGUARD EUROZONE STOCK INDEX",
    securityId: "security-vanguard-eurozone",
    quantity: "2.00000000",
    unitPriceOriginal: "49.79000000",
    needsReview: false,
    reviewReason: null,
    manualNotes:
      "Exact ISIN is IE0032126645 for the Vanguard Eurozone Stock Index Fund EUR Acc purchase.",
    llmPayload: {
      llm: {
        model: "gpt-5.4-mini",
        explanation: "Resolved the exact fund from the ISIN.",
        reason: "Exact ISIN match; NAV retrieved from Vanguard.",
        rawOutput: {
          resolution_process:
            "Matched the exact ISIN IE0032126645 from user context, verified the fund name on Vanguard, and confirmed the EUR Acc share class.",
          resolved_instrument_isin: "IE0032126645",
        },
      },
      rebuildEvidence: {
        resolvedSecurityId: "security-vanguard-eurozone",
        historicalPriceUsed: {
          sourceName: "llm_historical_nav",
          priceDate: "2026-03-03",
          quoteTimestamp: "2026-03-03T00:00:00Z",
          price: "49.79000000",
          currency: "EUR",
          marketState: null,
        },
        quantityDerivedFromHistoricalPrice: true,
        rebuiltAt: "2026-03-03T12:00:00Z",
      },
    },
  });

  const precedent = buildResolvedSourcePrecedent(
    sourceTransaction,
    "audit-source",
  );

  assert.equal(precedent.sourceAuditEventId, "audit-source");
  assert.equal(
    precedent.finalTransaction.securityId,
    "security-vanguard-eurozone",
  );
  assert.equal(precedent.finalTransaction.quantity, "2.00000000");
  assert.match(precedent.llm.resolutionProcess ?? "", /IE0032126645/);
  assert.equal(
    (
      precedent.rebuildEvidence as {
        quantityDerivedFromHistoricalPrice?: boolean;
      } | null
    )?.quantityDerivedFromHistoricalPrice,
    true,
  );
});

test("historical fund NAV lookup prompt prefers official issuer price-history pages over identity-only factsheets", async () => {
  let capturedSystemPrompt = "";
  let capturedUserPrompt = "";
  let capturedTools: Array<Record<string, unknown>> | undefined;
  let capturedToolChoice: string | undefined;

  const result = await lookupHistoricalFundPrice(
    {
      async generateText() {
        throw new Error("Not used in this test.");
      },
      async generateJson({ systemPrompt, userPrompt, tools, toolChoice }) {
        capturedSystemPrompt = systemPrompt;
        capturedUserPrompt = userPrompt;
        capturedTools = tools;
        capturedToolChoice = toolChoice;

        return {
          isin: "IE0007286036",
          target_date: "2025-12-31",
          security: "Vanguard Japan Stock Index Fund - EUR Acc",
          security_type: "open-ended fund",
          share_class: "EUR Acc",
          currency: "EUR",
          historical_nav: null,
          historical_nav_date: null,
          match_status: "exact",
          identity_source: {
            name: "Identity source",
            url: "https://example.com/identity",
          },
          historical_price_source: {
            name: "Price source",
            url: "https://example.com/prices",
          },
          explanation: "No dated NAV found.",
        };
      },
    },
    {
      isin: "IE0007286036",
      targetDate: "2025-12-31",
      securityNameHint: "Vanguard Japan Stock Index Fund - EUR Acc",
      transactionDescription: "VANGUARD JAPA STOCK IDX EUR AC",
      transactionCurrency: "EUR",
    },
    "gpt-5.4-mini",
  );

  assert.equal(result.analysisStatus, "done");
  assert.deepEqual(capturedTools, [{ type: "web_search" }]);
  assert.equal(capturedToolChoice, "auto");
  assert.match(
    capturedSystemPrompt,
    /prefer official issuer price-history pages, official daily price tables, historical NAV pages, official past-prices tools, and issuer-hosted CSV or downloadable price files/i,
  );
  assert.match(
    capturedSystemPrompt,
    /Do not stop after finding a factsheet or brochure that proves identity if it does not expose the dated NAV per share you need\./,
  );
  assert.match(
    capturedSystemPrompt,
    /If the issuer has both factsheets and dedicated prices pages, use the factsheet for identity and the prices page for the dated NAV\./,
  );
  assert.match(
    capturedSystemPrompt,
    /Financial Times fund tearsheets on markets\.ft\.com\/data\/funds\/tearsheet\/historical/i,
  );
  assert.match(
    capturedSystemPrompt,
    /For Vanguard and similar fund issuers, explicitly look for official prices or price-history pages rather than stopping at PDF factsheets that summarize performance only\./,
  );
  assert.match(
    capturedUserPrompt,
    /SEARCH_WORKFLOW = 1\) confirm identity with the exact ISIN, 2\) search official issuer price-history or NAV pages for the exact ISIN on the target date/i,
  );
  assert.match(
    capturedUserPrompt,
    /PRICE_SEARCH_TERMS = exact ISIN \+ target date \+ NAV \+ historical price \+ price history \+ past prices \+ daily prices \+ valuation\./,
  );
  assert.match(
    capturedUserPrompt,
    /SECONDARY_PRICE_SOURCE_HINT = after identity is exact, search markets\.ft\.com\/data\/funds\/tearsheet\/historical with the exact ISIN and matching share-class currency/i,
  );
});

test("transaction dataset column whitelist excludes description_embedding", () => {
  assert.equal(
    TRANSACTION_SELECT_COLUMN_NAMES.join(",").includes("description_embedding"),
    false,
  );
});

test("investment review propagation ranking rejects semantically nearby but different Vanguard funds", async () => {
  const account = createAccount({
    id: "broker-review-propagation-ranking",
    assetDomain: "investment",
    accountType: "brokerage_account",
  });
  const source = createTransaction({
    id: "source-eurozone-fund",
    accountId: account.id,
    accountEntityId: account.entityId,
    economicEntityId: account.entityId,
    transactionDate: "2026-03-03",
    postedDate: "2026-03-03",
    amountOriginal: "-47.90",
    amountBaseEur: "-47.90",
    currencyOriginal: "EUR",
    descriptionRaw: "VANGUARD EUROZONE STOCK INDEX",
    descriptionClean: "VANGUARD EUROZONE STOCK INDEX",
    transactionClass: "investment_trade_buy",
    categoryCode: "stock_buy",
    classificationStatus: "llm",
    classificationSource: "llm",
    classificationConfidence: "0.94",
    securityId: "security-vanguard-eurozone",
    needsReview: false,
    llmPayload: {
      llm: {
        rawOutput: {
          resolved_instrument_name:
            "Vanguard Eurozone Stock Index Fund EUR Acc",
          resolved_instrument_isin: "IE0032126645",
          current_price_type: "NAV",
        },
      },
    },
  });
  const trueCandidate = createTransaction({
    id: "candidate-eurozone-variant",
    accountId: account.id,
    accountEntityId: account.entityId,
    economicEntityId: account.entityId,
    transactionDate: "2026-03-10",
    postedDate: "2026-03-10",
    amountOriginal: "-48.10",
    amountBaseEur: "-48.10",
    currencyOriginal: "EUR",
    descriptionRaw: "VANGUARD EUROZONE STOCK INDEX EUR ACC",
    descriptionClean: "VANGUARD EUROZONE STOCK INDEX EUR ACC",
    transactionClass: "investment_trade_buy",
    categoryCode: "stock_buy",
    classificationStatus: "llm",
    classificationSource: "llm",
    classificationConfidence: "0.60",
    needsReview: true,
    reviewReason: "Still ambiguous.",
  });
  const falseCandidate = createTransaction({
    id: "candidate-global-small-cap",
    accountId: account.id,
    accountEntityId: account.entityId,
    economicEntityId: account.entityId,
    transactionDate: "2026-03-04",
    postedDate: "2026-03-04",
    amountOriginal: "-47.43",
    amountBaseEur: "-47.43",
    currencyOriginal: "EUR",
    descriptionRaw: "VANGUARD GLOB SMALL CAP INDEX",
    descriptionClean: "VANGUARD GLOB SMALL CAP INDEX",
    transactionClass: "investment_trade_buy",
    categoryCode: "stock_buy",
    classificationStatus: "llm",
    classificationSource: "llm",
    classificationConfidence: "0.60",
    needsReview: true,
    reviewReason: "Still ambiguous.",
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [source, trueCandidate, falseCandidate],
  });

  const matches = await rankReviewPropagationTransactions(
    dataset,
    account,
    source,
    {
      embeddingClient: {
        async embedTexts() {
          return [
            [1, 0],
            [0.995, 0.005],
            [0.28, 0.72],
          ];
        },
      },
    },
  );

  assert.deepEqual(
    matches.map((match) => match.transaction.id),
    ["candidate-eurozone-variant"],
  );
  assert.ok((matches[0]?.semanticSimilarity ?? 0) > 0.9);
});

test("vector similarity propagation includes abbreviated small-cap variants at the 0.9 threshold", async () => {
  const relaxedMatches =
    await findSimilarUnresolvedTransactionsByDescriptionEmbedding(
      createSimilaritySql([
        {
          id: "candidate-small-cap-abbreviated",
          userId: "user-1",
          accountId: "broker-1",
          needsReview: true,
          voidedAt: null,
          descriptionEmbedding: "[0.94624724,0.3234464]",
        },
        {
          id: "candidate-below-threshold",
          userId: "user-1",
          accountId: "broker-1",
          needsReview: true,
          voidedAt: null,
          descriptionEmbedding: "[0.88,0.47497368]",
        },
      ]),
      {
        userId: "user-1",
        sourceTransactionId: "source-1",
        accountId: "broker-1",
        sourceEmbedding: "[1,0]",
        threshold: 0.9,
        limit: 25,
      },
    );
  const strictMatches =
    await findSimilarUnresolvedTransactionsByDescriptionEmbedding(
      createSimilaritySql([
        {
          id: "candidate-small-cap-abbreviated",
          userId: "user-1",
          accountId: "broker-1",
          needsReview: true,
          voidedAt: null,
          descriptionEmbedding: "[0.94624724,0.3234464]",
        },
      ]),
      {
        userId: "user-1",
        sourceTransactionId: "source-1",
        accountId: "broker-1",
        sourceEmbedding: "[1,0]",
        threshold: 0.95,
        limit: 25,
      },
    );

  assert.deepEqual(relaxedMatches, [
    {
      transactionId: "candidate-small-cap-abbreviated",
      similarity: 0.94624724,
    },
  ]);
  assert.deepEqual(strictMatches, []);
});

test("investment parser recognizes named fund purchases even without explicit quantity", () => {
  const parsed = parseInvestmentEvent(
    createTransaction({
      accountId: "broker-1",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
      descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
    }),
  );

  assert.equal(parsed.transactionClass, "investment_trade_buy");
  assert.equal(parsed.securityHint, "VANGUARD US 500 STOCK INDEX EU");
});

test("investment parser stores sell quantities as negative values", () => {
  const parsed = parseInvestmentEvent(
    createTransaction({
      accountId: "broker-1",
      amountOriginal: "240.00",
      amountBaseEur: "240.00",
      descriptionRaw: "ADVANCED MICRO DEVICES @ 8",
      descriptionClean: "ADVANCED MICRO DEVICES @ 8",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
    }),
  );

  assert.equal(parsed.transactionClass, "investment_trade_sell");
  assert.equal(parsed.quantity, "-8.00000000");
});

test("investment parser recognizes periodic brokerage credits as interest", () => {
  const parsed = parseInvestmentEvent(
    createTransaction({
      accountId: "broker-1",
      amountOriginal: "0.14",
      amountBaseEur: "0.14",
      descriptionRaw: "PERIODO 19/02/2026 19/03/2026",
      descriptionClean: "PERIODO 19/02/2026 19/03/2026",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
    }),
  );

  assert.equal(parsed.transactionClass, "interest");
});

test("rule matching respects account scope", () => {
  const transaction = createTransaction({
    accountId: "account-1",
    descriptionRaw: "Notion subscription",
    descriptionClean: "NOTION SUBSCRIPTION",
  });
  const outOfScopeRule = createRule({
    scopeJson: { account_id: "account-2" },
  });

  assert.equal(applyRuleMatch(transaction, [outOfScopeRule]), null);
});

test("investment rebuild upgrades stale unknown investment rows into parsed trade buys", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  process.env.TWELVE_DATA_API_KEY = "";

  try {
    const account = createAccount({
      id: "broker-1",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "vanguard-row",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-24",
      postedDate: "2026-03-24",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
      descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason:
        "The description suggests an investment but lacks details for precise classification.",
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-24");
    const patch = rebuilt.transactionPatches[0];

    assert.equal(patch?.transactionClass, "investment_trade_buy");
    assert.equal(patch?.categoryCode, "stock_buy");
    assert.match(patch?.reviewReason ?? "", /Security mapping unresolved/i);
  } finally {
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild explains when a mapped trade still lacks quantity", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  process.env.TWELVE_DATA_API_KEY = "";

  try {
    const account = createAccount({
      id: "broker-2",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "mapped-without-quantity",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-24",
      postedDate: "2026-03-24",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
      descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "investment_parser",
      classificationSource: "investment_parser",
      classificationConfidence: "0.96",
      securityId: "security-vusa",
      needsReview: true,
      reviewReason:
        "The description suggests an investment but lacks details for precise classification.",
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-vusa",
          providerName: "manual",
          providerSymbol: "VUSA",
          canonicalSymbol: "VUSA",
          displaySymbol: "VUSA",
          name: "Vanguard S&P 500 UCITS ETF",
          exchangeName: "LSE",
          micCode: "XLON",
          assetType: "etf",
          quoteCurrency: "EUR",
          country: "IE",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-24");
    const patch = rebuilt.transactionPatches[0];

    assert.equal(patch?.needsReview, true);
    assert.match(
      patch?.reviewReason ?? "",
      /Mapped to VUSA, but market-data enrichment is unavailable/i,
    );
  } finally {
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild flags trades whose implied unit price is implausible", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousFetch = globalThis.fetch;
  process.env.TWELVE_DATA_API_KEY = "test-key";
  globalThis.fetch = async (input) => {
    const requestUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : (input as Request).url;
    const url = new URL(requestUrl);

    if (url.pathname.endsWith("/time_series")) {
      return new Response(
        JSON.stringify({
          values: [
            {
              datetime: "2026-03-16",
              close: "294.45",
            },
          ],
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    return new Response(JSON.stringify({ status: "error" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const account = createAccount({
      id: "broker-4",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "goog-mismatch",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-16",
      postedDate: "2026-03-16",
      amountOriginal: "1.89",
      amountBaseEur: "1.89",
      descriptionRaw: "ALPHABET INC CL C @ 15",
      descriptionClean: "ALPHABET INC CL C @ 15",
      transactionClass: "investment_trade_sell",
      categoryCode: "uncategorized_investment",
      classificationStatus: "investment_parser",
      classificationSource: "investment_parser",
      classificationConfidence: "0.96",
      securityId: "security-goog",
      quantity: "15.00000000",
      unitPriceOriginal: "0.13000000",
      needsReview: false,
      reviewReason: null,
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-goog",
          providerName: "manual",
          providerSymbol: "GOOG",
          canonicalSymbol: "GOOG",
          displaySymbol: "GOOG",
          name: "Alphabet Inc.",
          exchangeName: "NASDAQ",
          micCode: "XNGS",
          assetType: "stock",
          quoteCurrency: "USD",
          country: "US",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-16");
    const patch = rebuilt.transactionPatches[0];

    assert.equal(patch?.needsReview, true);
    assert.match(patch?.reviewReason ?? "", /Mapped to GOOG/i);
    assert.match(
      patch?.reviewReason ?? "",
      /diverges from available market data/i,
    );
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild rejects historical quotes that are far older than the requested trade date", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousFetch = globalThis.fetch;
  process.env.TWELVE_DATA_API_KEY = "test-key";

  globalThis.fetch = async (input) => {
    const requestUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : (input as Request).url;
    const url = new URL(requestUrl);

    if (url.pathname.endsWith("/time_series")) {
      return new Response(
        JSON.stringify({
          values: [
            {
              datetime: "2025-09-17",
              close: "24.90",
            },
          ],
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    return new Response(JSON.stringify({ status: "error" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const account = createAccount({
      id: "broker-historical-drift",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "intc-missing-quantity",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-12",
      postedDate: "2026-03-12",
      amountOriginal: "-41.00",
      amountBaseEur: "-37.72",
      currencyOriginal: "USD",
      descriptionRaw: "INTEL CORP",
      descriptionClean: "INTEL CORP",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "investment_parser",
      classificationSource: "investment_parser",
      classificationConfidence: "0.96",
      securityId: "security-intc-drift",
      quantity: null,
      unitPriceOriginal: null,
      needsReview: true,
      reviewReason: "Needs quantity derivation.",
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-intc-drift",
          providerName: "twelve_data",
          providerSymbol: "INTC",
          canonicalSymbol: "INTC",
          displaySymbol: "INTC",
          name: "Intel Corporation",
          exchangeName: "NASDAQ",
          micCode: "XNGS",
          assetType: "stock",
          quoteCurrency: "USD",
          country: "United States",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
      fxRates: [
        {
          baseCurrency: "USD",
          quoteCurrency: "EUR",
          asOfDate: "2026-03-12",
          asOfTimestamp: "2026-03-12T16:00:00Z",
          rate: "0.92000000",
          sourceName: "twelve_data",
          rawJson: {},
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-12");
    const patch = rebuilt.transactionPatches.find(
      (candidate) => candidate.id === "intc-missing-quantity",
    );

    assert.equal(rebuilt.upsertedPrices.length, 0);
    assert.match(
      patch?.reviewReason ?? "",
      /did not return a usable historical price/i,
    );
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild requests end-of-day quotes on weekends", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousFetch = globalThis.fetch;
  const requestedUrls: string[] = [];

  process.env.TWELVE_DATA_API_KEY = "test-key";
  globalThis.fetch = async (input) => {
    const requestUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : (input as Request).url;
    requestedUrls.push(requestUrl);
    const url = new URL(requestUrl);

    if (url.pathname.endsWith("/time_series")) {
      return new Response(
        JSON.stringify({
          values: [
            {
              datetime: "2026-04-01",
              close: "100.00",
            },
          ],
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    if (url.pathname.endsWith("/quote")) {
      return new Response(
        JSON.stringify({
          close: "110.00",
          currency: "USD",
          datetime: "2026-04-03",
          is_market_open: "false",
          last_quote_at: 1775232000,
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    return new Response(JSON.stringify({ status: "error" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const investmentAccount = createAccount({
      id: "brokerage-weekend",
      accountType: "brokerage_account",
      assetDomain: "investment",
      defaultCurrency: "USD",
    });
    const dataset = createDataset({
      accounts: [investmentAccount],
      transactions: [
        createTransaction({
          id: "weekend-buy",
          accountId: investmentAccount.id,
          accountEntityId: investmentAccount.entityId,
          economicEntityId: investmentAccount.entityId,
          transactionDate: "2026-04-01",
          postedDate: "2026-04-01",
          amountOriginal: "-100.00",
          amountBaseEur: "-92.00",
          currencyOriginal: "USD",
          descriptionRaw: "AMD @ 1",
          descriptionClean: "AMD @ 1",
          transactionClass: "investment_trade_buy",
          categoryCode: "stock_buy",
          classificationStatus: "investment_parser",
          classificationSource: "investment_parser",
          classificationConfidence: "0.96",
          securityId: "security-amd-weekend",
          quantity: "1.00000000",
          unitPriceOriginal: "100.00",
        }),
      ],
      securities: [
        {
          id: "security-amd-weekend",
          providerName: "twelve_data",
          providerSymbol: "AMD",
          canonicalSymbol: "AMD",
          displaySymbol: "AMD",
          name: "Advanced Micro Devices Inc",
          exchangeName: "NASDAQ",
          micCode: "XNAS",
          assetType: "stock",
          quoteCurrency: "USD",
          country: "US",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-04-01T08:00:00Z",
        },
      ],
      securityPrices: [
        {
          securityId: "security-amd-weekend",
          priceDate: "2026-04-03",
          quoteTimestamp: "2026-04-03T08:20:00Z",
          price: "152.40",
          currency: "USD",
          sourceName: "twelve_data",
          isRealtime: false,
          isDelayed: true,
          marketState: "closed",
          rawJson: {},
          createdAt: "2026-04-03T12:37:43Z",
        },
      ],
      fxRates: [
        {
          baseCurrency: "USD",
          quoteCurrency: "EUR",
          asOfDate: "2026-04-03",
          asOfTimestamp: "2026-04-03T20:00:00Z",
          rate: "0.92000000",
          sourceName: "twelve_data",
          rawJson: {},
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-04-04");
    const latestPrice = rebuilt.upsertedPrices.find(
      (price) =>
        price.securityId === "security-amd-weekend" &&
        price.priceDate === "2026-04-03",
    );
    const quoteRequest = requestedUrls.find((url) => url.includes("/quote"));

    assert.ok(quoteRequest);
    assert.equal(new URL(quoteRequest).searchParams.get("eod"), "true");
    assert.equal(latestPrice?.price, "110.00");
    assert.equal(latestPrice?.isDelayed, true);
    assert.equal(latestPrice?.isRealtime, false);
    assert.notDeepEqual(latestPrice?.rawJson, {});
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild uses stored historical prices for price sanity checks", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  delete process.env.TWELVE_DATA_API_KEY;

  try {
    const account = createAccount({
      id: "broker-4b",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "goog-stored-mismatch",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-16",
      postedDate: "2026-03-16",
      amountOriginal: "1.89",
      amountBaseEur: "1.89",
      descriptionRaw: "ALPHABET INC CL C @ 15",
      descriptionClean: "ALPHABET INC CL C @ 15",
      transactionClass: "investment_trade_sell",
      categoryCode: "uncategorized_investment",
      classificationStatus: "investment_parser",
      classificationSource: "investment_parser",
      classificationConfidence: "0.96",
      securityId: "security-goog-stored",
      quantity: "15.00000000",
      unitPriceOriginal: "0.13000000",
      needsReview: false,
      reviewReason: null,
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-goog-stored",
          providerName: "manual",
          providerSymbol: "GOOG",
          canonicalSymbol: "GOOG",
          displaySymbol: "GOOG",
          name: "Alphabet Inc.",
          exchangeName: "NASDAQ",
          micCode: "XNGS",
          assetType: "stock",
          quoteCurrency: "USD",
          country: "US",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
      securityPrices: [
        {
          securityId: "security-goog-stored",
          priceDate: "2026-03-13",
          quoteTimestamp: "2026-03-13T16:00:00Z",
          price: "301.45999",
          currency: "USD",
          sourceName: "twelve_data",
          isRealtime: false,
          isDelayed: true,
          marketState: "closed",
          rawJson: {},
          createdAt: "2026-03-13T16:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-16");
    const patch = rebuilt.transactionPatches[0];

    assert.equal(patch?.needsReview, true);
    assert.match(patch?.reviewReason ?? "", /Mapped to GOOG/i);
    assert.match(
      patch?.reviewReason ?? "",
      /diverges from available market data/i,
    );
  } finally {
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild clears quantity and unit price for non-trade rows", async () => {
  const account = createAccount({
    id: "broker-fee-cleanup",
    assetDomain: "investment",
    accountType: "brokerage_account",
    institutionName: "Broker",
    displayName: "Brokerage",
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [
      createTransaction({
        id: "commission-row",
        accountId: account.id,
        accountEntityId: account.entityId,
        economicEntityId: account.entityId,
        transactionDate: "2026-03-16",
        postedDate: "2026-03-16",
        amountOriginal: "1.89",
        amountBaseEur: "1.89",
        descriptionRaw: "ALPHABET INC CL C @ 15 COMMISSION",
        descriptionClean: "ALPHABET INC CL C @ 15 COMMISSION",
        transactionClass: "fee",
        categoryCode: "broker_fee",
        classificationStatus: "investment_parser",
        classificationSource: "investment_parser",
        classificationConfidence: "0.96",
        securityId: "security-goog-fee",
        quantity: "15.00000000",
        unitPriceOriginal: "0.13000000",
        needsReview: false,
        reviewReason: null,
      }),
    ],
    securities: [
      {
        id: "security-goog-fee",
        providerName: "manual",
        providerSymbol: "GOOG",
        canonicalSymbol: "GOOG",
        displaySymbol: "GOOG",
        name: "Alphabet Inc.",
        exchangeName: "NASDAQ",
        micCode: "XNGS",
        assetType: "stock",
        quoteCurrency: "USD",
        country: "US",
        isin: null,
        figi: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
        createdAt: "2026-01-01T00:00:00Z",
      },
    ],
  });

  const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-16");
  const patch = rebuilt.transactionPatches[0];

  assert.equal(patch?.quantity, null);
  assert.equal(patch?.unitPriceOriginal, null);
});

test("investment rebuild clears stale review flags for deterministic interest rows", async () => {
  const account = createAccount({
    id: "broker-interest",
    assetDomain: "investment",
    accountType: "brokerage_account",
    institutionName: "Broker",
    displayName: "Brokerage",
  });
  const transaction = createTransaction({
    id: "period-interest",
    accountId: account.id,
    accountEntityId: account.entityId,
    economicEntityId: account.entityId,
    transactionDate: "2026-03-20",
    postedDate: "2026-03-20",
    amountOriginal: "0.14",
    amountBaseEur: "0.14",
    descriptionRaw: "PERIODO 19/02/2026 19/03/2026",
    descriptionClean: "PERIODO 19/02/2026 19/03/2026",
    transactionClass: "interest",
    categoryCode: "interest",
    classificationStatus: "llm",
    classificationSource: "llm",
    classificationConfidence: "0.61",
    needsReview: true,
    reviewReason:
      "The transaction description and data do not clearly indicate a known transaction type or category.",
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [transaction],
  });

  const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-20");
  const patch = rebuilt.transactionPatches[0];

  assert.equal(patch?.needsReview, false);
  assert.equal(patch?.reviewReason, null);
});

test("investment rebuild remaps stale EU fund aliases away from USD OTC securities", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousFetch = globalThis.fetch;
  process.env.TWELVE_DATA_API_KEY = "test-key";
  globalThis.fetch = async (input) => {
    const requestUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : (input as Request).url;
    const url = new URL(requestUrl);

    if (url.pathname.endsWith("/symbol_search")) {
      return new Response(
        JSON.stringify({
          data: [
            {
              symbol: "0P00000MNK",
              instrument_name: "Vanguard U.S. 500 Stock Index F",
              exchange: "OTC",
              mic_code: "OTCM",
              instrument_type: "Mutual Fund",
              country: "United States",
              currency: "USD",
            },
            {
              symbol: "0P00000G12",
              instrument_name:
                "Vanguard U.S. 500 Stock Index Fund Investor EUR Accumulation",
              exchange: "XHAM",
              mic_code: "XHAM",
              instrument_type: "Mutual Fund",
              country: "Germany",
              currency: "EUR",
            },
          ],
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    return new Response(JSON.stringify({ status: "error" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const account = createAccount({
      id: "broker-3",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "stale-eu-alias",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-24",
      postedDate: "2026-03-24",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
      descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "investment_parser",
      classificationSource: "investment_parser",
      classificationConfidence: "0.96",
      securityId: "security-wrong-vanguard",
      needsReview: true,
      reviewReason:
        'Security mapping unresolved for "VANGUARD US 500 STOCK INDEX EU".',
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-wrong-vanguard",
          providerName: "twelve_data",
          providerSymbol: "0P00000MNK",
          canonicalSymbol: "0P00000MNK",
          displaySymbol: "0P00000MNK",
          name: "Vanguard U.S. 500 Stock Index F",
          exchangeName: "OTC",
          micCode: "OTCM",
          assetType: "etf",
          quoteCurrency: "USD",
          country: "United States",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
      securityAliases: [
        {
          id: "alias-wrong-vanguard",
          securityId: "security-wrong-vanguard",
          aliasTextNormalized: "VANGUARD US 500 STOCK INDEX EU",
          aliasSource: "provider",
          templateId: null,
          confidence: "0.9000",
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-24");

    assert.equal(rebuilt.insertedSecurities[0]?.providerSymbol, "0P00000G12");
    assert.equal(
      rebuilt.transactionPatches[0]?.securityId,
      rebuilt.insertedSecurities[0]?.id,
    );
    assert.equal(
      rebuilt.insertedAliases[0]?.aliasTextNormalized,
      "VANGUARD US 500 STOCK INDEX EU",
    );
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("manual review notes can remap an ETF security to a mutual fund candidate", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousOpenAiKey = process.env.OPENAI_API_KEY;
  const previousFetch = globalThis.fetch;
  const searchQueries: string[] = [];
  process.env.TWELVE_DATA_API_KEY = "test-key";
  delete process.env.OPENAI_API_KEY;
  globalThis.fetch = async (input) => {
    const requestUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : (input as Request).url;
    const url = new URL(requestUrl);
    if (url.pathname.endsWith("/symbol_search")) {
      searchQueries.push(url.searchParams.get("symbol") ?? "");
    }
    return new Response(JSON.stringify({ status: "error" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const account = createAccount({
      id: "broker-review-remap",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "review-remap-vanguard",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-24",
      postedDate: "2026-03-24",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
      descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "llm",
      classificationSource: "llm",
      classificationConfidence: "0.94",
      securityId: "security-vusa-eur",
      manualNotes: "This is an index fund in EUR, not an ETF.",
      llmPayload: {
        llm: {
          rawOutput: {
            resolved_instrument_name:
              "Vanguard U.S. 500 Stock Index Fund Investor EUR Accumulation",
            resolved_instrument_isin: "IE00B03HCZ61",
            resolved_instrument_ticker: null,
            resolved_instrument_exchange: null,
            current_price: 102.44,
            current_price_currency: "EUR",
            current_price_timestamp: "2026-04-04T09:00:00Z",
            current_price_source: "Official Vanguard factsheet",
            current_price_type: "NAV",
          },
        },
        reviewContext: {
          trigger: "manual_review_update",
          userProvidedContext: "This is an index fund in EUR, not an ETF.",
        },
      },
      needsReview: true,
      reviewReason:
        'Security mapping unresolved for "VANGUARD US 500 STOCK INDEX EU".',
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-vusa-eur",
          providerName: "twelve_data",
          providerSymbol: "VUSA",
          canonicalSymbol: "VUSA",
          displaySymbol: "VUSA",
          name: "Vanguard S&P 500 UCITS ETF EUR",
          exchangeName: "XETR",
          micCode: "XETR",
          assetType: "etf",
          quoteCurrency: "EUR",
          country: "Germany",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {
            instrumentType: "ETF",
          },
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
      securityAliases: [
        {
          id: "alias-vusa-eur",
          securityId: "security-vusa-eur",
          aliasTextNormalized: "VANGUARD US 500 STOCK INDEX EU",
          aliasSource: "provider",
          templateId: null,
          confidence: "0.9000",
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-24");

    assert.equal(rebuilt.insertedSecurities[0]?.providerName, "llm_web_search");
    assert.equal(rebuilt.insertedSecurities[0]?.providerSymbol, "IE00B03HCZ61");
    assert.equal(rebuilt.insertedSecurities[0]?.assetType, "other");
    assert.equal(rebuilt.upsertedPrices[0]?.sourceName, "llm_web_search");
    assert.equal(
      rebuilt.transactionPatches[0]?.securityId,
      rebuilt.insertedSecurities[0]?.id,
    );
    assert.equal(searchQueries.length, 0);
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
    if (previousOpenAiKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousOpenAiKey;
    }
  }
});

test("exact ISIN from a manual re-review can remap a mismatched ETF security", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousOpenAiKey = process.env.OPENAI_API_KEY;
  const previousFetch = globalThis.fetch;
  const searchQueries: string[] = [];
  process.env.TWELVE_DATA_API_KEY = "test-key";
  delete process.env.OPENAI_API_KEY;
  globalThis.fetch = async (input) => {
    const requestUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : (input as Request).url;
    const url = new URL(requestUrl);

    if (url.pathname.endsWith("/symbol_search")) {
      searchQueries.push(url.searchParams.get("symbol") ?? "");
      return new Response(
        JSON.stringify({
          data: [
            {
              symbol: "VUSA",
              instrument_name: "Vanguard S&P 500 UCITS ETF EUR",
              exchange: "XETR",
              mic_code: "XETR",
              instrument_type: "ETF",
              country: "Germany",
              currency: "EUR",
            },
            {
              symbol: "0P00000G12",
              instrument_name:
                "Vanguard U.S. 500 Stock Index Fund Investor EUR Accumulation",
              exchange: "XHAM",
              mic_code: "XHAM",
              instrument_type: "Mutual Fund",
              country: "Germany",
              currency: "EUR",
            },
          ],
        }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    return new Response(JSON.stringify({ status: "error" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const account = createAccount({
      id: "broker-review-isin-remap",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "review-isin-remap-vanguard",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-24",
      postedDate: "2026-03-24",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
      descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "llm",
      classificationSource: "llm",
      classificationConfidence: "0.94",
      securityId: "security-vusa-eur",
      manualNotes:
        "Exact ISIN is IE00B03HCZ61 for the Vanguard U.S. 500 Stock Index Fund EUR Acc purchase.",
      needsReview: true,
      reviewReason:
        'Security mapping unresolved for "VANGUARD US 500 STOCK INDEX EU".',
      llmPayload: {
        llm: {
          rawOutput: {
            resolved_instrument_name:
              "Vanguard U.S. 500 Stock Index Fund Investor EUR Accumulation",
            resolved_instrument_isin: "IE00B03HCZ61",
            resolved_instrument_ticker: null,
            resolved_instrument_exchange: null,
            current_price: 101.37,
            current_price_currency: "EUR",
            current_price_timestamp: "2026-04-04T09:00:00Z",
            current_price_source: "Official Vanguard factsheet",
            current_price_type: "NAV",
          },
        },
        reviewContext: {
          trigger: "manual_review_update",
          userProvidedContext:
            "Exact ISIN is IE00B03HCZ61 for the Vanguard U.S. 500 Stock Index Fund EUR Acc purchase.",
        },
      },
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-vusa-eur",
          providerName: "twelve_data",
          providerSymbol: "VUSA",
          canonicalSymbol: "VUSA",
          displaySymbol: "VUSA",
          name: "Vanguard S&P 500 UCITS ETF EUR",
          exchangeName: "XETR",
          micCode: "XETR",
          assetType: "etf",
          quoteCurrency: "EUR",
          country: "Germany",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {
            instrumentType: "ETF",
          },
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
      securityAliases: [
        {
          id: "alias-vusa-eur-isin",
          securityId: "security-vusa-eur",
          aliasTextNormalized: "VANGUARD US 500 STOCK INDEX EU",
          aliasSource: "provider",
          templateId: null,
          confidence: "0.9000",
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-24");

    assert.equal(rebuilt.insertedSecurities[0]?.providerName, "llm_web_search");
    assert.equal(rebuilt.insertedSecurities[0]?.providerSymbol, "IE00B03HCZ61");
    assert.equal(rebuilt.insertedSecurities[0]?.isin, "IE00B03HCZ61");
    assert.equal(rebuilt.upsertedPrices[0]?.sourceName, "llm_web_search");
    assert.equal(
      rebuilt.transactionPatches[0]?.securityId,
      rebuilt.insertedSecurities[0]?.id,
    );
    assert.equal(searchQueries.length, 0);
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
    if (previousOpenAiKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousOpenAiKey;
    }
  }
});

test("manual review ISIN fallback resolves web-mapped fund security even when structured instrument fields are sparse", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousOpenAiKey = process.env.OPENAI_API_KEY;
  const previousFetch = globalThis.fetch;
  const searchQueries: string[] = [];
  process.env.TWELVE_DATA_API_KEY = "test-key";
  delete process.env.OPENAI_API_KEY;
  globalThis.fetch = async (input) => {
    const requestUrl =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.toString()
          : (input as Request).url;
    const url = new URL(requestUrl);
    if (url.pathname.endsWith("/symbol_search")) {
      searchQueries.push(url.searchParams.get("symbol") ?? "");
    }
    return new Response(JSON.stringify({ status: "error" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const account = createAccount({
      id: "broker-review-isin-fallback",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "review-isin-fallback-vanguard",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-24",
      postedDate: "2026-03-24",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
      descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "llm",
      classificationSource: "llm",
      classificationConfidence: "0.94",
      securityId: null,
      manualNotes:
        "Exact ISIN is IE0032126645 for the Vanguard U.S. 500 Stock Index Fund EUR Acc purchase.",
      needsReview: true,
      reviewReason:
        'Security mapping unresolved for "VANGUARD US 500 STOCK INDEX EU".',
      llmPayload: {
        llm: {
          rawOutput: {
            resolved_instrument_name: null,
            resolved_instrument_isin: null,
            resolved_instrument_ticker: null,
            resolved_instrument_exchange: null,
            current_price: null,
            current_price_currency: null,
            current_price_timestamp: null,
            current_price_source: null,
            current_price_type: null,
            explanation:
              "The ISIN uniquely identifies the Vanguard U.S. 500 Stock Index Fund EUR Acc, and Vanguard published a EUR NAV for it.",
            reason:
              "Exact ISIN match; NAV retrieved from Vanguard official fund page.",
            transaction_class: "investment_trade_buy",
            category_code: "stock_buy",
            merchant_normalized: null,
            counterparty_name: null,
            economic_entity_override: null,
            security_hint: "VANGUARD US 500 STOCK INDEX EU",
            confidence: 0.94,
          },
        },
        reviewContext: {
          trigger: "manual_review_update",
          userProvidedContext:
            "Exact ISIN is IE0032126645 for the Vanguard U.S. 500 Stock Index Fund EUR Acc purchase.",
        },
      },
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-24");

    assert.equal(rebuilt.insertedSecurities[0]?.providerName, "llm_web_search");
    assert.equal(rebuilt.insertedSecurities[0]?.providerSymbol, "IE0032126645");
    assert.equal(rebuilt.insertedSecurities[0]?.isin, "IE0032126645");
    assert.equal(
      rebuilt.transactionPatches[0]?.securityId,
      rebuilt.insertedSecurities[0]?.id,
    );
    assert.match(
      rebuilt.transactionPatches[0]?.reviewReason ?? "",
      /stored nav/i,
    );
    assert.equal(searchQueries.length, 0);
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
    if (previousOpenAiKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousOpenAiKey;
    }
  }
});

test("investment rebuild prefers an exact stored alias over a stale carried security mapping", async () => {
  const account = createAccount({
    id: "broker-emerging-alias-remap",
    assetDomain: "investment",
    accountType: "brokerage_account",
    institutionName: "Broker",
    displayName: "Brokerage",
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [
      createTransaction({
        id: "emerging-markets-stale",
        accountId: account.id,
        accountEntityId: account.entityId,
        economicEntityId: account.entityId,
        transactionDate: "2026-01-06",
        postedDate: "2026-01-07",
        amountOriginal: "-197.97",
        amountBaseEur: "-197.97",
        currencyOriginal: "EUR",
        descriptionRaw: "EMERGING MARKETS STOCK EUR ACC",
        descriptionClean: "EMERGING MARKETS STOCK EUR ACC",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        classificationStatus: "llm",
        classificationSource: "llm",
        classificationConfidence: "0.93",
        securityId: "security-stale-sandp",
        quantity: null,
        unitPriceOriginal: null,
        needsReview: true,
        reviewReason:
          'Mapped to IE0032126645, but no reliable historical fund price was available to derive quantity for "Emerging Markets Stock EUR Acc".',
        manualNotes: "The ISIN is wrong.",
        llmPayload: {
          llm: {
            rawOutput: {
              securityHint: "EMERGING MARKETS STOCK EUR ACC",
              resolvedInstrumentIsin: null,
              resolvedInstrumentName: "Emerging Markets Stock EUR Acc",
              resolutionProcess: null,
            },
          },
        },
      }),
    ],
    securities: [
      {
        id: "security-stale-sandp",
        providerName: "llm_web_search",
        providerSymbol: "IE0032126645",
        canonicalSymbol: "IE0032126645",
        displaySymbol: "IE0032126645",
        name: "VANGUARD US 500 STOCK INDEX EU",
        exchangeName: "WEB",
        micCode: null,
        assetType: "other",
        quoteCurrency: "EUR",
        country: null,
        isin: "IE0032126645",
        figi: null,
        active: true,
        metadataJson: {
          instrumentType: "Mutual Fund",
        },
        lastPriceRefreshAt: null,
        createdAt: "2026-01-01T00:00:00Z",
      },
      {
        id: "security-emerging-eur",
        providerName: "morningstar",
        providerSymbol: "0P000060MS",
        canonicalSymbol: "0P000060MS",
        displaySymbol: "0P000060MS",
        name: "Vanguard Emerging Markets Stock Index Fund Investor EUR Accumulation",
        exchangeName: "FUND",
        micCode: null,
        assetType: "other",
        quoteCurrency: "EUR",
        country: null,
        isin: null,
        figi: null,
        active: true,
        metadataJson: {
          instrumentType: "Mutual Fund",
        },
        lastPriceRefreshAt: null,
        createdAt: "2026-01-02T00:00:00Z",
      },
    ],
    securityAliases: [
      {
        id: "alias-emerging-eur",
        securityId: "security-emerging-eur",
        aliasTextNormalized: "EMERGING MARKETS STOCK EUR ACC",
        aliasSource: "provider",
        templateId: null,
        confidence: "0.9000",
        createdAt: "2026-01-02T00:00:00Z",
      },
    ],
  });

  const rebuilt = await prepareInvestmentRebuild(dataset, "2026-01-07");
  const patch = rebuilt.transactionPatches.find(
    (candidate) => candidate.id === "emerging-markets-stale",
  );

  assert.equal(rebuilt.insertedSecurities.length, 0);
  assert.equal(patch?.securityId, "security-emerging-eur");
  assert.notEqual(patch?.securityId, "security-stale-sandp");
});

test("investment rebuild derives quantity from stored NAV history for exact fund ISIN matches", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousOpenAiKey = process.env.OPENAI_API_KEY;
  const previousFetch = globalThis.fetch;
  let fetchCalls = 0;
  delete process.env.TWELVE_DATA_API_KEY;
  delete process.env.OPENAI_API_KEY;
  globalThis.fetch = async () => {
    fetchCalls += 1;
    return new Response(JSON.stringify({ status: "error" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const account = createAccount({
      id: "broker-historical-nav-fund",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "eurozone-fund-buy",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-03",
      postedDate: "2026-03-03",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      currencyOriginal: "EUR",
      descriptionRaw: "VANGUARD EUROZONE STOCK INDEX",
      descriptionClean: "VANGUARD EUROZONE STOCK INDEX",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "llm",
      classificationSource: "llm",
      classificationConfidence: "0.94",
      securityId: null,
      quantity: null,
      unitPriceOriginal: null,
      needsReview: true,
      reviewReason: "Quantity still needs to be derived.",
      llmPayload: {
        llm: {
          rawOutput: {
            resolved_instrument_name:
              "Vanguard Eurozone Stock Index Fund EUR Acc",
            resolved_instrument_isin: "IE0032126645",
            current_price: 61.11,
            current_price_currency: "EUR",
            current_price_timestamp: "2026-04-04T09:00:00Z",
            current_price_source: "Official Vanguard factsheet",
            current_price_type: "NAV",
          },
        },
        reviewContext: {
          trigger: "manual_review_update",
          userProvidedContext:
            "Exact ISIN is IE0032126645 for the Vanguard Eurozone Stock Index Fund EUR Acc purchase.",
        },
      },
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-web-eurozone",
          providerName: "llm_web_search",
          providerSymbol: "IE0032126645",
          canonicalSymbol: "IE0032126645",
          displaySymbol: "IE0032126645",
          name: "Vanguard Eurozone Stock Index Fund EUR Acc",
          exchangeName: "WEB",
          micCode: null,
          assetType: "other",
          quoteCurrency: "EUR",
          country: null,
          isin: "IE0032126645",
          figi: null,
          active: true,
          metadataJson: {
            instrumentType: "Mutual Fund",
          },
          lastPriceRefreshAt: null,
          createdAt: "2026-03-01T00:00:00Z",
        },
        {
          id: "security-manual-eurozone",
          providerName: "manual_fund_nav",
          providerSymbol: "IE0032126645",
          canonicalSymbol: "VANESII",
          displaySymbol: "VANESII",
          name: "Vanguard Eurozone Stock Index Fund EUR Acc",
          exchangeName: "VANGUARD",
          micCode: null,
          assetType: "other",
          quoteCurrency: "EUR",
          country: "IE",
          isin: "IE0032126645",
          figi: null,
          active: true,
          metadataJson: {
            instrumentType: "mutual_fund",
            shareClass: "EUR Acc",
          },
          lastPriceRefreshAt: null,
          createdAt: "2026-03-01T00:00:00Z",
        },
      ],
      securityPrices: [
        {
          securityId: "security-manual-eurozone",
          priceDate: "2026-03-03",
          quoteTimestamp: "2026-03-03T16:00:00Z",
          price: "49.79000000",
          currency: "EUR",
          sourceName: "manual_nav_import",
          isRealtime: false,
          isDelayed: true,
          marketState: "official_nav",
          rawJson: {
            priceType: "nav",
          },
          createdAt: "2026-03-03T16:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-03");
    const patch = rebuilt.transactionPatches.find(
      (candidate) => candidate.id === "eurozone-fund-buy",
    );

    assert.equal(fetchCalls, 0);
    assert.equal(rebuilt.insertedSecurities.length, 0);
    assert.equal(patch?.securityId, "security-manual-eurozone");
    assert.equal(patch?.quantity, "2.00000000");
    assert.equal(patch?.unitPriceOriginal, "49.79000000");
    assert.equal(patch?.needsReview, false);
    assert.equal(patch?.reviewReason, null);
    assert.equal(
      (
        patch?.rebuildEvidence as {
          historicalPriceUsed?: {
            sourceName?: string | null;
            priceDate?: string | null;
          } | null;
          quantityDerivedFromHistoricalPrice?: boolean;
        } | null
      )?.historicalPriceUsed?.sourceName,
      "manual_nav_import",
    );
    assert.equal(
      (
        patch?.rebuildEvidence as {
          historicalPriceUsed?: {
            sourceName?: string | null;
            priceDate?: string | null;
          } | null;
          quantityDerivedFromHistoricalPrice?: boolean;
        } | null
      )?.historicalPriceUsed?.priceDate,
      "2026-03-03",
    );
    assert.equal(
      (
        patch?.rebuildEvidence as {
          quantityDerivedFromHistoricalPrice?: boolean;
        } | null
      )?.quantityDerivedFromHistoricalPrice,
      true,
    );
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
    if (previousOpenAiKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousOpenAiKey;
    }
  }
});

test("investment rebuild derives signed sell quantity from stored NAV history for exact fund ISIN matches", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  delete process.env.TWELVE_DATA_API_KEY;

  try {
    const account = createAccount({
      id: "broker-historical-nav-fund-sell",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "eurozone-fund-sell",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-03",
      postedDate: "2026-03-03",
      amountOriginal: "99.58",
      amountBaseEur: "99.58",
      currencyOriginal: "EUR",
      descriptionRaw: "VANGUARD EUROZONE STOCK INDEX",
      descriptionClean: "VANGUARD EUROZONE STOCK INDEX",
      transactionClass: "investment_trade_sell",
      categoryCode: "stock_buy",
      classificationStatus: "llm",
      classificationSource: "llm",
      classificationConfidence: "0.94",
      securityId: null,
      quantity: null,
      unitPriceOriginal: null,
      needsReview: true,
      reviewReason: "Quantity still needs to be derived.",
      llmPayload: {
        llm: {
          rawOutput: {
            resolved_instrument_name:
              "Vanguard Eurozone Stock Index Fund EUR Acc",
            resolved_instrument_isin: "IE0032126645",
            current_price_type: "NAV",
          },
        },
      },
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-manual-eurozone-sell",
          providerName: "manual_fund_nav",
          providerSymbol: "IE0032126645",
          canonicalSymbol: "VANESII",
          displaySymbol: "VANESII",
          name: "Vanguard Eurozone Stock Index Fund EUR Acc",
          exchangeName: "VANGUARD",
          micCode: null,
          assetType: "other",
          quoteCurrency: "EUR",
          country: "IE",
          isin: "IE0032126645",
          figi: null,
          active: true,
          metadataJson: {
            instrumentType: "mutual_fund",
          },
          lastPriceRefreshAt: null,
          createdAt: "2026-03-01T00:00:00Z",
        },
      ],
      securityPrices: [
        {
          securityId: "security-manual-eurozone-sell",
          priceDate: "2026-03-03",
          quoteTimestamp: "2026-03-03T16:00:00Z",
          price: "49.79000000",
          currency: "EUR",
          sourceName: "manual_nav_import",
          isRealtime: false,
          isDelayed: true,
          marketState: "official_nav",
          rawJson: {
            priceType: "nav",
          },
          createdAt: "2026-03-03T16:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-03");
    const patch = rebuilt.transactionPatches.find(
      (candidate) => candidate.id === "eurozone-fund-sell",
    );

    assert.equal(patch?.securityId, "security-manual-eurozone-sell");
    assert.equal(patch?.quantity, "-2.00000000");
    assert.equal(patch?.unitPriceOriginal, "49.79000000");
    assert.equal(patch?.needsReview, false);
    assert.equal(patch?.reviewReason, null);
  } finally {
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild still derives quantity from stored NAV history during scoped rebuilds", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousFetch = globalThis.fetch;
  let fetchCalls = 0;
  process.env.TWELVE_DATA_API_KEY = "test-key";
  globalThis.fetch = async () => {
    fetchCalls += 1;
    return new Response(JSON.stringify({ status: "error" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };

  try {
    const account = createAccount({
      id: "broker-scoped-stored-nav",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "eurozone-fund-scoped-rebuild",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2025-12-02",
      postedDate: "2025-12-03",
      amountOriginal: "-48.55",
      amountBaseEur: "-48.55",
      currencyOriginal: "EUR",
      descriptionRaw: "EUROZONE STOCK INDEX EUR @ 0.",
      descriptionClean: "EUROZONE STOCK INDEX EUR @ 0.",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "llm",
      classificationSource: "llm",
      classificationConfidence: "0.99",
      securityId: "security-manual-eurozone-scoped",
      quantity: null,
      unitPriceOriginal: null,
      needsReview: true,
      reviewReason: "Quantity still needs to be derived.",
      llmPayload: {
        llm: {
          rawOutput: {
            resolved_instrument_name:
              "Vanguard Eurozone Stock Index Fund EUR Acc",
            resolved_instrument_isin: "IE0008248803",
            current_price_type: "NAV",
          },
        },
      },
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-manual-eurozone-scoped",
          providerName: "manual_fund_nav",
          providerSymbol: "IE0008248803",
          canonicalSymbol: "VANESII",
          displaySymbol: "VANESII",
          name: "Vanguard Eurozone Stock Index Fund EUR Acc",
          exchangeName: "VANGUARD",
          micCode: null,
          assetType: "other",
          quoteCurrency: "EUR",
          country: "IE",
          isin: "IE0008248803",
          figi: null,
          active: true,
          metadataJson: {
            instrumentType: "mutual_fund",
          },
          lastPriceRefreshAt: null,
          createdAt: "2026-03-01T00:00:00Z",
        },
      ],
      securityPrices: [
        {
          securityId: "security-manual-eurozone-scoped",
          priceDate: "2025-12-02",
          quoteTimestamp: "2025-12-02T16:00:00Z",
          price: "374.52000000",
          currency: "EUR",
          sourceName: "manual_nav_import",
          isRealtime: false,
          isDelayed: true,
          marketState: "official_nav",
          rawJson: {
            priceType: "nav",
          },
          createdAt: "2025-12-02T16:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-04-04", {
      historicalLookupTransactionIds: ["some-other-transaction"],
    });
    const patch = rebuilt.transactionPatches.find(
      (candidate) => candidate.id === "eurozone-fund-scoped-rebuild",
    );

    assert.equal(fetchCalls, 0);
    assert.equal(patch?.quantity, "0.12963260");
    assert.equal(patch?.unitPriceOriginal, "374.52000000");
    assert.equal(patch?.needsReview, false);
    assert.equal(patch?.reviewReason, null);
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
  }
});

test("investment rebuild persists confirmed description aliases for exact fund resolutions", async () => {
  const account = createAccount({
    id: "broker-confirmed-aliases",
    assetDomain: "investment",
    accountType: "brokerage_account",
    institutionName: "Broker",
    displayName: "Brokerage",
  });
  const dataset = createDataset({
    accounts: [account],
    transactions: [
      createTransaction({
        id: "vanguard-confirmed-alias",
        accountId: account.id,
        accountEntityId: account.entityId,
        economicEntityId: account.entityId,
        transactionDate: "2026-03-03",
        postedDate: "2026-03-03",
        amountOriginal: "-99.58",
        amountBaseEur: "-99.58",
        currencyOriginal: "EUR",
        descriptionRaw: "VANGUARD S&P500 EUR ACC",
        descriptionClean: "VANGUARD S&P500 EUR ACC",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        classificationStatus: "llm",
        classificationSource: "llm",
        classificationConfidence: "0.94",
        securityId: "security-vanguard-sandp-fund",
        quantity: null,
        unitPriceOriginal: null,
        needsReview: true,
        reviewReason: "Quantity still needs to be derived.",
        llmPayload: {
          llm: {
            rawOutput: {
              securityHint: "VANGUARD S&P500 EUR ACC",
              resolvedInstrumentName:
                "Vanguard S&P 500 Stock Index Fund EUR Acc",
              resolvedInstrumentIsin: "IE0032126645",
              resolutionProcess:
                "Matched the exact ISIN IE0032126645 from the fund name and share class.",
            },
          },
        },
      }),
    ],
    securities: [
      {
        id: "security-vanguard-sandp-fund",
        providerName: "llm_web_search",
        providerSymbol: "IE0032126645",
        canonicalSymbol: "IE0032126645",
        displaySymbol: "IE0032126645",
        name: "Vanguard S&P 500 Stock Index Fund EUR Acc",
        exchangeName: "WEB",
        micCode: null,
        assetType: "other",
        quoteCurrency: "EUR",
        country: null,
        isin: "IE0032126645",
        figi: null,
        active: true,
        metadataJson: {
          instrumentType: "Mutual Fund",
        },
        lastPriceRefreshAt: null,
        createdAt: "2026-03-01T00:00:00Z",
      },
    ],
  });

  const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-03");

  assert.ok(
    rebuilt.insertedAliases.some(
      (alias) =>
        alias.securityId === "security-vanguard-sandp-fund" &&
        alias.aliasTextNormalized === "VANGUARD S&P500 EUR ACC",
    ),
  );
  assert.ok(
    rebuilt.insertedAliases.some(
      (alias) =>
        alias.securityId === "security-vanguard-sandp-fund" &&
        alias.aliasTextNormalized === "IE0032126645",
    ),
  );
});

test("investment rebuild remaps stale fund securities from camelized review output and persists the resolved ISIN", async () => {
  const previousApiKey = process.env.TWELVE_DATA_API_KEY;
  const previousOpenAiKey = process.env.OPENAI_API_KEY;
  const previousFetch = globalThis.fetch;
  delete process.env.TWELVE_DATA_API_KEY;
  delete process.env.OPENAI_API_KEY;
  globalThis.fetch = previousFetch;

  try {
    const account = createAccount({
      id: "broker-camelized-fund-remap",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const staleSecurityId = "security-stale-eurozone";
    const transaction = createTransaction({
      id: "eurozone-fund-buy-camelized",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-03",
      postedDate: "2026-03-03",
      amountOriginal: "-46.82",
      amountBaseEur: "-46.82",
      currencyOriginal: "EUR",
      descriptionRaw: "VANGUARD EUROZONE STOCK INDEX",
      descriptionClean: "VANGUARD EUROZONE STOCK INDEX",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "llm",
      classificationSource: "llm",
      classificationConfidence: "0.91",
      securityId: staleSecurityId,
      quantity: null,
      unitPriceOriginal: null,
      needsReview: true,
      reviewReason:
        'Mapped to IE0032126645, but no reliable historical fund price was available to derive quantity for "VANGUARD EUROZONE STOCK INDEX".',
      llmPayload: {
        llm: {
          rawOutput: {
            resolvedInstrumentName:
              "Vanguard Eurozone Stock Index Fund - EUR Acc",
            resolvedInstrumentIsin: "IE0008248803",
            resolvedInstrumentTicker: "VANESII",
            resolvedInstrumentExchange: null,
            currentPrice: null,
            currentPriceCurrency: null,
            currentPriceTimestamp: null,
            currentPriceSource: null,
            currentPriceType: null,
            resolutionProcess:
              "Resolved from the exact fund name and the corrected ISIN IE0008248803.",
          },
        },
        reviewContext: {
          trigger: "manual_review_update",
          userProvidedContext:
            "The prior mapping was wrong. The correct ISIN is IE0008248803.",
        },
      },
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: staleSecurityId,
          providerName: "llm_web_search",
          providerSymbol: "IE0032126645",
          canonicalSymbol: "IE0032126645",
          displaySymbol: "IE0032126645",
          name: "VANGUARD US 500 STOCK INDEX EU",
          exchangeName: "WEB",
          micCode: null,
          assetType: "other",
          quoteCurrency: "EUR",
          country: null,
          isin: "IE0032126645",
          figi: null,
          active: true,
          metadataJson: {
            instrumentType: "Mutual Fund",
          },
          lastPriceRefreshAt: null,
          createdAt: "2026-03-01T00:00:00Z",
        },
      ],
    });

    const rebuilt = await prepareInvestmentRebuild(dataset, "2026-03-03");
    const patch = rebuilt.transactionPatches.find(
      (candidate) => candidate.id === "eurozone-fund-buy-camelized",
    );

    assert.equal(rebuilt.insertedSecurities.length, 1);
    assert.equal(rebuilt.insertedSecurities[0]?.providerName, "llm_web_search");
    assert.equal(rebuilt.insertedSecurities[0]?.providerSymbol, "IE0008248803");
    assert.equal(rebuilt.insertedSecurities[0]?.displaySymbol, "IE0008248803");
    assert.equal(rebuilt.insertedSecurities[0]?.isin, "IE0008248803");
    assert.notEqual(patch?.securityId, staleSecurityId);
    assert.equal(patch?.securityId, rebuilt.insertedSecurities[0]?.id);
    assert.match(patch?.reviewReason ?? "", /IE0008248803/);
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.TWELVE_DATA_API_KEY;
    } else {
      process.env.TWELVE_DATA_API_KEY = previousApiKey;
    }
    if (previousOpenAiKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousOpenAiKey;
    }
  }
});

test("successful confident LLM classifications clear fallback review state", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  const previousFetch = globalThis.fetch;
  process.env.OPENAI_API_KEY = "test-key";
  globalThis.fetch = async () =>
    new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "transfer_internal",
          category_code: "uncategorized_investment",
          merchant_normalized: null,
          counterparty_name: null,
          economic_entity_override: null,
          security_hint: null,
          resolution_process: null,
          confidence: 0.9,
          explanation: "Looks like a transfer between owned accounts.",
          reason: "Description indicates an internal transfer.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );

  try {
    const account = createAccount({
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "broker-transfer",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      descriptionRaw: "Transferencia My Investor",
      descriptionClean: "TRANSFERENCIA MY INVESTOR",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
    });

    const decision = await enrichImportedTransaction(
      dataset,
      account,
      transaction,
      {
        trigger: "manual_review_update",
        reviewContext: {
          previousReviewReason: transaction.reviewReason ?? null,
          previousUserContext: "Previous manual note.",
          previousLlmPayload: {
            analysisStatus: "done",
            model: "gpt-4.1-mini",
          },
          userProvidedContext:
            "This is a broker commission for GOOG, not a stock sale.",
        },
      },
    );
    const llmPayload = decision.llmPayload as {
      reviewContext?: {
        userProvidedContext?: string;
        trigger?: string;
      };
      timing?: {
        requestedAt?: string;
        completedAt?: string;
        durationMs?: number;
      };
    };

    assert.equal(decision.classificationSource, "llm");
    assert.equal(decision.transactionClass, "transfer_internal");
    assert.equal(decision.needsReview, false);
    assert.equal(decision.reviewReason, null);
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
    globalThis.fetch = previousFetch;
  }
});

test("invalid LLM economic entity overrides are ignored", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  const previousFetch = globalThis.fetch;
  process.env.OPENAI_API_KEY = "test-key";
  globalThis.fetch = async () =>
    new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "investment_trade_buy",
          category_code: "stock_buy",
          merchant_normalized: "Vanguard",
          counterparty_name: "Vanguard Japan Stock EUR INS",
          economic_entity_override: "Vanguard Japan Stock EUR INS",
          security_hint: "Vanguard Japan Stock EUR INS",
          resolution_process: null,
          confidence: 0.91,
          explanation: "This is a clearly named Vanguard investment purchase.",
          reason:
            "The description names a Vanguard fund, but it does not contain a valid entity override.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );

  try {
    const account = createAccount({
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "MyInvestor",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "vanguard-invalid-entity-override",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      descriptionRaw: "VANGUARD JAPAN STOCK EUR INS @",
      descriptionClean: "VANGUARD JAPAN STOCK EUR INS @",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
    });

    const decision = await enrichImportedTransaction(
      dataset,
      account,
      transaction,
    );

    assert.equal(decision.classificationSource, "llm");
    assert.equal(decision.transactionClass, "investment_trade_buy");
    assert.equal(decision.economicEntityId, account.entityId);
    assert.equal(
      decision.llmPayload.llm &&
        typeof decision.llmPayload.llm === "object" &&
        "economicEntityId" in decision.llmPayload.llm
        ? (decision.llmPayload.llm as { economicEntityId: string | null })
            .economicEntityId
        : null,
      null,
    );
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
    globalThis.fetch = previousFetch;
  }
});

test("investment review includes portfolio state and can override commission-like sells", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  const previousImportModel = process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
  const previousFollowupModel =
    process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM;
  const previousFetch = globalThis.fetch;
  let capturedUserPrompt = "";
  let capturedSystemPrompt = "";
  let capturedTools: unknown[] | null = null;
  let capturedToolChoice: string | null = null;
  let capturedModel = "";
  process.env.OPENAI_API_KEY = "test-key";
  process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = "gpt-5.4-mini";
  process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM = "gpt-5.4";
  globalThis.fetch = async (input, init) => {
    assert.equal(input, "https://api.openai.com/v1/responses");
    const requestBody = JSON.parse(String(init?.body ?? "{}")) as {
      model?: string;
      tools?: unknown[];
      tool_choice?: string;
      input?: Array<{ role?: string; content?: Array<{ text?: string }> }>;
    };
    capturedModel =
      typeof requestBody.model === "string" ? requestBody.model : "";
    capturedTools = Array.isArray(requestBody.tools) ? requestBody.tools : null;
    capturedToolChoice =
      typeof requestBody.tool_choice === "string"
        ? requestBody.tool_choice
        : null;
    capturedSystemPrompt =
      requestBody.input?.find((item) => item.role === "system")?.content?.[0]
        ?.text ?? "";
    capturedUserPrompt =
      requestBody.input?.find((item) => item.role === "user")?.content?.[0]
        ?.text ?? "";

    return new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "fee",
          category_code: "broker_fee",
          merchant_normalized: null,
          counterparty_name: null,
          economic_entity_override: null,
          security_hint: "ALPHABET INC CL C",
          resolved_instrument_name: "Alphabet Inc Class C",
          resolved_instrument_isin: "US02079K1079",
          resolved_instrument_ticker: "GOOG",
          resolved_instrument_exchange: "NASDAQ",
          current_price: 215.4,
          current_price_currency: "USD",
          current_price_timestamp: "2026-03-16T20:00:00Z",
          current_price_source: "NASDAQ delayed quote",
          current_price_type: "delayed",
          resolution_process:
            "Matched the exact GOOG listing from the security hint and validated that the row behaves like a broker fee instead of a disposal.",
          confidence: 0.95,
          explanation: "The row looks like a broker commission, not a sale.",
          reason:
            "The implied per-share amount is far below the latest GOOG quote while the position remains open.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  };

  try {
    const account = createAccount({
      id: "broker-goog",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "MyInvestor",
      displayName: "Brokerage",
      defaultCurrency: "EUR",
    });
    const otherAccount = createAccount({
      id: "broker-other",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Other Broker",
      displayName: "Other Brokerage",
      defaultCurrency: "EUR",
    });
    const transaction = createTransaction({
      id: "goog-commission-row",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-16",
      postedDate: "2026-03-16",
      amountOriginal: "1.00",
      amountBaseEur: "1.00",
      currencyOriginal: "EUR",
      descriptionRaw: "ALPHABET INC CL C @ 8",
      descriptionClean: "ALPHABET INC CL C @ 8",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
      manualNotes: "Previous manual note.",
      securityId: "security-goog",
      quantity: null,
      unitPriceOriginal: null,
    });
    const baseCategories = createDataset().categories;
    const dataset = createDataset({
      accounts: [account, otherAccount],
      auditEvents: [
        {
          id: "audit-interest-example",
          actorType: "user",
          actorId: "user-1",
          actorName: "web-review-editor",
          sourceChannel: "web",
          commandName: "transactions.review_reanalyze",
          objectType: "transaction",
          objectId: "interest-example-tx",
          beforeJson: {
            accountId: account.id,
            transactionDate: "2026-03-20",
            postedDate: "2026-03-20",
            amountOriginal: "0.14",
            currencyOriginal: "EUR",
            descriptionRaw: "PERIODO 19/02/2026 19/03/2026",
            merchantNormalized: null,
            counterpartyName: null,
            securityId: null,
            quantity: null,
            unitPriceOriginal: null,
            transactionClass: "unknown",
            categoryCode: "uncategorized_investment",
            classificationSource: "llm",
            classificationStatus: "llm",
            classificationConfidence: "0.51",
            needsReview: true,
            reviewReason: "Needs user confirmation.",
            llmPayload: {
              model: "gpt-4.1-mini",
              explanation:
                "No deterministic classifier matched the imported row.",
              reason: "The row might be interest, but the context is thin.",
            },
          },
          afterJson: {
            accountId: account.id,
            transactionClass: "interest",
            categoryCode: "uncategorized_investment",
            merchantNormalized: "MyInvestor",
            counterpartyName: "MyInvestor",
            quantity: null,
            unitPriceOriginal: null,
            reviewReason: null,
            manualNotes: "This is, in fact, earned interest.",
            llmPayload: {
              reviewContext: {
                userProvidedContext: "This is, in fact, earned interest.",
              },
            },
          },
          createdAt: "2026-03-21T09:00:00Z",
          notes:
            "Re-ran LLM classification for a single transaction with manual review context.",
        },
      ],
      categories: [
        ...baseCategories,
        {
          code: "broker_fee",
          displayName: "Broker Fee",
          parentCode: null,
          scopeKind: "investment",
          directionKind: "investment",
          sortOrder: 50,
          active: true,
          metadataJson: {},
        },
      ],
      transactions: [
        transaction,
        createTransaction({
          id: "goog-similar-history",
          accountId: account.id,
          accountEntityId: account.entityId,
          economicEntityId: account.entityId,
          transactionDate: "2026-03-05",
          postedDate: "2026-03-05",
          amountOriginal: "-1040.00",
          amountBaseEur: "-1040.00",
          currencyOriginal: "EUR",
          descriptionRaw: "ALPHABET INC CL C @ 5",
          descriptionClean: "ALPHABET INC CL C @ 5",
          transactionClass: "investment_trade_buy",
          categoryCode: "stock_buy",
          classificationStatus: "investment_parser",
          classificationSource: "investment_parser",
          classificationConfidence: "0.96",
          needsReview: false,
          reviewReason: null,
          securityId: "security-goog",
          quantity: "5.00000000",
          unitPriceOriginal: "208.00000000",
        }),
        createTransaction({
          id: "goog-pending-history",
          accountId: account.id,
          accountEntityId: account.entityId,
          economicEntityId: account.entityId,
          transactionDate: "2026-03-08",
          postedDate: "2026-03-08",
          amountOriginal: "-999.00",
          amountBaseEur: "-999.00",
          currencyOriginal: "EUR",
          descriptionRaw: "ALPHABET INC CL C PENDING REVIEW",
          descriptionClean: "ALPHABET INC CL C PENDING REVIEW",
          transactionClass: "investment_trade_buy",
          categoryCode: "stock_buy",
          classificationStatus: "llm",
          classificationSource: "llm",
          classificationConfidence: "0.52",
          needsReview: true,
          reviewReason: "Still ambiguous.",
          securityId: "security-goog",
          quantity: "5.00000000",
          unitPriceOriginal: "199.80000000",
        }),
        createTransaction({
          id: "other-account-goog",
          accountId: otherAccount.id,
          accountEntityId: otherAccount.entityId,
          economicEntityId: otherAccount.entityId,
          transactionDate: "2026-03-06",
          postedDate: "2026-03-06",
          amountOriginal: "-1200.00",
          amountBaseEur: "-1200.00",
          currencyOriginal: "EUR",
          descriptionRaw: "ALPHABET INC CL C FROM OTHER ACCOUNT",
          descriptionClean: "ALPHABET INC CL C FROM OTHER ACCOUNT",
          transactionClass: "investment_trade_buy",
          categoryCode: "stock_buy",
          classificationStatus: "investment_parser",
          classificationSource: "investment_parser",
          classificationConfidence: "0.96",
          needsReview: false,
          reviewReason: null,
          securityId: "security-goog",
          quantity: "6.00000000",
          unitPriceOriginal: "200.00000000",
        }),
      ],
      securities: [
        {
          id: "security-goog",
          providerName: "twelve_data",
          providerSymbol: "GOOG",
          canonicalSymbol: "GOOG",
          displaySymbol: "GOOG",
          name: "Alphabet Inc Class C",
          exchangeName: "NASDAQ",
          micCode: "XNAS",
          assetType: "stock",
          quoteCurrency: "USD",
          country: "US",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
      securityPrices: [
        {
          securityId: "security-goog",
          priceDate: "2026-03-16",
          quoteTimestamp: "2026-03-16T20:00:00Z",
          price: "215.40",
          currency: "USD",
          sourceName: "twelve_data",
          isRealtime: false,
          isDelayed: true,
          marketState: "closed",
          rawJson: { close: "215.40" },
          createdAt: "2026-03-16T20:00:00Z",
        },
      ],
      investmentPositions: [
        {
          userId: "user-1",
          entityId: account.entityId,
          accountId: account.id,
          securityId: "security-goog",
          openQuantity: "45.00000000",
          openCostBasisEur: "7200.00000000",
          avgCostEur: "160.00000000",
          realizedPnlEur: "0.00000000",
          dividendsEur: "0.00000000",
          interestEur: "0.00000000",
          feesEur: "0.00000000",
          lastTradeDate: "2026-03-01",
          lastRebuiltAt: "2026-03-16T20:00:00Z",
          provenanceJson: { source: "transactions" },
          unrealizedComplete: true,
        },
      ],
    });

    const decision = await enrichImportedTransaction(
      dataset,
      account,
      transaction,
      {
        trigger: "manual_review_update",
        reviewContext: {
          userProvidedContext:
            "This is a broker commission for GOOG, not a stock sale.",
        },
      },
    );

    assert.equal(decision.classificationSource, "llm");
    assert.equal(decision.transactionClass, "fee");
    assert.equal(decision.categoryCode, "broker_fee");
    assert.equal(decision.needsReview, false);
    assert.equal(decision.quantity, null);
    assert.equal(decision.unitPriceOriginal, null);
    assert.equal(capturedModel, "gpt-5.4");
    assert.deepEqual(capturedTools, [{ type: "web_search" }]);
    assert.equal(capturedToolChoice, "auto");
    assert.match(
      capturedSystemPrompt,
      /You are a security-resolution and pricing agent for a personal finance application\./,
    );
    assert.match(
      capturedSystemPrompt,
      /Never map a transaction to a security based on index wording alone\./,
    );
    assert.match(
      capturedSystemPrompt,
      /If any exact identifier such as ISIN, CUSIP, or SEDOL appears anywhere in the transaction, prior analysis, or user review context, search that identifier directly first/,
    );
    assert.match(
      capturedSystemPrompt,
      /Once an exact ISIN is known for a mutual fund or index fund, use it to lock identity from issuer-originated pages that explicitly reference that ISIN\./,
    );
    assert.match(
      capturedSystemPrompt,
      /When you have an exact or near-exact resolution, populate the structured fields explicitly instead of leaving them only in explanation or reason\./,
    );
    assert.match(
      capturedSystemPrompt,
      /For mutual funds and non-exchange-traded index funds, use an explicit two-step workflow: first resolve identity, then stop at the exact ISIN and share class\./,
    );
    assert.match(capturedUserPrompt, /Portfolio state:/);
    assert.match(capturedUserPrompt, /Similar same-account resolved history:/);
    assert.match(
      capturedUserPrompt,
      /"descriptionRaw":"ALPHABET INC CL C @ 5"/,
    );
    assert.doesNotMatch(capturedUserPrompt, /ALPHABET INC CL C PENDING REVIEW/);
    assert.doesNotMatch(
      capturedUserPrompt,
      /ALPHABET INC CL C FROM OTHER ACCOUNT/,
    );
    assert.match(capturedUserPrompt, /"symbol":"GOOG"/);
    assert.match(capturedUserPrompt, /"quantity":"45\.00000000"/);
    assert.match(capturedUserPrompt, /"impliedUnitPrice":"0\.13"/);
    assert.match(capturedUserPrompt, /"latestHoldingPrice":"215\.40"/);
    assert.match(capturedUserPrompt, /Examples from prior user corrections:/);
    assert.match(
      capturedUserPrompt,
      /Example 1 transaction metadata: .*"descriptionRaw":"PERIODO 19\/02\/2026 19\/03\/2026"/,
    );
    assert.match(
      capturedUserPrompt,
      /Example 1 initial inference: .*"transactionClass":"unknown".*"model":"gpt-4\.1-mini"/,
    );
    assert.match(
      capturedUserPrompt,
      /Example 1 user feedback: This is, in fact, earned interest\./,
    );
    assert.match(
      capturedUserPrompt,
      /Example 1 corrected outcome: .*"transactionClass":"interest"/,
    );
    assert.match(capturedUserPrompt, /Review trigger: manual_review_update/);
    assert.match(
      capturedUserPrompt,
      /Previous user review context: Previous manual note\./,
    );
    assert.match(
      capturedUserPrompt,
      /New user review context: This is a broker commission for GOOG, not a stock sale\./,
    );
    const llmPayload = decision.llmPayload as {
      llm?: {
        rawOutput?: {
          current_price?: number | null;
          current_price_currency?: string | null;
          resolved_instrument_isin?: string | null;
        } | null;
      };
      reviewContext?: {
        userProvidedContext?: string | null;
        trigger?: string | null;
      };
      timing?: {
        requestedAt?: string | null;
        completedAt?: string | null;
        durationMs?: number | null;
      };
      reviewExamplesUsed?: Array<{
        auditEventId?: string | null;
      }>;
    };
    assert.equal(
      llmPayload.reviewContext?.userProvidedContext,
      "This is a broker commission for GOOG, not a stock sale.",
    );
    assert.equal(llmPayload.reviewContext?.trigger, "manual_review_update");
    assert.equal(llmPayload.llm?.rawOutput?.current_price, 215.4);
    assert.equal(llmPayload.llm?.rawOutput?.current_price_currency, "USD");
    assert.equal(
      llmPayload.llm?.rawOutput?.resolved_instrument_isin,
      "US02079K1079",
    );
    assert.equal(llmPayload.reviewExamplesUsed?.length, 1);
    assert.equal(
      llmPayload.reviewExamplesUsed?.[0]?.auditEventId,
      "audit-interest-example",
    );
    assert.equal(typeof llmPayload.timing?.requestedAt, "string");
    assert.equal(typeof llmPayload.timing?.completedAt, "string");
    assert.equal(typeof llmPayload.timing?.durationMs, "number");
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
    if (previousImportModel === undefined) {
      delete process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
    } else {
      process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = previousImportModel;
    }
    if (previousFollowupModel === undefined) {
      delete process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM;
    } else {
      process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM =
        previousFollowupModel;
    }
    globalThis.fetch = previousFetch;
  }
});

test("manual resolved review uses gpt-5.4-mini and includes similar resolved embedding context", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  const previousImportModel = process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
  const previousFollowupModel =
    process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM;
  const previousResolvedReviewModel =
    process.env.RESOLVED_TRANSACTION_REVIEW_LLM;
  const previousFetch = globalThis.fetch;
  let capturedUserPrompt = "";
  let capturedModel = "";
  process.env.OPENAI_API_KEY = "test-key";
  process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = "gpt-4.1-mini";
  process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM = "gpt-5.4";
  process.env.RESOLVED_TRANSACTION_REVIEW_LLM = "gpt-5.4-mini";
  globalThis.fetch = async (input, init) => {
    assert.equal(input, "https://api.openai.com/v1/responses");
    const requestBody = JSON.parse(String(init?.body ?? "{}")) as {
      model?: string;
      input?: Array<{ role?: string; content?: Array<{ text?: string }> }>;
    };
    capturedModel =
      typeof requestBody.model === "string" ? requestBody.model : "";
    capturedUserPrompt =
      requestBody.input?.find((item) => item.role === "user")?.content?.[0]
        ?.text ?? "";

    return new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "investment_trade_buy",
          category_code: "stock_buy",
          merchant_normalized: "MyInvestor",
          counterparty_name: "MyInvestor",
          economic_entity_override: null,
          security_hint: "VANGUARD S&P 500 UCITS ETF",
          resolved_instrument_name:
            "Vanguard S&P 500 UCITS ETF (USD) Distributing",
          resolved_instrument_isin: "IE00B3XXRP09",
          resolved_instrument_ticker: "VUSA",
          resolved_instrument_exchange: "XLON",
          current_price: 77.65,
          current_price_currency: "EUR",
          current_price_timestamp: "2026-04-04T16:00:00Z",
          current_price_source: "Official exchange quote",
          current_price_type: "delayed",
          resolution_process:
            "Matched the exact VUSA listing from a similar resolved transaction and confirmed the distributing UCITS ETF share class.",
          confidence: 0.93,
          explanation: "This is a Vanguard S&P 500 UCITS ETF purchase.",
          reason:
            "A highly similar resolved brokerage transaction points to the exact VUSA listing.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  };

  try {
    const account = createAccount({
      id: "broker-resolved-review",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "MyInvestor",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "resolved-review-target",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-04-02",
      postedDate: "2026-04-02",
      amountOriginal: "-1397.70",
      amountBaseEur: "-1397.70",
      currencyOriginal: "EUR",
      descriptionRaw: "VUSA Vanguard S&P 500 UCITS ETF",
      descriptionClean: "VUSA VANGUARD S&P 500 UCITS ETF",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "llm",
      classificationSource: "llm",
      classificationConfidence: "0.99",
      needsReview: false,
      reviewReason: null,
      securityId: "security-previously-wrong",
      quantity: "18.00000000",
      unitPriceOriginal: "77.65000000",
      llmPayload: {
        llm: {
          model: "gpt-5.4",
          rawOutput: {
            resolved_instrument_name: "Some previously assumed Vanguard fund",
          },
        },
      },
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
    });

    await enrichImportedTransaction(dataset, account, transaction, {
      trigger: "manual_resolved_review",
      reviewContext: {
        userProvidedContext:
          "This resolved row looks wrong. Reanalyze it from scratch.",
      },
      similarAccountTransactions: [
        {
          transactionDate: "2026-03-15",
          postedDate: "2026-03-15",
          amountOriginal: "-776.50",
          currencyOriginal: "EUR",
          descriptionRaw: "VANGUARD S&P 500 UCITS ETF VUSA",
          transactionClass: "investment_trade_buy",
          categoryCode: "stock_buy",
          merchantNormalized: "MyInvestor",
          counterpartyName: "MyInvestor",
          securityId: "security-vusa",
          quantity: "10.00000000",
          unitPriceOriginal: "77.65000000",
          reviewReason: null,
          similarityScore: "0.99",
          userProvidedContext:
            "Confirmed this exact ETF is VUSA on the London listing.",
          resolvedInstrumentName:
            "Vanguard S&P 500 UCITS ETF (USD) Distributing",
          resolvedInstrumentIsin: "IE00B3XXRP09",
          resolvedInstrumentTicker: "VUSA",
          resolvedInstrumentExchange: "XLON",
          currentPrice: 77.65,
          currentPriceCurrency: "EUR",
          currentPriceTimestamp: "2026-04-04T16:00:00Z",
          currentPriceSource: "Official exchange quote",
          currentPriceType: "delayed",
          resolutionProcess:
            "Resolved from the exact VUSA ticker and distributing ETF share class.",
          model: "gpt-5.4-mini",
        },
      ],
    });

    assert.equal(capturedModel, "gpt-5.4-mini");
    assert.match(capturedUserPrompt, /Review trigger: manual_resolved_review/);
    assert.match(capturedUserPrompt, /Similar same-account resolved history:/);
    assert.match(capturedUserPrompt, /"resolvedInstrumentTicker":"VUSA"/);
    assert.match(capturedUserPrompt, /"resolvedInstrumentIsin":"IE00B3XXRP09"/);
    assert.match(
      capturedUserPrompt,
      /"userProvidedContext":"Confirmed this exact ETF is VUSA on the London listing\."/,
    );
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
    if (previousImportModel === undefined) {
      delete process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
    } else {
      process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = previousImportModel;
    }
    if (previousFollowupModel === undefined) {
      delete process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM;
    } else {
      process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM =
        previousFollowupModel;
    }
    if (previousResolvedReviewModel === undefined) {
      delete process.env.RESOLVED_TRANSACTION_REVIEW_LLM;
    } else {
      process.env.RESOLVED_TRANSACTION_REVIEW_LLM = previousResolvedReviewModel;
    }
    globalThis.fetch = previousFetch;
  }
});

test("investment review includes persisted confirmed security mappings from stored aliases", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  const previousFetch = globalThis.fetch;
  let capturedUserPrompt = "";
  process.env.OPENAI_API_KEY = "test-key";
  globalThis.fetch = async (input, init) => {
    assert.equal(input, "https://api.openai.com/v1/responses");
    const requestBody = JSON.parse(String(init?.body ?? "{}")) as {
      input?: Array<{ role?: string; content?: Array<{ text?: string }> }>;
    };
    capturedUserPrompt =
      requestBody.input?.find((item) => item.role === "user")?.content?.[0]
        ?.text ?? "";

    return new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "investment_trade_buy",
          category_code: "stock_buy",
          merchant_normalized: "MyInvestor",
          counterparty_name: "MyInvestor",
          economic_entity_override: null,
          security_hint: "VANGUARD US 500 STOCK INDEX EU",
          resolved_instrument_name: null,
          resolved_instrument_isin: null,
          resolved_instrument_ticker: null,
          resolved_instrument_exchange: null,
          current_price: null,
          current_price_currency: null,
          current_price_timestamp: null,
          current_price_source: null,
          current_price_type: null,
          resolution_process: null,
          confidence: 0.82,
          explanation: "This is a fund purchase.",
          reason:
            "A stored confirmed mapping exists for this fund description.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  };

  try {
    const account = createAccount({
      id: "broker-persisted-mapping-prompt",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "vanguard-prompt-persisted",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-24",
      postedDate: "2026-03-24",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      currencyOriginal: "EUR",
      descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
      descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
      transactionClass: "unknown",
      categoryCode: "uncategorized_investment",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
      securityId: null,
      quantity: null,
      unitPriceOriginal: null,
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-vanguard-sandp-prompt",
          providerName: "llm_web_search",
          providerSymbol: "IE0032126645",
          canonicalSymbol: "IE0032126645",
          displaySymbol: "IE0032126645",
          name: "Vanguard S&P 500 Stock Index Fund EUR Acc",
          exchangeName: "WEB",
          micCode: null,
          assetType: "other",
          quoteCurrency: "EUR",
          country: null,
          isin: "IE0032126645",
          figi: null,
          active: true,
          metadataJson: {
            instrumentType: "Mutual Fund",
          },
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
      securityAliases: [
        {
          id: "alias-vanguard-sandp-prompt",
          securityId: "security-vanguard-sandp-prompt",
          aliasTextNormalized: "VANGUARD US 500 STOCK INDEX EU",
          aliasSource: "manual",
          templateId: null,
          confidence: "0.9900",
          createdAt: "2026-04-05T11:20:58.759Z",
        },
      ],
    });

    await enrichImportedTransaction(dataset, account, transaction, {
      trigger: "import_classification",
    });

    assert.match(capturedUserPrompt, /Persisted confirmed security mappings:/);
    assert.match(
      capturedUserPrompt,
      /"matchedAlias":"VANGUARD US 500 STOCK INDEX EU"/,
    );
    assert.match(capturedUserPrompt, /"isin":"IE0032126645"/);
    assert.match(capturedUserPrompt, /"aliasSource":"manual"/);
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
    globalThis.fetch = previousFetch;
  }
});

test("resolved-source propagation passes full resolved transaction data and llm output into candidate review context and keeps propagated precedent when still unresolved", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  const previousImportModel = process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
  const previousFollowupModel =
    process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM;
  const previousFetch = globalThis.fetch;
  let capturedUserPrompt = "";
  let capturedModel = "";
  process.env.OPENAI_API_KEY = "test-key";
  process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = "gpt-5.4-mini";
  process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM = "gpt-5.4";
  globalThis.fetch = async (input, init) => {
    assert.equal(input, "https://api.openai.com/v1/responses");
    const requestBody = JSON.parse(String(init?.body ?? "{}")) as {
      model?: string;
      input?: Array<{ role?: string; content?: Array<{ text?: string }> }>;
    };
    capturedModel =
      typeof requestBody.model === "string" ? requestBody.model : "";
    capturedUserPrompt =
      requestBody.input?.find((item) => item.role === "user")?.content?.[0]
        ?.text ?? "";

    return new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "investment_trade_buy",
          category_code: "stock_buy",
          merchant_normalized: null,
          counterparty_name: null,
          economic_entity_override: null,
          security_hint: "VANGUARD EUROZONE STOCK INDEX",
          resolved_instrument_name:
            "Vanguard Eurozone Stock Index Fund EUR Acc",
          resolved_instrument_isin: "IE0032126645",
          resolved_instrument_ticker: null,
          resolved_instrument_exchange: null,
          current_price: 61.11,
          current_price_currency: "EUR",
          current_price_timestamp: "2026-04-04T09:00:00Z",
          current_price_source: "Official Vanguard factsheet",
          current_price_type: "NAV",
          resolution_process: null,
          confidence: 0.62,
          explanation:
            "The source precedent helps, but the quantity remains unresolved.",
          reason:
            "The description likely refers to the same fund, but the quantity is still missing.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  };

  try {
    const account = createAccount({
      id: "broker-propagation-candidate",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const sourceTransaction = createTransaction({
      id: "resolved-source",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-03",
      postedDate: "2026-03-03",
      amountOriginal: "-99.58",
      amountBaseEur: "-99.58",
      currencyOriginal: "EUR",
      descriptionRaw: "VANGUARD EUROZONE STOCK INDEX",
      descriptionClean: "VANGUARD EUROZONE STOCK INDEX",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "llm",
      classificationSource: "llm",
      classificationConfidence: "0.94",
      securityId: "security-vanguard-eurozone",
      quantity: "2.00000000",
      unitPriceOriginal: "49.79000000",
      needsReview: false,
      reviewReason: null,
      manualNotes:
        "Exact ISIN is IE0032126645 for the Vanguard Eurozone Stock Index Fund EUR Acc purchase.",
      llmPayload: {
        llm: {
          model: "gpt-5.4-mini",
          explanation: "Resolved the exact fund from the ISIN.",
          reason:
            "Exact ISIN match; NAV retrieved from Vanguard official fund page.",
          rawOutput: {
            resolution_process:
              "Matched the exact ISIN IE0032126645 from user context, verified the official Vanguard fund page, and confirmed the EUR Acc share class.",
            resolved_instrument_isin: "IE0032126645",
          },
        },
        rebuildEvidence: {
          resolvedSecurityId: "security-vanguard-eurozone",
          historicalPriceUsed: {
            sourceName: "llm_historical_nav",
            priceDate: "2026-03-03",
            quoteTimestamp: "2026-03-03T00:00:00Z",
            price: "49.79000000",
            currency: "EUR",
            marketState: null,
          },
          quantityDerivedFromHistoricalPrice: true,
          rebuiltAt: "2026-03-03T12:00:00Z",
        },
      },
    });
    const candidateTransaction = createTransaction({
      id: "candidate-propagated",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2026-03-05",
      postedDate: "2026-03-05",
      amountOriginal: "-49.79",
      amountBaseEur: "-49.79",
      currencyOriginal: "EUR",
      descriptionRaw: "VANGUARD EUROZONE STOCK INDEX EUR ACC",
      descriptionClean: "VANGUARD EUROZONE STOCK INDEX EUR ACC",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "llm",
      classificationSource: "llm",
      classificationConfidence: "0.51",
      securityId: null,
      quantity: null,
      unitPriceOriginal: null,
      needsReview: true,
      reviewReason: "Security mapping unresolved.",
      llmPayload: {
        reviewContext: {
          propagatedContexts: [],
        },
      },
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [sourceTransaction, candidateTransaction],
    });
    const precedent = buildResolvedSourcePrecedent(
      sourceTransaction,
      "audit-source",
    );
    const propagatedEntry = buildResolvedSourcePropagatedContextEntry({
      sourceTransaction,
      sourceAuditEventId: "audit-source",
      similarity: 0.991,
      propagatedAt: "2026-04-05T09:00:00Z",
      precedent,
    });

    const decision = await enrichImportedTransaction(
      dataset,
      account,
      candidateTransaction,
      {
        trigger: "review_propagation",
        reviewContext: {
          propagatedContexts: [propagatedEntry],
          resolvedSourcePrecedent: precedent,
        },
      },
    );

    assert.equal(capturedModel, "gpt-5.4");
    assert.match(
      capturedUserPrompt,
      /Resolved source precedent from a similar transaction:/,
    );
    assert.match(capturedUserPrompt, /"sourceTransactionId":"resolved-source"/);
    assert.match(
      capturedUserPrompt,
      /"finalTransaction":\{"transactionClass":"investment_trade_buy","securityId":"security-vanguard-eurozone","quantity":"2\.00000000","unitPriceOriginal":"49\.79000000"/,
    );
    assert.match(
      capturedUserPrompt,
      /"resolutionProcess":"Matched the exact ISIN IE0032126645/,
    );
    assert.match(
      capturedUserPrompt,
      /"rawOutput":\{"resolution_process":"Matched the exact ISIN IE0032126645.*"resolved_instrument_isin":"IE0032126645"/,
    );
    assert.match(
      capturedUserPrompt,
      /"quantityDerivedFromHistoricalPrice":true/,
    );
    assert.match(
      capturedUserPrompt,
      /"historicalPriceUsed":\{"sourceName":"llm_historical_nav","priceDate":"2026-03-03","quoteTimestamp":"2026-03-03T00:00:00Z","price":"49\.79000000","currency":"EUR"/,
    );
    assert.match(
      capturedUserPrompt,
      /Propagated contexts from similar unresolved transactions:/,
    );
    const llmPayload = decision.llmPayload as {
      reviewContext?: {
        propagatedContexts?: Array<{
          sourceTransactionId?: string;
          kind?: string;
        }>;
        resolvedSourcePrecedent?: {
          sourceTransactionId?: string;
          finalTransaction?: {
            securityId?: string | null;
            quantity?: string | null;
            unitPriceOriginal?: string | null;
          } | null;
          llm?: {
            rawOutput?: {
              resolved_instrument_isin?: string | null;
            } | null;
          } | null;
          rebuildEvidence?: {
            quantityDerivedFromHistoricalPrice?: boolean;
          } | null;
        } | null;
      };
      applied?: {
        needsReview?: boolean;
      };
    };
    assert.equal(decision.needsReview, true);
    assert.equal(
      llmPayload.reviewContext?.resolvedSourcePrecedent?.sourceTransactionId,
      "resolved-source",
    );
    assert.equal(
      llmPayload.reviewContext?.resolvedSourcePrecedent?.finalTransaction
        ?.securityId,
      "security-vanguard-eurozone",
    );
    assert.equal(
      llmPayload.reviewContext?.resolvedSourcePrecedent?.finalTransaction
        ?.quantity,
      "2.00000000",
    );
    assert.equal(
      llmPayload.reviewContext?.resolvedSourcePrecedent?.finalTransaction
        ?.unitPriceOriginal,
      "49.79000000",
    );
    assert.equal(
      llmPayload.reviewContext?.resolvedSourcePrecedent?.llm?.rawOutput
        ?.resolved_instrument_isin,
      "IE0032126645",
    );
    assert.equal(
      llmPayload.reviewContext?.resolvedSourcePrecedent?.rebuildEvidence
        ?.quantityDerivedFromHistoricalPrice,
      true,
    );
    assert.equal(
      llmPayload.reviewContext?.propagatedContexts?.[0]?.sourceTransactionId,
      "resolved-source",
    );
    assert.equal(
      llmPayload.reviewContext?.propagatedContexts?.[0]?.kind,
      "resolved_source_precedent",
    );
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
    if (previousImportModel === undefined) {
      delete process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
    } else {
      process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = previousImportModel;
    }
    if (previousFollowupModel === undefined) {
      delete process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM;
    } else {
      process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM =
        previousFollowupModel;
    }
    globalThis.fetch = previousFetch;
  }
});

test("investment follow-up review keeps mapped trades unresolved when quantity is missing", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  const previousImportModel = process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
  const previousFollowupModel =
    process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM;
  const previousFetch = globalThis.fetch;

  process.env.OPENAI_API_KEY = "test-key";
  process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = "gpt-5.4-mini";
  process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM = "gpt-5.4";

  globalThis.fetch = async (input, init) => {
    assert.equal(input, "https://api.openai.com/v1/responses");
    const requestBody = JSON.parse(String(init?.body ?? "{}")) as {
      model?: string;
    };
    assert.equal(requestBody.model, "gpt-5.4");

    return new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "investment_trade_buy",
          category_code: "stock_buy",
          merchant_normalized: null,
          counterparty_name: null,
          economic_entity_override: null,
          security_hint: "EMERGING MARKETS STOCK EUR ACC",
          resolved_instrument_name:
            "Vanguard Emerging Markets Stock Index Fund EUR Acc",
          resolved_instrument_isin: "IE0031786696",
          resolved_instrument_ticker: null,
          resolved_instrument_exchange: null,
          current_price: 258.774,
          current_price_currency: "EUR",
          current_price_timestamp: "2026-04-02T00:00:00Z",
          current_price_source: "Financial Times",
          current_price_type: "NAV",
          resolution_process:
            "Confirmed the exact ISIN from issuer documentation and checked a dated fund-price source tied to the same ISIN.",
          confidence: 0.93,
          explanation:
            "The instrument is resolved, but no dated NAV was retrieved for the transaction date, so quantity is still missing.",
          reason:
            "This is the same fund purchase, but quantity remains unresolved without a dated NAV.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  };

  try {
    const account = createAccount({
      id: "broker-followup-quantity",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "candidate-with-stale-security",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2025-10-23",
      postedDate: "2025-10-23",
      amountOriginal: "-97.35",
      amountBaseEur: "-97.35",
      currencyOriginal: "EUR",
      descriptionRaw: "EMERGING MARKETS STOCK EUR ACC",
      descriptionClean: "EMERGING MARKETS STOCK EUR ACC",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "llm",
      classificationSource: "llm",
      classificationConfidence: "0.88",
      securityId: "security-em-stale",
      quantity: null,
      unitPriceOriginal: null,
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-em-stale",
          providerName: "manual",
          providerSymbol: "0P000060MS",
          canonicalSymbol: "0P000060MS",
          displaySymbol: "0P000060MS",
          name: "Vanguard Emerging Markets Stock Index Fund Investor EUR Accumulation",
          exchangeName: "FUNDS",
          micCode: null,
          assetType: "other",
          quoteCurrency: "EUR",
          country: "IE",
          isin: null,
          figi: null,
          active: true,
          metadataJson: {},
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
    });

    const decision = await enrichImportedTransaction(
      dataset,
      account,
      transaction,
      {
        trigger: "review_propagation",
      },
    );

    assert.equal(decision.transactionClass, "investment_trade_buy");
    assert.equal(decision.quantity, null);
    assert.equal(decision.needsReview, true);
    assert.equal(decision.reviewReason, "Quantity still needs to be derived.");
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
    if (previousImportModel === undefined) {
      delete process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
    } else {
      process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = previousImportModel;
    }
    if (previousFollowupModel === undefined) {
      delete process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM;
    } else {
      process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM =
        previousFollowupModel;
    }
    globalThis.fetch = previousFetch;
  }
});

test("investment manual review can apply user-provided NAV to derive quantity immediately", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  const previousImportModel = process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
  const previousFollowupModel =
    process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM;
  const previousFetch = globalThis.fetch;

  process.env.OPENAI_API_KEY = "test-key";
  process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = "gpt-5.4-mini";
  process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM = "gpt-5.4";

  globalThis.fetch = async (input, init) => {
    assert.equal(input, "https://api.openai.com/v1/responses");
    const requestBody = JSON.parse(String(init?.body ?? "{}")) as {
      model?: string;
    };
    assert.equal(requestBody.model, "gpt-5.4");

    return new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "investment_trade_buy",
          category_code: "stock_buy",
          merchant_normalized: "MyInvestor",
          counterparty_name: "MyInvestor",
          economic_entity_override: null,
          security_hint: "Vanguard Emerging Markets Stock Index Fund EUR Acc",
          quantity: "0.79797576",
          unit_price_original: "249.97000000",
          resolved_instrument_name:
            "Vanguard Emerging Markets Stock Index Fund EUR Acc",
          resolved_instrument_isin: "IE0031786696",
          resolved_instrument_ticker: null,
          resolved_instrument_exchange: null,
          current_price: null,
          current_price_currency: null,
          current_price_timestamp: null,
          current_price_source: null,
          current_price_type: null,
          resolution_process:
            "Used the confirmed ISIN mapping and the user-provided trade-date NAV of EUR 249.97 for this transaction to compute the share quantity.",
          confidence: 0.99,
          explanation:
            "This is a buy of the confirmed Vanguard Emerging Markets Stock Index Fund EUR Acc.",
          reason:
            "The exact ISIN is confirmed and the user provided the trade-date NAV, so quantity can be computed directly.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  };

  try {
    const account = createAccount({
      id: "broker-followup-nav",
      assetDomain: "investment",
      accountType: "brokerage_account",
      institutionName: "Broker",
      displayName: "Brokerage",
    });
    const transaction = createTransaction({
      id: "candidate-with-manual-nav",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      transactionDate: "2025-11-06",
      postedDate: "2025-11-07",
      amountOriginal: "-199.47",
      amountBaseEur: "-199.47",
      currencyOriginal: "EUR",
      descriptionRaw: "EMERGING MARKETS STOCK EUR ACC",
      descriptionClean: "EMERGING MARKETS STOCK EUR ACC",
      transactionClass: "investment_trade_buy",
      categoryCode: "stock_buy",
      classificationStatus: "llm",
      classificationSource: "llm",
      classificationConfidence: "0.88",
      securityId: "security-em-correct",
      quantity: null,
      unitPriceOriginal: null,
      needsReview: true,
      reviewReason:
        'Mapped to IE0031786696, but no reliable historical fund price was available to derive quantity for "Vanguard Emerging Markets Stock Index Fund EUR Acc".',
      manualNotes: "the NAV that day was 249.97 euros",
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      securities: [
        {
          id: "security-em-correct",
          providerName: "manual",
          providerSymbol: "IE0031786696",
          canonicalSymbol: "IE0031786696",
          displaySymbol: "IE0031786696",
          name: "Vanguard Emerging Markets Stock Index Fund EUR Acc",
          exchangeName: "FUNDS",
          micCode: null,
          assetType: "other",
          quoteCurrency: "EUR",
          country: "IE",
          isin: "IE0031786696",
          figi: null,
          active: true,
          metadataJson: { instrumentType: "Mutual Fund" },
          lastPriceRefreshAt: null,
          createdAt: "2026-01-01T00:00:00Z",
        },
      ],
    });

    const decision = await enrichImportedTransaction(
      dataset,
      account,
      transaction,
      {
        trigger: "manual_review_update",
        reviewContext: {
          userProvidedContext: "the NAV that day was 249.97 euros",
        },
      },
    );

    assert.equal(decision.transactionClass, "investment_trade_buy");
    assert.equal(decision.quantity, "0.79797576");
    assert.equal(decision.unitPriceOriginal, "249.97000000");
    assert.equal(decision.needsReview, false);
    assert.equal(decision.reviewReason, null);
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
    if (previousImportModel === undefined) {
      delete process.env.INVESTMENT_TRANSACTION_REVIEW_LLM;
    } else {
      process.env.INVESTMENT_TRANSACTION_REVIEW_LLM = previousImportModel;
    }
    if (previousFollowupModel === undefined) {
      delete process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM;
    } else {
      process.env.INVESTMENT_TRANSACTION_FOLLOWUP_REVIEW_LLM =
        previousFollowupModel;
    }
    globalThis.fetch = previousFetch;
  }
});

test("transaction enrichment request schema keeps nullable quantity fields required for strict OpenAI JSON mode", async () => {
  const previousKey = process.env.OPENAI_API_KEY;
  const previousFetch = globalThis.fetch;
  let capturedBody: Record<string, unknown> | null = null;

  process.env.OPENAI_API_KEY = "test-key";
  globalThis.fetch = async (input, init) => {
    assert.equal(input, "https://api.openai.com/v1/responses");
    capturedBody = JSON.parse(String(init?.body ?? "{}")) as Record<
      string,
      unknown
    >;

    return new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "investment_trade_buy",
          category_code: "stock_buy",
          merchant_normalized: "MyInvestor",
          counterparty_name: "MyInvestor",
          economic_entity_override: null,
          security_hint: "EMERGING MARKETS STOCK EUR ACC",
          quantity: null,
          unit_price_original: null,
          resolved_instrument_name:
            "Vanguard Emerging Markets Stock Index Fund EUR Acc",
          resolved_instrument_isin: "IE0031786696",
          resolved_instrument_ticker: null,
          resolved_instrument_exchange: null,
          current_price: null,
          current_price_currency: null,
          current_price_timestamp: null,
          current_price_source: null,
          current_price_type: null,
          resolution_process: null,
          confidence: 0.99,
          explanation: "Resolved as an investment trade.",
          reason: "The exact fund mapping is known.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  };

  try {
    const result = await analyzeBankTransaction(
      createLLMClient(),
      {
        account: {
          id: "broker-schema-check",
          assetDomain: "investment",
          institutionName: "Broker",
          displayName: "Brokerage",
          accountType: "brokerage_account",
        },
        allowedTransactionClasses: ["investment_trade_buy", "unknown"],
        allowedCategories: [{ code: "stock_buy", displayName: "Stock buy" }],
        transaction: {
          transactionDate: "2025-11-06",
          postedDate: "2025-11-07",
          amountOriginal: "-199.47",
          currencyOriginal: "EUR",
          descriptionRaw: "EMERGING MARKETS STOCK EUR ACC",
          merchantNormalized: "MyInvestor",
          counterpartyName: "MyInvestor",
          securityId: "security-emerging-markets",
          quantity: null,
          unitPriceOriginal: null,
          rawPayload: {},
        },
        deterministicHint: {
          transactionClass: "investment_trade_buy",
          categoryCode: "stock_buy",
          explanation: "Matched the deterministic investment statement parser.",
          source: "investment_parser",
        },
        portfolioState: {
          scope: "account",
          asOfDate: "2026-04-05",
          holdings: [],
          matchedHolding: null,
          priceSanityCheck: null,
        },
        similarAccountTransactions: [],
        reviewExamples: [],
        reviewContext: {
          trigger: "manual_review_update",
          userProvidedContext: "the NAV that day was 249.97 euros",
        },
        promptOverrides: null,
      },
      "gpt-5.4",
    );

    assert.equal(result.analysisStatus, "done");
    const format =
      (capturedBody?.text as { format?: { schema?: { required?: unknown } } })
        ?.format ?? null;
    const required = Array.isArray(format?.schema?.required)
      ? format.schema.required
      : [];
    assert.ok(required.includes("quantity"));
    assert.ok(required.includes("unit_price_original"));
  } finally {
    if (previousKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousKey;
    }
    globalThis.fetch = previousFetch;
  }
});

test("spending read model respects the selected period when building merchant totals", () => {
  const dataset = createDataset({
    transactions: [
      createTransaction({
        id: "apr-merchant",
        transactionDate: "2026-04-02",
        postedDate: "2026-04-02",
        amountOriginal: "-45.00",
        amountBaseEur: "-45.00",
        merchantNormalized: "COFFEE BAR",
        descriptionClean: "COFFEE BAR",
      }),
      createTransaction({
        id: "mar-merchant",
        transactionDate: "2026-03-20",
        postedDate: "2026-03-20",
        amountOriginal: "-90.00",
        amountBaseEur: "-90.00",
        merchantNormalized: "COFFEE BAR",
        descriptionClean: "COFFEE BAR",
      }),
    ],
    monthlyCashFlowRollups: [
      {
        entityId: "entity-1",
        month: "2026-03-01",
        incomeEur: "0.00",
        spendingEur: "90.00",
        operatingNetEur: "-90.00",
      },
      {
        entityId: "entity-1",
        month: "2026-04-01",
        incomeEur: "0.00",
        spendingEur: "45.00",
        operatingNetEur: "-45.00",
      },
    ],
  });

  const model = buildSpendingReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    period: resolvePeriodSelection({
      preset: "mtd",
      referenceDate: "2026-04-03",
    }),
    referenceDate: "2026-04-03",
  });

  assert.equal(model.transactions.length, 1);
  assert.equal(model.transactions[0]?.id, "apr-merchant");
  assert.deepEqual(model.merchantRows, [
    { label: "COFFEE BAR", amountEur: "45.00" },
  ]);
});

test("template config builder converts typed inputs into stored JSON rules", () => {
  const config = createTemplateConfig({
    columnMappings: [
      { target: "transaction_date", source: "Fecha" },
      { target: "description_raw", source: "Concepto" },
      { target: "amount_original_signed", source: "Importe" },
    ],
    signMode: "amount_direction_column",
    directionColumn: "Tipo",
    debitValuesText: "cargo",
    creditValuesText: "abono",
    dateDayFirst: true,
  });

  assert.deepEqual(config.columnMapJson, {
    transaction_date: "Fecha",
    description_raw: "Concepto",
    amount_original_signed: "Importe",
  });
  assert.deepEqual(config.signLogicJson, {
    mode: "amount_direction_column",
    direction_column: "Tipo",
    debit_values: ["cargo"],
    credit_values: ["abono"],
  });
  assert.deepEqual(config.normalizationRulesJson, {
    date_day_first: true,
  });
});

test("holding valuation is computed from positions, quotes, and FX instead of hardcoded values", () => {
  const investmentAccount = createAccount({
    id: "brokerage-1",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "USD",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    securities: [
      {
        id: "security-1",
        providerName: "twelve_data",
        providerSymbol: "ABC",
        canonicalSymbol: "ABC",
        displaySymbol: "ABC",
        name: "ABC Corp",
        exchangeName: "NYSE",
        exchangeMic: "XNYS",
        securityType: "stock",
        quoteCurrency: "USD",
        countryCode: "US",
        isin: null,
        cusip: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
      },
    ],
    securityPrices: [
      {
        securityId: "security-1",
        priceDate: "2026-04-03",
        quoteTimestamp: "2026-04-03T15:00:00Z",
        price: "10.00",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-04-03T15:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T15:00:00Z",
        rate: "0.500000",
        sourceName: "ecb",
        rawJson: {},
      },
    ],
    investmentPositions: [
      {
        userId: "user-1",
        entityId: "entity-1",
        accountId: "brokerage-1",
        securityId: "security-1",
        openQuantity: "4.00",
        openCostBasisEur: "15.00",
        avgCostEur: "3.75",
        realizedPnlEur: "0.00",
        dividendsEur: "0.00",
        interestEur: "0.00",
        feesEur: "0.00",
        lastTradeDate: "2026-04-01",
        lastRebuiltAt: "2026-04-03T16:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  const [holding] = buildHoldingRows(
    dataset,
    { kind: "consolidated" },
    "2026-04-03",
  );

  assert.equal(holding?.currentValueEur, "20.00");
  assert.equal(holding?.unrealizedPnlEur, "5.00");
  assert.equal(holding?.unrealizedPnlPercent, "33.33");
});

test("holding valuation uses as-of FX even when the latest quote is older than the FX series", () => {
  const investmentAccount = createAccount({
    id: "brokerage-1",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "USD",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    securities: [
      {
        id: "security-1",
        providerName: "twelve_data",
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices Inc",
        exchangeName: "NASDAQ",
        exchangeMic: "XNAS",
        securityType: "stock",
        quoteCurrency: "USD",
        countryCode: "US",
        isin: null,
        cusip: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
      },
    ],
    securityPrices: [
      {
        securityId: "security-1",
        priceDate: "2026-04-01",
        quoteTimestamp: "2026-04-01T15:00:00Z",
        price: "100.00",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-04-01T15:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-04",
        asOfTimestamp: "2026-04-04T15:00:00Z",
        rate: "0.920000",
        sourceName: "ecb",
        rawJson: {},
      },
    ],
    investmentPositions: [
      {
        userId: "user-1",
        entityId: "entity-1",
        accountId: "brokerage-1",
        securityId: "security-1",
        openQuantity: "10.00",
        openCostBasisEur: "900.00",
        avgCostEur: "90.00",
        realizedPnlEur: "0.00",
        dividendsEur: "0.00",
        interestEur: "0.00",
        feesEur: "0.00",
        lastTradeDate: "2026-04-01",
        lastRebuiltAt: "2026-04-04T16:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  const [holding] = buildHoldingRows(
    dataset,
    { kind: "consolidated" },
    "2026-04-04",
  );

  assert.equal(holding?.currentValueEur, "920.00");
  assert.equal(holding?.unrealizedPnlEur, "20.00");
  assert.equal(holding?.quoteFreshness, "delayed");
});

test("holding rows ignore placeholder seed quotes when a real market-data row exists", () => {
  const investmentAccount = createAccount({
    id: "brokerage-placeholder",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "USD",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    securities: [
      {
        id: "security-placeholder",
        providerName: "twelve_data",
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices Inc",
        exchangeName: "NASDAQ",
        exchangeMic: "XNAS",
        securityType: "stock",
        quoteCurrency: "USD",
        countryCode: "US",
        isin: null,
        cusip: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
      },
    ],
    securityPrices: [
      {
        securityId: "security-placeholder",
        priceDate: "2026-04-03",
        quoteTimestamp: "2026-04-03T08:20:00Z",
        price: "152.40",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-04-03T12:37:43Z",
      },
      {
        securityId: "security-placeholder",
        priceDate: "2026-04-02",
        quoteTimestamp: "2026-04-02T19:59:00Z",
        price: "217.50",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {
          symbol: "AMD",
          close: "217.5",
          datetime: "2026-04-02",
        },
        createdAt: "2026-04-03T20:08:02Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-04",
        asOfTimestamp: "2026-04-04T15:00:00Z",
        rate: "0.920000",
        sourceName: "ecb",
        rawJson: {},
      },
    ],
    investmentPositions: [
      {
        userId: "user-1",
        entityId: "entity-1",
        accountId: "brokerage-placeholder",
        securityId: "security-placeholder",
        openQuantity: "1.00",
        openCostBasisEur: "100.00",
        avgCostEur: "100.00",
        realizedPnlEur: "0.00",
        dividendsEur: "0.00",
        interestEur: "0.00",
        feesEur: "0.00",
        lastTradeDate: "2026-03-24",
        lastRebuiltAt: "2026-04-04T16:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  const [holding] = buildHoldingRows(
    dataset,
    { kind: "consolidated" },
    "2026-04-04",
  );

  assert.equal(holding?.currentPrice, "217.50");
  assert.equal(holding?.quoteTimestamp, "2026-04-02T19:59:00Z");
  assert.equal(holding?.quoteFreshness, "delayed");
});

test("holding freshness is stale when the latest delayed quote is older than five days", () => {
  const investmentAccount = createAccount({
    id: "brokerage-1",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "USD",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    securities: [
      {
        id: "security-1",
        providerName: "twelve_data",
        providerSymbol: "INTC",
        canonicalSymbol: "INTC",
        displaySymbol: "INTC",
        name: "Intel Corporation",
        exchangeName: "NASDAQ",
        exchangeMic: "XNAS",
        securityType: "stock",
        quoteCurrency: "USD",
        countryCode: "US",
        isin: null,
        cusip: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
      },
    ],
    securityPrices: [
      {
        securityId: "security-1",
        priceDate: "2026-03-20",
        quoteTimestamp: "2026-03-20T15:00:00Z",
        price: "50.00",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-03-20T15:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-04",
        asOfTimestamp: "2026-04-04T15:00:00Z",
        rate: "0.920000",
        sourceName: "ecb",
        rawJson: {},
      },
    ],
    investmentPositions: [
      {
        userId: "user-1",
        entityId: "entity-1",
        accountId: "brokerage-1",
        securityId: "security-1",
        openQuantity: "15.00",
        openCostBasisEur: "450.00",
        avgCostEur: "30.00",
        realizedPnlEur: "0.00",
        dividendsEur: "0.00",
        interestEur: "0.00",
        feesEur: "0.00",
        lastTradeDate: "2026-03-20",
        lastRebuiltAt: "2026-04-04T16:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  const [holding] = buildHoldingRows(
    dataset,
    { kind: "consolidated" },
    "2026-04-04",
  );

  assert.equal(holding?.currentValueEur, "690.00");
  assert.equal(holding?.quoteFreshness, "stale");
});

test("holding rows do not expose current pricing when the last quote is more than thirty days old", () => {
  const investmentAccount = createAccount({
    id: "brokerage-old-quote",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "USD",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    securities: [
      {
        id: "security-old-quote",
        providerName: "twelve_data",
        providerSymbol: "INTC",
        canonicalSymbol: "INTC",
        displaySymbol: "INTC",
        name: "Intel Corporation",
        exchangeName: "NASDAQ",
        exchangeMic: "XNGS",
        securityType: "stock",
        quoteCurrency: "USD",
        countryCode: "US",
        isin: null,
        cusip: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
      },
    ],
    securityPrices: [
      {
        securityId: "security-old-quote",
        priceDate: "2026-01-15",
        quoteTimestamp: "2026-01-15T15:00:00Z",
        price: "24.90",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-01-15T15:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-04",
        asOfTimestamp: "2026-04-04T15:00:00Z",
        rate: "0.920000",
        sourceName: "ecb",
        rawJson: {},
      },
    ],
    investmentPositions: [
      {
        userId: "user-1",
        entityId: "entity-1",
        accountId: "brokerage-old-quote",
        securityId: "security-old-quote",
        openQuantity: "15.00",
        openCostBasisEur: "450.00",
        avgCostEur: "30.00",
        realizedPnlEur: "0.00",
        dividendsEur: "0.00",
        interestEur: "0.00",
        feesEur: "0.00",
        lastTradeDate: "2026-01-15",
        lastRebuiltAt: "2026-04-04T16:00:00Z",
        provenanceJson: {},
        unrealizedComplete: true,
      },
    ],
  });

  const [holding] = buildHoldingRows(
    dataset,
    { kind: "consolidated" },
    "2026-04-04",
  );

  assert.equal(holding?.currentPrice, null);
  assert.equal(holding?.currentValueEur, null);
  assert.equal(holding?.quoteFreshness, "stale");
});

test("investment rebuild derives open positions and brokerage cash from imported investment rows", () => {
  const investmentAccount = createAccount({
    id: "brokerage-1",
    accountType: "brokerage_account",
    assetDomain: "investment",
    defaultCurrency: "EUR",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    transactions: [
      createTransaction({
        id: "buy-1",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-04-01",
        amountOriginal: "-200.00",
        amountBaseEur: "-200.00",
        descriptionRaw: "ADVANCED MICRO DEVICES @ 2",
        descriptionClean: "ADVANCED MICRO DEVICES @ 2",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        securityId: "security-1",
        quantity: "2.00000000",
        unitPriceOriginal: "100.00",
        rawPayload: {
          Import: {
            balanceOriginal: "450.00",
            balanceCurrency: "EUR",
          },
        },
      }),
      createTransaction({
        id: "sell-1",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-04-02",
        amountOriginal: "120.00",
        amountBaseEur: "120.00",
        descriptionRaw: "ADVANCED MICRO DEVICES @ 1",
        descriptionClean: "ADVANCED MICRO DEVICES @ 1",
        transactionClass: "investment_trade_sell",
        categoryCode: "uncategorized_investment",
        securityId: "security-1",
        quantity: "-1.00000000",
        unitPriceOriginal: "120.00",
        rawPayload: {
          Import: {
            balanceOriginal: "570.00",
            balanceCurrency: "EUR",
          },
        },
      }),
    ],
    securities: [
      {
        id: "security-1",
        providerName: "twelve_data",
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices Inc",
        exchangeName: "NASDAQ",
        micCode: "XNAS",
        assetType: "stock",
        quoteCurrency: "USD",
        country: "US",
        isin: null,
        figi: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
        createdAt: "2026-04-03T08:00:00Z",
      },
    ],
    securityPrices: [
      {
        securityId: "security-1",
        priceDate: "2026-04-03",
        quoteTimestamp: "2026-04-03T08:00:00Z",
        price: "150.00",
        currency: "USD",
        sourceName: "twelve_data",
        isRealtime: false,
        isDelayed: true,
        marketState: "closed",
        rawJson: {},
        createdAt: "2026-04-03T08:00:00Z",
      },
    ],
    fxRates: [
      {
        baseCurrency: "USD",
        quoteCurrency: "EUR",
        asOfDate: "2026-04-03",
        asOfTimestamp: "2026-04-03T08:00:00Z",
        rate: "0.90000000",
        sourceName: "twelve_data",
        rawJson: {},
      },
    ],
  });

  const rebuilt = rebuildInvestmentState(dataset, "2026-04-03");
  const balances = getLatestInvestmentCashBalances(dataset, "2026-04-03");

  assert.equal(rebuilt.positions.length, 1);
  assert.equal(rebuilt.positions[0]?.openQuantity, "1.00000000");
  assert.equal(rebuilt.positions[0]?.openCostBasisEur, "100.00000000");
  assert.equal(rebuilt.positions[0]?.realizedPnlEur, "20.00000000");
  assert.equal(balances[0]?.balanceBaseEur, "570.00000000");
  assert.equal(rebuilt.snapshots[0]?.totalPortfolioValueEur, "705.00000000");
});

test("investments read model keeps resolved broker transfers visible in the investments ledger", () => {
  const investmentAccount = createAccount({
    id: "brokerage-2",
    accountType: "brokerage_account",
    assetDomain: "investment",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    transactions: [
      createTransaction({
        id: "broker-transfer",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-04-02",
        postedDate: "2026-04-02",
        amountOriginal: "500.00",
        amountBaseEur: "500.00",
        descriptionRaw: "Transferencia entre cuentas",
        descriptionClean: "TRANSFERENCIA ENTRE CUENTAS",
        transactionClass: "transfer_internal",
        categoryCode: "uncategorized_investment",
        needsReview: false,
      }),
    ],
  });

  const model = buildInvestmentsReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    period: resolvePeriodSelection({
      preset: "mtd",
      referenceDate: "2026-04-03",
    }),
    referenceDate: "2026-04-03",
  });

  assert.equal(model.investmentRows.length, 1);
  assert.equal(model.investmentRows[0]?.transactionClass, "transfer_internal");
});

test("investments read model exposes unresolved investment items outside the selected period", () => {
  const investmentAccount = createAccount({
    id: "brokerage-3",
    accountType: "brokerage_account",
    assetDomain: "investment",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    transactions: [
      createTransaction({
        id: "older-review",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-03-24",
        postedDate: "2026-03-24",
        amountOriginal: "-99.58",
        amountBaseEur: "-99.58",
        descriptionRaw: "VANGUARD US 500 STOCK INDEX EU",
        descriptionClean: "VANGUARD US 500 STOCK INDEX EU",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        needsReview: true,
        reviewReason: "Mapped to VUSA, but quantity still needs to be derived.",
      }),
    ],
  });

  const model = buildInvestmentsReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    period: resolvePeriodSelection({
      preset: "mtd",
      referenceDate: "2026-04-03",
    }),
    referenceDate: "2026-04-03",
  });

  assert.equal(model.investmentRows.length, 0);
  assert.equal(model.unresolved.length, 1);
  assert.equal(
    model.unresolved[0]?.descriptionRaw,
    "VANGUARD US 500 STOCK INDEX EU",
  );
});

test("investments read model keeps processed rows available outside the selected period", () => {
  const investmentAccount = createAccount({
    id: "brokerage-processed",
    accountType: "brokerage_account",
    assetDomain: "investment",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    transactions: [
      createTransaction({
        id: "processed-buy",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-03-24",
        postedDate: "2026-03-24",
        amountOriginal: "-99.58",
        amountBaseEur: "-99.58",
        descriptionRaw: "ADVANCED MICRO DEVICES @ 2",
        descriptionClean: "ADVANCED MICRO DEVICES @ 2",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        needsReview: false,
        quantity: "2.00000000",
      }),
    ],
  });

  const model = buildInvestmentsReadModel(dataset, {
    scope: { kind: "consolidated" },
    displayCurrency: "EUR",
    period: resolvePeriodSelection({
      preset: "mtd",
      referenceDate: "2026-04-03",
    }),
    referenceDate: "2026-04-03",
  });

  assert.equal(model.investmentRows.length, 0);
  assert.equal(model.processedRows.length, 1);
  assert.equal(
    model.processedRows[0]?.descriptionRaw,
    "ADVANCED MICRO DEVICES @ 2",
  );
});

test("unresolved investment rows do not contribute to rebuilt positions or YTD investment KPIs", () => {
  const investmentAccount = createAccount({
    id: "brokerage-unresolved-kpi",
    accountType: "brokerage_account",
    assetDomain: "investment",
  });
  const dataset = createDataset({
    accounts: [investmentAccount],
    categories: [
      ...createDataset().categories,
      {
        code: "dividend",
        displayName: "Dividend",
        parentCode: null,
        scopeKind: "investment",
        directionKind: "income",
        sortOrder: 41,
        active: true,
        metadataJson: {},
      },
      {
        code: "stock_buy",
        displayName: "Stock Buy",
        parentCode: null,
        scopeKind: "investment",
        directionKind: "investment",
        sortOrder: 42,
        active: true,
        metadataJson: {},
      },
    ],
    securities: [
      {
        id: "security-amd",
        providerName: "manual",
        providerSymbol: "AMD",
        canonicalSymbol: "AMD",
        displaySymbol: "AMD",
        name: "Advanced Micro Devices",
        exchangeName: "NASDAQ",
        micCode: "XNAS",
        assetType: "stock",
        quoteCurrency: "USD",
        country: "US",
        isin: null,
        figi: null,
        active: true,
        metadataJson: {},
        lastPriceRefreshAt: null,
        createdAt: "2026-01-01T00:00:00Z",
      },
    ],
    transactions: [
      createTransaction({
        id: "unresolved-dividend",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-03-24",
        postedDate: "2026-03-24",
        amountOriginal: "18.50",
        amountBaseEur: "18.50",
        descriptionRaw: "Dividend from Vanguard",
        descriptionClean: "DIVIDEND FROM VANGUARD",
        transactionClass: "dividend",
        categoryCode: "dividend",
        needsReview: true,
        reviewReason: "Needs user confirmation.",
      }),
      createTransaction({
        id: "unresolved-buy",
        accountId: investmentAccount.id,
        accountEntityId: investmentAccount.entityId,
        economicEntityId: investmentAccount.entityId,
        transactionDate: "2026-03-25",
        postedDate: "2026-03-25",
        amountOriginal: "-100.00",
        amountBaseEur: "-100.00",
        descriptionRaw: "ADVANCED MICRO DEVICES @ 1",
        descriptionClean: "ADVANCED MICRO DEVICES @ 1",
        transactionClass: "investment_trade_buy",
        categoryCode: "stock_buy",
        needsReview: true,
        reviewReason: "Low-confidence investment classification.",
        securityId: "security-amd",
        quantity: "1.00000000",
        unitPriceOriginal: "100.00000000",
      }),
    ],
  });

  const rebuilt = rebuildInvestmentState(dataset, "2026-04-03");
  const model = buildInvestmentsReadModel(
    {
      ...dataset,
      investmentPositions: rebuilt.positions,
      dailyPortfolioSnapshots: rebuilt.snapshots,
    },
    {
      scope: { kind: "consolidated" },
      displayCurrency: "EUR",
      period: resolvePeriodSelection({
        preset: "mtd",
        referenceDate: "2026-04-03",
      }),
      referenceDate: "2026-04-03",
    },
  );

  assert.equal(rebuilt.positions.length, 0);
  assert.equal(model.dividendsYtd, "0.00");
  assert.equal(model.unresolved.length, 2);
});

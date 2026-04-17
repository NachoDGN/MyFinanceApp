import assert from "node:assert/strict";
import test from "node:test";

import {
  enrichImportedTransaction,
  getTransactionClassifierConfig,
} from "../packages/classification/src/index.ts";
import {
  assertRuleOutputsAllowedForScope,
  buildAllowedCategoriesForAccount,
  buildAllowedTransactionClassesForAccount,
  resolveAccountAssetDomain,
} from "../packages/domain/src/index.ts";

import {
  createAccount,
  createDataset,
  createTransaction,
} from "./support/create-dataset";

function buildScopedDataset() {
  const baseDataset = createDataset();
  const personalEntity = baseDataset.entities[0]!;
  const companyEntity = {
    id: "entity-company",
    userId: baseDataset.profile.id,
    slug: "company-a",
    displayName: "Company A",
    legalName: "Company A SL",
    entityKind: "company" as const,
    baseCurrency: "EUR" as const,
    active: true,
    createdAt: "2026-01-01T00:00:00Z",
  };
  const personalAccount = createAccount({
    id: "account-personal-checking",
    entityId: personalEntity.id,
    displayName: "Personal Checking",
    institutionName: "Santander",
    accountType: "checking",
    assetDomain: "cash",
  });
  const companyAccount = createAccount({
    id: "account-company-operating",
    entityId: companyEntity.id,
    displayName: "Company Operating",
    institutionName: "BBVA",
    accountType: "company_bank",
    assetDomain: "cash",
  });
  const investmentAccount = createAccount({
    id: "account-brokerage-cash",
    entityId: personalEntity.id,
    displayName: "Brokerage Ledger",
    institutionName: "MyInvestor",
    accountType: "brokerage_cash",
    assetDomain: "investment",
  });

  return {
    dataset: createDataset({
      entities: [personalEntity, companyEntity],
      accounts: [personalAccount, companyAccount, investmentAccount],
      categories: [
        {
          code: "subscriptions",
          displayName: "Subscriptions",
          parentCode: null,
          scopeKind: "personal",
          directionKind: "expense",
          sortOrder: 1,
          active: true,
          metadataJson: {},
        },
        {
          code: "business_income",
          displayName: "Business Income",
          parentCode: null,
          scopeKind: "personal",
          directionKind: "income",
          sortOrder: 2,
          active: true,
          metadataJson: {},
        },
        {
          code: "tax_credit",
          displayName: "Tax Credit",
          parentCode: null,
          scopeKind: "personal",
          directionKind: "income",
          sortOrder: 3,
          active: true,
          metadataJson: {},
        },
        {
          code: "software",
          displayName: "Software",
          parentCode: null,
          scopeKind: "company",
          directionKind: "expense",
          sortOrder: 4,
          active: true,
          metadataJson: {},
        },
        {
          code: "travel",
          displayName: "Travel",
          parentCode: null,
          scopeKind: "both",
          directionKind: "expense",
          sortOrder: 5,
          active: true,
          metadataJson: {},
        },
        {
          code: "debt",
          displayName: "Debt",
          parentCode: null,
          scopeKind: "both",
          directionKind: "neutral",
          sortOrder: 6,
          active: true,
          metadataJson: {},
        },
        {
          code: "salary",
          displayName: "Salary",
          parentCode: null,
          scopeKind: "system",
          directionKind: "income",
          sortOrder: 7,
          active: true,
          metadataJson: {},
        },
        {
          code: "other_expense",
          displayName: "Other",
          parentCode: null,
          scopeKind: "both",
          directionKind: "expense",
          sortOrder: 8,
          active: true,
          metadataJson: {},
        },
        {
          code: "other_income",
          displayName: "Other",
          parentCode: null,
          scopeKind: "both",
          directionKind: "income",
          sortOrder: 9,
          active: true,
          metadataJson: {},
        },
        {
          code: "brokerage",
          displayName: "Brokerage",
          parentCode: null,
          scopeKind: "investment",
          directionKind: "neutral",
          sortOrder: 10,
          active: true,
          metadataJson: {},
        },
        {
          code: "uncategorized_expense",
          displayName: "Uncategorized Expense",
          parentCode: null,
          scopeKind: "system",
          directionKind: "expense",
          sortOrder: 11,
          active: true,
          metadataJson: {},
        },
        {
          code: "dividend_income",
          displayName: "Dividend Income",
          parentCode: null,
          scopeKind: "system",
          directionKind: "income",
          sortOrder: 12,
          active: true,
          metadataJson: {},
        },
        {
          code: "interest_income",
          displayName: "Interest Income",
          parentCode: null,
          scopeKind: "system",
          directionKind: "income",
          sortOrder: 13,
          active: true,
          metadataJson: {},
        },
        {
          code: "transfer_between_accounts",
          displayName: "Transfer Between Accounts",
          parentCode: null,
          scopeKind: "system",
          directionKind: "neutral",
          sortOrder: 14,
          active: true,
          metadataJson: {},
        },
        {
          code: "uncategorized_investment",
          displayName: "Uncategorized Investment",
          parentCode: null,
          scopeKind: "investment",
          directionKind: "investment",
          sortOrder: 15,
          active: true,
          metadataJson: {},
        },
      ],
      transactions: [],
    }),
    personalEntity,
    companyEntity,
    personalAccount,
    companyAccount,
    investmentAccount,
  };
}

test("brokerage cash accounts resolve to the investment asset domain", () => {
  assert.equal(resolveAccountAssetDomain("brokerage_cash"), "investment");
  assert.equal(resolveAccountAssetDomain("brokerage_account"), "investment");
  assert.equal(resolveAccountAssetDomain("checking"), "cash");
});

test("cash transaction classifier defaults to gemini-3-flash-preview", () => {
  const previousLlmModel = process.env.LLM_TRANSACTION_MODEL;
  const previousGeminiModel = process.env.GEMINI_TRANSACTION_MODEL;
  const previousOpenAiModel = process.env.OPENAI_TRANSACTION_MODEL;

  delete process.env.LLM_TRANSACTION_MODEL;
  delete process.env.GEMINI_TRANSACTION_MODEL;
  delete process.env.OPENAI_TRANSACTION_MODEL;

  try {
    assert.equal(
      getTransactionClassifierConfig().model,
      "gemini-3-flash-preview",
    );
  } finally {
    if (previousLlmModel === undefined) {
      delete process.env.LLM_TRANSACTION_MODEL;
    } else {
      process.env.LLM_TRANSACTION_MODEL = previousLlmModel;
    }
    if (previousGeminiModel === undefined) {
      delete process.env.GEMINI_TRANSACTION_MODEL;
    } else {
      process.env.GEMINI_TRANSACTION_MODEL = previousGeminiModel;
    }
    if (previousOpenAiModel === undefined) {
      delete process.env.OPENAI_TRANSACTION_MODEL;
    } else {
      process.env.OPENAI_TRANSACTION_MODEL = previousOpenAiModel;
    }
  }
});

test("cash enrichment falls back to OpenAI when Gemini quota is exhausted", async () => {
  const previousGeminiKey = process.env.GEMINI_API_KEY;
  const previousOpenAiKey = process.env.OPENAI_API_KEY;
  const previousLlmModel = process.env.LLM_TRANSACTION_MODEL;
  const previousGeminiModel = process.env.GEMINI_TRANSACTION_MODEL;
  const previousOpenAiModel = process.env.OPENAI_TRANSACTION_MODEL;
  const previousFetch = globalThis.fetch;
  const requestedUrls: string[] = [];
  const requestedModels: string[] = [];

  process.env.GEMINI_API_KEY = "test-gemini-key";
  process.env.OPENAI_API_KEY = "test-openai-key";
  delete process.env.LLM_TRANSACTION_MODEL;
  delete process.env.GEMINI_TRANSACTION_MODEL;
  delete process.env.OPENAI_TRANSACTION_MODEL;

  const { dataset, personalAccount, personalEntity } = buildScopedDataset();
  const transaction = createTransaction({
    id: "cash-fallback-transaction",
    accountId: personalAccount.id,
    accountEntityId: personalEntity.id,
    economicEntityId: personalEntity.id,
    transactionDate: "2026-04-12",
    postedDate: "2026-04-12",
    amountOriginal: "-18.40",
    amountBaseEur: "-18.40",
    currencyOriginal: "EUR",
    descriptionRaw: "UBER BV trip madrid",
    descriptionClean: "UBER BV TRIP MADRID",
    transactionClass: "unknown",
    categoryCode: null,
    classificationStatus: "unknown",
    classificationSource: "system_fallback",
    classificationConfidence: "0.00",
    needsReview: true,
    reviewReason: "Needs LLM enrichment.",
  });
  const datasetWithTransaction = {
    ...dataset,
    transactions: [transaction],
  };

  globalThis.fetch = async (input, init) => {
    const url = String(input);
    requestedUrls.push(url);

    if (url.includes("generativelanguage.googleapis.com")) {
      return new Response(
        JSON.stringify({
          error: {
            code: 429,
            message:
              "Your project has exceeded its monthly spending cap.",
            status: "RESOURCE_EXHAUSTED",
          },
        }),
        {
          status: 429,
          headers: { "Content-Type": "application/json" },
        },
      );
    }

    assert.equal(url, "https://api.openai.com/v1/responses");
    const requestBody = JSON.parse(String(init?.body ?? "{}")) as {
      model?: string;
    };
    requestedModels.push(
      typeof requestBody.model === "string" ? requestBody.model : "",
    );

    return new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "expense",
          category_code: "travel",
          merchant_normalized: "UBER",
          counterparty_name: "Uber BV",
          economic_entity_override: null,
          security_hint: null,
          quantity: null,
          unit_price_original: null,
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
          confidence: 0.93,
          explanation: "Matched to a ride-hailing travel expense.",
          reason: "Uber trip expense.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  };

  try {
    const decision = await enrichImportedTransaction(
      datasetWithTransaction,
      personalAccount,
      transaction,
      {
        trigger: "import_classification",
      },
    );

    assert.ok(
      requestedUrls.some((url) =>
        /generativelanguage\.googleapis\.com/.test(url),
      ),
    );
    assert.equal(requestedModels.length, 1);
    assert.equal(requestedModels[0], "gpt-5.4-mini");
    assert.equal(decision.transactionClass, "expense");
    assert.equal(decision.categoryCode, "travel");
    assert.equal(decision.classificationSource, "llm");
    assert.equal(decision.needsReview, false);
    assert.equal(
      (decision.llmPayload as { model?: string }).model,
      "gpt-5.4-mini",
    );
  } finally {
    globalThis.fetch = previousFetch;
    if (previousGeminiKey === undefined) {
      delete process.env.GEMINI_API_KEY;
    } else {
      process.env.GEMINI_API_KEY = previousGeminiKey;
    }
    if (previousOpenAiKey === undefined) {
      delete process.env.OPENAI_API_KEY;
    } else {
      process.env.OPENAI_API_KEY = previousOpenAiKey;
    }
    if (previousLlmModel === undefined) {
      delete process.env.LLM_TRANSACTION_MODEL;
    } else {
      process.env.LLM_TRANSACTION_MODEL = previousLlmModel;
    }
    if (previousGeminiModel === undefined) {
      delete process.env.GEMINI_TRANSACTION_MODEL;
    } else {
      process.env.GEMINI_TRANSACTION_MODEL = previousGeminiModel;
    }
    if (previousOpenAiModel === undefined) {
      delete process.env.OPENAI_TRANSACTION_MODEL;
    } else {
      process.env.OPENAI_TRANSACTION_MODEL = previousOpenAiModel;
    }
  }
});

test("personal cash accounts only expose personal and system categories", () => {
  const { dataset, personalAccount } = buildScopedDataset();

  const allowedCategoryCodes = new Set(
    buildAllowedCategoriesForAccount(dataset, personalAccount).map(
      (category) => category.code,
    ),
  );
  const allowedTransactionClasses = new Set(
    buildAllowedTransactionClassesForAccount(personalAccount),
  );

  assert.equal(allowedCategoryCodes.has("subscriptions"), true);
  assert.equal(allowedCategoryCodes.has("business_income"), true);
  assert.equal(allowedCategoryCodes.has("tax_credit"), true);
  assert.equal(allowedCategoryCodes.has("salary"), true);
  assert.equal(allowedCategoryCodes.has("travel"), true);
  assert.equal(allowedCategoryCodes.has("debt"), true);
  assert.equal(allowedCategoryCodes.has("other_expense"), true);
  assert.equal(allowedCategoryCodes.has("other_income"), true);
  assert.equal(allowedCategoryCodes.has("software"), false);
  assert.equal(allowedTransactionClasses.has("investment_trade_buy"), false);
});

test("company cash accounts expose both-scope travel and debt categories", () => {
  const { dataset, companyAccount } = buildScopedDataset();

  const allowedCategoryCodes = new Set(
    buildAllowedCategoriesForAccount(dataset, companyAccount).map(
      (category) => category.code,
    ),
  );

  assert.equal(allowedCategoryCodes.has("travel"), true);
  assert.equal(allowedCategoryCodes.has("debt"), true);
  assert.equal(allowedCategoryCodes.has("other_expense"), true);
  assert.equal(allowedCategoryCodes.has("other_income"), true);
  assert.equal(allowedCategoryCodes.has("tax_credit"), false);
  assert.equal(allowedCategoryCodes.has("subscriptions"), false);
});

test("brokerage-ledger accounts expose investment trades plus the dedicated brokerage bucket", () => {
  const { dataset, investmentAccount } = buildScopedDataset();

  const allowedCategoryCodes = new Set(
    buildAllowedCategoriesForAccount(dataset, investmentAccount).map(
      (category) => category.code,
    ),
  );
  const allowedTransactionClasses = new Set(
    buildAllowedTransactionClassesForAccount(investmentAccount),
  );

  assert.equal(allowedCategoryCodes.has("uncategorized_investment"), true);
  assert.equal(allowedCategoryCodes.has("brokerage"), true);
  assert.equal(allowedCategoryCodes.has("dividend_income"), true);
  assert.equal(allowedCategoryCodes.has("interest_income"), true);
  assert.equal(allowedCategoryCodes.has("transfer_between_accounts"), true);
  assert.equal(allowedCategoryCodes.has("other_expense"), false);
  assert.equal(allowedCategoryCodes.has("other_income"), false);
  assert.equal(allowedCategoryCodes.has("subscriptions"), false);
  assert.equal(allowedCategoryCodes.has("software"), false);
  assert.equal(allowedTransactionClasses.has("investment_trade_buy"), true);
  assert.equal(allowedTransactionClasses.has("investment_trade_sell"), true);
  assert.equal(allowedTransactionClasses.has("dividend"), true);
  assert.equal(allowedTransactionClasses.has("transfer_internal"), true);
});

test("personal-only rule outputs must be scoped away from company cash accounts", () => {
  const { dataset, personalAccount } = buildScopedDataset();

  assert.doesNotThrow(() =>
    assertRuleOutputsAllowedForScope(
      dataset,
      { account_id: personalAccount.id },
      {
        transaction_class: "expense",
        category_code: "subscriptions",
        merchant_normalized: "NOTION",
      },
    ),
  );

  assert.throws(
    () =>
      assertRuleOutputsAllowedForScope(
        dataset,
        { global: true },
        {
          transaction_class: "expense",
          category_code: "subscriptions",
        },
      ),
    /Rule category "subscriptions" is not allowed/,
  );
});

test("cash enrichment keeps personal-account entity attribution locked and rejects company categories", async () => {
  const previousApiKey = process.env.GEMINI_API_KEY;
  const previousFetch = globalThis.fetch;
  const previousLlmModel = process.env.LLM_TRANSACTION_MODEL;
  let capturedUrl = "";

  process.env.GEMINI_API_KEY = "test-key";
  delete process.env.LLM_TRANSACTION_MODEL;

  globalThis.fetch = async (input, init) => {
    capturedUrl = String(input);
    const requestBody = JSON.parse(String(init?.body ?? "{}")) as {
      generationConfig?: { responseMimeType?: string };
    };
    assert.equal(
      requestBody.generationConfig?.responseMimeType,
      "application/json",
    );

    return new Response(
      JSON.stringify({
        candidates: [
          {
            content: {
              parts: [
                {
                  text: JSON.stringify({
                    transaction_class: "expense",
                    category_code: "software",
                    merchant_normalized: "NOTION",
                    counterparty_name: "Company A",
                    economic_entity_override: "entity-company",
                    security_hint: null,
                    quantity: null,
                    unit_price_original: null,
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
                    confidence: 0.96,
                    explanation: "This looks like a recurring Notion charge.",
                    reason:
                      "The merchant is Notion, but the company override is unsupported on a personal cash account.",
                  }),
                },
              ],
            },
          },
        ],
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  };

  try {
    const {
      dataset,
      personalAccount,
      personalEntity,
    } = buildScopedDataset();
    const transaction = createTransaction({
      id: "personal-notion-row",
      accountId: personalAccount.id,
      accountEntityId: personalEntity.id,
      economicEntityId: personalEntity.id,
      descriptionRaw: "Notion monthly invoice",
      descriptionClean: "NOTION MONTHLY INVOICE",
      transactionClass: "unknown",
      categoryCode: "uncategorized_expense",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
    });
    const decision = await enrichImportedTransaction(
      {
        ...dataset,
        transactions: [transaction],
      },
      personalAccount,
      transaction,
    );

    assert.equal(
      capturedUrl,
      "https://generativelanguage.googleapis.com/v1beta/models/gemini-3-flash-preview:generateContent",
    );
    assert.equal(decision.transactionClass, "expense");
    assert.equal(decision.categoryCode, "uncategorized_expense");
    assert.equal(decision.economicEntityId, personalEntity.id);
    assert.equal(decision.counterpartyName, "Company A");
    assert.equal(decision.needsReview, true);
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.GEMINI_API_KEY;
    } else {
      process.env.GEMINI_API_KEY = previousApiKey;
    }
    if (previousLlmModel === undefined) {
      delete process.env.LLM_TRANSACTION_MODEL;
    } else {
      process.env.LLM_TRANSACTION_MODEL = previousLlmModel;
    }
  }
});

test("loan cash flows default to debt when the classifier leaves them uncategorized", async () => {
  const previousApiKey = process.env.GEMINI_API_KEY;
  const previousFetch = globalThis.fetch;
  const previousLlmModel = process.env.LLM_TRANSACTION_MODEL;

  process.env.GEMINI_API_KEY = "test-key";
  delete process.env.LLM_TRANSACTION_MODEL;

  globalThis.fetch = async (_input, init) => {
    const requestBody = JSON.parse(String(init?.body ?? "{}")) as {
      generationConfig?: { responseMimeType?: string };
    };
    assert.equal(
      requestBody.generationConfig?.responseMimeType,
      "application/json",
    );

    return new Response(
      JSON.stringify({
        candidates: [
          {
            content: {
              parts: [
                {
                  text: JSON.stringify({
                    transaction_class: "loan_principal_payment",
                    category_code: "uncategorized_expense",
                    merchant_normalized: "Santander",
                    counterparty_name: null,
                    economic_entity_override: null,
                    security_hint: null,
                    quantity: null,
                    unit_price_original: null,
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
                    confidence: 0.92,
                    explanation: "Recurring loan principal settlement.",
                    reason: "Recurring loan principal settlement.",
                  }),
                },
              ],
            },
          },
        ],
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );
  };

  try {
    const { dataset, personalAccount, personalEntity } = buildScopedDataset();
    const transaction = createTransaction({
      id: "loan-payment-row",
      accountId: personalAccount.id,
      accountEntityId: personalEntity.id,
      economicEntityId: personalEntity.id,
      descriptionRaw: "Liquidacion Periodica Prestamo 0049 4748 103 0633537",
      descriptionClean: "LIQUIDACION PERIODICA PRESTAMO 0049 4748 103 0633537",
      transactionClass: "unknown",
      categoryCode: "uncategorized_expense",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
    });

    const decision = await enrichImportedTransaction(
      {
        ...dataset,
        transactions: [transaction],
      },
      personalAccount,
      transaction,
    );

    assert.equal(decision.transactionClass, "loan_principal_payment");
    assert.equal(decision.categoryCode, "debt");
    assert.equal(decision.needsReview, false);
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.GEMINI_API_KEY;
    } else {
      process.env.GEMINI_API_KEY = previousApiKey;
    }
    if (previousLlmModel === undefined) {
      delete process.env.LLM_TRANSACTION_MODEL;
    } else {
      process.env.LLM_TRANSACTION_MODEL = previousLlmModel;
    }
  }
});

test("revolut travel merchant codes resolve to travel before llm fallback", async () => {
  const previousApiKey = process.env.GEMINI_API_KEY;
  const previousFetch = globalThis.fetch;
  const previousLlmModel = process.env.LLM_TRANSACTION_MODEL;

  process.env.GEMINI_API_KEY = "test-key";
  delete process.env.LLM_TRANSACTION_MODEL;

  globalThis.fetch = async (_input, _init) =>
    new Response(
      JSON.stringify({
        candidates: [
          {
            content: {
              parts: [
                {
                  text: JSON.stringify({
                    transaction_class: "expense",
                    category_code: "uncategorized_expense",
                    merchant_normalized: "Booking.com",
                    counterparty_name: "Booking.com",
                    economic_entity_override: null,
                    security_hint: null,
                    quantity: null,
                    unit_price_original: null,
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
                    confidence: 0.97,
                    explanation: "Booking merchant detected, but no category chosen.",
                    reason: "Booking merchant detected, but no category chosen.",
                  }),
                },
              ],
            },
          },
        ],
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );

  try {
    const { dataset, companyAccount, companyEntity } = buildScopedDataset();
    const transaction = createTransaction({
      id: "company-booking-row",
      accountId: companyAccount.id,
      accountEntityId: companyEntity.id,
      economicEntityId: companyEntity.id,
      providerName: "revolut_business",
      providerRecordId: "revolut-booking-1:leg-1",
      descriptionRaw: "Booking.com",
      descriptionClean: "BOOKING.COM",
      merchantNormalized: "Booking.com",
      transactionClass: "unknown",
      categoryCode: "uncategorized_expense",
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
      rawPayload: {
        provider: "revolut_business",
        providerContext: {
          provider: "revolut_business",
          transaction: {
            id: "revolut-booking-1",
            type: "card_payment",
            state: "completed",
          },
          merchant: {
            id: "merchant-booking",
            name: "Booking.com",
            country: "NLD",
            categoryCode: "4722",
          },
          leg: {
            legId: "leg-1",
            amount: -652.78,
            currency: "EUR",
            accountId: "revolut-company-account",
          },
          expense: null,
        },
      },
    });

    const decision = await enrichImportedTransaction(
      {
        ...dataset,
        transactions: [transaction],
      },
      companyAccount,
      transaction,
    );

    assert.equal(decision.transactionClass, "expense");
    assert.equal(decision.categoryCode, "travel");
    assert.equal(decision.classificationSource, "system_fallback");
    assert.equal(decision.needsReview, false);
  } finally {
    globalThis.fetch = previousFetch;
    if (previousApiKey === undefined) {
      delete process.env.GEMINI_API_KEY;
    } else {
      process.env.GEMINI_API_KEY = previousApiKey;
    }
    if (previousLlmModel === undefined) {
      delete process.env.LLM_TRANSACTION_MODEL;
    } else {
      process.env.LLM_TRANSACTION_MODEL = previousLlmModel;
    }
  }
});

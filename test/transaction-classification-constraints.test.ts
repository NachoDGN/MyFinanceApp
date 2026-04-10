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

  return {
    dataset: createDataset({
      entities: [personalEntity, companyEntity],
      accounts: [personalAccount, companyAccount],
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
          code: "software",
          displayName: "Software",
          parentCode: null,
          scopeKind: "company",
          directionKind: "expense",
          sortOrder: 3,
          active: true,
          metadataJson: {},
        },
        {
          code: "salary",
          displayName: "Salary",
          parentCode: null,
          scopeKind: "system",
          directionKind: "income",
          sortOrder: 4,
          active: true,
          metadataJson: {},
        },
        {
          code: "uncategorized_expense",
          displayName: "Uncategorized Expense",
          parentCode: null,
          scopeKind: "system",
          directionKind: "expense",
          sortOrder: 5,
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
  };
}

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
  assert.equal(allowedCategoryCodes.has("salary"), true);
  assert.equal(allowedCategoryCodes.has("software"), false);
  assert.equal(allowedTransactionClasses.has("investment_trade_buy"), false);
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

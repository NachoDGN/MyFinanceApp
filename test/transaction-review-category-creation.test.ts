import assert from "node:assert/strict";
import test from "node:test";

import { enrichImportedTransaction } from "../packages/classification/src/index.ts";

import {
  createAccount,
  createDataset,
  createTransaction,
} from "./support/create-dataset";

test("manual transaction review can request category_creation for a missing category", async () => {
  const previousOpenAiKey = process.env.OPENAI_API_KEY;
  const previousLlmModel = process.env.LLM_TRANSACTION_MODEL;
  const previousFetch = globalThis.fetch;

  process.env.OPENAI_API_KEY = "test-key";
  process.env.LLM_TRANSACTION_MODEL = "gpt-5.4-mini";
  globalThis.fetch = async () =>
    new Response(
      JSON.stringify({
        output_text: JSON.stringify({
          transaction_class: "expense",
          category_code: "pets",
          merchant_normalized: "Marinocanis",
          counterparty_name: "Marinocanis S.l.",
          economic_entity_override: null,
          security_hint: null,
          quantity: null,
          unit_price_original: null,
          category_creation: {
            code: "pets",
            display_name: "Pets",
            parent_code: null,
            scope_kind: "personal",
            direction_kind: "expense",
            reason: "User identified the transaction as doggie daycare.",
          },
          confidence: 0.96,
          explanation: "User review identifies a pet-care expense.",
          reason: "Doggie daycare belongs in a missing Pets category.",
        }),
      }),
      {
        status: 200,
        headers: { "Content-Type": "application/json" },
      },
    );

  try {
    const account = createAccount();
    const transaction = createTransaction({
      id: "marinocanis-review",
      accountId: account.id,
      accountEntityId: account.entityId,
      economicEntityId: account.entityId,
      amountOriginal: "-545.00",
      amountBaseEur: "-545.00",
      descriptionRaw: "Marinocanis S.l.",
      descriptionClean: "MARINOCANIS S.L.",
      transactionClass: "unknown",
      categoryCode: null,
      classificationStatus: "unknown",
      classificationSource: "system_fallback",
      classificationConfidence: "0.00",
      needsReview: true,
      reviewReason: "Needs LLM enrichment.",
    });
    const dataset = createDataset({
      accounts: [account],
      transactions: [transaction],
      categories: createDataset().categories.filter(
        (category) => category.code !== "pets",
      ),
    });

    const decision = await enrichImportedTransaction(
      dataset,
      account,
      transaction,
      {
        trigger: "manual_review_update",
        reviewContext: {
          previousReviewReason: transaction.reviewReason,
          previousUserContext: null,
          previousLlmPayload: null,
          userProvidedContext:
            "This is a pet expense for doggie daycare. Create pets if missing.",
        },
      },
    );

    assert.equal(decision.transactionClass, "expense");
    assert.equal(decision.categoryCode, "pets");
    assert.equal(decision.needsReview, false);
    assert.deepEqual(decision.categoryCreation, {
      toolName: "category_creation",
      code: "pets",
      displayName: "Pets",
      parentCode: null,
      scopeKind: "personal",
      directionKind: "expense",
      reason: "User identified the transaction as doggie daycare.",
    });
    assert.equal(
      (
        decision.llmPayload.applied as {
          categoryCreation?: { code?: string };
        }
      ).categoryCreation?.code,
      "pets",
    );
  } finally {
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
    globalThis.fetch = previousFetch;
  }
});

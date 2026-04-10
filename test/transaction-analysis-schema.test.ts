import test from "node:test";
import assert from "node:assert/strict";

import { transactionAnalysisResponseSchema } from "../packages/llm/src/tasks/analyze-bank-transaction.ts";

test("transaction analysis schema accepts camelCase Gemini output and fills safe defaults", () => {
  const parsed = transactionAnalysisResponseSchema.parse({
    transactionClass: "expense",
    categoryCode: "home_maintenance",
    counterpartyName: "Cdad Prop Jardin De La Reina",
    economicEntityOverride: null,
    explanation:
      "Payment to a homeowners association for property maintenance.",
    resolutionProcess:
      "The description identifies a homeowners association fee.",
  });

  assert.equal(parsed.transaction_class, "expense");
  assert.equal(parsed.category_code, "home_maintenance");
  assert.equal(parsed.counterparty_name, "Cdad Prop Jardin De La Reina");
  assert.equal(parsed.merchant_normalized, null);
  assert.equal(parsed.security_hint, null);
  assert.equal(parsed.reason, parsed.explanation);
  assert.equal(parsed.confidence, 0.85);
  assert.equal(
    parsed.resolution_process,
    "The description identifies a homeowners association fee.",
  );
});

test("transaction analysis schema normalizes string confidence and preserves snake_case payloads", () => {
  const parsed = transactionAnalysisResponseSchema.parse({
    transaction_class: "income",
    category_code: "business_income",
    merchant_normalized: null,
    counterparty_name: "Dgnn Van Sociedad Limitada",
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
    confidence: "0.91",
    explanation: "Company payment for work performed.",
    reason: "Invoice settlement.",
  });

  assert.equal(parsed.transaction_class, "income");
  assert.equal(parsed.category_code, "business_income");
  assert.equal(parsed.confidence, 0.91);
  assert.equal(parsed.reason, "Invoice settlement.");
});

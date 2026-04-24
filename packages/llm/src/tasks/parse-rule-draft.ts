import { z } from "zod";

import { renderRuleDraftParserPromptFromInput } from "../prompts";
import { runStructuredPromptTask } from "../structured-task";
import type { LLMTaskClient } from "../types";

const ruleDraftScopeSchema = z
  .object({
    global: z.boolean().optional(),
    entity_id: z.string().nullable().optional(),
    account_id: z.string().nullable().optional(),
  })
  .strict();

const ruleDraftConditionsSchema = z
  .object({
    normalized_description_regex: z.string().nullable().optional(),
    merchant_equals: z.string().nullable().optional(),
    counterparty_equals: z.string().nullable().optional(),
    amount_sign: z.string().nullable().optional(),
    amount_min: z.number().nullable().optional(),
    amount_max: z.number().nullable().optional(),
    account_id: z.string().nullable().optional(),
    account_type: z.string().nullable().optional(),
    current_category_code: z.string().nullable().optional(),
  })
  .strict();

const ruleDraftOutputsSchema = z
  .object({
    transaction_class: z.string().nullable().optional(),
    category_code: z.string().nullable().optional(),
    merchant_normalized: z.string().nullable().optional(),
    counterparty_name: z.string().nullable().optional(),
    economic_entity_id_override: z.string().nullable().optional(),
    review_suppression: z.boolean().nullable().optional(),
  })
  .strict();

export const ruleDraftResponseSchema = z.object({
  title: z.string().min(1).max(120),
  summary: z.string().min(1).max(240),
  priority: z.number().int().min(1).max(999),
  scope_json: ruleDraftScopeSchema,
  conditions_json: ruleDraftConditionsSchema,
  outputs_json: ruleDraftOutputsSchema,
  confidence: z.number().min(0).max(1),
  explanation: z.array(z.string()).max(6).default([]),
});

const ruleDraftJsonSchema = {
  type: "object",
  additionalProperties: false,
  required: [
    "title",
    "summary",
    "priority",
    "scope_json",
    "conditions_json",
    "outputs_json",
    "confidence",
    "explanation",
  ],
  properties: {
    title: { type: "string" },
    summary: { type: "string" },
    priority: { type: "integer", minimum: 1, maximum: 999 },
    scope_json: {
      type: "object",
      additionalProperties: false,
      properties: {
        global: { type: "boolean" },
        entity_id: { type: ["string", "null"] },
        account_id: { type: ["string", "null"] },
      },
    },
    conditions_json: {
      type: "object",
      additionalProperties: false,
      properties: {
        normalized_description_regex: { type: ["string", "null"] },
        merchant_equals: { type: ["string", "null"] },
        counterparty_equals: { type: ["string", "null"] },
        amount_sign: { type: ["string", "null"] },
        amount_min: { type: ["number", "null"] },
        amount_max: { type: ["number", "null"] },
        account_id: { type: ["string", "null"] },
        account_type: { type: ["string", "null"] },
        current_category_code: { type: ["string", "null"] },
      },
    },
    outputs_json: {
      type: "object",
      additionalProperties: false,
      properties: {
        transaction_class: { type: ["string", "null"] },
        category_code: { type: ["string", "null"] },
        merchant_normalized: { type: ["string", "null"] },
        counterparty_name: { type: ["string", "null"] },
        economic_entity_id_override: { type: ["string", "null"] },
        review_suppression: { type: ["boolean", "null"] },
      },
    },
    confidence: { type: "number", minimum: 0, maximum: 1 },
    explanation: {
      type: "array",
      items: { type: "string" },
      maxItems: 6,
    },
  },
} satisfies Record<string, unknown>;

export interface ParseRuleDraftInput {
  requestText: string;
  supportedConditionKeys: readonly string[];
  supportedOutputKeys: readonly string[];
  allowedTransactionClasses: readonly string[];
  allowedCategoryCodes: readonly string[];
  entities: ReadonlyArray<{ displayName: string; slug: string; id: string }>;
  accounts: ReadonlyArray<{
    displayName: string;
    id: string;
    accountType: string;
    institutionName: string;
  }>;
  promptOverrides?: Record<string, unknown> | null;
}

export type ParsedRuleDraft = z.infer<typeof ruleDraftResponseSchema>;

export async function parseRuleDraftWithLLM(
  client: LLMTaskClient,
  input: ParseRuleDraftInput,
  modelName: string,
) {
  const prompt = renderRuleDraftParserPromptFromInput({
    supportedConditionKeys: input.supportedConditionKeys.join(", "),
    supportedOutputKeys: input.supportedOutputKeys.join(", "),
    allowedTransactionClasses: input.allowedTransactionClasses.join(", "),
    allowedCategoryCodes: input.allowedCategoryCodes.join(", "),
    entities: input.entities
      .map(
        (entity) =>
          `${entity.displayName} [slug=${entity.slug}, id=${entity.id}]`,
      )
      .join("; "),
    accounts: input.accounts
      .map(
        (account) =>
          `${account.displayName} [id=${account.id}, type=${account.accountType}, institution=${account.institutionName}]`,
      )
      .join("; "),
    requestText: input.requestText,
    promptOverrides: input.promptOverrides ?? null,
  });
  return runStructuredPromptTask(client, prompt, {
    modelName,
    responseSchema: ruleDraftResponseSchema,
    responseJsonSchema: ruleDraftJsonSchema,
    schemaName: "rule_draft_parse",
    temperature: 0,
  });
}

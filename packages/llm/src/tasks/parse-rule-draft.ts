import { z } from "zod";

import type { LLMTaskClient } from "../types";

export const ruleDraftResponseSchema = z.object({
  title: z.string().min(1).max(120),
  summary: z.string().min(1).max(240),
  priority: z.number().int().min(1).max(999),
  scope_json: z.record(z.string(), z.unknown()),
  conditions_json: z.record(z.string(), z.unknown()),
  outputs_json: z.record(z.string(), z.unknown()),
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
    scope_json: { type: "object", additionalProperties: true },
    conditions_json: { type: "object", additionalProperties: true },
    outputs_json: { type: "object", additionalProperties: true },
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
}

export type ParsedRuleDraft = z.infer<typeof ruleDraftResponseSchema>;

function buildSystemPrompt() {
  return [
    "Convert the user's natural-language rule request into deterministic transaction rule logic.",
    "Return one strict JSON object only.",
    "Use only the supported condition keys and output keys provided.",
    "Do not invent taxonomy codes, entity ids, account ids, or transaction classes.",
    "If the request is ambiguous, make the narrowest safe rule and lower confidence.",
  ].join(" ");
}

function buildUserPrompt(input: ParseRuleDraftInput) {
  return [
    `Supported condition keys: ${input.supportedConditionKeys.join(", ")}`,
    `Supported output keys: ${input.supportedOutputKeys.join(", ")}`,
    `Allowed transaction classes: ${input.allowedTransactionClasses.join(", ")}`,
    `Allowed category codes: ${input.allowedCategoryCodes.join(", ")}`,
    `Entities: ${input.entities
      .map((entity) => `${entity.displayName} [slug=${entity.slug}, id=${entity.id}]`)
      .join("; ")}`,
    `Accounts: ${input.accounts
      .map(
        (account) =>
          `${account.displayName} [id=${account.id}, type=${account.accountType}, institution=${account.institutionName}]`,
      )
      .join("; ")}`,
    `User request: ${input.requestText}`,
  ].join("\n");
}

export async function parseRuleDraftWithLLM(
  client: LLMTaskClient,
  input: ParseRuleDraftInput,
  modelName: string,
) {
  return client.generateJson({
    systemPrompt: buildSystemPrompt(),
    userPrompt: buildUserPrompt(input),
    modelName,
    responseSchema: ruleDraftResponseSchema,
    responseJsonSchema: ruleDraftJsonSchema,
    schemaName: "rule_draft_parse",
    temperature: 0,
  });
}

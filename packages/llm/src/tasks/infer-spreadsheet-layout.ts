import { z } from "zod";

import type { LLMTaskClient } from "../types";

export const spreadsheetColumnMapSchema = z.object({
  transaction_date: z.string().min(1),
  posted_date: z.string().min(1).nullable(),
  description_raw: z.string().min(1).nullable(),
  amount_original_signed: z.string().min(1).nullable(),
  currency_original: z.string().min(1).nullable(),
  balance_original: z.string().min(1).nullable(),
  external_reference: z.string().min(1).nullable(),
  transaction_type_raw: z.string().min(1).nullable(),
  security_symbol: z.string().min(1).nullable(),
  security_name: z.string().min(1).nullable(),
  quantity: z.string().min(1).nullable(),
  unit_price_original: z.string().min(1).nullable(),
  fees_original: z.string().min(1).nullable(),
  fx_rate: z.string().min(1).nullable(),
});

export const spreadsheetSignLogicSchema = z.object({
  mode: z.enum([
    "signed_amount",
    "amount_direction_column",
    "debit_credit_columns",
  ]),
  invert_sign: z.boolean().nullable(),
  direction_column: z.string().min(1).nullable(),
  debit_column: z.string().min(1).nullable(),
  credit_column: z.string().min(1).nullable(),
  debit_values: z.array(z.string().min(1)).max(8).nullable(),
  credit_values: z.array(z.string().min(1)).max(8).nullable(),
});

export const spreadsheetLayoutResponseSchema = z.object({
  column_map: spreadsheetColumnMapSchema,
  sign_logic: spreadsheetSignLogicSchema,
  date_day_first: z.boolean().default(true),
});

const spreadsheetLayoutJsonSchema = {
  type: "object",
  additionalProperties: false,
  required: ["column_map", "sign_logic", "date_day_first"],
  properties: {
    column_map: {
      type: "object",
      additionalProperties: false,
      required: [
        "transaction_date",
        "posted_date",
        "description_raw",
        "amount_original_signed",
        "currency_original",
        "balance_original",
        "external_reference",
        "transaction_type_raw",
        "security_symbol",
        "security_name",
        "quantity",
        "unit_price_original",
        "fees_original",
        "fx_rate",
      ],
      properties: {
        transaction_date: { type: "string" },
        posted_date: { type: ["string", "null"] },
        description_raw: { type: ["string", "null"] },
        amount_original_signed: { type: ["string", "null"] },
        currency_original: { type: ["string", "null"] },
        balance_original: { type: ["string", "null"] },
        external_reference: { type: ["string", "null"] },
        transaction_type_raw: { type: ["string", "null"] },
        security_symbol: { type: ["string", "null"] },
        security_name: { type: ["string", "null"] },
        quantity: { type: ["string", "null"] },
        unit_price_original: { type: ["string", "null"] },
        fees_original: { type: ["string", "null"] },
        fx_rate: { type: ["string", "null"] },
      },
    },
    sign_logic: {
      type: "object",
      additionalProperties: false,
      required: [
        "mode",
        "invert_sign",
        "direction_column",
        "debit_column",
        "credit_column",
        "debit_values",
        "credit_values",
      ],
      properties: {
        mode: {
          type: "string",
          enum: [
            "signed_amount",
            "amount_direction_column",
            "debit_credit_columns",
          ],
        },
        invert_sign: { type: ["boolean", "null"] },
        direction_column: { type: ["string", "null"] },
        debit_column: { type: ["string", "null"] },
        credit_column: { type: ["string", "null"] },
        debit_values: {
          type: ["array", "null"],
          items: { type: "string" },
          maxItems: 8,
        },
        credit_values: {
          type: ["array", "null"],
          items: { type: "string" },
          maxItems: 8,
        },
      },
    },
    date_day_first: { type: "boolean" },
  },
} satisfies Record<string, unknown>;

export interface InferSpreadsheetLayoutInput {
  tablePreviewCsv: string;
  fileKind: "csv" | "xlsx";
  sheetName?: string | null;
  canonicalFields: readonly string[];
  accountType: string;
  defaultCurrency: string;
  detectedHeaders: readonly string[];
}

export type SpreadsheetLayout = z.infer<typeof spreadsheetLayoutResponseSchema>;

function buildSystemPrompt() {
  return [
    "Infer the canonical column mapping and sign logic for a bank-import table.",
    "Return one strict JSON object only.",
    "Only map headers that are clearly present in the preview.",
    "Use only the exact source headers shown in the preview.",
    "Choose one sign logic mode and fill only the fields needed for that mode.",
    "If debits and credits are already signed in one column, use signed_amount.",
    "Always include every field in column_map and sign_logic. Use null when a field does not apply.",
  ].join(" ");
}

function buildUserPrompt(input: InferSpreadsheetLayoutInput) {
  return [
    `File kind: ${input.fileKind}.`,
    `Sheet name: ${input.sheetName ?? "null"}.`,
    `Account type: ${input.accountType}.`,
    `Default currency if no currency column exists: ${input.defaultCurrency}.`,
    `Canonical fields: ${input.canonicalFields.join(", ")}.`,
    `Detected headers: ${input.detectedHeaders.join(", ")}.`,
    "Table preview CSV:",
    input.tablePreviewCsv,
  ].join("\n");
}

export async function inferSpreadsheetLayout(
  client: LLMTaskClient,
  input: InferSpreadsheetLayoutInput,
  modelName: string,
) {
  return client.generateJson({
    systemPrompt: buildSystemPrompt(),
    userPrompt: buildUserPrompt(input),
    modelName,
    responseSchema: spreadsheetLayoutResponseSchema,
    responseJsonSchema: spreadsheetLayoutJsonSchema,
    schemaName: "spreadsheet_layout",
    temperature: 0,
    allowLocaleNumberStrings: true,
  });
}

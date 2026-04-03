import { z } from "zod";

import type { LLMTaskClient } from "../types";

export const spreadsheetLayoutResponseSchema = z.object({
  header_row_index: z.number().int().min(1),
  columns: z.record(z.string().min(1), z.string().min(1)),
  sheet_name: z.string().nullable().optional(),
  rows_to_skip_before_header: z.number().int().min(0).default(0),
});

const spreadsheetLayoutJsonSchema = {
  type: "object",
  additionalProperties: false,
  required: ["header_row_index", "columns", "rows_to_skip_before_header"],
  properties: {
    header_row_index: { type: "integer", minimum: 1 },
    columns: {
      type: "object",
      additionalProperties: { type: "string" },
    },
    sheet_name: { type: ["string", "null"] },
    rows_to_skip_before_header: { type: "integer", minimum: 0 },
  },
} satisfies Record<string, unknown>;

export interface InferSpreadsheetLayoutInput {
  previewCsv: string;
  fileKind: "csv" | "xlsx";
  sheetName?: string | null;
  canonicalFields: readonly string[];
}

export type SpreadsheetLayout = z.infer<typeof spreadsheetLayoutResponseSchema>;

function buildSystemPrompt() {
  return [
    "Infer spreadsheet layout for transaction imports.",
    "Return one strict JSON object only.",
    "Detect the header row index and map canonical finance fields to the source column labels.",
    "Only include columns that are clearly present in the preview.",
  ].join(" ");
}

function buildUserPrompt(input: InferSpreadsheetLayoutInput) {
  return [
    `File kind: ${input.fileKind}.`,
    `Sheet name: ${input.sheetName ?? "null"}.`,
    `Canonical fields: ${input.canonicalFields.join(", ")}.`,
    "Sheet preview CSV:",
    input.previewCsv,
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

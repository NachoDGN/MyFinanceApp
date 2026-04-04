import { z } from "zod";

import type { LLMTaskClient } from "../types";

export const spreadsheetTableStartResponseSchema = z.object({
  sheet_name: z.string().nullable(),
  header_row_index: z.number().int().min(1),
  rows_to_skip_before_header: z.number().int().min(0),
  start_column_letter: z
    .string()
    .trim()
    .regex(/^[A-Z]+$/),
});

const spreadsheetTableStartJsonSchema = {
  type: "object",
  additionalProperties: false,
  required: [
    "sheet_name",
    "header_row_index",
    "rows_to_skip_before_header",
    "start_column_letter",
  ],
  properties: {
    sheet_name: { type: ["string", "null"] },
    header_row_index: { type: "integer", minimum: 1 },
    rows_to_skip_before_header: { type: "integer", minimum: 0 },
    start_column_letter: {
      type: "string",
      pattern: "^[A-Z]+$",
    },
  },
} satisfies Record<string, unknown>;

export interface InferSpreadsheetTableStartInput {
  fileKind: "csv" | "xlsx";
  sheetPreviews: ReadonlyArray<{
    sheetName: string | null;
    previewCsv: string;
  }>;
}

export type SpreadsheetTableStart = z.infer<
  typeof spreadsheetTableStartResponseSchema
>;

function buildSystemPrompt() {
  return [
    "Locate the transaction table within a spreadsheet preview.",
    "Return one strict JSON object only.",
    "Each preview includes row numbers and Excel-style column letters.",
    "Identify the header row and the left-most column of the transaction table.",
    "Prefer the sheet that clearly contains transaction rows rather than cover pages or summaries.",
    "For XLSX files, sheet_name must exactly match one of the provided sheet labels. Do not invent, translate, or paraphrase sheet names.",
    "Always include sheet_name. Use null for CSV files or when uncertain.",
  ].join(" ");
}

function buildUserPrompt(input: InferSpreadsheetTableStartInput) {
  return [
    `File kind: ${input.fileKind}.`,
    "Workbook previews:",
    ...input.sheetPreviews.map((preview, index) =>
      [
        `Sheet ${index + 1}: ${preview.sheetName ?? "null"}`,
        preview.previewCsv,
      ].join("\n"),
    ),
  ].join("\n\n");
}

export async function inferSpreadsheetTableStart(
  client: LLMTaskClient,
  input: InferSpreadsheetTableStartInput,
  modelName: string,
) {
  return client.generateJson({
    systemPrompt: buildSystemPrompt(),
    userPrompt: buildUserPrompt(input),
    modelName,
    responseSchema: spreadsheetTableStartResponseSchema,
    responseJsonSchema: spreadsheetTableStartJsonSchema,
    schemaName: "spreadsheet_table_start",
    temperature: 0,
  });
}

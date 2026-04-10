import { z } from "zod";

import { renderSpreadsheetTableStartPromptFromInput } from "../prompts";
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
  fileKind: "csv" | "xls" | "xlsx";
  sheetPreviews: ReadonlyArray<{
    sheetName: string | null;
    previewCsv: string;
  }>;
  promptOverrides?: Record<string, unknown> | null;
}

export type SpreadsheetTableStart = z.infer<
  typeof spreadsheetTableStartResponseSchema
>;

export async function inferSpreadsheetTableStart(
  client: LLMTaskClient,
  input: InferSpreadsheetTableStartInput,
  modelName: string,
) {
  const prompt = renderSpreadsheetTableStartPromptFromInput({
    fileKind: input.fileKind,
    sheetPreviews: input.sheetPreviews.map((preview) => ({
      sheetName: preview.sheetName ?? "null",
      previewCsv: preview.previewCsv,
    })),
    promptOverrides: input.promptOverrides ?? null,
  });
  return client.generateJson({
    systemPrompt: prompt.systemPrompt,
    userPrompt: prompt.userPrompt,
    modelName,
    responseSchema: spreadsheetTableStartResponseSchema,
    responseJsonSchema: spreadsheetTableStartJsonSchema,
    schemaName: "spreadsheet_table_start",
    temperature: 0,
  });
}

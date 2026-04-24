import { basename } from "node:path";

import {
  createLLMClient,
  inferSpreadsheetLayout,
  inferSpreadsheetTableStart,
  isModelConfigured,
  type LLMTaskClient,
  type PromptProfileOverrides,
} from "@myfinance/llm";

import { logImportDebug } from "./import-debug";
import {
  columnLetterToIndex,
  inspectSpreadsheetWorkbook,
  previewSpreadsheetTable,
} from "./runner";
import {
  canonicalFieldKeys,
  isWorkbookFileKind,
  todayIso,
  type Account,
  type ImportTemplate,
} from "@myfinance/domain";

export interface InferImportTemplateDraftInput {
  userId: string;
  account: Pick<
    Account,
    "id" | "institutionName" | "accountType" | "defaultCurrency"
  >;
  filePath: string;
  originalFilename?: string;
}

export interface ImportTemplateInferenceDeps {
  llmClient?: LLMTaskClient;
  inspectWorkbook?: typeof inspectSpreadsheetWorkbook;
  previewTable?: typeof previewSpreadsheetTable;
  modelName?: string;
  referenceDate?: string;
  promptOverrides?: PromptProfileOverrides;
}

export function getImportTemplateInferenceConfig() {
  return {
    model:
      process.env.LLM_IMPORT_TEMPLATE_MODEL ??
      process.env.OPENAI_IMPORT_TEMPLATE_MODEL ??
      process.env.LLM_TRANSACTION_MODEL ??
      process.env.OPENAI_TRANSACTION_MODEL ??
      process.env.LLM_RULES_MODEL ??
      process.env.OPENAI_RULES_MODEL ??
      "gemini-3-flash-preview",
  };
}

export function isImportTemplateInferenceConfigured() {
  return isModelConfigured(getImportTemplateInferenceConfig().model);
}

function buildTemplateName(input: InferImportTemplateDraftInput) {
  const fileLabel = basename(input.originalFilename ?? input.filePath).replace(
    /\.[^.]+$/,
    "",
  );

  return `${input.account.institutionName} ${input.account.accountType} ${fileLabel} auto`;
}

function compactRecord<T extends Record<string, unknown>>(value: T) {
  return Object.fromEntries(
    Object.entries(value).filter(([, entry]) => {
      if (entry === null || entry === undefined || entry === "") return false;
      return !Array.isArray(entry) || entry.length > 0;
    }),
  );
}

function buildSignLogicJson(
  signLogic: Awaited<ReturnType<typeof inferSpreadsheetLayout>>["sign_logic"],
) {
  return compactRecord({
    mode: signLogic.mode,
    invert_sign: signLogic.invert_sign,
    direction_column: signLogic.direction_column,
    debit_column: signLogic.debit_column,
    credit_column: signLogic.credit_column,
    debit_values: signLogic.debit_values,
    credit_values: signLogic.credit_values,
  });
}

function normalizeSheetName(value: string) {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function resolvePreviewSheetName(
  sheetPreviews: ReadonlyArray<{ sheetName: string | null }>,
  inferredSheetName: string | null,
) {
  const availableSheetNames = sheetPreviews
    .map((sheet) => sheet.sheetName)
    .filter((sheetName): sheetName is string => Boolean(sheetName?.trim()));

  if (availableSheetNames.length === 0) {
    return null;
  }

  if (!inferredSheetName?.trim()) {
    return availableSheetNames[0];
  }

  const exactMatch = availableSheetNames.find(
    (sheetName) => sheetName === inferredSheetName,
  );
  if (exactMatch) {
    return exactMatch;
  }

  const normalizedInferredSheetName = normalizeSheetName(inferredSheetName);
  const normalizedExactMatch = availableSheetNames.find(
    (sheetName) =>
      normalizeSheetName(sheetName) === normalizedInferredSheetName,
  );
  if (normalizedExactMatch) {
    return normalizedExactMatch;
  }

  const normalizedContainmentMatch = availableSheetNames.find((sheetName) => {
    const normalizedSheetName = normalizeSheetName(sheetName);
    return (
      normalizedSheetName.includes(normalizedInferredSheetName) ||
      normalizedInferredSheetName.includes(normalizedSheetName)
    );
  });
  if (normalizedContainmentMatch) {
    return normalizedContainmentMatch;
  }

  return availableSheetNames[0];
}

function assertInferredLayout(
  layout: Awaited<ReturnType<typeof inferSpreadsheetLayout>>,
) {
  if (!layout.column_map.transaction_date) {
    throw new Error(
      "The inferred template is missing a transaction date column.",
    );
  }

  const signMode = layout.sign_logic.mode;
  if (
    signMode !== "debit_credit_columns" &&
    !layout.column_map.amount_original_signed
  ) {
    throw new Error("The inferred template is missing an amount column.");
  }
  if (
    signMode === "amount_direction_column" &&
    !layout.sign_logic.direction_column
  ) {
    throw new Error("The inferred template is missing a direction column.");
  }
  if (
    signMode === "debit_credit_columns" &&
    !layout.sign_logic.debit_column &&
    !layout.sign_logic.credit_column
  ) {
    throw new Error(
      "The inferred template is missing debit and credit columns.",
    );
  }
}

export async function inferImportTemplateDraft(
  input: InferImportTemplateDraftInput,
  deps: ImportTemplateInferenceDeps = {},
): Promise<Omit<ImportTemplate, "id" | "createdAt" | "updatedAt" | "version">> {
  const modelName = deps.modelName ?? getImportTemplateInferenceConfig().model;
  const referenceDate = deps.referenceDate ?? todayIso();
  logImportDebug("template-inference:start", {
    accountId: input.account.id,
    institutionName: input.account.institutionName,
    accountType: input.account.accountType,
    originalFilename: input.originalFilename ?? null,
    resolvedModel: modelName,
    referenceDate,
    hasOpenAiKey: Boolean(process.env.OPENAI_API_KEY),
  });
  if (!deps.llmClient && !isModelConfigured(modelName)) {
    logImportDebug("template-inference:missing-credentials", {
      resolvedModel: modelName,
      hasOpenAiKey: Boolean(process.env.OPENAI_API_KEY),
    });
    throw new Error(
      `Spreadsheet template inference requires LLM credentials. Resolved model: ${modelName}.`,
    );
  }

  const llmClient = deps.llmClient ?? createLLMClient();
  const inspectWorkbook = deps.inspectWorkbook ?? inspectSpreadsheetWorkbook;
  const previewTable = deps.previewTable ?? previewSpreadsheetTable;

  const workbookPreview = await inspectWorkbook(input.filePath);
  if (workbookPreview.fileKind === "pdf") {
    throw new Error(
      "PDF statement uploads use the dedicated AI PDF parser and do not support spreadsheet template inference.",
    );
  }
  if (workbookPreview.sheetPreviews.length === 0) {
    throw new Error(
      isWorkbookFileKind(workbookPreview.fileKind)
        ? "The uploaded spreadsheet does not contain any worksheet tabs with rows and columns to preview."
        : "No spreadsheet preview could be generated from the uploaded file.",
    );
  }
  logImportDebug("template-inference:workbook-preview", {
    fileKind: workbookPreview.fileKind,
    sheetNames: workbookPreview.sheetPreviews.map((sheet) => sheet.sheetName),
    delimiter: workbookPreview.delimiter ?? null,
    encoding: workbookPreview.encoding ?? null,
  });

  const tableStart = await inferSpreadsheetTableStart(
    llmClient,
    {
      fileKind: workbookPreview.fileKind,
      sheetPreviews: workbookPreview.sheetPreviews,
      promptOverrides: deps.promptOverrides?.spreadsheet_table_start ?? null,
    },
    modelName,
  );
  logImportDebug("template-inference:table-start", {
    sheetName: tableStart.sheet_name ?? null,
    headerRowIndex: tableStart.header_row_index,
    rowsToSkipBeforeHeader: tableStart.rows_to_skip_before_header,
    startColumnLetter: tableStart.start_column_letter,
  });

  const startColumnIndex = columnLetterToIndex(tableStart.start_column_letter);
  const rowsToSkipBeforeHeader =
    tableStart.rows_to_skip_before_header ??
    Math.max(tableStart.header_row_index - 1, 0);
  const resolvedSheetName = isWorkbookFileKind(workbookPreview.fileKind)
    ? resolvePreviewSheetName(
        workbookPreview.sheetPreviews,
        tableStart.sheet_name,
      )
    : null;
  if (
    isWorkbookFileKind(workbookPreview.fileKind) &&
    tableStart.sheet_name &&
    resolvedSheetName !== tableStart.sheet_name
  ) {
    logImportDebug("template-inference:sheet-name-corrected", {
      inferredSheetName: tableStart.sheet_name,
      resolvedSheetName,
      availableSheetNames: workbookPreview.sheetPreviews.map(
        (sheet) => sheet.sheetName,
      ),
    });
  }

  const tablePreview = await previewTable({
    filePath: input.filePath,
    fileKind: workbookPreview.fileKind,
    sheetName: resolvedSheetName,
    headerRowIndex: tableStart.header_row_index,
    rowsToSkipBeforeHeader,
    startColumnIndex,
    delimiter: workbookPreview.delimiter ?? null,
    encoding: workbookPreview.encoding ?? null,
  });
  logImportDebug("template-inference:table-preview", {
    sheetName: tablePreview.sheetName ?? null,
    headers: tablePreview.headers,
  });

  const layout = await inferSpreadsheetLayout(
    llmClient,
    {
      tablePreviewCsv: tablePreview.previewCsv,
      fileKind: workbookPreview.fileKind,
      sheetName: tablePreview.sheetName,
      canonicalFields: canonicalFieldKeys,
      accountType: input.account.accountType,
      defaultCurrency: input.account.defaultCurrency,
      detectedHeaders: tablePreview.headers,
      referenceDate,
      promptOverrides: deps.promptOverrides?.spreadsheet_layout ?? null,
    },
    modelName,
  );
  assertInferredLayout(layout);
  logImportDebug("template-inference:layout", {
    columnMap: layout.column_map,
    signLogic: layout.sign_logic,
    dateDayFirst: layout.date_day_first,
  });

  const template = {
    userId: input.userId,
    name: buildTemplateName(input),
    institutionName: input.account.institutionName,
    compatibleAccountType: input.account.accountType,
    fileKind: workbookPreview.fileKind,
    sheetName: tablePreview.sheetName,
    headerRowIndex: tableStart.header_row_index,
    rowsToSkipBeforeHeader,
    rowsToSkipAfterHeader: 0,
    delimiter:
      workbookPreview.fileKind === "csv"
        ? (workbookPreview.delimiter ?? ",")
        : null,
    encoding:
      workbookPreview.fileKind === "csv"
        ? (workbookPreview.encoding ?? "utf-8")
        : null,
    decimalSeparator: null,
    thousandsSeparator: null,
    dateFormat: "%Y-%m-%d",
    defaultCurrency: input.account.defaultCurrency,
    columnMapJson: compactRecord(layout.column_map),
    signLogicJson: buildSignLogicJson(layout.sign_logic),
    normalizationRulesJson: compactRecord({
      date_day_first: layout.date_day_first,
      start_column_index: startColumnIndex,
      start_column_letter: tableStart.start_column_letter,
    }),
    active: true,
  };
  logImportDebug("template-inference:complete", {
    templateName: template.name,
    compatibleAccountType: template.compatibleAccountType,
    fileKind: template.fileKind,
    sheetName: template.sheetName ?? null,
  });
  return template;
}

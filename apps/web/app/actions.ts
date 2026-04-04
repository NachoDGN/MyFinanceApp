"use server";

import { randomUUID } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { revalidatePath } from "next/cache";
import { z } from "zod";

import {
  createFinanceRepository,
  getDbRuntimeConfig,
  getPromptOverrides,
  updatePromptProfile,
} from "@myfinance/db";
import {
  accountTypeOptions,
  canonicalFieldKeys,
  createTemplateConfig,
  FinanceDomainService,
  inferImportTemplateDraft,
  logTemporaryImportDebug,
  signModeOptions,
  validateSpreadsheetFile,
} from "@myfinance/domain";
import { NEW_SPREADSHEET_TEMPLATE_ID } from "./import-constants";

const repository = createFinanceRepository();
const domain = new FinanceDomainService(repository);

const importFieldsSchema = z.object({
  accountId: z.string(),
  templateId: z.string(),
});

const columnMappingSchema = z.object({
  source: z.string().default(""),
  target: z.enum(canonicalFieldKeys),
});

const templateSchema = z.object({
  name: z.string().min(1),
  institutionName: z.string().min(1),
  compatibleAccountType: z.enum([
    "checking",
    "savings",
    "company_bank",
    "brokerage_cash",
    "brokerage_account",
    "credit_card",
    "other",
  ]),
  fileKind: z.enum(["csv", "xlsx"]),
  sheetName: z.string().nullable().optional(),
  headerRowIndex: z.coerce.number().int().min(1).default(1),
  rowsToSkipBeforeHeader: z.coerce.number().int().min(0).default(0),
  rowsToSkipAfterHeader: z.coerce.number().int().min(0).default(0),
  delimiter: z.string().nullable().optional(),
  encoding: z.string().nullable().optional(),
  decimalSeparator: z.string().nullable().optional(),
  thousandsSeparator: z.string().nullable().optional(),
  dateFormat: z.string().min(1).default("%Y-%m-%d"),
  defaultCurrency: z.string().min(1).default("EUR"),
  columnMappings: z.array(columnMappingSchema).min(1),
  signMode: z.enum(signModeOptions).default("signed_amount"),
  invertSign: z.boolean().default(false),
  directionColumn: z.string().nullable().optional(),
  debitColumn: z.string().nullable().optional(),
  creditColumn: z.string().nullable().optional(),
  debitValuesText: z.string().nullable().optional(),
  creditValuesText: z.string().nullable().optional(),
  dateDayFirst: z.boolean().default(true),
  active: z.boolean().default(true),
});

const accountSchema = z.object({
  entityId: z.string().uuid(),
  institutionName: z.string().min(1),
  displayName: z.string().min(1),
  accountType: z.enum(accountTypeOptions),
  defaultCurrency: z.string().min(1).default("EUR"),
  openingBalanceOriginal: z
    .string()
    .trim()
    .optional()
    .transform((value) => value || null),
  openingBalanceDate: z
    .string()
    .trim()
    .optional()
    .transform((value) => value || null),
  includeInConsolidation: z.boolean().default(true),
  importTemplateDefaultId: z
    .string()
    .trim()
    .optional()
    .transform((value) => value || null),
  matchingAliasesText: z.string().trim().optional().default(""),
  accountSuffix: z
    .string()
    .trim()
    .optional()
    .transform((value) => value || null),
  balanceMode: z.enum(["statement", "computed"]).default("statement"),
  staleAfterDays: z.coerce.number().int().min(1).max(365).nullable().optional(),
});

const promptProfileUpdateSchema = z.object({
  promptId: z.enum([
    "cash_transaction_analyzer",
    "investment_transaction_analyzer",
    "spreadsheet_table_start",
    "spreadsheet_layout",
    "rule_draft_parser",
  ]),
  sectionsJson: z.string().min(2),
});

function toAssetDomain(
  accountType: z.infer<typeof accountSchema>["accountType"],
) {
  return accountType === "brokerage_account" ? "investment" : "cash";
}

function parseAliases(value: string) {
  return [
    ...new Set(
      value
        .split(",")
        .map((entry) => entry.trim())
        .filter(Boolean),
    ),
  ];
}

async function withUploadedImport<T>(
  mode: "preview" | "commit",
  formData: FormData,
  run: (input: {
    accountId: string;
    templateId: string;
    originalFilename: string;
    filePath: string;
  }) => Promise<T>,
): Promise<T | (T & { resolvedTemplateName: string })> {
  const fields = importFieldsSchema.parse({
    accountId: formData.get("accountId"),
    templateId: formData.get("templateId"),
  });
  const file = formData.get("file");
  if (
    !file ||
    typeof file !== "object" ||
    typeof (file as File).arrayBuffer !== "function"
  ) {
    throw new Error("A file upload is required.");
  }

  const uploadDirectory = join(tmpdir(), "myfinance-imports", randomUUID());
  await mkdir(uploadDirectory, { recursive: true });
  const filePath = join(uploadDirectory, file.name || "upload.bin");
  await writeFile(filePath, Buffer.from(await file.arrayBuffer()));
  logTemporaryImportDebug("upload:start", {
    accountId: fields.accountId,
    templateId: fields.templateId,
    originalFilename: file.name,
    filePath,
  });

  try {
    const validation = await validateSpreadsheetFile(filePath);
    logTemporaryImportDebug("upload:file-validation", {
      accountId: fields.accountId,
      templateId: fields.templateId,
      originalFilename: file.name,
      issues: validation.issues,
    });
    const blockingIssue = validation.issues.find(
      (issue) => issue.severity === "error",
    );
    if (blockingIssue) {
      throw new Error(blockingIssue.message);
    }

    let resolvedTemplateId = fields.templateId;
    let resolvedTemplateName: string | null = null;

    if (fields.templateId === NEW_SPREADSHEET_TEMPLATE_ID) {
      logTemporaryImportDebug("upload:new-spreadsheet-selected", {
        accountId: fields.accountId,
        originalFilename: file.name,
      });
      const dataset = await repository.getDataset();
      const account = dataset.accounts.find(
        (candidate) => candidate.id === fields.accountId,
      );
      if (!account) {
        throw new Error(`Account ${fields.accountId} was not found.`);
      }

      const { seededUserId } = getDbRuntimeConfig();
      const promptOverrides = await getPromptOverrides();
      const inferredTemplate = await inferImportTemplateDraft({
        userId: seededUserId,
        account,
        filePath,
        originalFilename: file.name,
      }, { promptOverrides });
      const createResult = await domain.createTemplate({
        template: inferredTemplate,
        actorName: "web-action",
        sourceChannel: "web",
        apply: true,
      });
      resolvedTemplateId = createResult.templateId;
      resolvedTemplateName = inferredTemplate.name;
      logTemporaryImportDebug("upload:template-created", {
        accountId: fields.accountId,
        templateId: resolvedTemplateId,
        templateName: resolvedTemplateName,
      });
      revalidatePath("/templates");
      revalidatePath("/imports");
    }

    const result = await run({
      accountId: fields.accountId,
      templateId: resolvedTemplateId,
      originalFilename: file.name,
      filePath,
    });
    logTemporaryImportDebug("upload:run-complete", {
      accountId: fields.accountId,
      templateId: resolvedTemplateId,
      mode,
      rowCountParsed:
        result && typeof result === "object" && "rowCountParsed" in result
          ? result.rowCountParsed
          : null,
      rowCountFailed:
        result && typeof result === "object" && "rowCountFailed" in result
          ? result.rowCountFailed
          : null,
    });
    if (
      resolvedTemplateName &&
      result &&
      typeof result === "object" &&
      !Array.isArray(result)
    ) {
      return {
        ...(result as Record<string, unknown>),
        resolvedTemplateName,
        fileValidationIssues: validation.issues,
      } as unknown as T & { resolvedTemplateName: string };
    }
    if (result && typeof result === "object" && !Array.isArray(result)) {
      return {
        ...(result as Record<string, unknown>),
        fileValidationIssues: validation.issues,
      } as unknown as T;
    }
    return result;
  } catch (error) {
    logTemporaryImportDebug("upload:error", {
      accountId: fields.accountId,
      templateId: fields.templateId,
      originalFilename: file.name,
      error: error instanceof Error ? error.message : "Unknown upload error.",
    });
    throw error;
  } finally {
    await rm(uploadDirectory, { recursive: true, force: true });
  }
}

export async function previewImportAction(formData: FormData) {
  return withUploadedImport("preview", formData, async (input) => {
    logTemporaryImportDebug("preview-action:start", {
      accountId: input.accountId,
      templateId: input.templateId,
      originalFilename: input.originalFilename,
    });
    return domain.previewImport(input);
  });
}

export async function commitImportAction(formData: FormData) {
  logTemporaryImportDebug("commit-action:start");
  const result = await withUploadedImport("commit", formData, (input) =>
    domain.commitImport(input),
  );
  logTemporaryImportDebug("commit-action:complete", {
    accountId: result.accountId,
    templateId: result.templateId,
    importBatchId: "importBatchId" in result ? result.importBatchId : null,
    rowCountParsed: result.rowCountParsed,
    rowCountFailed: result.rowCountFailed,
  });
  revalidatePath("/imports");
  revalidatePath("/");
  revalidatePath("/transactions");
  revalidatePath("/accounts");
  revalidatePath("/spending");
  revalidatePath("/income");
  revalidatePath("/investments");
  return result;
}

export async function createTemplateAction(
  input: z.input<typeof templateSchema>,
) {
  const template = templateSchema.parse(input);
  const {
    columnMappings: _columnMappings,
    signMode: _signMode,
    invertSign: _invertSign,
    directionColumn: _directionColumn,
    debitColumn: _debitColumn,
    creditColumn: _creditColumn,
    debitValuesText: _debitValuesText,
    creditValuesText: _creditValuesText,
    dateDayFirst: _dateDayFirst,
    ...templateFields
  } = template;
  const { columnMapJson, signLogicJson, normalizationRulesJson } =
    createTemplateConfig({
      columnMappings: template.columnMappings,
      signMode: template.signMode,
      invertSign: template.invertSign,
      directionColumn: template.directionColumn,
      debitColumn: template.debitColumn,
      creditColumn: template.creditColumn,
      debitValuesText: template.debitValuesText,
      creditValuesText: template.creditValuesText,
      dateDayFirst: template.dateDayFirst,
    });
  const { seededUserId } = getDbRuntimeConfig();
  const result = await domain.createTemplate({
    template: {
      userId: seededUserId,
      ...templateFields,
      sheetName: templateFields.sheetName ?? null,
      delimiter: templateFields.delimiter ?? null,
      encoding: templateFields.encoding ?? null,
      decimalSeparator: templateFields.decimalSeparator ?? null,
      thousandsSeparator: templateFields.thousandsSeparator ?? null,
      columnMapJson,
      signLogicJson,
      normalizationRulesJson,
    },
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidatePath("/templates");
  revalidatePath("/imports");
  return result;
}

export async function deleteTemplateAction(templateId: string) {
  const parsed = z.string().uuid().parse(templateId);
  const result = await domain.deleteTemplate({
    templateId: parsed,
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidatePath("/templates");
  revalidatePath("/imports");
  revalidatePath("/accounts");
  return result;
}

export async function createAccountAction(
  input: z.input<typeof accountSchema>,
) {
  const account = accountSchema.parse(input);
  const result = await domain.createAccount({
    account: {
      entityId: account.entityId,
      institutionName: account.institutionName,
      displayName: account.displayName,
      accountType: account.accountType,
      assetDomain: toAssetDomain(account.accountType),
      defaultCurrency: account.defaultCurrency,
      openingBalanceOriginal: account.openingBalanceOriginal,
      openingBalanceCurrency: account.openingBalanceOriginal
        ? account.defaultCurrency
        : null,
      openingBalanceDate: account.openingBalanceOriginal
        ? account.openingBalanceDate
        : null,
      includeInConsolidation: account.includeInConsolidation,
      isActive: true,
      importTemplateDefaultId: account.importTemplateDefaultId,
      matchingAliases: parseAliases(account.matchingAliasesText),
      accountSuffix: account.accountSuffix,
      balanceMode: account.balanceMode,
      staleAfterDays: account.staleAfterDays ?? null,
    },
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidatePath("/accounts");
  revalidatePath("/imports");
  revalidatePath("/");
  return result;
}

export async function deleteAccountAction(accountId: string) {
  const parsed = z.string().uuid().parse(accountId);
  const result = await domain.deleteAccount({
    accountId: parsed,
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidatePath("/accounts");
  revalidatePath("/imports");
  revalidatePath("/");
  return result;
}

export async function resetWorkspaceAction() {
  const result = await domain.resetWorkspace({
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidatePath("/");
  revalidatePath("/accounts");
  revalidatePath("/imports");
  revalidatePath("/investments");
  revalidatePath("/income");
  revalidatePath("/insights");
  revalidatePath("/spending");
  revalidatePath("/transactions");
  revalidatePath("/rules");
  revalidatePath("/templates");
  revalidatePath("/settings");
  return result;
}

export async function updatePromptProfileAction(formData: FormData) {
  const fields = promptProfileUpdateSchema.parse({
    promptId: formData.get("promptId"),
    sectionsJson: formData.get("sectionsJson"),
  });

  const parsedSections = JSON.parse(fields.sectionsJson) as Record<string, unknown>;
  const profile = await updatePromptProfile({
    promptId: fields.promptId,
    sections: parsedSections,
    actorName: "web-action",
    sourceChannel: "web",
  });

  revalidatePath("/prompts");
  return {
    profile,
    message: `Saved prompt overrides for ${fields.promptId}.`,
  };
}

export async function queueRuleDraftAction(requestText: string) {
  const parsed = z.string().min(8).parse(requestText);
  const result = await domain.queueRuleDraft({
    requestText: parsed,
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidatePath("/rules");
  return result;
}

export async function applyRuleDraftAction(jobId: string) {
  const parsed = z.string().min(1).parse(jobId);
  const result = await domain.applyRuleDraft({
    jobId: parsed,
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidatePath("/rules");
  return result;
}

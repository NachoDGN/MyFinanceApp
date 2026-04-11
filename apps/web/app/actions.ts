"use server";

import { randomUUID } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

import { revalidatePath } from "next/cache";
import { z } from "zod";

import {
  createFinanceRepository,
  getDbRuntimeConfig,
  getPromptOverrides,
  refreshOwnedStockPrices,
  updatePromptProfile,
} from "@myfinance/db";
import {
  accountTypeOptions,
  canonicalFieldKeys,
  createTemplateConfig,
  FinanceDomainService,
  inferImportTemplateDraft,
  logTemporaryImportDebug,
  type Account,
  signModeOptions,
  validateSpreadsheetFile,
} from "@myfinance/domain";
import { NEW_SPREADSHEET_TEMPLATE_ID } from "./import-constants";

const repository = createFinanceRepository();
const domain = new FinanceDomainService(repository);
const entitySlugPattern = /^[a-z0-9]+(?:[-_][a-z0-9]+)*$/;

const importFieldsSchema = z.object({
  accountId: z.string(),
  templateId: z.string(),
});

const creditCardStatementFieldsSchema = z.object({
  settlementTransactionId: z.string().uuid(),
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
  fileKind: z.enum(["csv", "xls", "xlsx", "pdf"]),
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

const nullableDayCountSchema = z.preprocess((value) => {
  if (value === "" || value === null || value === undefined) {
    return null;
  }
  return value;
}, z.coerce.number().int().min(1).max(365).nullable());

const isoDateSchema = z
  .string()
  .trim()
  .regex(/^\d{4}-\d{2}-\d{2}$/, "Use a date in YYYY-MM-DD format.");

const currencyCodeSchema = z
  .string()
  .trim()
  .min(3)
  .max(3)
  .transform((value) => value.toUpperCase());

const nonNegativeAmountStringSchema = z.preprocess(
  (value) => {
    if (typeof value === "number") {
      return value.toString();
    }
    return value;
  },
  z
    .string()
    .trim()
    .min(1)
    .refine(
      (value) => Number.isFinite(Number(value)) && Number(value) >= 0,
      "Enter a valid non-negative amount.",
    ),
);

const accountFieldsSchema = z.object({
  institutionName: z.string().trim().min(1),
  displayName: z.string().trim().min(1),
  defaultCurrency: z.string().trim().min(1).default("EUR"),
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
  staleAfterDays: nullableDayCountSchema,
});

const accountSchema = accountFieldsSchema.extend({
  entityId: z.string().uuid(),
  accountType: z.enum(accountTypeOptions),
});

const accountUpdateSchema = accountFieldsSchema.extend({
  accountId: z.string().uuid(),
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

function isSupportedTimeZone(value: string) {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: value });
    return true;
  } catch {
    return false;
  }
}

const workspaceProfileSchema = z.object({
  displayName: z.string().trim().min(1),
  defaultBaseCurrency: z.enum(["EUR", "USD"]).default("EUR"),
  timezone: z
    .string()
    .trim()
    .min(1)
    .refine(isSupportedTimeZone, "Choose a valid IANA timezone."),
  preferredScope: z.string().trim().min(1).default("consolidated"),
  defaultDisplayCurrency: z.enum(["EUR", "USD"]).default("EUR"),
  defaultPeriodPreset: z.enum(["mtd", "ytd"]).default("mtd"),
  defaultCashStaleAfterDays: z.coerce.number().int().min(1).max(365).default(7),
  defaultInvestmentStaleAfterDays: z.coerce
    .number()
    .int()
    .min(1)
    .max(365)
    .default(3),
});

const entitySchema = z.object({
  slug: z
    .string()
    .trim()
    .toLowerCase()
    .min(1)
    .regex(
      entitySlugPattern,
      "Use lowercase letters, numbers, hyphens, or underscores for the entity slug.",
    ),
  displayName: z.string().trim().min(1),
  legalName: z
    .string()
    .trim()
    .optional()
    .transform((value) => value || null),
  entityKind: z.enum(["personal", "company"]).default("company"),
  baseCurrency: z.enum(["EUR", "USD"]).default("EUR"),
});

const entityUpdateSchema = entitySchema
  .pick({
    slug: true,
    displayName: true,
    legalName: true,
    baseCurrency: true,
  })
  .extend({
    entityId: z.string().uuid(),
  });

const manualInvestmentMatcherSchema = z
  .string()
  .trim()
  .min(2)
  .refine(
    (value) => value.split(/[\n,]+/).some((term) => term.trim().length > 0),
    "Provide at least one matcher term.",
  );

const createManualInvestmentSchema = z.object({
  entityId: z.string().uuid(),
  fundingAccountId: z.string().uuid(),
  label: z.string().trim().min(1),
  matcherText: manualInvestmentMatcherSchema,
  note: z
    .string()
    .trim()
    .optional()
    .transform((value) => value || null),
  snapshotDate: isoDateSchema,
  currentValueOriginal: nonNegativeAmountStringSchema,
  currentValueCurrency: currencyCodeSchema,
  valuationNote: z
    .string()
    .trim()
    .optional()
    .transform((value) => value || null),
});

const manualInvestmentValuationSchema = z.object({
  manualInvestmentId: z.string().uuid(),
  snapshotDate: isoDateSchema,
  currentValueOriginal: nonNegativeAmountStringSchema,
  currentValueCurrency: currencyCodeSchema,
  note: z
    .string()
    .trim()
    .optional()
    .transform((value) => value || null),
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

function toAccountPatch(fields: z.infer<typeof accountFieldsSchema>) {
  return {
    institutionName: fields.institutionName,
    displayName: fields.displayName,
    defaultCurrency: fields.defaultCurrency,
    openingBalanceOriginal: fields.openingBalanceOriginal,
    openingBalanceDate: fields.openingBalanceOriginal
      ? fields.openingBalanceDate
      : null,
    includeInConsolidation: fields.includeInConsolidation,
    importTemplateDefaultId: fields.importTemplateDefaultId,
    matchingAliases: parseAliases(fields.matchingAliasesText),
    accountSuffix: fields.accountSuffix,
    balanceMode: fields.balanceMode,
    staleAfterDays: fields.staleAfterDays ?? null,
  };
}

function revalidateWorkspacePaths() {
  revalidatePath("/");
  revalidatePath("/dashboard");
  revalidatePath("/accounts");
  revalidatePath("/imports");
  revalidatePath("/income");
  revalidatePath("/insights");
  revalidatePath("/investments");
  revalidatePath("/rules");
  revalidatePath("/settings");
  revalidatePath("/spending");
  revalidatePath("/templates");
  revalidatePath("/transactions");
}

async function withUploadedImportFile<T>(
  formData: FormData,
  options: {
    logContext?: Record<string, unknown>;
    onValidatedUpload: (input: {
      originalFilename: string;
      filePath: string;
      fileKind: Awaited<ReturnType<typeof validateSpreadsheetFile>>["fileKind"];
      fileValidationIssues: Awaited<
        ReturnType<typeof validateSpreadsheetFile>
      >["issues"];
    }) => Promise<T>;
  },
): Promise<T> {
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
    originalFilename: file.name,
    filePath,
    ...(options.logContext ?? {}),
  });

  try {
    const validation = await validateSpreadsheetFile(filePath);
    logTemporaryImportDebug("upload:file-validation", {
      originalFilename: file.name,
      issues: validation.issues,
      ...(options.logContext ?? {}),
    });
    const blockingIssue = validation.issues.find(
      (issue) => issue.severity === "error",
    );
    if (blockingIssue) {
      throw new Error(blockingIssue.message);
    }

    const result = await options.onValidatedUpload({
      originalFilename: file.name,
      filePath,
      fileKind: validation.fileKind,
      fileValidationIssues: validation.issues,
    });
    if (result && typeof result === "object" && !Array.isArray(result)) {
      return {
        ...(result as Record<string, unknown>),
        fileKind: validation.fileKind,
        fileValidationIssues: validation.issues,
      } as T;
    }
    return result;
  } catch (error) {
    logTemporaryImportDebug("upload:error", {
      originalFilename: file.name,
      error: error instanceof Error ? error.message : "Unknown upload error.",
      ...(options.logContext ?? {}),
    });
    throw error;
  } finally {
    await rm(uploadDirectory, { recursive: true, force: true });
  }
}

function buildCreditCardInferenceAccount(
  dataset: Awaited<ReturnType<typeof repository.getDataset>>,
  settlementTransactionId: string,
  userId: string,
): Account {
  const settlementTransaction = dataset.transactions.find(
    (candidate) => candidate.id === settlementTransactionId,
  );
  if (!settlementTransaction) {
    throw new Error(
      `Settlement transaction ${settlementTransactionId} was not found.`,
    );
  }

  const settlementAccount = dataset.accounts.find(
    (candidate) => candidate.id === settlementTransaction.accountId,
  );
  if (!settlementAccount) {
    throw new Error(
      `Settlement account ${settlementTransaction.accountId} was not found.`,
    );
  }

  return {
    id: `credit-card-template-inference:${settlementTransactionId}`,
    userId,
    entityId: settlementAccount.entityId,
    institutionName: settlementAccount.institutionName,
    displayName: `${settlementAccount.institutionName} Credit Card`,
    accountType: "credit_card",
    assetDomain: "cash",
    defaultCurrency: settlementAccount.defaultCurrency,
    openingBalanceOriginal: null,
    openingBalanceCurrency: null,
    openingBalanceDate: null,
    includeInConsolidation: true,
    isActive: true,
    importTemplateDefaultId: null,
    matchingAliases: [],
    accountSuffix: null,
    balanceMode: "computed",
    staleAfterDays: settlementAccount.staleAfterDays ?? null,
    lastImportedAt: null,
    createdAt: new Date().toISOString(),
  };
}

function buildCreditCardPdfTemplate(input: {
  userId: string;
  account: Pick<Account, "institutionName" | "accountType" | "defaultCurrency">;
  originalFilename: string;
}) {
  const fileLabel = basename(input.originalFilename).replace(/\.[^.]+$/, "");

  return {
    userId: input.userId,
    name: `${input.account.institutionName} ${input.account.accountType} ${fileLabel} pdf auto`,
    institutionName: input.account.institutionName,
    compatibleAccountType: input.account.accountType,
    fileKind: "pdf" as const,
    sheetName: null,
    headerRowIndex: 1,
    rowsToSkipBeforeHeader: 0,
    rowsToSkipAfterHeader: 0,
    delimiter: null,
    encoding: null,
    decimalSeparator: null,
    thousandsSeparator: null,
    dateFormat: "%Y-%m-%d",
    defaultCurrency: input.account.defaultCurrency,
    columnMapJson: {},
    signLogicJson: {},
    normalizationRulesJson: {
      parser_kind: "credit_card_statement_pdf",
      date_day_first: true,
    },
    active: true,
  };
}

function findReusableCreditCardPdfTemplate(
  dataset: Awaited<ReturnType<typeof repository.getDataset>>,
  institutionName: string,
) {
  return (
    dataset.templates.find((template) => {
      const parserKind =
        template.normalizationRulesJson &&
        typeof template.normalizationRulesJson === "object" &&
        !Array.isArray(template.normalizationRulesJson) &&
        "parser_kind" in template.normalizationRulesJson
          ? (template.normalizationRulesJson as { parser_kind?: unknown })
              .parser_kind
          : null;

      return (
        template.compatibleAccountType === "credit_card" &&
        template.fileKind === "pdf" &&
        template.institutionName === institutionName &&
        parserKind === "credit_card_statement_pdf"
      );
    }) ?? null
  );
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
  return withUploadedImportFile(formData, {
    logContext: {
      accountId: fields.accountId,
      templateId: fields.templateId,
      mode,
    },
    onValidatedUpload: async ({ originalFilename, filePath }) => {
      let resolvedTemplateId = fields.templateId;
      let resolvedTemplateName: string | null = null;

      if (fields.templateId === NEW_SPREADSHEET_TEMPLATE_ID) {
        logTemporaryImportDebug("upload:new-spreadsheet-selected", {
          accountId: fields.accountId,
          originalFilename,
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
        const inferredTemplate = await inferImportTemplateDraft(
          {
            userId: seededUserId,
            account,
            filePath,
            originalFilename,
          },
          { promptOverrides },
        );
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
        originalFilename,
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
        } as unknown as T & { resolvedTemplateName: string };
      }

      return result;
    },
  });
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

export async function commitCreditCardStatementImportAction(
  formData: FormData,
) {
  const fields = creditCardStatementFieldsSchema.parse({
    settlementTransactionId: formData.get("settlementTransactionId"),
    templateId: formData.get("templateId"),
  });

  const result = await withUploadedImportFile(formData, {
    logContext: {
      settlementTransactionId: fields.settlementTransactionId,
      templateId: fields.templateId,
      mode: "credit-card-statement-commit",
    },
    onValidatedUpload: async ({ originalFilename, filePath, fileKind }) => {
      let resolvedTemplateId = fields.templateId;
      let resolvedTemplateName: string | null = null;
      const dataset = await repository.getDataset();
      const { seededUserId } = getDbRuntimeConfig();
      const inferenceAccount = buildCreditCardInferenceAccount(
        dataset,
        fields.settlementTransactionId,
        seededUserId,
      );

      if (fileKind === "pdf") {
        if (fields.templateId !== NEW_SPREADSHEET_TEMPLATE_ID) {
          const selectedTemplate = dataset.templates.find(
            (candidate) => candidate.id === fields.templateId,
          );
          if (selectedTemplate?.fileKind !== "pdf") {
            throw new Error(
              "PDF credit-card statements require the AI PDF parser template.",
            );
          }
        }

        if (fields.templateId === NEW_SPREADSHEET_TEMPLATE_ID) {
          const reusableTemplate = findReusableCreditCardPdfTemplate(
            dataset,
            inferenceAccount.institutionName,
          );
          if (reusableTemplate) {
            resolvedTemplateId = reusableTemplate.id;
          } else {
            const pdfTemplate = buildCreditCardPdfTemplate({
              userId: seededUserId,
              account: inferenceAccount,
              originalFilename,
            });
            const createResult = await domain.createTemplate({
              template: pdfTemplate,
              actorName: "web-credit-card-statement",
              sourceChannel: "web",
              apply: true,
            });
            resolvedTemplateId = createResult.templateId;
            resolvedTemplateName = pdfTemplate.name;
            revalidatePath("/templates");
          }
        }
      } else if (fields.templateId === NEW_SPREADSHEET_TEMPLATE_ID) {
        const promptOverrides = await getPromptOverrides();
        const inferredTemplate = await inferImportTemplateDraft(
          {
            userId: seededUserId,
            account: inferenceAccount,
            filePath,
            originalFilename,
          },
          { promptOverrides },
        );
        const createResult = await domain.createTemplate({
          template: inferredTemplate,
          actorName: "web-credit-card-statement",
          sourceChannel: "web",
          apply: true,
        });
        resolvedTemplateId = createResult.templateId;
        resolvedTemplateName = inferredTemplate.name;
        revalidatePath("/templates");
      } else {
        const selectedTemplate = dataset.templates.find(
          (candidate) => candidate.id === fields.templateId,
        );
        if (selectedTemplate?.fileKind === "pdf") {
          throw new Error(
            "Spreadsheet credit-card statements need a spreadsheet template, not the PDF parser template.",
          );
        }
      }

      const committed = await domain.commitCreditCardStatementImport({
        settlementTransactionId: fields.settlementTransactionId,
        templateId: resolvedTemplateId,
        originalFilename,
        filePath,
      });

      if (resolvedTemplateName) {
        return {
          ...committed,
          resolvedTemplateName,
        };
      }

      return committed;
    },
  });

  revalidateWorkspacePaths();
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
  const patch = toAccountPatch(account);
  const result = await domain.createAccount({
    account: {
      entityId: account.entityId,
      institutionName: patch.institutionName,
      displayName: patch.displayName,
      accountType: account.accountType,
      assetDomain: toAssetDomain(account.accountType),
      defaultCurrency: patch.defaultCurrency,
      openingBalanceOriginal: patch.openingBalanceOriginal,
      openingBalanceCurrency: patch.openingBalanceOriginal
        ? patch.defaultCurrency
        : null,
      openingBalanceDate: patch.openingBalanceDate,
      includeInConsolidation: patch.includeInConsolidation,
      isActive: true,
      importTemplateDefaultId: patch.importTemplateDefaultId,
      matchingAliases: patch.matchingAliases,
      accountSuffix: patch.accountSuffix,
      balanceMode: patch.balanceMode,
      staleAfterDays: patch.staleAfterDays,
    },
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidateWorkspacePaths();
  return result;
}

export async function updateAccountAction(
  input: z.input<typeof accountUpdateSchema>,
) {
  const account = accountUpdateSchema.parse(input);
  const result = await domain.updateAccount({
    accountId: account.accountId,
    patch: toAccountPatch(account),
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidateWorkspacePaths();
  return result;
}

export async function updateWorkspaceProfileAction(
  input: z.input<typeof workspaceProfileSchema>,
) {
  const profile = workspaceProfileSchema.parse(input);
  const result = await domain.updateWorkspaceProfile({
    profile: {
      displayName: profile.displayName,
      defaultBaseCurrency: profile.defaultBaseCurrency,
      timezone: profile.timezone,
      workspaceSettingsJson: {
        preferredScope: profile.preferredScope,
        defaultDisplayCurrency: profile.defaultDisplayCurrency,
        defaultPeriodPreset: profile.defaultPeriodPreset,
        defaultCashStaleAfterDays: profile.defaultCashStaleAfterDays,
        defaultInvestmentStaleAfterDays:
          profile.defaultInvestmentStaleAfterDays,
      },
    },
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidateWorkspacePaths();
  return result;
}

export async function createEntityAction(input: z.input<typeof entitySchema>) {
  const entity = entitySchema.parse(input);
  const result = await domain.createEntity({
    entity: {
      slug: entity.slug,
      displayName: entity.displayName,
      legalName: entity.legalName,
      entityKind: entity.entityKind,
      baseCurrency: entity.baseCurrency,
    },
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidateWorkspacePaths();
  return result;
}

export async function updateEntityAction(
  input: z.input<typeof entityUpdateSchema>,
) {
  const entity = entityUpdateSchema.parse(input);
  const result = await domain.updateEntity({
    entityId: entity.entityId,
    patch: {
      slug: entity.slug,
      displayName: entity.displayName,
      legalName: entity.legalName,
      baseCurrency: entity.baseCurrency,
    },
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidateWorkspacePaths();
  return result;
}

export async function deleteEntityAction(entityId: string) {
  const parsed = z.string().uuid().parse(entityId);
  const result = await domain.deleteEntity({
    entityId: parsed,
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidateWorkspacePaths();
  return result;
}

export async function createManualInvestmentAction(
  input: z.input<typeof createManualInvestmentSchema>,
) {
  const trackedInvestment = createManualInvestmentSchema.parse(input);
  const result = await domain.createManualInvestment({
    ...trackedInvestment,
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidateWorkspacePaths();
  return result;
}

export async function recordManualInvestmentValuationAction(
  input: z.input<typeof manualInvestmentValuationSchema>,
) {
  const valuation = manualInvestmentValuationSchema.parse(input);
  const result = await domain.recordManualInvestmentValuation({
    ...valuation,
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidateWorkspacePaths();
  return result;
}

export async function deleteManualInvestmentAction(manualInvestmentId: string) {
  const parsed = z.string().uuid().parse(manualInvestmentId);
  const result = await domain.deleteManualInvestment({
    manualInvestmentId: parsed,
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidateWorkspacePaths();
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

export async function refreshOwnedStockPricesAction() {
  const result = await refreshOwnedStockPrices();
  revalidateWorkspacePaths();
  return result;
}

export async function updatePromptProfileAction(formData: FormData) {
  const fields = promptProfileUpdateSchema.parse({
    promptId: formData.get("promptId"),
    sectionsJson: formData.get("sectionsJson"),
  });

  const parsedSections = JSON.parse(fields.sectionsJson) as Record<
    string,
    unknown
  >;
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

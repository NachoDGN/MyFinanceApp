import { randomUUID } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";

import { getDbRuntimeConfig, getPromptOverrides } from "@myfinance/db";
import { resolveAccountAssetDomain, type Account } from "@myfinance/domain";
import {
  inferImportTemplateDraft,
  logImportDebug,
  validateSpreadsheetFile,
} from "@myfinance/ingestion";

import { domain, repository } from "./action-service";
import { NEW_SPREADSHEET_TEMPLATE_ID } from "../app/import-constants";
import {
  creditCardStatementFieldsSchema,
  importFieldsSchema,
} from "./action-schemas";
import { revalidateTemplatePaths } from "./api-revalidate";

export function toAssetDomain(accountType: Account["accountType"]) {
  return resolveAccountAssetDomain(accountType);
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

export function toAccountPatch(fields: {
  institutionName: string;
  displayName: string;
  defaultCurrency: string;
  openingBalanceOriginal: string | null;
  openingBalanceDate: string | null;
  includeInConsolidation: boolean;
  importTemplateDefaultId: string | null;
  matchingAliasesText: string;
  accountSuffix: string | null;
  balanceMode: "statement" | "computed";
  staleAfterDays: number | null;
}) {
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

type UploadedFileValidation = Awaited<
  ReturnType<typeof validateSpreadsheetFile>
>;

async function withUploadedImportFile<T>(
  formData: FormData,
  options: {
    logContext?: Record<string, unknown>;
    onValidatedUpload: (input: {
      originalFilename: string;
      filePath: string;
      fileKind: UploadedFileValidation["fileKind"];
      fileValidationIssues: UploadedFileValidation["issues"];
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
  logImportDebug("upload:start", {
    originalFilename: file.name,
    filePath,
    ...(options.logContext ?? {}),
  });

  try {
    const validation = await validateSpreadsheetFile(filePath);
    logImportDebug("upload:file-validation", {
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
    logImportDebug("upload:error", {
      originalFilename: file.name,
      error: error instanceof Error ? error.message : "Unknown upload error.",
      ...(options.logContext ?? {}),
    });
    throw error;
  } finally {
    await rm(uploadDirectory, { recursive: true, force: true });
  }
}

export function buildCreditCardInferenceAccount(
  dataset: Awaited<ReturnType<typeof repository.getDataset>>,
  settlementTransactionId: string,
  userId: string,
) {
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
    accountType: "credit_card" as const,
    assetDomain: "cash" as const,
    defaultCurrency: settlementAccount.defaultCurrency,
    openingBalanceOriginal: null,
    openingBalanceCurrency: null,
    openingBalanceDate: null,
    includeInConsolidation: true,
    isActive: true,
    importTemplateDefaultId: null,
    matchingAliases: [],
    accountSuffix: null,
    balanceMode: "computed" as const,
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
        logImportDebug("upload:new-spreadsheet-selected", {
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
        logImportDebug("upload:template-created", {
          accountId: fields.accountId,
          templateId: resolvedTemplateId,
          templateName: resolvedTemplateName,
        });
      }

      const result = await run({
        accountId: fields.accountId,
        templateId: resolvedTemplateId,
        originalFilename,
        filePath,
      });
      logImportDebug("upload:run-complete", {
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

export async function commitCreditCardStatementImportUpload(
  formData: FormData,
) {
  const fields = creditCardStatementFieldsSchema.parse({
    settlementTransactionId: formData.get("settlementTransactionId"),
    templateId: formData.get("templateId"),
  });

  return withUploadedImportFile(formData, {
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
            revalidateTemplatePaths();
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
        revalidateTemplatePaths();
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
}

export {
  buildCreditCardPdfTemplate,
  findReusableCreditCardPdfTemplate,
  withUploadedImport,
  withUploadedImportFile,
};

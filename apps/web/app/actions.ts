"use server";

import { Decimal } from "decimal.js";
import { z } from "zod";

import {
  deactivateLearnedReviewExample,
  getDbRuntimeConfig,
  refreshOwnedStockPrices,
  updatePromptProfile,
} from "@myfinance/db";
import {
  buildWorkspaceSettingsJson,
  createTemplateConfig,
} from "@myfinance/domain";
import { logImportDebug } from "@myfinance/ingestion";
import { domain, repository } from "../lib/action-service";
import {
  accountSchema,
  accountUpdateSchema,
  categorySchema,
  createManualInvestmentSchema,
  entitySchema,
  entityUpdateSchema,
  manualInvestmentValuationSchema,
  promptProfileUpdateSchema,
  revolutLowRiskFundReturnSchema,
  templateSchema,
  updateManualInvestmentSchema,
  workspaceProfileSchema,
} from "../lib/action-schemas";
import {
  commitCreditCardStatementImportUpload,
  toAccountPatch,
  toAssetDomain,
  withUploadedImport,
} from "../lib/action-plumbing";
import {
  revalidateAccountsPath,
  revalidateImportPaths,
  revalidatePromptPaths,
  revalidateRulesPaths,
  revalidateTemplatePaths,
  revalidateWorkspacePaths,
} from "../lib/api-revalidate";
import {
  REVOLUT_LOW_RISK_FUND_LABEL,
  REVOLUT_LOW_RISK_FUND_MATCHER_TEXT,
  REVOLUT_LOW_RISK_FUND_NOTE,
  resolveDiscoveredRevolutLowRiskFund,
} from "../lib/discovered-revolut-investment";

const uuidSchema = z.string().uuid();
const webActor = { actorName: "web-action", sourceChannel: "web" as const };
const webCommand = { ...webActor, apply: true as const };

function command<T extends object>(input: T) {
  return { ...input, ...webCommand };
}

async function revalidated<T>(
  resultPromise: Promise<T>,
  ...revalidators: Array<() => void>
) {
  const result = await resultPromise;
  revalidators.forEach((revalidate) => revalidate());
  return result;
}

export async function previewImportAction(formData: FormData) {
  return withUploadedImport("preview", formData, async (input) => {
    logImportDebug("preview-action:start", {
      accountId: input.accountId,
      templateId: input.templateId,
      originalFilename: input.originalFilename,
    });
    return domain.previewImport(input);
  });
}

export async function commitImportAction(formData: FormData) {
  logImportDebug("commit-action:start");
  const result = await withUploadedImport("commit", formData, (input) =>
    domain.commitImport(input),
  );
  logImportDebug("commit-action:complete", {
    accountId: result.accountId,
    templateId: result.templateId,
    importBatchId: "importBatchId" in result ? result.importBatchId : null,
    rowCountParsed: result.rowCountParsed,
    rowCountFailed: result.rowCountFailed,
  });
  revalidateImportPaths();
  return result;
}

export async function commitCreditCardStatementImportAction(
  formData: FormData,
) {
  return revalidated(
    commitCreditCardStatementImportUpload(formData),
    revalidateWorkspacePaths,
  );
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
  return revalidated(
    domain.createTemplate(
      command({
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
      }),
    ),
    revalidateTemplatePaths,
  );
}

export async function deleteTemplateAction(templateId: string) {
  return revalidated(
    domain.deleteTemplate(
      command({ templateId: uuidSchema.parse(templateId) }),
    ),
    revalidateTemplatePaths,
    revalidateAccountsPath,
  );
}

export async function createCategoryAction(
  input: z.input<typeof categorySchema>,
) {
  return revalidated(
    domain.createCategory(command({ category: categorySchema.parse(input) })),
    revalidateWorkspacePaths,
  );
}

export async function deleteCategoryAction(categoryCode: string) {
  return revalidated(
    domain.deleteCategory(
      command({ categoryCode: z.string().trim().min(1).parse(categoryCode) }),
    ),
    revalidateWorkspacePaths,
  );
}

export async function createAccountAction(
  input: z.input<typeof accountSchema>,
) {
  const account = accountSchema.parse(input);
  const patch = toAccountPatch(account);
  return revalidated(
    domain.createAccount(
      command({
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
      }),
    ),
    revalidateWorkspacePaths,
  );
}

export async function updateAccountAction(
  input: z.input<typeof accountUpdateSchema>,
) {
  const account = accountUpdateSchema.parse(input);
  return revalidated(
    domain.updateAccount(
      command({
        accountId: account.accountId,
        patch: toAccountPatch(account),
      }),
    ),
    revalidateWorkspacePaths,
  );
}

export async function updateWorkspaceProfileAction(
  input: z.input<typeof workspaceProfileSchema>,
) {
  const profile = workspaceProfileSchema.parse(input);
  return revalidated(
    domain.updateWorkspaceProfile(
      command({
        profile: {
          displayName: profile.displayName,
          defaultBaseCurrency: profile.defaultBaseCurrency,
          timezone: profile.timezone,
          workspaceSettingsJson: buildWorkspaceSettingsJson(profile),
        },
      }),
    ),
    revalidateWorkspacePaths,
  );
}

export async function deleteLearnedReviewExampleAction(
  learnedReviewExampleId: string,
) {
  const result = await revalidated(
    deactivateLearnedReviewExample({
      learnedReviewExampleId: uuidSchema.parse(learnedReviewExampleId),
    }),
    revalidatePromptPaths,
  );
  return {
    learnedReviewExampleId: result.id,
    message: "Learned example removed from future prompt assembly.",
  };
}

export async function createEntityAction(input: z.input<typeof entitySchema>) {
  return revalidated(
    domain.createEntity(command({ entity: entitySchema.parse(input) })),
    revalidateWorkspacePaths,
  );
}

export async function updateEntityAction(
  input: z.input<typeof entityUpdateSchema>,
) {
  const { entityId, ...patch } = entityUpdateSchema.parse(input);
  return revalidated(
    domain.updateEntity(command({ entityId, patch })),
    revalidateWorkspacePaths,
  );
}

export async function deleteEntityAction(entityId: string) {
  return revalidated(
    domain.deleteEntity(command({ entityId: uuidSchema.parse(entityId) })),
    revalidateWorkspacePaths,
  );
}

export async function createManualInvestmentAction(
  input: z.input<typeof createManualInvestmentSchema>,
) {
  return revalidated(
    domain.createManualInvestment(
      command(createManualInvestmentSchema.parse(input)),
    ),
    revalidateWorkspacePaths,
  );
}

export async function updateManualInvestmentAction(
  input: z.input<typeof updateManualInvestmentSchema>,
) {
  return revalidated(
    domain.updateManualInvestment(
      command(updateManualInvestmentSchema.parse(input)),
    ),
    revalidateWorkspacePaths,
  );
}

export async function recordManualInvestmentValuationAction(
  input: z.input<typeof manualInvestmentValuationSchema>,
) {
  return revalidated(
    domain.recordManualInvestmentValuation(
      command(manualInvestmentValuationSchema.parse(input)),
    ),
    revalidateWorkspacePaths,
  );
}

export async function recordRevolutLowRiskFundReturnAction(
  input: z.input<typeof revolutLowRiskFundReturnSchema>,
) {
  const parsed = revolutLowRiskFundReturnSchema.parse(input);
  const dataset = await repository.getDataset();
  const discovered = resolveDiscoveredRevolutLowRiskFund(
    dataset,
    parsed.snapshotDate,
  );
  if (!discovered) {
    throw new Error("No Revolut low-risk fund transfers were found.");
  }

  const currentValueOriginal = new Decimal(discovered.principalOriginal)
    .plus(parsed.returnOriginal)
    .toFixed(2);
  if (new Decimal(currentValueOriginal).lt(0)) {
    throw new Error("Return cannot reduce the fund value below zero.");
  }

  if (discovered.manualInvestmentId) {
    return revalidated(
      domain.recordManualInvestmentValuation(
        command({
          manualInvestmentId: discovered.manualInvestmentId,
          snapshotDate: parsed.snapshotDate,
          currentValueOriginal,
          currentValueCurrency: discovered.principalCurrency,
          note: "User-entered return for the auto-discovered Revolut low-risk fund.",
        }),
      ),
      revalidateWorkspacePaths,
    );
  }

  return revalidated(
    domain.createManualInvestment(
      command({
        entityId: discovered.entityId,
        fundingAccountId: discovered.fundingAccountId,
        label: REVOLUT_LOW_RISK_FUND_LABEL,
        matcherText: REVOLUT_LOW_RISK_FUND_MATCHER_TEXT,
        note: REVOLUT_LOW_RISK_FUND_NOTE,
        snapshotDate: parsed.snapshotDate,
        currentValueOriginal,
        currentValueCurrency: discovered.principalCurrency,
        valuationNote:
          "User-entered return for the auto-discovered Revolut low-risk fund.",
      }),
    ),
    revalidateWorkspacePaths,
  );
}

export async function deleteManualInvestmentAction(manualInvestmentId: string) {
  return revalidated(
    domain.deleteManualInvestment(
      command({ manualInvestmentId: uuidSchema.parse(manualInvestmentId) }),
    ),
    revalidateWorkspacePaths,
  );
}

export async function deleteAccountAction(accountId: string) {
  return revalidated(
    domain.deleteAccount(command({ accountId: uuidSchema.parse(accountId) })),
    revalidateImportPaths,
  );
}

export async function resetWorkspaceAction() {
  return revalidated(
    domain.resetWorkspace(webCommand),
    revalidateWorkspacePaths,
  );
}

export async function refreshOwnedStockPricesAction() {
  return revalidated(refreshOwnedStockPrices(), revalidateWorkspacePaths);
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
  const profile = await revalidated(
    updatePromptProfile({
      promptId: fields.promptId,
      sections: parsedSections,
      ...webActor,
    }),
    revalidatePromptPaths,
  );
  return {
    profile,
    message: `Saved prompt overrides for ${fields.promptId}.`,
  };
}

export async function queueRuleDraftAction(requestText: string) {
  const parsed = z.string().min(8).parse(requestText);
  return revalidated(
    domain.queueRuleDraft(command({ requestText: parsed })),
    revalidateRulesPaths,
  );
}

export async function applyRuleDraftAction(jobId: string) {
  const parsed = z.string().min(1).parse(jobId);
  return revalidated(
    domain.applyRuleDraft(command({ jobId: parsed })),
    revalidateRulesPaths,
  );
}

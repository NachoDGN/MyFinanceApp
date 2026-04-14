"use server";

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
  logImportDebug,
} from "@myfinance/domain";
import { domain } from "../lib/action-service";
import {
  accountSchema,
  accountUpdateSchema,
  createManualInvestmentSchema,
  entitySchema,
  entityUpdateSchema,
  manualInvestmentValuationSchema,
  promptProfileUpdateSchema,
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
  const result = await commitCreditCardStatementImportUpload(formData);

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
  revalidateTemplatePaths();
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
  revalidateTemplatePaths();
  revalidateAccountsPath();
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
      workspaceSettingsJson: buildWorkspaceSettingsJson(profile),
    },
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidateWorkspacePaths();
  return result;
}

export async function deleteLearnedReviewExampleAction(
  learnedReviewExampleId: string,
) {
  const parsedId = z.string().uuid().parse(learnedReviewExampleId);
  const result = await deactivateLearnedReviewExample({
    learnedReviewExampleId: parsedId,
  });
  revalidatePromptPaths();
  return {
    learnedReviewExampleId: result.id,
    message: "Learned example removed from future prompt assembly.",
  };
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

export async function updateManualInvestmentAction(
  input: z.input<typeof updateManualInvestmentSchema>,
) {
  const trackedInvestment = updateManualInvestmentSchema.parse(input);
  const result = await domain.updateManualInvestment({
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
  revalidateImportPaths();
  return result;
}

export async function resetWorkspaceAction() {
  const result = await domain.resetWorkspace({
    actorName: "web-action",
    sourceChannel: "web",
    apply: true,
  });
  revalidateWorkspacePaths();
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

  revalidatePromptPaths();
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
  revalidateRulesPaths();
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
  revalidateRulesPaths();
  return result;
}

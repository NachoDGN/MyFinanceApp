"use server";

import { randomUUID } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { revalidatePath } from "next/cache";
import { z } from "zod";

import { createFinanceRepository, getDbRuntimeConfig } from "@myfinance/db";
import {
  canonicalFieldKeys,
  createTemplateConfig,
  FinanceDomainService,
  signModeOptions,
} from "@myfinance/domain";

const domain = new FinanceDomainService(createFinanceRepository());

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

async function withUploadedImport(
  formData: FormData,
  run: (input: {
    accountId: string;
    templateId: string;
    originalFilename: string;
    filePath: string;
  }) => Promise<unknown>,
) {
  const fields = importFieldsSchema.parse({
    accountId: formData.get("accountId"),
    templateId: formData.get("templateId"),
  });
  const file = formData.get("file");
  if (!file || typeof file !== "object" || typeof (file as File).arrayBuffer !== "function") {
    throw new Error("A file upload is required.");
  }

  const uploadDirectory = join(tmpdir(), "myfinance-imports", randomUUID());
  await mkdir(uploadDirectory, { recursive: true });
  const filePath = join(uploadDirectory, file.name || "upload.bin");
  await writeFile(filePath, Buffer.from(await file.arrayBuffer()));

  try {
    return await run({
      accountId: fields.accountId,
      templateId: fields.templateId,
      originalFilename: file.name,
      filePath,
    });
  } finally {
    await rm(uploadDirectory, { recursive: true, force: true });
  }
}

export async function previewImportAction(formData: FormData) {
  return withUploadedImport(formData, (input) => domain.previewImport(input));
}

export async function commitImportAction(formData: FormData) {
  const result = await withUploadedImport(formData, (input) => domain.commitImport(input));
  revalidatePath("/imports");
  revalidatePath("/");
  revalidatePath("/transactions");
  revalidatePath("/accounts");
  revalidatePath("/spending");
  revalidatePath("/income");
  revalidatePath("/investments");
  return result;
}

export async function createTemplateAction(input: z.input<typeof templateSchema>) {
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
  const { columnMapJson, signLogicJson, normalizationRulesJson } = createTemplateConfig({
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

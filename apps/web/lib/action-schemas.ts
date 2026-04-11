import { z } from "zod";

import { accountTypeOptions, canonicalFieldKeys, signModeOptions } from "@myfinance/domain";

function isSupportedTimeZone(value: string) {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: value });
    return true;
  } catch {
    return false;
  }
}

const entitySlugPattern = /^[a-z0-9]+(?:[-_][a-z0-9]+)*$/;

export const importFieldsSchema = z.object({
  accountId: z.string(),
  templateId: z.string(),
});

export const creditCardStatementFieldsSchema = z.object({
  settlementTransactionId: z.string().uuid(),
  templateId: z.string(),
});

export const columnMappingSchema = z.object({
  source: z.string().default(""),
  target: z.enum(canonicalFieldKeys),
});

export const templateSchema = z.object({
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

export const nullableDayCountSchema = z.preprocess((value) => {
  if (value === "" || value === null || value === undefined) {
    return null;
  }
  return value;
}, z.coerce.number().int().min(1).max(365).nullable());

export const isoDateSchema = z
  .string()
  .trim()
  .regex(/^\d{4}-\d{2}-\d{2}$/, "Use a date in YYYY-MM-DD format.");

export const currencyCodeSchema = z
  .string()
  .trim()
  .min(3)
  .max(3)
  .transform((value) => value.toUpperCase());

export const nonNegativeAmountStringSchema = z.preprocess(
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

export const accountFieldsSchema = z.object({
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

export const accountSchema = accountFieldsSchema.extend({
  entityId: z.string().uuid(),
  accountType: z.enum(accountTypeOptions),
});

export const accountUpdateSchema = accountFieldsSchema.extend({
  accountId: z.string().uuid(),
});

export const promptProfileUpdateSchema = z.object({
  promptId: z.enum([
    "cash_transaction_analyzer",
    "investment_transaction_analyzer",
    "spreadsheet_table_start",
    "spreadsheet_layout",
    "rule_draft_parser",
  ]),
  sectionsJson: z.string().min(2),
});

export const workspaceProfileSchema = z.object({
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

export const entitySchema = z.object({
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

export const entityUpdateSchema = entitySchema
  .pick({
    slug: true,
    displayName: true,
    legalName: true,
    baseCurrency: true,
  })
  .extend({
    entityId: z.string().uuid(),
  });

export const manualInvestmentMatcherSchema = z
  .string()
  .trim()
  .min(2)
  .refine(
    (value) => value.split(/[\n,]+/).some((term) => term.trim().length > 0),
    "Provide at least one matcher term.",
  );

export const createManualInvestmentSchema = z.object({
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

export const updateManualInvestmentSchema = z.object({
  manualInvestmentId: z.string().uuid(),
  fundingAccountId: z.string().uuid(),
  label: z.string().trim().min(1),
  matcherText: manualInvestmentMatcherSchema,
  note: z
    .string()
    .trim()
    .optional()
    .transform((value) => value || null),
});

export const manualInvestmentValuationSchema = z.object({
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

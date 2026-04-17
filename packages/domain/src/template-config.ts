import type { AccountType, FileKind } from "./types";

export const accountTypeOptions = [
  "checking",
  "savings",
  "company_bank",
  "brokerage_cash",
  "brokerage_account",
  "credit_card",
  "other",
] as const satisfies readonly AccountType[];

export const fileKindOptions = [
  "csv",
  "xlsx",
  "xls",
] as const satisfies readonly FileKind[];

export function isWorkbookFileKind(
  fileKind: FileKind,
): fileKind is "xlsx" | "xls" {
  return fileKind === "xlsx" || fileKind === "xls";
}

export const signModeOptions = [
  "signed_amount",
  "amount_direction_column",
  "debit_credit_columns",
] as const;

export type TemplateSignMode = (typeof signModeOptions)[number];

export const canonicalFieldKeys = [
  "transaction_date",
  "posted_date",
  "description_raw",
  "amount_original_signed",
  "currency_original",
  "balance_original",
  "external_reference",
  "transaction_type_raw",
  "security_isin",
  "security_symbol",
  "security_name",
  "quantity",
  "unit_price_original",
  "fees_original",
  "fx_rate",
] as const;

export type CanonicalFieldKey = (typeof canonicalFieldKeys)[number];

export type TemplateColumnMapping = {
  source: string;
  target: CanonicalFieldKey;
};

export const canonicalFieldOptions = [
  {
    key: "transaction_date",
    label: "Transaction date",
    detail: "Required booking date",
    required: true,
  },
  {
    key: "posted_date",
    label: "Posted date",
    detail: "Optional bank-posted date",
  },
  {
    key: "description_raw",
    label: "Description",
    detail: "Human-readable transaction text",
  },
  {
    key: "amount_original_signed",
    label: "Signed amount",
    detail: "Required unless you use separate debit and credit columns",
  },
  {
    key: "currency_original",
    label: "Currency",
    detail: "Original transaction currency",
  },
  {
    key: "balance_original",
    label: "Balance",
    detail: "Running account balance after the movement",
  },
  {
    key: "external_reference",
    label: "Reference",
    detail: "Transfer id, trade id, or provider reference",
  },
  {
    key: "transaction_type_raw",
    label: "Transaction type",
    detail: "Provider-specific type code or label",
  },
  {
    key: "security_isin",
    label: "Security ISIN",
    detail: "Exact instrument ISIN when the file includes one",
  },
  {
    key: "security_symbol",
    label: "Security symbol",
    detail: "Ticker or instrument symbol",
  },
  {
    key: "security_name",
    label: "Security name",
    detail: "Instrument name when no symbol is present",
  },
  {
    key: "quantity",
    label: "Quantity",
    detail: "Share or unit count for investments",
  },
  {
    key: "unit_price_original",
    label: "Unit price",
    detail: "Per-unit price for trades",
  },
  {
    key: "fees_original",
    label: "Fees",
    detail: "Commission or transaction fee",
  },
  {
    key: "fx_rate",
    label: "FX rate",
    detail: "Optional conversion rate from the statement",
  },
] as const satisfies readonly {
  key: CanonicalFieldKey;
  label: string;
  detail: string;
  required?: boolean;
}[];

const fieldLabelByKey = Object.fromEntries(
  canonicalFieldOptions.map((field) => [field.key, field.label]),
) as Record<CanonicalFieldKey, string>;

export function createDefaultColumnMappings(): TemplateColumnMapping[] {
  return [
    { source: "Date", target: "transaction_date" },
    { source: "Description", target: "description_raw" },
    { source: "Amount", target: "amount_original_signed" },
    { source: "Currency", target: "currency_original" },
    { source: "Reference", target: "external_reference" },
    { source: "Balance", target: "balance_original" },
  ];
}

export function isCanonicalFieldKey(value: string): value is CanonicalFieldKey {
  return (canonicalFieldKeys as readonly string[]).includes(value);
}

function buildColumnMap(mappings: TemplateColumnMapping[]) {
  const columnMap: Partial<Record<CanonicalFieldKey, string>> = {};

  for (const mapping of mappings) {
    const source = mapping.source.trim();
    if (!source) continue;
    if (columnMap[mapping.target]) {
      throw new Error(
        `${fieldLabelByKey[mapping.target]} is mapped more than once.`,
      );
    }
    columnMap[mapping.target] = source;
  }

  if (!columnMap.transaction_date) {
    throw new Error("Transaction date mapping is required.");
  }

  return columnMap as Record<string, string>;
}

function splitValues(value: string | null | undefined) {
  return String(value ?? "")
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function buildSignLogic(input: {
  signMode: TemplateSignMode;
  invertSign: boolean;
  directionColumn?: string | null;
  debitColumn?: string | null;
  creditColumn?: string | null;
  debitValuesText?: string | null;
  creditValuesText?: string | null;
  columnMap: Record<string, string>;
}) {
  if (input.signMode === "debit_credit_columns") {
    const debitColumn = input.debitColumn?.trim();
    const creditColumn = input.creditColumn?.trim();
    if (!debitColumn && !creditColumn) {
      throw new Error("Provide at least one debit or credit column.");
    }
    return {
      mode: input.signMode,
      ...(debitColumn ? { debit_column: debitColumn } : {}),
      ...(creditColumn ? { credit_column: creditColumn } : {}),
    };
  }

  if (!input.columnMap.amount_original_signed) {
    throw new Error("Signed amount mapping is required for this sign mode.");
  }

  if (input.signMode === "amount_direction_column") {
    const directionColumn = input.directionColumn?.trim();
    if (!directionColumn) {
      throw new Error(
        "Direction column is required when sign is encoded separately.",
      );
    }
    const debitValues = splitValues(input.debitValuesText);
    const creditValues = splitValues(input.creditValuesText);

    return {
      mode: input.signMode,
      direction_column: directionColumn,
      ...(debitValues.length > 0 ? { debit_values: debitValues } : {}),
      ...(creditValues.length > 0 ? { credit_values: creditValues } : {}),
    };
  }

  return {
    mode: input.signMode,
    ...(input.invertSign ? { invert_sign: true } : {}),
  };
}

export function createTemplateConfig(input: {
  columnMappings: TemplateColumnMapping[];
  signMode: TemplateSignMode;
  invertSign?: boolean;
  directionColumn?: string | null;
  debitColumn?: string | null;
  creditColumn?: string | null;
  debitValuesText?: string | null;
  creditValuesText?: string | null;
  dateDayFirst?: boolean;
}) {
  const columnMapJson = buildColumnMap(input.columnMappings);

  return {
    columnMapJson,
    signLogicJson: buildSignLogic({
      signMode: input.signMode,
      invertSign: Boolean(input.invertSign),
      directionColumn: input.directionColumn,
      debitColumn: input.debitColumn,
      creditColumn: input.creditColumn,
      debitValuesText: input.debitValuesText,
      creditValuesText: input.creditValuesText,
      columnMap: columnMapJson,
    }),
    normalizationRulesJson: {
      date_day_first: input.dateDayFirst ?? true,
    },
  };
}

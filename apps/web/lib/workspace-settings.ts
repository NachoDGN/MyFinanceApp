import type { Entity } from "@myfinance/domain";

export const supportedDisplayCurrencies = ["EUR", "USD"] as const;
export const supportedPeriodPresets = ["mtd", "ytd"] as const;

export type SupportedDisplayCurrency =
  (typeof supportedDisplayCurrencies)[number];
export type SupportedPeriodPreset = (typeof supportedPeriodPresets)[number];

export type WorkspaceSettings = {
  preferredScope: string;
  defaultDisplayCurrency: SupportedDisplayCurrency;
  defaultPeriodPreset: SupportedPeriodPreset;
  defaultCashStaleAfterDays: number;
  defaultInvestmentStaleAfterDays: number;
};

export const DEFAULT_WORKSPACE_SETTINGS: WorkspaceSettings = {
  preferredScope: "consolidated",
  defaultDisplayCurrency: "EUR",
  defaultPeriodPreset: "mtd",
  defaultCashStaleAfterDays: 7,
  defaultInvestmentStaleAfterDays: 3,
};

function readPositiveInteger(
  value: unknown,
  fallback: number,
  minimum = 1,
  maximum = 365,
) {
  if (typeof value === "number" && Number.isInteger(value)) {
    return Math.min(maximum, Math.max(minimum, value));
  }
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isInteger(parsed)) {
      return Math.min(maximum, Math.max(minimum, parsed));
    }
  }
  return fallback;
}

function readSupportedCurrency(
  value: unknown,
  fallback: SupportedDisplayCurrency,
): SupportedDisplayCurrency {
  return value === "USD" ? "USD" : fallback;
}

function readSupportedPeriod(
  value: unknown,
  fallback: SupportedPeriodPreset,
): SupportedPeriodPreset {
  return value === "ytd" ? "ytd" : fallback;
}

export function resolvePreferredScope(
  preferredScope: unknown,
  entities: Entity[],
): string {
  if (preferredScope === "consolidated") {
    return "consolidated";
  }

  if (
    typeof preferredScope === "string" &&
    entities.some((entity) => entity.slug === preferredScope)
  ) {
    return preferredScope;
  }

  return "consolidated";
}

export function parseWorkspaceSettings(
  value: unknown,
  options: {
    entities: Entity[];
    profileDefaultBaseCurrency?: string | null;
  },
): WorkspaceSettings {
  const record =
    value && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : {};
  const defaultDisplayCurrency =
    options.profileDefaultBaseCurrency === "USD" ? "USD" : "EUR";

  return {
    preferredScope: resolvePreferredScope(record.preferredScope, options.entities),
    defaultDisplayCurrency: readSupportedCurrency(
      record.defaultDisplayCurrency,
      defaultDisplayCurrency,
    ),
    defaultPeriodPreset: readSupportedPeriod(
      record.defaultPeriodPreset,
      DEFAULT_WORKSPACE_SETTINGS.defaultPeriodPreset,
    ),
    defaultCashStaleAfterDays: readPositiveInteger(
      record.defaultCashStaleAfterDays,
      DEFAULT_WORKSPACE_SETTINGS.defaultCashStaleAfterDays,
    ),
    defaultInvestmentStaleAfterDays: readPositiveInteger(
      record.defaultInvestmentStaleAfterDays,
      DEFAULT_WORKSPACE_SETTINGS.defaultInvestmentStaleAfterDays,
    ),
  };
}

export function buildEntityScopeOptions(entities: Entity[]) {
  return [
    { value: "consolidated", label: "Consolidated" },
    ...entities.map((entity) => ({
      value: entity.slug,
      label: entity.displayName,
    })),
  ];
}

export function describePeriodPreset(preset: SupportedPeriodPreset) {
  return preset === "ytd" ? "Year to Date" : "Month to Date";
}

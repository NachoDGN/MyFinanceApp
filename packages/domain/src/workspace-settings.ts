import type { AssetDomain, Entity, Profile } from "./types";

export const supportedDisplayCurrencies = ["EUR", "USD"] as const;
export const supportedPeriodPresets = ["mtd", "ytd", "all"] as const;

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
  if (value === "all") {
    return "all";
  }
  return value === "ytd" ? "ytd" : fallback;
}

export function resolvePreferredScope(
  preferredScope: unknown,
  entities: Pick<Entity, "slug">[],
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
    entities?: Pick<Entity, "slug">[];
    profileDefaultBaseCurrency?: string | null;
  } = {},
): WorkspaceSettings {
  const record =
    value && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : {};
  const defaultDisplayCurrency =
    options.profileDefaultBaseCurrency === "USD" ? "USD" : "EUR";

  return {
    preferredScope: resolvePreferredScope(
      record.preferredScope,
      options.entities ?? [],
    ),
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

export function resolveWorkspaceSettings(
  profile: Pick<Profile, "defaultBaseCurrency" | "workspaceSettingsJson">,
  options: {
    entities?: Pick<Entity, "slug">[];
  } = {},
) {
  return parseWorkspaceSettings(profile.workspaceSettingsJson, {
    entities: options.entities,
    profileDefaultBaseCurrency: profile.defaultBaseCurrency,
  });
}

export function resolveAccountStaleThresholdDays(
  profile: Pick<Profile, "defaultBaseCurrency" | "workspaceSettingsJson">,
  assetDomain: AssetDomain,
  accountStaleAfterDays?: number | null,
  options: {
    entities?: Pick<Entity, "slug">[];
  } = {},
) {
  if (
    typeof accountStaleAfterDays === "number" &&
    Number.isFinite(accountStaleAfterDays) &&
    accountStaleAfterDays >= 1
  ) {
    return Math.round(accountStaleAfterDays);
  }

  const settings = resolveWorkspaceSettings(profile, options);
  return assetDomain === "investment"
    ? settings.defaultInvestmentStaleAfterDays
    : settings.defaultCashStaleAfterDays;
}

export function buildWorkspaceSettingsJson(
  settings: Pick<
    WorkspaceSettings,
    | "preferredScope"
    | "defaultDisplayCurrency"
    | "defaultPeriodPreset"
    | "defaultCashStaleAfterDays"
    | "defaultInvestmentStaleAfterDays"
  >,
) {
  return {
    preferredScope: settings.preferredScope,
    defaultDisplayCurrency: settings.defaultDisplayCurrency,
    defaultPeriodPreset: settings.defaultPeriodPreset,
    defaultCashStaleAfterDays: settings.defaultCashStaleAfterDays,
    defaultInvestmentStaleAfterDays: settings.defaultInvestmentStaleAfterDays,
  } satisfies Profile["workspaceSettingsJson"];
}

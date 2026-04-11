import type { Entity, SupportedPeriodPreset } from "@myfinance/domain";

export {
  buildWorkspaceSettingsJson,
  DEFAULT_WORKSPACE_SETTINGS,
  parseWorkspaceSettings,
  resolvePreferredScope,
  resolveWorkspaceSettings,
  supportedDisplayCurrencies,
  supportedPeriodPresets,
  type SupportedDisplayCurrency,
  type SupportedPeriodPreset,
  type WorkspaceSettings,
} from "@myfinance/domain";

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

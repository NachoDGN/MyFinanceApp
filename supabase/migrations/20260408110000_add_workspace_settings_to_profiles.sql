alter table public.profiles
  add column if not exists workspace_settings_json jsonb;

update public.profiles
set workspace_settings_json = coalesce(workspace_settings_json, '{}'::jsonb) || '{
  "preferredScope": "consolidated",
  "defaultDisplayCurrency": "EUR",
  "defaultPeriodPreset": "mtd",
  "defaultCashStaleAfterDays": 7,
  "defaultInvestmentStaleAfterDays": 3
}'::jsonb
where workspace_settings_json is null
   or workspace_settings_json = '{}'::jsonb;

alter table public.profiles
  alter column workspace_settings_json
  set default '{
    "preferredScope": "consolidated",
    "defaultDisplayCurrency": "EUR",
    "defaultPeriodPreset": "mtd",
    "defaultCashStaleAfterDays": 7,
    "defaultInvestmentStaleAfterDays": 3
  }'::jsonb;

alter table public.profiles
  alter column workspace_settings_json
  set not null;

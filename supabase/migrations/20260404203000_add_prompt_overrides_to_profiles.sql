alter table public.profiles
  add column if not exists prompt_overrides_json jsonb not null default '{}'::jsonb;

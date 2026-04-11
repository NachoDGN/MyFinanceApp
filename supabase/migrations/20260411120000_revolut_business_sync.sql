alter type public.job_type add value if not exists 'bank_sync';

create table if not exists public.bank_connections (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  entity_id uuid not null references public.entities(id) on delete cascade,
  provider text not null,
  connection_label text not null,
  status text not null default 'active',
  encrypted_refresh_token text not null,
  external_business_id text,
  last_cursor_created_at timestamptz,
  last_successful_sync_at timestamptz,
  last_sync_queued_at timestamptz,
  last_webhook_at timestamptz,
  auth_expires_at timestamptz,
  last_error text,
  metadata_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'bank_connections_provider_check'
  ) then
    alter table public.bank_connections
      add constraint bank_connections_provider_check
      check (provider in ('revolut_business'));
  end if;

  if not exists (
    select 1
    from pg_constraint
    where conname = 'bank_connections_status_check'
  ) then
    alter table public.bank_connections
      add constraint bank_connections_status_check
      check (status in ('active', 'reauthorization_required', 'error'));
  end if;
end $$;

create unique index if not exists idx_bank_connections_user_provider_entity
  on public.bank_connections(user_id, provider, entity_id);

create table if not exists public.bank_account_links (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  connection_id uuid not null references public.bank_connections(id) on delete cascade,
  account_id uuid not null references public.accounts(id) on delete cascade,
  provider text not null,
  external_account_id text not null,
  external_account_name text not null,
  external_currency text not null,
  last_seen_at timestamptz not null default timezone('utc', now()),
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'bank_account_links_provider_check'
  ) then
    alter table public.bank_account_links
      add constraint bank_account_links_provider_check
      check (provider in ('revolut_business'));
  end if;
end $$;

create unique index if not exists idx_bank_account_links_connection_external_account
  on public.bank_account_links(connection_id, external_account_id);

create unique index if not exists idx_bank_account_links_provider_account
  on public.bank_account_links(provider, account_id);

alter table public.import_batches
  alter column template_id drop not null;

alter table public.import_batches
  add column if not exists source_kind text not null default 'upload',
  add column if not exists provider_name text,
  add column if not exists bank_connection_id uuid references public.bank_connections(id) on delete set null;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'import_batches_source_kind_check'
  ) then
    alter table public.import_batches
      add constraint import_batches_source_kind_check
      check (source_kind in ('upload', 'bank_sync'));
  end if;
end $$;

update public.import_batches
set source_kind = 'upload'
where source_kind is null;

alter table public.transactions
  add column if not exists provider_name text,
  add column if not exists provider_record_id text;

create unique index if not exists idx_transactions_provider_identity
  on public.transactions(user_id, provider_name, provider_record_id)
  where provider_name is not null
    and provider_record_id is not null;

create index if not exists idx_import_batches_bank_connection_id
  on public.import_batches(bank_connection_id);

create index if not exists idx_bank_connections_user_id
  on public.bank_connections(user_id);

create index if not exists idx_bank_account_links_account_id
  on public.bank_account_links(account_id);

alter table public.bank_connections enable row level security;
alter table public.bank_account_links enable row level security;

create policy bank_connections_owner on public.bank_connections
  for all using (user_id = public.app_current_user_id())
  with check (user_id = public.app_current_user_id());

create policy bank_account_links_owner on public.bank_account_links
  for all using (user_id = public.app_current_user_id())
  with check (user_id = public.app_current_user_id());

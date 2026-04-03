create extension if not exists pgcrypto;

create type public.entity_kind as enum ('personal', 'company');
create type public.account_type as enum (
  'checking',
  'savings',
  'company_bank',
  'brokerage_cash',
  'brokerage_account',
  'credit_card',
  'other'
);
create type public.asset_domain as enum ('cash', 'investment');
create type public.file_kind as enum ('csv', 'xlsx');
create type public.import_batch_status as enum ('queued', 'previewed', 'processing', 'committed', 'failed');
create type public.transaction_class as enum (
  'income',
  'expense',
  'transfer_internal',
  'transfer_external',
  'suspected_internal_transfer_pending',
  'investment_trade_buy',
  'investment_trade_sell',
  'dividend',
  'interest',
  'fee',
  'refund',
  'reimbursement',
  'owner_contribution',
  'owner_draw',
  'loan_inflow',
  'loan_principal_payment',
  'loan_interest_payment',
  'fx_conversion',
  'balance_adjustment',
  'unknown'
);
create type public.transfer_match_status as enum ('matched', 'suspected_pending', 'manual', 'not_transfer');
create type public.reimbursement_status as enum ('none', 'expected', 'received', 'linked');
create type public.classification_status as enum (
  'manual_override',
  'rule',
  'transfer_match',
  'investment_parser',
  'llm',
  'unknown'
);
create type public.classification_source as enum (
  'manual',
  'user_rule',
  'transfer_matcher',
  'investment_parser',
  'alias_resolver',
  'llm',
  'system_fallback'
);
create type public.category_scope_kind as enum ('personal', 'company', 'investment', 'both', 'system');
create type public.category_direction_kind as enum ('income', 'expense', 'neutral', 'investment');
create type public.audit_source_channel as enum ('web', 'cli', 'worker', 'system');
create type public.audit_actor_type as enum ('user', 'agent', 'system');
create type public.job_status as enum ('queued', 'running', 'completed', 'failed');
create type public.job_type as enum (
  'classification',
  'transfer_rematch',
  'security_resolution',
  'price_refresh',
  'position_rebuild',
  'metric_refresh',
  'insight_refresh'
);
create type public.balance_source_kind as enum ('statement', 'computed');

create or replace function public.app_current_user_id()
returns uuid
language sql
stable
as $$
  select coalesce(
    auth.uid(),
    nullif(current_setting('app.current_user_id', true), '')::uuid
  );
$$;

create table if not exists public.profiles (
  id uuid primary key references auth.users(id) on delete cascade,
  email text not null,
  display_name text not null,
  default_base_currency text not null default 'EUR',
  timezone text not null default 'Europe/Madrid',
  created_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.entities (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  slug text not null,
  display_name text not null,
  legal_name text,
  entity_kind public.entity_kind not null,
  base_currency text not null default 'EUR',
  active boolean not null default true,
  created_at timestamptz not null default timezone('utc', now()),
  unique (user_id, slug)
);

create table if not exists public.import_templates (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  name text not null,
  institution_name text not null,
  compatible_account_type public.account_type not null,
  file_kind public.file_kind not null,
  sheet_name text,
  header_row_index integer not null default 1,
  rows_to_skip_before_header integer not null default 0,
  rows_to_skip_after_header integer not null default 0,
  delimiter text,
  encoding text,
  decimal_separator text default '.',
  thousands_separator text default ',',
  date_format text not null,
  default_currency text not null default 'EUR',
  column_map_json jsonb not null default '{}'::jsonb,
  sign_logic_json jsonb not null default '{}'::jsonb,
  normalization_rules_json jsonb not null default '{}'::jsonb,
  active boolean not null default true,
  version integer not null default 1,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.accounts (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  entity_id uuid not null references public.entities(id) on delete cascade,
  institution_name text not null,
  display_name text not null,
  account_type public.account_type not null,
  asset_domain public.asset_domain not null,
  default_currency text not null,
  opening_balance_original numeric(20, 8),
  opening_balance_currency text,
  opening_balance_date date,
  include_in_consolidation boolean not null default true,
  is_active boolean not null default true,
  import_template_default_id uuid references public.import_templates(id),
  matching_aliases text[] not null default '{}',
  account_suffix text,
  balance_mode text not null default 'statement',
  stale_after_days integer,
  last_imported_at timestamptz,
  created_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.import_batches (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  account_id uuid not null references public.accounts(id) on delete cascade,
  template_id uuid not null references public.import_templates(id) on delete restrict,
  storage_path text not null,
  original_filename text not null,
  file_sha256 text not null,
  status public.import_batch_status not null default 'previewed',
  row_count_detected integer not null default 0,
  row_count_parsed integer not null default 0,
  row_count_inserted integer not null default 0,
  row_count_duplicates integer not null default 0,
  row_count_failed integer not null default 0,
  preview_summary_json jsonb not null default '{}'::jsonb,
  commit_summary_json jsonb not null default '{}'::jsonb,
  imported_by_actor text not null,
  imported_at timestamptz not null default timezone('utc', now()),
  classification_triggered_at timestamptz,
  notes text
);

create table if not exists public.categories (
  code text primary key,
  display_name text not null,
  parent_code text references public.categories(code),
  scope_kind public.category_scope_kind not null,
  direction_kind public.category_direction_kind not null,
  sort_order integer not null default 0,
  active boolean not null default true,
  metadata_json jsonb not null default '{}'::jsonb
);

create table if not exists public.transactions (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  account_id uuid not null references public.accounts(id) on delete cascade,
  account_entity_id uuid not null references public.entities(id),
  economic_entity_id uuid not null references public.entities(id),
  import_batch_id uuid references public.import_batches(id) on delete set null,
  source_fingerprint text not null,
  duplicate_key text,
  transaction_date date not null,
  posted_date date,
  amount_original numeric(20, 8) not null,
  currency_original text not null,
  amount_base_eur numeric(20, 8) not null,
  fx_rate_to_eur numeric(20, 8),
  description_raw text not null,
  description_clean text not null,
  merchant_normalized text,
  counterparty_name text,
  transaction_class public.transaction_class not null default 'unknown',
  category_code text references public.categories(code),
  subcategory_code text,
  transfer_group_id uuid,
  related_account_id uuid references public.accounts(id),
  related_transaction_id uuid references public.transactions(id),
  transfer_match_status public.transfer_match_status not null default 'not_transfer',
  cross_entity_flag boolean not null default false,
  reimbursement_status public.reimbursement_status not null default 'none',
  classification_status public.classification_status not null default 'unknown',
  classification_source public.classification_source not null default 'system_fallback',
  classification_confidence numeric(5, 4) not null default 0,
  needs_review boolean not null default false,
  review_reason text,
  exclude_from_analytics boolean not null default false,
  correction_of_transaction_id uuid references public.transactions(id),
  voided_at timestamptz,
  manual_notes text,
  llm_payload jsonb,
  raw_payload jsonb not null default '{}'::jsonb,
  security_id uuid,
  quantity numeric(20, 8),
  unit_price_original numeric(20, 8),
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.classification_rules (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  priority integer not null,
  active boolean not null default true,
  scope_json jsonb not null default '{}'::jsonb,
  conditions_json jsonb not null default '{}'::jsonb,
  outputs_json jsonb not null default '{}'::jsonb,
  created_from_transaction_id uuid references public.transactions(id),
  auto_generated boolean not null default false,
  hit_count integer not null default 0,
  last_hit_at timestamptz,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.audit_events (
  id uuid primary key default gen_random_uuid(),
  actor_type public.audit_actor_type not null,
  actor_id uuid,
  actor_name text,
  source_channel public.audit_source_channel not null,
  command_name text not null,
  object_type text not null,
  object_id text not null,
  before_json jsonb,
  after_json jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  notes text
);

create table if not exists public.jobs (
  id uuid primary key default gen_random_uuid(),
  job_type public.job_type not null,
  payload_json jsonb not null default '{}'::jsonb,
  status public.job_status not null default 'queued',
  attempts integer not null default 0,
  available_at timestamptz not null default timezone('utc', now()),
  started_at timestamptz,
  finished_at timestamptz,
  last_error text,
  locked_by text,
  created_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.account_balance_snapshots (
  account_id uuid not null references public.accounts(id) on delete cascade,
  as_of_date date not null,
  balance_original numeric(20, 8) not null,
  balance_currency text not null,
  balance_base_eur numeric(20, 8) not null,
  source_kind public.balance_source_kind not null,
  import_batch_id uuid references public.import_batches(id) on delete set null,
  primary key (account_id, as_of_date)
);

create table if not exists public.securities (
  id uuid primary key default gen_random_uuid(),
  provider_name text not null,
  provider_symbol text not null,
  canonical_symbol text not null,
  display_symbol text not null,
  name text not null,
  exchange_name text not null,
  mic_code text,
  asset_type text not null,
  quote_currency text not null,
  country text,
  isin text,
  figi text,
  active boolean not null default true,
  metadata_json jsonb not null default '{}'::jsonb,
  last_price_refresh_at timestamptz,
  created_at timestamptz not null default timezone('utc', now()),
  unique (provider_name, provider_symbol)
);

alter table public.transactions
  add constraint transactions_security_id_fkey
  foreign key (security_id) references public.securities(id) on delete set null;

create table if not exists public.security_aliases (
  id uuid primary key default gen_random_uuid(),
  security_id uuid not null references public.securities(id) on delete cascade,
  alias_text_normalized text not null,
  alias_source text not null,
  template_id uuid references public.import_templates(id) on delete set null,
  confidence numeric(5, 4) not null default 1,
  created_at timestamptz not null default timezone('utc', now()),
  unique (security_id, alias_text_normalized)
);

create table if not exists public.security_prices (
  security_id uuid not null references public.securities(id) on delete cascade,
  price_date date not null,
  quote_timestamp timestamptz not null,
  price numeric(20, 8) not null,
  currency text not null,
  source_name text not null,
  is_realtime boolean not null default false,
  is_delayed boolean not null default true,
  market_state text,
  raw_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  primary key (security_id, price_date, source_name)
);

create table if not exists public.fx_rates (
  base_currency text not null,
  quote_currency text not null,
  as_of_date date not null,
  as_of_timestamp timestamptz not null,
  rate numeric(20, 8) not null,
  source_name text not null,
  raw_json jsonb not null default '{}'::jsonb,
  primary key (base_currency, quote_currency, as_of_date, source_name)
);

create table if not exists public.holding_adjustments (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  entity_id uuid not null references public.entities(id) on delete cascade,
  account_id uuid not null references public.accounts(id) on delete cascade,
  security_id uuid not null references public.securities(id) on delete cascade,
  effective_date date not null,
  share_delta numeric(20, 8) not null,
  cost_basis_delta_eur numeric(20, 8),
  reason text not null,
  note text,
  created_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.investment_positions (
  user_id uuid not null references public.profiles(id) on delete cascade,
  entity_id uuid not null references public.entities(id) on delete cascade,
  account_id uuid not null references public.accounts(id) on delete cascade,
  security_id uuid not null references public.securities(id) on delete cascade,
  open_quantity numeric(20, 8) not null,
  open_cost_basis_eur numeric(20, 8) not null,
  avg_cost_eur numeric(20, 8) not null,
  realized_pnl_eur numeric(20, 8) not null default 0,
  dividends_eur numeric(20, 8) not null default 0,
  interest_eur numeric(20, 8) not null default 0,
  fees_eur numeric(20, 8) not null default 0,
  last_trade_date date,
  last_rebuilt_at timestamptz not null default timezone('utc', now()),
  provenance_json jsonb not null default '{}'::jsonb,
  primary key (user_id, entity_id, account_id, security_id)
);

create table if not exists public.daily_portfolio_snapshots (
  id uuid primary key default gen_random_uuid(),
  snapshot_date date not null,
  user_id uuid not null references public.profiles(id) on delete cascade,
  entity_id uuid not null references public.entities(id) on delete cascade,
  account_id uuid references public.accounts(id) on delete cascade,
  security_id uuid references public.securities(id) on delete set null,
  market_value_eur numeric(20, 8),
  cost_basis_eur numeric(20, 8),
  unrealized_pnl_eur numeric(20, 8),
  cash_balance_eur numeric(20, 8),
  total_portfolio_value_eur numeric(20, 8) not null,
  generated_at timestamptz not null default timezone('utc', now())
);

create index if not exists idx_entities_user_id on public.entities(user_id);
create index if not exists idx_accounts_user_id on public.accounts(user_id);
create index if not exists idx_accounts_entity_id on public.accounts(entity_id);
create index if not exists idx_import_batches_account_id on public.import_batches(account_id);
create index if not exists idx_transactions_user_date on public.transactions(user_id, transaction_date desc);
create index if not exists idx_transactions_account_id on public.transactions(account_id);
create index if not exists idx_transactions_economic_entity_id on public.transactions(economic_entity_id);
create index if not exists idx_transactions_transfer_group_id on public.transactions(transfer_group_id);
create index if not exists idx_transactions_security_id on public.transactions(security_id);
create index if not exists idx_rules_user_priority on public.classification_rules(user_id, priority);
create index if not exists idx_jobs_status_available on public.jobs(status, available_at);
create index if not exists idx_security_prices_security_date on public.security_prices(security_id, price_date desc);
create index if not exists idx_fx_rates_pair_date on public.fx_rates(base_currency, quote_currency, as_of_date desc);

create or replace view public.v_transaction_analytics as
select
  t.id,
  t.user_id,
  t.account_id,
  t.account_entity_id,
  t.economic_entity_id,
  t.transaction_date,
  t.amount_base_eur,
  t.transaction_class,
  t.category_code,
  t.merchant_normalized,
  t.counterparty_name,
  t.needs_review,
  t.classification_source,
  t.classification_confidence,
  t.transfer_match_status,
  t.cross_entity_flag
from public.transactions t
where t.voided_at is null
  and t.exclude_from_analytics is false;

create or replace view public.v_latest_balance_by_account as
select distinct on (account_id)
  account_id,
  as_of_date,
  balance_original,
  balance_currency,
  balance_base_eur,
  source_kind,
  import_batch_id
from public.account_balance_snapshots
order by account_id, as_of_date desc;

create materialized view if not exists public.mv_monthly_income_totals as
select
  user_id,
  economic_entity_id as entity_id,
  date_trunc('month', transaction_date)::date as month,
  sum(amount_base_eur) as income_total_eur
from public.v_transaction_analytics
where transaction_class in ('income', 'dividend', 'interest')
group by 1, 2, 3;

create materialized view if not exists public.mv_monthly_spending_totals as
select
  user_id,
  economic_entity_id as entity_id,
  date_trunc('month', transaction_date)::date as month,
  category_code,
  sum(case when transaction_class = 'refund' then -amount_base_eur else abs(amount_base_eur) end) as spending_total_eur
from public.v_transaction_analytics
where transaction_class in ('expense', 'fee', 'refund')
group by 1, 2, 3, 4;

create materialized view if not exists public.mv_merchant_rollups_current_month as
select
  user_id,
  economic_entity_id as entity_id,
  merchant_normalized,
  sum(case when transaction_class = 'refund' then -amount_base_eur else abs(amount_base_eur) end) as merchant_total_eur
from public.v_transaction_analytics
where date_trunc('month', transaction_date) = date_trunc('month', timezone('Europe/Madrid', now()))
group by 1, 2, 3;

create materialized view if not exists public.mv_latest_holdings_summary as
with latest_prices as (
  select distinct on (security_id)
    security_id,
    price_date,
    quote_timestamp,
    price,
    currency,
    is_realtime,
    is_delayed
  from public.security_prices
  order by security_id, price_date desc, quote_timestamp desc
)
select
  p.user_id,
  p.entity_id,
  p.account_id,
  p.security_id,
  p.open_quantity,
  p.open_cost_basis_eur,
  p.avg_cost_eur,
  p.realized_pnl_eur,
  p.dividends_eur,
  p.interest_eur,
  p.fees_eur,
  lp.price as current_price,
  lp.currency as current_price_currency,
  case
    when lp.price is null then null
    when lp.currency = 'EUR' then lp.price * p.open_quantity
    else lp.price * p.open_quantity * coalesce((
      select rate from public.fx_rates fx
      where fx.base_currency = lp.currency
        and fx.quote_currency = 'EUR'
      order by as_of_date desc
      limit 1
    ), 1)
  end as current_value_eur,
  lp.quote_timestamp,
  lp.is_realtime,
  lp.is_delayed
from public.investment_positions p
left join latest_prices lp on lp.security_id = p.security_id;

create or replace view public.v_current_portfolio_valuation as
select
  user_id,
  entity_id,
  account_id,
  sum(coalesce(current_value_eur, 0)) as portfolio_market_value_eur,
  sum(open_cost_basis_eur) as portfolio_cost_basis_eur,
  sum(coalesce(current_value_eur, 0) - open_cost_basis_eur) as portfolio_unrealized_pnl_eur
from public.mv_latest_holdings_summary
group by 1, 2, 3;

create materialized view if not exists public.mv_dashboard_current as
with cash_latest as (
  select
    a.user_id,
    a.entity_id,
    sum(v.balance_base_eur) as cash_total_eur
  from public.v_latest_balance_by_account v
  join public.accounts a on a.id = v.account_id
  group by 1, 2
),
portfolio_latest as (
  select
    user_id,
    entity_id,
    sum(portfolio_market_value_eur) as portfolio_market_value_eur,
    sum(portfolio_unrealized_pnl_eur) as portfolio_unrealized_pnl_eur
  from public.v_current_portfolio_valuation
  group by 1, 2
)
select
  coalesce(c.user_id, p.user_id) as user_id,
  coalesce(c.entity_id, p.entity_id) as entity_id,
  coalesce(c.cash_total_eur, 0) as cash_total_eur,
  coalesce(p.portfolio_market_value_eur, 0) as portfolio_market_value_eur,
  coalesce(p.portfolio_unrealized_pnl_eur, 0) as portfolio_unrealized_pnl_eur,
  coalesce(c.cash_total_eur, 0) + coalesce(p.portfolio_market_value_eur, 0) as net_worth_eur
from cash_latest c
full outer join portfolio_latest p
  on p.user_id = c.user_id
 and p.entity_id = c.entity_id;

create or replace function public.refresh_finance_analytics()
returns void
language plpgsql
as $$
begin
  refresh materialized view public.mv_monthly_income_totals;
  refresh materialized view public.mv_monthly_spending_totals;
  refresh materialized view public.mv_merchant_rollups_current_month;
  refresh materialized view public.mv_latest_holdings_summary;
  refresh materialized view public.mv_dashboard_current;
end;
$$;

alter table public.profiles enable row level security;
alter table public.entities enable row level security;
alter table public.import_templates enable row level security;
alter table public.accounts enable row level security;
alter table public.import_batches enable row level security;
alter table public.transactions enable row level security;
alter table public.classification_rules enable row level security;
alter table public.audit_events enable row level security;
alter table public.account_balance_snapshots enable row level security;
alter table public.holding_adjustments enable row level security;
alter table public.investment_positions enable row level security;
alter table public.daily_portfolio_snapshots enable row level security;

create policy profiles_owner on public.profiles
  for all using (id = public.app_current_user_id())
  with check (id = public.app_current_user_id());

create policy entities_owner on public.entities
  for all using (user_id = public.app_current_user_id())
  with check (user_id = public.app_current_user_id());

create policy templates_owner on public.import_templates
  for all using (user_id = public.app_current_user_id())
  with check (user_id = public.app_current_user_id());

create policy accounts_owner on public.accounts
  for all using (user_id = public.app_current_user_id())
  with check (user_id = public.app_current_user_id());

create policy import_batches_owner on public.import_batches
  for all using (user_id = public.app_current_user_id())
  with check (user_id = public.app_current_user_id());

create policy transactions_owner on public.transactions
  for all using (user_id = public.app_current_user_id())
  with check (user_id = public.app_current_user_id());

create policy rules_owner on public.classification_rules
  for all using (user_id = public.app_current_user_id())
  with check (user_id = public.app_current_user_id());

create policy audit_events_select_owner on public.audit_events
  for select using (actor_id = public.app_current_user_id() or actor_id is null);

create policy balance_snapshots_owner on public.account_balance_snapshots
  for select using (
    exists (
      select 1
      from public.accounts a
      where a.id = account_balance_snapshots.account_id
        and a.user_id = public.app_current_user_id()
    )
  );

create policy holding_adjustments_owner on public.holding_adjustments
  for all using (user_id = public.app_current_user_id())
  with check (user_id = public.app_current_user_id());

create policy positions_owner on public.investment_positions
  for select using (user_id = public.app_current_user_id());

create policy snapshots_owner on public.daily_portfolio_snapshots
  for select using (user_id = public.app_current_user_id());

insert into storage.buckets (id, name, public)
values ('private-imports', 'private-imports', false)
on conflict (id) do nothing;

create policy private_imports_select on storage.objects
  for select using (
    bucket_id = 'private-imports'
    and owner = public.app_current_user_id()
  );

create policy private_imports_insert on storage.objects
  for insert with check (
    bucket_id = 'private-imports'
    and owner = public.app_current_user_id()
  );

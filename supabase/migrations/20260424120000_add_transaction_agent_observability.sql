create table if not exists public.transaction_agent_runs (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  question text not null,
  final_answer text,
  status text not null default 'running' check (status in ('running', 'succeeded', 'failed')),
  failure_message text,
  executor_model text not null,
  step_count integer not null default 0 check (step_count >= 0),
  tool_call_count integer not null default 0 check (tool_call_count >= 0),
  citation_ids text[] not null default array[]::text[],
  settings_json jsonb not null default '{}'::jsonb,
  metadata_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  completed_at timestamptz
);

create table if not exists public.transaction_agent_events (
  id bigserial primary key,
  run_id uuid not null references public.transaction_agent_runs(id) on delete cascade,
  step_index integer not null default 0 check (step_index >= 0),
  actor text not null check (actor in ('executor', 'tool', 'system')),
  event_type text not null check (event_type in ('decision', 'tool_call', 'tool_result', 'final_answer', 'error')),
  summary text not null,
  payload jsonb not null default '{}'::jsonb,
  latency_ms integer,
  created_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.transaction_agent_evidence (
  id bigserial primary key,
  run_id uuid not null references public.transaction_agent_runs(id) on delete cascade,
  event_id bigint references public.transaction_agent_events(id) on delete set null,
  evidence_id text not null,
  evidence_type text not null check (evidence_type in ('transaction', 'ledger_query', 'source_batch', 'import_batch', 'audit_event')),
  source_id text,
  title text not null,
  summary text not null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.transaction_agent_feedback (
  id bigserial primary key,
  run_id uuid not null references public.transaction_agent_runs(id) on delete cascade,
  user_id uuid not null references public.profiles(id) on delete cascade,
  label text not null check (label in ('positive', 'negative')),
  comment text,
  issue_tags text[] not null default array[]::text[],
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  unique (run_id, user_id)
);

create index if not exists transaction_agent_runs_user_created_idx
on public.transaction_agent_runs (user_id, created_at desc);

create index if not exists transaction_agent_events_run_step_idx
on public.transaction_agent_events (run_id, step_index, id);

create index if not exists transaction_agent_evidence_run_idx
on public.transaction_agent_evidence (run_id, id);

create index if not exists transaction_agent_feedback_user_idx
on public.transaction_agent_feedback (user_id, created_at desc);

create or replace function public.set_transaction_agent_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at := timezone('utc', now());
  return new;
end;
$$;

drop trigger if exists set_transaction_agent_runs_updated_at
on public.transaction_agent_runs;

create trigger set_transaction_agent_runs_updated_at
before update on public.transaction_agent_runs
for each row
execute function public.set_transaction_agent_updated_at();

drop trigger if exists set_transaction_agent_feedback_updated_at
on public.transaction_agent_feedback;

create trigger set_transaction_agent_feedback_updated_at
before update on public.transaction_agent_feedback
for each row
execute function public.set_transaction_agent_updated_at();

alter table public.transaction_agent_runs enable row level security;
alter table public.transaction_agent_events enable row level security;
alter table public.transaction_agent_evidence enable row level security;
alter table public.transaction_agent_feedback enable row level security;

drop policy if exists transaction_agent_runs_owner on public.transaction_agent_runs;
create policy transaction_agent_runs_owner on public.transaction_agent_runs
  for all using (user_id = public.app_current_user_id())
  with check (user_id = public.app_current_user_id());

drop policy if exists transaction_agent_events_owner on public.transaction_agent_events;
create policy transaction_agent_events_owner on public.transaction_agent_events
  for all using (
    exists (
      select 1
      from public.transaction_agent_runs r
      where r.id = transaction_agent_events.run_id
        and r.user_id = public.app_current_user_id()
    )
  )
  with check (
    exists (
      select 1
      from public.transaction_agent_runs r
      where r.id = transaction_agent_events.run_id
        and r.user_id = public.app_current_user_id()
    )
  );

drop policy if exists transaction_agent_evidence_owner on public.transaction_agent_evidence;
create policy transaction_agent_evidence_owner on public.transaction_agent_evidence
  for all using (
    exists (
      select 1
      from public.transaction_agent_runs r
      where r.id = transaction_agent_evidence.run_id
        and r.user_id = public.app_current_user_id()
    )
  )
  with check (
    exists (
      select 1
      from public.transaction_agent_runs r
      where r.id = transaction_agent_evidence.run_id
        and r.user_id = public.app_current_user_id()
    )
  );

drop policy if exists transaction_agent_feedback_owner on public.transaction_agent_feedback;
create policy transaction_agent_feedback_owner on public.transaction_agent_feedback
  for all using (user_id = public.app_current_user_id())
  with check (user_id = public.app_current_user_id());

create or replace view public.agent_ledger_transactions
with (security_invoker = true)
as
select
  t.id as transaction_id,
  t.user_id,
  t.account_id,
  a.display_name as account_name,
  a.institution_name,
  a.account_type,
  t.account_entity_id,
  ae.display_name as account_entity_name,
  ae.entity_kind as account_entity_kind,
  t.economic_entity_id,
  ee.display_name as economic_entity_name,
  ee.entity_kind as economic_entity_kind,
  t.import_batch_id,
  t.transaction_date,
  t.posted_date,
  t.amount_original,
  t.currency_original,
  t.amount_base_eur,
  t.fx_rate_to_eur,
  t.description_raw,
  t.description_clean,
  t.merchant_normalized,
  t.counterparty_name,
  t.transaction_class,
  t.category_code,
  c.display_name as category_name,
  t.subcategory_code,
  t.transfer_group_id,
  t.related_account_id,
  t.related_transaction_id,
  t.transfer_match_status,
  t.cross_entity_flag,
  t.reimbursement_status,
  t.classification_status,
  t.classification_source,
  t.classification_confidence,
  t.needs_review,
  t.review_reason,
  t.exclude_from_analytics,
  t.correction_of_transaction_id,
  t.voided_at,
  t.manual_notes,
  t.credit_card_statement_status,
  t.created_at,
  t.updated_at
from public.transactions t
join public.accounts a on a.id = t.account_id
join public.entities ae on ae.id = t.account_entity_id
join public.entities ee on ee.id = t.economic_entity_id
left join public.categories c on c.code = t.category_code;

create or replace view public.agent_ledger_search_rows
with (security_invoker = true)
as
select
  r.transaction_id,
  r.user_id,
  r.account_id,
  r.economic_entity_id,
  b.source_batch_key,
  r.transaction_date,
  r.posted_at,
  r.amount,
  r.currency,
  r.merchant,
  r.counterparty,
  r.category,
  r.direction,
  r.review_state,
  r.review_reason,
  r.original_text,
  r.contextualized_text,
  r.document_summary,
  r.embedding_status,
  r.created_at,
  r.updated_at
from public.transaction_search_rows r
join public.transaction_search_batches b on b.id = r.batch_id;

create or replace view public.agent_ledger_accounts
with (security_invoker = true)
as
select
  a.id as account_id,
  a.user_id,
  a.entity_id,
  e.display_name as entity_name,
  e.entity_kind,
  a.institution_name,
  a.display_name as account_name,
  a.account_type,
  a.asset_domain,
  a.default_currency,
  a.include_in_consolidation,
  a.is_active,
  a.account_suffix,
  a.balance_mode,
  a.stale_after_days,
  a.last_imported_at,
  a.created_at
from public.accounts a
join public.entities e on e.id = a.entity_id;

create or replace view public.agent_ledger_entities
with (security_invoker = true)
as
select
  id as entity_id,
  user_id,
  slug,
  display_name,
  legal_name,
  entity_kind,
  base_currency,
  active,
  created_at
from public.entities;

create or replace view public.agent_ledger_categories
with (security_invoker = true)
as
select
  code,
  display_name,
  parent_code,
  scope_kind,
  direction_kind,
  sort_order,
  active
from public.categories;

create or replace view public.agent_ledger_import_batches
with (security_invoker = true)
as
select
  id as import_batch_id,
  user_id,
  account_id,
  source_kind,
  provider_name,
  original_filename,
  status,
  row_count_detected,
  row_count_parsed,
  row_count_inserted,
  row_count_duplicates,
  row_count_failed,
  imported_by_actor,
  imported_at,
  credit_card_settlement_transaction_id,
  statement_net_amount_base_eur,
  notes
from public.import_batches;

create or replace view public.agent_ledger_audit_events
with (security_invoker = true)
as
select
  id as audit_event_id,
  actor_id as user_id,
  actor_type,
  actor_name,
  source_channel,
  command_name,
  object_type,
  object_id,
  notes,
  created_at
from public.audit_events;

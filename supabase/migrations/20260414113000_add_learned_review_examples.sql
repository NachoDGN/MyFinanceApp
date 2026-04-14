create table if not exists public.learned_review_examples (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  account_id uuid not null references public.accounts(id) on delete cascade,
  source_transaction_id uuid not null references public.transactions(id) on delete cascade,
  source_audit_event_id uuid references public.audit_events(id) on delete set null,
  prompt_profile_id text not null check (
    prompt_profile_id in (
      'cash_transaction_analyzer',
      'investment_transaction_analyzer'
    )
  ),
  user_context text not null,
  source_transaction_snapshot_json jsonb not null default '{}'::jsonb,
  initial_inference_snapshot_json jsonb not null default '{}'::jsonb,
  corrected_outcome_snapshot_json jsonb not null default '{}'::jsonb,
  metadata_json jsonb not null default '{}'::jsonb,
  active boolean not null default true,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now()),
  unique (user_id, source_transaction_id)
);

create index if not exists learned_review_examples_user_account_active_idx
on public.learned_review_examples (
  user_id,
  account_id,
  active,
  updated_at desc
);

alter table public.learned_review_examples enable row level security;

create policy learned_review_examples_owner on public.learned_review_examples
  for all using (user_id = public.app_current_user_id());

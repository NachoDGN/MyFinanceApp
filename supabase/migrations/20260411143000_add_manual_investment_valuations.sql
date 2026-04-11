create table if not exists public.manual_investments (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  entity_id uuid not null references public.entities(id) on delete cascade,
  funding_account_id uuid not null references public.accounts(id) on delete cascade,
  label text not null,
  matcher_text text not null,
  note text,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.manual_investment_valuations (
  id uuid primary key default gen_random_uuid(),
  user_id uuid not null references public.profiles(id) on delete cascade,
  manual_investment_id uuid not null references public.manual_investments(id) on delete cascade,
  snapshot_date date not null,
  current_value_original numeric(20, 8) not null,
  current_value_currency text not null,
  note text,
  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'manual_investment_valuations_current_value_original_check'
  ) then
    alter table public.manual_investment_valuations
      add constraint manual_investment_valuations_current_value_original_check
      check (current_value_original >= 0);
  end if;
end $$;

create index if not exists idx_manual_investments_user_entity
  on public.manual_investments(user_id, entity_id);

create index if not exists idx_manual_investments_funding_account
  on public.manual_investments(funding_account_id);

create unique index if not exists idx_manual_investment_valuations_investment_date
  on public.manual_investment_valuations(manual_investment_id, snapshot_date);

alter table public.manual_investments enable row level security;
alter table public.manual_investment_valuations enable row level security;

create policy manual_investments_owner on public.manual_investments
  for all using (user_id = public.app_current_user_id())
  with check (user_id = public.app_current_user_id());

create policy manual_investment_valuations_owner on public.manual_investment_valuations
  for all using (user_id = public.app_current_user_id())
  with check (user_id = public.app_current_user_id());

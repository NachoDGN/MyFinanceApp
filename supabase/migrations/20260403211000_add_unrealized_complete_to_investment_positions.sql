alter table public.investment_positions
  add column if not exists unrealized_complete boolean not null default true;

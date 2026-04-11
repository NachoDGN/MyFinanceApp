alter table public.transactions
  add column if not exists credit_card_statement_status text not null default 'not_applicable',
  add column if not exists linked_credit_card_account_id uuid references public.accounts(id);

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'transactions_credit_card_statement_status_check'
  ) then
    alter table public.transactions
      add constraint transactions_credit_card_statement_status_check
      check (
        credit_card_statement_status in ('not_applicable', 'upload_required', 'uploaded')
      );
  end if;
end $$;

alter table public.import_batches
  add column if not exists credit_card_settlement_transaction_id uuid references public.transactions(id) on delete set null,
  add column if not exists statement_net_amount_base_eur numeric(20, 8);

update public.transactions
set
  credit_card_statement_status = 'upload_required',
  needs_review = true,
  review_reason = coalesce(
    nullif(review_reason, ''),
    'Upload the matching credit-card statement to resolve category KPIs.'
  ),
  updated_at = timezone('utc', now())
where credit_card_statement_status = 'not_applicable'
  and (
    coalesce(description_raw, '') ~* 'LIQUIDACI[OÓ]N'
    or coalesce(description_clean, '') ~* 'LIQUIDACI[OÓ]N'
  )
  and (
    coalesce(description_raw, '') ~* 'TARJETAS? DE CR[EÉ]DITO'
    or coalesce(description_clean, '') ~* 'TARJETAS? DE CR[EÉ]DITO'
  );

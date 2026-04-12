insert into public.categories (
  code,
  display_name,
  parent_code,
  scope_kind,
  direction_kind,
  sort_order,
  active,
  metadata_json
)
values
  ('travel', 'Travel', null, 'both', 'expense', 10, true, '{}'::jsonb),
  ('debt', 'Debt', null, 'both', 'neutral', 16, true, '{}'::jsonb)
on conflict (code) do update
set
  display_name = excluded.display_name,
  parent_code = excluded.parent_code,
  scope_kind = excluded.scope_kind,
  direction_kind = excluded.direction_kind,
  sort_order = excluded.sort_order,
  active = excluded.active,
  metadata_json = excluded.metadata_json;

update public.transactions
set
  category_code = 'debt',
  updated_at = now()
where transaction_class in (
    'loan_inflow',
    'loan_principal_payment',
    'loan_interest_payment'
  )
  and (
    category_code is null or
    category_code in ('uncategorized_expense', 'uncategorized_income')
  );

update public.transactions
set
  needs_review = true,
  review_reason = coalesce(
    review_reason,
    'Assign a category before this transaction can be treated as resolved.'
  ),
  updated_at = now()
where category_code in ('uncategorized_expense', 'uncategorized_income')
  and coalesce(llm_payload ->> 'analysisStatus', '') <> 'pending'
  and voided_at is null;

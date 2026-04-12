update public.transactions
set
  category_code = 'travel',
  needs_review = false,
  review_reason = null,
  updated_at = now()
where provider_name = 'revolut_business'
  and transaction_class in ('expense', 'refund')
  and category_code in ('uncategorized_expense', 'uncategorized_income')
  and raw_payload -> 'providerContext' -> 'merchant' ->> 'categoryCode' in (
    '3001',
    '4722'
  );

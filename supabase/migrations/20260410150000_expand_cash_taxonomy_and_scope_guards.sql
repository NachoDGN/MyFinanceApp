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
  ('rent', 'Rent', null, 'personal', 'expense', 2, true, '{}'::jsonb),
  ('mortgage', 'Mortgage', null, 'personal', 'expense', 3, true, '{}'::jsonb),
  ('utilities', 'Utilities', null, 'personal', 'expense', 4, true, '{}'::jsonb),
  ('dining', 'Dining', null, 'personal', 'expense', 5, true, '{}'::jsonb),
  ('transport', 'Transport', null, 'personal', 'expense', 6, true, '{}'::jsonb),
  ('subscriptions', 'Subscriptions', null, 'personal', 'expense', 7, true, '{}'::jsonb),
  ('insurance', 'Insurance', null, 'personal', 'expense', 8, true, '{}'::jsonb),
  ('health', 'Health', null, 'personal', 'expense', 9, true, '{}'::jsonb),
  ('travel', 'Travel', null, 'personal', 'expense', 10, true, '{}'::jsonb),
  ('entertainment', 'Entertainment', null, 'personal', 'expense', 12, true, '{}'::jsonb),
  ('education', 'Education', null, 'personal', 'expense', 13, true, '{}'::jsonb),
  ('home_maintenance', 'Home Maintenance', null, 'personal', 'expense', 14, true, '{}'::jsonb),
  ('tax', 'Tax', null, 'personal', 'expense', 15, true, '{}'::jsonb),
  ('cash_withdrawal', 'Cash Withdrawal', null, 'personal', 'neutral', 16, true, '{}'::jsonb),
  ('business_income', 'Business Income', null, 'personal', 'income', 17, true, '{}'::jsonb),
  ('dividend_income', 'Dividend Income', null, 'system', 'income', 19, true, '{}'::jsonb),
  ('interest_income', 'Interest Income', null, 'system', 'income', 20, true, '{}'::jsonb),
  ('transfer_between_accounts', 'Transfer Between Accounts', null, 'system', 'neutral', 21, true, '{}'::jsonb),
  ('atm_fee', 'ATM Fee', null, 'system', 'expense', 22, true, '{}'::jsonb),
  ('bank_fee', 'Bank Fee', null, 'system', 'expense', 23, true, '{}'::jsonb),
  ('uncategorized_income', 'Uncategorized Income', null, 'system', 'income', 34, true, '{}'::jsonb)
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
  source_fingerprint = 'tx-notion-subscription-2026-04-02',
  economic_entity_id = '00000000-0000-0000-0000-000000000101',
  amount_original = -32.00,
  amount_base_eur = -32.00,
  description_raw = 'Notion subscription',
  description_clean = 'NOTION SUBSCRIPTION',
  merchant_normalized = 'NOTION',
  counterparty_name = null,
  transaction_class = 'expense',
  category_code = 'subscriptions',
  transfer_group_id = null,
  related_account_id = null,
  related_transaction_id = null,
  transfer_match_status = 'not_transfer',
  cross_entity_flag = false,
  reimbursement_status = 'none',
  classification_status = 'manual_override',
  classification_source = 'manual',
  classification_confidence = 1.0,
  needs_review = false,
  review_reason = null,
  manual_notes = 'Personal productivity subscription charged to the Santander checking account.',
  llm_payload = null,
  security_id = null,
  quantity = null,
  unit_price_original = null,
  updated_at = now()
where id = '00000000-0000-0000-0000-000000000504';

update public.classification_rules
set
  scope_json = '{"account_id":"00000000-0000-0000-0000-000000000201"}'::jsonb,
  outputs_json = '{"transaction_class":"expense","category_code":"subscriptions","merchant_normalized":"NOTION"}'::jsonb,
  updated_at = now()
where id = '00000000-0000-0000-0000-000000000702';

update public.jobs
set payload_json = '{
  "requestText":"Whenever my Santander personal card description contains NOTION, classify it as a personal subscriptions expense and set the merchant to NOTION.",
  "parsedRule":{
    "title":"Notion personal subscription rule",
    "summary":"Keeps recurring Notion charges in personal subscriptions for the Santander personal account.",
    "priority":25,
    "scopeJson":{"account_id":"00000000-0000-0000-0000-000000000201"},
    "conditionsJson":{"normalized_description_regex":"NOTION"},
    "outputsJson":{"transaction_class":"expense","category_code":"subscriptions","merchant_normalized":"NOTION"},
    "confidence":"0.94",
    "explanation":[
      "The request named a specific merchant and a personal spending category.",
      "The rule is scoped to the named Santander personal account for safety."
    ],
    "parseSource":"llm",
    "model":"gpt-4.1-mini",
    "generatedAt":"2026-04-03T08:25:00Z"
  },
  "appliedRuleId":"00000000-0000-0000-0000-000000000702"
}'::jsonb
where id = '00000000-0000-0000-0000-000000000900'
  and job_type = 'rule_parse';

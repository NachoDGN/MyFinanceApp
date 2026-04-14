insert into auth.users (
  instance_id,
  id,
  aud,
  role,
  email,
  encrypted_password,
  email_confirmed_at,
  confirmation_token,
  email_change,
  email_change_token_new,
  recovery_token,
  raw_app_meta_data,
  raw_user_meta_data,
  created_at,
  updated_at
)
values (
  '00000000-0000-0000-0000-000000000000',
  '00000000-0000-0000-0000-000000000001',
  'authenticated',
  'authenticated',
  'dev@myfinance.local',
  crypt('password', gen_salt('bf')),
  timezone('utc', now()),
  '',
  '',
  '',
  '',
  '{"provider":"email","providers":["email"]}',
  '{}',
  timezone('utc', now()),
  timezone('utc', now())
)
on conflict (id) do nothing;

insert into public.profiles (
  id,
  email,
  display_name,
  default_base_currency,
  timezone,
  workspace_settings_json
)
values (
  '00000000-0000-0000-0000-000000000001',
  'dev@myfinance.local',
  'Seeded Developer',
  'EUR',
  'Europe/Madrid',
  '{"preferredScope":"consolidated","defaultDisplayCurrency":"EUR","defaultPeriodPreset":"mtd","defaultCashStaleAfterDays":7,"defaultInvestmentStaleAfterDays":3}'::jsonb
)
on conflict (id) do nothing;

insert into public.entities (id, user_id, slug, display_name, legal_name, entity_kind, base_currency)
values
  ('00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000001', 'personal', 'Personal', null, 'personal', 'EUR'),
  ('00000000-0000-0000-0000-000000000102', '00000000-0000-0000-0000-000000000001', 'company_a', 'Company A', 'Company A SL', 'company', 'EUR'),
  ('00000000-0000-0000-0000-000000000103', '00000000-0000-0000-0000-000000000001', 'company_b', 'Company B', 'Company B SL', 'company', 'EUR')
on conflict (id) do nothing;

insert into public.import_templates (
  id,
  user_id,
  name,
  institution_name,
  compatible_account_type,
  file_kind,
  sheet_name,
  header_row_index,
  rows_to_skip_before_header,
  rows_to_skip_after_header,
  delimiter,
  encoding,
  decimal_separator,
  thousands_separator,
  date_format,
  default_currency,
  column_map_json,
  sign_logic_json,
  normalization_rules_json,
  active,
  version
)
values
  (
    '00000000-0000-0000-0000-000000000301',
    '00000000-0000-0000-0000-000000000001',
    'Santander Personal CSV v1',
    'Santander',
    'checking',
    'csv',
    null,
    1,
    0,
    0,
    ',',
    'utf-8',
    '.',
    ',',
    '%Y-%m-%d',
    'EUR',
    '{"transaction_date":"date","description_raw":"description","amount_original_signed":"amount","currency_original":"currency","balance_original":"balance","external_reference":"reference"}',
    '{"mode":"signed_amount"}',
    '{"trim_whitespace":true,"collapse_spaces":true}',
    true,
    1
  ),
  (
    '00000000-0000-0000-0000-000000000302',
    '00000000-0000-0000-0000-000000000001',
    'IBKR Activity XLSX v1',
    'Interactive Brokers',
    'brokerage_account',
    'xlsx',
    'Transactions',
    3,
    2,
    0,
    null,
    null,
    '.',
    ',',
    '%Y-%m-%d',
    'USD',
    '{"transaction_date":"date","description_raw":"description","amount_original_signed":"net_amount","currency_original":"currency","balance_original":"cash_balance","external_reference":"trade_id"}',
    '{"mode":"signed_amount"}',
    '{"trim_whitespace":true,"collapse_spaces":true}',
    true,
    1
  )
on conflict (id) do nothing;

insert into public.accounts (
  id,
  user_id,
  entity_id,
  institution_name,
  display_name,
  account_type,
  asset_domain,
  default_currency,
  opening_balance_original,
  opening_balance_currency,
  opening_balance_date,
  include_in_consolidation,
  is_active,
  import_template_default_id,
  matching_aliases,
  account_suffix,
  balance_mode,
  stale_after_days,
  last_imported_at
)
values
  ('00000000-0000-0000-0000-000000000201', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000101', 'Santander', 'Personal Checking', 'checking', 'cash', 'EUR', 9000.00, 'EUR', '2026-01-01', true, true, '00000000-0000-0000-0000-000000000301', '{"BROKER TRANSFER","IBKR"}', '8942', 'statement', 7, '2026-04-03T07:20:00Z'),
  ('00000000-0000-0000-0000-000000000202', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000102', 'BBVA', 'Company A Operating', 'company_bank', 'cash', 'EUR', 42000.00, 'EUR', '2026-01-01', true, true, null, '{"CLIENT PAYOUT"}', '1024', 'statement', 7, '2026-04-02T18:00:00Z'),
  ('00000000-0000-0000-0000-000000000203', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000103', 'CaixaBank', 'Company B Operating', 'company_bank', 'cash', 'EUR', 22000.00, 'EUR', '2026-01-01', true, true, null, '{"SUPPLIER","PAYROLL"}', '7781', 'statement', 7, '2026-03-21T16:45:00Z'),
  ('00000000-0000-0000-0000-000000000204', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000101', 'Interactive Brokers', 'Personal Brokerage', 'brokerage_account', 'investment', 'USD', 7500.00, 'USD', '2026-01-01', true, true, '00000000-0000-0000-0000-000000000302', '{"AMD","ALPHABET","DIVIDEND"}', 'IB01', 'statement', 3, '2026-04-03T07:25:00Z')
on conflict (id) do nothing;

insert into public.categories (code, display_name, parent_code, scope_kind, direction_kind, sort_order, active, metadata_json)
values
  ('groceries', 'Groceries', null, 'personal', 'expense', 1, true, '{}'::jsonb),
  ('rent', 'Rent', null, 'personal', 'expense', 2, true, '{}'::jsonb),
  ('mortgage', 'Mortgage', null, 'personal', 'expense', 3, true, '{}'::jsonb),
  ('utilities', 'Utilities', null, 'personal', 'expense', 4, true, '{}'::jsonb),
  ('dining', 'Dining', null, 'personal', 'expense', 5, true, '{}'::jsonb),
  ('transport', 'Transport', null, 'personal', 'expense', 6, true, '{}'::jsonb),
  ('subscriptions', 'Subscriptions', null, 'personal', 'expense', 7, true, '{}'::jsonb),
  ('insurance', 'Insurance', null, 'personal', 'expense', 8, true, '{}'::jsonb),
  ('health', 'Health', null, 'personal', 'expense', 9, true, '{}'::jsonb),
  ('travel', 'Travel', null, 'both', 'expense', 10, true, '{}'::jsonb),
  ('shopping', 'Shopping', null, 'personal', 'expense', 11, true, '{}'::jsonb),
  ('entertainment', 'Entertainment', null, 'personal', 'expense', 12, true, '{}'::jsonb),
  ('education', 'Education', null, 'personal', 'expense', 13, true, '{}'::jsonb),
  ('home_maintenance', 'Home Maintenance', null, 'personal', 'expense', 14, true, '{}'::jsonb),
  ('tax', 'Tax', null, 'both', 'expense', 15, true, '{}'::jsonb),
  ('debt', 'Debt', null, 'both', 'neutral', 16, true, '{}'::jsonb),
  ('cash_withdrawal', 'Cash Withdrawal', null, 'personal', 'neutral', 16, true, '{}'::jsonb),
  ('business_income', 'Business Income', null, 'personal', 'income', 17, true, '{}'::jsonb),
  ('tax_credit', 'Tax Credit', null, 'both', 'income', 18, true, '{}'::jsonb),
  ('government_subsidy', 'Government Subsidy', null, 'both', 'income', 19, true, '{}'::jsonb),
  ('salary', 'Salary', null, 'system', 'income', 18, true, '{}'::jsonb),
  ('dividend_income', 'Dividend Income', null, 'system', 'income', 19, true, '{}'::jsonb),
  ('interest_income', 'Interest Income', null, 'system', 'income', 20, true, '{}'::jsonb),
  ('transfer_between_accounts', 'Transfer Between Accounts', null, 'system', 'neutral', 21, true, '{}'::jsonb),
  ('atm_fee', 'ATM Fee', null, 'system', 'expense', 22, true, '{}'::jsonb),
  ('bank_fee', 'Bank Fee', null, 'system', 'expense', 23, true, '{}'::jsonb),
  ('software', 'Software', null, 'company', 'expense', 24, true, '{}'::jsonb),
  ('contractors', 'Contractors', null, 'company', 'expense', 25, true, '{}'::jsonb),
  ('client_payment', 'Client Payment', null, 'company', 'income', 26, true, '{}'::jsonb),
  ('office', 'Office', null, 'company', 'expense', 27, true, '{}'::jsonb),
  ('meals', 'Meals', null, 'company', 'expense', 28, true, '{}'::jsonb),
  ('dividend', 'Dividend', null, 'investment', 'income', 27, true, '{}'::jsonb),
  ('interest', 'Interest', null, 'investment', 'income', 28, true, '{}'::jsonb),
  ('stock_buy', 'Stock Buy', null, 'investment', 'investment', 29, true, '{}'::jsonb),
  ('broker_fee', 'Broker Fee', null, 'investment', 'investment', 30, true, '{}'::jsonb),
  ('cash_transfer_to_broker', 'Cash Transfer To Broker', null, 'investment', 'neutral', 31, true, '{}'::jsonb),
  ('cash_transfer_from_broker', 'Cash Transfer From Broker', null, 'investment', 'neutral', 32, true, '{}'::jsonb),
  ('uncategorized_expense', 'Uncategorized Expense', null, 'system', 'expense', 33, true, '{}'::jsonb),
  ('uncategorized_income', 'Uncategorized Income', null, 'system', 'income', 34, true, '{}'::jsonb),
  ('uncategorized_investment', 'Uncategorized Investment', null, 'investment', 'investment', 35, true, '{}'::jsonb)
on conflict (code) do nothing;

insert into public.import_batches (
  id,
  user_id,
  account_id,
  template_id,
  storage_path,
  original_filename,
  file_sha256,
  status,
  row_count_detected,
  row_count_parsed,
  row_count_inserted,
  row_count_duplicates,
  row_count_failed,
  preview_summary_json,
  commit_summary_json,
  imported_by_actor,
  imported_at,
  classification_triggered_at,
  notes
)
values
  (
    '00000000-0000-0000-0000-000000000401',
    '00000000-0000-0000-0000-000000000001',
    '00000000-0000-0000-0000-000000000201',
    '00000000-0000-0000-0000-000000000301',
    'private-imports/personal/santander-apr.csv',
    'santander-apr.csv',
    'd3b07384d113edec49eaa6238ad5ff00',
    'committed',
    8,
    8,
    8,
    0,
    0,
    '{"duplicateCount":0}',
    '{"queuedJobs":["classification","metric_refresh"]}',
    'Seeded Developer',
    '2026-04-03T07:20:00Z',
    '2026-04-03T07:22:00Z',
    null
  ),
  (
    '00000000-0000-0000-0000-000000000402',
    '00000000-0000-0000-0000-000000000001',
    '00000000-0000-0000-0000-000000000204',
    '00000000-0000-0000-0000-000000000302',
    'private-imports/broker/ibkr-apr.xlsx',
    'ibkr-apr.xlsx',
    '8ad8757baa8564dc136c1e07507f4a98',
    'committed',
    6,
    6,
    5,
    1,
    0,
    '{"duplicateCount":1}',
    '{"queuedJobs":["classification","transfer_rematch","position_rebuild","metric_refresh"]}',
    'Seeded Developer',
    '2026-04-03T07:25:00Z',
    '2026-04-03T07:26:00Z',
    'One duplicate trade row skipped.'
  )
on conflict (id) do nothing;

insert into public.securities (
  id,
  provider_name,
  provider_symbol,
  canonical_symbol,
  display_symbol,
  name,
  exchange_name,
  mic_code,
  asset_type,
  quote_currency,
  country,
  isin,
  figi,
  active,
  metadata_json,
  last_price_refresh_at
)
values
  ('00000000-0000-0000-0000-000000000901', 'twelve_data', 'AMD', 'AMD', 'AMD', 'Advanced Micro Devices Inc', 'NASDAQ', 'XNAS', 'stock', 'USD', 'US', null, null, true, '{"sector":"Semiconductors"}', '2026-04-03T08:20:00Z'),
  ('00000000-0000-0000-0000-000000000902', 'twelve_data', 'GOOGL', 'GOOGL', 'GOOGL', 'Alphabet Inc Class A', 'NASDAQ', 'XNAS', 'stock', 'USD', 'US', null, null, true, '{"sector":"Communication Services"}', '2026-04-03T08:20:00Z'),
  ('00000000-0000-0000-0000-000000000903', 'twelve_data', 'VWCE', 'VWCE', 'VWCE', 'Vanguard FTSE All-World UCITS ETF', 'XETRA', 'XETR', 'etf', 'EUR', 'DE', 'IE00BK5BQT80', null, true, '{"theme":"All World"}', '2026-04-03T08:20:00Z')
on conflict (id) do nothing;

insert into public.security_aliases (id, security_id, alias_text_normalized, alias_source, confidence)
values
  ('00000000-0000-0000-0000-000000000951', '00000000-0000-0000-0000-000000000901', 'ADVANCED MICRO DEVICES', 'manual', 1.0),
  ('00000000-0000-0000-0000-000000000952', '00000000-0000-0000-0000-000000000903', 'VANGUARD FTSE ALL WORLD', 'manual', 1.0)
on conflict (id) do nothing;

insert into public.security_prices (
  security_id,
  price_date,
  quote_timestamp,
  price,
  currency,
  source_name,
  is_realtime,
  is_delayed,
  market_state,
  raw_json
)
values
  ('00000000-0000-0000-0000-000000000901', '2026-04-03', '2026-04-03T08:20:00Z', 152.40, 'USD', 'twelve_data', false, true, 'closed', '{}'::jsonb),
  ('00000000-0000-0000-0000-000000000903', '2026-04-03', '2026-04-03T08:20:00Z', 135.50, 'EUR', 'twelve_data', false, true, 'closed', '{}'::jsonb)
on conflict do nothing;

insert into public.fx_rates (base_currency, quote_currency, as_of_date, as_of_timestamp, rate, source_name, raw_json)
values
  ('USD', 'EUR', '2026-04-03', '2026-04-03T08:20:00Z', 0.920000, 'twelve_data', '{}'::jsonb),
  ('EUR', 'USD', '2026-04-03', '2026-04-03T08:20:00Z', 1.086957, 'twelve_data', '{}'::jsonb)
on conflict do nothing;

insert into public.transactions (
  id,
  user_id,
  account_id,
  account_entity_id,
  economic_entity_id,
  import_batch_id,
  source_fingerprint,
  duplicate_key,
  transaction_date,
  posted_date,
  amount_original,
  currency_original,
  amount_base_eur,
  fx_rate_to_eur,
  description_raw,
  description_clean,
  merchant_normalized,
  counterparty_name,
  transaction_class,
  category_code,
  transfer_group_id,
  related_account_id,
  related_transaction_id,
  transfer_match_status,
  cross_entity_flag,
  reimbursement_status,
  classification_status,
  classification_source,
  classification_confidence,
  needs_review,
  review_reason,
  exclude_from_analytics,
  manual_notes,
  llm_payload,
  raw_payload,
  security_id,
  quantity,
  unit_price_original
)
values
  ('00000000-0000-0000-0000-000000000501', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000201', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000401', 'tx-salary-2026-04-01', null, '2026-04-01', '2026-04-01', 4200.00, 'EUR', 4200.00, 1.0, 'Payroll ACME Europe', 'PAYROLL ACME EUROPE', 'ACME EUROPE', 'Employer', 'income', 'salary', null, null, null, 'not_transfer', false, 'none', 'rule', 'user_rule', 1.0, false, null, false, null, null, '{"source":"santander"}', null, null, null),
  ('00000000-0000-0000-0000-000000000502', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000202', '00000000-0000-0000-0000-000000000102', '00000000-0000-0000-0000-000000000102', null, 'tx-client-2026-04-01', null, '2026-04-01', '2026-04-01', 6800.00, 'EUR', 6800.00, 1.0, 'Client payout / UX retainer', 'CLIENT PAYOUT UX RETAINER', 'CLIENT A', 'Client A', 'income', 'client_payment', null, null, null, 'not_transfer', false, 'none', 'rule', 'user_rule', 1.0, false, null, false, null, null, '{"source":"bbva"}', null, null, null),
  ('00000000-0000-0000-0000-000000000504', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000201', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000401', 'tx-notion-subscription-2026-04-02', null, '2026-04-02', '2026-04-02', -32.00, 'EUR', -32.00, 1.0, 'Notion subscription', 'NOTION SUBSCRIPTION', 'NOTION', null, 'expense', 'subscriptions', null, null, null, 'not_transfer', false, 'none', 'manual_override', 'manual', 1.0, false, null, false, 'Personal productivity subscription charged to the Santander checking account.', null, '{"source":"santander"}', null, null, null),
  ('00000000-0000-0000-0000-000000000505', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000201', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000401', 'tx-broker-transfer-out-2026-04-02', 'xfer-2026-04-02-2000', '2026-04-02', '2026-04-02', -2000.00, 'EUR', -2000.00, 1.0, 'Transfer to IBKR account', 'TRANSFER TO IBKR ACCOUNT', null, 'Interactive Brokers', 'transfer_internal', 'cash_transfer_to_broker', '00000000-0000-0000-0000-000000000601', '00000000-0000-0000-0000-000000000204', '00000000-0000-0000-0000-000000000506', 'matched', false, 'none', 'transfer_match', 'transfer_matcher', 1.0, false, null, false, null, null, '{"source":"santander"}', null, null, null),
  ('00000000-0000-0000-0000-000000000506', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000204', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000402', 'tx-broker-transfer-in-2026-04-02', 'xfer-2026-04-02-2000', '2026-04-02', '2026-04-02', 2000.00, 'EUR', 2000.00, 1.0, 'Bank transfer received', 'BANK TRANSFER RECEIVED', null, 'Santander', 'transfer_internal', 'cash_transfer_from_broker', '00000000-0000-0000-0000-000000000601', '00000000-0000-0000-0000-000000000201', '00000000-0000-0000-0000-000000000505', 'matched', false, 'none', 'transfer_match', 'transfer_matcher', 1.0, false, null, false, null, null, '{"source":"ibkr"}', null, null, null),
  ('00000000-0000-0000-0000-000000000507', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000204', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000402', 'tx-amd-buy-2026-04-02', null, '2026-04-02', '2026-04-02', -134.20, 'EUR', -134.20, 1.0, 'ADVANCED MICRO DEVICES @ 1', 'ADVANCED MICRO DEVICES @ 1', 'AMD', null, 'investment_trade_buy', 'stock_buy', null, null, null, 'not_transfer', false, 'none', 'investment_parser', 'investment_parser', 0.96, false, null, false, 'Quantity derived from descriptor.', null, '{"source":"ibkr"}', '00000000-0000-0000-0000-000000000901', 1.000000, 132.00),
  ('00000000-0000-0000-0000-000000000508', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000204', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000402', 'tx-amd-fee-2026-04-02', null, '2026-04-02', '2026-04-02', -2.20, 'EUR', -2.20, 1.0, 'Commission for ADVANCED MICRO DEVICES', 'COMMISSION FOR ADVANCED MICRO DEVICES', 'INTERACTIVE BROKERS', null, 'fee', 'broker_fee', null, null, '00000000-0000-0000-0000-000000000507', 'not_transfer', false, 'none', 'investment_parser', 'investment_parser', 0.94, false, null, false, null, null, '{"source":"ibkr"}', '00000000-0000-0000-0000-000000000901', null, null),
  ('00000000-0000-0000-0000-000000000509', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000204', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000402', 'tx-alphabet-2026-04-03', null, '2026-04-03', '2026-04-03', -980.00, 'USD', -901.60, 0.92, 'ALPHABET INC @ 7', 'ALPHABET INC @ 7', null, null, 'unknown', 'uncategorized_investment', null, null, null, 'not_transfer', false, 'none', 'unknown', 'system_fallback', 0.45, true, 'Security mapping ambiguous between Alphabet share classes.', false, null, '{"reason":"Ambiguous security hint."}', '{"source":"ibkr"}', null, 7.000000, null),
  ('00000000-0000-0000-0000-000000000510', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000204', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000402', 'tx-dividend-2026-04-03', null, '2026-04-03', '2026-04-03', 18.50, 'USD', 17.02, 0.92, 'Dividend from Vanguard FTSE All-World', 'DIVIDEND FROM VANGUARD FTSE ALL WORLD', 'VANGUARD', null, 'dividend', 'dividend', null, null, null, 'not_transfer', false, 'none', 'investment_parser', 'investment_parser', 0.98, false, null, false, null, null, '{"source":"ibkr"}', '00000000-0000-0000-0000-000000000903', null, null)
on conflict (id) do nothing;

insert into public.classification_rules (
  id, user_id, priority, active, scope_json, conditions_json, outputs_json, created_from_transaction_id, auto_generated, hit_count, last_hit_at
)
values
  ('00000000-0000-0000-0000-000000000701', '00000000-0000-0000-0000-000000000001', 10, true, '{"account_id":"00000000-0000-0000-0000-000000000202"}', '{"normalized_description_regex":"CLIENT PAYOUT"}', '{"transaction_class":"income","category_code":"client_payment"}', '00000000-0000-0000-0000-000000000502', false, 12, '2026-04-01T08:00:00Z'),
  ('00000000-0000-0000-0000-000000000702', '00000000-0000-0000-0000-000000000001', 20, true, '{"account_id":"00000000-0000-0000-0000-000000000201"}', '{"merchant_equals":"NOTION"}', '{"transaction_class":"expense","category_code":"subscriptions","merchant_normalized":"NOTION"}', '00000000-0000-0000-0000-000000000504', true, 3, '2026-04-02T07:21:00Z')
on conflict (id) do nothing;

insert into public.jobs (id, job_type, payload_json, status, attempts, available_at)
values
  (
    '00000000-0000-0000-0000-000000000900',
    'rule_parse',
    '{"requestText":"Whenever my Santander personal card description contains NOTION, classify it as a personal subscriptions expense and set the merchant to NOTION.","parsedRule":{"title":"Notion personal subscription rule","summary":"Keeps recurring Notion charges in personal subscriptions for the Santander personal account.","priority":25,"scopeJson":{"account_id":"00000000-0000-0000-0000-000000000201"},"conditionsJson":{"normalized_description_regex":"NOTION"},"outputsJson":{"transaction_class":"expense","category_code":"subscriptions","merchant_normalized":"NOTION"},"confidence":"0.94","explanation":["The request named a specific merchant and a personal spending category.","The rule is scoped to the named Santander personal account for safety."],"parseSource":"llm","model":"gpt-4.1-mini","generatedAt":"2026-04-03T08:25:00Z"},"appliedRuleId":"00000000-0000-0000-0000-000000000702"}',
    'completed',
    1,
    '2026-04-03T08:20:00Z'
  ),
  ('00000000-0000-0000-0000-000000000901', 'price_refresh', '{"scope":"consolidated"}', 'queued', 0, '2026-04-03T08:30:00Z')
on conflict (id) do nothing;

insert into public.account_balance_snapshots (account_id, as_of_date, balance_original, balance_currency, balance_base_eur, source_kind, import_batch_id)
values
  ('00000000-0000-0000-0000-000000000201', '2026-04-03', 12779.80, 'EUR', 12779.80, 'statement', '00000000-0000-0000-0000-000000000401'),
  ('00000000-0000-0000-0000-000000000202', '2026-04-03', 48400.00, 'EUR', 48400.00, 'statement', null),
  ('00000000-0000-0000-0000-000000000203', '2026-03-20', 19750.00, 'EUR', 19750.00, 'statement', null),
  ('00000000-0000-0000-0000-000000000204', '2026-04-03', 9810.30, 'USD', 9025.48, 'statement', '00000000-0000-0000-0000-000000000402')
on conflict do nothing;

insert into public.holding_adjustments (id, user_id, entity_id, account_id, security_id, effective_date, share_delta, cost_basis_delta_eur, reason, note)
values
  ('00000000-0000-0000-0000-000000000961', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000204', '00000000-0000-0000-0000-000000000903', '2026-01-01', 40.000000, 5000.00, 'opening_position', 'Historical ETF holding')
on conflict (id) do nothing;

insert into public.investment_positions (
  user_id, entity_id, account_id, security_id, open_quantity, open_cost_basis_eur, avg_cost_eur, realized_pnl_eur, dividends_eur, interest_eur, fees_eur, last_trade_date, provenance_json
)
values
  ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000204', '00000000-0000-0000-0000-000000000901', 1.000000, 134.20, 134.20, 0.00, 0.00, 0.00, 2.20, '2026-04-02', '{"source":"transactions"}'),
  ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000204', '00000000-0000-0000-0000-000000000903', 40.000000, 5000.00, 125.00, 0.00, 17.02, 0.00, 0.00, '2026-01-01', '{"source":"holding_adjustment"}')
on conflict do nothing;

insert into public.daily_portfolio_snapshots (
  id, snapshot_date, user_id, entity_id, account_id, security_id, market_value_eur, cost_basis_eur, unrealized_pnl_eur, cash_balance_eur, total_portfolio_value_eur, generated_at
)
values
  ('00000000-0000-0000-0000-000000000971', '2026-03-31', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000204', null, 5291.60, 5134.20, 157.40, 10009.60, 15301.20, '2026-03-31T22:59:59Z'),
  ('00000000-0000-0000-0000-000000000972', '2026-04-03', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000101', '00000000-0000-0000-0000-000000000204', null, 5571.41, 5134.20, 437.21, 9025.48, 14596.89, '2026-04-03T22:59:59Z')
on conflict (id) do nothing;

select public.refresh_finance_analytics();

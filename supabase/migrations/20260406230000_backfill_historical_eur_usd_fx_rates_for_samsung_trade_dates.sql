insert into public.fx_rates (
  base_currency,
  quote_currency,
  as_of_date,
  as_of_timestamp,
  rate,
  source_name,
  raw_json
)
values
  (
    'EUR',
    'USD',
    '2026-02-06',
    '2026-02-06T16:00:00Z',
    1.17940000,
    'banque_france',
    '{"pair":"EUR/USD","note":"Backfilled historical daily parity for Samsung trade-date display."}'::jsonb
  ),
  (
    'USD',
    'EUR',
    '2026-02-06',
    '2026-02-06T16:00:00Z',
    0.84788876,
    'banque_france',
    '{"pair":"USD/EUR","note":"Backfilled reciprocal historical daily parity for Samsung trade-date display."}'::jsonb
  ),
  (
    'EUR',
    'USD',
    '2026-03-04',
    '2026-03-04T16:00:00Z',
    1.16490000,
    'banque_france',
    '{"pair":"EUR/USD","note":"Backfilled historical daily parity for Samsung trade-date display."}'::jsonb
  ),
  (
    'USD',
    'EUR',
    '2026-03-04',
    '2026-03-04T16:00:00Z',
    0.85844278,
    'banque_france',
    '{"pair":"USD/EUR","note":"Backfilled reciprocal historical daily parity for Samsung trade-date display."}'::jsonb
  )
on conflict (base_currency, quote_currency, as_of_date, source_name)
do update set
  as_of_timestamp = excluded.as_of_timestamp,
  rate = excluded.rate,
  raw_json = excluded.raw_json;
